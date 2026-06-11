"""
justhodl-upside-radar v1.0 — Market-Wide Multi-Bagger Discovery
===============================================================
The missing discovery layer: every liquid US stock (not a 48-name universe),
screened daily for the four price signatures that precede huge winners, then
gated by bagger anatomy fundamentals. Self-warming: one Polygon grouped call
per session maintains rolling 252d close-rings for all liquidity-qualified
tickers (~2-3k names); cold-start backfills within its time budget and
resumes across runs until warm.

Scans (price-side, measured thresholds):
  BREAKOUT   within 2% of 252d high (or fresh high) on dollar-volume z ≥ 2
  RS LEADER  12m return ≥ 95th pct of universe AND 3m return > 0
  COILED     63d range ≤ 12% of price AND within 5% of 252d high
  FOOTPRINT  dollar-volume ≥ 4× 20d avg, up day, still ≥15% below 252d high

Anatomy gate (FMP, top candidates only): revenue growth, share-count change
(dilution kills baggers), market cap, margin trend → anatomy_score.
Top names log to the closed loop; /skill.html grades the whole thesis.
"""
import json, os, time, gzip, io as _io, urllib.request
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from decimal import Decimal
import boto3

S3 = boto3.client("s3", region_name="us-east-1")
DDB = boto3.resource("dynamodb", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/upside-radar.json"
STATE_KEY = "data/_upside/state.json.gz"
POLY_KEY = os.environ.get("POLYGON_KEY", "zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d")
FMP_KEY = os.environ.get("FMP_KEY", "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb")
VERSION = "1.0.1"
RING = 256
DV_FLOOR = 3_000_000  # $ 20d avg dollar-volume
TIME_BUDGET = 420


def grouped(date):
    u = (f"https://api.polygon.io/v2/aggs/grouped/locale/us/market/stocks/{date}"
         f"?adjusted=true&apiKey={POLY_KEY}")
    try:
        j = json.loads(urllib.request.urlopen(u, timeout=60).read())
        out = {}
        for r in (j.get("results") or []):
            t, c, v = r.get("T"), r.get("c"), r.get("v") or 0
            if not t or not c or len(t) > 5 or "." in t:
                continue
            if len(t) == 5 and t[-1] in "WURQ":   # warrants/units/rights/bankrupt
                continue
            out[t] = (float(c), float(v))
        return out
    except Exception as e:
        print(f"[grouped] {date}: {str(e)[:50]}")
        return {}


def fetch_etf_set():
    """Non-common-stock tickers (ETF/ETN/FUND/...) via Polygon reference."""
    out = set()
    url = ("https://api.polygon.io/v3/reference/tickers?market=stocks"
           f"&active=true&limit=1000&apiKey={POLY_KEY}")
    try:
        for _ in range(40):
            j = json.loads(urllib.request.urlopen(url, timeout=40).read())
            for r in j.get("results") or []:
                if r.get("type") and r["type"] != "CS":
                    out.add(r.get("ticker"))
            nxt = j.get("next_url")
            if not nxt:
                break
            url = nxt + f"&apiKey={POLY_KEY}"
    except Exception as e:
        print(f"[etfset] {str(e)[:60]}")
    return out


def load_state():
    try:
        raw = S3.get_object(Bucket=BUCKET, Key=STATE_KEY)["Body"].read()
        return json.loads(gzip.decompress(raw))
    except Exception:
        return {"last_date": None, "days_seen": 0, "dv": {}, "rings": {}}


def save_state(st):
    buf = gzip.compress(json.dumps(st, separators=(",", ":")).encode())
    S3.put_object(Bucket=BUCKET, Key=STATE_KEY, Body=buf,
                  ContentType="application/json", ContentEncoding="gzip")
    print(f"[state] {st['days_seen']}d · rings={len(st['rings'])} · {len(buf)//1024}KB")


def sessions_to_fill(st, max_back=270):
    today = datetime.now(timezone.utc).date()
    out, d = [], today
    while len(out) < max_back:
        d -= timedelta(days=1)
        if d.weekday() < 5:
            out.append(d.isoformat())
    out = list(reversed(out))
    if st["last_date"]:
        out = [x for x in out if x > st["last_date"]]
    return out


def ingest(st, t0):
    todo = sessions_to_fill(st)
    n = 0
    for ds in todo:
        if time.time() - t0 > TIME_BUDGET:
            print(f"[ingest] budget hit, {len(todo)-n} sessions remain")
            break
        g = grouped(ds)
        if not g:
            continue
        dv, rings = st["dv"], st["rings"]
        for t, (c, v) in g.items():
            d_ = c * v
            prev = dv.get(t, d_)
            dv[t] = prev * 0.95 + d_ * 0.05   # 20d-ish EMA of dollar volume
            if dv[t] >= DV_FLOOR and c >= 3:
                r = rings.get(t)
                if r is None:
                    rings[t] = [round(c, 3)]
                else:
                    r.append(round(c, 3))
                    if len(r) > RING:
                        del r[: len(r) - RING]
            elif t in rings and dv[t] < DV_FLOOR * 0.5:
                del rings[t]
        st["last_date"] = ds
        st["days_seen"] += 1
        n += 1
    return n


def fmp_anatomy(t):
    try:
        u1 = f"https://financialmodelingprep.com/stable/key-metrics?symbol={t}&limit=5&apikey={FMP_KEY}"
        u2 = f"https://financialmodelingprep.com/stable/income-statement?symbol={t}&limit=5&apikey={FMP_KEY}"
        km = json.loads(urllib.request.urlopen(u1, timeout=25).read()) or []
        inc = json.loads(urllib.request.urlopen(u2, timeout=25).read()) or []
        if not inc:
            return None
        rev = [r.get("revenue") for r in inc if r.get("revenue")]
        gp = [r.get("grossProfit") for r in inc if r.get("grossProfit") is not None]
        sh = [r.get("weightedAverageShsOutDil") or r.get("weightedAverageShsOut")
              for r in inc]
        rev_g = round((rev[0] / rev[1] - 1) * 100, 1) if len(rev) > 1 and rev[1] else None
        dil = round((sh[0] / sh[-1] - 1) * 100, 1) if len(sh) > 1 and sh[0] and sh[-1] else None
        gm_now = round(gp[0] / rev[0] * 100, 1) if gp and rev and rev[0] else None
        gm_then = round(gp[-1] / rev[-1] * 100, 1) if len(gp) > 1 and rev[-1] else None
        mcap = (km[0].get("marketCap") if km else None)
        score = 0
        if rev_g is not None:
            score += 30 if rev_g >= 25 else 18 if rev_g >= 15 else 5 if rev_g > 0 else -15
        if dil is not None:
            score += 20 if dil <= 2 else 0 if dil <= 10 else -25
        if gm_now is not None and gm_then is not None:
            score += 15 if gm_now > gm_then + 1 else 0 if gm_now >= gm_then - 1 else -10
        if mcap:
            score += 20 if mcap < 3e9 else 12 if mcap < 10e9 else 0
        return {"ticker": t, "rev_growth_pct": rev_g, "share_chg_4y_pct": dil,
                "gross_margin_now": gm_now, "gross_margin_then": gm_then,
                "mcap_bn": round(mcap / 1e9, 2) if mcap else None,
                "anatomy_score": score}
    except Exception as e:
        print(f"[fmp] {t}: {str(e)[:40]}")
        return None


def lambda_handler(event=None, context=None):
    t0 = time.time()
    st = load_state()
    if not st.get("etf") or st.get("etf_asof", "") < \
            (datetime.now(timezone.utc) - timedelta(days=7)).date().isoformat():
        es = fetch_etf_set()
        if len(es) > 1000:
            st["etf"] = sorted(es)
            st["etf_asof"] = datetime.now(timezone.utc).date().isoformat()
            print(f"[etfset] {len(es)} non-CS cached")
    fetched = ingest(st, t0)
    save_state(st)
    rings = st["rings"]
    warm = st["days_seen"]
    out = {"engine": "upside-radar", "version": VERSION,
           "generated_at": datetime.now(timezone.utc).isoformat(),
           "state": {"sessions_seen": warm, "rings": len(rings),
                      "fetched_this_run": fetched, "warm": warm >= 250,
                      "as_of": st["last_date"]},
           "scans": {}}

    if rings:
        rows = []
        rets252 = []
        for t, r in rings.items():
            n = len(r)
            if n < 70:
                continue
            c = r[-1]
            look = r[-min(n, 252):]
            hi, lo = max(look), min(look)
            l63 = r[-63:]
            hi63, lo63 = max(l63), min(l63)
            ret63 = (c / r[-63] - 1) * 100
            ret252 = (c / r[-min(n, 252)] - 1) * 100 if n >= 200 else None
            if ret252 is not None:
                rets252.append(ret252)
            rows.append({"t": t, "c": c, "hi": hi, "dist_hi_pct": round((c / hi - 1) * 100, 2),
                          "range63_pct": round((hi63 - lo63) / c * 100, 1),
                          "ret63": round(ret63, 1),
                          "ret252": round(ret252, 1) if ret252 is not None else None,
                          "ring_n": n, "dv": st["dv"].get(t)})
        rets252.sort()

        def pctl(x):
            if x is None or not rets252:
                return None
            import bisect as _b
            return round(100 * _b.bisect_left(rets252, x) / len(rets252), 1)

        latest = grouped(st["last_date"]) if st["last_date"] else {}
        etfset = set(st.get("etf") or [])
        breakout, leaders, coiled, footprint = [], [], [], []
        for r in rows:
            t = r["t"]
            if t in etfset or not t.isalpha() or not t.isupper():
                continue
            if r["range63_pct"] < 2 and abs(r["ret252"] or 0) < 3:
                continue  # cash-like instrument
            r["suspect_split"] = bool((r["ret252"] or 0) > 800)
            cv = latest.get(t)
            dvz = None
            if cv and r["dv"]:
                dvz = round((cv[0] * cv[1]) / r["dv"], 2)   # today $vol vs EMA (x)
            near_hi = r["dist_hi_pct"] >= -2.0
            if near_hi and (dvz or 0) >= 2.0:
                breakout.append({**r, "dvol_x": dvz})
            rp = pctl(r["ret252"])
            if (rp is not None and rp >= 95 and r["ret63"] > 0 and r["ring_n"] >= 250 and not r["suspect_split"]):
                leaders.append({**r, "rs_pctile": rp})
            if (3 <= r["range63_pct"] <= 12 and r["dist_hi_pct"] >= -5 and r["ring_n"] >= 150 and (r["ret252"] or 0) > 5):
                coiled.append(r)
            if (dvz or 0) >= 4.0 and cv and cv[0] > r["c"] * 0.999 and r["dist_hi_pct"] <= -15:
                footprint.append({**r, "dvol_x": dvz})
        breakout.sort(key=lambda x: -(x.get("dvol_x") or 0))
        leaders.sort(key=lambda x: -(x.get("rs_pctile") or 0))
        coiled.sort(key=lambda x: x["range63_pct"])
        footprint.sort(key=lambda x: -(x.get("dvol_x") or 0))
        out["scans"] = {"breakout": breakout[:20], "rs_leaders": leaders[:20],
                         "coiled": coiled[:20], "footprint": footprint[:15],
                         "universe_n": len(rows)}

        # anatomy gate on the union of top candidates
        cand = []
        seen = set()
        for lst in (breakout[:8], leaders[:8], coiled[:6], footprint[:6]):
            for r in lst:
                if r["t"] not in seen:
                    seen.add(r["t"])
                    cand.append(r["t"])
        anatomy = []
        with ThreadPoolExecutor(max_workers=5) as ex:
            for f in as_completed({ex.submit(fmp_anatomy, t): t for t in cand[:22]}):
                a = f.result()
                if a:
                    anatomy.append(a)
        anatomy.sort(key=lambda a: -a["anatomy_score"])
        out["anatomy"] = anatomy

        # closed loop: breakout ∩ anatomy_score≥40 → upside_radar UP
        n_logged = 0
        amap = {a["ticker"]: a for a in anatomy}
        d0 = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        for r in breakout[:8]:
            a = amap.get(r["t"])
            if not a or a["anatomy_score"] < 40 or warm < 100:
                continue
            try:
                nowt = datetime.now(timezone.utc)
                conf = round(min(0.65, 0.50 + a["anatomy_score"] / 400), 2)
                DDB.Table("justhodl-signals").put_item(Item={
                    "signal_id": f"upside-radar#{r['t']}#{d0}",
                    "signal_type": "upside_radar", "signal_value": f"breakout+anatomy{a['anatomy_score']}",
                    "predicted_direction": "UP", "confidence": Decimal(str(conf)),
                    "measure_against": "ticker", "baseline_price": str(r["c"]),
                    "benchmark": "SPY", "check_windows": ["day_21", "day_63"],
                    "check_timestamps": {f"day_{w}": (nowt + timedelta(days=w)).isoformat()
                                          for w in (21, 63)},
                    "outcomes": {}, "accuracy_scores": {}, "logged_at": nowt.isoformat(),
                    "logged_epoch": int(nowt.timestamp()), "status": "pending",
                    "schema_version": "2", "horizon_days_primary": 63,
                    "regime_at_log": "DISCOVERY",
                    "ttl": int(nowt.timestamp()) + 150 * 86400,
                    "metadata": {"engine": "upside-radar", "v": VERSION,
                                 "dvol_x": str(r.get("dvol_x")),
                                 "anatomy": str(a["anatomy_score"])},
                    "rationale": (f"{r['t']} 252d-high breakout on {r.get('dvol_x')}× dollar "
                                   f"volume; anatomy {a['anatomy_score']} (rev {a['rev_growth_pct']}%, "
                                   f"dilution {a['share_chg_4y_pct']}%, mcap {a['mcap_bn']}B)")})
                n_logged += 1
            except Exception as e:
                print(f"[log] {str(e)[:60]}")
        out["signals_logged"] = n_logged

    out["methodology"] = (
        "Full-market state machine: one grouped bar per session maintains 252d close-"
        "rings for every name clearing $3M 20d dollar-volume and $3. Four price scans "
        "(252d-high breakout on volume, RS≥95th pct leadership, 63d coil under highs, "
        "4× volume footprint) feed a bagger-anatomy fundamental gate (revenue growth, "
        "dilution, margin trend, size). Breakouts passing anatomy ≥40 log to the closed "
        "loop at measured confidence. Warm-up honest: scans flagged partial until 252 "
        "sessions seen.")
    out["duration_s"] = round(time.time() - t0, 1)
    S3.put_object(Bucket=BUCKET, Key=OUT_KEY, Body=json.dumps(out, default=str).encode(),
                  ContentType="application/json", CacheControl="public, max-age=1800")
    print(f"[upside] warm={warm} rings={len(rings)} "
          f"scans={[k + ':' + str(len(v)) for k, v in out['scans'].items() if isinstance(v, list)]} "
          f"{out['duration_s']}s")
    return {"statusCode": 200, "body": json.dumps(out["state"])}
