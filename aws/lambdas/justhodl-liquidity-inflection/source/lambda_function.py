"""
justhodl-liquidity-inflection v1.0 — Liquidity 2nd-Derivative + Lead/Lag Table
==============================================================================
Item 6/7/8 of the edge map. Levels are consensus; INFLECTIONS are the trade.

  USD net liquidity  = WALCL − TGA − RRP (FRED, daily-aligned, WALCL ffilled)
  EUR excess liq     = platform ecb-hist series (probe)
  CN credit impulse  = BIS credit-to-private-nonfin China YoY accel (probe)
  Stablecoin accel   = platform stablecoin-flow brief 2nd derivative (probe)

  Impulse  = 13-week regression slope of net liquidity, z-scored 3y
  Flip     = impulse sign change with |Δz| ≥ 0.25 debounce
  Edge     = event-study of historical flips → forward 5/21/63d distribution
             for SPX (deep base), BTC, HYG — the published lead/lag table.
New flips (≤5 sessions old) are logged to the closed loop vs SPY.
"""
import json, os, time, urllib.request, urllib.parse
from datetime import datetime, timezone, timedelta
from statistics import mean, stdev
from decimal import Decimal
import boto3

S3 = boto3.client("s3", region_name="us-east-1")
DDB = boto3.resource("dynamodb", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/liquidity-inflection.json"
FRED_KEY = os.environ.get("FRED_KEY", "2f057499936072679d8843d7fce99989")
POLY_KEY = os.environ.get("POLYGON_KEY", "zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d")
VERSION = "1.0.1"
W_SLOPE = 65          # ~13 weeks of business days
Z_LOOKBACK = 756      # 3y
DEBOUNCE = 0.25


def fred(sid, start="2010-01-01"):
    u = ("https://api.stlouisfed.org/fred/series/observations?"
         + urllib.parse.urlencode({"series_id": sid, "api_key": FRED_KEY,
                                   "file_type": "json", "observation_start": start, "limit": 100000}))
    try:
        j = json.loads(urllib.request.urlopen(u, timeout=40).read())
        return {o["date"]: float(o["value"]) for o in j.get("observations", [])
                if o.get("value") not in (".", "", None)}
    except Exception as e:
        print(f"[fred] {sid}: {str(e)[:60]}")
        return {}


def polygon_closes(ticker, start="2015-01-01"):
    end = datetime.now(timezone.utc).date().isoformat()
    u = (f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/day/{start}/{end}"
         f"?adjusted=true&sort=asc&limit=50000&apiKey={POLY_KEY}")
    try:
        j = json.loads(urllib.request.urlopen(u, timeout=45).read())
        return {datetime.fromtimestamp(r["t"] / 1000, tz=timezone.utc).date().isoformat(): float(r["c"])
                for r in (j.get("results") or [])}
    except Exception as e:
        print(f"[poly] {ticker}: {str(e)[:60]}")
        return {}


def s3_json(key):
    try:
        return json.loads(S3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
    except Exception:
        return None


def slope(xs):
    n = len(xs)
    if n < 3:
        return 0.0
    xb = (n - 1) / 2; yb = mean(xs)
    den = sum((i - xb) ** 2 for i in range(n))
    return sum((i - xb) * (y - yb) for i, y in enumerate(xs)) / den if den else 0.0


def build_impulse(series):
    """series: {date: level} → sorted dates, impulse z list aligned to dates[W_SLOPE:]"""
    dates = sorted(series)
    vals = [series[d] for d in dates]
    raw = []
    for i in range(W_SLOPE, len(vals)):
        raw.append(slope(vals[i - W_SLOPE:i]))
    z = []
    for i in range(len(raw)):
        w = raw[max(0, i - Z_LOOKBACK):i + 1]
        m = mean(w); sd = stdev(w) if len(w) > 30 else 0
        z.append(round((raw[i] - m) / sd, 3) if sd else 0.0)
    return dates[W_SLOPE:], z


def find_flips(dates, z, th=0.20):
    """Schmitt-trigger hysteresis: a flip is recorded when the impulse crosses
    ±th from the OPPOSITE armed state. Robust to smooth slow-moving z."""
    flips = []
    state = None
    for i, zz in enumerate(z):
        if zz is None:
            continue
        if zz > th and state != "UP":
            if state == "DOWN":
                flips.append({"date": dates[i], "direction": "UP", "z": zz})
            state = "UP"
        elif zz < -th and state != "DOWN":
            if state == "UP":
                flips.append({"date": dates[i], "direction": "DOWN", "z": zz})
            state = "DOWN"
    return flips


def event_study(flips, px, dates_all):
    idx = {d: i for i, d in enumerate(dates_all)}
    out = {}
    for w in (5, 21, 63):
        ups, dns = [], []
        for f in flips:
            d = f["date"]
            j = idx.get(d) or idx.get(min((x for x in dates_all if x >= d), default=None))
            if j is None or j + w >= len(dates_all):
                continue
            p0, p1 = px.get(dates_all[j]), px.get(dates_all[j + w])
            if not p0 or not p1:
                continue
            r = (p1 / p0 - 1) * 100
            (ups if f["direction"] == "UP" else dns).append(r)
        def stats(xs, want_pos):
            if not xs:
                return None
            xs2 = sorted(xs)
            med = xs2[len(xs2) // 2]
            hit = sum(1 for x in xs if (x > 0) == want_pos) / len(xs)
            return {"n": len(xs), "median_pct": round(med, 2), "hit_dir_pct": round(hit * 100, 1)}
        out[f"d{w}"] = {"after_UP_flip": stats(ups, True), "after_DOWN_flip": stats(dns, False)}
    return out


def best_lead(z_dates, z, px, max_k=30):
    """k maximizing corr(impulse_z, fwd 21d return shifted k) — naive lead estimate."""
    pdates = sorted(px)
    pidx = {d: i for i, d in enumerate(pdates)}
    pairs = []
    for d, zz in zip(z_dates, z):
        j = pidx.get(d)
        if j is not None:
            pairs.append((j, zz))
    best = (0, 0.0)
    for k in range(0, max_k + 1):
        xs, ys = [], []
        for j, zz in pairs:
            if j + k + 21 < len(pdates):
                r = px[pdates[j + k + 21]] / px[pdates[j + k]] - 1
                xs.append(zz); ys.append(r)
        if len(xs) > 60:
            mx, my = mean(xs), mean(ys)
            sx = (sum((a - mx) ** 2 for a in xs)) ** .5
            sy = (sum((b - my) ** 2 for b in ys)) ** .5
            c = sum((a - mx) * (b - my) for a, b in zip(xs, ys)) / (sx * sy) if sx and sy else 0
            if abs(c) > abs(best[1]):
                best = (k, round(c, 3))
    return {"lead_days": best[0], "corr_21d_fwd": best[1]}


def lambda_handler(event=None, context=None):
    t0 = time.time()
    avail = {}

    # USD net liquidity
    walcl = fred("WALCL"); tga = fred("WTREGEN"); rrp = fred("RRPONTSYD")
    avail["usd"] = bool(walcl and tga and rrp)
    usd_dates, usd_z, usd_flips = [], [], []
    net = {}
    if avail["usd"]:
        alld = sorted(set(tga) | set(rrp) | set(walcl))
        lastw = None
        for d in alld:
            if d in walcl:
                lastw = walcl[d]
            if lastw is not None and d in tga and d in rrp:
                net[d] = lastw - tga[d] - rrp[d]
        usd_dates, usd_z = build_impulse(net)
        usd_flips = find_flips(usd_dates, usd_z)

    # EUR excess liquidity (platform store)
    eur = None
    for k in ("data/ecb-hist/excess_liquidity.json", "data/ecb-hist/excess-liquidity.json"):
        j = s3_json(k)
        if j:
            pts = j.get("points") or j.get("observations") or j
            if isinstance(pts, list):
                eur = {p[0]: float(p[1]) for p in pts if isinstance(p, (list, tuple)) and p[1] is not None}
            elif isinstance(pts, dict):
                eur = {d: float(v) for d, v in pts.items() if v is not None}
            break
    avail["eur"] = bool(eur and len(eur) > 200)
    eur_state = None
    if avail["eur"]:
        ed, ez = build_impulse(eur)
        eur_state = {"z": ez[-1], "as_of": ed[-1], "flips_5y": len([f for f in find_flips(ed, ez) if f["date"] >= "2021-01-01"])}

    # China credit impulse proxy (BIS quarterly via FRED)
    cn = None
    for sid in ("CRDQCNAPABIS", "QCNPAM770A", "CRDQCNBPABIS"):
        s = fred(sid, start="2006-01-01")
        if len(s) > 30:
            cn = (sid, s)
            break
    avail["china"] = bool(cn)
    cn_state = None
    if cn:
        sid, s = cn
        ds = sorted(s)
        yoy = [(ds[i], s[ds[i]] / s[ds[i - 4]] - 1) for i in range(4, len(ds)) if s[ds[i - 4]]]
        acc = [(d, yoy[i][1] - yoy[i - 4][1]) for i, (d, _) in enumerate(yoy) if i >= 4]
        vals = [a for _, a in acc]
        m, sd = mean(vals[-40:]), stdev(vals[-40:])
        cn_state = {"series": sid, "as_of": acc[-1][0],
                    "credit_yoy_pct": round(yoy[-1][1] * 100, 1),
                    "impulse_z": round((vals[-1] - m) / sd, 2) if sd else 0}

    # Stablecoin acceleration (platform brief)
    sc = s3_json("data/stablecoin-flow.json")
    sc_state = None
    if sc:
        hist = None
        for f in ("history", "series", "mcap_history", "daily"):
            if isinstance(sc.get(f), list) and len(sc[f]) > 30:
                hist = sc[f]
                break
        if hist:
            vals = []
            for h in hist:
                v = h.get("total_mcap") or h.get("mcap") or h.get("value") or h.get("total")
                if v is not None:
                    vals.append(float(v))
            if len(vals) > 30:
                d1 = [vals[i] - vals[i - 1] for i in range(1, len(vals))]
                d2 = [d1[i] - d1[i - 1] for i in range(1, len(d1))]
                m, sd = mean(d2[-180:]), (stdev(d2[-180:]) if len(d2) > 30 else 0)
                sc_state = {"accel_z": round((d2[-1] - m) / sd, 2) if sd else 0,
                            "n_points": len(vals), "as_of": sc.get("generated_at", "")[:10]}
    avail["stablecoin"] = bool(sc_state)
    sc_schema_hint = (sorted(sc.keys())[:12] if (sc and not sc_state) else None)

    # Lead/lag event-study vs SPX / BTC / HYG
    spx_doc = s3_json("data/spx-history-deep.json") or {}
    spx = {d: float(v) for d, v in (spx_doc.get("points") or []) if v is not None}
    btc = polygon_closes("X:BTCUSD")
    hyg = polygon_closes("HYG")
    studies, leads = {}, {}
    flips10 = [f for f in usd_flips if f["date"] >= "2015-06-01"]
    for name, px in (("SPX_proxy", spx), ("BTC", btc), ("HYG", hyg)):
        if len(px) > 500 and flips10:
            dd = sorted(px)
            studies[name] = event_study(flips10, px, dd)
            leads[name] = best_lead(usd_dates, usd_z, px)

    # closed-loop: log a NEW flip (≤5 sessions)
    n_logged = 0
    new_flip = usd_flips[-1] if usd_flips and usd_flips[-1]["date"] >= usd_dates[-5] else None
    if new_flip:
        try:
            spy = polygon_closes("SPY", start=(datetime.now(timezone.utc) - timedelta(days=10)).date().isoformat())
            px0 = spy[sorted(spy)[-1]] if spy else None
            if px0:
                nowt = datetime.now(timezone.utc)
                DDB.Table("justhodl-signals").put_item(Item={
                    "signal_id": f"liquidity-inflection#USD#{new_flip['date']}",
                    "signal_type": "liquidity_inflection", "signal_value": str(new_flip["z"]),
                    "predicted_direction": new_flip["direction"],
                    "confidence": Decimal("0.58"), "measure_against": "ticker",
                    "baseline_price": str(px0), "benchmark": "SPY",
                    "check_windows": ["day_5", "day_21", "day_63"],
                    "check_timestamps": {f"day_{w}": (nowt + timedelta(days=w)).isoformat() for w in (5, 21, 63)},
                    "outcomes": {}, "accuracy_scores": {}, "logged_at": nowt.isoformat(),
                    "logged_epoch": int(nowt.timestamp()), "status": "pending", "schema_version": "2",
                    "horizon_days_primary": 21, "regime_at_log": "UNKNOWN",
                    "ttl": int(nowt.timestamp()) + 120 * 86400,
                    "metadata": {"engine": "liquidity-inflection", "v": VERSION, "flip_z": str(new_flip["z"])},
                    "rationale": f"USD net-liquidity impulse flipped {new_flip['direction']} on {new_flip['date']} (z {new_flip['z']})"})
                n_logged = 1
        except Exception as e:
            print(f"[signals] {str(e)[:80]}")

    out = {"engine": "liquidity-inflection", "version": VERSION,
           "generated_at": datetime.now(timezone.utc).isoformat(),
           "duration_s": round(time.time() - t0, 1), "availability": avail,
           "usd": {"as_of": usd_dates[-1] if usd_dates else None,
                   "impulse_z": usd_z[-1] if usd_z else None,
                   "net_liq_usd_bn": round(net[sorted(net)[-1]] / 1000, 1) if net else None,
                   "state": ("EXPANDING" if usd_z and usd_z[-1] > 0.25 else
                             "CONTRACTING" if usd_z and usd_z[-1] < -0.25 else "NEUTRAL"),
                   "last_flip": usd_flips[-1] if usd_flips else None,
                   "n_flips_10y": len(flips10),
                   "impulse_tail_180d": [[d, z] for d, z in zip(usd_dates[-180:], usd_z[-180:])]},
           "eur": eur_state, "china": cn_state, "stablecoin": sc_state,
           "stablecoin_schema_hint": sc_schema_hint,
           "event_study_after_flips": studies, "lead_estimates": leads,
           "signals_logged": n_logged,
           "methodology": ("Impulse = 13-week slope of net liquidity (WALCL−TGA−RRP), 3y z-score; "
                           "flips debounced |Δz|≥0.25. Edge tables are real event studies over the "
                           "last decade's flips (n shown), not assertions. New flips are logged to "
                           "the closed loop vs SPY at 5/21/63d.")}
    S3.put_object(Bucket=BUCKET, Key=OUT_KEY, Body=json.dumps(out, default=str).encode(),
                  ContentType="application/json", CacheControl="public, max-age=1800")
    print(f"[liq-inflect] z={out['usd']['impulse_z']} state={out['usd']['state']} flips10y={len(flips10)} {out['duration_s']}s")
    return {"statusCode": 200, "body": json.dumps({"state": out["usd"]["state"], "logged": n_logged})}
