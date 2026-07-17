"""
justhodl-ignition v1.0 — Pre-Pump Ignition Composite
=====================================================
Detect ACCUMULATION BEFORE PRICE across 8 independent pillars, each probe-tolerant
(institutional pattern: degrade gracefully, report availability, never fake coverage):

  P1 OBV divergence      — OBV 30d slope up while price slope flat/down (Polygon)
  P2 Insider cluster     — recent insider-buy clustering (platform brief → FMP fallback)
  P3 FTD spike           — SEC fails-to-deliver, latest half-month vs prior (sec.gov ZIP)
  P4 Dark-pool share     — FINRA ATS weekly share of volume rising vs flat price
  P5 13F cluster inits   — institutional new-position clustering (FMP)
  P6 Filing cadence      — EDGAR submissions 30d count vs trailing baseline
  P7 Revision breadth    — analyst up-vs-down actions, 30d (FMP grades)
  P8 Shelf overhang      — live S-3 shelf = supply risk (negative pillar)

Composite = coverage-renormalized weighted z-blend → 0-100. Top calls logged to the
closed loop (schema v2, Decimal confidence) and graded vs SPY at 5/21/63d.
"""
import json, os, time, io, zipfile, urllib.request, urllib.parse
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from statistics import mean, stdev
from decimal import Decimal
import boto3

S3 = boto3.client("s3", region_name="us-east-1")
DDB = boto3.resource("dynamodb", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/ignition.json"
FMP_KEY = os.environ.get("FMP_KEY", "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb")
POLY_KEY = os.environ.get("POLYGON_KEY", "zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d")
UA = {"User-Agent": "JustHodl Research admin@justhodl.ai"}
VERSION = "1.1.0"

DEFAULT_UNIVERSE = ["NVDA","AMD","AVGO","TSM","MU","SMCI","VRT","ETN","PWR","ANET","CLS","FLEX","JBL",
                    "COHR","LITE","MRVL","ARM","ASML","AMAT","LRCX","KLAC","TER","ONTO","CAMT","ACLS",
                    "GEV","HUBB","MOD","AAON","IESC","STRL","PH","EMR","ROK","NDSN","GGG","CW","HEI",
                    "TDG","AXON","KTOS","LDOS","BWXT","VST","CEG","NRG","DELL","HPE"]

WEIGHTS = {"obv_div": 0.20, "ftd": 0.20, "insider": 0.15, "f13_init": 0.15,
           "dark_share": 0.15, "rev_breadth": 0.15, "cadence": 0.10, "shelf": -0.05}


def http_json(url, headers=None, timeout=25):
    try:
        return json.loads(urllib.request.urlopen(
            urllib.request.Request(url, headers=headers or UA), timeout=timeout).read())
    except Exception as e:
        print(f"[http] {url.split('?')[0][-60:]}: {str(e)[:60]}")
        return None


def fmp(path, params):
    params["apikey"] = FMP_KEY
    return http_json(f"https://financialmodelingprep.com/stable/{path}?" + urllib.parse.urlencode(params))


_FILED_Q = None


def filed_q():
    """Most recent FULLY-FILED 13F quarter (3360 doctrine): quarter-end >=60d
    past AND AAPL investorsHolding>3000. Without explicit year/quarter FMP
    defaults to the latest MID-FILING quarter whose *Change fields are garbage
    (Q2-2026 on Jul 16: AAPL 946 holders, fake -$2.3T change)."""
    global _FILED_Q
    if _FILED_Q:
        return _FILED_Q
    from datetime import date as _date
    now = datetime.now(timezone.utc)
    qends = []
    for cy in (now.year, now.year - 1):
        for cq in (4, 3, 2, 1):
            m = cq * 3
            qends.append((cy, cq, _date(cy, m, 30 if m in (6, 9) else 31)))
    eligible = sorted([t for t in qends if (now.date() - t[2]).days >= 60],
                      key=lambda t: -t[2].toordinal())
    for cy, cq, _qd in eligible[:4]:
        try:
            js = fmp("institutional-ownership/symbol-positions-summary",
                     {"symbol": "AAPL", "year": cy, "quarter": cq})
            if isinstance(js, list) and js and (js[0].get("investorsHolding") or 0) > 3000:
                _FILED_Q = (cy, cq)
                print(f"[filed_q] Q{cq} {cy} fully filed ({js[0].get('investorsHolding')} AAPL holders)")
                return _FILED_Q
        except Exception:
            continue
    _FILED_Q = (now.year - 1, 4)
    return _FILED_Q


def load_universe():
    for key in ("data/master-ranker.json", "data/universe.json"):
        try:
            j = json.loads(S3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
            for f in ("tickers", "universe", "symbols"):
                if isinstance(j.get(f), list) and len(j[f]) >= 20:
                    return [str(t).upper() for t in j[f][:60]], key
        except Exception:
            continue
    return DEFAULT_UNIVERSE, "default_universe"


# ── P3: SEC FTD ─────────────────────────────────────────────────────
def load_ftd():
    """Latest two half-month FTD files → per-symbol latest sum + change ratio."""
    now = datetime.now(timezone.utc)
    cands = []
    for k in range(4):
        d = (now.replace(day=15) - timedelta(days=31 * k))
        ym = d.strftime("%Y%m")
        cands += [f"{ym}b", f"{ym}a"]
    files = []
    for tag in cands:
        if len(files) >= 2:
            break
        url = f"https://www.sec.gov/files/data/fails-deliver-data/cnsfails{tag}.zip"
        try:
            raw = urllib.request.urlopen(urllib.request.Request(url, headers=UA), timeout=40).read()
            zf = zipfile.ZipFile(io.BytesIO(raw))
            txt = zf.read(zf.namelist()[0]).decode("utf-8", "replace")
            agg = {}
            for ln in txt.split("\n")[1:]:
                c = ln.split("|")
                if len(c) >= 4 and c[2]:
                    try:
                        agg[c[2].strip()] = agg.get(c[2].strip(), 0) + int(c[3])
                    except ValueError:
                        pass
            files.append((tag, agg))
            print(f"[ftd] {tag}: {len(agg)} symbols")
        except Exception as e:
            print(f"[ftd] {tag}: {str(e)[:50]}")
    if not files:
        return None, []
    latest = files[0][1]
    prior = files[1][1] if len(files) > 1 else {}
    out = {}
    for sym, q in latest.items():
        p = prior.get(sym, 0)
        out[sym] = {"ftd_shares": q, "ftd_chg_ratio": round(q / p, 2) if p > 0 else None}
    return out, [t for t, _ in files]


# ── P4: dark-pool ATS share (from justhodl-dark-pool engine) ─────────
def load_dark():
    """Read per-name ATS (dark-pool) weekly shares from justhodl-dark-pool's dark_map
    (FINRA ATS transparency, equity-filtered). Falls back to a direct FINRA query."""
    try:
        dp = json.loads(S3.get_object(Bucket=BUCKET, Key="data/dark-pool.json")["Body"].read())
        dm = dp.get("dark_map") or {}
        if dm:
            return {k: int(v) for k, v in dm.items() if v}
    except Exception as e:
        print(f"[dark] dark-pool.json unavailable ({str(e)[:60]}); falling back to direct FINRA")
    # fallback: direct FINRA ATS_W_SMBL query (filtered correctly this time)
    url = ("https://api.finra.org/data/group/otcMarket/name/weeklySummary"
           "?limit=5000&fields=issueSymbolIdentifier,totalWeeklyShareQuantity,summaryTypeCode,summaryStartDate")
    j = http_json(url, headers={**UA, "Accept": "application/json"}, timeout=30)
    if not isinstance(j, list) or not j:
        return None
    out = {}
    fresh = (datetime.now(timezone.utc) - timedelta(days=45)).date().isoformat()
    for r in j:
        sym = r.get("issueSymbolIdentifier")
        if sym and r.get("summaryTypeCode") == "ATS_W_SMBL" and str(r.get("summaryStartDate", "")) >= fresh:
            out[sym] = out.get(sym, 0) + int(r.get("totalWeeklyShareQuantity") or 0)
    return out or None


# ── P6/P8: EDGAR cadence + shelf ───────────────────────────────────
def load_cik_map():
    j = http_json("https://www.sec.gov/files/company_tickers.json")
    if not j:
        return {}
    return {v["ticker"].upper(): str(v["cik_str"]).zfill(10) for v in j.values()}


def edgar_profile(ticker, cik):
    j = http_json(f"https://data.sec.gov/submissions/CIK{cik}.json")
    if not j:
        return None
    rec = j.get("filings", {}).get("recent", {})
    dates = rec.get("filingDate") or []
    forms = rec.get("form") or []
    now = datetime.now(timezone.utc).date()
    n30 = sum(1 for d in dates if (now - datetime.strptime(d, "%Y-%m-%d").date()).days <= 30)
    n180 = sum(1 for d in dates if 30 < (now - datetime.strptime(d, "%Y-%m-%d").date()).days <= 210)
    shelf = any(f.startswith("S-3") and (now - datetime.strptime(d, "%Y-%m-%d").date()).days <= 540
                for f, d in zip(forms, dates))
    return {"n30": n30, "base30": round(n180 / 6.0, 2), "cadence_ratio": round(n30 / (n180 / 6.0), 2) if n180 else None,
            "shelf_active": shelf}


# ── per-ticker pillars (FMP + Polygon) ─────────────────────────────
def obv_divergence(ticker):
    end = datetime.now(timezone.utc).date().isoformat()
    start = (datetime.now(timezone.utc) - timedelta(days=200)).date().isoformat()
    j = http_json(f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/day/{start}/{end}"
                  f"?adjusted=true&sort=asc&limit=250&apiKey={POLY_KEY}")
    rows = (j or {}).get("results") or []
    if len(rows) < 45:
        return None
    obv, o = [], 0.0
    closes = [r["c"] for r in rows]
    for i, r in enumerate(rows):
        if i:
            o += r["v"] if r["c"] > rows[i - 1]["c"] else (-r["v"] if r["c"] < rows[i - 1]["c"] else 0)
        obv.append(o)
    def slope(xs):
        n = len(xs); xb = (n - 1) / 2; yb = mean(xs)
        den = sum((i - xb) ** 2 for i in range(n))
        return sum((i - xb) * (y - yb) for i, y in enumerate(xs)) / den if den else 0
    w = 30
    obv_s = slope(obv[-w:]) / (abs(mean(obv[-w:])) + 1e-9)
    px_s = slope(closes[-w:]) / (mean(closes[-w:]) + 1e-9)
    return {"obv_slope_n": round(obv_s * 1e3, 3), "px_slope_n": round(px_s * 1e3, 3),
            "divergence": round(obv_s * 1e3 - px_s * 1e3, 3), "px": closes[-1],
            "adv30": round(mean(r["v"] for r in rows[-30:]), 0)}


def fmp_pillars(ticker, probes):
    out = {}
    p = fmp("profile", {"symbol": ticker}) or []
    p0 = p[0] if isinstance(p, list) and p else {}
    out["mkt_cap"] = p0.get("mktCap") or p0.get("marketCap")
    out["name"] = p0.get("companyName")
    out["sector"] = p0.get("sector")
    if probes.get("grades"):
        g = fmp("grades", {"symbol": ticker, "limit": 40}) or []
        cut = (datetime.now(timezone.utc) - timedelta(days=30)).date().isoformat()
        up = dn = 0
        for r in (g if isinstance(g, list) else []):
            if str(r.get("date", ""))[:10] >= cut:
                a = (r.get("action") or r.get("newGrade") or "").lower()
                prev = (r.get("previousGrade") or "").lower()
                if "upgrade" in a or (a and prev and a > prev):
                    up += 1
                elif "downgrade" in a:
                    dn += 1
        out["rev_up30"], out["rev_dn30"] = up, dn
        out["rev_net"] = up - dn
    if probes.get("inst"):
        ep = probes["inst"] if isinstance(probes["inst"], str) else "institutional-ownership/symbol-positions-summary"
        prm = {"symbol": ticker}
        if "positions-summary" in ep:
            yy, qq = filed_q()
            prm.update({"year": yy, "quarter": qq})
        h = fmp(ep, prm) or []
        h0 = h[0] if isinstance(h, list) and h else (h if isinstance(h, dict) else {})
        out["inst_investors_chg"] = h0.get("investorsHoldingChange") or h0.get("newPositions")
    if probes.get("insider_fmp"):
        it = fmp("insider-trading/search", {"symbol": ticker, "limit": 40}) or []
        cut = (datetime.now(timezone.utc) - timedelta(days=45)).date().isoformat()
        buys = [r for r in (it if isinstance(it, list) else [])
                if str(r.get("transactionDate", ""))[:10] >= cut and "P" in str(r.get("transactionType", ""))]
        out["insider_buys45"] = len(buys)
        out["insider_buyers45"] = len({r.get("reportingName") for r in buys})
    return out


def zify(rows, field, key=None):
    key = key or field
    vals = [r[field] for r in rows if isinstance(r.get(field), (int, float))]
    if len(vals) < 8:
        return False
    m, sd = mean(vals), (stdev(vals) if len(vals) > 1 else 0)
    if not sd:
        return False
    for r in rows:
        if isinstance(r.get(field), (int, float)):
            r[f"z_{key}"] = round(max(-3, min(3, (r[field] - m) / sd)), 2)
    return True


def log_signals(top, regime):
    try:
        tbl = DDB.Table("justhodl-signals")
        now = datetime.now(timezone.utc)
        d0 = now.strftime("%Y-%m-%d")
        n = 0
        for r in top:
            if not r.get("px"):
                continue
            item = {"signal_id": f"ignition#{r['ticker']}#{d0}", "signal_type": "ignition",
                    "signal_value": str(r["ignition_score"]), "predicted_direction": "UP",
                    "confidence": Decimal(str(min(0.72, round(0.42 + r["ignition_score"] / 260, 2)))),
                    "measure_against": "ticker", "baseline_price": str(r["px"]), "benchmark": "SPY",
                    "check_windows": ["day_5", "day_21", "day_63"],
                    "check_timestamps": {f"day_{w}": (now + timedelta(days=w)).isoformat() for w in (5, 21, 63)},
                    "outcomes": {}, "accuracy_scores": {},
                    "logged_at": now.isoformat(), "logged_epoch": int(now.timestamp()),
                    "status": "pending", "schema_version": "2", "horizon_days_primary": 21,
                    "regime_at_log": regime or "UNKNOWN", "ttl": int(now.timestamp()) + 120 * 86400,
                    "metadata": {"engine": "ignition", "v": VERSION, "score": str(r["ignition_score"]),
                                 "coverage_pct": str(r["coverage_pct"]),
                                 "pillars": ",".join(r.get("pillars_hit", []))},
                    "rationale": f"{r['ticker']} ignition {r['ignition_score']} (coverage {r['coverage_pct']}%): " +
                                 "; ".join(r.get("why", [])[:4])}
            tbl.put_item(Item=item)
            n += 1
        return n
    except Exception as e:
        print(f"[signals] {str(e)[:90]}")
        return 0


def lambda_handler(event=None, context=None):
    t0 = time.time()
    universe, usrc = load_universe()

    # availability probes (one call each)
    probes = {}
    g = fmp("grades", {"symbol": "NVDA", "limit": 2})
    probes["grades"] = isinstance(g, list) and len(g) > 0
    probes["inst"] = False
    for ep in ("institutional-ownership/symbol-positions-summary", "institutional-holders"):
        prm = {"symbol": "NVDA"}
        if "positions-summary" in ep:
            _y, _q = filed_q()
            prm.update({"year": _y, "quarter": _q})
        h = fmp(ep, prm)
        if isinstance(h, (list, dict)) and h:
            probes["inst"] = ep
            break
    insider_brief = None
    for k in ("data/insider-buys-enriched.json", "data/insider-buys.json"):
        try:
            insider_brief = json.loads(S3.get_object(Bucket=BUCKET, Key=k)["Body"].read())
            probes["insider_brief"] = k
            break
        except Exception:
            continue
    it = fmp("insider-trading/search", {"symbol": "NVDA", "limit": 2})
    probes["insider_fmp"] = isinstance(it, list) and len(it) > 0

    ftd_map, ftd_files = load_ftd()
    probes["ftd"] = bool(ftd_map)
    dark_map = load_dark()
    probes["dark"] = bool(dark_map)
    cik = load_cik_map()
    probes["edgar"] = bool(cik)

    # per-ticker collection
    rows = []
    def work(t):
        r = {"ticker": t}
        od = obv_divergence(t)
        if od:
            r.update(od)
        r.update(fmp_pillars(t, probes))
        if probes["edgar"] and t in cik:
            ep = edgar_profile(t, cik[t])
            if ep:
                r.update(ep)
        if ftd_map and t in ftd_map:
            r.update(ftd_map[t])
            if r.get("adv30"):
                r["ftd_days_adv"] = round(ftd_map[t]["ftd_shares"] / r["adv30"], 3)
        if dark_map and t in dark_map and r.get("adv30"):
            r["dark_to_adv_w"] = round(dark_map[t] / (r["adv30"] * 5), 3)
        return r
    with ThreadPoolExecutor(max_workers=5) as ex:
        for f in as_completed({ex.submit(work, t): t for t in universe}):
            try:
                rows.append(f.result())
            except Exception as e:
                print(f"[tick] {str(e)[:60]}")

    # insider from platform brief if present
    if insider_brief:
        per = {}
        def _walk(o):
            if isinstance(o, dict):
                tk = o.get("ticker") or o.get("symbol")
                if isinstance(tk, str) and 1 <= len(tk) <= 6 and tk.isupper():
                    per[tk] = per.get(tk, 0) + 1
                for v in o.values():
                    _walk(v)
            elif isinstance(o, list):
                for v in o:
                    _walk(v)
        _walk(insider_brief)
        for r in rows:
            if r["ticker"] in per:
                r["insider_buys45"] = per[r["ticker"]]

    # z-scores per pillar
    avail = {
        "obv_div": zify(rows, "divergence", "obv"),
        "ftd": zify(rows, "ftd_days_adv", "ftd") or zify(rows, "ftd_chg_ratio", "ftd"),
        "insider": zify(rows, "insider_buys45", "insider"),
        "f13_init": zify(rows, "inst_investors_chg", "f13"),
        "dark_share": zify(rows, "dark_to_adv_w", "dark"),
        "rev_breadth": zify(rows, "rev_net", "rev"),
        "cadence": zify(rows, "cadence_ratio", "cad"),
    }
    zmap = {"obv_div": "z_obv", "ftd": "z_ftd", "insider": "z_insider", "f13_init": "z_f13",
            "dark_share": "z_dark", "rev_breadth": "z_rev", "cadence": "z_cad"}

    for r in rows:
        num = den = 0.0
        hit, why = [], []
        for pillar, w in WEIGHTS.items():
            if pillar == "shelf":
                if r.get("shelf_active"):
                    num += w * 1.0; den += abs(w)
                    why.append("live S-3 shelf (supply risk)")
                continue
            if not avail.get(pillar):
                continue
            z = r.get(zmap[pillar])
            if z is None:
                continue
            num += w * z; den += abs(w)
            if z >= 1.0:
                hit.append(pillar)
                why.append({"obv_div": f"OBV/price divergence z {z}",
                            "ftd": f"FTD pressure z {z}",
                            "insider": f"insider cluster z {z} ({r.get('insider_buys45')} buys)",
                            "f13_init": f"13F initiations z {z}",
                            "dark_share": f"dark-pool share z {z}",
                            "rev_breadth": f"revision breadth z {z} (net {r.get('rev_net')})",
                            "cadence": f"filing cadence z {z} ({r.get('cadence_ratio')}x)"}[pillar])
        blend = num / den if den else 0.0
        r["coverage_pct"] = round(100 * den / sum(abs(w) for w in WEIGHTS.values()), 0)
        r["ignition_score"] = round(max(0, min(100, 50 + 15 * blend)), 1)
        r["pillars_hit"] = hit
        r["why"] = why
    rows = [r for r in rows if r.get("coverage_pct", 0) >= 35]
    rows.sort(key=lambda r: -r["ignition_score"])

    regime = None
    try:
        bs = json.loads(S3.get_object(Bucket=BUCKET, Key="data/best-setups.json")["Body"].read())
        bvr = bs.get("bond_vol_regime")
        regime = bvr.get("regime") if isinstance(bvr, dict) else bvr
    except Exception:
        pass
    n_logged = log_signals(rows[:8], regime)

    out = {"engine": "ignition", "version": VERSION,
           "generated_at": datetime.now(timezone.utc).isoformat(),
           "duration_s": round(time.time() - t0, 1),
           "universe_source": usrc, "universe_n": len(universe), "scored_n": len(rows),
           "pillar_availability": {**{k: bool(v) for k, v in avail.items()},
                                    "shelf": probes.get("edgar", False)},
           "probes": {k: (v if isinstance(v, (bool, str)) else bool(v)) for k, v in probes.items()},
           "ftd_files": ftd_files, "signals_logged": n_logged, "regime_at_log": regime,
           "top_calls": [r["ticker"] for r in rows[:8]],
           "ranks": rows[:40],
           "methodology": ("Ignition = coverage-renormalized weighted z-blend over 8 accumulation "
                           "pillars (OBV divergence 20%, FTD 20%, insider 15%, 13F inits 15%, "
                           "dark-pool share 15%, revision breadth 15%, filing cadence 10%, "
                           "live-shelf −5%). Unavailable pillars excluded with weights renormalized; "
                           "per-name coverage% shown. Top calls logged to the closed loop and graded "
                           "vs SPY at 5/21/63d — see /skill.html.")}
    S3.put_object(Bucket=BUCKET, Key=OUT_KEY, Body=json.dumps(out, default=str).encode(),
                  ContentType="application/json", CacheControl="public, max-age=1800")
    print(f"[ignition] scored={len(rows)} logged={n_logged} avail={out['pillar_availability']} {out['duration_s']}s")
    return {"statusCode": 200, "body": json.dumps({"scored": len(rows), "logged": n_logged})}
