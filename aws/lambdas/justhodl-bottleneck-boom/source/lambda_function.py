"""
justhodl-bottleneck-boom v1.0 — Supply-Bottleneck Boom Detector
================================================================
Thesis: the biggest single-name re-ratings of this cycle (AI infra) were driven by
DEMAND OUTRUNNING SUPPLY — visible quarters early in (a) industry backlog/orders data
and (b) per-company revenue acceleration with cheap revenue-to-market-cap.

Layer 1 (industry pressure, FRED/Census M3, monthly, decades deep):
  unfilled orders (backlog), new orders, shipments → backlog/shipments ratio z,
  new-orders YoY z, semis industrial-production strain.
Layer 2 (company capture, FMP /stable/ over the live universe):
  revenue growth LEVEL + ACCELERATION (q/q of YoY), revenue-to-market-cap
  (inverse P/S percentile), inventory turnover (draw = scarcity).
Boom Score = company capture (z-blend → 0-100) shaded by its industry's pressure.
Top calls are logged to the closed loop (canonical schema v2) and graded like
every other engine — this signal must EARN its place on the scorecard.
"""
import json, os, time, urllib.request, urllib.parse
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from statistics import mean, stdev
import boto3
from decimal import Decimal

S3 = boto3.client("s3", region_name="us-east-1")
DDB = boto3.resource("dynamodb", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/bottleneck-boom.json"
FRED_KEY = os.environ.get("FRED_KEY", "2f057499936072679d8843d7fce99989")
FMP_KEY = os.environ.get("FMP_KEY", "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb")
SIGNALS_TABLE = os.environ.get("SIGNALS_TABLE", "justhodl-signals")
VERSION = "1.0.0"

# FRED series per pressure group (probe-tolerant: failures are skipped + reported)
GROUPS = {
    "TOTAL_MFG": {"unfilled": "AMTMUO", "new_orders": "AMTMNO", "shipments": "AMTMVS"},
    "COMPUTERS_ELECTRONICS": {"unfilled": "A34SUO", "new_orders": "A34SNO", "shipments": "A34SVS"},
}
SEMI_IP = "IPB53122S"   # industrial production: semiconductors & related (strain proxy)

DEFAULT_UNIVERSE = [
    "NVDA","AMD","AVGO","TSM","MU","SMCI","VRT","ETN","PWR","ANET","CLS","FLEX","JBL","COHR","LITE",
    "MRVL","ARM","ASML","AMAT","LRCX","KLAC","TER","ONTO","CAMT","ACLS","GEV","HUBB","MOD","AAON",
    "IESC","STRL","PH","EMR","ROK","NDSN","GGG","CW","HEI","TDG","AXON","KTOS","LDOS","BWXT","VST",
    "CEG","NRG","DELL","HPE",
]


def fred(sid, start="2005-01-01"):
    u = ("https://api.stlouisfed.org/fred/series/observations?"
         + urllib.parse.urlencode({"series_id": sid, "api_key": FRED_KEY, "file_type": "json",
                                   "observation_start": start, "limit": 100000}))
    try:
        j = json.loads(urllib.request.urlopen(u, timeout=30).read())
        return [(o["date"], float(o["value"])) for o in j.get("observations", [])
                if o.get("value") not in (".", "", None)]
    except Exception as e:
        print(f"[fred] {sid}: {str(e)[:60]}")
        return []


def z_latest(vals, lookback=120):
    if len(vals) < 24:
        return None
    w = vals[-lookback:]
    m, sd = mean(w), (stdev(w) if len(w) > 1 else 0)
    return round((vals[-1] - m) / sd, 2) if sd else 0.0


def yoy_series(pts):
    d = dict(pts)
    keys = sorted(d)
    out = []
    for i, k in enumerate(keys):
        if i >= 12 and d[keys[i - 12]]:
            out.append(d[k] / d[keys[i - 12]] - 1.0)
    return out


def industry_pressure():
    res, used, failed = {}, [], []
    for g, cfg in GROUPS.items():
        uf, no, sh = fred(cfg["unfilled"]), fred(cfg["new_orders"]), fred(cfg["shipments"])
        for sid, pts in ((cfg["unfilled"], uf), (cfg["new_orders"], no), (cfg["shipments"], sh)):
            (used if pts else failed).append(sid)
        entry = {"as_of": uf[-1][0] if uf else (no[-1][0] if no else None)}
        if uf and sh:
            shm = dict(sh)
            ratio = [u / shm[d] for d, u in uf if d in shm and shm[d]]
            entry["backlog_to_shipments"] = round(ratio[-1], 3) if ratio else None
            entry["backlog_ratio_z"] = z_latest(ratio)
        if no:
            ny = yoy_series(no)
            entry["new_orders_yoy_pct"] = round(ny[-1] * 100, 1) if ny else None
            entry["new_orders_yoy_z"] = z_latest(ny)
        if uf:
            uy = yoy_series(uf)
            entry["backlog_yoy_pct"] = round(uy[-1] * 100, 1) if uy else None
            entry["backlog_yoy_z"] = z_latest(uy)
        zs = [v for v in (entry.get("backlog_ratio_z"), entry.get("new_orders_yoy_z"),
                          entry.get("backlog_yoy_z")) if v is not None]
        entry["pressure_0_100"] = round(min(100, max(0, 50 + 16 * (sum(zs) / len(zs)))), 1) if zs else None
        res[g] = entry
    ip = fred(SEMI_IP, start="2000-01-01")
    if ip:
        iy = yoy_series(ip)
        res["SEMIS_IP_STRAIN"] = {"ip_yoy_pct": round(iy[-1] * 100, 1) if iy else None,
                                  "ip_yoy_z": z_latest(iy), "as_of": ip[-1][0]}
        used.append(SEMI_IP)
    else:
        failed.append(SEMI_IP)
    return res, used, failed


def fmp(path, params):
    params["apikey"] = FMP_KEY
    u = f"https://financialmodelingprep.com/stable/{path}?" + urllib.parse.urlencode(params)
    try:
        return json.loads(urllib.request.urlopen(u, timeout=25).read())
    except Exception as e:
        print(f"[fmp] {path} {params.get('symbol')}: {str(e)[:50]}")
        return None


def load_universe():
    for key in ("data/master-ranker.json", "data/master-ranker-universe.json", "data/universe.json"):
        try:
            j = json.loads(S3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
            for field in ("tickers", "universe", "symbols"):
                if isinstance(j.get(field), list) and len(j[field]) >= 20:
                    return [str(t).upper() for t in j[field][:80]], key
            if isinstance(j.get("ranks"), list):
                tk = [r.get("ticker") or r.get("symbol") for r in j["ranks"]][:80]
                if len([t for t in tk if t]) >= 20:
                    return [t for t in tk if t], key
        except Exception:
            continue
    return DEFAULT_UNIVERSE, "default_universe"


def fetch_ticker(t):
    g = fmp("income-statement-growth", {"symbol": t, "period": "quarter", "limit": 6}) or []
    r = fmp("ratios-ttm", {"symbol": t}) or []
    p = fmp("profile", {"symbol": t}) or []
    g0 = g[0] if g else {}
    g1 = g[1] if len(g) > 1 else {}
    r0 = r[0] if isinstance(r, list) and r else (r if isinstance(r, dict) else {})
    p0 = p[0] if isinstance(p, list) and p else (p if isinstance(p, dict) else {})
    def pick(d, *names):
        for n in names:
            if d.get(n) is not None:
                return d[n]
        return None
    lvl = pick(g0, "growthRevenue", "revenueGrowth")
    prv = pick(g1, "growthRevenue", "revenueGrowth")
    ps = pick(r0, "priceToSalesRatioTTM", "priceToSalesTTM", "priceSalesRatioTTM")
    it = pick(r0, "inventoryTurnoverTTM", "inventoryTurnover")
    return {"ticker": t, "name": p0.get("companyName"), "sector": p0.get("sector"),
            "industry": p0.get("industry"), "mkt_cap": p0.get("mktCap") or p0.get("marketCap"),
            "rev_growth_yoy": round(lvl * 100, 1) if lvl is not None else None,
            "rev_accel_pp": round((lvl - prv) * 100, 1) if (lvl is not None and prv is not None) else None,
            "ps_ttm": round(ps, 2) if ps is not None else None,
            "rev_to_mcap_pct": round(100.0 / ps, 1) if ps else None,
            "inv_turnover": round(it, 2) if it is not None else None}


def zify(rows, field):
    vals = [r[field] for r in rows if r.get(field) is not None]
    if len(vals) < 8:
        return
    m, sd = mean(vals), (stdev(vals) if len(vals) > 1 else 0)
    for r in rows:
        if r.get(field) is not None and sd:
            r[f"z_{field}"] = round(max(-3, min(3, (r[field] - m) / sd)), 2)


def group_for(r):
    ind = (r.get("industry") or "").lower()
    sec = (r.get("sector") or "").lower()
    if "semiconductor" in ind:
        return "COMPUTERS_ELECTRONICS", 1.0
    if "technology" in sec or "communication" in ind or "hardware" in ind or "electronic" in ind:
        return "COMPUTERS_ELECTRONICS", 0.9
    if "industrial" in sec or "aerospace" in ind or "defense" in ind or "electrical" in ind or "machinery" in ind:
        return "TOTAL_MFG", 1.0
    return "TOTAL_MFG", 0.6


def log_signals(top, regime):
    try:
        tbl = DDB.Table(SIGNALS_TABLE)
        now = datetime.now(timezone.utc)
        d0 = now.strftime("%Y-%m-%d")
        n = 0
        for r in top:
            q = fmp("quote-short", {"symbol": r["ticker"]}) or []
            px = (q[0].get("price") if isinstance(q, list) and q else None)
            if not px:
                continue
            windows = [5, 21, 63]
            item = {
                "signal_id": f"bottleneck-boom#{r['ticker']}#{d0}",
                "signal_type": "bottleneck_boom",
                "signal_value": str(r["boom_score"]),
                "predicted_direction": "UP",
                "confidence": Decimal(str(min(0.75, round(0.45 + r["boom_score"] / 250, 2)))),
                "measure_against": "ticker",
                "baseline_price": str(px),
                "benchmark": "SPY",
                "check_windows": [f"day_{w}" for w in windows],
                "check_timestamps": {f"day_{w}": (now + timedelta(days=w)).isoformat() for w in windows},
                "outcomes": {}, "accuracy_scores": {},
                "logged_at": now.isoformat(), "logged_epoch": int(now.timestamp()),
                "status": "pending", "schema_version": "2",
                "horizon_days_primary": 21,
                "regime_at_log": regime or "UNKNOWN",
                "ttl": int(now.timestamp()) + 120 * 86400,
                "metadata": {"boom_score": str(r["boom_score"]), "group": r["pressure_group"],
                             "rev_accel_pp": str(r.get("rev_accel_pp")), "engine": "bottleneck-boom", "v": VERSION},
                "rationale": (f"{r['ticker']} boom {r['boom_score']}: rev {r.get('rev_growth_yoy')}% yoy "
                              f"(accel {r.get('rev_accel_pp')}pp), rev/mcap {r.get('rev_to_mcap_pct')}%, "
                              f"group {r['pressure_group']} pressure {r.get('group_pressure')}"),
            }
            tbl.put_item(Item=item)
            n += 1
        return n
    except Exception as e:
        print(f"[signals] {str(e)[:90]}")
        return 0


def lambda_handler(event=None, context=None):
    t0 = time.time()
    pressure, used, failed = industry_pressure()
    universe, src = load_universe()
    rows = []
    with ThreadPoolExecutor(max_workers=6) as ex:
        futs = {ex.submit(fetch_ticker, t): t for t in universe}
        for f in as_completed(futs):
            try:
                rows.append(f.result())
            except Exception as e:
                print(f"[ticker] {futs[f]}: {str(e)[:50]}")
    rows = [r for r in rows if r.get("rev_growth_yoy") is not None]
    for f in ("rev_growth_yoy", "rev_accel_pp", "rev_to_mcap_pct", "inv_turnover"):
        zify(rows, f)
    for r in rows:
        g, w = group_for(r)
        r["pressure_group"] = g
        gp = (pressure.get(g) or {}).get("pressure_0_100")
        if g == "COMPUTERS_ELECTRONICS":
            strain = ((pressure.get("SEMIS_IP_STRAIN") or {}).get("ip_yoy_z") or 0)
            gp = min(100, (gp or 50) + 6 * max(0, strain))
        r["group_pressure"] = round(gp, 1) if gp is not None else None
        comp = (0.35 * (r.get("z_rev_accel_pp") or 0) + 0.25 * (r.get("z_rev_growth_yoy") or 0)
                + 0.25 * (r.get("z_rev_to_mcap_pct") or 0) + 0.15 * (r.get("z_inv_turnover") or 0))
        base = 50 + 14 * comp
        shade = 0.6 + 0.4 * ((gp if gp is not None else 50) / 100) * w
        r["boom_score"] = round(max(0, min(100, base * shade)), 1)
    rows.sort(key=lambda r: -r["boom_score"])
    regime = None
    try:
        bs = json.loads(S3.get_object(Bucket=BUCKET, Key="data/best-setups.json")["Body"].read())
        bvr = bs.get("bond_vol_regime")
        regime = bvr.get("regime") if isinstance(bvr, dict) else bvr
    except Exception:
        pass
    top = rows[:8]
    n_logged = log_signals(top, regime)
    out = {
        "engine": "bottleneck-boom", "version": VERSION,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "duration_s": round(time.time() - t0, 1),
        "industry_pressure": pressure, "fred_used": used, "fred_failed": failed,
        "universe_source": src, "universe_n": len(universe), "scored_n": len(rows),
        "signals_logged": n_logged, "regime_at_log": regime,
        "top_calls": [r["ticker"] for r in top],
        "ranks": rows[:40],
        "methodology": ("Boom = company capture (z-blend: 35% rev acceleration, 25% rev growth, "
                        "25% revenue-to-market-cap [inverse P/S], 15% inventory turnover) shaded by "
                        "industry bottleneck pressure (Census M3 backlog/shipments + new-orders YoY z, "
                        "FRED; semis IP strain bonus). Top calls logged to the closed loop "
                        "(schema v2) and graded vs SPY at 5/21/63d — see /skill.html."),
    }
    S3.put_object(Bucket=BUCKET, Key=OUT_KEY, Body=json.dumps(out, default=str).encode(),
                  ContentType="application/json", CacheControl="public, max-age=1800")
    print(f"[bottleneck] scored={len(rows)} logged={n_logged} in {out['duration_s']}s")
    return {"statusCode": 200, "body": json.dumps({"scored": len(rows), "logged": n_logged})}
