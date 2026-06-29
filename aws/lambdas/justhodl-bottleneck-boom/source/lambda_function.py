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
VERSION = "2.5.1"

# FRED series per pressure group (probe-tolerant: failures are skipped + reported)
GROUPS = {
    "TOTAL_MFG": {"unfilled": "AMTMUO", "new_orders": "AMTMNO", "shipments": "AMTMVS"},
    "COMPUTERS_ELECTRONICS": {"unfilled": "A34SUO", "new_orders": "A34SNO", "shipments": "A34SVS"},
    "MACHINERY": {"unfilled": "A33SUO", "new_orders": "A33SNO", "shipments": "A33SVS"},
    "ELECTRICAL_EQUIP": {"unfilled": "A35SUO", "new_orders": "A35SNO", "shipments": "A35SVS"},
    "AEROSPACE_DEFENSE": {"unfilled": "A36SUO", "new_orders": "A36SNO", "shipments": "A36SVS"},
}
SEMI_IP = "IPB53122S"   # industrial production: semiconductors & related (strain proxy)

DEFAULT_UNIVERSE = [
    "NVDA","AMD","AVGO","TSM","MU","SMCI","VRT","ETN","PWR","ANET","CLS","FLEX","JBL","COHR","LITE",
    "MRVL","ARM","ASML","AMAT","LRCX","KLAC","TER","ONTO","CAMT","ACLS","GEV","HUBB","MOD","AAON",
    "IESC","STRL","PH","EMR","ROK","NDSN","GGG","CW","HEI","TDG","AXON","KTOS","LDOS","BWXT","VST",
    "CEG","NRG","DELL","HPE",
]

# Capacity-cycle ("born in the bust") universe — money-losing / capex-cutting cyclicals across the
# industries that actually go through Druckenmiller supply destruction. The supply scan needs these.
CYCLICAL_UNIVERSE = [
    "DVN","APA","CTRA","OXY","EQT","AR","RRC","MTDR","SLB","HAL","BKR","NOV","FTI","VLO","MPC","PSX","DK",
    "DOW","LYB","CE","EMN","WLK","OLN","CC","ASH",
    "NUE","STLD","X","CLF","AA","ATI","MP","FCX","SCCO","RGLD",
    "IP","PKG","GEF","SLVM",
    "ZIM","SBLK","GNK","INSW","FRO","MATX",
    "GM","F","LEA","BWA","GT","DAN","ADNT",
    "BLDR","MAS","EXP","VMC","MLM","DHI","LEN",
    "WDC","STX",
    "MOS","CF","NTR","IPI","FMC",
    "UAL","DAL","AAL","LUV","ALK",
    "FSLR","ENPH","SEDG","RUN","ARRY",
]


def consensus_growth(t):
    """#6 — forward consensus revenue growth from analyst estimates (next FY vs current FY)."""
    est = fmp("analyst-estimates", {"symbol": t, "period": "annual", "limit": 4}) or []
    if not isinstance(est, list) or len(est) < 2:
        return None
    def rev(e):
        return e.get("revenueAvg") or e.get("estimatedRevenueAvg")
    rows = sorted([e for e in est if rev(e)], key=lambda e: e.get("date") or "")
    if len(rows) < 2:
        return None
    cur, nxt = rev(rows[-2]), rev(rows[-1])
    return round((nxt / cur - 1) * 100, 1) if (cur and nxt) else None


def margin_trend(t):
    """Buffett pricing-power confirmation — is the (money-losing) name's operating margin
    TROUGHING and turning up? Margin inflection is the earliest financial sign that supply
    tightness is converting to pricing power, the trigger from 'watch' to 'confirmed'."""
    inc = fmp("income-statement", {"symbol": t, "period": "quarter", "limit": 8}) or []
    if not isinstance(inc, list) or len(inc) < 5:
        return None
    seq = list(reversed(inc))  # oldest -> newest
    m = []
    for q in seq:
        rev, op = q.get("revenue"), q.get("operatingIncome")
        if rev:
            m.append(round(op / rev * 100, 1) if op is not None else None)
    m = [x for x in m if x is not None]
    if len(m) < 5:
        return None
    cur, prev, trough = m[-1], m[-2], min(m[:-1])
    inflecting = bool(cur > prev and (prev - trough) < 1.5 and cur > trough)
    return {"op_margin_now": cur, "op_margin_trough": round(trough, 1),
            "op_margin_4q_ago": m[-5], "margin_inflecting": inflecting}


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


def _yoy_pts(pts):
    """Like yoy_series but keeps dates: [(date, yoy)]."""
    d = dict(pts)
    keys = sorted(d)
    out = []
    for i, k in enumerate(keys):
        if i >= 12 and d[keys[i - 12]]:
            out.append((k, d[k] / d[keys[i - 12]] - 1.0))
    return out


def _z_dated(pts, lookback=120):
    """pts: [(date, value)] → {date: z} where z is value vs its trailing lookback window."""
    out = {}
    vals = [v for _, v in pts]
    dates = [d for d, _ in pts]
    for i in range(len(vals)):
        if i < 23:
            continue
        w = vals[max(0, i - lookback + 1):i + 1]
        m = mean(w); sd = stdev(w) if len(w) > 1 else 0
        out[dates[i]] = (vals[i] - m) / sd if sd else 0.0
    return out


def _pressure_history(comp_z_dicts, months=24):
    """Reconstruct the 0-100 pressure score per month (same formula as the latest snapshot:
    50 + 16·mean(component z's)), for the trailing `months`. comp_z_dicts: list of {date: z}."""
    dicts = [d for d in comp_z_dicts if d]
    if not dicts:
        return []
    all_dates = sorted(set().union(*[set(d.keys()) for d in dicts]))
    hist = []
    for dt in all_dates:
        zs = [d[dt] for d in dicts if dt in d]
        if zs:
            p = min(100, max(0, 50 + 16 * (sum(zs) / len(zs))))
            hist.append({"d": dt[:7], "p": round(p, 1)})
    return hist[-months:]


def _classify_direction(hist):
    """Where in the bottleneck cycle are we?
       OPENING — pressure rising (early, best risk/reward)
       PEAKING — high but flattening/rolling (late, window narrowing)
       CLOSING — easing (re-rate window shutting)
       STABLE  — no clear trend."""
    if len(hist) < 7:
        return None, None
    cur, m3, m6 = hist[-1]["p"], hist[-4]["p"], hist[-7]["p"]
    slope6 = round(cur - m6, 1)
    slope3 = round(cur - m3, 1)
    if slope6 >= 3 and slope3 >= 0:
        d = "OPENING"
    elif slope6 <= -3:
        d = "CLOSING"
    elif cur >= 55:
        d = "PEAKING"
    else:
        d = "STABLE"
    return d, slope6


def _gscpi():
    """#1 — NY Fed Global Supply Chain Pressure Index: the literal bottleneck index (free xlsx).
    Baltic Dry + Harpex + BLS air-freight + PMI supplier-delivery-times across 7 economies;
    >0 = supply chains tighter than average. Monthly = macro backdrop, not a trigger."""
    import io, zipfile
    from xml.etree import ElementTree as ET
    from collections import defaultdict
    url = "https://www.newyorkfed.org/medialibrary/research/interactives/gscpi/downloads/gscpi_data.xlsx"
    ns = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}"
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0 (compatible; JustHodlBot/1.0)"})
        raw = urllib.request.urlopen(req, timeout=45).read()
        zf = zipfile.ZipFile(io.BytesIO(raw))
        sst = []
        if "xl/sharedStrings.xml" in zf.namelist():
            for si in ET.fromstring(zf.read("xl/sharedStrings.xml")).findall(f"{ns}si"):
                sst.append("".join(t.text or "" for t in si.iter(f"{ns}t")))
        best = []                                       # scan every sheet, keep the longest GSCPI-like column
        for name in sorted(n for n in zf.namelist() if n.startswith("xl/worksheets/sheet")):
            rows = []
            for row in ET.fromstring(zf.read(name)).iter(f"{ns}row"):
                cells = {}
                for c in row.findall(f"{ns}c"):
                    col = "".join(ch for ch in c.get("r", "") if ch.isalpha())
                    v = c.find(f"{ns}v")
                    if v is None or v.text is None:
                        continue
                    cells[col] = (sst[int(v.text)] if (c.get("t") == "s" and int(v.text) < len(sst))
                                  else v.text)
                if cells:
                    rows.append(cells)
            gcol = None
            for r in rows[:8]:
                for col, val in r.items():
                    if isinstance(val, str) and "GSCPI" in val.upper():
                        gcol = col
            cand = defaultdict(list)
            for r in rows:
                for col, val in r.items():
                    try:
                        f = float(val)
                        if -6 < f < 8:                  # GSCPI range; excludes date serials (>40000)
                            cand[col].append(f)
                    except (TypeError, ValueError):
                        pass
            nums = (cand.get(gcol) if (gcol and cand.get(gcol))
                    else cand[max(cand, key=lambda k: len(cand[k]))] if cand else [])
            if len(nums) > len(best):
                best = nums
        nums = best
        if len(nums) < 14:
            print(f"[gscpi] parsed only {len(nums)} obs")
            return None
        cur = nums[-1]
        w = nums[-120:]
        m, sd = mean(w), (stdev(w) if len(w) > 1 else 0)
        chg3 = round(cur - nums[-4], 2)
        return {"level": round(cur, 2), "chg_3m": chg3,
                "z_vs_history": round((cur - m) / sd, 2) if sd else None,
                "direction": "TIGHTENING" if chg3 > 0.1 else "EASING" if chg3 < -0.1 else "STABLE",
                "n_obs": len(nums)}
    except Exception as e:
        print(f"[gscpi] {type(e).__name__}: {str(e)[:120]}")
        return None


def physical_throughput():
    """#1 leading layer — physical goods movement leads the financials by quarters.
    GSCPI (macro bottleneck backdrop) + truck tonnage + rail carloads (goods-cycle throughput).
    Rising throughput into rising backlog = real tightening before COGS/margins move."""
    out = {}
    g = _gscpi()
    if g:
        out["gscpi"] = g
    for key, sid in (("truck_tonnage", "TRUCKD11"), ("rail_carloads", "RAILFRTCARLOADSD11")):
        o = fred(sid, start="2010-01-01")
        if not o:
            continue
        ny = yoy_series(o)
        out[key] = {"yoy_pct": round(ny[-1] * 100, 1) if ny else None,
                    "accel_pp": round((ny[-1] - ny[-4]) * 100, 1) if len(ny) >= 4 else None,
                    "z": z_latest(ny) if ny else None, "as_of": o[-1][0]}
    parts = []
    if g and g.get("z_vs_history") is not None:
        parts.append(g["z_vs_history"])
    for k in ("truck_tonnage", "rail_carloads"):
        if out.get(k, {}).get("z") is not None:
            parts.append(out[k]["z"] * 0.5)        # throughput half-weight vs the dedicated index
    if parts:
        sc = sum(parts) / len(parts)
        out["physical_pressure_z"] = round(sc, 2)
        out["physical_state"] = ("TIGHTENING" if sc > 0.5 else "LOOSENING" if sc < -0.5 else "BALANCED")
        out["confirms_bottleneck"] = bool(sc > 0.5)
    return out


def _winvar(series, n=36, start=0):
    w = series[-(n + start):len(series) - start] if start else series[-n:]
    return stdev(w) ** 2 if len(w) > 1 else None


def industry_pressure():
    res, used, failed = {}, [], []
    # bullwhip baseline (Lee/Forrester) — Var(end demand): RSAFS retail sales YoY
    _dem = fred("RSAFS", start="2000-01-01")
    _dem_yoy = yoy_series(_dem) if _dem else []
    _dem_var = _winvar(_dem_yoy) if _dem_yoy else None
    for g, cfg in GROUPS.items():
        uf, no, sh = fred(cfg["unfilled"]), fred(cfg["new_orders"]), fred(cfg["shipments"])
        for sid, pts in ((cfg["unfilled"], uf), (cfg["new_orders"], no), (cfg["shipments"], sh)):
            (used if pts else failed).append(sid)
        entry = {"as_of": uf[-1][0] if uf else (no[-1][0] if no else None)}
        ratio_pts = []
        if uf and sh:
            shm = dict(sh)
            ratio_pts = [(d, u / shm[d]) for d, u in uf if d in shm and shm[d]]
            ratio = [v for _, v in ratio_pts]
            entry["backlog_to_shipments"] = round(ratio[-1], 3) if ratio else None
            entry["backlog_ratio_z"] = z_latest(ratio)
        if no:
            ny = yoy_series(no)
            entry["new_orders_yoy_pct"] = round(ny[-1] * 100, 1) if ny else None
            entry["new_orders_yoy_z"] = z_latest(ny)
            # bullwhip ratio — Var(upstream orders)/Var(end demand); >1 = amplification, the
            # earliest shortage tell (order volatility blows up before prices move)
            ov, ovp = _winvar(ny), _winvar(ny, start=36)
            if ov is not None and _dem_var:
                entry["bullwhip_ratio"] = round(ov / _dem_var, 2)
                _prior = round(ovp / _dem_var, 2) if ovp is not None else None
                entry["bullwhip_prior"] = _prior
                # the LEADING signal is amplification RISING, not the absolute level (orders are
                # structurally more volatile than demand, so the ratio is always >1)
                if _prior:
                    _chg = entry["bullwhip_ratio"] / _prior - 1
                    entry["bullwhip_state"] = ("AMPLIFYING" if _chg > 0.15
                                               else "DAMPING" if _chg < -0.15 else "STABLE")
                    entry["bullwhip_chg_pct"] = round(_chg * 100, 1)
        if uf:
            uy = yoy_series(uf)
            entry["backlog_yoy_pct"] = round(uy[-1] * 100, 1) if uy else None
            entry["backlog_yoy_z"] = z_latest(uy)
        zs = [v for v in (entry.get("backlog_ratio_z"), entry.get("new_orders_yoy_z"),
                          entry.get("backlog_yoy_z")) if v is not None]
        entry["pressure_0_100"] = round(min(100, max(0, 50 + 16 * (sum(zs) / len(zs)))), 1) if zs else None
        # 24-month pressure history + cycle direction (where in the bottleneck are we?)
        comp = [_z_dated(ratio_pts), _z_dated(_yoy_pts(no)) if no else {},
                _z_dated(_yoy_pts(uf)) if uf else {}]
        entry["history"] = _pressure_history(comp)
        entry["direction"], entry["trend_6mo"] = _classify_direction(entry["history"])
        res[g] = entry
    ip = fred(SEMI_IP, start="2000-01-01")
    if ip:
        iy = yoy_series(ip)
        semi = {"ip_yoy_pct": round(iy[-1] * 100, 1) if iy else None,
                "ip_yoy_z": z_latest(iy), "as_of": ip[-1][0]}
        semi["history"] = _pressure_history([_z_dated(_yoy_pts(ip))])
        semi["direction"], semi["trend_6mo"] = _classify_direction(semi["history"])
        res["SEMIS_IP_STRAIN"] = semi
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


def _ttm(rows, field, n=4, start=0):
    vals = []
    for r in (rows[start:start + n] if rows else []):
        v = r.get(field)
        if v is not None:
            try:
                vals.append(float(v))
            except (TypeError, ValueError):
                pass
    return sum(vals) if vals else None


def fetch_ticker(t):
    g = fmp("income-statement-growth", {"symbol": t, "period": "quarter", "limit": 6}) or []
    r = fmp("ratios-ttm", {"symbol": t}) or []
    p = fmp("profile", {"symbol": t}) or []
    cf = fmp("cash-flow-statement", {"symbol": t, "period": "quarter", "limit": 8}) or []
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
    nm = pick(r0, "netProfitMarginTTM", "netProfitMarginTtm")
    # ── supply-side (cash flow, quarterly) — the destruction / Druckenmiller layer ──
    capex_ttm = _ttm(cf, "capitalExpenditure", 4, 0)
    capex_prev = _ttm(cf, "capitalExpenditure", 4, 4)
    da_ttm = _ttm(cf, "depreciationAndAmortization", 4, 0)
    fcf_ttm = _ttm(cf, "freeCashFlow", 4, 0)
    ni_ttm = _ttm(cf, "netIncome", 4, 0)
    cap_abs = abs(capex_ttm) if capex_ttm is not None else None
    capp_abs = abs(capex_prev) if capex_prev is not None else None
    capex_yoy = round((cap_abs / capp_abs - 1) * 100, 1) if (cap_abs and capp_abs) else None
    capex_to_da = round(cap_abs / abs(da_ttm), 2) if (cap_abs is not None and da_ttm) else None
    money_losing = bool((ni_ttm is not None and ni_ttm < 0) or (fcf_ttm is not None and fcf_ttm < 0))
    capex_cut = bool((capex_yoy is not None and capex_yoy < 0) or (capex_to_da is not None and capex_to_da < 1.0))
    return {"ticker": t, "name": p0.get("companyName"), "sector": p0.get("sector"),
            "industry": p0.get("industry"), "mkt_cap": p0.get("mktCap") or p0.get("marketCap"),
            "rev_growth_yoy": round(lvl * 100, 1) if lvl is not None else None,
            "rev_accel_pp": round((lvl - prv) * 100, 1) if (lvl is not None and prv is not None) else None,
            "ps_ttm": round(ps, 2) if ps is not None else None,
            "rev_to_mcap_pct": round(100.0 / ps, 1) if ps else None,
            "inv_turnover": round(it, 2) if it is not None else None,
            "net_margin_pct": round(nm * 100, 1) if nm is not None else None,
            "capex_yoy_pct": capex_yoy, "capex_to_da": capex_to_da,
            "money_losing": money_losing, "capex_cut": capex_cut,
            "capex_under_replacement": bool(capex_to_da is not None and capex_to_da < 1.0),
            "druckenmiller_setup": bool(money_losing and capex_to_da is not None and capex_to_da < 1.0)}


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
    # finer-grained Census M3 buckets within industrials — give each name its OWN industry backlog
    if "industrial" in sec or "aerospace" in ind or "defense" in ind or "electrical" in ind or "machinery" in ind:
        if "aerospace" in ind or "defense" in ind:
            return "AEROSPACE_DEFENSE", 1.0
        if "electrical" in ind:
            return "ELECTRICAL_EQUIP", 1.0
        if "machinery" in ind:
            return "MACHINERY", 1.0
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


CAP_UTIL = {   # FRED G17 capacity utilization per pressure group (probe-tolerant)
    "TOTAL_MFG": "CUMFNS",
    "COMPUTERS_ELECTRONICS": "CAPUTLG3344S",
    "MACHINERY": "CAPUTLG333S",
    "ELECTRICAL_EQUIP": "CAPUTLG335S",
    "AEROSPACE_DEFENSE": "CAPUTLG3364T9S",
}


def _slope6_pts(pts):
    return round(pts[-1][1] - pts[-7][1], 2) if len(pts) >= 7 else None


def _median(xs):
    s = sorted(xs)
    return s[len(s) // 2] if s else None


def industry_supply(rows, pressure):
    """Layer 0 — the supply / capital-cycle side. Per group: capex destruction, money-losing
    share, capacity utilization level+trend → capital-cycle phase + supply_cycle_score.
    Phases: SCARCITY_BUILDING (Druckenmiller sweet spot, supply exiting) / TIGHT (boom) /
    CAPACITY_FLOODING (Marks warning, capital piling in) / GLUT (oversupply, seeds next scarcity)."""
    by_g = {}
    for r in rows:
        by_g.setdefault(r.get("pressure_group"), []).append(r)
    groups = {}
    for g in GROUPS:
        members = by_g.get(g, [])
        n = len(members)
        cy = [r["capex_yoy_pct"] for r in members if r.get("capex_yoy_pct") is not None]
        cda = [r["capex_to_da"] for r in members if r.get("capex_to_da") is not None]
        ent = {
            "n_companies": n,
            "capex_yoy_median": round(_median(cy), 1) if cy else None,
            "capex_to_da_median": round(_median(cda), 2) if cda else None,
            "pct_money_losing": round(100 * sum(1 for r in members if r.get("money_losing")) / n) if n else None,
            "pct_capex_cut": round(100 * sum(1 for r in members if r.get("capex_cut")) / n) if n else None,
            "pct_druckenmiller": round(100 * sum(1 for r in members if r.get("druckenmiller_setup")) / n) if n else None,
        }
        ucu = fred(CAP_UTIL.get(g, ""), start="2000-01-01") if CAP_UTIL.get(g) else []
        if ucu:
            ent["cap_util"] = round(ucu[-1][1], 1)
            ent["cap_util_z"] = z_latest([v for _, v in ucu])
            ent["cap_util_6mo_chg"] = _slope6_pts(ucu)
            ent["cap_util_as_of"] = ucu[-1][0]
        dem = (pressure.get(g) or {}).get("direction")
        cm = ent.get("capex_yoy_median"); ml = ent.get("pct_money_losing") or 0
        util = ent.get("cap_util_z"); util_tr = ent.get("cap_util_6mo_chg")
        capex_falling = (cm is not None and cm < 0) or ((ent.get("pct_capex_cut") or 0) >= 50)
        capex_surging = (cm is not None and cm >= 20)
        util_high = (util is not None and util >= 0.5)
        util_low = (util is not None and util <= -0.5)
        phase, score = "NEUTRAL", 50
        if capex_falling and ml >= 30 and not util_low:
            phase, score = "SCARCITY_BUILDING", 80
        elif capex_surging and util_high:
            phase, score = "CAPACITY_FLOODING", 30
        elif util_high and dem in ("OPENING", "PEAKING"):
            phase, score = "TIGHT", 62
        elif util_low and not capex_falling:
            phase, score = "GLUT", 22
        ent["capital_cycle_phase"] = phase
        ent["supply_cycle_score"] = score
        ent["demand_direction"] = dem
        # rough months-to-tightness proxy: deeper capex cuts + rising util => sooner (#5)
        if phase == "SCARCITY_BUILDING":
            depth = abs(cm) if cm is not None else 10
            ent["est_months_to_tightness"] = max(6, min(24, round(24 - depth * 0.4 - (util_tr or 0) * 8)))
        groups[g] = ent
    return groups


COMMODITY_MAP = {   # commodity -> FRED price series + representative producers (in the scan)
    "CRUDE_OIL": {"fred": "MCOILWTICO", "tickers": ["DVN", "APA", "CTRA", "OXY", "EQT", "AR", "RRC", "MTDR"]},
    "NATGAS": {"fred": "MHHNGSP", "tickers": ["EQT", "AR", "RRC", "CTRA"]},
    "COPPER": {"fred": "PCOPPUSDM", "tickers": ["FCX", "SCCO"]},
    "ALUMINUM": {"fred": "PALUMUSDM", "tickers": ["AA"]},
    "STEEL_IRON": {"fred": "WPU101", "tickers": ["NUE", "STLD", "X", "CLF"]},
}


def commodity_cycle(rows):
    """#7 — 'the cure for low prices is low prices': flag commodities whose price is DEPRESSED
    while the producers are CUTTING supply (capex), the setup that precedes a price recovery."""
    by_t = {r["ticker"]: r for r in rows}
    out = {}
    for name, cfg in COMMODITY_MAP.items():
        pts = fred(cfg["fred"], start="2005-01-01")
        if not pts:
            continue
        vals = [v for _, v in pts]
        pz = z_latest(vals)
        yy = yoy_series(pts)
        caps = [by_t[t]["capex_yoy_pct"] for t in cfg["tickers"]
                if by_t.get(t) and by_t[t].get("capex_yoy_pct") is not None]
        cap_med = _median(caps) if caps else None
        supply_exiting = bool(cap_med is not None and cap_med < 0)
        price_depressed = bool(pz is not None and pz <= -0.4)
        # which companies / sector are behind this commodity, and who is cutting
        producers = []
        for t in cfg["tickers"]:
            rr = by_t.get(t)
            if rr:
                producers.append({"ticker": t, "sector": rr.get("sector"), "industry": rr.get("industry"),
                                  "capex_yoy_pct": rr.get("capex_yoy_pct"), "capex_to_da": rr.get("capex_to_da"),
                                  "money_losing": rr.get("money_losing")})
            else:
                producers.append({"ticker": t})
        _inds = [p["industry"] for p in producers if p.get("industry")]
        _secs = [p["sector"] for p in producers if p.get("sector")]
        out[name] = {
            "price": round(vals[-1], 2), "price_z": pz,
            "price_yoy_pct": round(yy[-1] * 100, 1) if yy else None, "as_of": pts[-1][0],
            "producer_capex_yoy_med": round(cap_med, 1) if cap_med is not None else None,
            "producer_sector": max(set(_secs), key=_secs.count) if _secs else None,
            "producer_industry": max(set(_inds), key=_inds.count) if _inds else None,
            "producers": producers,
            "n_producers_cutting": sum(1 for p in producers
                                       if p.get("capex_to_da") is not None and p["capex_to_da"] < 1),
            "supply_exiting": supply_exiting, "price_depressed": price_depressed,
            "cure_for_low_prices_setup": bool(price_depressed and supply_exiting),
        }
    return out


def log_early(calls, regime):
    """#5 — log early supply-bottleneck calls as their OWN signal family, graded on LONG
    forward windows (6/12/18mo) since capital-cycle theses take 18-24 months to ripen."""
    try:
        tbl = DDB.Table(SIGNALS_TABLE)
        now = datetime.now(timezone.utc)
        d0 = now.strftime("%Y-%m-%d")
        n = 0
        for r in calls:
            q = fmp("quote-short", {"symbol": r["ticker"]}) or []
            px = (q[0].get("price") if isinstance(q, list) and q else None)
            if not px:
                continue
            windows = [126, 252, 378]
            item = {
                "signal_id": f"supply-bottleneck-early#{r['ticker']}#{d0}",
                "signal_type": "supply_bottleneck_early",
                "signal_value": str(r["early_bottleneck_score"]),
                "predicted_direction": "UP",
                "confidence": Decimal(str(min(0.70, round(0.40 + r["early_bottleneck_score"] / 300, 2)))),
                "measure_against": "ticker", "baseline_price": str(px), "benchmark": "SPY",
                "check_windows": [f"day_{w}" for w in windows],
                "check_timestamps": {f"day_{w}": (now + timedelta(days=w)).isoformat() for w in windows},
                "outcomes": {}, "accuracy_scores": {},
                "logged_at": now.isoformat(), "logged_epoch": int(now.timestamp()),
                "status": "pending", "schema_version": "2",
                "horizon_days_primary": 252,
                "regime_at_log": regime or "UNKNOWN",
                "ttl": int(now.timestamp()) + 420 * 86400,
                "metadata": {"early_score": str(r["early_bottleneck_score"]), "group": r["pressure_group"],
                             "phase": r.get("capital_cycle_phase"), "capex_yoy": str(r.get("capex_yoy_pct")),
                             "money_losing": str(r.get("money_losing")), "engine": "bottleneck-boom", "v": VERSION},
                "rationale": (f"{r['ticker']} EARLY bottleneck {r['early_bottleneck_score']}: "
                              f"{r['pressure_group']} {r.get('capital_cycle_phase')}; "
                              f"capex {r.get('capex_yoy_pct')}% yoy, capex/D&A {r.get('capex_to_da')}, "
                              f"money_losing={r.get('money_losing')} — supply exiting, 18-24mo thesis."),
            }
            tbl.put_item(Item=item)
            n += 1
        return n
    except Exception as e:
        print(f"[early] {str(e)[:90]}")
        return 0


def lambda_handler(event=None, context=None):
    t0 = time.time()
    pressure, used, failed = industry_pressure()
    phys = physical_throughput()
    universe, src = load_universe()
    universe = list(dict.fromkeys(list(universe) + CYCLICAL_UNIVERSE))[:96]  # add cyclical pond for #2
    rows = []
    with ThreadPoolExecutor(max_workers=8) as ex:
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
    # ── Layer 0: supply / capital-cycle (#1 destruction, #2 Druckenmiller, #3 phase, #4 supply-weighted) ──
    grp_supply = industry_supply(rows, pressure)
    # Goldratt wiring (READ the existing chokepoint engine, do NOT rebuild) — an irreplaceable
    # supplier that is ALSO in a scarcity-building industry = supply exiting a name nobody can route around
    crit_map = {}
    try:
        _ck = json.loads(S3.get_object(Bucket=BUCKET, Key="data/chokepoint.json")["Body"].read())
        for _lst in ("all_chokepoints", "highest_conviction_book", "confirmed_chokepoint_book",
                     "discovered_chokepoint_book", "industry_leaders", "hidden_chokepoint_book",
                     "cheap_chokepoint_book"):
            for _r in (_ck.get(_lst) or []):
                if isinstance(_r, dict) and _r.get("ticker") and _r.get("criticality") is not None:
                    crit_map[str(_r["ticker"]).upper()] = _r["criticality"]
        for _tk, _v in (_ck.get("structural_names") or {}).items():
            if isinstance(_v, dict) and _v.get("criticality") is not None:
                crit_map.setdefault(str(_tk).upper(), _v["criticality"])
    except Exception as e:
        print(f"[chokepoint] {str(e)[:60]}")
    for r in rows:
        gs = grp_supply.get(r.get("pressure_group")) or {}
        sbase = gs.get("supply_cycle_score", 50)
        r["capital_cycle_phase"] = gs.get("capital_cycle_phase")
        r["chokepoint_criticality"] = crit_map.get(r["ticker"])
        tilt = 0
        if r.get("druckenmiller_setup"):
            tilt += 18
        elif r.get("capex_cut"):
            tilt += 8
        if r.get("money_losing"):
            tilt += 6
        if r.get("capex_yoy_pct") is not None and r["capex_yoy_pct"] > 25:
            tilt -= 10                                  # name is itself flooding capacity
        _crit = r["chokepoint_criticality"]
        if _crit is not None and _crit >= 50 and r["capital_cycle_phase"] == "SCARCITY_BUILDING":
            tilt += 12                                  # Goldratt: binding constraint + supply exiting
            r["chokepoint_in_scarcity"] = True
        r["early_bottleneck_score"] = round(max(0, min(100, sbase + tilt)), 1)
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
    # #2/#5 — early bottleneck calls: money-losing + capex-cutting in scarcity-building industries
    early = sorted([r for r in rows if r.get("capital_cycle_phase") == "SCARCITY_BUILDING"
                    and r.get("druckenmiller_setup")], key=lambda r: -r["early_bottleneck_score"])[:8]
    if not early:                                       # fallback: best early scores in scarcity/glut
        early = sorted([r for r in rows if r.get("capital_cycle_phase") in ("SCARCITY_BUILDING", "GLUT")],
                       key=lambda r: -r["early_bottleneck_score"])[:8]
    # #6 — consensus gap: max conviction when engine sees future tightness but the Street is bearish
    for r in early:
        cg = consensus_growth(r["ticker"])
        r["consensus_fwd_growth_pct"] = cg
        phase_fwd = {"SCARCITY_BUILDING": 1.0, "GLUT": 0.6, "TIGHT": 0.3}.get(r.get("capital_cycle_phase"), 0.2)
        r["consensus_gap_score"] = (round(max(0, min(100, 50 + phase_fwd * 40 - cg * 0.8)), 1)
                                    if cg is not None else None)
        mt = margin_trend(r["ticker"])
        if mt:
            r["op_margin_now"] = mt["op_margin_now"]
            r["op_margin_trough"] = mt["op_margin_trough"]
            r["margin_inflecting"] = mt["margin_inflecting"]
    n_early = log_early(early, regime)
    # sector clustering — multiple names in one industry cutting capacity = SYSTEMATIC sector bust,
    # a far stronger Druckenmiller signal than an isolated single-company story
    _ind = {}
    for r in early:
        ind = r.get("industry")
        if ind:
            _ind.setdefault(ind, []).append(r["ticker"])
    early_sector_clusters = sorted(
        [{"industry": k, "tickers": v, "n": len(v), "systematic": len(v) >= 2} for k, v in _ind.items()],
        key=lambda x: -x["n"])
    # #3 — capacity-flood warnings: current boom names whose industry is now FLOODING capacity
    flood = [r["ticker"] for r in top
             if (grp_supply.get(r.get("pressure_group")) or {}).get("capital_cycle_phase") == "CAPACITY_FLOODING"]
    phase_counts = {}
    for _g, _e in grp_supply.items():
        ph = _e.get("capital_cycle_phase")
        phase_counts[ph] = phase_counts.get(ph, 0) + 1
    # #7 — commodity cure-for-low-prices module
    commodities = commodity_cycle(rows)
    cure_setups = [k for k, v in commodities.items() if v.get("cure_for_low_prices_setup")]
    # #8 (read side) — corroborate with sibling supply/cycle engines (defensive: never raises)
    cross = {}
    def _rd(key):
        try:
            return json.loads(S3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
        except Exception:
            return None
    for label, key, fields in (
        ("global_business_cycle", "data/global-business-cycle.json", ("phase", "cycle_phase", "regime", "score")),
        ("liquidity_capacity", "data/liquidity-capacity.json", ("regime", "state", "score", "label")),
        ("supply_chain_graph", "data/supply-chain-graph.json", ("stress", "overall_stress", "score", "regime")),
    ):
        j = _rd(key)
        if isinstance(j, dict):
            cross[label] = {f: j.get(f) for f in fields if j.get(f) is not None} or {"keys": list(j.keys())[:8]}
    th = _rd("data/themes-detected.json")
    if isinstance(th, dict):
        themes = th.get("themes") or th.get("detected") or th.get("ranks")
        if isinstance(themes, list):
            cross["supply_inflection_themes"] = [(t.get("name") or t.get("theme") or t.get("ticker"))
                                                 for t in themes[:6] if isinstance(t, dict)]
    out = {
        "engine": "bottleneck-boom", "version": VERSION,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "duration_s": round(time.time() - t0, 1),
        "industry_pressure": pressure, "physical_throughput": phys, "fred_used": used, "fred_failed": failed,
        "universe_source": src, "universe_n": len(universe), "scored_n": len(rows),
        "signals_logged": n_logged, "regime_at_log": regime,
        "top_calls": [r["ticker"] for r in top],
        "industry_supply": grp_supply,
        "capital_cycle_phase_counts": phase_counts,
        "capacity_flood_warnings": flood,
        "commodity_cycle": commodities,
        "cure_for_low_prices": cure_setups,
        "cross_engine_confirm": cross,
        "early_signals_logged": n_early,
        "early_bottleneck_calls": [
            {"ticker": r["ticker"], "name": r.get("name"), "score": r["early_bottleneck_score"],
             "group": r["pressure_group"], "phase": r.get("capital_cycle_phase"),
             "sector": r.get("sector"), "industry": r.get("industry"),
             "capex_yoy_pct": r.get("capex_yoy_pct"), "capex_to_da": r.get("capex_to_da"),
             "money_losing": r.get("money_losing"), "net_margin_pct": r.get("net_margin_pct"),
             "rev_growth_yoy": r.get("rev_growth_yoy"),
             "consensus_fwd_growth_pct": r.get("consensus_fwd_growth_pct"),
             "consensus_gap_score": r.get("consensus_gap_score"),
             "chokepoint_criticality": r.get("chokepoint_criticality"),
             "chokepoint_in_scarcity": r.get("chokepoint_in_scarcity", False),
             "op_margin_now": r.get("op_margin_now"), "op_margin_trough": r.get("op_margin_trough"),
             "margin_inflecting": r.get("margin_inflecting", False)} for r in early],
        "early_sector_clusters": early_sector_clusters,
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
    try:
        boto3.client("lambda", region_name="us-east-1").invoke(
            FunctionName="justhodl-bottleneck-research", InvocationType="Event")
    except Exception as e:
        print(f"[bottleneck] research invoke failed: {str(e)[:80]}")
    return {"statusCode": 200, "body": json.dumps({"scored": len(rows), "logged": n_logged})}
