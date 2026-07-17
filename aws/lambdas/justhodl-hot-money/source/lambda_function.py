"""
justhodl-hot-money  ·  v1.0  —  CROSS-BORDER HOT-MONEY TRACKER
================================================================================
"Hot money" = short-term speculative capital that moves across borders chasing
returns. This engine answers three questions, in order:

  1. WHICH COUNTRY is hot money flowing into (worldwide)?
  2. WHICH SECTORS in that country is it going into?
  3. WHICH STOCKS in those sectors?

Layer 1 — country leaderboard. ~40 single-country / regional ETFs → countries.
Per country, a composite hot_money_score from FOUR corroborating signals (each
z-scored across the universe):
    • real ETF flow   — creation/redemption Δshares×price (the genuine tell), 0.40
    • relative momentum — country ETF 20d return minus the world (ACWX),       0.30
    • volume surge     — 5d vs 60d average volume,                              0.15
    • currency strength — the local FX vs USD (inflows bid up the currency),    0.15
Weights renormalize when a signal is missing. Countries with multiple ETFs
(China = FXI/MCHI/KWEB/ASHR) aggregate to one country score.

Layer 2/3 — for the top inflow countries, FMP ETF sector-weightings give the
sector map and ETF holdings give the names, ranked by weight + recent momentum
(where US-priced). Top inflow-country ETFs log to the scorecard, graded on
forward excess-vs-ACWX — measure-before-trust, same as everything else.
"""
import os, json, time, math, urllib.request
from datetime import datetime, timezone, timedelta
from decimal import Decimal
import boto3

S3 = boto3.client("s3", "us-east-1")
BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/hot-money.json"
POLY = os.environ.get("POLYGON_KEY", "zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d")
FMP = os.environ.get("FMP_KEY", "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb")
VERSION = "1.4.0"
WORLD = "ACWX"          # world ex-US benchmark for relative momentum
N_DRILL = 6             # how many top inflow countries to drill into sectors/stocks

# ETF -> (country, region). One country can have several ETFs.
ETF_COUNTRY = {
    "EWZ": ("Brazil", "LatAm"), "EWW": ("Mexico", "LatAm"), "ARGT": ("Argentina", "LatAm"),
    "ECH": ("Chile", "LatAm"), "GXG": ("Colombia", "LatAm"), "ILF": ("LatAm", "LatAm"),
    "INDA": ("India", "Asia"), "INDY": ("India", "Asia"), "FXI": ("China", "Asia"),
    "MCHI": ("China", "Asia"), "KWEB": ("China", "Asia"), "ASHR": ("China", "Asia"),
    "EWH": ("Hong Kong", "Asia"), "EWT": ("Taiwan", "Asia"), "EWY": ("South Korea", "Asia"),
    "EWJ": ("Japan", "Asia"), "EWS": ("Singapore", "Asia"), "THD": ("Thailand", "Asia"),
    "EIDO": ("Indonesia", "Asia"), "EPHE": ("Philippines", "Asia"), "VNM": ("Vietnam", "Asia"),
    "EWM": ("Malaysia", "Asia"), "AAXJ": ("Asia ex-Japan", "Asia"),
    "EWG": ("Germany", "Europe"), "EWU": ("UK", "Europe"), "EWQ": ("France", "Europe"),
    "EWI": ("Italy", "Europe"), "EWP": ("Spain", "Europe"), "EWL": ("Switzerland", "Europe"),
    "EWN": ("Netherlands", "Europe"), "EWD": ("Sweden", "Europe"), "EPOL": ("Poland", "Europe"),
    "GREK": ("Greece", "Europe"), "TUR": ("Turkey", "Europe"),
    "EZA": ("South Africa", "MEA"), "EGPT": ("Egypt", "MEA"), "KSA": ("Saudi Arabia", "MEA"),
    "EIS": ("Israel", "MEA"),
    "EWA": ("Australia", "Oceania"), "EWC": ("Canada", "N.America"),
    "EEM": ("EM broad", "Global"), "VWO": ("EM broad", "Global"), "EFA": ("DM ex-US", "Global"),
}
# country -> (fx-regime pair, sign)  sign=+1 if a positive pair-return = local FX strengthening
COUNTRY_FX = {
    "Brazil": ("USD_BRL", -1), "Mexico": ("USD_MXN", -1), "India": ("USD_INR", -1),
    "China": ("USD_CNH", -1), "South Korea": ("USD_KRW", -1), "Japan": ("USD_JPY", -1),
    "South Africa": ("USD_ZAR", -1), "Turkey": ("USD_TRY", -1), "Canada": ("USD_CAD", -1),
    "Australia": ("AUD_USD", 1), "UK": ("GBP_USD", 1), "Switzerland": ("USD_CHF", -1),
    "Germany": ("EUR_USD", 1), "France": ("EUR_USD", 1), "Italy": ("EUR_USD", 1),
    "Spain": ("EUR_USD", 1), "Netherlands": ("EUR_USD", 1),
}


def _get(url, timeout=30):
    try:
        return json.loads(urllib.request.urlopen(url, timeout=timeout).read())
    except Exception as e:
        return {"_err": str(e)[:60]}


def aggs(tk, days=80):
    frm = (datetime.now(timezone.utc).date() - timedelta(days=days)).isoformat()
    to = datetime.now(timezone.utc).date().isoformat()
    j = _get(f"https://api.polygon.io/v2/aggs/ticker/{tk}/range/1/day/{frm}/{to}"
             f"?adjusted=true&sort=asc&limit=200&apiKey={POLY}")
    return j.get("results") or []


def fund_flow(tk, days=30):
    """Real creation/redemption flow from Polygon's ETF Global feed (the $99/mo edge)."""
    end = datetime.now(timezone.utc).date()
    start = end - timedelta(days=days + 10)
    j = _get(f"https://api.polygon.io/etf-global/v1/fund-flows?composite_ticker={tk}"
             f"&processed_date.gte={start.isoformat()}&processed_date.lte={end.isoformat()}"
             f"&order=desc&sort=processed_date&limit=40&apiKey={POLY}")
    res = j.get("results") or []
    if not res:
        return {}
    flows = [r.get("fund_flow") for r in res if r.get("fund_flow") is not None]
    latest = res[0]
    nav, sh = latest.get("nav"), latest.get("shares_outstanding")
    aum = (nav * sh) if (nav and sh) else None
    return {"flow_5d_usd": (sum(flows[:5]) if flows else None),
            "flow_21d_usd": (sum(flows[:21]) if flows else None), "aum_usd": aum}


COUNTRY_CCY = {
    "Brazil": "BRL", "Mexico": "MXN", "Argentina": "ARS", "Chile": "CLP", "Colombia": "COP",
    "India": "INR", "China": "CNH", "Hong Kong": None, "Taiwan": "TWD", "South Korea": "KRW",
    "Japan": "JPY", "Singapore": "SGD", "Thailand": "THB", "Indonesia": "IDR", "Philippines": "PHP",
    "Vietnam": "VND", "Malaysia": "MYR", "Germany": "EUR", "UK": "GBP", "France": "EUR",
    "Italy": "EUR", "Spain": "EUR", "Switzerland": "CHF", "Netherlands": "EUR", "Sweden": "SEK",
    "Poland": "PLN", "Greece": "EUR", "Turkey": "TRY", "South Africa": "ZAR", "Egypt": "EGP",
    "Saudi Arabia": None, "Israel": "ILS", "Australia": "AUD", "Canada": "CAD",
}
_FX_CACHE = {}


def fx_strength_20d(country):
    """Local-currency 20d strength vs USD (%, + = strengthening → inflow tell). Polygon FX, cached per ccy.
    Pegged currencies (HKD, SAR) return None — no meaningful signal."""
    ccy = COUNTRY_CCY.get(country)
    if not ccy:
        return None
    if ccy in _FX_CACHE:
        return _FX_CACHE[ccy]
    tkr, sign = (f"C:{ccy}USD", 1) if ccy in ("EUR", "GBP", "AUD", "NZD") else (f"C:USD{ccy}", -1)
    cl = [r.get("c") for r in aggs(tkr, days=40) if r.get("c")]
    val = round(sign * (cl[-1] / cl[-22] - 1) * 100, 2) if (len(cl) > 21 and cl[-22]) else None
    _FX_CACHE[ccy] = val
    return val


def _ret(closes, n):
    if len(closes) <= n:
        return None
    a, b = closes[-1 - n], closes[-1]
    return (b / a - 1) * 100 if a else None


def _read(key):
    try:
        return json.loads(S3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
    except Exception:
        return {}


def _pctf(v):
    try:
        return float(str(v).replace("%", "").strip() or 0)
    except Exception:
        return 0.0


def _fmp_list(paths, timeout=12):
    """Endpoint ladder: FMP has renamed /stable feeds before (2026 class) and
    the old name 400s while callers silently go empty. Try spellings in order,
    first non-empty list wins."""
    for i, u in enumerate(paths):
        j = _get(u, timeout=timeout)
        if isinstance(j, list) and j:
            if i:
                print(f"[fmp-ladder] rung {i} won: {u.split('?')[0].split('/stable/')[-1]}")
            return j
    return []


def etf_sectors(etf):
    j = _fmp_list([
        f"https://financialmodelingprep.com/stable/etf/sector-weightings?symbol={etf}&apikey={FMP}",
        f"https://financialmodelingprep.com/stable/etf-sector-weightings?symbol={etf}&apikey={FMP}",
    ])
    out = [{"sector": x.get("sector"), "weight_pct": round(_pctf(x.get("weightPercentage")), 1)}
           for x in j if isinstance(x, dict) and x.get("sector")]
    return sorted(out, key=lambda x: -x["weight_pct"])[:6]


def etf_holdings(etf):
    j = _fmp_list([
        f"https://financialmodelingprep.com/stable/etf/holdings?symbol={etf}&apikey={FMP}",
        f"https://financialmodelingprep.com/stable/etf-holdings?symbol={etf}&apikey={FMP}",
    ])
    names = []
    for h in j:
        if not isinstance(h, dict):
            continue
        tk = h.get("asset") or h.get("symbol")
        if not tk:
            continue
        names.append({"ticker": tk,
                      "name": (h.get("name") or h.get("securityName") or "")[:30],
                      "weight_pct": round(_pctf(h.get("weightPercentage") or h.get("weight")), 2)})
    return sorted(names, key=lambda x: -x["weight_pct"])[:15]


def _day_changes(us, foreign, t0, hard_stop):
    """US via one Polygon snapshot; foreign via ONE comma-joined FMP quote
    (many /stable endpoints accept lists), per-symbol fallback for misses."""
    chg = {}
    if us:
        snap = _get("https://api.polygon.io/v2/snapshot/locale/us/markets/stocks/tickers"
                    f"?tickers={','.join(us)}&apiKey={POLY}")
        if isinstance(snap, dict):
            for t in (snap.get("tickers") or []):
                chg[t["ticker"]] = t.get("todaysChangePerc")
    if foreign:
        q = _get(f"https://financialmodelingprep.com/stable/quote?symbol={','.join(foreign[:12])}&apikey={FMP}",
                 timeout=12)
        if isinstance(q, list):
            for row in q:
                if isinstance(row, dict) and row.get("symbol") and row.get("changePercentage") is not None:
                    chg[row["symbol"]] = row["changePercentage"]
        for sym in foreign[:12]:
            if sym in chg:
                continue
            if time.time() - t0 > hard_stop:
                break
            q1 = _get(f"https://financialmodelingprep.com/stable/quote?symbol={sym}&apikey={FMP}", timeout=10)
            if isinstance(q1, list) and q1 and q1[0].get("changePercentage") is not None:
                chg[sym] = q1[0]["changePercentage"]
    return chg


def _drill_one(c, etf, t0, hard_stop):
    """Sectors + holdings + day momentum for one country ETF (shared by the
    hot-inflow loop and the standing Asia/Europe focus loop — one code path)."""
    if not etf:
        return None
    sectors = etf_sectors(etf)
    names = etf_holdings(etf)
    us = [n["ticker"] for n in names if n["ticker"] and "." not in n["ticker"]
          and n["ticker"].isalpha() and len(n["ticker"]) <= 5]
    foreign = [n["ticker"] for n in names if n["ticker"] and "." in n["ticker"]]
    chg = _day_changes(us, foreign, t0, hard_stop)
    for n in names:
        if n["ticker"] in chg and chg[n["ticker"]] is not None:
            n["day_chg_pct"] = round(chg[n["ticker"]], 2)
    return {"etf": etf, "hot_money_score": c["hot_money_score"],
            "rel_mom_20d": c["rel_mom_20d"], "net_flow_5d_usd": c["net_flow_5d_usd"],
            "top_sectors": sectors, "top_holdings": names}


def _z(vals):
    xs = [v for v in vals if v is not None]
    if len(xs) < 3:
        return {i: 0.0 for i in range(len(vals))}
    m = sum(xs) / len(xs)
    sd = math.sqrt(sum((x - m) ** 2 for x in xs) / (len(xs) - 1)) or 1.0
    return {i: ((v - m) / sd if v is not None else 0.0) for i, v in enumerate(vals)}


def lambda_handler(event=None, context=None):
    t0 = time.time()
    flows = (_read("data/etf-true-flows.json").get("by_etf")) or {}
    fx = (_read("data/polygon-fx-regime.json").get("pair_data")) or {}

    # world benchmark momentum
    wclose = [r["c"] for r in aggs(WORLD)]
    world_20 = _ret(wclose, 20) or 0.0

    # per-ETF metrics
    rows = []
    for tk, (country, region) in ETF_COUNTRY.items():
        if time.time() - t0 > 480:
            break
        res = aggs(tk)
        if len(res) < 25:
            continue
        closes = [r["c"] for r in res]
        vols = [r.get("v") or 0 for r in res]
        r20 = _ret(closes, 20)
        vol5 = sum(vols[-5:]) / 5 if len(vols) >= 5 else 0
        vol60 = sum(vols[-60:]) / min(60, len(vols)) if vols else 0
        vsurge = (vol5 / vol60) if vol60 else None
        ff = fund_flow(tk)                              # real creation/redemption flow
        nf5 = ff.get("flow_5d_usd")
        nf21 = ff.get("flow_21d_usd")
        aum = ff.get("aum_usd")
        flow_norm = (nf5 / aum * 100) if (nf5 is not None and aum) else None
        if flow_norm is None:                           # fallback: FMP Δshares×price
            fl = flows.get(tk) or {}
            nf5 = fl.get("net_flow_5d_usd")
            a2 = (fl.get("aum_est_b") or 0) * 1e9
            flow_norm = (nf5 / a2 * 100) if (nf5 is not None and a2) else fl.get("shares_chg_5d_pct")
        rows.append({"etf": tk, "country": country, "region": region,
                     "price": round(closes[-1], 2), "ret_5d": _ret(closes, 5), "ret_20d": r20,
                     "rel_mom": (r20 - world_20) if r20 is not None else None,
                     "vol_surge": round(vsurge, 2) if vsurge else None,
                     "flow_5d_usd": nf5, "flow_21d_usd": nf21, "flow_norm_pct": flow_norm})

    # z-score components across ETFs
    zflow = _z([r["flow_norm_pct"] for r in rows])
    zmom = _z([r["rel_mom"] for r in rows])
    zvol = _z([r["vol_surge"] for r in rows])
    # flow ACCELERATION per ETF — 5d daily run-rate minus 21d daily run-rate (the leading edge of hot money)
    accel_raw = []
    for r in rows:
        f5, f21 = r.get("flow_5d_usd"), r.get("flow_21d_usd")
        accel_raw.append((f5 / 5.0 - f21 / 21.0) if (f5 is not None and f21 is not None) else None)
    zacc = _z(accel_raw)
    for i, r in enumerate(rows):
        # widened FX: local-currency 20d strength via Polygon FX (every country, not just the 19 majors)
        fxs = fx_strength_20d(r["country"])
        fxz = (fxs / 3.0) if fxs is not None else None
        have_flow = r["flow_norm_pct"] is not None
        have_acc = accel_raw[i] is not None
        w_flow, w_acc, w_mom, w_vol, w_fx = 0.30, 0.13, 0.27, 0.15, 0.15
        if not have_flow:
            w_mom += w_flow * 0.6; w_vol += w_flow * 0.4; w_flow = 0.0
        if not have_acc:
            w_mom += w_acc * 0.6; w_vol += w_acc * 0.4; w_acc = 0.0
        if fxz is None:
            w_mom += w_fx * 0.7; w_vol += w_fx * 0.3; w_fx = 0.0
        r["score"] = round(w_flow * zflow[i] + w_acc * zacc[i] + w_mom * zmom[i]
                           + w_vol * zvol[i] + w_fx * (fxz or 0.0), 3)
        r["fx_strength"] = fxs

    # aggregate to country
    countries = {}
    for r in rows:
        c = countries.setdefault(r["country"], {"country": r["country"], "region": r["region"],
                                                 "etfs": [], "scores": [], "rel_moms": [], "flows": [],
                                                 "flows21": [], "fxs": [], "ret20s": []})
        c["etfs"].append(r["etf"]); c["scores"].append(r["score"])
        if r["rel_mom"] is not None:
            c["rel_moms"].append(r["rel_mom"])
        if r.get("ret_20d") is not None:
            c["ret20s"].append(r["ret_20d"])
        if r["flow_5d_usd"] is not None:
            c["flows"].append(r["flow_5d_usd"])
        if r.get("flow_21d_usd") is not None:
            c["flows21"].append(r["flow_21d_usd"])
        if r.get("fx_strength") is not None:
            c["fxs"].append(r["fx_strength"])
    # carry axis — rate/carry differential is a primary driver of cross-border hot money
    _cs = _read("data/carry-surface.json") or {}
    ccy_carry = {}
    for a in (_cs.get("all_assets") or []):
        if a.get("asset_class") == "fx" and isinstance(a.get("carry_pct"), (int, float)):
            lc, sc = a.get("long_currency"), a.get("short_currency")
            if sc == "USD" and lc and lc not in ccy_carry:
                ccy_carry[lc] = a["carry_pct"]                 # long local vs USD
            elif lc == "USD" and sc and sc not in ccy_carry:
                ccy_carry[sc] = -a["carry_pct"]                # invert USD-base quote
    clist = []
    for c in countries.values():
        c["hot_money_score"] = round(sum(c["scores"]) / len(c["scores"]), 3)
        c["rel_mom_20d"] = round(sum(c["rel_moms"]) / len(c["rel_moms"]), 2) if c["rel_moms"] else None
        c["net_flow_5d_usd"] = round(sum(c["flows"])) if c["flows"] else None
        nf21 = round(sum(c["flows21"])) if c["flows21"] else None
        c["net_flow_21d_usd"] = nf21
        fxavg = round(sum(c["fxs"]) / len(c["fxs"]), 2) if c["fxs"] else None
        c["fx_strength"] = fxavg
        # carry: rate differential of holding the local currency vs USD (a hot-money pull)
        _carry = ccy_carry.get(COUNTRY_CCY.get(c["country"]))
        c["carry_pct"] = round(_carry, 2) if isinstance(_carry, (int, float)) else None
        c["carry_signal"] = (None if _carry is None else
                             "HIGH_CARRY" if _carry >= 3 else "NEGATIVE_CARRY" if _carry < 0 else "MODERATE_CARRY")
        # ── USD-return decomposition (the foreign-investor experience: equity + FX) ──
        usd_ret = round(sum(c["ret20s"]) / len(c["ret20s"]), 2) if c["ret20s"] else None
        c["usd_return_20d"] = usd_ret
        loc_eq = round(usd_ret - fxavg, 2) if (usd_ret is not None and fxavg is not None) else None
        c["local_equity_20d"] = loc_eq
        if usd_ret is not None and fxavg is not None and loc_eq is not None:
            if loc_eq > 1 and fxavg > 0.5:
                c["return_driver"] = "TWIN_ENGINE"      # equity AND currency both lifting USD returns
            elif fxavg > abs(loc_eq):
                c["return_driver"] = "FX_DRIVEN"
            elif loc_eq > abs(fxavg):
                c["return_driver"] = "EQUITY_DRIVEN"    # FX flat/against — often exporter/local money
            else:
                c["return_driver"] = "MIXED"
        else:
            c["return_driver"] = None
        # flow VELOCITY — 5d daily run-rate vs 21d daily run-rate (the acceleration tell)
        f5, mom = c["net_flow_5d_usd"], c["rel_mom_20d"]
        if f5 is not None and nf21 is not None and abs(nf21) > 0:
            accel = (f5 / 5.0) - (nf21 / 21.0)
            c["flow_velocity"] = "ACCELERATING" if accel > 0 else "DECELERATING"
        else:
            c["flow_velocity"] = None
        # CONVICTION — the reflexive twin-engine (currency + equity + flow aligned) ranks highest
        fpos = (f5 or 0) > 0
        fneg = (f5 or 0) < 0
        mpos = (mom or 0) > 0
        fxpos = (fxavg or 0) > 0
        if fpos and c["return_driver"] == "TWIN_ENGINE":
            conv = "TWIN_ENGINE"               # flow in + equity up + currency up = reflexive hot-money loop
        elif fpos and mpos and (fxpos or fxavg is None):
            conv = "CONFIRMED_INFLOW"          # money in, price up, currency firm — genuine foreign
        elif fpos and not mpos:
            conv = "EARLY_ACCUMULATION"         # money arriving before price — early or value
        elif mpos and not fpos:
            conv = "PRICE_LED"                  # price up without confirmed foreign flow — may be local
        elif fneg and not mpos:
            conv = "CONFIRMED_OUTFLOW"
        elif fneg:
            conv = "OUTFLOW"
        else:
            conv = "MIXED"
        c["conviction"] = conv
        for k in ("scores", "rel_moms", "flows", "flows21", "fxs", "ret20s"):
            c.pop(k, None)
        clist.append(c)
    clist.sort(key=lambda x: x["hot_money_score"], reverse=True)
    for rank, c in enumerate(clist, 1):
        c["rank"] = rank

    inflow = [c for c in clist if c["hot_money_score"] > 0 and c["country"] not in ("EM broad", "DM ex-US")]
    outflow = [c for c in clist if c["hot_money_score"] < 0 and c["country"] not in ("EM broad", "DM ex-US")]

    # ── Layer 2/3: drill the top inflow countries into sectors + stocks ──
    primary_etf = {}
    for tk, (country, _r) in ETF_COUNTRY.items():
        primary_etf.setdefault(country, tk)        # first listed = primary
    drill = {}
    for c in inflow[:N_DRILL]:
        if time.time() - t0 > 700:
            break
        d1 = _drill_one(c, primary_etf.get(c["country"]), t0, 740)
        if d1:
            drill[c["country"]] = d1

    # ── ops 3372 (ADDITIVE): standing FOCUS drill — Asia hubs & Europe are
    # drilled EVERY run regardless of inflow rank (Khalid: HK/TW/KR/CN +
    # other Asia hubs + Europe must always be populated).
    FOCUS = ["Hong Kong", "Taiwan", "South Korea", "China", "Japan", "Singapore", "India",
             "Germany", "UK", "France", "Switzerland", "Netherlands", "Italy", "Spain", "Sweden"]
    by_country = {c["country"]: c for c in clist}
    for name in FOCUS:
        if name in drill or name not in by_country:
            if name in drill:
                drill[name]["focus"] = True
            continue
        if time.time() - t0 > 800:
            print(f"[focus] budget stop before {name}")
            break
        d1 = _drill_one(by_country[name], primary_etf.get(name), t0, 820)
        if d1:
            d1["focus"] = True
            drill[name] = d1

    # ── EM-DEBT channel — sovereign + local-currency bond ETF flows (hot money the equity ETFs miss) ──
    em_debt = []
    for tk in ("EMB", "EMLC", "PCY", "EMHY", "VWOB"):
        if time.time() - t0 > 760:
            break
        res2 = aggs(tk)
        if len(res2) < 25:
            continue
        cl = [r["c"] for r in res2]
        ff2 = fund_flow(tk)
        em_debt.append({"etf": tk, "name": {"EMB": "USD EM sovereign", "EMLC": "local-ccy EM govt",
                        "PCY": "USD EM sovereign", "EMHY": "EM high-yield", "VWOB": "USD EM govt"}.get(tk, tk),
                        "price": round(cl[-1], 2), "ret_20d_pct": _ret(cl, 20),
                        "flow_5d_usd": ff2.get("flow_5d_usd"), "flow_21d_usd": ff2.get("flow_21d_usd")})
    net5 = sum(x["flow_5d_usd"] for x in em_debt if x.get("flow_5d_usd") is not None) if em_debt else None
    em_debt_block = {
        "thesis": "EM sovereign / local-currency bond ETF flows — the fixed-income channel of cross-border hot money (carry trades).",
        "net_flow_5d_usd": round(net5) if net5 is not None else None,
        "signal": (None if net5 is None else
                   ("INFLOW — hot money into EM fixed income (carry / risk-on)" if net5 > 0
                    else "OUTFLOW — capital leaving EM debt (risk-off / rate stress)")),
        "by_etf": sorted(em_debt, key=lambda x: -(x.get("flow_5d_usd") or 0))}

    # risk-regime overlay — hot money into EM is a risk-on behaviour; context matters
    rr = _read("data/risk-regime.json")
    regime = {"risk_regime": rr.get("risk_regime"), "score": rr.get("risk_regime_score"),
              "note": ("Hot money floods EM/high-beta in risk-on and flees in risk-off — read these "
                       "inflows through the prevailing regime.")} if rr else None

    out = {"engine": "hot-money", "version": VERSION,
           "generated_at": datetime.now(timezone.utc).isoformat(),
           "duration_s": round(time.time() - t0, 1), "world_benchmark": WORLD,
           "world_ret_20d_pct": round(world_20, 2), "n_countries": len(clist),
           "risk_regime": regime,
           "thesis": ("Cross-border hot money funnel: real ETF creation/redemption flow + relative momentum vs "
                      "the world + volume surge + currency strength rank which countries capital is rotating "
                      "INTO, then ETF holdings drill into the sectors and stocks inside the hot countries."),
           "method": "composite z-score (flow 0.40 / rel-mom 0.30 / vol 0.15 / FX 0.15), weights renormalize when a signal is missing",
           "legend": {"TWIN_ENGINE": "flow in + local equity up + currency up — the reflexive hot-money loop (highest conviction)",
                      "CONFIRMED_INFLOW": "flow + price + currency aligned (genuine foreign hot money)",
                      "return_driver": "TWIN_ENGINE / FX_DRIVEN / EQUITY_DRIVEN — what is lifting the USD return",
                      "EARLY_ACCUMULATION": "money arriving before price moves (early / value)",
                      "PRICE_LED": "price up without confirmed foreign flow (possibly local money)",
                      "flow_velocity": "ACCELERATING = 5d flow run-rate above the 21d run-rate"},
           "inflow_leaders": inflow[:15], "outflow_leaders": list(reversed(outflow))[:15],
           "all_countries": clist, "drilldowns": drill,
           "focus_list": ["Hong Kong", "Taiwan", "South Korea", "China", "Japan", "Singapore", "India",
                          "Germany", "UK", "France", "Switzerland", "Netherlands", "Italy", "Spain", "Sweden"],
           "focus_note": "focus=true drilldowns are the standing Asia-hub + Europe set, populated every run regardless of inflow rank",
           "em_debt_flows": em_debt_block}

    # closed loop — log top inflow-country ETFs, graded on forward excess-vs-ACWX
    try:
        nowt = datetime.now(timezone.utc)
        tbl = boto3.resource("dynamodb", "us-east-1").Table("justhodl-signals")
        logged = 0
        for c in inflow[:6]:
            etf = primary_etf.get(c["country"])
            pr = next((r["price"] for r in rows if r["etf"] == etf), None)
            if not pr:
                continue
            tbl.put_item(Item={
                "signal_id": f"hotmoney-UP#{etf}#{nowt.date().isoformat()}",
                "signal_type": "hot_money_inflow", "predicted_direction": "UP",
                "signal_value": str(c["hot_money_score"]), "confidence": Decimal("0.55"),
                "measure_against": "ticker_vs_acwx", "baseline_price": str(pr), "benchmark": WORLD,
                "check_windows": ["day_5", "day_21", "day_63"], "outcomes": {}, "accuracy_scores": {},
                "status": "pending", "logged_at": nowt.isoformat(), "logged_epoch": int(nowt.timestamp()),
                "horizon_days_primary": 21, "schema_version": "2",
                "ttl": int(nowt.timestamp()) + 120 * 86400,
                "metadata": {"engine": "hot-money", "v": VERSION, "country": c["country"],
                             "score": str(c["hot_money_score"])},
                "rationale": f"Hot money inflow #{c['rank']} {c['country']} ({etf}) score {c['hot_money_score']}"})
            logged += 1
        out["signals_logged"] = logged
    except Exception as e:
        print(f"[loop] {str(e)[:70]}")

    S3.put_object(Bucket=BUCKET, Key=OUT_KEY, Body=json.dumps(out, default=str).encode(),
                  ContentType="application/json", CacheControl="public, max-age=3600")
    top = ", ".join(f"{c['country']}({c['hot_money_score']})" for c in inflow[:5])
    print(f"[hot-money] countries={len(clist)} drill={len(drill)} top-inflow: {top} {out['duration_s']}s")
    return {"statusCode": 200, "body": json.dumps({"n_countries": len(clist), "drill": len(drill)})}
