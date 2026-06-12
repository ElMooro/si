"""
justhodl-stock-valuations v1.0 — per-stock valuation cockpit + HP-Score
=======================================================================
LAYER A — S&P 500 VALUATION TABLE (the 6-aspect grid):
  P/E · P/B · P/S · EV/EBITDA · P/FCF · FCF yield · div yield · ROE · ROA ·
  gross/op margin · D/E · current ratio · revenue/EPS/FCF growth · PEG,
  sector-relative composite percentile -> CHEAP / FAIR / RICH label,
  + intrinsic-value gap joined from justhodl-gf-value (DCF/EV-EBIT/Graham blend).
LAYER B — HP-SCORE (Huge-Potential, the 10x10 rubric):
  small/mid universe (FMP company-screener ladder; honest fallback union of
  deep-value, overlap, insider, map cheap_candidates). Ten categories x 10:
  revenue growth · valuation cheapness · balance sheet · cash runway ·
  gross-margin quality · dilution · catalyst-proxy · insider · chart strength ·
  sector tailwind. Red flags hard-cap the total. Score >=75 logs hp_score to
  the graded closed loop ("worth serious research" -> measured, not vibes).
Weekly-cached fundamentals (budgeted, resume-across-runs); daily scoring.
"""
import json, os, time, gzip, urllib.request
from datetime import datetime, timezone, timedelta
from decimal import Decimal
import boto3

S3 = boto3.client("s3", region_name="us-east-1")
DDB = boto3.resource("dynamodb", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/stock-valuations.json"
STATE_KEY = "data/_value/state.json"
UP_STATE = "data/_upside/state.json.gz"
FMP_KEY = os.environ.get("FMP_KEY", "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb")
VERSION = "1.1.0"
DIAG = []
SECTOR_ALIAS = {"Financial Services": "Financials", "Consumer Cyclical":
                 "Consumer Discretionary", "Healthcare": "Health Care",
                 "Consumer Defensive": "Consumer Staples",
                 "Basic Materials": "Materials"}
DUAL_DROP = {"GOOG": "GOOGL", "FOX": "FOXA", "NWS": "NWSA"}

RATIO_LADDERS = {
  "pe": ["priceToEarningsRatioTTM", "priceEarningsRatioTTM", "peRatioTTM"],
  "pb": ["priceToBookRatioTTM", "priceBookValueRatioTTM", "ptbRatioTTM"],
  "ps": ["priceToSalesRatioTTM", "priceSalesRatioTTM"],
  "ev_ebitda": ["enterpriseValueOverEBITDATTM", "evToEBITDATTM",
                 "enterpriseValueMultipleTTM"],
  "p_fcf": ["priceToFreeCashFlowRatioTTM", "priceToFreeCashFlowsRatioTTM",
             "pfcfRatioTTM"],
  "ev_fcf": ["evToFreeCashFlowTTM", "enterpriseValueOverFreeCashFlowTTM",
             "enterpriseValueToFreeCashFlowTTM"],
  "ev_s": ["evToSalesTTM", "enterpriseValueOverRevenueTTM", "evToRevenueTTM",
            "enterpriseValueToSalesTTM"],
  "div_y": ["dividendYieldTTM", "dividendYielTTM", "dividendYieldPercentageTTM"],
  "fcf_y": ["freeCashFlowYieldTTM"],
  "roe": ["returnOnEquityTTM", "roeTTM", "returnOnCommonEquityTTM",
          "returnOnStockholdersEquityTTM"],
  "roa": ["returnOnAssetsTTM", "returnOnTangibleAssetsTTM", "roaTTM"],
  "gm": ["grossProfitMarginTTM"], "om": ["operatingProfitMarginTTM"],
  "de": ["debtToEquityRatioTTM", "debtEquityRatioTTM"],
  "cr": ["currentRatioTTM"],
}
GROWTH_LADDERS = {"rev_g": ["revenueGrowth"], "eps_g": ["epsgrowth", "epsGrowth"],
                   "fcf_g": ["freeCashFlowGrowth"]}
# sector-specific composite weights (audit fix: equal-mean overweights noisy ratios;
# P/B is the financial lens, EV/EBITDA the industrial lens, P/S+P/FCF the software lens)
SECTOR_WEIGHTS = {
  "Financials":             {"pb": .45, "pe": .35, "p_fcf": .20, "ps": 0, "ev_ebitda": 0},
  "Real Estate":            {"p_fcf": .40, "ev_ebitda": .30, "pb": .30, "ps": 0, "pe": 0},
  "Technology":             {"ps": .30, "p_fcf": .30, "ev_ebitda": .25, "pe": .15, "pb": 0},
  "Communication Services": {"ev_ebitda": .35, "p_fcf": .30, "pe": .20, "ps": .15, "pb": 0},
}
DEFAULT_WEIGHTS = {"ev_ebitda": .30, "p_fcf": .25, "pe": .25, "ps": .10, "pb": .10}
FIN_SECTORS = {"Financials", "Financial Services"}
_sampled = {"ratios": False, "growth": False, "screener": False}


def jget(url, timeout=25):
    req = urllib.request.Request(url, headers={"User-Agent": "JustHodl Research admin@justhodl.ai"})
    return json.loads(urllib.request.urlopen(req, timeout=timeout).read())


def f(x):
    try:
        v = float(x)
        return v if v == v else None
    except Exception:
        return None


def pick(row, ladder):
    for k in ladder:
        v = f(row.get(k))
        if v is not None:
            return v
    return None


def fetch_ratios(t):
    j = jget(f"https://financialmodelingprep.com/stable/ratios-ttm?symbol={t}&apikey={FMP_KEY}")
    if not (isinstance(j, list) and j):
        return None
    row = j[0]
    if not _sampled["ratios"]:
        _sampled["ratios"] = True
        DIAG.append("ratios-ttm keys sample: " + ",".join(sorted(row.keys())[:24]))
        DIAG.append("ratios-ttm return/yield keys: " + ",".join(
            k for k in sorted(row.keys()) if "eturn" in k or "ividend" in k or "ield" in k))
        DIAG.append("ratios-ttm EV keys: " + (",".join(
            k for k in sorted(row.keys()) if "nterpris" in k or k.startswith("ev")) or "NONE"))
    return {k: pick(row, lad) for k, lad in RATIO_LADDERS.items()}


KM_LADDERS = {"roe": ["returnOnEquityTTM", "roeTTM", "returnOnCommonEquityTTM"],
               "ev_s": ["evToSalesTTM", "enterpriseValueOverRevenueTTM"],
               "ev": ["enterpriseValueTTM", "enterpriseValue"],
               "roa": ["returnOnAssetsTTM", "roaTTM", "returnOnTangibleAssetsTTM"],
               "fcf_y": ["freeCashFlowYieldTTM", "fcfYieldTTM"],
               "ev_fcf": ["evToFreeCashFlowTTM", "enterpriseValueOverFreeCashFlowTTM"]}


def fetch_keymetrics(t):
    j = jget(f"https://financialmodelingprep.com/stable/key-metrics-ttm?symbol={t}&apikey={FMP_KEY}")
    if not (isinstance(j, list) and j):
        return {}
    row = j[0]
    if not _sampled.get("km"):
        _sampled["km"] = True
        DIAG.append("key-metrics-ttm return/yield keys: " + ",".join(
            k for k in sorted(row.keys()) if "eturn" in k or "ield" in k)[:200])
        DIAG.append("key-metrics-ttm EV keys: " + (",".join(
            k for k in sorted(row.keys()) if "nterpris" in k or k.startswith("ev")) or "NONE")[:180])
    return {k: pick(row, lad) for k, lad in KM_LADDERS.items()}


def fetch_growth(t):
    j = jget(f"https://financialmodelingprep.com/stable/financial-growth?symbol={t}"
              f"&period=annual&limit=1&apikey={FMP_KEY}")
    if not (isinstance(j, list) and j):
        return {}
    row = j[0]
    if not _sampled["growth"]:
        _sampled["growth"] = True
        DIAG.append("financial-growth keys sample: " + ",".join(sorted(row.keys())[:20]))
    return {k: pick(row, lad) for k, lad in GROWTH_LADDERS.items()}


def fetch_hp_raw(t):
    """4 calls: ratios, income q8, cashflow q4, balance q1."""
    r = {"ratios": fetch_ratios(t) or {}}
    try:
        q = jget(f"https://financialmodelingprep.com/stable/income-statement?symbol={t}"
                  f"&period=quarter&limit=8&apikey={FMP_KEY}")
        r["inc"] = [{"d": x.get("date"), "rev": f(x.get("revenue")),
                      "gp": f(x.get("grossProfit")), "ni": f(x.get("netIncome")),
                      "sh": f(x.get("weightedAverageShsOut") or x.get("weightedAverageShsOutDil"))}
                     for x in (q if isinstance(q, list) else [])]
    except Exception:
        r["inc"] = []
    try:
        c = jget(f"https://financialmodelingprep.com/stable/cash-flow-statement?symbol={t}"
                  f"&period=quarter&limit=4&apikey={FMP_KEY}")
        r["cf"] = [{"ocf": f(x.get("operatingCashFlow")), "capex": f(x.get("capitalExpenditure")),
                     "fcf": f(x.get("freeCashFlow"))} for x in (c if isinstance(c, list) else [])]
    except Exception:
        r["cf"] = []
    try:
        b = jget(f"https://financialmodelingprep.com/stable/balance-sheet-statement?symbol={t}"
                  f"&period=quarter&limit=1&apikey={FMP_KEY}")
        b0 = b[0] if isinstance(b, list) and b else {}
        r["bal"] = {"cash": f(b0.get("cashAndShortTermInvestments") or b0.get("cashAndCashEquivalents")),
                     "debt": f(b0.get("totalDebt"))}
    except Exception:
        r["bal"] = {}
    r["asof"] = datetime.now(timezone.utc).date().isoformat()
    return r


def hp_universe():
    """FMP company-screener ladder; fallback = union of existing desk lists."""
    try:
        j = jget("https://financialmodelingprep.com/stable/company-screener?"
                  "marketCapMoreThan=75000000&marketCapLowerThan=3000000000"
                  "&volumeMoreThan=150000&isActivelyTrading=true"
                  f"&exchange=NYSE,NASDAQ&limit=500&apikey={FMP_KEY}")
        if isinstance(j, list) and len(j) > 50:
            if not _sampled["screener"]:
                _sampled["screener"] = True
                DIAG.append("screener keys sample: " + ",".join(sorted(j[0].keys())[:18]))
            rows = [(x.get("symbol"), SECTOR_ALIAS.get(x.get("sector") or "", x.get("sector")),
                      f(x.get("marketCap"))) for x in j if x.get("symbol")]
            rows = [r for r in rows if r[0] and "." not in r[0] and (r[2] or 0) >= 75e6]
            rows.sort(key=lambda r: -(r[2] or 0))
            DIAG.append(f"hp universe: screener {len(rows)} rows")
            return rows[:200], "fmp_company_screener"
    except Exception as e:
        DIAG.append(f"screener: {str(e)[:60]}")
    names = {}
    for key, paths in (("deep-value", [("candidates", "symbol"), ("results", "symbol")]),
                        ("deep-value-overlap", [("board", "ticker"), ("rows", "ticker")]),
                        ("insider-radar", [("latest_buys", "ticker"), ("clusters", "ticker")]),
                        ("market-map", [("cheap_candidates", "t")])):
        try:
            d = json.loads(S3.get_object(Bucket=BUCKET, Key=f"data/{key}.json")["Body"].read())
            for fld, tk in paths:
                for row in (d.get(fld) or []):
                    t = row.get(tk)
                    if t:
                        names[str(t).upper()] = (None, None)
        except Exception:
            continue
    rows = [(t, s, m) for t, (s, m) in names.items()]
    DIAG.append(f"hp universe: fallback union {len(rows)} names")
    return rows[:200], "fallback_union(deep-value,overlap,insider,cheap_candidates)"


def sc_rev_growth(yoy):
    if yoy is None:
        return 3
    p = yoy * 100
    return 10 if p >= 50 else 8 if p >= 20 else 6 if p >= 10 else 4 if p >= 0 else 1 if p >= -10 else 0


def sc_cheap(ps, pfcf, fcf_neg=False):
    s = 0.0
    if ps is not None:
        s += 6 if 0 < ps < 1 else 5 if ps < 2 else 3.5 if ps < 3 else 2 if ps < 6 else 0.5 if ps < 10 else 0
    else:
        s += 2
    if pfcf is not None and pfcf > 0:
        s += 4 if pfcf < 5 else 3 if pfcf < 10 else 1.5 if pfcf < 20 else 0.5
    if ps is not None and ps > 10 and (pfcf is None or pfcf <= 0 or fcf_neg):
        s = min(s, 1.0)   # rich on sales with no cash support is never "cheap"
    return round(min(s, 10), 1)


def lambda_handler(event=None, context=None):
    t0 = time.time()
    DIAG.clear()
    try:
        st = json.loads(S3.get_object(Bucket=BUCKET, Key=STATE_KEY)["Body"].read())
    except Exception:
        st = {"sp": {}, "hp": {}, "sp_asof": "", "hp_asof": "", "hp_rows": [], "hp_src": ""}
    week_ago = (datetime.now(timezone.utc) - timedelta(days=7)).date().isoformat()
    if not st.get("v103_refetch"):
        st["sp_asof"] = ""
        st["v103_refetch"] = True
        DIAG.append("v1.0.3: forcing sp refetch (EV/Sales + EV ladders)")

    sec = {}
    try:
        j = jget(f"https://financialmodelingprep.com/stable/sp500-constituent?apikey={FMP_KEY}")
        sec = {r["symbol"]: SECTOR_ALIAS.get(r.get("sector") or "Other", r.get("sector") or "Other")
                for r in j if r.get("symbol")}
        for d_, keep in DUAL_DROP.items():
            if d_ in sec and keep in sec:
                del sec[d_]
    except Exception as e:
        DIAG.append(f"constituents: {str(e)[:50]}")

    rings, spy_r = {}, []
    try:
        up = json.loads(gzip.decompress(S3.get_object(Bucket=BUCKET, Key=UP_STATE)["Body"].read()))
        rings = up.get("rings") or {}
        spy_r = rings.get("SPY") or []
    except Exception as e:
        DIAG.append(f"rings: {str(e)[:40]}")

    sp_fresh = st.get("sp_asof", "") >= week_ago
    todo = [t for t in sec if t not in st["sp"]]
    if not sp_fresh:
        todo += [t for t in sec if t in st["sp"]]
    a0 = time.time(); got = 0
    for t in todo:
        if time.time() - a0 > 300:
            DIAG.append(f"sp budget hit ({got} done, {len(todo)-got} remain)")
            break
        r = None
        try:
            r = fetch_ratios(t)
        except Exception:
            pass
        if r:
            try:
                r.update(fetch_growth(t))
            except Exception:
                pass
            try:
                km = fetch_keymetrics(t)
                for k, v in km.items():
                    if r.get(k) is None and v is not None:
                        r[k] = v
            except Exception:
                pass
            r["asof"] = datetime.now(timezone.utc).date().isoformat()
            st["sp"][t] = r
        got += 1
        time.sleep(0.10)
    if got:
        st["sp_asof"] = datetime.now(timezone.utc).date().isoformat()
    DIAG.append(f"sp cache: {len(st['sp'])} cached, {got} refreshed")

    if st.get("hp_asof", "") < week_ago or not st.get("hp_rows"):
        rows, src = hp_universe()
        st["hp_rows"], st["hp_src"] = rows, src
        st["hp_asof"] = datetime.now(timezone.utc).date().isoformat()
        st["hp"] = {t: v for t, v in st["hp"].items() if t in {r[0] for r in rows}}
    hp_rows = st.get("hp_rows") or []
    hp_secmap = {r[0]: r[1] for r in hp_rows}
    hp_mcap = {r[0]: r[2] for r in hp_rows if len(r) > 2 and r[2]}
    todo = [r[0] for r in hp_rows if r[0] not in st["hp"]]
    b0 = time.time(); hgot = 0
    for t in todo:
        if time.time() - b0 > 160:
            DIAG.append(f"hp budget hit ({hgot} done, {len(todo)-hgot} remain)")
            break
        try:
            st["hp"][t] = fetch_hp_raw(t)
        except Exception:
            pass
        hgot += 1
        time.sleep(0.10)
    DIAG.append(f"hp cache: {len(st['hp'])}/{len(hp_rows)} names, {hgot} refreshed (src {st.get('hp_src')})")
    S3.put_object(Bucket=BUCKET, Key=STATE_KEY, Body=json.dumps(st).encode(),
                  ContentType="application/json")

    gf = {}
    try:
        g = json.loads(S3.get_object(Bucket=BUCKET, Key="data/gf-value.json")["Body"].read())
        for row in (g.get("all_tickers") or g.get("deepest_value")
                     or g.get("results") or g.get("rows") or []):
            t = row.get("ticker") or row.get("symbol")
            gap = row.get("margin_of_safety_pct")
            if gap is None:
                gap = row.get("upside_pct") or row.get("fair_gap_pct") or row.get("gap_pct")
            if t and gap is not None:
                gf[str(t).upper()] = round(f(gap) or 0, 1)
        DIAG.append(f"gf-value gaps: {len(gf)}")
    except Exception as e:
        DIAG.append(f"gf-value: {str(e)[:40]}")

    sec_rank = {}
    try:
        sg = json.loads(S3.get_object(Bucket=BUCKET, Key="data/sector-groups.json")["Body"].read())
        for gx in sg.get("groups") or []:
            sec_rank[gx.get("sector")] = gx.get("rank_1m")
    except Exception:
        pass

    idx_set, ovl_set = set(), set()
    try:
        ii = json.loads(S3.get_object(Bucket=BUCKET, Key="data/index-inclusion.json")["Body"].read())
        idx_set = {r.get("ticker") for r in (ii.get("watch_list") or []) if r.get("ticker")}
    except Exception:
        pass
    try:
        ov = json.loads(S3.get_object(Bucket=BUCKET, Key="data/deep-value-overlap.json")["Body"].read())
        ovl_set = {r.get("ticker") for r in ((ov.get("board") or ov.get("rows") or [])[:40])
                    if r.get("ticker")}
    except Exception:
        pass
    DIAG.append(f"catalyst joins: index-inclusion {len(idx_set)}, overlap {len(ovl_set)}")

    ins_buy, ins_cluster = set(), set()
    try:
        ir = json.loads(S3.get_object(Bucket=BUCKET, Key="data/insider-radar.json")["Body"].read())
        ins_buy = {b.get("ticker") for b in (ir.get("latest_buys") or []) if b.get("ticker")}
        ins_cluster = {c.get("ticker") for c in (ir.get("clusters") or []) if c.get("ticker")}
    except Exception:
        pass

    by_sec = {}
    for t, r in st["sp"].items():
        if t in sec:
            by_sec.setdefault(sec[t], []).append((t, r))

    def pctile(vals, v):
        """Winsorized sector percentile; None for invalid v; negatives handled by caller."""
        xs = sorted(x for x in vals if x is not None and x > 0)
        if v is None or len(xs) < 8:
            return None
        if v <= 0:
            return 70.0   # NM (negative earnings/FCF): worse than neutral, not max-penalty
        idx = sum(1 for x in xs if x <= v)
        return max(5.0, min(95.0, round(idx / len(xs) * 100, 0)))

    sp_table = []
    for s_, arr in by_sec.items():
        cols = {k: [r.get(k) for _, r in arr] for k in ("ps", "pe", "pb", "ev_ebitda", "p_fcf")}
        W = SECTOR_WEIGHTS.get(s_, DEFAULT_WEIGHTS)
        for t, r in arr:
            num, den = 0.0, 0.0
            for k, w in W.items():
                if w <= 0:
                    continue
                pc = pctile(cols[k], r.get(k))
                if pc is not None:
                    num += w * pc
                    den += w
            vpct = round(num / den, 0) if den else 50.0
            label = "CHEAP" if vpct <= 25 else "RICH" if vpct >= 75 else "FAIR"
            # second-stage class: low multiple is NOT undervalued until quality confirms
            roe_, fy_, rg_ = r.get("roe"), r.get("fcf_y"), r.get("rev_g")
            quality_ok = ((roe_ or 0) > 0.10 or (fy_ or 0) > 0.04) and (rg_ is None or rg_ > -0.02)
            trapish = ((rg_ or 0) < -0.05 or (fy_ is not None and fy_ < 0)
                        or (roe_ is not None and roe_ < 0))
            if label == "CHEAP":
                vclass = ("VALUE TRAP RISK" if trapish
                           else "POTENTIALLY UNDERVALUED" if quality_ok else "LOW MULTIPLE")
            elif label == "RICH":
                vclass = "HIGH MULTIPLE"
            else:
                vclass = "SECTOR MID"
            peg = None
            if r.get("pe") and r.get("eps_g") and r["eps_g"] > 0:
                peg = round(r["pe"] / (r["eps_g"] * 100), 2)
            row = {"t": t, "sector": s_}
            PCT4 = {"div_y", "fcf_y", "roe", "roa", "gm", "om"}
            for k in RATIO_LADDERS:
                row[k] = (round(r[k], 4 if k in PCT4 else 2)
                           if r.get(k) is not None else None)
            row.update({"rev_g": round(r["rev_g"] * 100, 1) if r.get("rev_g") is not None else None,
                         "eps_g": round(r["eps_g"] * 100, 1) if r.get("eps_g") is not None else None,
                         "fcf_g": round(r["fcf_g"] * 100, 1) if r.get("fcf_g") is not None else None,
                         "peg": peg, "value_pct": vpct, "label": label,
                         "vclass": vclass, "gf_gap": gf.get(t)})
            sp_table.append(row)
    sp_table.sort(key=lambda x: x["value_pct"])

    def chart_score(t):
        r = rings.get(t)
        if not r or len(r) < 130:
            return 5, "no ring"
        sma50 = sum(r[-50:]) / 50
        sma50_prev = sum(r[-60:-10]) / 50
        above = r[-1] > sma50
        rising = sma50 > sma50_prev
        hl = min(r[-63:]) > min(r[-126:-63]) if len(r) >= 126 else False
        rs = None
        if len(spy_r) > 64 and len(r) > 64:
            rs = (r[-1] / r[-64] - 1) - (spy_r[-1] / spy_r[-64] - 1)
        s = (3 if above else 0) + (3 if rising else 0) + (2 if hl else 0) + (2 if (rs or 0) > 0 else 0)
        return s, f"{'>' if above else '<'}50dma{'up' if rising else 'dn'}, HL {hl}, RS {round((rs or 0)*100,1)}"

    hp_out = []
    for t, raw in st["hp"].items():
        inc = [x for x in (raw.get("inc") or []) if x.get("rev") is not None]
        cf = raw.get("cf") or []
        bal = raw.get("bal") or {}
        rr = raw.get("ratios") or {}
        rev_ttm = sum(x["rev"] for x in inc[:4]) if len(inc) >= 4 else None
        rev_prior = sum(x["rev"] for x in inc[4:8]) if len(inc) >= 8 else None
        yoy = (rev_ttm / rev_prior - 1) if (rev_ttm and rev_prior and rev_prior > 0) else None
        gm = (sum(x["gp"] or 0 for x in inc[:4]) / rev_ttm) if rev_ttm else rr.get("gm")
        gm_prior = (sum(x["gp"] or 0 for x in inc[4:8]) / rev_prior) if rev_prior else None
        sh_now = inc[0].get("sh") if inc else None
        sh_yr = inc[4].get("sh") if len(inc) > 4 else None
        dil = (sh_now / sh_yr - 1) if (sh_now and sh_yr and sh_yr > 0) else None
        fcf4 = [x.get("fcf") for x in cf if x.get("fcf") is not None]
        fcf_ttm = sum(fcf4) if len(fcf4) >= 2 else None
        burn_q = (-fcf_ttm / max(len(fcf4), 1)) if (fcf_ttm is not None and fcf_ttm < 0) else 0
        cash, debt = bal.get("cash"), bal.get("debt")
        mc_hp = hp_mcap.get(t)
        ev_hp = (mc_hp + (debt or 0) - (cash or 0)) if mc_hp else None
        ev_s_hp = (round(ev_hp / rev_ttm, 2) if (ev_hp is not None and rev_ttm and rev_ttm > 0)
                    else rr.get("ev_s"))
        net_cash = bool(cash is not None and debt is not None and cash > debt)
        runway_q = (cash / burn_q) if (burn_q > 0 and cash) else None
        ni_now = sum(x.get("ni") or 0 for x in inc[:4]) if len(inc) >= 4 else None
        ni_prior = sum(x.get("ni") or 0 for x in inc[4:8]) if len(inc) >= 8 else None
        is_fin = hp_secmap.get(t) in FIN_SECTORS
        if is_fin:
            ev_hp, ev_s_hp, net_cash = None, None, False
        cats = {}
        cats["revenue_growth"] = sc_rev_growth(yoy)
        cats["valuation"] = sc_cheap(rr.get("ps"), rr.get("p_fcf"),
                                      fcf_neg=(fcf_ttm is not None and fcf_ttm < 0))
        if is_fin:
            cats["balance_sheet"] = 6   # cash-vs-debt n/m for financials (float/leverage model)
        elif cash is not None and debt is not None:
            cats["balance_sheet"] = 10 if cash > debt else 7 if debt < cash * 2 else \
                4 if (rr.get("de") or 9) < 1.5 else 1
        else:
            cats["balance_sheet"] = 4
        latest_fcf = cf[0].get("fcf") if cf else None
        if (fcf_ttm or 0) > 0 and (latest_fcf or 0) > 0:
            cats["runway"] = 10
        elif (latest_fcf or 0) > 0:
            cats["runway"] = 7
        elif (fcf_ttm or 0) > 0:
            cats["runway"] = 6
        elif runway_q:
            cats["runway"] = 8 if runway_q >= 8 else 5 if runway_q >= 4 else \
                3 if runway_q >= 2 else 1
        else:
            cats["runway"] = 2
        g = gm if gm is not None else rr.get("gm")
        if is_fin:
            cats["gross_margin"] = 6   # GM n/m for financials
        else:
            cats["gross_margin"] = (9 if (g or 0) >= 0.7 else 7 if (g or 0) >= 0.4 else
                                     5 if (g or 0) >= 0.2 else 2 if (g or 0) > 0 else 0)
            if g is not None and gm_prior is not None and g > gm_prior + 0.01:
                cats["gross_margin"] = min(10, cats["gross_margin"] + 1)
        cats["dilution"] = (10 if (dil or 0) <= 0 else 8 if dil < 0.03 else 5 if dil < 0.08
                             else 3 if dil < 0.15 else 0) if dil is not None else 5
        accel = (yoy is not None and rev_prior and len(inc) >= 8 and inc[0]["rev"]
                  and inc[4]["rev"] and inc[0]["rev"] / inc[4]["rev"] - 1 > yoy)
        losses_shrink = (ni_now is not None and ni_prior is not None and ni_now > ni_prior)
        cs, cdet = chart_score(t)
        cats["catalyst_proxy"] = min(10, (4 if accel else 0) + (3 if t in ins_cluster else 0)
                                      + (3 if losses_shrink else 0)
                                      + (2 if t in idx_set else 0) + (2 if t in ovl_set else 0))
        cats["insider"] = 9 if t in ins_cluster else 6 if t in ins_buy else 3
        cats["chart"] = cs
        sr = sec_rank.get(hp_secmap.get(t))
        cats["sector_tailwind"] = round(11 - sr, 0) if sr else 5
        flags, soft = [], []
        if yoy is not None and yoy < 0 and (fcf_ttm or 0) < 0:
            flags.append("revenue declining + burning cash")
        if yoy is not None and yoy < -0.20:
            flags.append(f"revenue down {yoy*100:.0f}% YoY")
        if g is not None and not is_fin and g < 0:
            flags.append("negative gross margin")
        if dil is not None and dil >= 0.25:
            flags.append(f"heavy dilution {dil*100:.0f}%/yr")
        if debt and cash is not None and not is_fin and debt > max(cash * 4, 1) and (fcf_ttm or 0) < 0:
            flags.append("debt heavy + FCF negative")
        if rr.get("cr") is not None and rr["cr"] < 1 and not is_fin:
            soft.append(f"current ratio {rr['cr']:.2f}")
        if runway_q is not None and runway_q < 4:
            soft.append(f"runway {runway_q:.1f}q")
        if is_fin:
            soft.append("financial: EV/GM/cash-vs-debt n/m, neutral-scored")
        total = round(sum(cats.values()), 1)
        if flags:
            total = min(total, 45.0)
        pillars = {"value": cats["valuation"],
                    "quality": round(cats["gross_margin"] * 0.6
                                      + (10 if (fcf_ttm or 0) > 0 else
                                         4 if fcf_ttm is None else 1) * 0.4, 1),
                    "survival": round((cats["balance_sheet"] + cats["runway"]
                                        + cats["dilution"]) / 3, 1),
                    "rerating": round((cats["catalyst_proxy"] + cats["insider"]
                                        + cats["chart"] + cats["sector_tailwind"]) / 4, 1)}
        growth_p = cats["revenue_growth"]
        if pillars["value"] >= 7 and (pillars["quality"] <= 3 or pillars["survival"] <= 3):
            hp_class = "VALUE TRAP RISK"
        elif pillars["value"] >= 7 and pillars["survival"] >= 5:
            hp_class = "DEEP VALUE"
        elif pillars["value"] >= 5 and losses_shrink and growth_p >= 4:
            hp_class = "TURNAROUND"
        elif pillars["value"] >= 3 and growth_p >= 6 and pillars["quality"] >= 6:
            hp_class = "GARP"
        elif pillars["value"] <= 3 and pillars["rerating"] >= 6:
            hp_class = "MOMENTUM (not cheap)"
        elif pillars["value"] <= 3 and pillars["quality"] >= 7:
            hp_class = "QUALITY AT PREMIUM"
        else:
            hp_class = "MIXED"
        hp_out.append({"t": t, "sector": hp_secmap.get(t), "score": total, "cats": cats,
                        "flags": flags, "soft_flags": soft, "pillars": pillars,
                        "hp_class": hp_class, "chart_detail": cdet,
                        "metrics": {"ps": rr.get("ps"), "p_fcf": rr.get("p_fcf"),
                                     "rev_yoy_pct": round(yoy * 100, 1) if yoy is not None else None,
                                     "gross_margin_pct": round((g or 0) * 100, 1) if g is not None else None,
                                     "dilution_yoy_pct": round(dil * 100, 1) if dil is not None else None,
                                     "ev": ev_hp, "ev_s": ev_s_hp, "net_cash": net_cash,
                                     "fcf_ttm": fcf_ttm, "cash": cash, "debt": debt,
                                     "runway_q": round(runway_q, 1) if runway_q else None}})
    hp_out.sort(key=lambda x: -x["score"])
    serious = [x for x in hp_out if x["score"] >= 75 and not x["flags"]]
    logged = []
    try:
        if serious:
            c0 = serious[0]
            r0 = rings.get(c0["t"]) or []
            if r0:
                nowt = datetime.now(timezone.utc)
                DDB.Table("justhodl-signals").put_item(Item={
                    "signal_id": f"hp_score#{c0['t']}#{nowt.date().isoformat()}",
                    "signal_type": "hp_score", "predicted_direction": "UP",
                    "confidence": Decimal(str(round(min(0.7, 0.5 + c0["score"] / 400), 2))),
                    "baseline_price": Decimal(str(round(r0[-1], 4))),
                    "measure_against": "ticker", "ticker": c0["t"], "benchmark": "SPY",
                    "horizon_days_primary": 63, "check_windows": [21, 63],
                    "status": "pending", "logged_epoch": int(nowt.timestamp()),
                    "ttl": int(nowt.timestamp()) + 150 * 86400,
                    "rationale": f"HP-Score {c0['score']}/100: " + json.dumps(c0["cats"]),
                }, ConditionExpression="attribute_not_exists(signal_id)")
                logged.append(c0["t"])
    except Exception as e:
        if "ConditionalCheckFailed" not in str(e):
            DIAG.append(f"hp loop: {str(e)[:50]}")

    out = {"engine": "stock-valuations", "version": VERSION,
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "duration_s": round(time.time() - t0, 1),
            "sp_table": sp_table, "sp_coverage": len(sp_table), "sp_universe": len(sec),
            "sp_asof": st.get("sp_asof"),
            "hp": hp_out[:80], "hp_coverage": len(hp_out), "hp_universe": len(hp_rows),
            "hp_src": st.get("hp_src"), "hp_logged": logged, "n_serious": len(serious),
            "diagnostics": list(DIAG),
            "methodology": ("Layer A: sector-WEIGHTED composite of winsorized (5-95) "
                             "sector percentiles — P/B-led for Financials, FFO-proxy-led for "
                             "REITs, P/S+P/FCF-led for Tech, EV/EBITDA-led default; negative "
                             "earnings/FCF map to 70th pct (NM: worse than neutral, not "
                             "max-penalty). Labels are MULTIPLE labels (LOW<=25th, HIGH>=75th); "
                             "a low multiple is only POTENTIALLY UNDERVALUED when quality "
                             "confirms (ROE>10%% or FCF yield>4%%, revenue not shrinking) and "
                             "flips to VALUE TRAP RISK on shrinking revenue / negative FCF or "
                             "ROE. gf_gap is a MODEL fair-value estimate (gf-value composite), "
                             "not ground-truth intrinsic value. Layer B HP-Score: 10x10 rubric "
                             "with financial-sector neutralization (cash-vs-debt/GM/EV n/m for "
                             "insurers+banks), tiered runway (FCF+ TTM & latest q=10, latest "
                             "only=7, else cash/burn quarters), valuation hard rule (P/S>10 "
                             "with no/negative FCF caps cheapness at 1), catalyst joins from "
                             "index-inclusion watch + deep-value-overlap board. Hard red flags "
                             "cap totals at 45; soft flags shown, not capped. Four derived "
                             "pillars (Value/Quality/Survival/Re-rating) classify each name: "
                             "DEEP VALUE, TURNAROUND, GARP, MOMENTUM (not cheap), QUALITY AT "
                             "PREMIUM, VALUE TRAP RISK, MIXED — high HP-Score does NOT mean "
                             "cheap; read the class. Score>=75 with no hard flags logs "
                             "hp_score (UP, 63d) to the graded loop. Research, not advice.")}
    clean = json.loads(json.dumps(out, default=str), parse_constant=lambda c: None)
    S3.put_object(Bucket=BUCKET, Key=OUT_KEY, Body=json.dumps(clean).encode(),
                  ContentType="application/json", CacheControl="public, max-age=1800")
    print(f"[valuations] sp={len(sp_table)} hp={len(hp_out)} serious={len(serious)} "
          f"{out['duration_s']}s")
    return {"statusCode": 200, "body": json.dumps({"sp": len(sp_table), "hp": len(hp_out)})}
