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
VERSION = "1.4.2"
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
               "sbc_rev": ["stockBasedCompensationToRevenueTTM",
                            "stockBasedCompensationToRevenue"],
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


HIST_LADDERS = {
  "ps": ["priceToSalesRatio", "priceSalesRatio"],
  "pe": ["priceToEarningsRatio", "priceEarningsRatio"],
  "pb": ["priceToBookRatio", "priceBookValueRatio", "priceToBookValueRatio"],
  "ev_ebitda": ["enterpriseValueMultiple", "evToEBITDA", "enterpriseValueOverEBITDA"],
  "p_fcf": ["priceToFreeCashFlowRatio", "priceToFreeCashFlowsRatio"],
  "gm": ["grossProfitMargin", "grossProfitMarginRatio"],
}
# semiconductor names on the S&P (cycle-inversion detector applies to these)
SEMI_SET = {"NVDA", "AMD", "AVGO", "QCOM", "TXN", "ADI", "MU", "INTC", "AMAT",
             "LRCX", "KLAC", "NXPI", "ON", "MCHP", "MPWR", "TER", "SWKS", "QRVO"}


def fetch_hist(t):
    """10 years of annual ratios for own-history percentiles."""
    j = jget(f"https://financialmodelingprep.com/stable/ratios?symbol={t}"
              f"&period=annual&limit=10&apikey={FMP_KEY}")
    if not (isinstance(j, list) and j):
        return None
    if not _sampled.get("hist"):
        _sampled["hist"] = True
        DIAG.append("ratios-annual keys sample: " + ",".join(
            k for k in sorted(j[0].keys()) if "price" in k.lower()
            or "nterpris" in k.lower())[:220])
    out = {}
    for k, lad in HIST_LADDERS.items():
        vals = []
        for row in j:
            v = pick(row, lad)
            if v is not None and v > 0:
                vals.append(round(v, 3))
        if len(vals) >= 5:
            out[k] = vals
    return out or None


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
        r["cf"] = [{"ocf": f(x.get("operatingCashFlow")),
                     "capex": f(x.get("capitalExpenditure")),
                     "fcf": f(x.get("freeCashFlow")),
                     "sbc": f(x.get("stockBasedCompensation"))}
                    for x in (c if isinstance(c, list) else [])]
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
                      f(x.get("marketCap")), x.get("industry")) for x in j if x.get("symbol")]
            FUND_IND = ("Asset Management -", "Closed-End Fund", "Shell Compan",
                         "Exchange Traded Fund")
            rows = [(x.get("symbol"), SECTOR_ALIAS.get(x.get("sector") or "", x.get("sector")),
                      f(x.get("marketCap")), x.get("industry"))
                     for x in j if x.get("symbol")
                     and not x.get("isEtf") and not x.get("isFund")
                     and not any((x.get("industry") or "").startswith(fi) for fi in FUND_IND)]
            rows = [r for r in rows if r[0] and "." not in r[0] and (r[2] or 0) >= 75e6]
            rows.sort(key=lambda r: -(r[2] or 0))
            DIAG.append(f"hp universe: screener {len(rows)} rows")
            return rows[:400], "fmp_company_screener"
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
    rows = [(t, s, m, None) for t, (s, m) in names.items()]
    DIAG.append(f"hp universe: fallback union {len(rows)} names")
    return rows[:400], "fallback_union(deep-value,overlap,insider,cheap_candidates)"


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
    if not st.get("v141_universe"):
        st["hp_asof"] = ""
        st["v141_universe"] = True
        DIAG.append("v1.4.1: universe regen excluding funds/ETFs/CEF industries")
    if not st.get("v140_universe"):
        st["hp_asof"] = ""
        st["v140_universe"] = True
        DIAG.append("v1.4.0: regenerating HP universe at 400 names w/ industry")
    if not st.get("v130_refetch"):
        st["sp_asof"] = ""
        st["hist_asof"] = ""
        st["hp"] = {}
        st["v130_refetch"] = True
        DIAG.append("v1.3.0: full refetch (SBC/rev, gross-margin history, HP SBC capture)")
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

    rings, spy_r, dv = {}, [], {}
    try:
        up = json.loads(gzip.decompress(S3.get_object(Bucket=BUCKET, Key=UP_STATE)["Body"].read()))
        rings = up.get("rings") or {}
        dv = up.get("dv") or {}
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

    # ── own-history cache (10y annual ratios; the "vs history" axis) ──
    st.setdefault("hist", {})
    hist_fresh = st.get("hist_asof", "") >= week_ago
    htodo = [t for t in sec if t not in st["hist"]]
    if not hist_fresh:
        htodo += [t for t in sec if t in st["hist"]]
    h0 = time.time(); hgot2 = 0
    for t in htodo:
        if time.time() - h0 > 150:
            DIAG.append(f"hist budget hit ({hgot2} done, {len(htodo)-hgot2} remain)")
            break
        try:
            hv = fetch_hist(t)
            if hv:
                st["hist"][t] = hv
        except Exception:
            pass
        hgot2 += 1
        time.sleep(0.10)
    if hgot2:
        st["hist_asof"] = datetime.now(timezone.utc).date().isoformat()
    DIAG.append(f"hist cache: {len(st['hist'])} names with >=5y, {hgot2} refreshed")

    if st.get("hp_asof", "") < week_ago or not st.get("hp_rows"):
        rows, src = hp_universe()
        st["hp_rows"], st["hp_src"] = rows, src
        st["hp_asof"] = datetime.now(timezone.utc).date().isoformat()
        st["hp"] = {t: v for t, v in st["hp"].items() if t in {r[0] for r in rows}}
    hp_rows = st.get("hp_rows") or []
    hp_secmap = {r[0]: r[1] for r in hp_rows}
    hp_mcap = {r[0]: r[2] for r in hp_rows if len(r) > 2 and r[2]}
    hp_ind = {r[0]: r[3] for r in hp_rows if len(r) > 3 and r[3]}
    todo = [r[0] for r in hp_rows if r[0] not in st["hp"]]
    b0 = time.time(); hgot = 0
    for t in todo:
        if time.time() - b0 > 200:
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
            pe_ = r.get("pe")
            # negative ROE only signals trouble when earnings are also non-positive;
            # buyback-driven negative book equity (HPQ-type) is not a trap signal
            trapish = ((rg_ or 0) < -0.05 or (fy_ is not None and fy_ < 0)
                        or (roe_ is not None and roe_ < 0
                            and (pe_ is None or pe_ <= 0)))
            if label == "CHEAP":
                vclass = ("VALUE TRAP RISK" if trapish
                           else "POTENTIALLY UNDERVALUED" if quality_ok else "LOW MULTIPLE")
            elif label == "RICH":
                vclass = "HIGH MULTIPLE"
            else:
                vclass = "SECTOR MID"
            # own-history axis: where does the CURRENT multiple sit vs its own 10y?
            hrow = st["hist"].get(t) or {}
            hn, hd = 0.0, 0.0
            for k, w in W.items():
                if w <= 0:
                    continue
                hv, cv = hrow.get(k), r.get(k)
                if hv and cv is not None and cv > 0 and len(hv) >= 5:
                    hp_ = sum(1 for x in hv if x <= cv) / len(hv) * 100
                    hn += w * max(5.0, min(95.0, hp_))
                    hd += w
            hist_pct = round(hn / hd, 0) if hd else None
            deep_discount = bool(vpct <= 25 and hist_pct is not None and hist_pct <= 25)
            # Rule of 40 with zero extra fetches: FCF margin = FCF yield x P/S
            fcfm_ = (r.get("fcf_y") * r.get("ps") * 100
                      if (r.get("fcf_y") is not None and r.get("ps")) else None)
            rule40 = (round((r.get("rev_g") or 0) * 100 + fcfm_, 0)
                       if (fcfm_ is not None and r.get("rev_g") is not None) else None)
            # semi cycle inversion: low P/E AT peak own-history margins = trap;
            # high/negative P/E at trough margins = recovery candidate
            cycle_note = None
            if t in SEMI_SET:
                hpe = hrow.get("pe") or []
                hgm = hrow.get("gm") or []
                cpe, cgm = r.get("pe"), r.get("gm")
                if len(hpe) >= 8 and len(hgm) >= 8 and cgm is not None:
                    gm_pos = sum(1 for x in hgm if x <= cgm) / len(hgm)
                    if cpe is not None and cpe > 0:
                        pe_pos = sum(1 for x in hpe if x <= cpe) / len(hpe)
                        if pe_pos <= 0.25 and gm_pos >= 0.80:
                            cycle_note = "PEAK-CYCLE? low P/E at peak margins"
                    if (cpe is None or cpe <= 0 or
                            (cpe > 0 and sum(1 for x in hpe if x <= cpe) / len(hpe) >= 0.80)) \
                            and gm_pos <= 0.25:
                        cycle_note = "CYCLE-TROUGH candidate (depressed margins)"
            peg = None
            if r.get("pe") and r.get("eps_g") and r["eps_g"] > 0:
                peg = round(r["pe"] / (r["eps_g"] * 100), 2)
            row = {"t": t, "sector": s_}
            PCT4 = {"div_y", "fcf_y", "roe", "roa", "gm", "om"}
            for k in RATIO_LADDERS:
                row[k] = (round(r[k], 4 if k in PCT4 else 2)
                           if r.get(k) is not None else None)
            row["sbc_rev"] = (round(r["sbc_rev"], 4)
                               if r.get("sbc_rev") is not None else None)
            row.update({"rev_g": round(r["rev_g"] * 100, 1) if r.get("rev_g") is not None else None,
                         "eps_g": round(r["eps_g"] * 100, 1) if r.get("eps_g") is not None else None,
                         "fcf_g": round(r["fcf_g"] * 100, 1) if r.get("fcf_g") is not None else None,
                         "peg": peg, "value_pct": vpct, "label": label,
                         "vclass": vclass, "hist_pct": hist_pct,
                         "deep_discount": deep_discount, "rule40": rule40,
                         "cycle_note": cycle_note, "gf_gap": gf.get(t)})
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
        sbc4 = [x.get("sbc") for x in cf if x.get("sbc") is not None]
        sbc_ttm = sum(sbc4) if len(sbc4) >= 2 else None
        sbc_pct_rev = (round(sbc_ttm / rev_ttm * 100, 1)
                        if (sbc_ttm is not None and rev_ttm) else None)
        fcf_after_sbc = (round(fcf_ttm - sbc_ttm)
                          if (fcf_ttm is not None and sbc_ttm is not None) else None)
        capex4 = [x.get("capex") for x in cf if x.get("capex") is not None]
        capex_pct_rev = (round(abs(sum(capex4)) / rev_ttm * 100, 1)
                          if (capex4 and rev_ttm) else None)
        fcf_margin = (round(fcf_ttm / rev_ttm * 100, 1)
                       if (fcf_ttm is not None and rev_ttm) else None)
        rule40 = (round((yoy or 0) * 100 + fcf_margin, 0)
                   if (fcf_margin is not None and yoy is not None) else None)
        gp_ttm = sum(x.get("gp") or 0 for x in inc[:4]) if len(inc) >= 4 else None
        gp_prior = sum(x.get("gp") or 0 for x in inc[4:8]) if len(inc) >= 8 else None
        gp_yoy = (gp_ttm / gp_prior - 1) if (gp_ttm and gp_prior and gp_prior > 0) else None
        op_leverage = (round((gp_yoy - yoy) * 100, 1)
                        if (gp_yoy is not None and yoy is not None) else None)
        if hp_secmap.get(t) in FIN_SECTORS:
            op_leverage = None   # GP semantics n/m for financials
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
        if sbc_pct_rev is not None and sbc_pct_rev >= 20:
            soft.append(f"SBC {sbc_pct_rev:.0f}% of revenue")
        if ((yoy or 0) > 0.15 and (capex_pct_rev or 0) > 30 and (fcf_ttm or 0) < 0):
            soft.append(f"capex pass-through {capex_pct_rev:.0f}% of rev (AI-spender pattern)")
        if is_fin:
            soft.append("financial: EV/GM/cash-vs-debt n/m, neutral-scored")
        total = round(sum(cats.values()), 1)
        if flags:
            total = min(total, 45.0)
        q_base = (cats["gross_margin"] * 0.6
                   + (10 if (fcf_ttm or 0) > 0 else 4 if fcf_ttm is None else 1) * 0.4)
        if sbc_pct_rev is not None and sbc_pct_rev >= 15:
            q_base -= 2          # SBC is a real cost (doc tier: 10-20 watch, 20+ major)
        if op_leverage is not None and op_leverage > 2 and (yoy or 0) > 0:
            q_base += 1          # gross profit outgrowing revenue = operating leverage
        pillars = {"value": cats["valuation"],
                    "quality": round(max(0, min(10, q_base)), 1),
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
        # doc S21 "cheap AI stock" formula as a strict badge
        formula21 = bool(not is_fin
                          and (yoy or 0) >= 0.20
                          and (g or 0) >= 0.50
                          and rr.get("ps") is not None
                          and (rr["ps"] < 5 or (rr["ps"] < 8 and (yoy or 0) >= 0.40))
                          and (fcf_ttm or 0) > 0
                          and (dil is None or dil < 0.05)
                          and cats["chart"] >= 6
                          and not flags)
        hp_out.append({"t": t, "sector": hp_secmap.get(t), "score": total, "cats": cats,
                        "flags": flags, "soft_flags": soft, "pillars": pillars,
                        "hp_class": hp_class, "formula21": formula21,
                        "chart_detail": cdet,
                        "metrics": {"ps": rr.get("ps"), "p_fcf": rr.get("p_fcf"),
                                     "rev_yoy_pct": round(yoy * 100, 1) if yoy is not None else None,
                                     "gross_margin_pct": round((g or 0) * 100, 1) if g is not None else None,
                                     "dilution_yoy_pct": round(dil * 100, 1) if dil is not None else None,
                                     "rule40": rule40, "fcf_margin": fcf_margin,
                                     "sbc_pct_rev": sbc_pct_rev,
                                     "fcf_after_sbc": fcf_after_sbc,
                                     "capex_pct_rev": capex_pct_rev,
                                     "op_leverage": op_leverage,
                                     "ev": ev_hp, "ev_s": ev_s_hp, "net_cash": net_cash,
                                     "fcf_ttm": fcf_ttm, "cash": cash, "debt": debt,
                                     "runway_q": round(runway_q, 1) if runway_q else None}})
    hp_out.sort(key=lambda x: -x["score"])
    # ── UNDERLOOKED scoring: small + ignored-by-volume + strong + cheap + basing ──
    mcs = sorted(m for m in hp_mcap.values() if m)
    tov = {t: dv[t] / m for t, m in hp_mcap.items() if m and dv.get(t)}
    tol = sorted(tov.values())

    def pup(arr, v):
        if not arr or v is None:
            return 0.5
        return sum(1 for x in arr if x <= v) / len(arr)

    for x in hp_out:
        t = x["t"]
        m = hp_mcap.get(t)
        if x["flags"] or not m:
            x["underlooked"] = None
            x["industry"] = hp_ind.get(t)
            continue
        small = (1 - pup(mcs, m)) * 20
        attn = ((1 - pup(tol, tov.get(t))) * 25) if t in tov else 12.0
        fund = (x["cats"]["revenue_growth"] + x["pillars"]["quality"]
                 + x["pillars"]["survival"]) / 30 * 35
        val = x["pillars"]["value"] / 10 * 10
        r_ = rings.get(t) or []
        hi_ = ((r_[-1] / max(r_[-252:]) - 1) * 100) if len(r_) >= 60 else None
        base = (10 if (hi_ is not None and -35 <= hi_ <= -5)
                 else 6 if (hi_ is not None and (-50 <= hi_ < -35 or hi_ > -5))
                 else 2)
        x["underlooked"] = round(small + attn + fund + val + base, 1)
        x["industry"] = hp_ind.get(t)
        x["turnover_bp"] = round(tov[t] * 10000, 1) if t in tov else None
        x["off_hi_pct"] = round(hi_, 1) if hi_ is not None else None
    ranked = sorted([x for x in hp_out if x.get("underlooked") is not None],
                     key=lambda x: -x["underlooked"])

    def slim(x):
        mt = x.get("metrics") or {}
        return {"t": x["t"], "underlooked": x["underlooked"], "hp_score": x["score"],
                 "class": x.get("hp_class"), "industry": x.get("industry"),
                 "sector": x.get("sector"), "mcap": hp_mcap.get(x["t"]),
                 "turnover_bp": x.get("turnover_bp"), "off_hi_pct": x.get("off_hi_pct"),
                 "ps": round(mt["ps"], 2) if mt.get("ps") is not None else None,
                 "rev_yoy_pct": mt.get("rev_yoy_pct"),
                 "rule40": mt.get("rule40"), "runway_q": mt.get("runway_q"),
                 "net_cash": mt.get("net_cash"), "soft_flags": x.get("soft_flags")}
    industries = {}
    for x in ranked:
        ind = x.get("industry") or "Other"
        industries.setdefault(ind, [])
        if len(industries[ind]) < 10:
            industries[ind].append(slim(x))
    underlooked_top = [slim(x) for x in ranked[:25]]
    DIAG.append(f"underlooked: {len(ranked)} scored across {len(industries)} industries")
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
            "hist_coverage": sum(1 for x in sp_table if x.get("hist_pct") is not None),
            "n_deep_discount": sum(1 for x in sp_table if x.get("deep_discount")),
            "sp_asof": st.get("sp_asof"),
            "hp": hp_out[:80], "hp_coverage": len(hp_out), "hp_universe": len(hp_rows),
            "industries": industries, "n_industries": len(industries),
            "underlooked_top": underlooked_top,
            "hp_src": st.get("hp_src"), "hp_logged": logged, "n_serious": len(serious),
            "diagnostics": list(DIAG),
            "methodology": ("Layer A: sector-WEIGHTED composite of winsorized (5-95) "
                             "sector percentiles — P/B-led for Financials, FFO-proxy-led for "
                             "REITs, P/S+P/FCF-led for Tech, EV/EBITDA-led default; negative "
                             "earnings/FCF map to 70th pct (NM: worse than neutral, not "
                             "max-penalty). Labels are MULTIPLE labels (LOW<=25th, HIGH>=75th); hist_pct = the same weighted composite vs the name's OWN 10y annual multiples (FMP ratios-annual), and deep_discount marks bottom-quartile on BOTH the sector axis and the own-history axis — 'below sector & history'; "
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
                             "cheap; read the class. Playbook layer: Rule-of-40 (growth+FCF margin) on both layers; SBC%-of-rev with FCF-after-SBC (SBC>=15% docks quality); operating leverage (GP growth vs revenue growth, bonus when positive); capex pass-through soft-flag (growing + capex>30%rev + FCF<0 = AI-spender pattern); semi cycle inversion on 18 S&P semis (low P/E at own-history peak margins = PEAK-CYCLE trap note; depressed margins + high/neg P/E = TROUGH candidate); formula21 badge = strict cheap-AI screen (20%+ growth, 50%+ GM, P/S<5 (<8 if 40%+), FCF+, dilution<5%, chart>=6, no hard flags). UNDERLOOKED board: per-industry top-10 by underlooked score = smallness(20) + low-attention turnover dv/mcap(25) + fundamental strength growth+quality+survival(35) + value pillar(10) + basing chart -35..-5% off-hi(10); hard-flagged names excluded. Score>=75 with no hard flags logs "
                             "hp_score (UP, 63d) to the graded loop. Research, not advice.")}
    clean = json.loads(json.dumps(out, default=str), parse_constant=lambda c: None)
    S3.put_object(Bucket=BUCKET, Key=OUT_KEY, Body=json.dumps(clean).encode(),
                  ContentType="application/json", CacheControl="public, max-age=1800")
    print(f"[valuations] sp={len(sp_table)} hp={len(hp_out)} serious={len(serious)} "
          f"{out['duration_s']}s")
    return {"statusCode": 200, "body": json.dumps({"sp": len(sp_table), "hp": len(hp_out)})}
