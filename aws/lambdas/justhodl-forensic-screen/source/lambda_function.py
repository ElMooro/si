"""justhodl-forensic-screen

Forensic accounting / earnings quality screen for the S&P 500 universe.

Beyond the Altman Z-Score and Piotroski Score that the screener already
computes (bankruptcy risk + financial health), this Lambda produces the
two most-cited earnings-manipulation detectors that hedge fund forensic
teams use:

  1. BENEISH M-SCORE — 8-factor model that statistically predicts
     earnings manipulation. M > -1.78 ≈ flagged as possible manipulator.
     Built from 2 years of statements:
         M = -4.84 + 0.92·DSRI + 0.528·GMI + 0.404·AQI + 0.892·SGI
                  + 0.115·DEPI − 0.172·SGAI + 4.679·TATA − 0.327·LVGI

     DSRI = Days Sales in Receivables Index
     GMI  = Gross Margin Index (prior / current — rising = squeeze)
     AQI  = Asset Quality Index (non-current non-PPE / total assets ratio change)
     SGI  = Sales Growth Index (revenue growth velocity)
     DEPI = Depreciation Index (slowing depreciation = earnings boost)
     SGAI = SG&A Index (rising costs)
     LVGI = Leverage Index (rising debt)
     TATA = Total Accruals to Total Assets

  2. SLOAN ACCRUALS — Earnings quality test (Sloan 1996).
       accrual_ratio = (NetIncome − OperatingCashFlow) / TotalAssets
     High positive values = earnings driven by accruals not cash =
     historically lower future returns.

Plus two composite quality factors:

  3. WORKING CAPITAL DIVERGENCE — when working-capital growth wildly
     exceeds revenue growth, often a channel-stuffing / receivables
     buildup signal. Ratio = WC_growth − Revenue_growth.

  4. GOODWILL BLOAT — goodwill ÷ total assets. >40% means earnings are
     hostage to intangible asset valuations and a write-down event is
     a tail risk.

Composite "earnings concern" score 0-100 weighted across these four
factors. Top 25 most-flagged + top 25 cleanest both surfaced.

Reads:
  - screener/data.json    → universe + market caps
  - FMP /stable/financial-statement-symbol-list (sanity)
  - FMP /stable/income-statement?symbol={s}&period=annual&limit=2
  - FMP /stable/balance-sheet-statement?symbol={s}&period=annual&limit=2
  - FMP /stable/cash-flow-statement?symbol={s}&period=annual&limit=2

Writes:
  - data/forensic-screen.json

Schedule: rate(12 hours) — financial statements update slowly.
"""
from __future__ import annotations
import json
import os
import time
import urllib.request
import urllib.parse
from datetime import datetime, timezone

import boto3
from botocore.exceptions import ClientError

REGION = "us-east-1"
S3_BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
SCREENER_KEY = os.environ.get("SCREENER_KEY", "screener/data.json")
OUTPUT_KEY = os.environ.get("OUTPUT_KEY", "data/forensic-screen.json")
FMP_KEY = os.environ.get("FMP_KEY", "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb")
FMP_BASE = "https://financialmodelingprep.com/stable"
USER_AGENT = os.environ.get("USER_AGENT", "JustHodl Forensic Screen raafouis@gmail.com")
N_TICKERS = int(os.environ.get("N_TICKERS", "503"))   # full S&P 500
HTTP_TIMEOUT = 12

# Beneish thresholds
M_SCORE_FLAG = -1.78    # > this = potentially manipulating
M_SCORE_HIGH_RISK = -1.0   # > this = strong flag

s3 = boto3.client("s3", region_name=REGION)


# ─── HTTP / FMP helpers ─────────────────────────────────────────────────

def _fmp(endpoint: str, params: dict | None = None):
    p = dict(params or {})
    p["apikey"] = FMP_KEY
    qs = urllib.parse.urlencode(p)
    url = f"{FMP_BASE}/{endpoint}?{qs}"
    try:
        req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
        with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT) as r:
            return json.loads(r.read())
    except Exception as e:
        print(f"  fmp_err {endpoint} {params}: {e}")
        return None


def sf(v):
    """Safe float — returns None if not parseable."""
    try:
        if v is None or v == "":
            return None
        return float(v)
    except (TypeError, ValueError):
        return None


def safe_div(a, b):
    if a is None or b is None or b == 0:
        return None
    try:
        return a / b
    except Exception:
        return None


# ─── Statement fetch ────────────────────────────────────────────────────

FLOW_KEYS = ("revenue", "costOfRevenue", "grossProfit",
             "generalAndAdministrativeExpenses",
             "sellingGeneralAndAdministrativeExpenses",
             "depreciationAndAmortization", "netIncome",
             "operatingIncome", "ebitda", "operatingCashFlow",
             "netCashProvidedByOperatingActivities",
             "capitalExpenditure")


def _ttm(rows):
    """Synthetic TTM statement: flow fields summed over 4 quarters
    (None if any quarter missing the field)."""
    out = {}
    for k in FLOW_KEYS:
        vals = [sf(r.get(k)) for r in rows]
        out[k] = sum(vals) if all(v is not None for v in vals) else None
    return out


def fetch_statements(symbol: str):
    """Quarterly limit=9 -> TTM windows. Enables the M-Score TREND
    (now vs one quarter ago) at the same 3-call cost; annual fallback
    when quarterly history is short."""
    income = _fmp("income-statement", {"symbol": symbol, "period": "quarter", "limit": 9})
    balance = _fmp("balance-sheet-statement", {"symbol": symbol, "period": "quarter", "limit": 9})
    cashflow = _fmp("cash-flow-statement", {"symbol": symbol, "period": "quarter", "limit": 9})
    if all(isinstance(x, list) and len(x) >= 8
           for x in (income, balance, cashflow)):
        st = {
            "income_t": _ttm(income[0:4]), "income_p": _ttm(income[4:8]),
            "balance_t": balance[0], "balance_p": balance[4],
            "cashflow_t": _ttm(cashflow[0:4]),
            "cashflow_p": _ttm(cashflow[4:8]),
        }
        if all(len(x) >= 9 for x in (income, balance, cashflow)):
            st["prev_q"] = {
                "income_t": _ttm(income[1:5]), "income_p": _ttm(income[5:9]),
                "balance_t": balance[1], "balance_p": balance[5],
                "cashflow_t": _ttm(cashflow[1:5]),
                "cashflow_p": _ttm(cashflow[5:9]),
            }
        scores = _fmp("financial-scores", {"symbol": symbol})
        scores = scores[0] if isinstance(scores, list) and scores else \
            (scores if isinstance(scores, dict) else {})
        st["scores"] = scores
        return st
    # annual fallback (short history)
    income = _fmp("income-statement", {"symbol": symbol, "period": "annual", "limit": 2})
    balance = _fmp("balance-sheet-statement", {"symbol": symbol, "period": "annual", "limit": 2})
    cashflow = _fmp("cash-flow-statement", {"symbol": symbol, "period": "annual", "limit": 2})

    if not (isinstance(income, list) and isinstance(balance, list) and isinstance(cashflow, list)):
        return None
    if len(income) < 2 or len(balance) < 2 or len(cashflow) < 2:
        return None

    scores = _fmp("financial-scores", {"symbol": symbol})
    scores = scores[0] if isinstance(scores, list) and scores else \
        (scores if isinstance(scores, dict) else {})
    return {
        "income_t":  income[0],   # most recent
        "income_p":  income[1],   # prior
        "balance_t": balance[0],
        "balance_p": balance[1],
        "cashflow_t": cashflow[0],
        "cashflow_p": cashflow[1],
        "scores": scores,
    }


# ─── Beneish M-Score factors ────────────────────────────────────────────

def compute_beneish(s):
    """Compute the 8 Beneish factors and the composite M-Score.
    Returns dict of factors + m_score, or {error: ...}.
    """
    it, ip = s["income_t"], s["income_p"]
    bt, bp = s["balance_t"], s["balance_p"]
    ct, cp = s["cashflow_t"], s["cashflow_p"]

    # Pull common fields (FMP uses snake_case in /stable)
    rev_t = sf(it.get("revenue"))
    rev_p = sf(ip.get("revenue"))
    cogs_t = sf(it.get("costOfRevenue"))
    cogs_p = sf(ip.get("costOfRevenue"))
    sga_t = sf(it.get("generalAndAdministrativeExpenses") or it.get("sellingGeneralAndAdministrativeExpenses"))
    sga_p = sf(ip.get("generalAndAdministrativeExpenses") or ip.get("sellingGeneralAndAdministrativeExpenses"))
    dep_t = sf(it.get("depreciationAndAmortization"))
    dep_p = sf(ip.get("depreciationAndAmortization"))
    ni_t = sf(it.get("netIncome"))

    receivables_t = sf(bt.get("netReceivables") or bt.get("accountsReceivables"))
    receivables_p = sf(bp.get("netReceivables") or bp.get("accountsReceivables"))
    current_assets_t = sf(bt.get("totalCurrentAssets"))
    current_assets_p = sf(bp.get("totalCurrentAssets"))
    total_assets_t = sf(bt.get("totalAssets"))
    total_assets_p = sf(bp.get("totalAssets"))
    ppe_t = sf(bt.get("propertyPlantEquipmentNet") or bt.get("propertyPlantEquipmentNetOfDepreciation"))
    ppe_p = sf(bp.get("propertyPlantEquipmentNet") or bp.get("propertyPlantEquipmentNetOfDepreciation"))
    long_debt_t = sf(bt.get("longTermDebt"))
    long_debt_p = sf(bp.get("longTermDebt"))
    current_debt_t = sf(bt.get("shortTermDebt"))
    current_debt_p = sf(bp.get("shortTermDebt"))
    goodwill_t = sf(bt.get("goodwill"))
    intangibles_t = sf(bt.get("intangibleAssets") or bt.get("goodwillAndIntangibleAssets"))

    cfo_t = sf(ct.get("operatingCashFlow") or ct.get("netCashProvidedByOperatingActivities"))

    out = {"factors": {}, "components_missing": []}

    # DSRI = (Receivables_t / Revenue_t) / (Receivables_p / Revenue_p)
    days_t = safe_div(receivables_t, rev_t)
    days_p = safe_div(receivables_p, rev_p)
    dsri = safe_div(days_t, days_p)

    # GMI = Gross_Margin_p / Gross_Margin_t  (>1 means margins compressing)
    gm_t = safe_div(rev_t - cogs_t if rev_t and cogs_t else None, rev_t)
    gm_p = safe_div(rev_p - cogs_p if rev_p and cogs_p else None, rev_p)
    gmi = safe_div(gm_p, gm_t)

    # AQI = (1 - (CurrentAssets+PPE)/TotalAssets)_t / same_p
    # (rising = more weight in non-current non-PPE = often goodwill / intangibles)
    if all(v is not None for v in [current_assets_t, ppe_t, total_assets_t,
                                     current_assets_p, ppe_p, total_assets_p]) \
            and total_assets_t > 0 and total_assets_p > 0:
        aqi_t = 1 - ((current_assets_t + ppe_t) / total_assets_t)
        aqi_p = 1 - ((current_assets_p + ppe_p) / total_assets_p)
        aqi = safe_div(aqi_t, aqi_p)
    else:
        aqi = None

    # SGI = Revenue_t / Revenue_p  (sales growth)
    sgi = safe_div(rev_t, rev_p)

    # DEPI = (Dep_p / (Dep_p + PPE_p)) / (Dep_t / (Dep_t + PPE_t))
    # >1 = depreciation rate slowing (earnings boost)
    if all(v is not None for v in [dep_t, dep_p, ppe_t, ppe_p]):
        dep_rate_t = safe_div(dep_t, dep_t + ppe_t)
        dep_rate_p = safe_div(dep_p, dep_p + ppe_p)
        depi = safe_div(dep_rate_p, dep_rate_t)
    else:
        depi = None

    # SGAI = (SGA_t / Rev_t) / (SGA_p / Rev_p)  >1 = SGA rising faster than rev
    sgai_t = safe_div(sga_t, rev_t)
    sgai_p = safe_div(sga_p, rev_p)
    sgai = safe_div(sgai_t, sgai_p)

    # LVGI = ((LTDebt+CurrentDebt)/TotalAssets)_t / same_p  >1 = leverage rising
    if all(v is not None for v in [long_debt_t, current_debt_t, total_assets_t,
                                     long_debt_p, current_debt_p, total_assets_p]) \
            and total_assets_t > 0 and total_assets_p > 0:
        lev_t = (long_debt_t + current_debt_t) / total_assets_t
        lev_p = (long_debt_p + current_debt_p) / total_assets_p
        lvgi = safe_div(lev_t, lev_p)
    else:
        lvgi = None

    # TATA = (Income before XO − CFO) / TotalAssets
    if ni_t is not None and cfo_t is not None and total_assets_t and total_assets_t > 0:
        tata = (ni_t - cfo_t) / total_assets_t
    else:
        tata = None

    factors = {
        "DSRI": round(dsri, 4) if dsri else None,
        "GMI":  round(gmi, 4) if gmi else None,
        "AQI":  round(aqi, 4) if aqi else None,
        "SGI":  round(sgi, 4) if sgi else None,
        "DEPI": round(depi, 4) if depi else None,
        "SGAI": round(sgai, 4) if sgai else None,
        "LVGI": round(lvgi, 4) if lvgi else None,
        "TATA": round(tata, 6) if tata else None,
    }
    out["factors"] = factors

    # Compute composite if all components present
    components = [dsri, gmi, aqi, sgi, depi, sgai, lvgi, tata]
    missing = [k for k, v in factors.items() if v is None]
    out["components_missing"] = missing

    if not missing:
        m = (-4.84
             + 0.920 * dsri
             + 0.528 * gmi
             + 0.404 * aqi
             + 0.892 * sgi
             + 0.115 * depi
             - 0.172 * sgai
             + 4.679 * tata
             - 0.327 * lvgi)
        out["m_score"] = round(m, 4)
        out["m_flag"] = m > M_SCORE_FLAG
        out["m_high_risk"] = m > M_SCORE_HIGH_RISK
    else:
        out["m_score"] = None
        out["m_flag"] = None
        out["m_high_risk"] = None

    # Bonus: Sloan accruals
    sloan = None
    if ni_t is not None and cfo_t is not None and total_assets_t and total_assets_t > 0:
        sloan = (ni_t - cfo_t) / total_assets_t
    out["sloan_accruals"] = round(sloan, 6) if sloan is not None else None
    out["sloan_high_risk"] = (sloan is not None and sloan > 0.10)  # > 10% = typical Sloan threshold

    # Working capital divergence (revenue vs receivables)
    if rev_t and rev_p and receivables_t and receivables_p and rev_p > 0 and receivables_p > 0:
        rev_growth = (rev_t - rev_p) / rev_p
        rec_growth = (receivables_t - receivables_p) / receivables_p
        wc_divergence = rec_growth - rev_growth
        out["wc_divergence"] = round(wc_divergence, 4)
        out["wc_divergence_flag"] = wc_divergence > 0.20   # receivables grew 20% faster than revenue
    else:
        out["wc_divergence"] = None
        out["wc_divergence_flag"] = None

    # Goodwill bloat
    if goodwill_t is not None and total_assets_t and total_assets_t > 0:
        goodwill_pct = goodwill_t / total_assets_t
        out["goodwill_pct"] = round(goodwill_pct, 4)
        out["goodwill_bloat_flag"] = goodwill_pct > 0.40
    else:
        out["goodwill_pct"] = None
        out["goodwill_bloat_flag"] = None

    return out


FIN_SECTORS = {"Financial Services", "Financials", "Financial",
               "Insurance", "Real Estate", "Banks"}


def compute_strength(st, sector):
    """Institutional three-statement strength 0-100 + A+..F grade.
    Income 30 / Balance 35 / Cash 35. Sector-aware: Z + goodwill +
    FCF legs are not meaningful for financials -- equity/assets and
    CFO backing carry the balance/cash legs there."""
    it, ip = st["income_t"], st["income_p"]
    bt = st["balance_t"]
    ct = st["cashflow_t"]
    sc = st.get("scores") or {}
    fin = (sector or "") in FIN_SECTORS
    legs = {}

    inc = 0
    rev_t, rev_p = sf(it.get("revenue")), sf(ip.get("revenue"))
    g = None
    if rev_t and rev_p:
        g = (rev_t / rev_p - 1) * 100
        inc += 12 if g > 10 else 8 if g > 0 else 0
    gm_t = safe_div(sf(it.get("grossProfit")), rev_t)
    gm_p = safe_div(sf(ip.get("grossProfit")), rev_p)
    if gm_t is not None and gm_p is not None and gm_t >= gm_p - 0.005:
        inc += 8
    om_t = safe_div(sf(it.get("operatingIncome")), rev_t)
    om_p = safe_div(sf(ip.get("operatingIncome")), rev_p)
    if om_t is not None and om_t > 0:
        inc += 5
        if om_p is not None and om_t > om_p:
            inc += 5
    legs["income"] = min(30, inc)

    bal = 0
    z = sf(sc.get("altmanZScore"))
    if fin:
        ea = safe_div(sf(bt.get("totalStockholdersEquity")),
                      sf(bt.get("totalAssets")))
        if ea is not None:
            bal += 15 if ea > 0.08 else 7 if ea > 0.05 else 0
    elif z is not None:
        bal += 15 if z >= 3 else 7 if z >= 1.8 else 0
    ebitda = sf(it.get("ebitda"))
    nd = (sf(bt.get("totalDebt")) or 0) - (
        sf(bt.get("cashAndCashEquivalents")) or 0)
    nde = safe_div(nd, ebitda) if ebitda and ebitda > 0 else None
    if nde is not None:
        bal += 10 if nde < 1 else 6 if nde < 2 else 0
    elif nd <= 0:
        bal += 10
    cr = safe_div(sf(bt.get("totalCurrentAssets")),
                  sf(bt.get("totalCurrentLiabilities")))
    if cr is not None and cr >= 1.2:
        bal += 5
    if not fin:
        gw = safe_div(sf(bt.get("goodwill")), sf(bt.get("totalAssets")))
        if gw is None or gw < 0.20:
            bal += 5
    else:
        bal += 5
    legs["balance"] = min(35, bal)

    cash = 0
    ni = sf(it.get("netIncome"))
    ocf = sf(ct.get("operatingCashFlow")) or \
        sf(ct.get("netCashProvidedByOperatingActivities"))
    backing = safe_div(ocf, ni) if ni and ni > 0 else None
    if backing is not None:
        cash += 12 if backing >= 1 else 6 if backing >= 0.8 else 0
    elif ocf and ocf > 0:
        cash += 6
    capex = abs(sf(ct.get("capitalExpenditure")) or 0)
    if not fin:
        fcfm = safe_div((ocf or 0) - capex, rev_t)
        if fcfm is not None:
            cash += 10 if fcfm > 0.05 else 5 if fcfm > 0 else 0
        if ocf and safe_div(capex, ocf) is not None and \
                capex / ocf < 0.6:
            cash += 5
    else:
        cash += 15 if (ocf or 0) > 0 else 0
    pio = sf(sc.get("piotroskiScore"))
    if pio is not None:
        cash += 8 if pio >= 7 else 4 if pio >= 5 else 0
    legs["cash"] = min(35, cash)

    total = legs["income"] + legs["balance"] + legs["cash"]
    grade = ("A+" if total >= 88 else "A" if total >= 80 else
             "A-" if total >= 74 else "B+" if total >= 68 else
             "B" if total >= 60 else "B-" if total >= 54 else
             "C+" if total >= 48 else "C" if total >= 40 else
             "D" if total >= 30 else "F")
    return {"strength_score": total, "strength_grade": grade,
            "strength_legs": legs, "altman_z": z, "piotroski": pio,
            "rev_growth_pct": round(g, 1) if g is not None else None}


def composite_concern_score(forensic, sector=None):
    """0-100 — how concerning is this stock from a forensic POV.
    Higher = more concerning. For financial-sector names the Beneish /
    Sloan / WC-divergence legs are ZEROED — Beneish (1999) covers
    non-financial firms only; bank balance-sheet structure produces
    spurious flags (3108 problem board was bank-heavy for exactly this
    reason). Financials can still be flagged via goodwill, dilution
    (added by caller) and weak three-statement strength."""
    fin = (sector or "") in FIN_SECTORS
    score = 0
    if fin:
        if forensic.get("goodwill_bloat_flag"):
            score += 15
        return score
    if forensic.get("m_score") is not None:
        m = forensic["m_score"]
        if m > -0.5:
            score += 40   # very high
        elif m > M_SCORE_HIGH_RISK:
            score += 30
        elif m > M_SCORE_FLAG:
            score += 20
    if forensic.get("sloan_high_risk"):
        score += 25
    if forensic.get("wc_divergence_flag"):
        score += 20
    if forensic.get("goodwill_bloat_flag"):
        score += 15
    return min(100, score)


# ─── Main loop ──────────────────────────────────────────────────────────

def lambda_handler(event=None, context=None):
    started = time.time()
    print(f"[forensic] starting at {datetime.now(timezone.utc).isoformat()}")

    # 1. Universe from screener (top by mcap)
    try:
        screener_obj = s3.get_object(Bucket=S3_BUCKET, Key=SCREENER_KEY)
        screener = json.loads(screener_obj["Body"].read())
    except Exception as e:
        return {"statusCode": 500, "body": json.dumps({"error": f"screener load: {e}"})}

    universe = screener.get("rows") or screener.get("stocks") or screener.get("data") or []
    if not universe:
        return {"statusCode": 500, "body": json.dumps({"error": "screener has no universe"})}
    # Sort by mcap descending, take top N
    def get_mcap(row):
        return sf(row.get("mcap") or row.get("marketCap")) or 0
    universe.sort(key=get_mcap, reverse=True)
    universe = universe[:N_TICKERS]
    print(f"[forensic] universe: {len(universe)} tickers (top by mcap)")

    # 2. Fetch + score each
    try:
        sfl = json.loads(s3.get_object(
            Bucket=S3_BUCKET, Key="data/share-flows.json")["Body"].read()
        ).get("tickers") or {}
    except Exception:
        sfl = {}

    from concurrent.futures import ThreadPoolExecutor, as_completed
    results = []
    n_ok = n_skipped = n_errors = 0

    def one(row):
        symbol = row.get("symbol") or row.get("ticker")
        if not symbol:
            return None, "skip"
        stmts = fetch_statements(symbol)
        if not stmts:
            return None, "skip"
        forensic = compute_beneish(stmts)
        if stmts.get("prev_q"):
            prevf = compute_beneish(stmts["prev_q"])
            mp = prevf.get("m_score")
            mn = forensic.get("m_score")
            # deltas are only meaningful when both windows scored the
            # SAME component set; a field present in one window only
            # produces non-comparable M values (3110: |dM|>5 artifacts)
            same_comps = (sorted(prevf.get("components_missing") or [])
                          == sorted(forensic.get("components_missing")
                                    or []))
            if mp is not None and mn is not None and same_comps \
                    and abs(mn - mp) <= 3:
                forensic["m_score_prev_q"] = mp
                forensic["m_score_delta_q"] = round(mn - mp, 4)
                forensic["m_deteriorating"] = (
                    mn - mp >= 0.15 and mn > -2.5)
            elif mp is not None and mn is not None:
                forensic["m_trend_suspect"] = True
        forensic["symbol"] = symbol
        forensic["mcap"] = sf(row.get("mcap") or row.get("marketCap"))
        forensic["sector"] = row.get("sector")
        forensic.update(compute_strength(stmts, row.get("sector")))
        if (row.get("sector") or "") in FIN_SECTORS:
            for k in ("m_flag", "sloan_high_risk",
                      "wc_divergence_flag"):
                if forensic.get(k):
                    forensic[k] = False
                    forensic.setdefault("fin_suppressed_flags",
                                        []).append(k)
        cs = composite_concern_score(forensic, row.get("sector"))
        if forensic.get("m_deteriorating") and \
                (row.get("sector") or "") not in FIN_SECTORS:
            cs = min(100, cs + 10)
        sfr = sfl.get(symbol) or {}
        if sfr.get("extreme") or sfr.get("read") == "EXTREME_DILUTION":
            cs = min(100, cs + 15)
            forensic["dilution_flag"] = True
        elif (sfr.get("sh_yoy_pct") or 0) >= 5:
            cs = min(100, cs + 8)
            forensic["dilution_flag"] = True
        forensic["concern_score"] = cs
        return forensic, "ok"

    with ThreadPoolExecutor(max_workers=10) as exe:
        futs = {exe.submit(one, row): row for row in universe}
        for fu in as_completed(futs):
            try:
                r, st = fu.result()
            except Exception as e:
                n_errors += 1
                print("  err:", e)
                continue
            if st == "ok" and r:
                results.append(r)
                n_ok += 1
            else:
                n_skipped += 1

    # 3. Sort + pick highlights
    results.sort(key=lambda x: -(x.get("concern_score") or 0))

    # Filter financial-sector stocks from the "concerning" list — Beneish (1999)
    # was designed for non-financial firms; banks/insurers/REITs have totally
    # different balance sheet structure that produces spurious M-Score flags.
    # Still computed and stored in all_results for transparency, but excluded
    # from the headline most-concerning view.
    FINANCIAL_SECTORS = {"Financial Services", "Financials", "Financial",
                         "Insurance", "Real Estate", "Banks"}
    non_financial = [r for r in results if (r.get("sector") or "") not in FINANCIAL_SECTORS]
    n_financial_excluded = len(results) - len(non_financial)
    most_concerning = non_financial[:25]

    cleanest = [r for r in non_financial if r.get("m_score") is not None]
    cleanest.sort(key=lambda x: x.get("m_score") or 999)
    cleanest = cleanest[:25]

    # industry-relative strength percentile + sector medians
    by_sec = {}
    for r in results:
        if r.get("strength_score") is not None:
            by_sec.setdefault(r.get("sector") or "Unknown",
                              []).append(r["strength_score"])
    sec_med = {}
    for sec2, vals in by_sec.items():
        v = sorted(vals)
        sec_med[sec2] = v[len(v) // 2]
    for r in results:
        vals = sorted(by_sec.get(r.get("sector") or "Unknown", []))
        ss = r.get("strength_score")
        if ss is not None and len(vals) >= 5:
            r["industry_pctile"] = round(
                100.0 * sum(1 for x in vals if x <= ss) / len(vals))

    strong = [r for r in results if r.get("strength_score") is not None
              and all(r.get("strength_legs", {}).get(k) is not None
                      for k in ("income", "balance", "cash"))]
    fortress = sorted(strong,
                      key=lambda x: (-x["strength_score"],
                                     x.get("concern_score") or 0))
    fortress = [r for r in fortress
                if (r.get("concern_score") or 0) < 40][:25]
    problems = sorted([r for r in results
                       if (r.get("concern_score") or 0) >= 40
                       or (r.get("strength_score") or 100) <= 35],
                      key=lambda x: (-(x.get("concern_score") or 0),
                                     x.get("strength_score") or 0))

    # 4. Distribution stats
    m_scores = [r.get("m_score") for r in results if r.get("m_score") is not None]
    sloan_vals = [r.get("sloan_accruals") for r in results if r.get("sloan_accruals") is not None]

    def stats(arr):
        if not arr:
            return {}
        a = sorted(arr)
        n = len(a)
        return {
            "n": n,
            "min": round(a[0], 4),
            "p25": round(a[n // 4], 4),
            "median": round(a[n // 2], 4),
            "p75": round(a[3 * n // 4], 4),
            "max": round(a[-1], 4),
            "mean": round(sum(a) / n, 4),
        }

    n_m_flagged = sum(1 for r in results if r.get("m_flag"))
    n_sloan_flagged = sum(1 for r in results if r.get("sloan_high_risk"))
    n_wc_flagged = sum(1 for r in results if r.get("wc_divergence_flag"))
    n_gw_flagged = sum(1 for r in results if r.get("goodwill_bloat_flag"))

    output = {
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "duration_s": round(time.time() - started, 1),
        "n_universe_attempted": len(universe),
        "n_scored_ok": n_ok,
        "n_skipped_missing_data": n_skipped,
        "n_errors": n_errors,
        "summary": {
            "n_with_m_score": len(m_scores),
            "n_m_flagged": n_m_flagged,
            "n_sloan_flagged": n_sloan_flagged,
            "n_wc_divergence_flagged": n_wc_flagged,
            "n_goodwill_bloat_flagged": n_gw_flagged,
            "n_financial_sector_excluded_from_most_concerning": n_financial_excluded,
            "m_score_distribution": stats(m_scores),
            "sloan_distribution": stats(sloan_vals),
        },
        "thresholds": {
            "m_score_flag": M_SCORE_FLAG,
            "m_score_high_risk": M_SCORE_HIGH_RISK,
            "sloan_high_risk": 0.10,
            "wc_divergence_flag": 0.20,
            "goodwill_bloat_flag": 0.40,
        },
        "version": "2.0.0",
        "fortress_financials": fortress,
        "problem_financials": problems[:40],
        "sector_strength_medians": sec_med,
        "n_strength": len(strong),
        "most_concerning_top_25": most_concerning,
        "cleanest_top_25": cleanest,
        "all_results": results,   # full list for the page to filter
        "v": "1.0",
    }

    s3.put_object(
        Bucket=S3_BUCKET, Key=OUTPUT_KEY,
        Body=json.dumps(output, default=str).encode(),
        ContentType="application/json",
        CacheControl="public, max-age=600",
    )

    print(f"[forensic] done: {n_ok} scored, {n_m_flagged} M-flagged, "
          f"{n_sloan_flagged} Sloan-flagged, duration={time.time()-started:.1f}s")
    return {
        "statusCode": 200,
        "body": json.dumps({
            "n_scored_ok": n_ok,
            "n_m_flagged": n_m_flagged,
            "n_sloan_flagged": n_sloan_flagged,
            "duration_s": round(time.time() - started, 1),
        }),
    }
