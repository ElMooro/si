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
N_TICKERS = int(os.environ.get("N_TICKERS", "200"))   # top by mcap
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

def fetch_statements(symbol: str):
    """Fetch 2 most recent annual income/balance/cashflow statements."""
    income = _fmp("income-statement", {"symbol": symbol, "period": "annual", "limit": 2})
    balance = _fmp("balance-sheet-statement", {"symbol": symbol, "period": "annual", "limit": 2})
    cashflow = _fmp("cash-flow-statement", {"symbol": symbol, "period": "annual", "limit": 2})

    if not (isinstance(income, list) and isinstance(balance, list) and isinstance(cashflow, list)):
        return None
    if len(income) < 2 or len(balance) < 2 or len(cashflow) < 2:
        return None

    return {
        "income_t":  income[0],   # most recent
        "income_p":  income[1],   # prior
        "balance_t": balance[0],
        "balance_p": balance[1],
        "cashflow_t": cashflow[0],
        "cashflow_p": cashflow[1],
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


def composite_concern_score(forensic):
    """0-100 — how concerning is this stock from a forensic POV.
    Higher = more concerning."""
    score = 0
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
    results = []
    n_ok = 0
    n_skipped = 0
    n_errors = 0
    for i, row in enumerate(universe):
        symbol = row.get("symbol") or row.get("ticker")
        if not symbol:
            continue
        try:
            stmts = fetch_statements(symbol)
            if not stmts:
                n_skipped += 1
                continue
            forensic = compute_beneish(stmts)
            forensic["symbol"] = symbol
            forensic["mcap"] = sf(row.get("mcap") or row.get("marketCap"))
            forensic["sector"] = row.get("sector")
            forensic["concern_score"] = composite_concern_score(forensic)
            results.append(forensic)
            n_ok += 1
            # Tiny rate-limiting between calls
            if i % 10 == 9:
                time.sleep(0.3)
        except Exception as e:
            n_errors += 1
            print(f"  err {symbol}: {e}")
            continue

    # 3. Sort + pick highlights
    results.sort(key=lambda x: -(x.get("concern_score") or 0))
    most_concerning = results[:25]
    cleanest = [r for r in results if r.get("m_score") is not None]
    cleanest.sort(key=lambda x: x.get("m_score") or 999)
    cleanest = cleanest[:25]

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
