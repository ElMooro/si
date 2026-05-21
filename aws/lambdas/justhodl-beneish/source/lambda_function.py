"""
Pro Pack v3 #6 - Beneish M-Score (GuruFocus fraud-detection gap-closer)
========================================================================

The Beneish M-Score is an 8-variable composite that estimates the likelihood
a company is manipulating earnings. Messod Beneish (Indiana U, 1999) showed
M-Score correctly classified 76% of manipulators (incl. Enron 6 months
before collapse).

Formula:
  M = -4.84 + 0.92*DSRI + 0.528*GMI + 0.404*AQI + 0.892*SGI + 0.115*DEPI
      - 0.172*SGAI + 4.679*TATA - 0.327*LVGI

Interpretation:
  M > -1.78 = LIKELY MANIPULATOR (high red-flag risk)
  M < -2.22 = UNLIKELY MANIPULATOR (clean)
  -2.22 to -1.78 = AMBIGUOUS / WATCH-LIST

8 component variables (each year-over-year ratio):
  DSRI  Days Sales Receivable Index   - AR growing faster than sales? (channel-stuffing)
  GMI   Gross Margin Index            - margins deteriorating? (incentive to manage)
  AQI   Asset Quality Index           - more non-current non-PPE assets? (capitalizing exp)
  SGI   Sales Growth Index            - high growth = incentive + cover
  DEPI  Depreciation Index            - slowing depreciation? (extending asset life to boost income)
  SGAI  SG&A Index                    - SG&A growing slower than sales? (operating leverage manipulation)
  TATA  Total Accruals to Total Assets - cash vs accrual divergence
  LVGI  Leverage Index                - debt covenant pressure?

Universe: top-50 SP500 by mcap (STATIC_TOP50_SPX shared with starmine).
Output: per-ticker M-Score + risk category + 8 component breakdowns
        + top 10 RED FLAGS + market-wide median + composite distribution.

Schedule: weekly Sundays 03:00 UTC (FMP fundamentals don't change daily).
"""
import os
import sys
import json
import time
import statistics
import urllib.request
import urllib.error
import urllib.parse
from datetime import datetime, timezone

import boto3

VERSION = "1.0.0"
S3_BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
S3_KEY = "data/beneish.json"
FMP_BASE = "https://financialmodelingprep.com/stable"
FMP_KEY = os.environ.get("FMP_KEY", "")
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")

UNIVERSE_TOP_N = 50
FMP_SLEEP_SEC = 0.4
HTTP_TIMEOUT = 20

# Same static universe used by starmine for cross-tool consistency
STATIC_TOP50_SPX = [
    {"symbol": "AAPL", "sector": "Technology"},
    {"symbol": "MSFT", "sector": "Technology"},
    {"symbol": "NVDA", "sector": "Technology"},
    {"symbol": "GOOGL", "sector": "Communication Services"},
    {"symbol": "GOOG", "sector": "Communication Services"},
    {"symbol": "AMZN", "sector": "Consumer Cyclical"},
    {"symbol": "META", "sector": "Communication Services"},
    {"symbol": "TSLA", "sector": "Consumer Cyclical"},
    {"symbol": "BRK-B", "sector": "Financial Services"},
    {"symbol": "JPM", "sector": "Financial Services"},
    {"symbol": "LLY", "sector": "Healthcare"},
    {"symbol": "V", "sector": "Financial Services"},
    {"symbol": "XOM", "sector": "Energy"},
    {"symbol": "UNH", "sector": "Healthcare"},
    {"symbol": "JNJ", "sector": "Healthcare"},
    {"symbol": "MA", "sector": "Financial Services"},
    {"symbol": "WMT", "sector": "Consumer Defensive"},
    {"symbol": "PG", "sector": "Consumer Defensive"},
    {"symbol": "AVGO", "sector": "Technology"},
    {"symbol": "HD", "sector": "Consumer Cyclical"},
    {"symbol": "ORCL", "sector": "Technology"},
    {"symbol": "MRK", "sector": "Healthcare"},
    {"symbol": "COST", "sector": "Consumer Defensive"},
    {"symbol": "ABBV", "sector": "Healthcare"},
    {"symbol": "BAC", "sector": "Financial Services"},
    {"symbol": "CVX", "sector": "Energy"},
    {"symbol": "ADBE", "sector": "Technology"},
    {"symbol": "KO", "sector": "Consumer Defensive"},
    {"symbol": "CRM", "sector": "Technology"},
    {"symbol": "PEP", "sector": "Consumer Defensive"},
    {"symbol": "AMD", "sector": "Technology"},
    {"symbol": "ACN", "sector": "Technology"},
    {"symbol": "TMO", "sector": "Healthcare"},
    {"symbol": "MCD", "sector": "Consumer Cyclical"},
    {"symbol": "CSCO", "sector": "Technology"},
    {"symbol": "WFC", "sector": "Financial Services"},
    {"symbol": "ABT", "sector": "Healthcare"},
    {"symbol": "LIN", "sector": "Basic Materials"},
    {"symbol": "DHR", "sector": "Healthcare"},
    {"symbol": "DIS", "sector": "Communication Services"},
    {"symbol": "TXN", "sector": "Technology"},
    {"symbol": "NFLX", "sector": "Communication Services"},
    {"symbol": "GE", "sector": "Industrials"},
    {"symbol": "IBM", "sector": "Technology"},
    {"symbol": "INTU", "sector": "Technology"},
    {"symbol": "AMGN", "sector": "Healthcare"},
    {"symbol": "VZ", "sector": "Communication Services"},
    {"symbol": "PFE", "sector": "Healthcare"},
    {"symbol": "QCOM", "sector": "Technology"},
    {"symbol": "CMCSA", "sector": "Communication Services"},
]


def http_json(url, retries=2):
    last_err = None
    for attempt in range(retries + 1):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
            with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT) as r:
                return json.loads(r.read().decode("utf-8", errors="ignore"))
        except urllib.error.HTTPError as e:
            last_err = f"HTTP {e.code}"
            if e.code == 429 and attempt < retries:
                time.sleep(5 * (attempt + 1))
                continue
            return {"_error": last_err, "_code": e.code}
        except Exception as e:
            last_err = str(e)[:100]
            if attempt < retries:
                time.sleep(2)
                continue
    return {"_error": last_err}


def fmp_income_statement(symbol, n=3):
    url = (f"{FMP_BASE}/income-statement?symbol={symbol}"
           f"&limit={n}&apikey={FMP_KEY}")
    d = http_json(url)
    return d if isinstance(d, list) else []


def fmp_balance_sheet(symbol, n=3):
    url = (f"{FMP_BASE}/balance-sheet-statement?symbol={symbol}"
           f"&limit={n}&apikey={FMP_KEY}")
    d = http_json(url)
    return d if isinstance(d, list) else []


def fmp_cash_flow(symbol, n=3):
    url = (f"{FMP_BASE}/cash-flow-statement?symbol={symbol}"
           f"&limit={n}&apikey={FMP_KEY}")
    d = http_json(url)
    return d if isinstance(d, list) else []


def safe_ratio(num, den, default=None, cap_abs=10.0):
    """Compute num/den with guardrails."""
    try:
        if den is None or num is None:
            return default
        d = float(den)
        if d == 0:
            return default
        r = float(num) / d
        # Cap to prevent garbage from dominating composite
        return max(-cap_abs, min(cap_abs, r))
    except (TypeError, ValueError):
        return default


def compute_beneish(income_list, bs_list, cf_list):
    """Compute Beneish M-Score given 2 most recent yearly statements.
    Returns {'m_score': float, 'components': {...}, 'verdict': str}
    or None if insufficient data.
    """
    if (len(income_list) < 2 or len(bs_list) < 2 or len(cf_list) < 1):
        return None
    # FMP returns most recent first
    t = 0    # current year (t)
    t_1 = 1  # prior year (t-1)

    i_t = income_list[t]
    i_1 = income_list[t_1]
    b_t = bs_list[t]
    b_1 = bs_list[t_1]
    cf_t = cf_list[t]

    def g(d, k):
        v = d.get(k)
        try: return float(v) if v is not None else None
        except (TypeError, ValueError): return None

    # Get raw values
    sales_t = g(i_t, "revenue")
    sales_1 = g(i_1, "revenue")
    cogs_t = g(i_t, "costOfRevenue")
    cogs_1 = g(i_1, "costOfRevenue")
    sga_t = g(i_t, "sellingGeneralAndAdministrativeExpenses")
    sga_1 = g(i_1, "sellingGeneralAndAdministrativeExpenses")
    dep_t = g(i_t, "depreciationAndAmortization")
    dep_1 = g(i_1, "depreciationAndAmortization")
    ni_t = g(i_t, "netIncome")

    ar_t = g(b_t, "netReceivables")
    ar_1 = g(b_1, "netReceivables")
    ppe_t = g(b_t, "propertyPlantEquipmentNet")
    ppe_1 = g(b_1, "propertyPlantEquipmentNet")
    ta_t = g(b_t, "totalAssets")
    ta_1 = g(b_1, "totalAssets")
    ca_t = g(b_t, "totalCurrentAssets")
    ca_1 = g(b_1, "totalCurrentAssets")
    cl_t = g(b_t, "totalCurrentLiabilities")
    cl_1 = g(b_1, "totalCurrentLiabilities")
    ltd_t = g(b_t, "longTermDebt")
    ltd_1 = g(b_1, "longTermDebt")

    cfo_t = g(cf_t, "operatingCashFlow")

    # Validate critical inputs
    if any(x is None or x == 0 for x in
           [sales_t, sales_1, ta_t, ta_1]):
        return None

    components = {}

    # 1. DSRI (Days Sales Receivable Index)
    if ar_t is not None and ar_1 is not None and ar_1 != 0 and sales_1 != 0:
        components["dsri"] = safe_ratio(
            ar_t / sales_t if sales_t else 0,
            ar_1 / sales_1 if sales_1 else 0,
            default=1.0)
    else:
        components["dsri"] = 1.0

    # 2. GMI (Gross Margin Index): GM_t-1 / GM_t (deterioration > 1)
    gm_t = ((sales_t - cogs_t) / sales_t) if (cogs_t is not None and sales_t) else None
    gm_1 = ((sales_1 - cogs_1) / sales_1) if (cogs_1 is not None and sales_1) else None
    if gm_t is not None and gm_1 is not None and gm_t != 0:
        components["gmi"] = safe_ratio(gm_1, gm_t, default=1.0)
    else:
        components["gmi"] = 1.0

    # 3. AQI (Asset Quality Index): non-PPE, non-current asset ratio increasing?
    if ppe_t is not None and ppe_1 is not None and ca_t is not None and ca_1 is not None:
        aq_t = 1 - (ca_t + ppe_t) / ta_t
        aq_1 = 1 - (ca_1 + ppe_1) / ta_1
        components["aqi"] = safe_ratio(aq_t, aq_1, default=1.0)
    else:
        components["aqi"] = 1.0

    # 4. SGI (Sales Growth Index)
    components["sgi"] = safe_ratio(sales_t, sales_1, default=1.0)

    # 5. DEPI (Depreciation Index): dep rate t-1 / dep rate t (slowing = >1)
    if (dep_t is not None and dep_1 is not None and
        ppe_t is not None and ppe_1 is not None and
        (dep_t + ppe_t) > 0 and (dep_1 + ppe_1) > 0):
        dep_rate_t = dep_t / (dep_t + ppe_t)
        dep_rate_1 = dep_1 / (dep_1 + ppe_1)
        if dep_rate_t > 0:
            components["depi"] = safe_ratio(dep_rate_1, dep_rate_t, default=1.0)
        else:
            components["depi"] = 1.0
    else:
        components["depi"] = 1.0

    # 6. SGAI (SG&A Index)
    if (sga_t is not None and sga_1 is not None and
        sga_1 > 0 and sales_1 > 0 and sales_t > 0):
        components["sgai"] = safe_ratio(
            sga_t / sales_t,
            sga_1 / sales_1,
            default=1.0)
    else:
        components["sgai"] = 1.0

    # 7. TATA (Total Accruals to Total Assets)
    if (ni_t is not None and cfo_t is not None and ta_t > 0):
        components["tata"] = safe_ratio(ni_t - cfo_t, ta_t, default=0.0)
    else:
        components["tata"] = 0.0

    # 8. LVGI (Leverage Index): total debt to assets ratio change
    debt_t = (ltd_t or 0) + (cl_t or 0)
    debt_1 = (ltd_1 or 0) + (cl_1 or 0)
    if debt_t > 0 and debt_1 > 0 and ta_t > 0 and ta_1 > 0:
        components["lvgi"] = safe_ratio(debt_t / ta_t, debt_1 / ta_1,
                                         default=1.0)
    else:
        components["lvgi"] = 1.0

    # Beneish formula
    m = (
        -4.84
        + 0.920 * components["dsri"]
        + 0.528 * components["gmi"]
        + 0.404 * components["aqi"]
        + 0.892 * components["sgi"]
        + 0.115 * components["depi"]
        - 0.172 * components["sgai"]
        + 4.679 * components["tata"]
        - 0.327 * components["lvgi"]
    )
    m = round(m, 3)

    # Verdict bands
    if m > -1.78:
        verdict = "LIKELY_MANIPULATOR"
    elif m < -2.22:
        verdict = "UNLIKELY_MANIPULATOR"
    else:
        verdict = "WATCH_LIST"

    return {"m_score": m, "components": {k: round(v, 3) for k, v in
                                          components.items()},
            "verdict": verdict,
            "fiscal_year_t": i_t.get("calendarYear") or
                              i_t.get("date", "")[:4],
            "fiscal_year_t_minus_1": i_1.get("calendarYear") or
                                       i_1.get("date", "")[:4]}


def analyze_ticker(symbol):
    """Fetch 3 statements + compute M-Score."""
    income = fmp_income_statement(symbol, n=3)
    time.sleep(FMP_SLEEP_SEC)
    bs = fmp_balance_sheet(symbol, n=3)
    time.sleep(FMP_SLEEP_SEC)
    cf = fmp_cash_flow(symbol, n=3)
    time.sleep(FMP_SLEEP_SEC)
    return compute_beneish(income, bs, cf)


def telegram_notify(msg):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        return
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        data = urllib.parse.urlencode({
            "chat_id": TELEGRAM_CHAT_ID,
            "text": msg,
            "parse_mode": "Markdown",
        }).encode("utf-8")
        urllib.request.urlopen(urllib.request.Request(url, data=data),
                               timeout=10)
    except Exception:
        pass


def lambda_handler(event, context):
    started = datetime.now(timezone.utc)
    log = []

    if not FMP_KEY:
        return {"statusCode": 500,
                "body": json.dumps({"ok": False, "error": "FMP_KEY not set"})}

    per_ticker = []
    for i, t in enumerate(STATIC_TOP50_SPX[:UNIVERSE_TOP_N]):
        sym = t["symbol"]
        try:
            res = analyze_ticker(sym)
            if res is not None:
                res["ticker"] = sym
                res["sector"] = t.get("sector", "")
                per_ticker.append(res)
        except Exception as e:
            log.append(f"err {sym}: {str(e)[:80]}")
        if i % 10 == 9:
            log.append(f"progress: {i+1}/{UNIVERSE_TOP_N}")

    if not per_ticker:
        return {"statusCode": 500,
                "body": json.dumps({"ok": False,
                                    "error": "no tickers analyzed",
                                    "log": log[-5:]})}

    # Classify + aggregate
    m_scores = [t["m_score"] for t in per_ticker]
    n_red = sum(1 for t in per_ticker
                if t["verdict"] == "LIKELY_MANIPULATOR")
    n_watch = sum(1 for t in per_ticker if t["verdict"] == "WATCH_LIST")
    n_clean = sum(1 for t in per_ticker
                  if t["verdict"] == "UNLIKELY_MANIPULATOR")

    red_flags = sorted(
        [t for t in per_ticker if t["verdict"] == "LIKELY_MANIPULATOR"],
        key=lambda x: x["m_score"], reverse=True)
    cleanest = sorted(per_ticker, key=lambda x: x["m_score"])[:10]

    median_m = round(statistics.median(m_scores), 3)
    max_m = max(m_scores)
    min_m = min(m_scores)

    # Universe state classification
    if n_red >= 5:
        universe_state = "ELEVATED_FRAUD_RISK"
    elif n_red >= 2:
        universe_state = "MODERATE_FRAUD_RISK"
    else:
        universe_state = "LOW_FRAUD_RISK"

    # Sector breakdown of red flags
    sector_red = {}
    for t in red_flags:
        sec = t.get("sector", "Unknown") or "Unknown"
        sector_red[sec] = sector_red.get(sec, 0) + 1

    out = {
        "ok": True,
        "version": VERSION,
        "generated_at": started.isoformat(),
        "universe_state": universe_state,
        "n_universe_analyzed": len(per_ticker),
        "n_likely_manipulator": n_red,
        "n_watch_list": n_watch,
        "n_unlikely_manipulator": n_clean,
        "median_m_score": median_m,
        "max_m_score": max_m,
        "min_m_score": min_m,
        "sector_breakdown_red_flags": sector_red,
        "red_flags": [
            {"ticker": t["ticker"], "sector": t.get("sector"),
             "m_score": t["m_score"], "verdict": t["verdict"],
             "components": t["components"],
             "fiscal_year_t": t.get("fiscal_year_t")}
            for t in red_flags
        ],
        "cleanest_10": [
            {"ticker": t["ticker"], "sector": t.get("sector"),
             "m_score": t["m_score"], "verdict": t["verdict"],
             "components": t["components"],
             "fiscal_year_t": t.get("fiscal_year_t")}
            for t in cleanest
        ],
        "all_tickers": [
            {"ticker": t["ticker"], "sector": t.get("sector"),
             "m_score": t["m_score"], "verdict": t["verdict"],
             "fiscal_year_t": t.get("fiscal_year_t")}
            for t in sorted(per_ticker, key=lambda x: x["m_score"],
                            reverse=True)
        ],
        "verdict_bands": {
            "LIKELY_MANIPULATOR": "M > -1.78 (high red-flag risk)",
            "WATCH_LIST":         "-2.22 <= M <= -1.78 (ambiguous)",
            "UNLIKELY_MANIPULATOR": "M < -2.22 (clean)",
        },
        "universe_state_bands": {
            "ELEVATED_FRAUD_RISK": "5+ red flags in top-50 SPX",
            "MODERATE_FRAUD_RISK": "2-4 red flags",
            "LOW_FRAUD_RISK": "0-1 red flags",
        },
        "methodology": {
            "formula": ("M = -4.84 + 0.92*DSRI + 0.528*GMI + 0.404*AQI "
                        "+ 0.892*SGI + 0.115*DEPI - 0.172*SGAI "
                        "+ 4.679*TATA - 0.327*LVGI"),
            "components_explained": {
                "DSRI": "Days Sales Receivable Index - channel-stuffing red flag",
                "GMI":  "Gross Margin Index - margin deterioration incentive",
                "AQI":  "Asset Quality Index - capitalizing expenses",
                "SGI":  "Sales Growth Index - growth pressure incentive",
                "DEPI": "Depreciation Index - extending asset life",
                "SGAI": "SG&A Index - operating leverage manipulation",
                "TATA": "Total Accruals to Total Assets - cash/accrual divergence",
                "LVGI": "Leverage Index - debt-covenant pressure",
            },
            "academic_source": ("Beneish M (1999). 'The Detection of "
                                 "Earnings Manipulation.' Financial Analysts "
                                 "Journal, 55(5), 24-36."),
            "universe": f"S&P 500 top-{UNIVERSE_TOP_N} by market cap",
        },
        "sources": {
            "income_statement": "FMP /stable/income-statement",
            "balance_sheet": "FMP /stable/balance-sheet-statement",
            "cash_flow": "FMP /stable/cash-flow-statement",
            "data_provider": "Financial Modeling Prep (audited 10-K data)",
        },
        "edge_basis": ("Beneish (1999) showed M-Score classified 76% of "
                       "actual SEC-charged manipulators correctly. Most "
                       "famously flagged Enron in 1998-1999, 2 years before "
                       "the 2001 collapse. Now a GuruFocus and Bloomberg "
                       "standard quality screen."),
        "log_summary": log[-5:],
    }

    try:
        s3 = boto3.client("s3")
        s3.put_object(
            Bucket=S3_BUCKET, Key=S3_KEY,
            Body=json.dumps(out, default=str).encode("utf-8"),
            ContentType="application/json",
            CacheControl="public, max-age=300",
        )
    except Exception as e:
        return {"statusCode": 500,
                "body": json.dumps({"ok": False,
                                    "error": f"s3 put failed: {str(e)[:200]}"})}

    if universe_state == "ELEVATED_FRAUD_RISK":
        top_3 = red_flags[:3]
        names = ", ".join(f"{t['ticker']}(M={t['m_score']})" for t in top_3)
        telegram_notify(
            f"🚩 *Beneish ELEVATED_FRAUD_RISK*\n"
            f"{n_red} red flags in top-50 SPX\n"
            f"Top concerns: {names}\n"
            f"justhodl.ai/beneish.html"
        )

    return {
        "statusCode": 200,
        "body": json.dumps({
            "ok": True,
            "version": VERSION,
            "universe_state": universe_state,
            "n_analyzed": len(per_ticker),
            "n_red_flags": n_red,
            "n_watch": n_watch,
            "n_clean": n_clean,
            "median_m": median_m,
            "top_3_red_flags": [
                {"t": t["ticker"], "m": t["m_score"]}
                for t in red_flags[:3]
            ],
        }),
    }


if __name__ == "__main__":
    r = lambda_handler({}, None)
    print(json.dumps(r, indent=2))
