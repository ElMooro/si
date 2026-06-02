"""
justhodl-equity-research (v1.0.1)
════════════════════════
Institutional-grade equity research desk. Given a ticker, produces a
full research paper with the same coverage a hedge fund analyst would
generate as homework before committing capital.

INVOKED via Lambda URL: ?ticker=ORCL  (or POST with {"ticker":"ORCL"})
Cache: results stored in S3 at equity-research/<TICKER>.json for 24h.

DATA FETCHED (in parallel)
══════════════════════════
FMP /stable/ endpoints:
  - profile                      → company description, sector, mkt cap
  - quote                        → current price, day range, volume
  - income-statement (20y annual + 8q quarterly)
  - balance-sheet-statement (20y annual)
  - cash-flow-statement (20y annual)
  - ratios (15y annual)          → P/E, P/B, ROE, ROA, ROIC, margins
  - ratios-ttm                   → current TTM ratios
  - key-metrics (15y annual)     → market cap, EV, FCF yield, debt/equity
  - key-metrics-ttm              → current
  - financial-growth (10y)       → revenue, EPS, FCF growth history
  - analyst-estimates            → forward EPS/revenue projections
  - price-target-consensus       → analyst consensus PT
  - dcf                          → FMP's DCF estimate
  - financial-scores             → Piotroski, Altman Z
  - peers                        → peer tickers
  - historical-price-eod (10y)   → returns, volatility, max drawdown

DERIVED ANALYSIS
════════════════
  - 20-year revenue CAGR, EPS CAGR, FCF CAGR
  - Margin trend (gross/operating/net over time)
  - Balance sheet quality (debt/equity trend, current ratio, working capital)
  - Cash flow quality (CFO vs net income, FCF conversion)
  - Earnings consistency (quarters of consecutive growth, beat rate)
  - Industry P/E comparison (peer average vs ticker)
  - Drawdown history (max DD, avg DD recovery time)
  - Buyback + dividend history (capital return)

CLAUDE SYNTHESIS
════════════════
After all data is gathered + summarized, Claude produces:
  - Executive summary (3-4 sentences institutional voice)
  - Bull case (investment thesis with 4-5 specific drivers)
  - Bear case (risks with 4-5 specific concerns)
  - Valuation assessment (DCF gap, multiples vs peers vs history)
  - Financial health (5-pillar score: profitability/growth/leverage/
                                 liquidity/quality)
  - Final verdict (BUY/HOLD/SELL with conviction grade + 12-month PT)
  - Key catalysts (next 12 months)
  - Invalidation triggers (what would change the thesis)

OUTPUT (JSON)
═════════════
{
  "ticker", "generated_at", "from_cache",
  "company": {name, sector, industry, country, exchange, ceo, employees,
              description, market_cap, ipo_date},
  "quote": {price, change_pct, volume, day_range, year_range},
  "verdict": {rating: BUY|HOLD|SELL, conviction_grade: A|B|C|D,
              price_target_12m, upside_pct, confidence_pct},
  "executive_summary": "...",
  "thesis": {bull_case: {...}, bear_case: {...}},
  "valuation": {pe_ratio, pe_industry, pe_5yr_avg, peg, dcf_estimate,
                dcf_upside_pct, ev_ebitda, p_b, fcf_yield, peer_table},
  "financial_health": {pillars, overall_score, altman_z, piotroski},
  "growth": {revenue_5yr_cagr, revenue_10yr_cagr, eps_5yr_cagr,
             eps_10yr_cagr, fcf_5yr_cagr, recent_quarters},
  "statements": {income_annual[20], balance_annual[20],
                 cashflow_annual[20], income_quarterly[8]},
  "margins": {gross_trend[], operating_trend[], net_trend[]},
  "returns": {ytd, 1yr, 3yr_cagr, 5yr_cagr, 10yr_cagr, max_drawdown_pct},
  "analyst": {pt_consensus, n_analysts, estimate_eps_fwd},
  "catalysts_12m": [...],
  "invalidation_triggers": [...],
  "metadata": {data_freshness, sources, elapsed_sec, claude_model}
}
"""

import json
import os
import sys
import time
import traceback
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import boto3

# ═════════════════════════════════════════════════════════════════════
# Config
# ═════════════════════════════════════════════════════════════════════

FMP_KEY      = os.environ.get("FMP_KEY", "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb")
FMP_BASE     = "https://financialmodelingprep.com/stable"
ANTHROPIC_KEY = os.environ.get("ANTHROPIC_API_KEY") or os.environ.get("ANTHROPIC_KEY", "")
MODEL        = os.environ.get("CLAUDE_MODEL", "claude-haiku-4-5-20251001")
S3_BUCKET    = "justhodl-dashboard-live"
CACHE_PREFIX = "equity-research/"
CACHE_TTL    = 24 * 3600   # 24h cache (statements don't change daily)
FETCH_TIMEOUT = 20         # FMP per-call timeout
CLAUDE_TIMEOUT = 90

s3 = boto3.client("s3", region_name="us-east-1")


# ═════════════════════════════════════════════════════════════════════
# HTTP helpers
# ═════════════════════════════════════════════════════════════════════

def fmp_get(endpoint: str, **params) -> Optional[Any]:
    """Call FMP /stable/{endpoint} with apikey + params."""
    q = dict(params)
    q["apikey"] = FMP_KEY
    qs = urllib.parse.urlencode(q)
    url = f"{FMP_BASE}/{endpoint}?{qs}"
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "JustHodlEquityResearch/1.0"})
        with urllib.request.urlopen(req, timeout=FETCH_TIMEOUT) as r:
            return json.loads(r.read())
    except urllib.error.HTTPError as e:
        if e.code in (401, 403):
            print(f"[fmp_get] {endpoint} → {e.code} (entitlement)")
        else:
            print(f"[fmp_get] {endpoint} → HTTP {e.code}")
    except Exception as e:
        print(f"[fmp_get] {endpoint} → {type(e).__name__}: {str(e)[:120]}")
    return None


def claude_call(system: str, user: str, max_tokens: int = 6000) -> str:
    """Single-message call to Anthropic API."""
    if not ANTHROPIC_KEY:
        raise RuntimeError("ANTHROPIC_API_KEY not set")
    payload = json.dumps({
        "model": MODEL,
        "max_tokens": max_tokens,
        "system": system,
        "messages": [{"role": "user", "content": user}],
    }).encode("utf-8")
    req = urllib.request.Request(
        "https://api.anthropic.com/v1/messages", data=payload,
        headers={
            "Content-Type": "application/json",
            "x-api-key": ANTHROPIC_KEY,
            "anthropic-version": "2023-06-01",
        },
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=CLAUDE_TIMEOUT) as r:
        data = json.loads(r.read())
    if not data.get("content"):
        raise RuntimeError(f"Empty Claude response: {data}")
    return "".join(b.get("text", "") for b in data["content"] if b.get("type") == "text").strip()


# ═════════════════════════════════════════════════════════════════════
# Data fetching — all FMP endpoints in parallel
# ═════════════════════════════════════════════════════════════════════

def fetch_all(ticker: str) -> Dict[str, Any]:
    """Pull all FMP data points in parallel."""
    fetches = {
        "profile":          ("profile", {"symbol": ticker}),
        "quote":            ("quote",   {"symbol": ticker}),
        "income_annual":    ("income-statement", {"symbol": ticker, "period": "annual", "limit": 20}),
        "income_quarterly": ("income-statement", {"symbol": ticker, "period": "quarter", "limit": 8}),
        "balance_annual":   ("balance-sheet-statement", {"symbol": ticker, "period": "annual", "limit": 20}),
        "cashflow_annual":  ("cash-flow-statement", {"symbol": ticker, "period": "annual", "limit": 20}),
        "ratios_annual":    ("ratios", {"symbol": ticker, "period": "annual", "limit": 15}),
        "ratios_ttm":       ("ratios-ttm", {"symbol": ticker}),
        "key_metrics":      ("key-metrics", {"symbol": ticker, "period": "annual", "limit": 15}),
        "key_metrics_ttm":  ("key-metrics-ttm", {"symbol": ticker}),
        "growth":           ("financial-growth", {"symbol": ticker, "period": "annual", "limit": 10}),
        "estimates":        ("analyst-estimates", {"symbol": ticker, "period": "annual", "limit": 5}),
        "pt_consensus":     ("price-target-consensus", {"symbol": ticker}),
        "dcf":              ("discounted-cash-flow", {"symbol": ticker}),
        "scores":           ("financial-scores", {"symbol": ticker}),
        "peers":            ("stock-peers", {"symbol": ticker}),
        "earnings":         ("earnings", {"symbol": ticker, "limit": 12}),
        "ownership":        ("acquisition-of-beneficial-ownership", {"symbol": ticker}),
        "transcript_dates": ("earning-call-transcript-dates", {"symbol": ticker}),
        "prices_eod":       ("historical-price-eod/light",
                              {"symbol": ticker, "from": _date_n_years_ago(10)}),
        "dividends":        ("dividends", {"symbol": ticker, "limit": 20}),
    }

    results: Dict[str, Any] = {}
    with ThreadPoolExecutor(max_workers=10) as ex:
        futures = {ex.submit(fmp_get, ep, **params): name
                   for name, (ep, params) in fetches.items()}
        for fut in as_completed(futures):
            name = futures[fut]
            try:
                results[name] = fut.result()
            except Exception as e:
                print(f"[fetch_all] {name} crashed: {e}")
                results[name] = None
    return results


def _date_n_years_ago(n: int) -> str:
    from datetime import timedelta
    return (datetime.now(timezone.utc) - timedelta(days=365 * n)).strftime("%Y-%m-%d")


# ═════════════════════════════════════════════════════════════════════
# Derived analytics
# ═════════════════════════════════════════════════════════════════════

def _safe_num(d: dict, key: str, default=None):
    """Get a numeric field, returning default if missing/None/zero-string."""
    if not isinstance(d, dict):
        return default
    v = d.get(key)
    if v is None or v == "":
        return default
    try:
        return float(v)
    except (TypeError, ValueError):
        return default


def _first(maybe_list):
    """FMP often returns a list with one item; unwrap or return None."""
    if isinstance(maybe_list, list) and maybe_list:
        return maybe_list[0]
    if isinstance(maybe_list, dict):
        return maybe_list
    return None


def cagr(end_val: float, start_val: float, n_years: int) -> Optional[float]:
    if start_val is None or end_val is None or start_val <= 0 or n_years <= 0:
        return None
    try:
        return (pow(end_val / start_val, 1 / n_years) - 1) * 100
    except (ValueError, ZeroDivisionError):
        return None


def compute_growth(income_annual: list) -> dict:
    """Compute multi-period CAGRs from annual income statements."""
    if not isinstance(income_annual, list) or len(income_annual) < 2:
        return {}
    # FMP returns most-recent first
    rev_field = "revenue"
    eps_field = "epsDiluted"
    ni_field = "netIncome"

    def cagr_for(field, n):
        if len(income_annual) < n + 1:
            return None
        end = _safe_num(income_annual[0], field)
        start = _safe_num(income_annual[n], field)
        return cagr(end, start, n)

    return {
        "revenue_3yr_cagr":  cagr_for(rev_field, 3),
        "revenue_5yr_cagr":  cagr_for(rev_field, 5),
        "revenue_10yr_cagr": cagr_for(rev_field, 10),
        "eps_3yr_cagr":      cagr_for(eps_field, 3),
        "eps_5yr_cagr":      cagr_for(eps_field, 5),
        "eps_10yr_cagr":     cagr_for(eps_field, 10),
        "ni_5yr_cagr":       cagr_for(ni_field, 5),
        "ni_10yr_cagr":      cagr_for(ni_field, 10),
    }


def compute_fcf_cagr(cf_annual: list) -> dict:
    if not isinstance(cf_annual, list) or len(cf_annual) < 2:
        return {}
    def cagr_for_fcf(n):
        if len(cf_annual) < n + 1:
            return None
        end = _safe_num(cf_annual[0], "freeCashFlow")
        start = _safe_num(cf_annual[n], "freeCashFlow")
        return cagr(end, start, n)
    return {
        "fcf_3yr_cagr":  cagr_for_fcf(3),
        "fcf_5yr_cagr":  cagr_for_fcf(5),
        "fcf_10yr_cagr": cagr_for_fcf(10),
    }


def compute_margin_trend(income_annual: list, n: int = 10) -> dict:
    """Pull margin time-series for the last n years."""
    if not isinstance(income_annual, list):
        return {"gross_trend": [], "operating_trend": [], "net_trend": []}
    rows = income_annual[:n]
    return {
        "gross_trend":     [{"date": r.get("date"), "value": _pct(_safe_num(r, "grossProfitRatio"))}
                              for r in rows if r.get("date")],
        "operating_trend": [{"date": r.get("date"), "value": _pct(_safe_num(r, "operatingIncomeRatio"))}
                              for r in rows if r.get("date")],
        "net_trend":       [{"date": r.get("date"), "value": _pct(_safe_num(r, "netIncomeRatio"))}
                              for r in rows if r.get("date")],
    }


def _pct(x):
    """Convert FMP's ratio (0.45 = 45%) to percent."""
    if x is None: return None
    return round(x * 100, 2)


def compute_quarterly_consistency(quarterly: list) -> dict:
    """Count consecutive YoY-growth quarters + recent revenue surprises."""
    if not isinstance(quarterly, list) or len(quarterly) < 5:
        return {"consecutive_yoy_growth": 0, "recent_quarters": []}
    # Sort newest first (FMP default) — compute YoY by stepping 4 quarters
    consec = 0
    rows = quarterly[:8]
    recent = []
    for i, q in enumerate(rows):
        rev = _safe_num(q, "revenue")
        eps = _safe_num(q, "epsDiluted")
        date = q.get("date")
        yoy = None
        if len(quarterly) > i + 4:
            prev_rev = _safe_num(quarterly[i + 4], "revenue")
            if prev_rev and rev:
                yoy = round((rev / prev_rev - 1) * 100, 2)
        recent.append({"date": date, "revenue": rev, "eps_diluted": eps, "yoy_rev_growth": yoy})
        if yoy is not None and yoy > 0 and i == consec:
            consec += 1
    return {"consecutive_yoy_growth": consec, "recent_quarters": recent}


def compute_returns(prices_eod: list, current_price: Optional[float]) -> dict:
    """From EOD prices, compute YTD/1y/3y/5y returns + max drawdown."""
    if not isinstance(prices_eod, list) or len(prices_eod) < 60 or current_price is None:
        return {}
    # FMP returns descending by date. Compute returns from oldest to newest.
    prices = sorted(prices_eod, key=lambda p: p.get("date") or "")
    if not prices:
        return {}

    def price_n_days_ago(n: int) -> Optional[float]:
        if n >= len(prices):
            return _safe_num(prices[0], "price") or _safe_num(prices[0], "close")
        p = prices[-1 - n]
        return _safe_num(p, "price") or _safe_num(p, "close")

    def ret(prev: Optional[float]) -> Optional[float]:
        if prev is None or prev <= 0: return None
        return round((current_price / prev - 1) * 100, 2)

    # YTD: find first price of current year
    now_year = datetime.now(timezone.utc).year
    ytd_start = None
    for p in prices:
        d = p.get("date") or ""
        if d.startswith(str(now_year)):
            ytd_start = _safe_num(p, "price") or _safe_num(p, "close")
            break

    # Max drawdown over the 10y window
    closes = [(_safe_num(p, "price") or _safe_num(p, "close")) for p in prices]
    closes = [c for c in closes if c is not None and c > 0]
    max_dd = 0.0
    peak = 0
    if closes:
        peak = closes[0]
        for c in closes:
            if c > peak: peak = c
            dd = (peak - c) / peak * 100 if peak > 0 else 0
            if dd > max_dd: max_dd = dd

    return {
        "ytd_pct":        ret(ytd_start) if ytd_start else None,
        "1yr_pct":        ret(price_n_days_ago(252)),
        "3yr_cagr_pct":   _to_cagr(current_price, price_n_days_ago(252 * 3), 3),
        "5yr_cagr_pct":   _to_cagr(current_price, price_n_days_ago(252 * 5), 5),
        "10yr_cagr_pct":  _to_cagr(current_price, price_n_days_ago(min(252 * 10, len(prices) - 1)), 10),
        "max_drawdown_pct": round(max_dd, 2),
    }


def _to_cagr(end, start, n):
    if not start or start <= 0 or not end or end <= 0:
        return None
    try:
        return round((pow(end / start, 1 / n) - 1) * 100, 2)
    except (ValueError, ZeroDivisionError):
        return None


def compute_balance_quality(balance_annual: list) -> dict:
    """Working capital, current ratio, debt/equity trend."""
    if not isinstance(balance_annual, list) or not balance_annual:
        return {}
    latest = balance_annual[0]
    debt = _safe_num(latest, "totalDebt", 0) or 0
    eq   = _safe_num(latest, "totalEquity", 0) or _safe_num(latest, "totalStockholdersEquity", 0) or 0
    ca   = _safe_num(latest, "totalCurrentAssets")
    cl   = _safe_num(latest, "totalCurrentLiabilities")
    return {
        "total_debt":            debt,
        "total_equity":          eq,
        "debt_to_equity":        round(debt / eq, 2) if eq else None,
        "current_ratio":         round(ca / cl, 2) if (ca and cl) else None,
        "working_capital":       round((ca or 0) - (cl or 0), 0) if (ca and cl) else None,
        "cash_and_st_inv":       _safe_num(latest, "cashAndShortTermInvestments"),
    }


def compute_cf_quality(income_annual: list, cf_annual: list) -> dict:
    """CFO/NI ratio (cash quality) and FCF conversion."""
    if not (isinstance(income_annual, list) and isinstance(cf_annual, list)
            and income_annual and cf_annual):
        return {}
    latest_ni  = _safe_num(income_annual[0], "netIncome")
    latest_cfo = _safe_num(cf_annual[0], "operatingCashFlow") or _safe_num(cf_annual[0], "netCashProvidedByOperatingActivities")
    latest_fcf = _safe_num(cf_annual[0], "freeCashFlow")
    return {
        "cfo_to_ni":            round(latest_cfo / latest_ni, 2) if (latest_cfo and latest_ni) else None,
        "fcf_conversion_pct":   round(latest_fcf / latest_ni * 100, 1) if (latest_fcf and latest_ni) else None,
        "latest_cfo":           latest_cfo,
        "latest_fcf":           latest_fcf,
    }


def compute_valuation(profile: dict, ratios_ttm: dict, key_ttm: dict,
                       ratios_annual: list, dcf: dict, pt_consensus: dict,
                       quote: dict) -> dict:
    """Pull all the valuation metrics into one section."""
    # FMP /stable/ratios-ttm field names (verified by ops 1139):
    #   priceToEarningsRatioTTM, priceToBookRatioTTM, priceToSalesRatioTTM,
    #   priceToFreeCashFlowRatioTTM (NO 's'), enterpriseValueMultipleTTM,
    #   operatingProfitMarginTTM. priceEarningsToGrowthRatioTTM doesn't exist —
    #   it's priceToEarningsGrowthRatioTTM.
    # ROE/ROIC live in /stable/key-metrics-ttm not ratios-ttm.
    pe_ttm     = (_safe_num(ratios_ttm, "priceToEarningsRatioTTM")
                  or _safe_num(ratios_ttm, "peRatioTTM"))
    pb_ttm     = _safe_num(ratios_ttm, "priceToBookRatioTTM")
    ps_ttm     = _safe_num(ratios_ttm, "priceToSalesRatioTTM")
    pfcf_ttm   = (_safe_num(ratios_ttm, "priceToFreeCashFlowRatioTTM")
                  or _safe_num(ratios_ttm, "priceToFreeCashFlowsRatioTTM"))
    ev_ebitda  = (_safe_num(ratios_ttm, "enterpriseValueMultipleTTM")
                  or _safe_num(key_ttm,  "evToEBITDATTM")
                  or _safe_num(key_ttm,  "enterpriseValueOverEBITDATTM"))
    fcf_yield  = _safe_num(key_ttm, "freeCashFlowYieldTTM")
    div_yield  = (_safe_num(ratios_ttm, "dividendYieldTTM")
                  or _safe_num(ratios_ttm, "dividendYieldPercentageTTM")
                  or _safe_num(key_ttm, "dividendYieldTTM"))
    peg        = (_safe_num(ratios_ttm, "priceToEarningsGrowthRatioTTM")
                  or _safe_num(ratios_ttm, "priceEarningsToGrowthRatioTTM"))
    roe        = _safe_num(key_ttm, "returnOnEquityTTM")
    roic       = _safe_num(key_ttm, "returnOnInvestedCapitalTTM")

    # 5yr avg PE from annual ratios
    pe_5yr = None
    if isinstance(ratios_annual, list) and ratios_annual:
        pes = [_safe_num(r, "priceToEarningsRatio") or _safe_num(r, "priceEarningsRatio")
               for r in ratios_annual[:5]]
        pes = [p for p in pes if p is not None and 0 < p < 200]
        if pes:
            pe_5yr = round(sum(pes) / len(pes), 1)

    # DCF
    dcf_obj = _first(dcf) or {}
    dcf_val = _safe_num(dcf_obj, "dcf") or _safe_num(dcf_obj, "Dcf")
    current_px = _safe_num(quote, "price") or _safe_num(profile, "price")
    dcf_upside = None
    if dcf_val and current_px and current_px > 0:
        dcf_upside = round((dcf_val / current_px - 1) * 100, 1)

    # Analyst PT
    pt_obj = _first(pt_consensus) or {}
    pt_median = _safe_num(pt_obj, "targetMedian") or _safe_num(pt_obj, "targetConsensus")
    pt_high   = _safe_num(pt_obj, "targetHigh")
    pt_low    = _safe_num(pt_obj, "targetLow")
    pt_upside = None
    if pt_median and current_px and current_px > 0:
        pt_upside = round((pt_median / current_px - 1) * 100, 1)

    return {
        "pe_ttm":            round(pe_ttm, 2) if pe_ttm else None,
        "pe_5yr_avg":        pe_5yr,
        "pb_ttm":            round(pb_ttm, 2) if pb_ttm else None,
        "ps_ttm":            round(ps_ttm, 2) if ps_ttm else None,
        "pfcf_ttm":          round(pfcf_ttm, 2) if pfcf_ttm else None,
        "ev_ebitda":         round(ev_ebitda, 2) if ev_ebitda else None,
        "peg_ratio":         round(peg, 2) if peg else None,
        "fcf_yield_pct":     round(fcf_yield * 100, 2) if fcf_yield else None,
        "div_yield_pct":     round(div_yield * 100, 2) if div_yield else None,
        "roe_ttm_pct":       _pct(roe),
        "roic_ttm_pct":      _pct(roic),
        "dcf_estimate":      round(dcf_val, 2) if dcf_val else None,
        "dcf_upside_pct":    dcf_upside,
        "analyst_pt_median": pt_median,
        "analyst_pt_high":   pt_high,
        "analyst_pt_low":    pt_low,
        "analyst_pt_upside_pct": pt_upside,
    }


def compute_financial_health(scores: list, ratios_ttm: dict, key_ttm: dict,
                              balance_qual: dict, cf_qual: dict,
                              growth: dict) -> dict:
    """5-pillar health score: profitability/growth/leverage/liquidity/quality."""
    scores_obj = _first(scores) or {}

    altman_z   = _safe_num(scores_obj, "altmanZScore")
    piotroski  = _safe_num(scores_obj, "piotroskiScore")

    # Pillar grades (each 0-100)
    pillars: Dict[str, Any] = {}

    # 1. Profitability — ROE > 15, margin > 10
    # ROE lives in key-metrics-ttm not ratios-ttm (FMP).
    roe = _safe_num(key_ttm, "returnOnEquityTTM") or _safe_num(ratios_ttm, "returnOnEquityTTM") or 0
    op_margin = _safe_num(ratios_ttm, "operatingProfitMarginTTM") or 0
    prof_score = min(100, (roe * 100 * 3 + op_margin * 100 * 3) / 2)  # cap at 100
    pillars["profitability"] = {
        "score": round(max(0, min(100, prof_score)), 0),
        "roe_pct":        _pct(roe),
        "op_margin_pct":  _pct(op_margin),
    }

    # 2. Growth — 5yr revenue + EPS CAGR. Score = (rev_cagr + eps_cagr) * 4
    rev5 = growth.get("revenue_5yr_cagr") or 0
    eps5 = growth.get("eps_5yr_cagr") or 0
    growth_score = (rev5 + eps5) * 3
    pillars["growth"] = {
        "score": round(max(0, min(100, growth_score)), 0),
        "rev_5y_cagr_pct": rev5,
        "eps_5y_cagr_pct": eps5,
    }

    # 3. Leverage — debt/equity. Score = 100 - 30 * d/e (1.0 = 70, 2.0 = 40)
    de = balance_qual.get("debt_to_equity")
    leverage_score = 100 - 30 * (de or 0) if de is not None else 50
    pillars["leverage"] = {
        "score": round(max(0, min(100, leverage_score)), 0),
        "debt_to_equity": de,
        "altman_z_score": altman_z,
    }

    # 4. Liquidity — current ratio
    cr = balance_qual.get("current_ratio")
    liquidity_score = 50 + (cr or 1) * 25 if cr is not None else 50
    pillars["liquidity"] = {
        "score": round(max(0, min(100, liquidity_score)), 0),
        "current_ratio": cr,
        "working_capital": balance_qual.get("working_capital"),
    }

    # 5. Quality — CFO/NI, Piotroski. Score = piotroski * 10 + 20 if cfo/ni > 1
    cfo_ni = cf_qual.get("cfo_to_ni")
    quality_score = ((piotroski or 5) * 10) + (20 if (cfo_ni or 0) > 1 else 0)
    pillars["quality"] = {
        "score": round(max(0, min(100, quality_score)), 0),
        "piotroski_score": piotroski,
        "cfo_to_ni":  cfo_ni,
    }

    overall = sum(p["score"] for p in pillars.values()) / 5
    return {
        "pillars":     pillars,
        "overall_score": round(overall, 0),
        "altman_z":    altman_z,
        "piotroski":   piotroski,
    }


def fetch_latest_transcript(ticker: str, transcript_dates: list) -> Optional[dict]:
    """Fetch the most recent earnings call transcript.

    /stable/earning-call-transcript-dates gives us a list of (date,
    fiscalYear, quarter) tuples. We pick the most recent and fetch
    /stable/earning-call-transcript?symbol=X&year=Y&quarter=Q to get
    the actual call content.

    Returns: {date, year, quarter, content_truncated, content_full_chars}
             or None if no transcripts.
    """
    if not isinstance(transcript_dates, list) or not transcript_dates:
        return None
    # Most-recent first by date string
    sorted_dates = sorted(transcript_dates,
                            key=lambda d: d.get("date") or "",
                            reverse=True)
    latest_meta = sorted_dates[0]
    year = latest_meta.get("fiscalYear")
    quarter = latest_meta.get("quarter")
    if not year or not quarter:
        return None

    # Fetch the actual call
    r = fmp_get("earning-call-transcript", symbol=ticker,
                  year=year, quarter=quarter)
    transcript = _first(r) or {}
    content = transcript.get("content") or ""
    if not content:
        return None

    # Transcripts can be 50K-150K chars. We need to truncate intelligently
    # for the Claude payload. Hedge fund analysts care most about:
    #   - The intro / prepared remarks (CEO + CFO outlook)
    #   - The Q&A (where analysts probe weaknesses)
    # Strategy: take the first 8000 chars (prepared remarks) + last
    # 8000 chars (final Q&A often has the most pointed exchanges).
    full_len = len(content)
    if full_len <= 16000:
        truncated = content
    else:
        truncated = (content[:8000] +
                       "\n\n…[middle of call omitted for brevity]…\n\n" +
                       content[-8000:])

    return {
        "date":               latest_meta.get("date"),
        "fiscal_year":        year,
        "quarter":            quarter,
        "full_chars":         full_len,
        "truncated_chars":    len(truncated),
        "content_truncated":  truncated,
    }


def compute_institutional_activity(ownership_filings: list) -> dict:
    """SEC 13D/13G beneficial ownership filings analysis.

    13D = activist filing (intent to influence). 13G = passive >5% holder.
    The pattern matters more than the count: a cluster of recent filings
    from blue-chip institutions (Vanguard, Blackrock, Berkshire,
    Wellington) suggests crossing-the-threshold accumulation.

    NB: This is NOT insider trading (Form 4) — FMP doesn't expose Form 4
    on the current plan. 13D/13G data is the closest proxy: institutional
    'smart money' position changes that cross 5% reporting threshold.
    """
    if not isinstance(ownership_filings, list) or not ownership_filings:
        return {}

    # FMP returns recent filings; some may be 5+ years old. Filter to last 24 months.
    from datetime import timedelta
    cutoff = (datetime.now(timezone.utc) - timedelta(days=730)).strftime("%Y-%m-%d")
    recent = [f for f in ownership_filings if (f.get("filingDate") or "") >= cutoff]
    # Sort newest first
    recent.sort(key=lambda f: f.get("filingDate") or "", reverse=True)

    # Top 8 most-recent filings for display
    recent_top = recent[:8]
    filings_display = []
    for f in recent_top:
        filings_display.append({
            "filing_date":          f.get("filingDate"),
            "filer":                f.get("nameOfReportingPerson"),
            "shares_owned":         _safe_num(f, "amountBeneficiallyOwned"),
            "pct_of_class":         _safe_num(f, "percentOfClass"),
            "filer_jurisdiction":   f.get("citizenshipOrPlaceOfOrganization"),
            "filer_type":           f.get("typeOfReportingPerson"),
            "url":                  f.get("url"),
        })

    # Aggregate
    unique_filers = set(f.get("nameOfReportingPerson") for f in recent if f.get("nameOfReportingPerson"))
    total_pct = sum(_safe_num(f, "percentOfClass") or 0 for f in recent)

    return {
        "n_filings_total":      len(ownership_filings),
        "n_filings_recent_24m": len(recent),
        "n_unique_filers_24m":  len(unique_filers),
        "filings_display":      filings_display,
        "summary_note":         "13D/13G filings show institutional positions crossing the 5% reporting threshold. Not insider Form 4 (which FMP doesn't expose on current plan). This is institutional 'smart money' accumulation/divestment.",
    }


def compute_capital_allocation(cf_annual: list, income_annual: list,
                                 quote: dict) -> dict:
    """Institutional capital allocation analysis.

    Hedge funds care about:
      - Total capital returned to shareholders (divs + buybacks) over time
      - Payout ratio (capital returned / net income) — sustainability check
      - Shareholder yield (capital returned / market cap) — total return floor
      - Buyback yield vs dividend yield (cash distribution mix)
      - Capex / revenue trend (capital intensity, business model signal)
      - 'Cash-cow vs capex-heavy' framing
    """
    if not (isinstance(cf_annual, list) and isinstance(income_annual, list)
            and cf_annual and income_annual):
        return {}
    market_cap = _safe_num(quote, "marketCap") or 0

    # Per-year detail (last 10y)
    timeline = []
    for i, cf in enumerate(cf_annual[:10]):
        date = cf.get("date") or ""
        ni_row = None
        # Match income statement by date
        for inc in income_annual:
            if inc.get("date") == date:
                ni_row = inc
                break
        net_income = _safe_num(ni_row, "netIncome") if ni_row else None

        # FMP stores dividendsPaid and commonStockRepurchased as NEGATIVE
        # (cash outflows). Convert to positive for reader clarity.
        divs_raw = _safe_num(cf, "dividendsPaid")
        bb_raw   = _safe_num(cf, "commonStockRepurchased")
        capex_raw = _safe_num(cf, "capitalExpenditure")

        divs_paid    = abs(divs_raw) if divs_raw is not None else None
        buybacks     = abs(bb_raw)   if bb_raw is not None else None
        capex        = abs(capex_raw) if capex_raw is not None else None
        capital_returned = (divs_paid or 0) + (buybacks or 0)

        payout_ratio = None
        if net_income and net_income > 0 and capital_returned > 0:
            payout_ratio = round(capital_returned / net_income * 100, 1)

        fcf = _safe_num(cf, "freeCashFlow")
        fcf_payout = None
        if fcf and fcf > 0 and capital_returned > 0:
            fcf_payout = round(capital_returned / fcf * 100, 1)

        # Get revenue for capex/revenue trend
        revenue = _safe_num(ni_row, "revenue") if ni_row else None
        capex_to_rev = round(capex / revenue * 100, 2) if (capex and revenue and revenue > 0) else None

        timeline.append({
            "year":              date[:4] if date else None,
            "net_income":        net_income,
            "free_cash_flow":    fcf,
            "dividends_paid":    divs_paid,
            "buybacks":          buybacks,
            "capex":             capex,
            "revenue":           revenue,
            "capital_returned":  capital_returned if capital_returned > 0 else None,
            "payout_ratio_pct":  payout_ratio,
            "fcf_payout_pct":    fcf_payout,
            "capex_to_revenue_pct": capex_to_rev,
        })

    # ── Aggregates (rolling 10y where available)
    def sum_field(name, n=10):
        vals = [t.get(name) for t in timeline[:n]]
        clean = [v for v in vals if isinstance(v, (int, float))]
        return sum(clean) if clean else None

    total_divs_10y     = sum_field("dividends_paid")
    total_buybacks_10y = sum_field("buybacks")
    total_capex_10y    = sum_field("capex")
    total_returned_10y = (total_divs_10y or 0) + (total_buybacks_10y or 0)

    # ── Shareholder yield = recent annualized capital return / mkt cap
    latest_return = timeline[0].get("capital_returned") if timeline else None
    shareholder_yield_pct = None
    if latest_return and market_cap > 0:
        shareholder_yield_pct = round(latest_return / market_cap * 100, 2)

    # Distribution mix (latest year): what fraction is buybacks vs divs
    buyback_share_pct = None
    if timeline and timeline[0].get("capital_returned"):
        latest = timeline[0]
        if latest.get("buybacks") is not None:
            buyback_share_pct = round((latest["buybacks"] or 0) / latest["capital_returned"] * 100, 1)

    # Capital intensity trend (capex/rev)
    capex_trend = [t.get("capex_to_revenue_pct") for t in timeline if t.get("capex_to_revenue_pct") is not None]
    capex_recent_avg = round(sum(capex_trend[:3]) / len(capex_trend[:3]), 2) if capex_trend[:3] else None
    capex_older_avg  = round(sum(capex_trend[5:8]) / len(capex_trend[5:8]), 2) if len(capex_trend) >= 8 else None

    capex_intensity_trend = None
    if capex_recent_avg is not None and capex_older_avg is not None and capex_older_avg > 0:
        change = (capex_recent_avg - capex_older_avg) / capex_older_avg
        if change > 0.2:    capex_intensity_trend = "rising"
        elif change < -0.2: capex_intensity_trend = "falling"
        else:               capex_intensity_trend = "stable"

    return {
        "timeline":               timeline,
        "total_dividends_10y":    total_divs_10y,
        "total_buybacks_10y":     total_buybacks_10y,
        "total_capex_10y":        total_capex_10y,
        "total_returned_10y":     total_returned_10y if total_returned_10y > 0 else None,
        "shareholder_yield_pct":  shareholder_yield_pct,
        "buyback_share_of_return_pct": buyback_share_pct,
        "latest_payout_ratio_pct": timeline[0].get("payout_ratio_pct") if timeline else None,
        "latest_fcf_payout_pct":  timeline[0].get("fcf_payout_pct") if timeline else None,
        "capex_to_revenue_recent_avg": capex_recent_avg,
        "capex_to_revenue_older_avg":  capex_older_avg,
        "capex_intensity_trend":  capex_intensity_trend,
    }


def compute_earnings_track_record(earnings_rows: list) -> dict:
    """Institutional-style earnings beat/miss analysis.

    Hedge fund framing: a stock that beats consensus 7 of 8 quarters is a
    'high-quality compounder'; one that beats by SHRINKING magnitude is
    showing deteriorating fundamentals even if the beats continue.

    Returns:
      - eps_beats / eps_misses / eps_inline counts
      - eps_beat_rate (% of quarters with epsActual >= epsEstimated)
      - eps_avg_beat_pct (mean (actual-est)/est across beat quarters)
      - eps_avg_miss_pct (mean across miss quarters, negative number)
      - eps_current_streak  (consecutive beats, negative = miss streak)
      - eps_magnitude_trend ('expanding' | 'stable' | 'shrinking' | None)
      - revenue_beat_rate, revenue_avg_surprise_pct (same for revenue)
      - quarters: detailed list of past quarters
    """
    if not isinstance(earnings_rows, list):
        return {}
    # Filter to past quarters only (actual numbers reported, not forward)
    past = [r for r in earnings_rows
              if isinstance(r, dict)
              and r.get("epsActual") is not None
              and r.get("epsEstimated") is not None]
    if not past:
        return {}
    # Newest first (FMP default), confirm by sorting
    past = sorted(past, key=lambda r: r.get("date") or "", reverse=True)[:12]

    def surprise_pct(actual, est):
        try:
            if est is None or est == 0: return None
            return round((float(actual) - float(est)) / abs(float(est)) * 100, 2)
        except (TypeError, ValueError):
            return None

    # Detailed quarter list
    quarters = []
    for r in past:
        eps_act, eps_est = r.get("epsActual"), r.get("epsEstimated")
        rev_act, rev_est = r.get("revenueActual"), r.get("revenueEstimated")
        quarters.append({
            "date":              r.get("date"),
            "eps_estimated":     eps_est,
            "eps_actual":        eps_act,
            "eps_surprise_pct":  surprise_pct(eps_act, eps_est),
            "revenue_estimated": rev_est,
            "revenue_actual":    rev_act,
            "revenue_surprise_pct": surprise_pct(rev_act, rev_est),
        })

    # EPS beat/miss aggregates
    eps_surprises = [q["eps_surprise_pct"] for q in quarters if q["eps_surprise_pct"] is not None]
    eps_beats = [s for s in eps_surprises if s > 0.5]   # >0.5% counts as a beat
    eps_misses = [s for s in eps_surprises if s < -0.5]
    eps_inline = [s for s in eps_surprises if -0.5 <= s <= 0.5]

    # Current streak: count from most recent quarter, sign = direction
    streak = 0
    if quarters and quarters[0]["eps_surprise_pct"] is not None:
        direction = 1 if quarters[0]["eps_surprise_pct"] > 0.5 else (-1 if quarters[0]["eps_surprise_pct"] < -0.5 else 0)
        if direction != 0:
            for q in quarters:
                s = q["eps_surprise_pct"]
                if s is None: break
                if direction > 0 and s > 0.5: streak += 1
                elif direction < 0 and s < -0.5: streak += 1
                else: break
            streak *= direction

    # Magnitude trend: compare first half avg beat vs second half
    magnitude_trend = None
    if len(eps_beats) >= 4 and len(quarters) >= 6:
        recent_beats = [q["eps_surprise_pct"] for q in quarters[:4]
                         if q["eps_surprise_pct"] is not None and q["eps_surprise_pct"] > 0.5]
        older_beats = [q["eps_surprise_pct"] for q in quarters[4:8]
                        if q["eps_surprise_pct"] is not None and q["eps_surprise_pct"] > 0.5]
        if recent_beats and older_beats:
            recent_avg = sum(recent_beats) / len(recent_beats)
            older_avg = sum(older_beats) / len(older_beats)
            if recent_avg > older_avg * 1.2:   magnitude_trend = "expanding"
            elif recent_avg < older_avg * 0.7: magnitude_trend = "shrinking"
            else: magnitude_trend = "stable"

    # Revenue surprise aggregates
    rev_surprises = [q["revenue_surprise_pct"] for q in quarters
                       if q["revenue_surprise_pct"] is not None]
    rev_beats = [s for s in rev_surprises if s > 0.5]
    rev_misses = [s for s in rev_surprises if s < -0.5]

    def avg(lst): return round(sum(lst) / len(lst), 2) if lst else None

    return {
        "n_quarters":              len(quarters),
        "eps_beats":               len(eps_beats),
        "eps_misses":              len(eps_misses),
        "eps_inline":              len(eps_inline),
        "eps_beat_rate_pct":       round(len(eps_beats) / max(1, len(eps_surprises)) * 100, 1),
        "eps_avg_beat_pct":        avg(eps_beats),
        "eps_avg_miss_pct":        avg(eps_misses),
        "eps_current_streak":      streak,
        "eps_magnitude_trend":     magnitude_trend,
        "revenue_beats":           len(rev_beats),
        "revenue_misses":          len(rev_misses),
        "revenue_beat_rate_pct":   round(len(rev_beats) / max(1, len(rev_surprises)) * 100, 1)
                                       if rev_surprises else None,
        "revenue_avg_surprise_pct": avg(rev_surprises),
        "quarters":                quarters,
    }


def build_peer_comparison(subject_ticker: str, subject_ratios_ttm: dict,
                            subject_key_ttm: dict, subject_quote: dict,
                            subject_company: dict, peers_list: list,
                            peer_details: dict) -> dict:
    """Build a side-by-side comparison table: subject + peers with key valuation
    metrics, plus peer-median summary stats. The peer median functions as the
    'industry P/E' benchmark hedge fund analysts compare against."""
    import statistics as _stats

    def make_row(sym, name, price, mkt_cap, ratios, key_metrics, is_subject=False):
        # ratios = /stable/ratios-ttm response.  Provides PE/PB/PS/EV multiple / op margin.
        # key_metrics = /stable/key-metrics-ttm response.  Provides ROE / ROIC / EV/EBITDA.
        # FMP field naming verified by ops 1139.
        return {
            "symbol":      sym,
            "name":        name,
            "price":       price,
            "market_cap":  mkt_cap,
            "pe":          (_safe_num(ratios, "priceToEarningsRatioTTM")
                              or _safe_num(ratios, "peRatioTTM")),
            "pb":          _safe_num(ratios, "priceToBookRatioTTM"),
            "ps":          _safe_num(ratios, "priceToSalesRatioTTM"),
            "ev_ebitda":   (_safe_num(ratios, "enterpriseValueMultipleTTM")
                              or _safe_num(key_metrics, "evToEBITDATTM")
                              or _safe_num(key_metrics, "enterpriseValueOverEBITDATTM")),
            "roe_pct":     _pct(_safe_num(key_metrics, "returnOnEquityTTM")),
            "op_margin_pct": _pct(_safe_num(ratios, "operatingProfitMarginTTM")),
            "is_subject":  is_subject,
        }

    rows = [
        make_row(subject_ticker, subject_company.get("name") or subject_ticker,
                  _safe_num(subject_quote, "price"),
                  _safe_num(subject_quote, "marketCap") or subject_company.get("market_cap"),
                  subject_ratios_ttm, subject_key_ttm, is_subject=True)
    ]
    for peer in peers_list[:5]:
        sym = peer.get("symbol")
        if not sym: continue
        detail = peer_details.get(sym, {})
        rows.append(make_row(
            sym, peer.get("companyName") or sym,
            peer.get("price"), peer.get("mktCap"),
            detail.get("ratios", {}), detail.get("key_metrics", {}),
        ))

    def median(vals):
        clean = [v for v in vals if isinstance(v, (int, float)) and v == v]  # filter NaN
        # Also filter ridiculous outliers (P/E > 500 = junk)
        clean = [v for v in clean if -500 < v < 500]
        return round(_stats.median(clean), 2) if clean else None

    peer_rows = rows[1:]
    summary = {
        "n_peers":           len(peer_rows),
        "median_pe":         median([r["pe"] for r in peer_rows]),
        "median_pb":         median([r["pb"] for r in peer_rows]),
        "median_ps":         median([r["ps"] for r in peer_rows]),
        "median_ev_ebitda":  median([r["ev_ebitda"] for r in peer_rows]),
        "median_roe_pct":    median([r["roe_pct"] for r in peer_rows]),
        "median_op_margin_pct": median([r["op_margin_pct"] for r in peer_rows]),
    }

    # Subject's premium / discount vs peer median (negative = trading at discount)
    subject = rows[0]
    relative: Dict[str, Any] = {}
    for key in ("pe", "pb", "ps", "ev_ebitda"):
        sub = subject.get(key)
        med = summary.get(f"median_{key}")
        if isinstance(sub, (int, float)) and isinstance(med, (int, float)) and med > 0:
            relative[f"premium_pct_{key}"] = round((sub / med - 1) * 100, 1)

    return {
        "sector":     subject_company.get("sector"),
        "industry":   subject_company.get("industry"),
        "rows":       rows,
        "summary":    summary,
        "relative":   relative,
    }


# ═════════════════════════════════════════════════════════════════════
# Claude synthesis
# ═════════════════════════════════════════════════════════════════════

CLAUDE_SYSTEM = """You are a senior equity research analyst at a top hedge fund.
You write the kind of research memo a portfolio manager would read before making a multi-million dollar
position decision. Your output is structured, opinionated, and rooted ENTIRELY in the data provided.

NEVER invent numbers. If a metric is missing, state that. Use specific data points.
Be DIRECTIONAL — equivocation is for losers. Tell the PM whether to buy, hold, or sell.

OUTPUT JSON ONLY, no markdown, no preamble.

Schema:
{
  "executive_summary": "3-4 sentence top-line. Lead with the recommendation and key thesis.",
  "investment_thesis": {
    "title":             "Punchy 5-8 word headline",
    "thesis_paragraph":  "150-200 word case for owning the stock, with specific numbers from the data",
    "key_drivers": [
      {"driver": "...", "supporting_data": "specific metric e.g. 'rev 5y CAGR 14.2%'"},
      ... (4-5 drivers)
    ]
  },
  "risk_factors": {
    "title": "Punchy risk-headline",
    "risk_paragraph": "150-200 words on what could go wrong, with specific numbers",
    "key_risks": [
      {"risk": "...", "evidence": "specific data e.g. 'debt/equity 2.4x vs sector 0.8x'"},
      ... (4-5 risks)
    ]
  },
  "valuation_assessment": "150 words on whether the stock is cheap/fair/expensive given P/E vs 5yr avg, DCF gap, peer multiples, and FCF yield. Be specific.",
  "peer_comparison_assessment": "100 words on how the subject's valuation multiples compare to the peer-median (which functions as the industry P/E benchmark). Reference SPECIFIC peer ticker(s) where helpful. Frame as: 'trading at X% premium/discount to peer median P/E of Y'.",
  "earnings_track_record_assessment": "80 words on the company's earnings consistency. Cite the EPS beat rate, current streak, magnitude trend, and revenue surprise. Hedge fund framing: 'beats 7 of 8 quarters but with shrinking magnitude = deteriorating quality' is more useful than just 'beats consensus regularly'.",
  "capital_allocation_assessment": "80 words on management's capital allocation. Cite total capital returned 10y, shareholder yield, dividend vs buyback mix, payout ratio sustainability, and capex intensity trend. Frame as 'cash-cow returning $X to shareholders' vs 'reinvesting heavily into capex' — both can be good, depends on ROIC.",
  "institutional_activity_assessment": "60 words on recent SEC 13D/13G beneficial-ownership filings (institutional positions crossing 5% threshold). If filings are stale (>24mo old) or absent, say so plainly. If recent clustering by notable institutions (Berkshire, Vanguard, Blackrock, Wellington), call it out as 'smart money accumulation'.",
  "earnings_call_sentiment": {
    "available": true|false (set to false if no transcript was provided),
    "overall_tone": "BULLISH | CONFIDENT | NEUTRAL | CAUTIOUS | DEFENSIVE",
    "tone_summary": "100 words describing the management's tone across the prepared remarks and Q&A. Cite specific phrases ('several large customers paused projects', 'we expect double-digit growth to accelerate') — direct attribution to CEO or CFO when possible. Distinguish the prepared-remarks tone from the Q&A tone — Q&A often reveals more.",
    "key_topics": ["3-5 topic clusters management spent the most time on, e.g. 'AI infrastructure capex', 'China demand softness', 'pricing power in enterprise'"],
    "guidance_change": "RAISED | MAINTAINED | LOWERED | NOT_PROVIDED",
    "guidance_summary": "50 words on what management said about forward guidance and how it changed from prior quarter (if mentioned).",
    "notable_quotes": [
      {"speaker": "name + title", "quote": "verbatim short quote", "significance": "why this matters to a PM"},
      ... (2-3 quotes that contain the most information)
    ]
  },
  "financial_health_summary": "100 words on the 5-pillar score, calling out the strongest and weakest pillars with the actual numbers.",
  "competitive_position": "100 words on the company's moat and industry position based on margins, growth durability, and ROIC vs peers.",
  "catalysts_12m": [
    {"event": "...", "timeframe": "Q2 2026 | H1 2027 | etc", "potential_impact": "..."},
    ... (3-5 catalysts)
  ],
  "invalidation_triggers": [
    {"trigger": "what would change the thesis", "monitor": "what to watch"},
    ... (3-5 triggers)
  ],
  "verdict": {
    "rating":              "STRONG_BUY | BUY | HOLD | SELL | STRONG_SELL",
    "conviction_grade":    "A+ | A | A- | B+ | B | B- | C+ | C | C- | D",
    "price_target_12m":    <number — the 12-month PT in USD>,
    "upside_pct":          <number — implied upside vs current>,
    "confidence_pct":      <0-100 reflecting probability the thesis plays out>,
    "position_size_pct":   <recommended position size 1-15% for a concentrated book>,
    "time_horizon_months": <how long to hold, typically 6-24>,
    "verdict_rationale":   "30-50 word reasoning tying together thesis + risks + valuation"
  }
}"""


def build_claude_prompt(payload: dict) -> str:
    """Compose user prompt — JSON dump of every meaningful field."""
    return (
        f"Produce institutional equity research for "
        f"{payload['company'].get('name','?')} ({payload['ticker']}).\n\n"
        "All data follows. Synthesize per the schema in the system prompt.\n\n"
        "```json\n" + json.dumps(payload, indent=2, default=str)[:60000] + "\n```\n"
    )


def parse_claude(text: str) -> dict:
    """Strip ```json fences and parse."""
    import re
    text = re.sub(r"^```(?:json)?\s*", "", text.strip(), flags=re.MULTILINE)
    text = re.sub(r"\s*```\s*$", "", text.strip(), flags=re.MULTILINE)
    # Find first balanced JSON object
    depth, start = 0, -1
    for i, ch in enumerate(text):
        if ch == "{":
            if depth == 0: start = i
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0 and start >= 0:
                try:
                    return json.loads(text[start:i+1])
                except json.JSONDecodeError:
                    continue
    return json.loads(text)


# ═════════════════════════════════════════════════════════════════════
# Main handler
# ═════════════════════════════════════════════════════════════════════

def lambda_handler(event, context):
    t0 = time.time()

    # ── Extract ticker from query string OR POST body
    ticker = None
    try:
        if isinstance(event, dict):
            qs = event.get("queryStringParameters") or {}
            ticker = qs.get("ticker")
            if not ticker and event.get("body"):
                body = event["body"]
                if isinstance(body, str):
                    try: body = json.loads(body)
                    except Exception: body = {}
                if isinstance(body, dict): ticker = body.get("ticker")
    except Exception as e:
        return _http_error(400, f"Could not parse request: {e}")

    if not ticker:
        return _http_error(400, "Missing 'ticker' query parameter")
    ticker = ticker.strip().upper()
    # Tickers can contain letters, digits, and class-share separators (- or .)
    # e.g. AAPL, MSFT, BRK-B, BRK.B, RDS-A
    import re as _re
    if not _re.fullmatch(r"[A-Z0-9.\-]{1,10}", ticker):
        return _http_error(400, f"Invalid ticker: {ticker}")

    # ── Check cache
    force_refresh = False
    if isinstance(event, dict):
        qs = event.get("queryStringParameters") or {}
        force_refresh = qs.get("refresh") in ("1", "true", "yes")
    cache_key = f"{CACHE_PREFIX}{ticker}.json"
    if not force_refresh:
        try:
            obj = s3.get_object(Bucket=S3_BUCKET, Key=cache_key)
            cached = json.loads(obj["Body"].read())
            cached["from_cache"] = True
            cached["cache_age_seconds"] = int(time.time() - _iso_to_epoch(cached.get("generated_at")))
            if cached["cache_age_seconds"] < CACHE_TTL:
                print(f"[cache] HIT {ticker} age={cached['cache_age_seconds']}s")
                return _http_ok(cached)
        except s3.exceptions.NoSuchKey:
            pass
        except Exception as e:
            print(f"[cache] read error: {e}")

    # ── Fetch all FMP endpoints in parallel
    print(f"[research] fetching {ticker}")
    raw = fetch_all(ticker)
    n_ok = sum(1 for v in raw.values() if v)
    print(f"[research] {n_ok}/{len(raw)} endpoints returned data")

    profile_obj = _first(raw.get("profile")) or {}
    quote_obj   = _first(raw.get("quote"))   or {}

    if not profile_obj and not quote_obj:
        return _http_error(404, f"No data for ticker {ticker}")

    income_annual    = raw.get("income_annual") if isinstance(raw.get("income_annual"), list) else []
    income_quarterly = raw.get("income_quarterly") if isinstance(raw.get("income_quarterly"), list) else []
    balance_annual   = raw.get("balance_annual") if isinstance(raw.get("balance_annual"), list) else []
    cashflow_annual  = raw.get("cashflow_annual") if isinstance(raw.get("cashflow_annual"), list) else []
    ratios_annual    = raw.get("ratios_annual") if isinstance(raw.get("ratios_annual"), list) else []
    ratios_ttm       = _first(raw.get("ratios_ttm")) or {}
    key_metrics      = raw.get("key_metrics") if isinstance(raw.get("key_metrics"), list) else []
    key_ttm          = _first(raw.get("key_metrics_ttm")) or {}
    growth_series    = raw.get("growth") if isinstance(raw.get("growth"), list) else []
    estimates        = raw.get("estimates") if isinstance(raw.get("estimates"), list) else []
    pt_consensus     = raw.get("pt_consensus") or {}
    dcf              = raw.get("dcf") or {}
    scores           = raw.get("scores") or {}
    peers_obj        = _first(raw.get("peers")) or {}
    prices_eod       = raw.get("prices_eod") if isinstance(raw.get("prices_eod"), list) else []
    dividends        = raw.get("dividends") if isinstance(raw.get("dividends"), list) else []
    earnings         = raw.get("earnings") if isinstance(raw.get("earnings"), list) else []
    ownership_data   = raw.get("ownership") if isinstance(raw.get("ownership"), list) else []
    transcript_dates_data = raw.get("transcript_dates") if isinstance(raw.get("transcript_dates"), list) else []

    # The peers endpoint returns a LIST of peer objects directly (not wrapped).
    # Each has symbol, companyName, price, mktCap.
    peers_list = raw.get("peers") if isinstance(raw.get("peers"), list) else []
    peer_symbols = [p.get("symbol") for p in peers_list if p.get("symbol")][:5]

    # ── Second-round parallel fetch: peer ratios + key metrics for comparison
    peer_details: Dict[str, Dict[str, Any]] = {}
    if peer_symbols:
        with ThreadPoolExecutor(max_workers=10) as ex:
            futures = {}
            for s in peer_symbols:
                futures[ex.submit(fmp_get, "ratios-ttm", symbol=s)] = (s, "ratios")
                futures[ex.submit(fmp_get, "key-metrics-ttm", symbol=s)] = (s, "key_metrics")
            for fut in as_completed(futures):
                sym, kind = futures[fut]
                try:
                    peer_details.setdefault(sym, {})[kind] = _first(fut.result()) or {}
                except Exception:
                    peer_details.setdefault(sym, {})[kind] = {}
        print(f"[peers] fetched detail for {len(peer_details)} peers")

    # ── Derive analytics
    growth_metrics    = compute_growth(income_annual)
    fcf_metrics       = compute_fcf_cagr(cashflow_annual)
    margin_trend      = compute_margin_trend(income_annual, n=12)
    qty_consistency   = compute_quarterly_consistency(income_quarterly)
    current_price     = _safe_num(quote_obj, "price") or _safe_num(profile_obj, "price")
    returns           = compute_returns(prices_eod, current_price)
    balance_qual      = compute_balance_quality(balance_annual)
    cf_qual           = compute_cf_quality(income_annual, cashflow_annual)
    valuation         = compute_valuation(profile_obj, ratios_ttm, key_ttm,
                                            ratios_annual, dcf, pt_consensus, quote_obj)
    health            = compute_financial_health(scores, ratios_ttm, key_ttm,
                                                   balance_qual, cf_qual,
                                                   {**growth_metrics, **fcf_metrics})

    # Company block needs to be built BEFORE peer comparison since we pass it in.
    _company_block_for_peers = {
        "name":         profile_obj.get("companyName") or profile_obj.get("name"),
        "sector":       profile_obj.get("sector"),
        "industry":     profile_obj.get("industry"),
        "market_cap":   _safe_num(profile_obj, "mktCap") or _safe_num(profile_obj, "marketCap"),
    }
    peer_comparison = build_peer_comparison(
        ticker, ratios_ttm, key_ttm, quote_obj,
        _company_block_for_peers, peers_list, peer_details,
    )

    # ── Earnings beat/miss track record
    earnings_track_record = compute_earnings_track_record(earnings)

    # ── Capital allocation timeline
    capital_allocation = compute_capital_allocation(cashflow_annual, income_annual, quote_obj)

    # ── Institutional activity (13D/13G filings — smart money tracking)
    institutional_activity = compute_institutional_activity(ownership_data)

    # ── Earnings call transcript (most recent quarter)
    earnings_call = fetch_latest_transcript(ticker, transcript_dates_data)
    if earnings_call:
        print(f"[transcript] {ticker} Q{earnings_call['quarter']} {earnings_call['fiscal_year']} "
              f"({earnings_call['date']}) — {earnings_call['full_chars']} chars full, "
              f"{earnings_call['truncated_chars']} sent to Claude")

    # ── Compact statements (every year, just essential fields)
    def compact_income(rows):
        keys = ("date","revenue","grossProfit","operatingIncome","netIncome",
                "epsDiluted","weightedAverageShsOutDil","grossProfitRatio",
                "operatingIncomeRatio","netIncomeRatio")
        return [{k: r.get(k) for k in keys if k in r} for r in rows]

    def compact_balance(rows):
        keys = ("date","totalAssets","totalLiabilities","totalEquity",
                "totalStockholdersEquity","totalCurrentAssets",
                "totalCurrentLiabilities","cashAndShortTermInvestments",
                "totalDebt","longTermDebt","shortTermDebt","goodwill")
        return [{k: r.get(k) for k in keys if k in r} for r in rows]

    def compact_cf(rows):
        keys = ("date","operatingCashFlow","netCashProvidedByOperatingActivities",
                "capitalExpenditure","freeCashFlow","dividendsPaid",
                "commonStockRepurchased","netCashUsedForInvestingActivities",
                "netCashUsedProvidedByFinancingActivities","netIncome")
        return [{k: r.get(k) for k in keys if k in r} for r in rows]

    # ── Build payload for Claude
    company_block = {
        "name":         profile_obj.get("companyName") or profile_obj.get("name"),
        "sector":       profile_obj.get("sector"),
        "industry":     profile_obj.get("industry"),
        "country":      profile_obj.get("country"),
        "exchange":     profile_obj.get("exchange"),
        "ceo":          profile_obj.get("ceo"),
        "employees":    profile_obj.get("fullTimeEmployees"),
        "ipo_date":     profile_obj.get("ipoDate"),
        "market_cap":   _safe_num(profile_obj, "mktCap") or _safe_num(profile_obj, "marketCap"),
        "description":  (profile_obj.get("description") or "")[:1200],
        "website":      profile_obj.get("website"),
        "beta":         _safe_num(profile_obj, "beta"),
    }

    quote_block = {
        "price":            current_price,
        "change_pct":       _safe_num(quote_obj, "changesPercentage") or _safe_num(quote_obj, "changePercentage"),
        "volume":           _safe_num(quote_obj, "volume"),
        "avg_volume":       _safe_num(quote_obj, "avgVolume"),
        "day_low":          _safe_num(quote_obj, "dayLow"),
        "day_high":         _safe_num(quote_obj, "dayHigh"),
        "year_low":         _safe_num(quote_obj, "yearLow"),
        "year_high":        _safe_num(quote_obj, "yearHigh"),
    }

    # Forward estimates (next 2 years)
    est_block = []
    if isinstance(estimates, list):
        for e in estimates[:3]:
            est_block.append({
                "date":            e.get("date"),
                "revenue_avg":     _safe_num(e, "estimatedRevenueAvg"),
                "eps_avg":         _safe_num(e, "estimatedEpsAvg"),
                "num_analysts_rev":_safe_num(e, "numberAnalystsEstimatedRevenue"),
                "num_analysts_eps":_safe_num(e, "numberAnalystsEstimatedEps"),
            })

    payload = {
        "ticker":          ticker,
        "company":         company_block,
        "quote":           quote_block,
        "valuation":       valuation,
        "growth":          {**growth_metrics, **fcf_metrics, **qty_consistency},
        "margins":         margin_trend,
        "balance_quality": balance_qual,
        "cashflow_quality": cf_qual,
        "financial_health": health,
        "returns":         returns,
        "analyst_estimates": est_block,
        "peer_comparison": peer_comparison,
        "earnings_track_record": earnings_track_record,
        "capital_allocation": capital_allocation,
        "institutional_activity": institutional_activity,
        "earnings_call_excerpt": earnings_call,
        "statements_preview": {
            "income_top_5y":      compact_income(income_annual[:5]),
            "balance_top_5y":     compact_balance(balance_annual[:5]),
            "cashflow_top_5y":    compact_cf(cashflow_annual[:5]),
        },
    }

    # ── Call Claude for synthesis
    claude_synthesis = {}
    claude_elapsed = None
    try:
        t_claude = time.time()
        user_prompt = build_claude_prompt(payload)
        response_text = claude_call(CLAUDE_SYSTEM, user_prompt, max_tokens=6000)
        claude_elapsed = round(time.time() - t_claude, 2)
        claude_synthesis = parse_claude(response_text)
        print(f"[claude] {len(response_text)} chars in {claude_elapsed}s")
    except Exception as e:
        print(f"[claude] ERROR: {e}\n{traceback.format_exc()[:600]}")
        claude_synthesis = {
            "executive_summary": f"AI synthesis failed: {str(e)[:200]}. Underlying data is available below.",
            "verdict": {"rating": "HOLD", "conviction_grade": "C",
                         "verdict_rationale": "Manual review required — AI synthesis unavailable."},
        }

    # ── Assemble final document
    document = {
        "schema_version": "1.0",
        "ticker":         ticker,
        "generated_at":   datetime.now(timezone.utc).isoformat(),
        "from_cache":     False,
        "company":        company_block,
        "quote":          quote_block,
        "verdict":        claude_synthesis.get("verdict") or {},
        "executive_summary":   claude_synthesis.get("executive_summary"),
        "investment_thesis":   claude_synthesis.get("investment_thesis"),
        "risk_factors":        claude_synthesis.get("risk_factors"),
        "valuation_assessment":claude_synthesis.get("valuation_assessment"),
        "peer_comparison_assessment": claude_synthesis.get("peer_comparison_assessment"),
        "earnings_track_record_assessment": claude_synthesis.get("earnings_track_record_assessment"),
        "capital_allocation_assessment": claude_synthesis.get("capital_allocation_assessment"),
        "institutional_activity_assessment": claude_synthesis.get("institutional_activity_assessment"),
        "earnings_call_sentiment": claude_synthesis.get("earnings_call_sentiment"),
        "financial_health_summary": claude_synthesis.get("financial_health_summary"),
        "competitive_position":claude_synthesis.get("competitive_position"),
        "catalysts_12m":       claude_synthesis.get("catalysts_12m") or [],
        "invalidation_triggers": claude_synthesis.get("invalidation_triggers") or [],
        "valuation":           valuation,
        "growth":              {**growth_metrics, **fcf_metrics, **qty_consistency},
        "margins":             margin_trend,
        "balance_quality":     balance_qual,
        "cashflow_quality":    cf_qual,
        "financial_health":    health,
        "returns":             returns,
        "analyst_estimates":   est_block,
        "peer_comparison":     peer_comparison,
        "earnings_track_record": earnings_track_record,
        "capital_allocation":  capital_allocation,
        "institutional_activity": institutional_activity,
        "earnings_call": {
            "date":              earnings_call.get("date") if earnings_call else None,
            "fiscal_year":       earnings_call.get("fiscal_year") if earnings_call else None,
            "quarter":           earnings_call.get("quarter") if earnings_call else None,
            "full_chars":        earnings_call.get("full_chars") if earnings_call else None,
        } if earnings_call else None,
        "short_interest": {
            # FMP /stable/ doesn't expose short interest on the current plan
            # (verified ops 1141). Real data requires either:
            #   1. FINRA Gateway registration (in KHALID_ACTIONS.md pending list)
            #   2. FMP plan upgrade
            #   3. NYSE/Nasdaq direct feed
            # Until one of those, this field is a placeholder so the
            # frontend can render a transparent 'data gap' card rather
            # than silently omitting an important institutional signal.
            "available":          False,
            "reason":             "FMP /stable/short-interest is not exposed on the current plan tier. FINRA Gateway registration is the standard institutional source for short interest (% of float, days to cover, trend) — registration is pending in the operator's action backlog. Once enabled, this section will surface short interest % of float, days-to-cover, and historical trend.",
            "alternate_sources":  ["FINRA Gateway", "NYSE Short Interest XML feed", "Nasdaq Short Interest Reports"],
        },
        "statements": {
            "income_annual":     compact_income(income_annual),
            "balance_annual":    compact_balance(balance_annual),
            "cashflow_annual":   compact_cf(cashflow_annual),
            "income_quarterly":  compact_income(income_quarterly),
        },
        "metadata": {
            "data_sources_loaded":  n_ok,
            "data_sources_total":   len(raw),
            "fmp_endpoints":        list(raw.keys()),
            "fmp_endpoints_failed": [k for k, v in raw.items() if not v],
            "claude_model":         MODEL,
            "claude_elapsed_sec":   claude_elapsed,
            "total_elapsed_sec":    round(time.time() - t0, 2),
        },
    }

    # ── Cache to S3
    try:
        s3.put_object(
            Bucket=S3_BUCKET, Key=cache_key,
            Body=json.dumps(document, default=str).encode("utf-8"),
            ContentType="application/json",
            CacheControl=f"public, max-age={CACHE_TTL}",
        )
        print(f"[cache] WROTE {cache_key}")
    except Exception as e:
        print(f"[cache] write failed: {e}")

    return _http_ok(document)


def _iso_to_epoch(iso_str: Optional[str]) -> float:
    if not iso_str: return 0
    try:
        return datetime.fromisoformat(iso_str.replace("Z", "+00:00")).timestamp()
    except Exception:
        return 0


def _http_ok(body: dict) -> dict:
    return {
        "statusCode": 200,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
            "Cache-Control": "public, max-age=3600",
        },
        "body": json.dumps(body, default=str),
    }


def _http_error(status: int, msg: str) -> dict:
    return {
        "statusCode": status,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
        },
        "body": json.dumps({"error": msg}),
    }
