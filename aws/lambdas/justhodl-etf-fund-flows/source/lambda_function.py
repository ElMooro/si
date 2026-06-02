"""justhodl-etf-fund-flows

Institutional ETF Capital Flow Intelligence Engine.

WHY THIS MATTERS
════════════════
ETF Global Fund Flows is the most expensive Polygon subscription ($99/mo)
and the most differentiated edge in the entire JustHodl.AI data stack.
Daily creation/redemption flows for 3,000+ ETFs is data that hedge funds
pay Lipper/Morningstar/EPFR $30K+/yr for. Academic backing is real:

  - Ben-David et al. (2017): ETF flows precede underlying price action
  - BIS Quarterly Review (2018): ETF flows are leading indicator of
    cross-asset risk sentiment
  - Israeli et al. (2017): TLT inflows precede flight-to-safety regimes

THE INSTITUTIONAL EDGE
══════════════════════
Single-ETF flows are noise. The alpha is in the COMBINATIONS:

  - Sector rotation: which sectors are seeing persistent inflow vs
    outflow over 5/21 days (not just today's print)
  - Smart vs dumb money: SPY/QQQ/sector-SPDR flows (institutional) vs
    ARKK/SOXL/leveraged flows (retail-favored). Divergence = contrarian
    signal
  - Risk-on/off: equity inflows + Treasury outflows = risk-on;
    Treasury inflows + equity outflows = de-risking
  - Growth vs value: VUG/MTUM flows vs IWD/VLUE flows
  - Credit stress: HYG outflows + LQD inflows = flight to quality
  - Domestic/international: SPY/QQQ vs EFA/EEM/VWO flows

OUTPUT ARCHITECTURE
═══════════════════
Writes 5 S3 files:

  etf-flows/daily.json
      Full snapshot: per-ETF raw flows, % AUM, 5/21d cumulative,
      90d z-score, persistence days, signal label
      Read by: /flows.html dashboard, analytics workbench

  etf-flows/composite.json
      6 institutional composite signals with current values + history
      Read by: /flows.html, signal-board, research/critique prompts

  etf-flows/rotation.json
      Sector rotation matrix: which sectors are gaining/losing,
      with category aggregates
      Read by: /flows.html sector heatmap

  etf-flows/per-ticker-context.json
      Lookup: {TICKER: sector_etf_flow_context} for every
      research-universe ticker. Injected into research/critique prompts.
      Read by: equity-research Lambda, research-critique Lambda

  etf-flows/history/{YYYY-MM-DD}/{ETF}.json
      Date-stamped snapshots for backtest attribution by regime.

POLYGON API
═══════════
Endpoint: https://api.polygon.io/etf-global/v1/fund-flows
Auth: ?apiKey=$POLYGON_KEY
Per-ETF request: ?composite_ticker={ETF}
Response: { results: [{ processed_date, fund_flow_daily, fund_flow_5d,
            fund_flow_21d, aum, ... }] }

Scheduled: cron(0 22 * * ? *) = 17:00 ET daily (after Polygon's
EOD processing). Rate limit: ~100 calls in ~1 minute (well within
Polygon's 1000/min Starter limit).
"""
import json
import os
import time
import urllib.request
import urllib.error
import statistics
import math
from datetime import datetime, timezone
from typing import Optional, List, Dict, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed

import boto3

S3_BUCKET = "justhodl-dashboard-live"
OUTPUT_PREFIX = "etf-flows/"
POLYGON_KEY = os.environ.get("POLYGON_KEY", "")
POLYGON_BASE = "https://api.polygon.io"
ETF_FLOWS_ENDPOINT = f"{POLYGON_BASE}/etf-global/v1/fund-flows"
FETCH_TIMEOUT = 15
MAX_WORKERS = 8

s3 = boto3.client("s3", region_name="us-east-1")


# ═════════════════════════════════════════════════════════════════════
# ETF UNIVERSE — institutionally tagged ~100 ETFs
# ═════════════════════════════════════════════════════════════════════
# Each entry has:
#   category:    high-level bucket (broad/sector/factor/country/treasury/
#                                    credit/commodity/thematic/leveraged/crypto)
#   subcategory: granular tag (technology/financials/momentum/japan/etc.)
#   smart_money: True if the ETF is institutionally-favored, False if
#                retail-favored. Used for the smart-vs-dumb composite.
#   region:      US/Intl/EM/Global
#   ref_sector:  GICS sector this ETF maps to (for ticker→ETF lookup)

ETF_UNIVERSE = {
    # Broad market — smart money's preferred US exposure
    "SPY":  {"category": "broad", "subcategory": "us_large_cap",       "smart_money": True,  "region": "US",   "ref_sector": None},
    "VOO":  {"category": "broad", "subcategory": "us_large_cap",       "smart_money": True,  "region": "US",   "ref_sector": None},
    "IVV":  {"category": "broad", "subcategory": "us_large_cap",       "smart_money": True,  "region": "US",   "ref_sector": None},
    "QQQ":  {"category": "broad", "subcategory": "us_megacap_tech",    "smart_money": True,  "region": "US",   "ref_sector": None},
    "IWM":  {"category": "broad", "subcategory": "us_small_cap",       "smart_money": True,  "region": "US",   "ref_sector": None},
    "VTI":  {"category": "broad", "subcategory": "us_total",           "smart_money": True,  "region": "US",   "ref_sector": None},
    "DIA":  {"category": "broad", "subcategory": "us_dow",             "smart_money": True,  "region": "US",   "ref_sector": None},
    # GICS Sector SPDRs — the institutional sector rotation universe
    "XLK":  {"category": "sector", "subcategory": "technology",         "smart_money": True,  "region": "US", "ref_sector": "Technology"},
    "XLF":  {"category": "sector", "subcategory": "financials",         "smart_money": True,  "region": "US", "ref_sector": "Financial Services"},
    "XLE":  {"category": "sector", "subcategory": "energy",             "smart_money": True,  "region": "US", "ref_sector": "Energy"},
    "XLV":  {"category": "sector", "subcategory": "healthcare",         "smart_money": True,  "region": "US", "ref_sector": "Healthcare"},
    "XLP":  {"category": "sector", "subcategory": "staples",            "smart_money": True,  "region": "US", "ref_sector": "Consumer Defensive"},
    "XLY":  {"category": "sector", "subcategory": "discretionary",      "smart_money": True,  "region": "US", "ref_sector": "Consumer Cyclical"},
    "XLI":  {"category": "sector", "subcategory": "industrials",        "smart_money": True,  "region": "US", "ref_sector": "Industrials"},
    "XLB":  {"category": "sector", "subcategory": "materials",          "smart_money": True,  "region": "US", "ref_sector": "Basic Materials"},
    "XLU":  {"category": "sector", "subcategory": "utilities",          "smart_money": True,  "region": "US", "ref_sector": "Utilities"},
    "XLRE": {"category": "sector", "subcategory": "real_estate",        "smart_money": True,  "region": "US", "ref_sector": "Real Estate"},
    "XLC":  {"category": "sector", "subcategory": "communication",      "smart_money": True,  "region": "US", "ref_sector": "Communication Services"},
    # Factor ETFs — style positioning
    "MTUM": {"category": "factor", "subcategory": "momentum",           "smart_money": True,  "region": "US", "ref_sector": None},
    "VLUE": {"category": "factor", "subcategory": "value",              "smart_money": True,  "region": "US", "ref_sector": None},
    "QUAL": {"category": "factor", "subcategory": "quality",            "smart_money": True,  "region": "US", "ref_sector": None},
    "USMV": {"category": "factor", "subcategory": "min_vol",            "smart_money": True,  "region": "US", "ref_sector": None},
    "IWD":  {"category": "factor", "subcategory": "russell_value",      "smart_money": True,  "region": "US", "ref_sector": None},
    "IWF":  {"category": "factor", "subcategory": "russell_growth",     "smart_money": True,  "region": "US", "ref_sector": None},
    "SPLV": {"category": "factor", "subcategory": "low_vol",            "smart_money": True,  "region": "US", "ref_sector": None},
    "VUG":  {"category": "factor", "subcategory": "vanguard_growth",    "smart_money": True,  "region": "US", "ref_sector": None},
    "VTV":  {"category": "factor", "subcategory": "vanguard_value",     "smart_money": True,  "region": "US", "ref_sector": None},
    # Country/Region — global positioning
    "EFA":  {"category": "country", "subcategory": "eafe",              "smart_money": True,  "region": "Intl",   "ref_sector": None},
    "VEA":  {"category": "country", "subcategory": "developed_intl",    "smart_money": True,  "region": "Intl",   "ref_sector": None},
    "EEM":  {"category": "country", "subcategory": "emerging_markets",  "smart_money": True,  "region": "EM",     "ref_sector": None},
    "VWO":  {"category": "country", "subcategory": "emerging_markets",  "smart_money": True,  "region": "EM",     "ref_sector": None},
    "EWJ":  {"category": "country", "subcategory": "japan",             "smart_money": True,  "region": "Intl",   "ref_sector": None},
    "FXI":  {"category": "country", "subcategory": "china_large",       "smart_money": True,  "region": "EM",     "ref_sector": None},
    "MCHI": {"category": "country", "subcategory": "china_broad",       "smart_money": True,  "region": "EM",     "ref_sector": None},
    "KWEB": {"category": "country", "subcategory": "china_internet",    "smart_money": False, "region": "EM",     "ref_sector": None},
    "EWZ":  {"category": "country", "subcategory": "brazil",            "smart_money": True,  "region": "EM",     "ref_sector": None},
    "INDA": {"category": "country", "subcategory": "india",             "smart_money": True,  "region": "EM",     "ref_sector": None},
    "EWG":  {"category": "country", "subcategory": "germany",           "smart_money": True,  "region": "Intl",   "ref_sector": None},
    "EWU":  {"category": "country", "subcategory": "uk",                "smart_money": True,  "region": "Intl",   "ref_sector": None},
    "EWY":  {"category": "country", "subcategory": "south_korea",       "smart_money": True,  "region": "EM",     "ref_sector": None},
    "EWT":  {"category": "country", "subcategory": "taiwan",            "smart_money": True,  "region": "EM",     "ref_sector": None},
    "EWA":  {"category": "country", "subcategory": "australia",         "smart_money": True,  "region": "Intl",   "ref_sector": None},
    "EWC":  {"category": "country", "subcategory": "canada",            "smart_money": True,  "region": "Intl",   "ref_sector": None},
    # Treasury — duration positioning + flight-to-quality
    "TLT":  {"category": "treasury", "subcategory": "long_20plus",      "smart_money": True,  "region": "US",   "ref_sector": None},
    "IEF":  {"category": "treasury", "subcategory": "7_10yr",           "smart_money": True,  "region": "US",   "ref_sector": None},
    "SHY":  {"category": "treasury", "subcategory": "1_3yr",            "smart_money": True,  "region": "US",   "ref_sector": None},
    "GOVT": {"category": "treasury", "subcategory": "broad",            "smart_money": True,  "region": "US",   "ref_sector": None},
    "BIL":  {"category": "treasury", "subcategory": "tbill",            "smart_money": True,  "region": "US",   "ref_sector": None},
    "TIP":  {"category": "treasury", "subcategory": "tips",             "smart_money": True,  "region": "US",   "ref_sector": None},
    "AGG":  {"category": "treasury", "subcategory": "agg_bond",         "smart_money": True,  "region": "US",   "ref_sector": None},
    # Credit — risk appetite proxy
    "HYG":  {"category": "credit", "subcategory": "high_yield",         "smart_money": True,  "region": "US",   "ref_sector": None},
    "JNK":  {"category": "credit", "subcategory": "high_yield",         "smart_money": True,  "region": "US",   "ref_sector": None},
    "LQD":  {"category": "credit", "subcategory": "investment_grade",   "smart_money": True,  "region": "US",   "ref_sector": None},
    "EMB":  {"category": "credit", "subcategory": "em_bond",            "smart_money": True,  "region": "EM",   "ref_sector": None},
    # Commodities — inflation/macro positioning
    "GLD":  {"category": "commodity", "subcategory": "gold",            "smart_money": True,  "region": "US",   "ref_sector": None},
    "IAU":  {"category": "commodity", "subcategory": "gold",            "smart_money": True,  "region": "US",   "ref_sector": None},
    "SLV":  {"category": "commodity", "subcategory": "silver",          "smart_money": True,  "region": "US",   "ref_sector": None},
    "USO":  {"category": "commodity", "subcategory": "oil",             "smart_money": False, "region": "US",   "ref_sector": None},
    "UNG":  {"category": "commodity", "subcategory": "nat_gas",         "smart_money": False, "region": "US",   "ref_sector": None},
    "DBC":  {"category": "commodity", "subcategory": "broad",           "smart_money": True,  "region": "US",   "ref_sector": None},
    "DBA":  {"category": "commodity", "subcategory": "agriculture",     "smart_money": True,  "region": "US",   "ref_sector": None},
    "CPER": {"category": "commodity", "subcategory": "copper",          "smart_money": True,  "region": "US",   "ref_sector": None},
    # Thematic — typically retail-driven
    "ARKK": {"category": "thematic", "subcategory": "innovation",       "smart_money": False, "region": "US",   "ref_sector": None},
    "ARKW": {"category": "thematic", "subcategory": "web3",             "smart_money": False, "region": "US",   "ref_sector": None},
    "ARKG": {"category": "thematic", "subcategory": "genomics",         "smart_money": False, "region": "US",   "ref_sector": None},
    "SOXX": {"category": "thematic", "subcategory": "semiconductors",   "smart_money": True,  "region": "US",   "ref_sector": "Technology"},
    "SMH":  {"category": "thematic", "subcategory": "semiconductors",   "smart_money": True,  "region": "US",   "ref_sector": "Technology"},
    "TAN":  {"category": "thematic", "subcategory": "solar",            "smart_money": False, "region": "US",   "ref_sector": None},
    "ICLN": {"category": "thematic", "subcategory": "clean_energy",     "smart_money": False, "region": "Global","ref_sector": None},
    "LIT":  {"category": "thematic", "subcategory": "lithium",          "smart_money": False, "region": "Global","ref_sector": None},
    "IBB":  {"category": "thematic", "subcategory": "biotech",          "smart_money": True,  "region": "US",   "ref_sector": "Healthcare"},
    "XBI":  {"category": "thematic", "subcategory": "biotech",          "smart_money": True,  "region": "US",   "ref_sector": "Healthcare"},
    "KRE":  {"category": "thematic", "subcategory": "regional_banks",   "smart_money": True,  "region": "US",   "ref_sector": "Financial Services"},
    # Leveraged — pure retail proxy
    "TQQQ": {"category": "leveraged", "subcategory": "3x_qqq_bull",     "smart_money": False, "region": "US",   "ref_sector": None},
    "SQQQ": {"category": "leveraged", "subcategory": "3x_qqq_bear",     "smart_money": False, "region": "US",   "ref_sector": None},
    "SOXL": {"category": "leveraged", "subcategory": "3x_semi_bull",    "smart_money": False, "region": "US",   "ref_sector": None},
    "SOXS": {"category": "leveraged", "subcategory": "3x_semi_bear",    "smart_money": False, "region": "US",   "ref_sector": None},
    "TMF":  {"category": "leveraged", "subcategory": "3x_treasury",     "smart_money": False, "region": "US",   "ref_sector": None},
    "UVXY": {"category": "leveraged", "subcategory": "vol_long",        "smart_money": False, "region": "US",   "ref_sector": None},
    "SVXY": {"category": "leveraged", "subcategory": "vol_short",       "smart_money": False, "region": "US",   "ref_sector": None},
    # Crypto
    "IBIT": {"category": "crypto",   "subcategory": "bitcoin",          "smart_money": True,  "region": "US",   "ref_sector": None},
    "FBTC": {"category": "crypto",   "subcategory": "bitcoin",          "smart_money": True,  "region": "US",   "ref_sector": None},
    "ETHA": {"category": "crypto",   "subcategory": "ethereum",         "smart_money": True,  "region": "US",   "ref_sector": None},
    "BITO": {"category": "crypto",   "subcategory": "bitcoin_futures",  "smart_money": False, "region": "US",   "ref_sector": None},
}


# ═════════════════════════════════════════════════════════════════════
# Polygon API client
# ═════════════════════════════════════════════════════════════════════
def fetch_etf_flow_window(ticker: str, days: int = 100) -> dict:
    """Fetch last ~`days` of fund flow data for one ETF in a single call.

    Polygon's /etf-global/v1/fund-flows returns:
        results: [{ processed_date, effective_date, composite_ticker,
                    shares_outstanding, nav, fund_flow }, ...]
    Defaults to ASC order with limit=1 (giving us the OLDEST record).
    We pass order=desc + sort=processed_date + a date range to get the
    most recent ~90 trading days. From those we compute everything:
    latest snapshot, 5d/21d cumulative, AUM (shares*nav), z-score,
    persistence.

    Returns dict with:
      ticker, processed_date (latest), nav, shares_outstanding, aum_usd,
      daily_flow_usd, fund_flow_5d_usd, fund_flow_21d_usd, history (list)
    """
    if not POLYGON_KEY:
        return {"ticker": ticker, "error": "POLYGON_KEY not set"}
    from datetime import timedelta
    end_date = datetime.now(timezone.utc).date()
    start_date = end_date - timedelta(days=days + 10)
    url = (
        f"{ETF_FLOWS_ENDPOINT}"
        f"?composite_ticker={ticker}"
        f"&processed_date.gte={start_date.isoformat()}"
        f"&processed_date.lte={end_date.isoformat()}"
        f"&order=desc"
        f"&sort=processed_date"
        f"&limit=120"
        f"&apiKey={POLYGON_KEY}"
    )
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "JustHodl-ETFFlows/1.0"})
        with urllib.request.urlopen(req, timeout=FETCH_TIMEOUT) as r:
            data = json.loads(r.read())
            results = data.get("results") or []
            if not results:
                return {"ticker": ticker, "error": "no_results",
                        "raw_status": data.get("status"),
                        "request_id": data.get("request_id")}
            # Already sorted desc by API, but be defensive
            results = sorted(
                results, key=lambda x: x.get("processed_date") or "",
                reverse=True,
            )
            latest = results[0]
            nav = _num(latest.get("nav"))
            shares = _num(latest.get("shares_outstanding"))
            aum = (nav * shares) if (nav is not None and shares is not None) else None

            # Cumulatives
            flows = [
                _num(r.get("fund_flow")) for r in results
                if r.get("fund_flow") is not None
            ]
            flow_daily = flows[0] if flows else None
            flow_5d = sum(flows[:5]) if len(flows) >= 5 else (
                sum(flows) if flows else None
            )
            flow_21d = sum(flows[:21]) if len(flows) >= 21 else (
                sum(flows) if flows else None
            )
            # Capture sample row for schema diagnostics
            sample_row = {k: v for k, v in latest.items()}
            return {
                "ticker": ticker,
                "processed_date": latest.get("processed_date"),
                "effective_date": latest.get("effective_date"),
                "nav": nav,
                "shares_outstanding": shares,
                "aum_usd": aum,
                "daily_flow_usd": flow_daily,
                "fund_flow_5d_usd": flow_5d,
                "fund_flow_21d_usd": flow_21d,
                "history": [
                    {"processed_date": r.get("processed_date"),
                     "flow": _num(r.get("fund_flow")),
                     "nav": _num(r.get("nav")),
                     "shares_outstanding": _num(r.get("shares_outstanding"))}
                    for r in results
                ],
                "raw_sample": sample_row,
                "n_history": len(results),
            }
    except urllib.error.HTTPError as e:
        body = ""
        try:
            body = e.read().decode("utf-8", errors="ignore")[:300]
        except Exception:
            pass
        return {"ticker": ticker, "error": f"http_{e.code}", "body": body}
    except Exception as e:
        return {"ticker": ticker, "error": str(e)[:200]}


def _num(v) -> Optional[float]:
    try:
        return float(v) if v is not None else None
    except (ValueError, TypeError):
        return None


def fetch_universe_parallel() -> dict:
    """Fetch all ETFs in parallel — one call each gets snapshot + history."""
    results = {}
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        future_to_ticker = {
            ex.submit(fetch_etf_flow_window, t, 100): t
            for t in ETF_UNIVERSE.keys()
        }
        for fut in as_completed(future_to_ticker):
            t = future_to_ticker[fut]
            try:
                results[t] = fut.result()
            except Exception as e:
                results[t] = {"ticker": t, "error": str(e)[:200]}
    return results


# ═════════════════════════════════════════════════════════════════════
# Per-ETF analytics
# ═════════════════════════════════════════════════════════════════════
def compute_per_etf_metrics(snapshot: dict, history: list) -> dict:
    """Compute all institutional metrics for one ETF.

    Returns a dict ready for the daily snapshot. None values when
    inputs are missing (graceful degradation).
    """
    if snapshot.get("error"):
        return {**snapshot, "signal_label": "DATA_MISSING"}

    daily_flow = snapshot.get("daily_flow_usd")
    aum = snapshot.get("aum_usd")
    flow_5d = snapshot.get("fund_flow_5d_usd")
    flow_21d = snapshot.get("fund_flow_21d_usd")

    # Flow as % of AUM (normalized for size — $1B into SPY is meaningless,
    # $1B into ARKK is a 10%+ swing)
    pct_aum_daily = (daily_flow / aum * 100) if (daily_flow and aum and aum > 0) else None
    pct_aum_5d = (flow_5d / aum * 100) if (flow_5d and aum and aum > 0) else None
    pct_aum_21d = (flow_21d / aum * 100) if (flow_21d and aum and aum > 0) else None

    # If 5d/21d not in API, compute from history
    if flow_5d is None and history:
        recent_5 = sorted(history, key=lambda x: x.get("processed_date") or "", reverse=True)[:5]
        flows_5 = [x["flow"] for x in recent_5 if x.get("flow") is not None]
        if flows_5:
            flow_5d = sum(flows_5)
            pct_aum_5d = (flow_5d / aum * 100) if aum and aum > 0 else None
    if flow_21d is None and history:
        recent_21 = sorted(history, key=lambda x: x.get("processed_date") or "", reverse=True)[:21]
        flows_21 = [x["flow"] for x in recent_21 if x.get("flow") is not None]
        if flows_21:
            flow_21d = sum(flows_21)
            pct_aum_21d = (flow_21d / aum * 100) if aum and aum > 0 else None

    # 90-day z-score on daily flow (the workhorse signal)
    flow_zscore_90d = None
    if daily_flow is not None and history:
        hist_flows = [x["flow"] for x in history if x.get("flow") is not None]
        if len(hist_flows) >= 30:
            mean = statistics.mean(hist_flows)
            try:
                stdev = statistics.stdev(hist_flows)
                if stdev > 0:
                    flow_zscore_90d = round((daily_flow - mean) / stdev, 2)
            except statistics.StatisticsError:
                pass

    # Persistence: consecutive days in same direction
    persistence_days = 0
    if history:
        sorted_hist = sorted(history, key=lambda x: x.get("processed_date") or "", reverse=True)
        if daily_flow is not None:
            sign = 1 if daily_flow > 0 else -1
            for row in sorted_hist:
                f = row.get("flow")
                if f is None:
                    continue
                if (f > 0 and sign > 0) or (f < 0 and sign < 0):
                    persistence_days += 1
                else:
                    break

    # Signal label — based on 90d z-score thresholds
    # These thresholds are calibrated to surface real institutional moves:
    # z > 2.0 = top 2.5% of historical inflows (rare, meaningful)
    # z 1.0-2.0 = elevated inflow
    # |z| < 1.0 = normal noise
    label = "NEUTRAL"
    if flow_zscore_90d is not None:
        if flow_zscore_90d >= 2.0:
            label = "STRONG_INFLOW"
        elif flow_zscore_90d >= 1.0:
            label = "INFLOW"
        elif flow_zscore_90d <= -2.0:
            label = "STRONG_OUTFLOW"
        elif flow_zscore_90d <= -1.0:
            label = "OUTFLOW"

    return {
        "ticker": snapshot["ticker"],
        "processed_date": snapshot.get("processed_date"),
        "category": ETF_UNIVERSE[snapshot["ticker"]]["category"],
        "subcategory": ETF_UNIVERSE[snapshot["ticker"]]["subcategory"],
        "smart_money": ETF_UNIVERSE[snapshot["ticker"]]["smart_money"],
        "region": ETF_UNIVERSE[snapshot["ticker"]]["region"],
        "ref_sector": ETF_UNIVERSE[snapshot["ticker"]]["ref_sector"],
        "daily_flow_usd": daily_flow,
        "flow_5d_usd": flow_5d,
        "flow_21d_usd": flow_21d,
        "pct_aum_daily": round(pct_aum_daily, 3) if pct_aum_daily is not None else None,
        "pct_aum_5d": round(pct_aum_5d, 2) if pct_aum_5d is not None else None,
        "pct_aum_21d": round(pct_aum_21d, 2) if pct_aum_21d is not None else None,
        "aum_usd": snapshot.get("aum_usd"),
        "flow_zscore_90d": flow_zscore_90d,
        "persistence_days": persistence_days,
        "signal_label": label,
        "n_history_points": len(history),
    }


# ═════════════════════════════════════════════════════════════════════
# Category aggregations — the rotation signal
# ═════════════════════════════════════════════════════════════════════
def aggregate_by_category(metrics: list) -> dict:
    """Sum flows by (category, subcategory). Drives the rotation heatmap."""
    out = {}
    for m in metrics:
        if m.get("error") or not m.get("daily_flow_usd"):
            continue
        cat = m["category"]
        sub = m["subcategory"]
        for key in [cat, f"{cat}.{sub}"]:
            agg = out.setdefault(key, {
                "n_etfs": 0, "total_daily_flow_usd": 0, "total_5d_usd": 0,
                "total_21d_usd": 0, "total_aum_usd": 0,
                "etfs": [], "avg_zscore": [], "category": cat, "subcategory": sub if "." in key else None,
            })
            agg["n_etfs"] += 1
            agg["total_daily_flow_usd"] += m.get("daily_flow_usd") or 0
            agg["total_5d_usd"] += m.get("flow_5d_usd") or 0
            agg["total_21d_usd"] += m.get("flow_21d_usd") or 0
            agg["total_aum_usd"] += m.get("aum_usd") or 0
            agg["etfs"].append(m["ticker"])
            if m.get("flow_zscore_90d") is not None:
                agg["avg_zscore"].append(m["flow_zscore_90d"])
    # Finalize: compute averages
    for key, agg in out.items():
        z = agg.pop("avg_zscore", [])
        agg["avg_zscore_90d"] = round(statistics.mean(z), 2) if z else None
        agg["pct_aum_daily"] = (
            round(100 * agg["total_daily_flow_usd"] / agg["total_aum_usd"], 3)
            if agg["total_aum_usd"] > 0 else None
        )
        agg["pct_aum_5d"] = (
            round(100 * agg["total_5d_usd"] / agg["total_aum_usd"], 2)
            if agg["total_aum_usd"] > 0 else None
        )
    return out


# ═════════════════════════════════════════════════════════════════════
# COMPOSITE SIGNALS — the institutional alpha
# ═════════════════════════════════════════════════════════════════════
def compute_composite_signals(metrics: list, cat_aggs: dict) -> dict:
    """6 institutional composite signals derived from per-ETF flows.

    Each is normalized to a -100 to +100 score (negative = bearish,
    positive = bullish for the named direction).
    """
    by_ticker = {m["ticker"]: m for m in metrics if not m.get("error")}

    def _z(t: str) -> Optional[float]:
        m = by_ticker.get(t, {})
        return m.get("flow_zscore_90d")

    def _avg(tickers: List[str]) -> Optional[float]:
        vals = [_z(t) for t in tickers if _z(t) is not None]
        return statistics.mean(vals) if vals else None

    def _score(positive_avg: Optional[float], negative_avg: Optional[float]) -> Optional[float]:
        """Combine two avg-z-scores into a -100..+100 composite."""
        if positive_avg is None or negative_avg is None:
            return None
        spread = positive_avg - negative_avg
        # Map spread to -100..+100 (clamp at +/-4 z which is the practical extreme)
        return round(max(-100, min(100, spread / 4.0 * 100)), 1)

    # 1. DEFENSIVE ROTATION
    # Positive = money moving to defensive sectors (XLP/XLU/XLV/TLT)
    # Negative = money flowing to cyclical (XLK/XLY/XLF/IWM)
    defensive_avg = _avg(["XLP", "XLU", "XLV", "TLT"])
    cyclical_avg = _avg(["XLK", "XLY", "XLF", "IWM"])
    defensive_rotation = _score(defensive_avg, cyclical_avg)

    # 2. SMART vs DUMB MONEY
    # Positive = smart money buying (SPY/QQQ/sector SPDRs), dumb selling
    # Negative = retail euphoria (ARKK/SOXL/leveraged inflows)
    smart_avg = _avg(["SPY", "QQQ", "IVV", "VOO", "XLK", "XLF", "XLV"])
    dumb_avg = _avg(["ARKK", "TQQQ", "SOXL", "UVXY", "KWEB"])
    smart_dumb = _score(smart_avg, dumb_avg)

    # 3. RISK-ON / RISK-OFF
    # Positive = equity inflows vs Treasury outflows = risk-on
    # Negative = Treasury inflows vs equity outflows = de-risking
    equity_avg = _avg(["SPY", "QQQ", "IWM", "EFA", "EEM"])
    treasury_avg = _avg(["TLT", "IEF", "AGG"])
    risk_on_off = _score(equity_avg, treasury_avg)

    # 4. DOMESTIC vs INTERNATIONAL
    # Positive = US equity inflows vs international outflows
    # Negative = global rotation out of US
    domestic_avg = _avg(["SPY", "QQQ", "VTI"])
    intl_avg = _avg(["EFA", "VEA", "EEM", "VWO"])
    domestic_vs_intl = _score(domestic_avg, intl_avg)

    # 5. GROWTH vs VALUE
    # Positive = growth flows
    # Negative = value rotation
    growth_avg = _avg(["VUG", "IWF", "MTUM", "QQQ"])
    value_avg = _avg(["VTV", "IWD", "VLUE"])
    growth_vs_value = _score(growth_avg, value_avg)

    # 6. CREDIT STRESS (flight-to-quality detector)
    # Positive = IG inflows + HY outflows = stress (flight to quality)
    # Negative = HY inflows + IG outflows = risk appetite healthy
    ig_avg = _avg(["LQD", "AGG", "TIP"])
    hy_avg = _avg(["HYG", "JNK", "EMB"])
    credit_stress = _score(ig_avg, hy_avg)

    # Overall regime suggestion based on composites
    # If defensive_rotation > 30 AND risk_on_off < -30 → DEFENSIVE
    # If both > 30 → bullish (smart money in growth)
    # etc.
    regime = "NEUTRAL"
    if defensive_rotation is not None and risk_on_off is not None:
        if defensive_rotation >= 30 and risk_on_off <= -30:
            regime = "DEFENSIVE"
        elif defensive_rotation <= -30 and risk_on_off >= 30:
            regime = "RISK_ON"
        elif credit_stress is not None and credit_stress >= 40:
            regime = "CREDIT_STRESS"
        elif abs(defensive_rotation) < 15 and abs(risk_on_off) < 15:
            regime = "NEUTRAL"
        else:
            regime = "TRANSITION"

    return {
        "defensive_rotation": {
            "score": defensive_rotation,
            "label": "DEFENSIVE INFLOWS" if (defensive_rotation or 0) >= 30 else ("CYCLICAL INFLOWS" if (defensive_rotation or 0) <= -30 else "MIXED"),
            "components": {"defensive_avg_z": defensive_avg, "cyclical_avg_z": cyclical_avg},
        },
        "smart_vs_dumb": {
            "score": smart_dumb,
            "label": "SMART MONEY BUYING" if (smart_dumb or 0) >= 30 else ("RETAIL EUPHORIA" if (smart_dumb or 0) <= -30 else "MIXED"),
            "components": {"smart_avg_z": smart_avg, "dumb_avg_z": dumb_avg},
        },
        "risk_on_off": {
            "score": risk_on_off,
            "label": "RISK-ON" if (risk_on_off or 0) >= 30 else ("DE-RISKING" if (risk_on_off or 0) <= -30 else "MIXED"),
            "components": {"equity_avg_z": equity_avg, "treasury_avg_z": treasury_avg},
        },
        "domestic_vs_intl": {
            "score": domestic_vs_intl,
            "label": "US PREFERRED" if (domestic_vs_intl or 0) >= 30 else ("INTL ROTATION" if (domestic_vs_intl or 0) <= -30 else "MIXED"),
        },
        "growth_vs_value": {
            "score": growth_vs_value,
            "label": "GROWTH BID" if (growth_vs_value or 0) >= 30 else ("VALUE ROTATION" if (growth_vs_value or 0) <= -30 else "MIXED"),
        },
        "credit_stress": {
            "score": credit_stress,
            "label": "FLIGHT TO QUALITY" if (credit_stress or 0) >= 30 else ("RISK APPETITE HEALTHY" if (credit_stress or 0) <= -30 else "MIXED"),
        },
        "regime": regime,
    }


# ═════════════════════════════════════════════════════════════════════
# Per-ticker context — for research/critique prompt injection
# ═════════════════════════════════════════════════════════════════════
def build_per_ticker_context(metrics: list, composite: dict) -> dict:
    """Build {ticker: flow_context} lookup for ALL tickers in research universe.

    Each entry includes:
      - sector_etf, sector_etf_flow_label, sector_etf_zscore (sector context)
      - market_regime (the composite regime tag)
      - smart_money_signal (from smart_vs_dumb composite)
      - prompt_snippet: a 2-3 sentence string ready to inject into Claude

    We attempt to look up each research ticker's GICS sector → find the
    matching XL* sector ETF in our universe → pull its flow context.
    """
    sector_to_etf = {
        "Technology":             "XLK",
        "Financial Services":     "XLF",
        "Energy":                 "XLE",
        "Healthcare":             "XLV",
        "Consumer Defensive":     "XLP",
        "Consumer Cyclical":      "XLY",
        "Industrials":            "XLI",
        "Basic Materials":        "XLB",
        "Utilities":              "XLU",
        "Real Estate":            "XLRE",
        "Communication Services": "XLC",
    }

    by_ticker = {m["ticker"]: m for m in metrics if not m.get("error")}
    regime = composite.get("regime", "NEUTRAL")
    smart_dumb_label = composite.get("smart_vs_dumb", {}).get("label", "MIXED")
    risk_on_off_label = composite.get("risk_on_off", {}).get("label", "MIXED")

    # We don't know research-universe tickers here; we generate context per
    # SECTOR and a small lookup for known tickers can be built downstream.
    # We DO provide a sector-level lookup that any ticker can use.
    per_sector = {}
    for sector_name, etf in sector_to_etf.items():
        m = by_ticker.get(etf)
        if not m:
            continue
        # Generate a 2-3 sentence prompt snippet for Claude
        flow_label = m.get("signal_label", "NEUTRAL")
        z = m.get("flow_zscore_90d")
        pct_5d = m.get("pct_aum_5d")
        persistence = m.get("persistence_days")
        flow_5d_b = (m.get("flow_5d_usd") or 0) / 1e9  # to billions

        # Compose the snippet
        if z is None:
            snippet = f"Sector ETF {etf} ({sector_name}): flow data unavailable."
        else:
            direction = "inflow" if z > 0 else "outflow"
            magnitude = (
                "extreme " if abs(z) >= 2.0 else
                "elevated " if abs(z) >= 1.0 else
                "modest "
            )
            persist_str = f" over {persistence} consecutive days" if persistence and persistence >= 3 else ""
            snippet = (
                f"Sector ETF {etf} ({sector_name}): {magnitude}{direction} "
                f"(z={z}σ vs 90-day baseline, 5d cumulative {flow_5d_b:+.2f}B, "
                f"{pct_5d:+.2f}% of AUM{persist_str}). "
                f"Market regime: {regime}. Risk posture: {risk_on_off_label}. Smart money: {smart_dumb_label}."
            )

        per_sector[sector_name] = {
            "sector_etf": etf,
            "flow_label": flow_label,
            "flow_zscore_90d": z,
            "flow_5d_usd": m.get("flow_5d_usd"),
            "pct_aum_5d": pct_5d,
            "persistence_days": persistence,
            "regime": regime,
            "prompt_snippet": snippet,
        }
    return {
        "by_sector": per_sector,
        "global_regime": regime,
        "smart_vs_dumb_label": smart_dumb_label,
        "risk_on_off_label": risk_on_off_label,
    }


# ═════════════════════════════════════════════════════════════════════
# S3 writers
# ═════════════════════════════════════════════════════════════════════
def _write_json(key: str, obj: dict, cache_ttl: int = 600):
    s3.put_object(
        Bucket=S3_BUCKET, Key=key,
        Body=json.dumps(obj, default=str).encode(),
        ContentType="application/json",
        CacheControl=f"public, max-age={cache_ttl}",
    )


# ═════════════════════════════════════════════════════════════════════
# Handler
# ═════════════════════════════════════════════════════════════════════
def lambda_handler(event, context):
    t0 = time.time()
    print(f"[etf-flows] starting at {datetime.now(timezone.utc).isoformat()}")
    print(f"[etf-flows] universe: {len(ETF_UNIVERSE)} ETFs")

    # 1. Parallel fetch: one call per ETF returns latest snapshot + 100d history
    print("[etf-flows] phase 1: fetching 100-day windows for all ETFs...")
    snapshots = fetch_universe_parallel()
    n_ok = sum(1 for s in snapshots.values() if not s.get("error"))
    print(f"[etf-flows] got data for {n_ok}/{len(ETF_UNIVERSE)} ETFs")

    # 2. Compute per-ETF metrics (history is already inside each snapshot)
    print("[etf-flows] phase 2: computing per-ETF analytics...")
    metrics = [
        compute_per_etf_metrics(snapshots[t], snapshots[t].get("history", []) or [])
        for t in ETF_UNIVERSE.keys()
    ]

    # 4. Category aggregations
    print("[etf-flows] phase 3: category aggregations...")
    category_aggs = aggregate_by_category(metrics)

    # 5. Composite signals (the alpha)
    print("[etf-flows] phase 4: computing composite signals...")
    composite = compute_composite_signals(metrics, category_aggs)
    print(f"[etf-flows] regime: {composite.get('regime')}")

    # 6. Per-ticker context (for prompt injection)
    print("[etf-flows] phase 5: building per-ticker context...")
    per_ticker = build_per_ticker_context(metrics, composite)

    elapsed = round(time.time() - t0, 1)

    # 7. Write outputs to S3
    print("[etf-flows] phase 6: writing 5 S3 outputs...")
    today_iso = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    meta = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "universe_size": len(ETF_UNIVERSE),
        "n_ok": n_ok,
        "n_failed": len(ETF_UNIVERSE) - n_ok,
        "elapsed_s": elapsed,
        "schema_version": "1.0",
    }

    # 7a. Daily full snapshot
    _write_json(f"{OUTPUT_PREFIX}daily.json", {**meta, "metrics": metrics})

    # 7b. Composite signals
    _write_json(f"{OUTPUT_PREFIX}composite.json", {**meta, "composite": composite})

    # 7c. Rotation matrix (category aggregates)
    _write_json(f"{OUTPUT_PREFIX}rotation.json", {**meta, "by_category": category_aggs})

    # 7d. Per-ticker context for prompt injection
    _write_json(f"{OUTPUT_PREFIX}per-ticker-context.json", {**meta, "context": per_ticker})

    # 7e. Historical archive (date-stamped)
    archive_key = f"{OUTPUT_PREFIX}history/{today_iso}.json"
    _write_json(archive_key, {**meta, "metrics": metrics, "composite": composite}, cache_ttl=86400)

    print(f"[etf-flows] DONE in {elapsed}s")

    # Summary
    inflow_top = sorted(
        [m for m in metrics if m.get("flow_zscore_90d") is not None],
        key=lambda x: x["flow_zscore_90d"] or 0, reverse=True
    )[:5]
    outflow_top = sorted(
        [m for m in metrics if m.get("flow_zscore_90d") is not None],
        key=lambda x: x["flow_zscore_90d"] or 0
    )[:5]

    return {
        "statusCode": 200,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
        },
        "body": json.dumps({
            "ok": True,
            "elapsed_s": elapsed,
            "n_etfs_ok": n_ok,
            "regime": composite.get("regime"),
            "composite_summary": {k: v.get("label") for k, v in composite.items() if isinstance(v, dict) and "label" in v},
            "top_5_inflows": [{"ticker": m["ticker"], "z": m.get("flow_zscore_90d"), "label": m.get("signal_label")} for m in inflow_top],
            "top_5_outflows": [{"ticker": m["ticker"], "z": m.get("flow_zscore_90d"), "label": m.get("signal_label")} for m in outflow_top],
            "outputs_written": [
                f"{OUTPUT_PREFIX}daily.json",
                f"{OUTPUT_PREFIX}composite.json",
                f"{OUTPUT_PREFIX}rotation.json",
                f"{OUTPUT_PREFIX}per-ticker-context.json",
                archive_key,
            ],
        }, default=str),
    }
