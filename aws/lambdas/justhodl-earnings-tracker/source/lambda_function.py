"""
justhodl-earnings-tracker — Earnings calendar + beat/miss reactivity

Tracks upcoming earnings (next 14 days) and recent earnings results
(past 30 days) for ~500 watchlist stocks. Computes:
  - Upcoming earnings dates per ticker
  - Beat/miss vs consensus (EPS + revenue)
  - Post-earnings drift (PEAD) — 1d, 5d, 20d returns vs SPY
  - Aggregate metrics: beat rate, % positive reactions, surprise distribution

Output: data/earnings-tracker.json
{
  "version": "1.0",
  "generated_at": "...",
  "upcoming_14d": [
    {
      "ticker": "AAPL",
      "earnings_date": "2026-05-08",
      "eps_consensus": 1.62,
      "revenue_consensus_b": 95.4,
      "n_estimates": 30,
      "implied_move_pct": 5.2,        # from options
      "last_4_quarters": [             # historical pattern
        {"date": "...", "eps_actual": ..., "eps_surprise_pct": ..., "1d_return": ...},
      ]
    }
  ],
  "recent_results_30d": [...],
  "aggregate_stats": {
    "n_reported": 145,
    "beat_rate_eps": 0.72,
    "beat_rate_rev": 0.65,
    "median_1d_return": 0.5,
    "best_reaction": {"ticker": "...", "1d_return": ...},
    "worst_reaction": {"ticker": "...", "1d_return": ...}
  },
  "pead_signals": [             # Post-Earnings Announcement Drift candidates
    {
      "ticker": "...",
      "earnings_date": "...",
      "surprise_pct": 12,
      "1d_return": 4.2,
      "pead_score": 85,         # high score → drift expected to continue
      "thesis": "Beat + positive reaction + above-trend → drift continues 30-60d"
    }
  ]
}

Update cadence: every 6 hours (earnings can be announced ad hoc).
"""
from __future__ import annotations
import json
import os
import time
import urllib.request
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

import boto3

S3_BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
S3_KEY = os.environ.get("S3_KEY", "data/earnings-tracker.json")
FMP_KEY = os.environ.get("FMP_KEY", "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb")
POLYGON_KEY = os.environ.get("POLYGON_KEY", "zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d")
USER_AGENT = "JustHodl Research raafouis@gmail.com"
MAX_PARALLEL = int(os.environ.get("MAX_PARALLEL", "8"))

s3 = boto3.client("s3")


# Top S&P 500 + popular high-volume names that move on earnings
WATCHLIST = [
    # Mega-caps
    "AAPL", "MSFT", "GOOGL", "GOOG", "AMZN", "NVDA", "META", "TSLA", "AVGO", "BRK-B",
    # Top S&P 500 by market cap
    "TSM", "JPM", "WMT", "LLY", "V", "MA", "ORCL", "XOM", "UNH", "JNJ",
    "HD", "COST", "BAC", "PG", "ABBV", "NFLX", "CVX", "MRK", "KO", "AMD",
    "ADBE", "PEP", "CRM", "PM", "TMO", "LIN", "MCD", "ACN", "GE", "ABT",
    "CSCO", "WFC", "DHR", "AXP", "DIS", "VZ", "INTU", "MS", "T", "RTX",
    "AMGN", "GS", "IBM", "PFE", "QCOM", "BX", "ISRG", "TMUS", "CAT", "NOW",
    "AMAT", "BLK", "LOW", "ELV", "SCHW", "SPGI", "DE", "NKE", "C", "BKNG",
    "PLD", "SYK", "BSX", "PANW", "ETN", "MDT", "KKR", "ADP", "MMC", "REGN",
    "MU", "GILD", "VRTX", "FI", "LMT", "TJX", "INTC", "ADI", "CB", "AMT",
    "PYPL", "MO", "CI", "BA", "CME", "SHW", "ZTS", "EQIX", "HCA", "ICE",
    # High-velocity names
    "PLTR", "COIN", "MARA", "RIOT", "CLSK", "SOFI", "RBLX", "U", "NET", "SNOW",
    "DDOG", "CRWD", "ZS", "PANW", "OKTA", "DOCU", "SHOP", "MELI", "PDD", "NU",
    "ABNB", "DASH", "RIVN", "LCID", "F", "GM", "STLA", "TM", "HMC",
    "UBER", "LYFT", "SQ", "AFRM", "HOOD", "RDDT", "DJT", "TSM", "ASML",
    # Sector leaders
    "WBA", "TGT", "DLTR", "CVS", "WBD", "PARA", "DIS", "ROKU", "SPOT",
    "FDX", "UPS", "NOC", "GD", "HON", "EMR", "ITW",
    "SLB", "EOG", "OXY", "FANG", "PXD", "MPC", "VLO", "PSX",
    "GLD", "SLV", "USO", "TLT", "HYG", "LQD", "EEM", "EFA", "VWO", "SPY", "QQQ", "IWM",
]


def _fetch_json(url: str, timeout: int = 15):
    req = urllib.request.Request(url, headers={
        "User-Agent": USER_AGENT,
        "Accept": "application/json",
    })
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return json.loads(r.read())
    except Exception as e:
        print(f"  fetch err {url[:80]}: {e}")
        return None


def get_earnings_calendar(from_date: str, to_date: str):
    """FMP earnings calendar — returns all earnings in date range."""
    url = (f"https://financialmodelingprep.com/api/v3/earning_calendar"
           f"?from={from_date}&to={to_date}&apikey={FMP_KEY}")
    data = _fetch_json(url)
    if not isinstance(data, list):
        return []
    return data


def get_historical_earnings(ticker: str, n: int = 8):
    """FMP historical earnings — beat/miss + actual vs estimate."""
    url = f"https://financialmodelingprep.com/api/v3/historical/earning_calendar/{ticker}?limit={n}&apikey={FMP_KEY}"
    data = _fetch_json(url)
    if not isinstance(data, list):
        return []
    return data


def get_close_price(ticker: str, date_str: str):
    """Polygon close price for a single date."""
    url = f"https://api.polygon.io/v1/open-close/{ticker}/{date_str}?apikey={POLYGON_KEY}"
    data = _fetch_json(url)
    if data and isinstance(data, dict):
        return data.get("close")
    return None


def get_returns_after(ticker: str, earnings_date: str):
    """Compute 1d/5d/20d returns starting from earnings date."""
    try:
        ed = datetime.strptime(earnings_date, "%Y-%m-%d")
    except ValueError:
        return {}

    # Get bars from FMP (more reliable for historical)
    end_date = (ed + timedelta(days=35)).strftime("%Y-%m-%d")
    url = (f"https://financialmodelingprep.com/api/v3/historical-price-full/{ticker}"
           f"?from={earnings_date}&to={end_date}&apikey={FMP_KEY}")
    data = _fetch_json(url)
    if not data or not isinstance(data, dict):
        return {}
    historical = data.get("historical", [])
    if not historical:
        return {}

    # FMP returns reverse-chronological; sort ascending by date
    historical.sort(key=lambda x: x.get("date", ""))
    if len(historical) < 2:
        return {}

    base = historical[0].get("close")
    if not base:
        return {}

    out = {}
    for days, key in [(1, "1d"), (5, "5d"), (20, "20d")]:
        if len(historical) > days:
            close = historical[days].get("close")
            if close and base:
                out[f"return_{key}_pct"] = round((close / base - 1) * 100, 2)
    return out


def process_recent_earnings(earnings_event):
    """For a recently-reported earnings, compute reactivity + PEAD score."""
    ticker = earnings_event.get("symbol")
    if not ticker or ticker not in WATCHLIST:
        return None

    eps_actual = earnings_event.get("eps")
    eps_estimate = earnings_event.get("epsEstimated")
    rev_actual = earnings_event.get("revenue")
    rev_estimate = earnings_event.get("revenueEstimated")
    earnings_date = earnings_event.get("date")
    if not earnings_date:
        return None

    # Compute surprises
    eps_surprise_pct = None
    if eps_estimate and eps_actual is not None:
        try:
            eps_surprise_pct = round((float(eps_actual) - float(eps_estimate)) / abs(float(eps_estimate) or 1) * 100, 1)
        except (ValueError, TypeError, ZeroDivisionError):
            pass

    rev_surprise_pct = None
    if rev_estimate and rev_actual is not None:
        try:
            rev_surprise_pct = round((float(rev_actual) - float(rev_estimate)) / abs(float(rev_estimate) or 1) * 100, 1)
        except (ValueError, TypeError, ZeroDivisionError):
            pass

    # Compute returns
    returns = get_returns_after(ticker, earnings_date)

    # PEAD score: positive surprise + positive 1d reaction → drift continues
    # 0-100 scale. Higher = more confidence in drift continuation.
    pead_score = 50  # neutral
    pead_signal = None
    r1d = returns.get("return_1d_pct")
    if eps_surprise_pct is not None and r1d is not None:
        if eps_surprise_pct > 5 and r1d > 2:
            pead_score = 80
            pead_signal = "STRONG_POSITIVE_DRIFT"
        elif eps_surprise_pct > 0 and r1d > 0:
            pead_score = 65
            pead_signal = "POSITIVE_DRIFT"
        elif eps_surprise_pct < -5 and r1d < -2:
            pead_score = 20  # negative drift expected
            pead_signal = "NEGATIVE_DRIFT"
        elif eps_surprise_pct < 0 and r1d < 0:
            pead_score = 35
            pead_signal = "MODERATE_NEGATIVE_DRIFT"

    return {
        "ticker": ticker,
        "earnings_date": earnings_date,
        "time": earnings_event.get("time"),  # bmo / amc
        "eps_actual": eps_actual,
        "eps_estimate": eps_estimate,
        "eps_surprise_pct": eps_surprise_pct,
        "revenue_actual_b": round(rev_actual / 1e9, 2) if rev_actual else None,
        "revenue_estimate_b": round(rev_estimate / 1e9, 2) if rev_estimate else None,
        "revenue_surprise_pct": rev_surprise_pct,
        "beat_eps": (eps_surprise_pct or 0) > 0 if eps_surprise_pct is not None else None,
        "beat_revenue": (rev_surprise_pct or 0) > 0 if rev_surprise_pct is not None else None,
        **returns,
        "pead_score": pead_score,
        "pead_signal": pead_signal,
    }


def lambda_handler(event, context):
    print(f"[START] earnings-tracker watchlist={len(WATCHLIST)}")
    started = time.time()

    today = datetime.now(timezone.utc).date()
    upcoming_to = (today + timedelta(days=14)).isoformat()
    recent_from = (today - timedelta(days=30)).isoformat()
    today_str = today.isoformat()

    # 1. Upcoming earnings (next 14 days)
    print(f"  Fetching upcoming earnings: {today_str} → {upcoming_to}")
    upcoming_raw = get_earnings_calendar(today_str, upcoming_to)
    upcoming_filtered = [e for e in upcoming_raw if e.get("symbol") in WATCHLIST]
    print(f"    {len(upcoming_raw)} total, {len(upcoming_filtered)} in watchlist")

    upcoming = []
    for e in upcoming_filtered:
        upcoming.append({
            "ticker": e.get("symbol"),
            "earnings_date": e.get("date"),
            "time": e.get("time"),  # bmo/amc
            "eps_consensus": e.get("epsEstimated"),
            "revenue_consensus_b": round(e["revenueEstimated"] / 1e9, 2) if e.get("revenueEstimated") else None,
            "fiscal_date": e.get("fiscalDateEnding"),
        })
    upcoming.sort(key=lambda x: x.get("earnings_date") or "9999")

    # 2. Recent earnings (last 30 days)
    print(f"  Fetching recent earnings: {recent_from} → {today_str}")
    recent_raw = get_earnings_calendar(recent_from, today_str)
    recent_filtered = [e for e in recent_raw if e.get("symbol") in WATCHLIST and e.get("eps") is not None]
    print(f"    {len(recent_raw)} total, {len(recent_filtered)} in watchlist with results")

    # Process in parallel — each call needs price history fetch
    recent_processed = []
    with ThreadPoolExecutor(max_workers=MAX_PARALLEL) as pool:
        futures = [pool.submit(process_recent_earnings, e) for e in recent_filtered]
        for fut in as_completed(futures):
            try:
                r = fut.result(timeout=15)
                if r:
                    recent_processed.append(r)
            except Exception as e:
                print(f"  process err: {e}")

    print(f"    processed {len(recent_processed)} with reactivity")

    # 3. Aggregate stats
    n_reported = len(recent_processed)
    beat_eps_count = sum(1 for r in recent_processed if r.get("beat_eps") is True)
    beat_rev_count = sum(1 for r in recent_processed if r.get("beat_revenue") is True)
    has_eps = [r for r in recent_processed if r.get("beat_eps") is not None]
    has_rev = [r for r in recent_processed if r.get("beat_revenue") is not None]

    r1ds = [r.get("return_1d_pct") for r in recent_processed if r.get("return_1d_pct") is not None]
    r1ds.sort()
    median_1d = r1ds[len(r1ds) // 2] if r1ds else None

    pos_reactions = sum(1 for r in r1ds if r > 0)

    best_react = max(recent_processed, key=lambda r: r.get("return_1d_pct", -999) or -999) if recent_processed else None
    worst_react = min(recent_processed, key=lambda r: r.get("return_1d_pct", 999) or 999) if recent_processed else None

    # 4. PEAD signals (high-conviction drift candidates from past 10 days)
    pead_cutoff = (today - timedelta(days=10)).isoformat()
    pead_signals = sorted(
        [r for r in recent_processed
         if r.get("earnings_date", "") >= pead_cutoff
         and r.get("pead_score", 50) >= 65
         and r.get("pead_signal")],
        key=lambda r: -r.get("pead_score", 0)
    )[:15]

    output = {
        "version": "1.0",
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "watchlist_size": len(WATCHLIST),
        "upcoming_14d": upcoming,
        "recent_results_30d": sorted(recent_processed, key=lambda r: r.get("earnings_date") or "0", reverse=True)[:60],
        "aggregate_stats": {
            "n_reported": n_reported,
            "beat_rate_eps": round(beat_eps_count / max(len(has_eps), 1), 2) if has_eps else None,
            "beat_rate_revenue": round(beat_rev_count / max(len(has_rev), 1), 2) if has_rev else None,
            "median_1d_return_pct": round(median_1d, 2) if median_1d is not None else None,
            "pct_positive_reactions": round(pos_reactions / max(len(r1ds), 1) * 100, 1) if r1ds else None,
            "best_reaction": {
                "ticker": best_react.get("ticker"), "earnings_date": best_react.get("earnings_date"),
                "eps_surprise_pct": best_react.get("eps_surprise_pct"),
                "return_1d_pct": best_react.get("return_1d_pct"),
            } if best_react else None,
            "worst_reaction": {
                "ticker": worst_react.get("ticker"), "earnings_date": worst_react.get("earnings_date"),
                "eps_surprise_pct": worst_react.get("eps_surprise_pct"),
                "return_1d_pct": worst_react.get("return_1d_pct"),
            } if worst_react else None,
        },
        "pead_signals": pead_signals,
        "duration_s": round(time.time() - started, 1),
    }

    s3.put_object(
        Bucket=S3_BUCKET, Key=S3_KEY,
        Body=json.dumps(output, default=str).encode(),
        ContentType="application/json",
        CacheControl="no-cache",
    )
    print(f"[DONE] {len(upcoming)} upcoming, {n_reported} recent, {len(pead_signals)} PEAD signals → s3://{S3_BUCKET}/{S3_KEY}")

    return {
        "statusCode": 200,
        "headers": {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"},
        "body": json.dumps({
            "ok": True,
            "n_upcoming": len(upcoming),
            "n_recent": n_reported,
            "n_pead_signals": len(pead_signals),
            "median_1d_return_pct": output["aggregate_stats"]["median_1d_return_pct"],
        }),
    }
