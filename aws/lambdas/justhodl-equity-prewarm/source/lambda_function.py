"""justhodl-equity-prewarm

Runs nightly at 03:00 ET (08:00 UTC). Pre-generates equity research for
the top ~80 S&P 500 tickers by market cap + sector representation so any
mainstream search hits the cache and returns in <1 second.

Strategy:
  - Hardcoded list of ~80 most-searched institutional tickers, selected for:
    - Top 50 by market cap (large-cap focus)
    - 5 picks from each of the 11 GICS sectors (sector representation)
    - 10 popular small/mid-cap names traders frequently look at
  - Parallel batches of 3 concurrent calls (respects FMP rate limits)
  - Each ticker call uses ?refresh=1 to force regeneration
  - Logs success/failure per ticker to S3 under equity-prewarm/runs/{date}.json
  - Total run time: ~80 tickers × 90s / 3 parallel ≈ 40 minutes

Cost considerations:
  - 80 × Claude calls × ~6000 output tokens = ~480K tokens/night
  - At Haiku 4.5 pricing (~$1/MTok output): ~$0.48 per night = $14.40/month
  - FMP: 80 × 21 endpoints = 1680 calls — well within plan limits
  - Lambda: 80 × ~90s = ~120 min Lambda time = ~$0.50/month

Scheduling: EventBridge cron(0 8 * * ? *)   # 08:00 UTC = 03:00 ET
"""
import json
import os
import time
import urllib.request
import urllib.error
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
import boto3

# ═══════════════════════════════════════════════════════════════════
# Config
# ═══════════════════════════════════════════════════════════════════
S3_BUCKET = "justhodl-dashboard-live"
S3_LOG_PREFIX = "equity-prewarm/runs"
RESEARCH_LAMBDA_URL = "https://6nkrwmk2ntjx54okqvtzokosb40whvfb.lambda-url.us-east-1.on.aws/"
PARALLEL_WORKERS = 6        # 6 concurrent calls → ~95s/wave × ~8.5 waves = 12 min for 50
                            # Stays well within Lambda's 900s ceiling.
                            # FMP rate: 6 workers × ~25 calls each = 150 burst then 85s gap
                            # = ~1.8 calls/sec average. Within FMP plan limits.
PER_TICKER_TIMEOUT = 180    # seconds — matches research Lambda's own timeout
HTTP_HEADERS = {"User-Agent": "justhodl-equity-prewarm/1.0"}

# ═══════════════════════════════════════════════════════════════════
# Ticker universe — top 50 institutional names by market cap + sector rep
# ═══════════════════════════════════════════════════════════════════
# Pre-warmed list. Selection criteria:
#   - Top 30 by US market cap (most-searched names)
#   - At least 2 names from each major GICS sector
#   - 3 popular ETFs (people look up SPY/QQQ/VOO often)
# Total: ~52 names, sized to complete pre-warm in <13 min.

TICKER_UNIVERSE = [
    # ── Mega-cap tech (top 10)
    "AAPL", "MSFT", "NVDA", "GOOGL", "AMZN", "META", "AVGO", "TSLA", "ORCL", "NFLX",

    # ── Tech / Software (sector rep)
    "ADBE", "CRM", "AMD", "QCOM", "CSCO", "INTU", "IBM",

    # ── Financials
    "BRK-B", "JPM", "BAC", "V", "MA", "WFC", "GS", "AXP",

    # ── Healthcare
    "UNH", "LLY", "JNJ", "ABBV", "MRK", "TMO", "ABT", "PFE",

    # ── Consumer staples / discretionary
    "PG", "KO", "PEP", "WMT", "COST", "HD", "MCD", "NKE",

    # ── Industrials / energy / materials
    "BA", "CAT", "RTX", "XOM", "CVX", "LIN",

    # ── Telecoms / media
    "T", "VZ", "DIS",

    # ── ETFs
    "SPY", "QQQ", "VOO",
]


# ═══════════════════════════════════════════════════════════════════
# Worker
# ═══════════════════════════════════════════════════════════════════
def prewarm_ticker(ticker: str) -> dict:
    """Call the research Lambda for one ticker. Returns success / timing dict."""
    url = f"{RESEARCH_LAMBDA_URL}?ticker={ticker}&refresh=1"
    req = urllib.request.Request(url, headers=HTTP_HEADERS)
    t0 = time.time()
    try:
        with urllib.request.urlopen(req, timeout=PER_TICKER_TIMEOUT) as r:
            body = r.read()
            elapsed = round(time.time() - t0, 1)
        try:
            doc = json.loads(body)
            rating = (doc.get("verdict") or {}).get("rating")
            ev = (doc.get("scenarios") or {}).get("expected_value_12m")
            return {
                "ticker":    ticker,
                "status":    "ok",
                "http":      r.status,
                "elapsed_s": elapsed,
                "size_kb":   round(len(body) / 1024, 1),
                "rating":    rating,
                "ev_12m":    ev,
            }
        except Exception:
            return {"ticker": ticker, "status": "ok_unparseable",
                    "http": r.status, "elapsed_s": elapsed, "size_bytes": len(body)}
    except urllib.error.HTTPError as e:
        return {"ticker": ticker, "status": "http_error",
                "code": e.code, "msg": e.reason,
                "elapsed_s": round(time.time() - t0, 1)}
    except Exception as e:
        return {"ticker": ticker, "status": "error",
                "error": str(e)[:300],
                "elapsed_s": round(time.time() - t0, 1)}


def lambda_handler(event, context):
    started = datetime.now(timezone.utc)

    # Allow event override for testing: {"tickers": ["AAPL", "MSFT"]} or
    # {"limit": 5} (use first N from universe). EventBridge sends an event
    # dict with detail/source but no override fields → uses default universe.
    override = (event or {}).get("tickers")
    limit = (event or {}).get("limit")
    if isinstance(override, list) and override:
        universe = [t.upper() for t in override if isinstance(t, str)]
        print(f"[prewarm] event override: using {len(universe)} tickers from request")
    else:
        # Deduplicate while preserving order
        seen = set()
        universe = [t for t in TICKER_UNIVERSE if not (t in seen or seen.add(t))]
        if isinstance(limit, int) and 0 < limit < len(universe):
            universe = universe[:limit]
            print(f"[prewarm] event limit: {limit} tickers")

    print(f"[prewarm] starting at {started.isoformat()} · {len(universe)} tickers · {PARALLEL_WORKERS} workers")

    results = []

    with ThreadPoolExecutor(max_workers=PARALLEL_WORKERS) as ex:
        futures = {ex.submit(prewarm_ticker, t): t for t in universe}
        for i, fut in enumerate(as_completed(futures), 1):
            res = fut.result()
            results.append(res)
            print(f"[prewarm] {i:>3}/{len(universe)} {res['ticker']:6s} "
                    f"{res.get('status','?'):6s} {res.get('elapsed_s','?')}s "
                    f"{res.get('rating','—')}")

    finished = datetime.now(timezone.utc)
    n_ok = sum(1 for r in results if r["status"] == "ok")
    n_err = len(results) - n_ok

    summary = {
        "run_started":   started.isoformat(),
        "run_finished":  finished.isoformat(),
        "wall_seconds":  round((finished - started).total_seconds(), 1),
        "n_total":       len(universe),
        "n_succeeded":   n_ok,
        "n_failed":      n_err,
        "parallel_workers": PARALLEL_WORKERS,
        "tickers":       universe,
        "results":       results,
    }

    # Write log to S3
    s3 = boto3.client("s3")
    key = f"{S3_LOG_PREFIX}/{started.strftime('%Y-%m-%d_%H%M%S')}.json"
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=key,
        Body=json.dumps(summary, indent=2, default=str).encode(),
        ContentType="application/json",
        CacheControl="public, max-age=300",
        ACL="public-read",
    )
    # "latest" pointer for easy debugging from a dashboard
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=f"{S3_LOG_PREFIX}/latest.json",
        Body=json.dumps(summary, indent=2, default=str).encode(),
        ContentType="application/json",
        CacheControl="public, max-age=300",
        ACL="public-read",
    )

    print(f"[prewarm] DONE · {n_ok}/{len(universe)} succeeded · "
          f"{summary['wall_seconds']}s wall time · log: s3://{S3_BUCKET}/{key}")

    return {
        "statusCode": 200,
        "body": json.dumps({
            "n_succeeded":   n_ok,
            "n_failed":      n_err,
            "wall_seconds":  summary["wall_seconds"],
            "log_key":       key,
            "results_summary": [{"ticker": r["ticker"], "status": r["status"],
                                   "elapsed_s": r.get("elapsed_s")} for r in results],
        }),
    }
