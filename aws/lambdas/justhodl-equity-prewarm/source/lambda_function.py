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
EDGAR_LAMBDA_URL = "https://ru3djltl3oucvsocjrih37sowu0fxgkm.lambda-url.us-east-1.on.aws/"
PARALLEL_WORKERS = 6        # 6 concurrent research-Lambda invocations
                            # Each ticker also kicks off an EDGAR pre-warm in
                            # PARALLEL inside the worker, so total wall time
                            # is dominated by research (~95s), not the sum.
PER_TICKER_TIMEOUT = 180    # seconds — matches research Lambda's own timeout
EDGAR_TIMEOUT = 120         # seconds — matches edgar-insiders Lambda
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
def _call_research(ticker: str) -> dict:
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
                "status":    "ok",
                "http":      r.status,
                "elapsed_s": elapsed,
                "size_kb":   round(len(body) / 1024, 1),
                "rating":    rating,
                "ev_12m":    ev,
            }
        except Exception:
            return {"status": "ok_unparseable",
                    "http": r.status, "elapsed_s": elapsed, "size_bytes": len(body)}
    except urllib.error.HTTPError as e:
        return {"status": "http_error", "code": e.code, "msg": e.reason,
                "elapsed_s": round(time.time() - t0, 1)}
    except Exception as e:
        return {"status": "error", "error": str(e)[:300],
                "elapsed_s": round(time.time() - t0, 1)}


def _call_edgar(ticker: str) -> dict:
    """Call the EDGAR insiders Lambda. Returns signal summary or error."""
    url = f"{EDGAR_LAMBDA_URL}?ticker={ticker}&refresh=1"
    req = urllib.request.Request(url, headers=HTTP_HEADERS)
    t0 = time.time()
    try:
        with urllib.request.urlopen(req, timeout=EDGAR_TIMEOUT) as r:
            body = r.read()
            elapsed = round(time.time() - t0, 1)
        try:
            doc = json.loads(body)
            return {
                "status":            "ok",
                "elapsed_s":         elapsed,
                "n_filings_90d":     doc.get("n_filings_90d"),
                "n_buys":            doc.get("n_buys"),
                "n_sells":           doc.get("n_sells"),
                "signal_label":      doc.get("signal_label"),
                "signal_score":      doc.get("signal_score"),
                "sell_acceleration": doc.get("sell_acceleration"),
                "cluster_detected":  doc.get("cluster_detected"),
            }
        except Exception:
            return {"status": "ok_unparseable", "elapsed_s": elapsed}
    except urllib.error.HTTPError as e:
        return {"status": "http_error", "code": e.code,
                "elapsed_s": round(time.time() - t0, 1)}
    except Exception as e:
        return {"status": "error", "error": str(e)[:200],
                "elapsed_s": round(time.time() - t0, 1)}


def prewarm_ticker(ticker: str) -> dict:
    """Pre-warm BOTH research and EDGAR data for one ticker, in parallel.

    Research Lambda takes ~90-100s (Claude synthesis dominates).
    EDGAR Lambda takes ~10-15s (SEC EDGAR fetches dominate).

    Running them in parallel inside one worker means total wall time per
    ticker is max(research, edgar) ≈ research time. EDGAR pre-warm is
    essentially free within the same time slot.
    """
    t0 = time.time()
    with ThreadPoolExecutor(max_workers=2) as ex:
        f_research = ex.submit(_call_research, ticker)
        f_edgar    = ex.submit(_call_edgar, ticker)
        research = f_research.result()
        edgar    = f_edgar.result()

    # Top-level shape mirrors the original prewarm format (status + rating)
    # but adds an edgar sub-dict
    return {
        "ticker":    ticker,
        "status":    research.get("status", "?"),
        "http":      research.get("http"),
        "elapsed_s": round(time.time() - t0, 1),
        "size_kb":   research.get("size_kb"),
        "rating":    research.get("rating"),
        "ev_12m":    research.get("ev_12m"),
        "edgar":     edgar,
    }


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
            edgar_summary = res.get("edgar", {}) or {}
            edgar_str = ""
            if edgar_summary.get("status") == "ok":
                edgar_str = (f"| EDGAR {edgar_summary.get('signal_label','?')} "
                              f"(B{edgar_summary.get('n_buys',0)}/S{edgar_summary.get('n_sells',0)})")
            elif edgar_summary.get("status"):
                edgar_str = f"| EDGAR {edgar_summary['status']}"
            print(f"[prewarm] {i:>3}/{len(universe)} {res['ticker']:6s} "
                    f"{res.get('status','?'):6s} {res.get('elapsed_s','?')}s "
                    f"{res.get('rating','—')} {edgar_str}")

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

    # Write log to S3. Bucket has ACLs disabled; equity-prewarm/* is public
    # via bucket policy (PublicReadEquityPrewarm statement, ops 1151).
    s3 = boto3.client("s3")
    key = f"{S3_LOG_PREFIX}/{started.strftime('%Y-%m-%d_%H%M%S')}.json"
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=key,
        Body=json.dumps(summary, indent=2, default=str).encode(),
        ContentType="application/json",
        CacheControl="public, max-age=300",
    )
    # "latest" pointer for easy debugging from a dashboard
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=f"{S3_LOG_PREFIX}/latest.json",
        Body=json.dumps(summary, indent=2, default=str).encode(),
        ContentType="application/json",
        CacheControl="public, max-age=300",
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
