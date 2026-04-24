"""
justhodl-outcome-checker
Runs weekly. For every pending signal in DynamoDB whose check window
has elapsed, fetches the actual market price and scores the prediction.
"""

import json
import boto3
import time
import urllib.request
import urllib.error
from datetime import datetime, timezone
from decimal import Decimal
from boto3.dynamodb.conditions import Attr

dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
ssm      = boto3.client("ssm",       region_name="us-east-1")
s3       = boto3.client("s3",        region_name="us-east-1")

SIGNALS_TABLE  = "justhodl-signals"
OUTCOMES_TABLE = "justhodl-outcomes"
S3_BUCKET      = "justhodl-dashboard-live"

POLYGON_KEY    = "zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d"
FMP_KEY        = "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb"


def float_to_decimal(obj):
    if isinstance(obj, float):
        return Decimal(str(round(obj, 6)))
    if isinstance(obj, dict):
        return {k: float_to_decimal(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [float_to_decimal(v) for v in obj]
    return obj


# ─── Price fetchers (v2 — endpoints that actually work on our plan) ─────
# Replaced 2026-04-24: old /v2/last/trade needs paid Polygon ($29/mo),
# old /v3/quote-short was retired by FMP Aug 2025. All outcomes were
# silently failing with HTTP 403 before this fix.

def get_current_price_polygon(ticker):
    """Get latest (previous day close) price from Polygon — free tier."""
    url = f"https://api.polygon.io/v2/aggs/ticker/{ticker}/prev?adjusted=true&apiKey={POLYGON_KEY}"
    try:
        with urllib.request.urlopen(url, timeout=10) as r:
            data = json.loads(r.read().decode())
            results = data.get("results") or []
            if results and isinstance(results, list):
                return float(results[0].get("c") or 0)
    except urllib.error.HTTPError as e:
        print(f"[PRICE] Polygon HTTP {e.code} for {ticker}")
    except Exception as e:
        print(f"[PRICE] Polygon error for {ticker}: {e}")
    return None


def get_current_price_fmp(ticker):
    """Get current price from FMP /stable/quote — modern premium endpoint."""
    url = f"https://financialmodelingprep.com/stable/quote?symbol={ticker}&apikey={FMP_KEY}"
    try:
        with urllib.request.urlopen(url, timeout=10) as r:
            data = json.loads(r.read().decode())
            if data and isinstance(data, list) and len(data) > 0:
                price = data[0].get("price")
                if price is not None:
                    return float(price)
    except urllib.error.HTTPError as e:
        print(f"[PRICE] FMP HTTP {e.code} for {ticker}")
    except Exception as e:
        print(f"[PRICE] FMP error for {ticker}: {e}")
    return None


def get_coingecko_price(ticker):
    """Crypto fallback via CoinGecko (free, no key needed)."""
    coingecko_map = {
        "BTC-USD": "bitcoin",
        "BTC":     "bitcoin",
        "ETH-USD": "ethereum",
        "ETH":     "ethereum",
        "SOL-USD": "solana",
        "SOL":     "solana",
    }
    cg_id = coingecko_map.get(ticker.upper())
    if not cg_id:
        return None
    url = f"https://api.coingecko.com/api/v3/simple/price?ids={cg_id}&vs_currencies=usd"
    try:
        with urllib.request.urlopen(url, timeout=10) as r:
            data = json.loads(r.read().decode())
            return float(data.get(cg_id, {}).get("usd") or 0)
    except Exception as e:
        print(f"[PRICE] CoinGecko error for {ticker}: {e}")
    return None


def get_price(ticker):
    """
    Get current price with fallback chain.
      1. Crypto → CoinGecko first (most reliable, free)
      2. Stocks/ETFs → FMP /stable first (real-time on our tier)
      3. Polygon /prev as fallback (free tier, previous close)
      4. S3 report.json as last resort
    """
    if not ticker:
        return None

    # Crypto path: CoinGecko first
    if ticker.upper() in ("BTC-USD", "ETH-USD", "SOL-USD", "BTC", "ETH", "SOL"):
        price = get_coingecko_price(ticker)
        if price:
            return price
        # Fall back to Polygon crypto endpoint
        polygon_crypto_map = {
            "BTC-USD": "X:BTCUSD", "BTC": "X:BTCUSD",
            "ETH-USD": "X:ETHUSD", "ETH": "X:ETHUSD",
        }
        polygon_ticker = polygon_crypto_map.get(ticker.upper(), ticker)
        price = get_current_price_polygon(polygon_ticker)
        if price:
            return price

    # Stocks/ETFs: FMP first (real-time), then Polygon /prev
    price = get_current_price_fmp(ticker)
    if price:
        return price

    price = get_current_price_polygon(ticker)
    if price:
        return price

    # Last resort: S3 report.json (note: schema is nested under stocks[ticker].price)
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key="data/report.json")
        report = json.loads(obj["Body"].read().decode())
        # Try direct stocks map
        stocks = report.get("stocks") or {}
        if ticker in stocks and isinstance(stocks[ticker], dict):
            price = stocks[ticker].get("price")
            if price is not None:
                return float(price)
        # Legacy fallback keys
        legacy_map = {
            "SPY":     report.get("sp500"),
            "BTC-USD": report.get("btcPrice"),
            "ETH-USD": report.get("ethPrice"),
            "GLD":     report.get("goldPrice"),
            "USO":     report.get("oilPrice"),
        }
        price = legacy_map.get(ticker)
        if price:
            return float(price)
    except Exception:
        pass

    return None


def get_benchmark_price(benchmark="SPY"):
    """Get benchmark price for relative comparisons."""
    return get_price(benchmark)


# ─── Outcome scoring ───────────────────────────────────────────────────────
def score_directional(predicted_direction, baseline_price, current_price, threshold_pct=0.5):
    """
    Score a directional prediction.
    Returns: (correct: bool, actual_direction: str, return_pct: float)
    """
    if not baseline_price or not current_price or baseline_price == 0:
        return None, "UNKNOWN", 0.0

    return_pct = ((current_price - baseline_price) / baseline_price) * 100

    if   return_pct >  threshold_pct: actual = "UP"
    elif return_pct < -threshold_pct: actual = "DOWN"
    else:                             actual = "NEUTRAL"

    if predicted_direction == "UP"      and actual == "UP":      correct = True
    elif predicted_direction == "DOWN"  and actual == "DOWN":    correct = True
    elif predicted_direction == "NEUTRAL" and actual == "NEUTRAL": correct = True
    else:                                                         correct = False

    return correct, actual, return_pct


def score_relative(predicted_direction, ticker, benchmark,
                   baseline_price, current_price,
                   baseline_benchmark, current_benchmark):
    """
    Score a relative prediction (outperform/underperform benchmark).
    Returns: (correct: bool, excess_return: float)
    """
    if not all([baseline_price, current_price, baseline_benchmark, current_benchmark]):
        return None, 0.0
    if baseline_price == 0 or baseline_benchmark == 0:
        return None, 0.0

    asset_return     = ((current_price - baseline_price) / baseline_price) * 100
    benchmark_return = ((current_benchmark - baseline_benchmark) / baseline_benchmark) * 100
    excess_return    = asset_return - benchmark_return

    if   predicted_direction == "OUTPERFORM":   correct = excess_return > 0
    elif predicted_direction == "UNDERPERFORM": correct = excess_return < 0
    else:                                       correct = abs(excess_return) < 1.0

    return correct, excess_return


# ─── Main outcome checker ──────────────────────────────────────────────────
def check_pending_signals():
    """Scan DynamoDB for signals whose check windows have elapsed and score them."""
    table   = dynamodb.Table(SIGNALS_TABLE)
    now     = datetime.now(timezone.utc)
    now_iso = now.isoformat()

    # Scan for pending/partial signals
    results = table.scan(
        FilterExpression=Attr("status").is_in(["pending", "partial"])
    )
    signals = results.get("Items", [])

    # Handle DynamoDB pagination
    while "LastEvaluatedKey" in results:
        results = table.scan(
            FilterExpression=Attr("status").is_in(["pending", "partial"]),
            ExclusiveStartKey=results["LastEvaluatedKey"]
        )
        signals += results.get("Items", [])

    print(f"[CHECKER] Found {len(signals)} pending signals")

    outcomes_table   = dynamodb.Table(OUTCOMES_TABLE)
    processed_count  = 0
    price_cache      = {}  # cache prices to avoid repeated API calls

    for signal in signals:
        signal_id   = signal["signal_id"]
        signal_type = signal["signal_type"]
        ticker      = signal.get("measure_against") or signal.get("ticker")
        benchmark   = signal.get("benchmark", "SPY")
        pred_dir    = signal.get("predicted_direction", "NEUTRAL")
        baseline    = float(signal.get("baseline_price") or 0)
        check_ts    = signal.get("check_timestamps", {})
        existing_outcomes = dict(signal.get("outcomes", {}))
        pred_type   = "relative" if pred_dir in ("OUTPERFORM", "UNDERPERFORM") else "directional"

        outcomes_updated = False

        for window_key, check_time_iso in check_ts.items():
            # Skip already evaluated windows
            if window_key in existing_outcomes:
                continue

            # Check if this window has elapsed
            if check_time_iso > now_iso:
                continue

            # Fetch current price (cached)
            if ticker not in price_cache:
                price_cache[ticker] = get_price(ticker)
                time.sleep(0.3)  # rate limit

            current_price = price_cache[ticker]

            if not current_price:
                print(f"[CHECKER] No price for {ticker}, skipping window {window_key}")
                continue

            # Score the prediction
            if pred_type == "relative":
                if benchmark not in price_cache:
                    price_cache[benchmark] = get_price(benchmark)
                    time.sleep(0.3)

                baseline_bm  = float(signal.get("baseline_benchmark_price") or 0)
                current_bm   = price_cache.get(benchmark)
                correct, excess = score_relative(
                    pred_dir, ticker, benchmark,
                    baseline, current_price,
                    baseline_bm, current_bm
                )
                outcome = {
                    "correct":        correct,
                    "excess_return":  float(excess) if excess else 0.0,
                    "asset_price":    float(current_price),
                    "benchmark_price": float(current_bm) if current_bm else None,
                    "checked_at":     now_iso,
                }
            else:
                correct, actual_dir, return_pct = score_directional(
                    pred_dir, baseline, current_price
                )
                outcome = {
                    "correct":           correct,
                    "actual_direction":  actual_dir,
                    "return_pct":        float(return_pct),
                    "price_at_signal":   float(baseline),
                    "price_at_check":    float(current_price),
                    "checked_at":        now_iso,
                }

            existing_outcomes[window_key] = outcome
            outcomes_updated = True
            print(f"[CHECKER] {signal_type} [{window_key}] → "
                  f"{'✅ CORRECT' if correct else '❌ WRONG'} "
                  f"(predicted {pred_dir}, got {outcome.get('actual_direction', '')} "
                  f"{outcome.get('return_pct', outcome.get('excess_return', '')):.2f}%)")

            # Also write to outcomes table for easy aggregation
            outcomes_table.put_item(Item=float_to_decimal({
                "outcome_id":    f"{signal_id}_{window_key}",
                "signal_id":     signal_id,
                "signal_type":   signal_type,
                "signal_value":  signal.get("signal_value"),
                "window_key":    window_key,
                "correct":       correct,
                "predicted_dir": pred_dir,
                "outcome":       outcome,
                "logged_at":     signal.get("logged_at"),
                "checked_at":    now_iso,
                "ttl":           int((now.timestamp()) + 365 * 86400),
            }))

        if not outcomes_updated:
            continue

        # Determine new status
        all_windows = set(check_ts.keys())
        done_windows = set(existing_outcomes.keys())
        if done_windows >= all_windows:
            new_status = "complete"
        elif done_windows:
            new_status = "partial"
        else:
            new_status = "pending"

        # Update signal record
        table.update_item(
            Key={"signal_id": signal_id},
            UpdateExpression="SET outcomes = :o, #s = :s, last_checked = :t",
            ExpressionAttributeValues={
                ":o": float_to_decimal(existing_outcomes),
                ":s": new_status,
                ":t": now_iso,
            },
            ExpressionAttributeNames={"#s": "status"}
        )
        processed_count += 1

    print(f"[CHECKER] Processed {processed_count} signals")
    return processed_count


def lambda_handler(event, context):
    processed = check_pending_signals()
    return {
        "statusCode": 200,
        "body": json.dumps({
            "processed": processed,
            "timestamp": datetime.now(timezone.utc).isoformat()
        })
    }
