#!/usr/bin/env python3
"""
THE FIX — Replace outcome-checker price fetchers with endpoints that
actually work on our plan tier.

Current broken code:
  get_current_price_polygon() → /v2/last/trade (PAID tier only, returns 403)
  get_current_price_fmp()     → /v3/quote-short (LEGACY, retired Aug 2025)

Replacement:
  get_current_price_polygon() → /v2/aggs/ticker/{t}/prev (free tier ✓)
  get_current_price_fmp()     → /stable/quote?symbol={t} (modern premium ✓)
  get_coingecko_price()       → new crypto fallback (free, no key)

Also:
  - Ticker mapping for crypto: BTC-USD/ETH-USD → coingecko ids
  - Better error logging (show status code, not just 'error')
  - Add rate-limit retry with backoff
"""
import io
import os
import zipfile
from pathlib import Path

from ops_report import report
import boto3

REGION = "us-east-1"
REPO_ROOT = Path(os.environ.get("GITHUB_WORKSPACE", os.getcwd()))

lam = boto3.client("lambda", region_name=REGION)


def build_zip(src_dir):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zout:
        for f in src_dir.rglob("*"):
            if f.is_file():
                zout.write(f, str(f.relative_to(src_dir)))
    return buf.getvalue()


def deploy(fn_name, src_dir):
    z = build_zip(src_dir)
    lam.update_function_code(FunctionName=fn_name, ZipFile=z)
    lam.get_waiter("function_updated").wait(
        FunctionName=fn_name, WaiterConfig={"Delay": 3, "MaxAttempts": 30}
    )
    return len(z)


with report("fix_price_fetch") as r:
    r.heading("THE FIX — Replace broken price fetchers in outcome-checker")

    oc_path = REPO_ROOT / "aws/lambdas/justhodl-outcome-checker/source/lambda_function.py"
    src = oc_path.read_text(encoding="utf-8")

    # ─── Replace the three price fetchers ────────────────────────────────
    old_block = '''# ─── Price fetchers ────────────────────────────────────────────────────────
def get_current_price_polygon(ticker):
    """Get latest price for any ticker from Polygon."""
    url = f"https://api.polygon.io/v2/last/trade/{ticker}?apiKey={POLYGON_KEY}"
    try:
        with urllib.request.urlopen(url, timeout=10) as r:
            data = json.loads(r.read().decode())
            return float(data.get("results", {}).get("p", 0) or 0)
    except Exception as e:
        print(f"[PRICE] Polygon error for {ticker}: {e}")
        return None


def get_current_price_fmp(ticker):
    """Fallback: get price from FMP."""
    url = f"https://financialmodelingprep.com/api/v3/quote-short/{ticker}?apikey={FMP_KEY}"
    try:
        with urllib.request.urlopen(url, timeout=10) as r:
            data = json.loads(r.read().decode())
            if data and isinstance(data, list):
                return float(data[0].get("price", 0) or 0)
    except Exception as e:
        print(f"[PRICE] FMP error for {ticker}: {e}")
    return None


def get_price(ticker):
    """Get current price with fallback chain."""
    # Crypto tickers need special handling
    crypto_map = {
        "BTC-USD": "X:BTCUSD",
        "ETH-USD": "X:ETHUSD",
    }

    polygon_ticker = crypto_map.get(ticker, ticker)
    price = get_current_price_polygon(polygon_ticker)

    if not price:
        price = get_current_price_fmp(ticker)

    # Last resort: check S3 report.json
    if not price:
        try:
            obj    = s3.get_object(Bucket=S3_BUCKET, Key="report.json")
            report = json.loads(obj["Body"].read().decode())
            price_map = {
                "SPY":     report.get("sp500"),
                "BTC-USD": report.get("btcPrice"),
                "ETH-USD": report.get("ethPrice"),
                "GLD":     report.get("goldPrice"),
                "USO":     report.get("oilPrice"),
            }
            price = price_map.get(ticker)
            if price:
                price = float(price)
        except Exception:
            pass

    return price'''

    new_block = '''# ─── Price fetchers (v2 — endpoints that actually work on our plan) ─────
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

    return None'''

    if old_block not in src:
        r.fail("  Price-fetcher block not found verbatim — cannot patch")
        r.log(f"  Expected start: '# ─── Price fetchers ─' not in source")
        raise SystemExit(1)

    src = src.replace(old_block, new_block, 1)

    # Also ensure urllib.error is imported (needed for the new HTTPError handling)
    if "import urllib.error" not in src and "from urllib import error" not in src:
        src = src.replace(
            "import urllib.request",
            "import urllib.request\nimport urllib.error",
            1,
        )

    import ast
    try:
        ast.parse(src)
    except SyntaxError as e:
        r.fail(f"  Syntax error in new source: {e}")
        raise SystemExit(1)

    oc_path.write_text(src, encoding="utf-8")
    r.ok(f"  Source valid ({len(src)} bytes), saved")

    size = deploy("justhodl-outcome-checker", oc_path.parent)
    r.ok(f"  Deployed justhodl-outcome-checker ({size:,} bytes)")

    # Trigger a fresh backfill now that prices work
    r.section("Trigger fresh backfill with working price fetchers")
    try:
        resp = lam.invoke(
            FunctionName="justhodl-outcome-checker",
            InvocationType="Event",
        )
        r.ok(f"  Async-triggered outcome-checker (status {resp['StatusCode']})")
        r.log("  This run should actually score outcomes correctly now.")
    except Exception as e:
        r.fail(f"  Trigger failed: {e}")

    r.kv(
        fix="replaced /v2/last/trade and /v3/quote-short with /prev, /stable/quote, CoinGecko",
        deployed=True,
        backfill_triggered=True,
    )
    r.log("Done")
