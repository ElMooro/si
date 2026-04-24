#!/usr/bin/env python3
"""
Test replacement price endpoints — before we patch outcome-checker,
confirm which endpoints actually work with our keys.

Candidates:
  Polygon (free tier):
    - /v2/aggs/ticker/{ticker}/prev          — prev day OHLC (widely free)
    - /v2/aggs/ticker/{ticker}/range/1/day/{from}/{to}  — daily bars

  FMP (post-Aug-2025 subscription):
    - /api/v3/quote/{ticker}                 — full quote (modern)
    - /stable/quote?symbol={ticker}          — v4 stable endpoint

  Crypto (already works):
    - Polygon /v2/last/trade/X:BTCUSD        — confirmed working
    - CoinGecko /simple/price                — backup
"""
import urllib.request
import urllib.error
import json
from ops_report import report

POLYGON_KEY = "zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d"
FMP_KEY = "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb"


def http(url, timeout=10):
    try:
        with urllib.request.urlopen(url, timeout=timeout) as r:
            body = r.read().decode()
            return {"ok": True, "status": r.status, "body": body[:500]}
    except urllib.error.HTTPError as e:
        try: body = e.read().decode()
        except: body = ""
        return {"ok": False, "status": e.code, "body": body[:300]}
    except Exception as e:
        return {"ok": False, "status": "error", "body": str(e)}


with report("price_endpoint_test") as r:
    r.heading("Test replacement price endpoints")

    for ticker in ["SPY", "AAPL", "GLD"]:
        r.section(f"Ticker {ticker}")

        # Polygon prev
        url = f"https://api.polygon.io/v2/aggs/ticker/{ticker}/prev?adjusted=true&apiKey={POLYGON_KEY}"
        res = http(url)
        r.log(f"  Polygon /prev:      status={res['status']} {'✓' if res['ok'] else '✗'}")
        if res["ok"]:
            r.log(f"    body: {res['body'][:250]}")

        # Polygon daily range (last 7 days)
        url = f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/day/2026-04-17/2026-04-24?adjusted=true&apiKey={POLYGON_KEY}"
        res = http(url)
        r.log(f"  Polygon /range/1/day: status={res['status']} {'✓' if res['ok'] else '✗'}")
        if res["ok"]:
            r.log(f"    body: {res['body'][:250]}")

        # FMP v3 quote (not quote-short)
        url = f"https://financialmodelingprep.com/api/v3/quote/{ticker}?apikey={FMP_KEY}"
        res = http(url)
        r.log(f"  FMP v3 /quote:       status={res['status']} {'✓' if res['ok'] else '✗'}")
        if res["ok"]:
            r.log(f"    body: {res['body'][:250]}")

        # FMP stable
        url = f"https://financialmodelingprep.com/stable/quote?symbol={ticker}&apikey={FMP_KEY}"
        res = http(url)
        r.log(f"  FMP /stable/quote:   status={res['status']} {'✓' if res['ok'] else '✗'}")
        if res["ok"]:
            r.log(f"    body: {res['body'][:250]}")

    # Crypto via CoinGecko as backup
    r.section("Crypto fallback: CoinGecko")
    for coin_id, symbol in [("bitcoin", "BTC-USD"), ("ethereum", "ETH-USD")]:
        url = f"https://api.coingecko.com/api/v3/simple/price?ids={coin_id}&vs_currencies=usd"
        res = http(url)
        r.log(f"  CoinGecko {symbol}: status={res['status']} {'✓' if res['ok'] else '✗'}")
        if res["ok"]:
            r.log(f"    body: {res['body'][:200]}")

    r.log("Done")
