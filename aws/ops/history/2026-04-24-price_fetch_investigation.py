#!/usr/bin/env python3
"""
CRITICAL BUG: outcome-checker can't fetch prices.

Every single outcome in today's backfill scored as UNKNOWN/None
because get_price() returns None for SPY, GLD, USO, BTC-USD, etc.

This is THE reason accuracy=0.0 for all signals — not the sentiment
framing theory. The learning loop has been silently broken.

Investigate:
  A. Is Polygon returning actual prices? Test one ticker directly.
  B. Is FMP returning actual prices? Test one ticker directly.
  C. What HTTP errors is the Lambda actually seeing?
  D. Is the API key valid?

If root cause is identifiable, apply fix. Otherwise document.
"""
import urllib.request
import urllib.error
import json
from datetime import datetime, timezone, timedelta
from ops_report import report
import boto3

REGION = "us-east-1"
ssm = boto3.client("ssm", region_name=REGION)
logs = boto3.client("logs", region_name=REGION)
lam = boto3.client("lambda", region_name=REGION)

# Pull key from Lambda env (same source the outcome-checker uses)
POLYGON_KEY = "zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d"
FMP_KEY = "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb"


def test_polygon(ticker):
    url = f"https://api.polygon.io/v2/last/trade/{ticker}?apiKey={POLYGON_KEY}"
    try:
        with urllib.request.urlopen(url, timeout=10) as r:
            status = r.status
            body = r.read().decode()
            data = json.loads(body)
            return {"ok": True, "status": status, "data_preview": body[:500]}
    except urllib.error.HTTPError as e:
        try:
            body = e.read().decode()
        except:
            body = ""
        return {"ok": False, "status": e.code, "error": str(e), "body": body[:500]}
    except Exception as e:
        return {"ok": False, "status": "error", "error": str(e)}


def test_fmp(ticker):
    url = f"https://financialmodelingprep.com/api/v3/quote-short/{ticker}?apikey={FMP_KEY}"
    try:
        with urllib.request.urlopen(url, timeout=10) as r:
            status = r.status
            body = r.read().decode()
            data = json.loads(body)
            return {"ok": True, "status": status, "data_preview": body[:500]}
    except urllib.error.HTTPError as e:
        try:
            body = e.read().decode()
        except:
            body = ""
        return {"ok": False, "status": e.code, "error": str(e), "body": body[:500]}
    except Exception as e:
        return {"ok": False, "status": "error", "error": str(e)}


with report("price_fetch_investigation") as r:
    r.heading("Investigate why outcome-checker returns None for all prices")

    # ═══════════ A. Test Polygon directly ═══════════
    r.section("A. Test Polygon /v2/last/trade endpoint")
    for ticker in ["SPY", "AAPL", "NVDA", "X:BTCUSD", "GLD"]:
        res = test_polygon(ticker)
        r.log(f"  {ticker}: status={res.get('status')} ok={res.get('ok')}")
        if res.get("body"):
            r.log(f"    body: {res['body'][:200]}")
        elif res.get("data_preview"):
            r.log(f"    preview: {res['data_preview'][:200]}")
        elif res.get("error"):
            r.log(f"    error: {res['error']}")

    # ═══════════ B. Test FMP directly ═══════════
    r.section("B. Test FMP /api/v3/quote-short endpoint")
    for ticker in ["SPY", "AAPL", "NVDA", "GLD"]:
        res = test_fmp(ticker)
        r.log(f"  {ticker}: status={res.get('status')} ok={res.get('ok')}")
        if res.get("body"):
            r.log(f"    body: {res['body'][:200]}")
        elif res.get("data_preview"):
            r.log(f"    preview: {res['data_preview'][:200]}")
        elif res.get("error"):
            r.log(f"    error: {res['error']}")

    # ═══════════ C. What errors is outcome-checker logging? ═══════════
    r.section("C. Full outcome-checker error lines from recent run")
    try:
        streams = logs.describe_log_streams(
            logGroupName="/aws/lambda/justhodl-outcome-checker",
            orderBy="LastEventTime", descending=True, limit=1,
        ).get("logStreams", [])
        if streams:
            s = streams[0]
            start = int((datetime.now(timezone.utc) - timedelta(minutes=15)).timestamp() * 1000)
            ev = logs.get_log_events(
                logGroupName="/aws/lambda/justhodl-outcome-checker",
                logStreamName=s["logStreamName"],
                startTime=start, limit=1000, startFromHead=True,
            )
            events = ev.get("events", [])
            err_lines = []
            for e in events:
                m = e.get("message", "").strip()
                if "[PRICE]" in m or "error" in m.lower() or "429" in m or "403" in m:
                    err_lines.append(m)
            r.log(f"  Found {len(err_lines)} error lines in last run:")
            for e in err_lines[:20]:
                r.log(f"    {e[:240]}")
    except Exception as e:
        r.warn(f"  {e}")

    # ═══════════ D. Check outcome-checker env has keys ═══════════
    r.section("D. Outcome-checker environment config")
    try:
        cfg = lam.get_function_configuration(FunctionName="justhodl-outcome-checker")
        env_keys = sorted(list((cfg.get("Environment", {}) or {}).get("Variables", {}).keys()))
        r.log(f"  Env var keys: {env_keys}")
        r.log(f"  Timeout: {cfg.get('Timeout')}s, Memory: {cfg.get('MemorySize')} MB")
    except Exception as e:
        r.warn(f"  {e}")

    r.log("Done")
