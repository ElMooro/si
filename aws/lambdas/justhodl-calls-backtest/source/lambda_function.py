"""Publish a diagnostic replay of explicit, qualified Calls allocations.

Calls v2 excludes legacy narrative, UNKNOWN, WAIT and HOLD from allocation
instructions. The pure replay kernel uses subsequent regular-session opens;
missing execution costs keep all results in diagnostic-only status.
"""
import json
import time
import urllib.request
from datetime import datetime, timezone, timedelta
import boto3
from managed_secret import managed_secret
from calls_contract import timestamp
from calls_replay import replay

BUCKET = "justhodl-dashboard-live"
S3 = boto3.client("s3", region_name="us-east-1")
POLYGON_KEY = managed_secret(('POLYGON_KEY', 'POLYGON_API_KEY', 'POLY_KEY'), ("/justhodl/polygon/api-key",))


def fetch_spy_daily(start_iso, end_iso):
    """Return actual session open/close bars; provider failures are not empty history."""
    url = f"https://api.polygon.io/v2/aggs/ticker/SPY/range/1/day/{start_iso}/{end_iso}?adjusted=true&sort=asc&limit=5000&apiKey={POLYGON_KEY}"
    req = urllib.request.Request(url, headers={"Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=25) as response:
        data = json.load(response)
    if not isinstance(data.get("results"), list) or not data["results"]:
        raise ValueError("SPY price history unavailable")
    if data.get("next_url"):
        raise ValueError("SPY history truncated; pagination required")
    result = {}
    for row in data["results"]:
        day = datetime.fromtimestamp(row["t"] / 1000, timezone.utc).strftime("%Y-%m-%d")
        if day in result:
            raise ValueError("Duplicate SPY session")
        result[day] = {"open": row.get("o"), "close": row.get("c")}
    return result


def publish(out):
    body = json.dumps(out, allow_nan=False).encode()
    # Preserve the existing key for consumers and provide a public data read model.
    S3.put_object(Bucket=BUCKET, Key="backtest/calls-results.json", Body=body,
                  ContentType="application/json", CacheControl="public, max-age=60")
    S3.put_object(Bucket=BUCKET, Key="data/calls-replay.json", Body=body,
                  ContentType="application/json", CacheControl="public, max-age=60")


def lambda_handler(event=None, context=None):
    started, now = time.time(), datetime.now(timezone.utc)
    try:
        document = json.loads(S3.get_object(Bucket=BUCKET, Key="data/decisive-call-history.json")["Body"].read())
        rows = document.get("snapshots")
        if not isinstance(rows, list):
            raise ValueError("Invalid Calls ledger")
        candidates = [r for r in rows if isinstance(r, dict) and r.get("decision_status") == "VALID"
                      and r.get("sizing_eligible") is True and timestamp(r.get("timestamp"))]
        prices = {}
        if candidates:
            start = min(timestamp(r["timestamp"]) for r in candidates) - timedelta(days=5)
            prices = fetch_spy_daily(start.date().isoformat(), now.date().isoformat())
        out = replay(rows, prices, now)
        out["ledger_generated_at"] = document.get("generated_at") or document.get("last_updated")
        out["duration_s"] = round(time.time() - started, 2)
        publish(out)
        return {"statusCode": 200, "body": json.dumps({"status": out["status"],
                "n_calls": out["summary"]["n_calls"], "calibration_eligible": False})}
    except Exception as exc:
        # Retire a prior successful read model instead of serving it as current.
        out = replay([], {}, now)
        out.update(status="error", reason="ledger_or_price_read_failed", error_type=type(exc).__name__)
        publish(out)
        return {"statusCode": 503, "body": json.dumps({"status": "error", "reason": out["reason"]})}
