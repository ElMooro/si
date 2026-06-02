"""1163 — Test equity-prewarm with EDGAR parallel for 3 fresh tickers."""
import json, time, urllib.request, urllib.error, ssl
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/1163_prewarm_edgar_parallel.json"
ctx = ssl.create_default_context()

lam = boto3.client("lambda", region_name="us-east-1")
s3 = boto3.client("s3", region_name="us-east-1")

out = {"started": datetime.now(timezone.utc).isoformat()}

# Pick 3 small/mid-caps that aren't in our prewarm universe so this is genuinely fresh
test_tickers = ["NET", "DDOG", "TEAM"]
out["test_tickers"] = test_tickers

# Async-invoke prewarm (sync invoke would timeout — Lambda takes >60s)
print("[1163] Async-invoking prewarm Lambda with 3 fresh tickers (NET, DDOG, TEAM)...")
try:
    resp = lam.invoke(
        FunctionName="justhodl-equity-prewarm",
        InvocationType="Event",  # async — fire-and-forget
        Payload=json.dumps({"tickers": test_tickers}).encode(),
    )
    out["invoke"] = {"status_code": resp["StatusCode"], "ok": resp["StatusCode"] in (202, 200)}
    print(f"  Async invoke status: {resp['StatusCode']}")
except Exception as e:
    out["invoke"] = {"error": str(e)}
    print(f"  ❌ Invoke failed: {e}")

# Wait ~3 min for it to chew through 3 tickers (each ~95s, 6 workers means all 3 in one wave)
print("[1163] Waiting 180s for prewarm + EDGAR to complete...")
time.sleep(180)

# Verify by reading latest.json log
print("[1163] Reading latest run log...")
try:
    obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="equity-prewarm/runs/latest.json")
    log = json.loads(obj["Body"].read())
    out["log"] = {
        "wall_seconds": log.get("wall_seconds"),
        "n_succeeded": log.get("n_succeeded"),
        "n_failed": log.get("n_failed"),
        "tickers": log.get("tickers"),
        "results": [
            {"ticker": r.get("ticker"),
             "status": r.get("status"),
             "elapsed_s": r.get("elapsed_s"),
             "rating": r.get("rating"),
             "edgar": r.get("edgar"),
             }
            for r in (log.get("results") or [])
        ],
    }
    print(f"  Latest run: {log.get('n_succeeded')}/{log.get('n_succeeded',0)+log.get('n_failed',0)} ok, wall={log.get('wall_seconds')}s")
except Exception as e:
    out["log_error"] = str(e)[:300]

# Verify the EDGAR files actually appeared in S3
print("[1163] Verifying EDGAR files in S3...")
for t in test_tickers:
    try:
        obj = s3.get_object(Bucket="justhodl-dashboard-live", Key=f"edgar-insiders/{t}.json")
        body = json.loads(obj["Body"].read())
        out.setdefault("edgar_in_s3", {})[t] = {
            "ok": True,
            "size_kb": round(obj["ContentLength"]/1024, 1),
            "n_filings_90d": body.get("n_filings_90d"),
            "signal_label": body.get("signal_label"),
            "signal_score": body.get("signal_score"),
        }
        print(f"  ✓ edgar-insiders/{t}.json {round(obj['ContentLength']/1024,1)}KB · {body.get('signal_label')}")
    except Exception as e:
        out.setdefault("edgar_in_s3", {})[t] = {"error": str(e)[:200]}
        print(f"  ❌ edgar-insiders/{t}.json: {e}")

out["finished"] = datetime.now(timezone.utc).isoformat()
with open(REPORT, "w") as f:
    json.dump(out, f, indent=2, default=str)
print("[1163] DONE")
