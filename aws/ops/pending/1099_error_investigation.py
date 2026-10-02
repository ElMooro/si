"""
Ops 1099: investigate recent Lambda errors.
Checks CloudWatch logs for error patterns in:
 - justhodl-short-interest (3 errors/24h)
 - justhodl-sec-8k-enrich (1 error/24h)
Writes: aws/ops/reports/1099_error_investigation.json. Never raises.
"""
from __future__ import annotations
import json, os, sys
from datetime import datetime, timezone, timedelta
OUT_PATH = os.path.join("aws", "ops", "reports", "1099_error_investigation.json")
REGION = "us-east-1"
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "shared"))
import boto3
logs = boto3.client("logs", region_name=REGION)

TARGETS = [
    ("/aws/lambda/justhodl-short-interest", "short-interest"),
    ("/aws/lambda/justhodl-sec-8k-enrich", "sec-8k"),
]

def find_errors(log_group, hours=24):
    try:
        start = int((datetime.now(timezone.utc) - timedelta(hours=hours)).timestamp() * 1000)
        # Filter for ERROR or Exception
        r = logs.filter_log_events(
            logGroupName=log_group,
            startTime=start,
            filterPattern="?ERROR ?Exception ?Traceback",
            limit=20)
        events = []
        for e in r.get("events", [])[:10]:
            msg = e["message"]
            # Extract first 300 chars, find error type
            events.append({
                "timestamp": datetime.fromtimestamp(e["timestamp"]/1000, timezone.utc).strftime("%H:%M:%S"),
                "message": msg[:300].replace("\n", " | ")})
        return events
    except Exception as e:
        return [{"error": str(e)[:100]}]

def main():
    rep = {"script": "1099_error_investigation",
           "read_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
           "targets": {}}
    for lg, name in TARGETS:
        rep["targets"][name] = {
            "log_group": lg,
            "error_events": find_errors(lg)}
    rep["status"] = "COMPLETE"
    os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)
    with open(OUT_PATH, "w") as f:
        json.dump(rep, f, indent=2)
    total = sum(len(v["error_events"]) for v in rep["targets"].values())
    print(json.dumps({"error_events_found": total}))

if __name__ == "__main__":
    main()
