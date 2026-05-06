#!/usr/bin/env python3
"""Step 251 — dump raw honest_summary block + Lambda log tail to diagnose."""
import json, os, time
import boto3
from datetime import datetime, timezone, timedelta

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
LAMBDA_NAME = "justhodl-backtest-engine"
LOG_GROUP = f"/aws/lambda/{LAMBDA_NAME}"
SUMMARY_KEY = "backtest/summary.json"
REPORT_PATH = "aws/ops/reports/251_diag.json"

s3 = boto3.client("s3", region_name=REGION)
logs = boto3.client("logs", region_name=REGION)


def main():
    out = {"probed_at": datetime.now(timezone.utc).isoformat(timespec="seconds")}

    # Read raw S3 summary
    body = json.loads(s3.get_object(Bucket=BUCKET, Key=SUMMARY_KEY)["Body"].read())
    out["engine_version"] = body.get("v")
    out["engine_generated_at"] = body.get("generated_at")
    out["honest_summary_RAW"] = body.get("honest_summary")
    out["constants"] = body.get("constants")

    # Tail the Lambda CloudWatch logs (last 5 minutes)
    end = int(time.time() * 1000)
    start = end - 5 * 60 * 1000
    try:
        # Find latest log stream
        streams = logs.describe_log_streams(
            logGroupName=LOG_GROUP, orderBy="LastEventTime",
            descending=True, limit=3,
        )["logStreams"]
        events = []
        for st in streams:
            ev = logs.get_log_events(
                logGroupName=LOG_GROUP, logStreamName=st["logStreamName"],
                startTime=start, endTime=end, limit=200,
            )["events"]
            for e in ev:
                msg = e["message"].strip()
                # Skip noise, keep ERROR + START + REPORT + our prints
                if any(s in msg for s in ["[backtest", "ERROR", "Traceback", "honest_summary", "v2.0", "v2.0.1"]):
                    events.append({"ts": e["timestamp"], "msg": msg[:500]})
        events.sort(key=lambda x: x["ts"])
        out["log_events"] = events[-30:]
    except Exception as e:
        out["log_err"] = str(e)

    os.makedirs(os.path.dirname(REPORT_PATH), exist_ok=True)
    with open(REPORT_PATH, "w") as f:
        json.dump(out, f, indent=2, default=str)
    print(json.dumps(out, indent=2, default=str)[:3000])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
