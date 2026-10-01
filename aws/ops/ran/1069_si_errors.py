"""
Ops 1069: short-interest error timing check (read-only).

1068 found 6 errors in 24h for justhodl-short-interest, but the regex fix
deployed at ~13:33Z. This pulls the Errors metric in 1h buckets and the
timestamps of recent error log events to determine whether errors are
pre-fix (before 13:33Z) or post-fix.

Writes: aws/ops/reports/1069_si_errors.json (committed back by run-ops).
Read-only. Never raises.
"""
from __future__ import annotations

import json
import os
import sys
import time
from datetime import datetime, timedelta, timezone

OUT_PATH = os.path.join("aws", "ops", "reports", "1069_si_errors.json")
FN = "justhodl-short-interest"
REGION = "us-east-1"
FIX_TIME = datetime(2026, 10, 1, 13, 33, tzinfo=timezone.utc)

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                "..", "..", "shared"))
import boto3  # noqa: E402

cw = boto3.client("cloudwatch", region_name=REGION)
logs = boto3.client("logs", region_name=REGION)


def main():
    """Bucket errors hourly, list recent error events, write the report."""
    now = datetime.now(timezone.utc)
    report = {"script": "1069_si_errors",
              "read_at": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
              "fix_deployed_at": FIX_TIME.strftime("%Y-%m-%dT%H:%M:%SZ")}
    buckets = []
    try:
        r = cw.get_metric_statistics(
            Namespace="AWS/Lambda", MetricName="Errors",
            Dimensions=[{"Name": "FunctionName", "Value": FN}],
            StartTime=now - timedelta(hours=24), EndTime=now,
            Period=3600, Statistics=["Sum"])
        for d in sorted(r.get("Datapoints", []), key=lambda x: x["Timestamp"]):
            buckets.append({"hour": d["Timestamp"].strftime("%H:%M"),
                            "errors": int(d.get("Sum", 0))})
    except Exception as e:  # noqa: BLE001
        buckets = [{"error": str(e)[:100]}]
    report["hourly_errors"] = buckets
    events = []
    try:
        streams = logs.describe_log_streams(
            logGroupName=f"/aws/lambda/{FN}",
            orderBy="LastEventTime", descending=True, limit=3)
        for s in streams.get("logStreams", []):
            evs = logs.filter_log_events(
                logGroupName=f"/aws/lambda/{FN}",
                logStreamNames=[s["logStreamName"]],
                filterPattern="ERROR", limit=5)
            for e in evs.get("events", []):
                ts = datetime.fromtimestamp(e["timestamp"] / 1000,
                                            tz=timezone.utc)
                events.append({
                    "at": ts.strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "after_fix": ts >= FIX_TIME,
                    "msg": e.get("message", "")[:220]})
                if len(events) >= 8:
                    break
            if len(events) >= 8:
                break
    except Exception as e:  # noqa: BLE001
        events = [{"error": str(e)[:100]}]
    report["recent_error_events"] = events
    post = [e for e in events if e.get("after_fix") is True]
    report["post_fix_errors_seen"] = len(post) > 0
    os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)
    with open(OUT_PATH, "w") as f:
        json.dump(report, f, indent=2)
    print(json.dumps(report, indent=2)[:4000])


if __name__ == "__main__":
    main()
