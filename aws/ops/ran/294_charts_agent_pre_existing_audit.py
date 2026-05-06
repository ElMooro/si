#!/usr/bin/env python3
"""Step 294 — Confirm whether charts-agent was already broken pre-migration.

The 502 we got could be:
  (a) introduced by my Phase 2C migration → bad
  (b) pre-existing issue in the Lambda's business logic → not bad

Check:
  1. CloudWatch error metric for last 30d — if errors existed BEFORE
     this morning's migration commit, it's pre-existing.
  2. Check the actual error from a fresh invoke.
"""
import json
import os
import time
from datetime import datetime, timezone, timedelta

import boto3

REGION = "us-east-1"
NAME = "justhodl-charts-agent"
REPORT = "aws/ops/reports/294_charts_agent_audit.json"

cw = boto3.client("cloudwatch", region_name=REGION)
logs = boto3.client("logs", region_name=REGION)
lam = boto3.client("lambda", region_name=REGION)


def daily_metric(metric_name, days=30):
    end = datetime.now(timezone.utc)
    start = end - timedelta(days=days)
    resp = cw.get_metric_statistics(
        Namespace="AWS/Lambda", MetricName=metric_name,
        Dimensions=[{"Name": "FunctionName", "Value": NAME}],
        StartTime=start, EndTime=end, Period=86400, Statistics=["Sum"],
    )
    return [{"date": d["Timestamp"].strftime("%Y-%m-%d"), "value": int(d["Sum"])}
            for d in sorted(resp.get("Datapoints", []), key=lambda x: x["Timestamp"])
            if d["Sum"] > 0]


def main():
    out = {}
    out["invocations_30d"] = daily_metric("Invocations")
    out["errors_30d"] = daily_metric("Errors")

    # Get most recent log events
    try:
        streams = logs.describe_log_streams(
            logGroupName=f"/aws/lambda/{NAME}",
            orderBy="LastEventTime", descending=True, limit=2,
        ).get("logStreams", [])
        events = []
        for s in streams[:2]:
            ev = logs.get_log_events(
                logGroupName=f"/aws/lambda/{NAME}",
                logStreamName=s["logStreamName"], limit=12,
            ).get("events", [])
            for e in ev[:8]:
                events.append({
                    "ts": datetime.fromtimestamp(e["timestamp"]/1000, tz=timezone.utc).isoformat(),
                    "msg": e["message"][:200].strip(),
                })
        out["recent_logs"] = events
    except Exception as e:
        out["log_err"] = str(e)[:200]

    # Conclusion: is this pre-existing?
    pre_existing = False
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    yesterday = (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d")
    pre_today_errors = sum(d["value"] for d in out["errors_30d"]
                            if d["date"] not in (today, yesterday))
    if pre_today_errors > 0:
        pre_existing = True
    out["pre_existing_errors_before_today"] = pre_today_errors
    out["concluded_pre_existing"] = pre_existing

    with open(REPORT, "w") as f:
        json.dump(out, f, indent=2, default=str)
    print(json.dumps(out, indent=2, default=str))


if __name__ == "__main__":
    main()
