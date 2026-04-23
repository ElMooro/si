#!/usr/bin/env python3
"""Deeper dig into daily-report logs to see why cache-write isn't firing."""
import json
from datetime import datetime, timezone, timedelta
from ops_report import report
import boto3

logs = boto3.client("logs", region_name="us-east-1")
s3 = boto3.client("s3", region_name="us-east-1")


with report("deep_debug_v32") as r:
    r.heading("Why is fred-cache.json not being written?")

    # Check if cache actually exists
    r.section("Cache existence check")
    try:
        obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="data/fred-cache.json")
        r.log(f"  Cache exists: {obj['ContentLength']} bytes, modified {obj['LastModified'].isoformat()}")
    except Exception as e:
        r.log(f"  Cache missing: {e}")

    # Get logs from the most recent complete run
    r.section("Most recent complete run — full log")
    log_group = "/aws/lambda/justhodl-daily-report-v3"
    streams = logs.describe_log_streams(
        logGroupName=log_group, orderBy="LastEventTime",
        descending=True, limit=5,
    ).get("logStreams", [])

    for s_idx, s in enumerate(streams[:5]):
        name = s.get("logStreamName", "")
        last = s.get("lastEventTimestamp", 0)
        last_dt = datetime.fromtimestamp(last / 1000, tz=timezone.utc) if last else None
        age_min = (datetime.now(timezone.utc) - last_dt).total_seconds() / 60 if last_dt else 999
        r.log(f"\nStream {s_idx+1}: ...{name[-30:]} ({age_min:.1f} min ago)")

        start = int((datetime.now(timezone.utc) - timedelta(minutes=30)).timestamp() * 1000)
        ev = logs.get_log_events(
            logGroupName=log_group, logStreamName=name,
            startTime=start, limit=200, startFromHead=False,
        )
        # Show all V10 lines + errors
        shown = 0
        for e in ev.get("events", []):
            msg = e.get("message", "").strip()
            if any(k in msg for k in ("[V10]", "FRED", "ERROR", "Error", "Exception", "TRACEBACK", "backstop", "cache")):
                r.log(f"  {msg[:240]}")
                shown += 1
                if shown > 40:
                    r.log("  ... (truncated)")
                    break
        if shown == 0:
            r.log("  (no matching lines — maybe still running)")

    r.log("Done")
