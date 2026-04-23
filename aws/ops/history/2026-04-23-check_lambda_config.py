#!/usr/bin/env python3
"""Check Lambda timeout + reserved concurrency + concurrent invocations."""
import json
from datetime import datetime, timezone, timedelta
from ops_report import report
import boto3

lam = boto3.client("lambda", region_name="us-east-1")
cw = boto3.client("cloudwatch", region_name="us-east-1")
s3 = boto3.client("s3", region_name="us-east-1")


with report("check_lambda_config") as r:
    r.heading("daily-report-v3 config + concurrency check")

    # Config
    r.section("Lambda configuration")
    cfg = lam.get_function_configuration(FunctionName="justhodl-daily-report-v3")
    r.log(f"  Timeout: {cfg.get('Timeout')}s")
    r.log(f"  MemorySize: {cfg.get('MemorySize')} MB")
    r.log(f"  Runtime: {cfg.get('Runtime')}")
    r.log(f"  LastModified: {cfg.get('LastModified')}")
    r.log(f"  CodeSize: {cfg.get('CodeSize')}")
    r.kv(timeout=cfg.get("Timeout"), memory=cfg.get("MemorySize"))

    # Reserved concurrency
    try:
        rc = lam.get_function_concurrency(FunctionName="justhodl-daily-report-v3")
        r.log(f"  Reserved concurrency: {rc.get('ReservedConcurrentExecutions')}")
    except Exception as e:
        r.log(f"  Reserved concurrency: not set ({e})")

    # Concurrent executions in last 30 min
    r.section("ConcurrentExecutions metric")
    try:
        end = datetime.now(timezone.utc)
        start = end - timedelta(minutes=30)
        resp = cw.get_metric_statistics(
            Namespace="AWS/Lambda", MetricName="ConcurrentExecutions",
            Dimensions=[{"Name": "FunctionName", "Value": "justhodl-daily-report-v3"}],
            StartTime=start, EndTime=end, Period=60, Statistics=["Maximum"],
        )
        for p in sorted(resp.get("Datapoints", []), key=lambda x: x["Timestamp"]):
            ts = p["Timestamp"].isoformat()
            r.log(f"  {ts}: max concurrent = {p.get('Maximum', 0):.0f}")
    except Exception as e:
        r.warn(f"  {e}")

    # Invocations in last 30 min
    r.section("Invocations (1-min buckets)")
    try:
        end = datetime.now(timezone.utc)
        start = end - timedelta(minutes=30)
        resp = cw.get_metric_statistics(
            Namespace="AWS/Lambda", MetricName="Invocations",
            Dimensions=[{"Name": "FunctionName", "Value": "justhodl-daily-report-v3"}],
            StartTime=start, EndTime=end, Period=60, Statistics=["Sum"],
        )
        total = 0
        for p in sorted(resp.get("Datapoints", []), key=lambda x: x["Timestamp"]):
            cnt = p.get("Sum", 0)
            total += cnt
            if cnt > 0:
                r.log(f"  {p['Timestamp'].isoformat()}: {cnt:.0f} invocations")
        r.log(f"  TOTAL last 30 min: {total:.0f}")
    except Exception as e:
        r.warn(f"  {e}")

    # Duration max + average
    r.section("Duration distribution")
    try:
        resp = cw.get_metric_statistics(
            Namespace="AWS/Lambda", MetricName="Duration",
            Dimensions=[{"Name": "FunctionName", "Value": "justhodl-daily-report-v3"}],
            StartTime=start, EndTime=end, Period=300,
            Statistics=["Average", "Maximum", "Minimum"],
        )
        for p in sorted(resp.get("Datapoints", []), key=lambda x: x["Timestamp"]):
            avg = p.get("Average", 0)
            mx = p.get("Maximum", 0)
            mn = p.get("Minimum", 0)
            r.log(f"  {p['Timestamp'].isoformat()}: min={mn:.0f}ms avg={avg:.0f}ms max={mx:.0f}ms")
    except Exception as e:
        r.warn(f"  {e}")

    r.log("Done")
