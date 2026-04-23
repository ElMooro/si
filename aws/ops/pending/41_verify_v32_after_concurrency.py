#!/usr/bin/env python3
"""Final v3.2 check — after concurrency=1 took effect."""
import json
from datetime import datetime, timezone, timedelta
from ops_report import report
import boto3

s3 = boto3.client("s3", region_name="us-east-1")
cw = boto3.client("cloudwatch", region_name="us-east-1")
logs = boto3.client("logs", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"

with report("verify_v32_after_concurrency_fix") as r:
    r.heading("After concurrency=1 fix — cache + smart TTL check")

    # Cache existence + shape + meta
    r.section("1. fred-cache.json")
    try:
        obj = s3.get_object(Bucket=BUCKET, Key="data/fred-cache.json")
        cache = json.loads(obj["Body"].read().decode())
        lm = obj["LastModified"]
        age_min = (datetime.now(timezone.utc) - lm).total_seconds() / 60
        r.ok(f"  Exists: {obj['ContentLength']:,} bytes, {len(cache)} series, {age_min:.1f} min old")

        list_shape = sum(1 for e in cache.values() if isinstance(e, list))
        with_meta = sum(1 for e in cache.values() if isinstance(e, list) and e and isinstance(e[0], dict) and e[0].get("_meta"))
        r.log(f"  List shape: {list_shape}/{len(cache)}   With _meta stamps: {with_meta}/{len(cache)}")
        r.kv(check="cache", series=len(cache), list_shape=list_shape, with_meta=with_meta)

        for sid in ("WALCL", "UNRATE", "DGS10", "CPIAUCSL", "VIXCLS"):
            e = cache.get(sid)
            if isinstance(e, list) and e:
                latest = e[0] if isinstance(e[0], dict) else None
                if latest:
                    meta = latest.get("_meta", {}).get("fetched_at", "no-stamp")
                    gap = None
                    if len(e) > 1 and isinstance(e[1], dict):
                        try:
                            d0 = datetime.strptime(latest["date"], "%Y-%m-%d")
                            d1 = datetime.strptime(e[1]["date"], "%Y-%m-%d")
                            gap = (d0 - d1).days
                        except Exception: pass
                    r.log(f"  {sid}: date={latest.get('date')} meta={meta[:19]} cadence_gap={gap}d")
    except Exception as e:
        r.fail(f"  {e}")

    # Logs from latest stream
    r.section("2. Most recent run's log summary")
    try:
        streams = logs.describe_log_streams(
            logGroupName="/aws/lambda/justhodl-daily-report-v3",
            orderBy="LastEventTime", descending=True, limit=2,
        ).get("logStreams", [])
        start = int((datetime.now(timezone.utc) - timedelta(minutes=12)).timestamp() * 1000)
        for s_idx, s in enumerate(streams[:2]):
            name = s.get("logStreamName", "")
            age = (datetime.now(timezone.utc) - datetime.fromtimestamp(s.get("lastEventTimestamp", 0)/1000, tz=timezone.utc)).total_seconds() / 60
            r.log(f"\n  Stream {s_idx+1}: ...{name[-24:]} ({age:.1f} min ago)")
            ev = logs.get_log_events(
                logGroupName="/aws/lambda/justhodl-daily-report-v3",
                logStreamName=name, startTime=start, limit=200, startFromHead=False,
            )
            for e in ev.get("events", []):
                msg = e.get("message", "").strip()
                if any(k in msg for k in ("[V10]", "FRED v3.2", "skipped", "backstop", "DONE", "ERROR", "TRACEBACK")):
                    r.log(f"    {msg[:220]}")
    except Exception as e:
        r.warn(f"  {e}")

    # Duration + concurrency trend
    r.section("3. Duration + ConcurrentExecutions (since concurrency=1 at 15:54)")
    try:
        end = datetime.now(timezone.utc)
        start = end - timedelta(minutes=20)
        for metric, stat in [("Duration", "Average"), ("ConcurrentExecutions", "Maximum"), ("Errors", "Sum"), ("Throttles", "Sum")]:
            resp = cw.get_metric_statistics(
                Namespace="AWS/Lambda", MetricName=metric,
                Dimensions=[{"Name": "FunctionName", "Value": "justhodl-daily-report-v3"}],
                StartTime=start, EndTime=end, Period=60, Statistics=[stat],
            )
            r.log(f"  {metric} ({stat}):")
            for p in sorted(resp.get("Datapoints", []), key=lambda x: x["Timestamp"]):
                ts = p["Timestamp"].isoformat()[11:19]
                v = p.get(stat, 0)
                unit = "ms" if metric == "Duration" else ""
                r.log(f"    {ts}: {v:.0f}{unit}")
    except Exception as e:
        r.warn(f"  {e}")

    r.log("Done")
