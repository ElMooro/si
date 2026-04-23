#!/usr/bin/env python3
"""
Final verification after v3.2 bug fixes.

Expected to see:
  1. daily-report-v3 log: "FRED v3.2: skipped N via smart TTL" with N > 0
  2. fred-cache.json rebuilt with list shape + _meta.fetched_at stamps
  3. Lambda duration drops dramatically on warm-cache runs
  4. Zero NameError in recent logs
"""
import json
from datetime import datetime, timezone, timedelta
from ops_report import report
import boto3

s3 = boto3.client("s3", region_name="us-east-1")
cw = boto3.client("cloudwatch", region_name="us-east-1")
logs = boto3.client("logs", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"


with report("verify_v32_final") as r:
    r.heading("v3.2 final verification — after timezone fix + cache split")

    # 1. Check for NameError traces and successful v3.2 log lines
    r.section("1. Recent daily-report-v3 logs (filter for v3.2 output + errors)")
    try:
        log_group = "/aws/lambda/justhodl-daily-report-v3"
        streams = logs.describe_log_streams(
            logGroupName=log_group, orderBy="LastEventTime",
            descending=True, limit=3,
        ).get("logStreams", [])

        for stream_num, s in enumerate(streams[:3]):
            name = s.get("logStreamName", "")
            last = s.get("lastEventTimestamp", 0)
            last_dt = datetime.fromtimestamp(last / 1000, tz=timezone.utc) if last else None
            age_min = (datetime.now(timezone.utc) - last_dt).total_seconds() / 60 if last_dt else 999
            r.log(f"  Stream {stream_num+1}: {name[-36:]} ({age_min:.1f} min ago)")
            start = int((datetime.now(timezone.utc) - timedelta(minutes=20)).timestamp() * 1000)
            ev = logs.get_log_events(
                logGroupName=log_group, logStreamName=name,
                startTime=start, limit=200, startFromHead=False,
            )
            relevant = []
            for e in ev.get("events", []):
                msg = e.get("message", "").strip()
                if any(k in msg for k in ("FRED v3.2", "NameError", "timezone", "V10] DONE", "V10] Start", "TRACEBACK", "[ERROR]", "skipped", "backstop")):
                    relevant.append(f"    {msg[:220]}")
            if relevant:
                for line in relevant[-15:]:
                    r.log(line)
                if any("NameError" in l for l in relevant):
                    r.fail("    ✗ NameError still present — fix didn't work")
                    r.kv(check="name-error", status="STILL_PRESENT")
                elif any("FRED v3.2" in l for l in relevant):
                    r.ok("    ✓ v3.2 log output present, no NameError")
                    r.kv(check="name-error", status="CLEAN")
            else:
                r.log("    (no v3.2-related events in this stream yet)")
    except Exception as e:
        r.warn(f"  Log fetch: {e}")

    # 2. fred-cache.json rebuilt with correct shape
    r.section("2. fred-cache.json — shape + _meta stamps")
    try:
        obj = s3.get_object(Bucket=BUCKET, Key="data/fred-cache.json")
        cache = json.loads(obj["Body"].read().decode())
        lm = obj["LastModified"]
        age_min = (datetime.now(timezone.utc) - lm).total_seconds() / 60
        r.log(f"  Size: {obj['ContentLength']:,} bytes")
        r.log(f"  Last modified: {lm.isoformat()} ({age_min:.1f} min ago)")
        r.log(f"  Total series: {len(cache)}")

        list_shape = 0
        dict_shape = 0
        with_meta = 0
        for sid, entry in cache.items():
            if isinstance(entry, list):
                list_shape += 1
                if entry and isinstance(entry[0], dict) and entry[0].get("_meta"):
                    with_meta += 1
            elif isinstance(entry, dict):
                dict_shape += 1

        r.log(f"  List-shape entries (daily-report format): {list_shape}")
        r.log(f"  Dict-shape entries (secretary format): {dict_shape}")
        r.log(f"  Entries with _meta.fetched_at: {with_meta}")

        # Spot-check a few known series
        r.log("  Sample entries:")
        for sid in ("WALCL", "UNRATE", "DGS10", "CPIAUCSL", "VIXCLS"):
            entry = cache.get(sid)
            if isinstance(entry, list) and entry:
                latest = entry[0] if isinstance(entry[0], dict) else None
                if latest:
                    meta_stamp = latest.get("_meta", {}).get("fetched_at", "no-stamp")
                    r.log(f"    {sid}: value={latest.get('value')} date={latest.get('date')} fetched={meta_stamp[:19]}")
                    # Cadence
                    if len(entry) >= 2:
                        d0 = entry[0].get("date")
                        d1 = entry[1].get("date")
                        if d0 and d1:
                            gap = (datetime.strptime(d0, "%Y-%m-%d") - datetime.strptime(d1, "%Y-%m-%d")).days
                            r.log(f"      → gap between last 2 obs: {gap} days")
            else:
                r.log(f"    {sid}: MISSING or wrong shape")

        r.kv(check="cache", list_shape=list_shape, dict_shape=dict_shape,
             with_meta=with_meta, total=len(cache))
    except Exception as e:
        r.fail(f"  Cache fetch failed: {e}")

    # 3. Secretary-specific cache
    r.section("3. fred-cache-secretary.json (separate key)")
    try:
        obj = s3.get_object(Bucket=BUCKET, Key="data/fred-cache-secretary.json")
        cache = json.loads(obj["Body"].read().decode())
        r.log(f"  Size: {obj['ContentLength']:,} bytes, {len(cache)} series")
        r.kv(check="secretary-cache", size=obj["ContentLength"], series=len(cache))
    except Exception as e:
        r.log(f"  Not yet created (normal — secretary only writes if >70% fetched): {e}")

    # 4. Duration trend over last 40 min
    r.section("4. daily-report-v3 Duration trend (last 40 min)")
    try:
        end = datetime.now(timezone.utc)
        start = end - timedelta(minutes=40)
        resp = cw.get_metric_statistics(
            Namespace="AWS/Lambda", MetricName="Duration",
            Dimensions=[{"Name": "FunctionName", "Value": "justhodl-daily-report-v3"}],
            StartTime=start, EndTime=end, Period=300,
            Statistics=["Average", "Maximum"],
        )
        for p in sorted(resp.get("Datapoints", []), key=lambda x: x["Timestamp"]):
            ts = p["Timestamp"].isoformat()
            r.log(f"  {ts}: avg {p.get('Average', 0):.0f} ms, max {p.get('Maximum', 0):.0f} ms")
    except Exception as e:
        r.warn(f"  Metrics: {e}")

    # 5. Errors (should be zero now)
    r.section("5. daily-report-v3 Errors (last 40 min)")
    try:
        resp = cw.get_metric_statistics(
            Namespace="AWS/Lambda", MetricName="Errors",
            Dimensions=[{"Name": "FunctionName", "Value": "justhodl-daily-report-v3"}],
            StartTime=start, EndTime=end, Period=300, Statistics=["Sum"],
        )
        total = 0
        for p in sorted(resp.get("Datapoints", []), key=lambda x: x["Timestamp"]):
            total += p.get("Sum", 0)
            r.log(f"  {p['Timestamp'].isoformat()}: {p.get('Sum', 0):.0f} errors")
        r.kv(check="errors", total_last_40min=int(total))
    except Exception as e:
        r.warn(f"  Metrics: {e}")

    r.log("Done")
