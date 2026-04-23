#!/usr/bin/env python3
"""
Verify v3.2 smart TTL is actually skipping fetches.

Expected in the next scheduled run:
  - "FRED v3.2: skipped N via smart TTL ({'weekly': X, 'monthly': Y...})"
  - Scan time drops dramatically
  - fred-cache.json has _meta.fetched_at stamps on entries
"""
import json
from datetime import datetime, timezone, timedelta
from ops_report import report
import boto3

s3 = boto3.client("s3", region_name="us-east-1")
cw = boto3.client("cloudwatch", region_name="us-east-1")
logs = boto3.client("logs", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"


with report("verify_smart_ttl_v32") as r:
    r.heading("v3.2 verification — smart TTL in production")

    # 1. Recent daily-report-v3 logs — look for "FRED v3.2"
    r.section("1. daily-report-v3 logs (latest stream)")
    try:
        log_group = "/aws/lambda/justhodl-daily-report-v3"
        streams = logs.describe_log_streams(
            logGroupName=log_group, orderBy="LastEventTime",
            descending=True, limit=2,
        ).get("logStreams", [])

        for s in streams[:2]:
            name = s.get("logStreamName", "")
            last = s.get("lastEventTimestamp", 0)
            last_dt = datetime.fromtimestamp(last / 1000, tz=timezone.utc) if last else None
            age = (datetime.now(timezone.utc) - last_dt).total_seconds() / 60 if last_dt else 999
            r.log(f"  Stream: {name[-40:]} ({age:.1f} min ago)")
            start = int((datetime.now(timezone.utc) - timedelta(minutes=15)).timestamp() * 1000)
            ev = logs.get_log_events(
                logGroupName=log_group, logStreamName=name,
                startTime=start, limit=100, startFromHead=False,
            )
            shown = 0
            for e in ev.get("events", [])[-80:]:
                msg = e.get("message", "").strip()
                if any(k in msg for k in ("FRED v3.2", "FRED:", "skipped", "V10", "Start", "DONE", "TRACEBACK", "ERROR", "429")):
                    r.log(f"    {msg[:200]}")
                    shown += 1
                    if shown > 25:
                        break
            if shown:
                break  # only first stream
    except Exception as e:
        r.warn(f"  Log fetch failed: {e}")

    # 2. fred-cache.json entries — do any have _meta stamp?
    r.section("2. fred-cache.json — check for _meta.fetched_at stamps")
    try:
        obj = s3.get_object(Bucket=BUCKET, Key="data/fred-cache.json")
        cache = json.loads(obj["Body"].read().decode())
        lm = obj["LastModified"]
        age_min = (datetime.now(timezone.utc) - lm).total_seconds() / 60
        r.log(f"  Cache last modified: {lm.isoformat()} ({age_min:.1f} min ago)")
        r.log(f"  Total series: {len(cache)}")

        stamped = 0
        sample_keys = ("WALCL", "RRPONTSYD", "WTREGEN", "UNRATE", "CPIAUCSL", "DGS10", "VIXCLS")
        for sid in sample_keys:
            entry = cache.get(sid)
            if entry and isinstance(entry, list) and entry:
                latest = entry[0] if isinstance(entry[0], dict) else None
                if latest:
                    meta = latest.get("_meta")
                    date = latest.get("date")
                    value = latest.get("value")
                    if meta:
                        stamped += 1
                    r.log(f"  {sid}: date={date} value={value} meta={meta}")

        # Sample any entry with _meta to confirm new code path ran
        for sid, entry in list(cache.items())[:30]:
            if isinstance(entry, list) and entry:
                latest = entry[0] if isinstance(entry[0], dict) else None
                if latest and latest.get("_meta"):
                    stamped += 1
        r.kv(check="cache-meta-stamps", sample_stamped=stamped, total=len(cache))
    except Exception as e:
        r.fail(f"  Cache fetch failed: {e}")

    # 3. Infer cadence distribution from cache
    r.section("3. Inferred cadence distribution (what TTL would classify)")
    try:
        def infer_cadence(obs_list):
            if not obs_list or len(obs_list) < 2:
                return None
            gaps = []
            for i in range(min(5, len(obs_list) - 1)):
                try:
                    d0 = datetime.strptime(obs_list[i]["date"], "%Y-%m-%d")
                    d1 = datetime.strptime(obs_list[i+1]["date"], "%Y-%m-%d")
                    g = (d0 - d1).days
                    if g > 0: gaps.append(g)
                except Exception:
                    pass
            if not gaps: return None
            gaps.sort()
            return gaps[len(gaps) // 2]

        cadence_buckets = {"daily (≤3d)": 0, "weekly (4-10d)": 0, "monthly (11-45d)": 0,
                           "quarterly (46-120d)": 0, "annual (>120d)": 0, "unknown": 0}
        for sid, entry in cache.items():
            if not isinstance(entry, list):
                continue
            days = infer_cadence(entry)
            if days is None:
                cadence_buckets["unknown"] += 1
            elif days <= 3:
                cadence_buckets["daily (≤3d)"] += 1
            elif days <= 10:
                cadence_buckets["weekly (4-10d)"] += 1
            elif days <= 45:
                cadence_buckets["monthly (11-45d)"] += 1
            elif days <= 120:
                cadence_buckets["quarterly (46-120d)"] += 1
            else:
                cadence_buckets["annual (>120d)"] += 1
        for bucket, count in cadence_buckets.items():
            r.log(f"  {bucket}: {count}")
        r.kv(check="cadence-dist",
             daily=cadence_buckets["daily (≤3d)"],
             weekly=cadence_buckets["weekly (4-10d)"],
             monthly=cadence_buckets["monthly (11-45d)"])
    except Exception as e:
        r.warn(f"  Cadence analysis failed: {e}")

    # 4. Lambda metrics — duration trend
    r.section("4. daily-report-v3 duration trend (last 30 min)")
    try:
        end = datetime.now(timezone.utc)
        start = end - timedelta(minutes=30)
        resp = cw.get_metric_statistics(
            Namespace="AWS/Lambda", MetricName="Duration",
            Dimensions=[{"Name": "FunctionName", "Value": "justhodl-daily-report-v3"}],
            StartTime=start, EndTime=end, Period=300, Statistics=["Average"],
        )
        for p in sorted(resp.get("Datapoints", []), key=lambda x: x["Timestamp"]):
            ts = p["Timestamp"].isoformat()
            r.log(f"  {ts}: {p.get('Average'):.0f} ms")
    except Exception as e:
        r.warn(f"  Metrics fetch failed: {e}")

    r.log("Done")
