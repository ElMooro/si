#!/usr/bin/env python3
"""Step 229 — find the source of the 47-day zero-write bug.

The producer Lambda (likely justhodl-intelligence or
justhodl-daily-report-v3) silently wrote khalid_index=0 from
Mar 9 → Apr 24, then started writing real scores (43) from Apr 25.

Investigate:
  1. Identify which Lambda actually writes these archives
  2. Look at code paths that could yield 0 silently
  3. Check Lambda Logs/Events around the Apr 24 → Apr 25 transition
"""
import json
from datetime import datetime, timezone, timedelta
from ops_report import report
import boto3

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"

s3 = boto3.client("s3", region_name=REGION)
lam = boto3.client("lambda", region_name=REGION)
logs = boto3.client("logs", region_name=REGION)


with report("find_zero_producer_bug") as r:
    r.heading("Find the producer of zero-valued archives Mar 9 → Apr 24")

    # 1. Which lambda writes archive/intelligence/?
    r.section("1. Inspect a recent (working) and old (broken) archive entry")
    paginator = s3.get_paginator("list_objects_v2")
    keys = []
    for page in paginator.paginate(Bucket=BUCKET, Prefix="archive/intelligence/"):
        for obj in page.get("Contents", []):
            keys.append((obj["Key"], obj["LastModified"]))
    keys.sort(key=lambda x: x[1])

    # Get a Mar 15 file (broken) and an Apr 26 file (working)
    broken_keys = [k for k, t in keys if t.month == 3 and t.day in (15, 16)][:2]
    working_keys = [k for k, t in keys if t.month == 4 and t.day == 26][-2:]

    for label, klist in [("BROKEN (Mar 15-16)", broken_keys), ("WORKING (Apr 26)", working_keys)]:
        r.log(f"")
        r.log(f"=== {label} ===")
        for k in klist:
            r.log(f"  {k}")
            obj = s3.get_object(Bucket=BUCKET, Key=k)
            data = json.loads(obj["Body"].read())
            r.log(f"    top keys: {list(data.keys())[:12]}")
            r.log(f"    version: {data.get('version')}")
            r.log(f"    data_sources: {str(data.get('data_sources'))[:200]}")
            scores = data.get("scores", {})
            r.log(f"    scores: {json.dumps(scores, default=str)[:300]}")

    # 2. Find candidate producer Lambdas
    r.section("2. List candidate producer Lambdas (matching 'intelligence' or 'daily-report')")
    candidates = []
    paginator = lam.get_paginator("list_functions")
    for page in paginator.paginate():
        for fn in page.get("Functions", []):
            n = fn["FunctionName"]
            if "intelligence" in n or "daily-report" in n or "report" in n:
                candidates.append({
                    "name": n,
                    "last_modified": fn.get("LastModified"),
                    "runtime": fn.get("Runtime"),
                    "description": fn.get("Description", ""),
                })
    for c in candidates:
        r.log(f"  {c['name']:40s}  modified={c['last_modified']}  runtime={c['runtime']}")
        if c["description"]:
            r.log(f"    desc: {c['description'][:120]}")

    # 3. Check CloudWatch logs for justhodl-daily-report-v3 around Apr 24-25
    r.section("3. CloudWatch logs around Apr 24 → Apr 25 transition")
    candidate_lambdas = ["justhodl-daily-report-v3", "justhodl-intelligence", "justhodl-morning-intelligence"]

    for fn in candidate_lambdas:
        log_group = f"/aws/lambda/{fn}"
        try:
            # Get recent log streams
            streams = logs.describe_log_streams(
                logGroupName=log_group,
                orderBy="LastEventTime",
                descending=True,
                limit=10,
            ).get("logStreams", [])
            r.log(f"")
            r.log(f"  {fn} — {len(streams)} recent log streams")
            for s in streams[:3]:
                last_event = datetime.fromtimestamp(s.get("lastEventTimestamp", 0) / 1000, tz=timezone.utc)
                r.log(f"    {s['logStreamName']:60s}  last event: {last_event}")
        except Exception as e:
            r.log(f"  {fn}: log group not found or error: {e}")

    # 4. Search CloudWatch logs around the cutover date for relevant errors
    r.section("4. Search logs around 2026-04-24 → 2026-04-25 cutover")
    cutover_start = int(datetime(2026, 4, 24, tzinfo=timezone.utc).timestamp() * 1000)
    cutover_end = int(datetime(2026, 4, 25, 23, 59, tzinfo=timezone.utc).timestamp() * 1000)

    for fn in ["justhodl-daily-report-v3"]:
        log_group = f"/aws/lambda/{fn}"
        try:
            r.log(f"  searching {log_group} for 'khalid_index' or 'ka_index' or '0' messages...")
            resp = logs.filter_log_events(
                logGroupName=log_group,
                startTime=cutover_start,
                endTime=cutover_end,
                filterPattern='"khalid_index" OR "ka_index" OR "Error" OR "Exception" OR "fallback"',
                limit=50,
            )
            events = resp.get("events", [])
            r.log(f"    {len(events)} matching events in window")
            for e in events[:15]:
                t = datetime.fromtimestamp(e["timestamp"] / 1000, tz=timezone.utc)
                msg = e.get("message", "")[:200].replace("\n", " ")
                r.log(f"    {t.isoformat()}  {msg}")
        except Exception as e:
            r.log(f"  search failed: {e}")

    r.section("FINAL")
    r.log("Done")
