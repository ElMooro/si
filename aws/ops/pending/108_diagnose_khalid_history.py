#!/usr/bin/env python3
"""
Step 108 — Diagnose where Khalid Index daily history lives.

The compute_khalid_timeline Lambda only finds 2 timeline points. Need
to figure out where Khalid Index history is actually stored:
  1. signal_type='khalid_index' in DynamoDB? (probably not, given 2 points)
  2. learning/morning_run_log.json? (morning Lambda writes this daily)
  3. archive/ S3 snapshots? (each report.json has khalid_index)
  4. Some other key under learning/ ?

Strategy:
  - List all distinct signal_types from DDB + their counts
  - Check learning/morning_run_log.json (per memory, morning Lambda
    writes this with khalid_raw + khalid_regime)
  - List S3 keys under learning/
  - List S3 keys under archive/ (sample; this is huge)

Then patch the Lambda to read from the BEST source and re-deploy.
"""
import json
import os
from collections import Counter
from datetime import datetime, timezone, timedelta
from pathlib import Path

from ops_report import report
import boto3

REGION = "us-east-1"
REPO_ROOT = Path(os.environ.get("GITHUB_WORKSPACE", os.getcwd()))

ddb = boto3.resource("dynamodb", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)


with report("diagnose_khalid_history") as r:
    r.heading("Diagnose Khalid Index history sources")

    # ─── 1. Distinct signal_types in DDB ────────────────────────────────
    r.section("1. Distinct signal_types in justhodl-signals")
    t = ddb.Table("justhodl-signals")
    type_counts = Counter()
    last_seen = {}
    n_scanned = 0
    kwargs = {}
    while True:
        resp = t.scan(ProjectionExpression="signal_type, logged_at", **kwargs)
        for item in resp.get("Items", []):
            st = item.get("signal_type")
            ts = item.get("logged_at")
            if st:
                type_counts[st] += 1
                if ts and (st not in last_seen or str(ts) > str(last_seen[st])):
                    last_seen[st] = str(ts)
            n_scanned += 1
        if "LastEvaluatedKey" not in resp:
            break
        kwargs["ExclusiveStartKey"] = resp["LastEvaluatedKey"]
        if n_scanned > 8000:
            break

    r.log(f"  Scanned {n_scanned} items, {len(type_counts)} distinct signal_types:\n")
    for st, cnt in sorted(type_counts.items(), key=lambda x: -x[1]):
        last = last_seen.get(st, "?")[:19]
        r.log(f"    {cnt:>5}  {st:35} last: {last}")

    has_khalid = "khalid_index" in type_counts
    r.log(f"\n  Has signal_type='khalid_index': {has_khalid}")

    # ─── 2. learning/morning_run_log.json ───────────────────────────────
    r.section("2. learning/morning_run_log.json")
    try:
        obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="learning/morning_run_log.json")
        body = obj["Body"].read().decode("utf-8")
        data = json.loads(body)
        r.log(f"  Type: {type(data).__name__}, size: {len(body):,}B")
        r.log(f"  Last modified: {obj['LastModified']}")
        if isinstance(data, list):
            r.log(f"  Length: {len(data)}")
            if data:
                sample = data[-1]
                r.log(f"  Latest entry keys: {sorted(sample.keys()) if isinstance(sample, dict) else 'not dict'}")
                if isinstance(sample, dict):
                    r.log(f"  Sample: {json.dumps(sample, default=str)[:500]}")
        elif isinstance(data, dict):
            r.log(f"  Top keys: {sorted(data.keys())}")
            r.log(f"  Sample: {json.dumps(data, default=str)[:500]}")
    except Exception as e:
        r.warn(f"  Couldn't read morning_run_log.json: {e}")

    # ─── 3. List S3 keys under learning/ ────────────────────────────────
    r.section("3. S3 keys under learning/")
    paginator = s3.get_paginator("list_objects_v2")
    learning_keys = []
    for page in paginator.paginate(Bucket="justhodl-dashboard-live", Prefix="learning/"):
        for obj in page.get("Contents", []):
            learning_keys.append({
                "key": obj["Key"],
                "size": obj["Size"],
                "modified": obj["LastModified"],
            })
        if not page.get("IsTruncated"):
            break
    r.log(f"  Found {len(learning_keys)} keys under learning/")
    for k in learning_keys:
        age_h = (datetime.now(timezone.utc) - k["modified"]).total_seconds() / 3600
        r.log(f"    {k['key']:50} {k['size']:>8}B  age {age_h:.1f}h")

    # ─── 4. archive/ S3 keys (sample) ───────────────────────────────────
    r.section("4. archive/ S3 sample (most recent 20)")
    archive_keys = []
    for page in paginator.paginate(Bucket="justhodl-dashboard-live", Prefix="archive/"):
        for obj in page.get("Contents", []):
            archive_keys.append({
                "key": obj["Key"],
                "size": obj["Size"],
                "modified": obj["LastModified"],
            })
        # Stop after collecting some
        if len(archive_keys) > 100:
            break
    archive_keys.sort(key=lambda x: -x["modified"].timestamp())
    r.log(f"  Total archive keys (collected so far): {len(archive_keys)}")
    r.log(f"  Most recent 20:")
    for k in archive_keys[:20]:
        age_h = (datetime.now(timezone.utc) - k["modified"]).total_seconds() / 3600
        r.log(f"    {k['key']:55} {k['size']:>10}B  age {age_h:.1f}h")

    # ─── 5. Spot-check: what's currently in data/report.json's khalid? ─
    r.section("5. Current data/report.json khalid_index value")
    try:
        obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="data/report.json")
        rep = json.loads(obj["Body"].read().decode("utf-8"))
        # Find any khalid-related fields
        for k, v in rep.items():
            if "khalid" in k.lower():
                r.log(f"  {k}: {json.dumps(v, default=str)[:200]}")
    except Exception as e:
        r.warn(f"  Couldn't read report.json: {e}")

    r.log("Done")
