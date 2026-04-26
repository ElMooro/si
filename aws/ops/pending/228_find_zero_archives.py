#!/usr/bin/env python3
"""Step 228 — find what's in the zero-valued archive files.

Last 200 archive entries: 190 have khalid_index=0, 10 have 43.
The earliest sampled files have 49. Something changed in the
producing Lambda recently that started writing 0s.

Find the cutover date so we know:
  1. When the data corruption started
  2. How to filter the HMM training set (drop 0-valued)
  3. Whether to alert/fix the producing Lambda
"""
import json
from datetime import datetime, timezone, timedelta
from collections import defaultdict
from ops_report import report
import boto3

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"

s3 = boto3.client("s3", region_name=REGION)


with report("find_zero_value_archives") as r:
    r.heading("When did khalid_index/ka_index = 0 start appearing?")

    paginator = s3.get_paginator("list_objects_v2")
    keys = []
    for page in paginator.paginate(Bucket=BUCKET, Prefix="archive/intelligence/"):
        for obj in page.get("Contents", []):
            keys.append((obj["Key"], obj["LastModified"]))
    keys.sort(key=lambda x: x[1])

    r.section(f"1. Total archive entries: {len(keys)}")

    # Sample: every 10th entry to map zeros vs valid by day
    r.section("2. Walk through every 5th archive, collect score + day")
    by_day = defaultdict(list)
    for i, (k, t) in enumerate(keys):
        if i % 5 != 0 and i != len(keys) - 1:
            continue
        try:
            obj = s3.get_object(Bucket=BUCKET, Key=k)
            data = json.loads(obj["Body"].read())
            scores = data.get("scores", {})
            ka = scores.get("ka_index")
            khalid = scores.get("khalid_index")
            chosen = ka if ka is not None else khalid
            day = t.strftime("%Y-%m-%d")
            by_day[day].append({"ka": ka, "khalid": khalid, "key": k.split("/")[-1]})
        except Exception:
            pass

    r.section("3. Daily summary: any 0s, any non-0s, distinct values")
    sorted_days = sorted(by_day.keys())
    for day in sorted_days:
        entries = by_day[day]
        kh_vals = [e["khalid"] for e in entries if e["khalid"] is not None]
        ka_vals = [e["ka"] for e in entries if e["ka"] is not None]
        chosen_vals = [e["ka"] if e["ka"] is not None else e["khalid"]
                       for e in entries
                       if e["ka"] is not None or e["khalid"] is not None]
        if not chosen_vals:
            r.warn(f"  {day}: NO data")
            continue
        n_zero = sum(1 for v in chosen_vals if v == 0)
        distinct = sorted(set(chosen_vals))
        r.log(f"  {day}: n={len(entries):2d}  zeros={n_zero}  ka_present={sum(1 for e in entries if e['ka'] is not None)}/{len(entries)}  distinct={distinct}")

    # Find the LATEST date where chosen_vals were not all 0
    r.section("4. Latest day with NON-zero data")
    for day in reversed(sorted_days):
        entries = by_day[day]
        chosen = [e["ka"] if e["ka"] is not None else e["khalid"] for e in entries]
        non_zero = [v for v in chosen if v is not None and v != 0]
        if non_zero:
            r.log(f"  {day}: {len(non_zero)} non-zero values: {sorted(set(non_zero))}")
            break

    # Latest entry full content
    r.section("5. Most recent archive entry full structure")
    if keys:
        latest_key = keys[-1][0]
        obj = s3.get_object(Bucket=BUCKET, Key=latest_key)
        data = json.loads(obj["Body"].read())
        r.log(f"  key: {latest_key}")
        r.log(f"  generated_at: {data.get('generated_at')}")
        r.log(f"  scores: {json.dumps(data.get('scores', {}), default=str, indent=2)[:400]}")

    r.section("FINAL")
    r.log("Done")
