#!/usr/bin/env python3
"""Step 227 — diagnose KA Index time series the loader returns.

The HMM fit two distinct cluster means: 0.0 (3 states) and 43.0 (1 state).
That's a binary distribution, not a continuous score. Something is wrong
with how we extract scores from the archive intelligence files.

Hypothesis 1: Most archive files have ka_index missing (returning 0
                    via .get default — but our code uses None default, so
                    that shouldn't be it).
Hypothesis 2: Files use a different score key (e.g. "score" instead of
                    "ka_index").
Hypothesis 3: Mix of valid 43.0 + invalid 0.0 = bimodal distribution.

This step prints the actual values to find out.
"""
import json
from collections import Counter
from datetime import datetime, timezone, timedelta
from ops_report import report
import boto3

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"

s3 = boto3.client("s3", region_name=REGION)


with report("diagnose_ka_index_series") as r:
    r.heading("Diagnose KA Index time series — why bimodal at 0/43?")

    # Replicate the loader's logic
    cutoff_date = datetime.now(timezone.utc) - timedelta(days=365)

    paginator = s3.get_paginator("list_objects_v2")
    keys = []
    for page in paginator.paginate(Bucket=BUCKET, Prefix="archive/intelligence/"):
        for obj in page.get("Contents", []):
            if obj["LastModified"].replace(tzinfo=timezone.utc) >= cutoff_date:
                keys.append((obj["Key"], obj["LastModified"]))

    r.section(f"1. archive/intelligence/ contains {len(keys)} keys in last 365d")
    if keys:
        r.log(f"  earliest: {keys[0][0]}  ({keys[0][1]})")
        r.log(f"  latest:   {keys[-1][0]}  ({keys[-1][1]})")
        r.log(f"  sample 5:")
        for k, t in keys[:5]:
            r.log(f"    {k}  ({t})")

    r.section("2. Sample values from first 20 files")
    values = []
    for k, t in keys[:20]:
        try:
            obj = s3.get_object(Bucket=BUCKET, Key=k)
            data = json.loads(obj["Body"].read())
            scores = data.get("scores", {})
            ka = scores.get("ka_index")
            khalid = scores.get("khalid_index")
            top_keys = list(data.keys())[:8]
            score_keys = list(scores.keys())[:8] if isinstance(scores, dict) else []
            r.log(f"  {k.split('/')[-1]}")
            r.log(f"    top-level keys: {top_keys}")
            r.log(f"    scores keys:    {score_keys}")
            r.log(f"    ka_index={ka!r}  khalid_index={khalid!r}")
            chosen = ka if ka is not None else khalid
            if chosen is not None:
                values.append(float(chosen))
        except Exception as e:
            r.warn(f"  {k}: {e}")

    r.section("3. Distribution of values from those 20 samples")
    if values:
        r.log(f"  count:  {len(values)}")
        r.log(f"  min:    {min(values)}")
        r.log(f"  max:    {max(values)}")
        r.log(f"  mean:   {sum(values)/len(values):.2f}")
        r.log(f"  median: {sorted(values)[len(values)//2]}")
        # Bucket counts
        buckets = Counter(round(v / 10) * 10 for v in values)
        for b in sorted(buckets.keys()):
            bar = "█" * buckets[b]
            r.log(f"    {b:3d}: {bar} ({buckets[b]})")

    # Now check the actual bigger sample to see distribution
    r.section("4. Full distribution across last 200 files (the loader's actual cap)")
    full_values = []
    for k, t in keys[-200:]:
        try:
            obj = s3.get_object(Bucket=BUCKET, Key=k)
            data = json.loads(obj["Body"].read())
            scores = data.get("scores", {})
            ka = scores.get("ka_index")
            khalid = scores.get("khalid_index")
            chosen = ka if ka is not None else khalid
            if chosen is not None:
                full_values.append(float(chosen))
        except Exception:
            pass

    if full_values:
        r.log(f"  count:        {len(full_values)}")
        r.log(f"  unique values: {len(set(full_values))}")
        r.log(f"  min/max:      {min(full_values):.2f} / {max(full_values):.2f}")
        r.log(f"  mean:         {sum(full_values)/len(full_values):.2f}")
        # Distribution buckets
        buckets = Counter(round(v / 5) * 5 for v in full_values)
        r.log(f"  histogram (buckets of 5):")
        for b in sorted(buckets.keys()):
            bar = "█" * min(40, buckets[b])
            r.log(f"    {b:3d}: {bar} ({buckets[b]})")

        # Top distinct values
        val_counts = Counter(round(v, 1) for v in full_values)
        r.log(f"  top 10 most-common values:")
        for v, c in val_counts.most_common(10):
            r.log(f"    {v:6.1f}: x{c}")

    r.section("FINAL")
    r.log("Done")
