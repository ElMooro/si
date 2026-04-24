#!/usr/bin/env python3
"""
Quick S3 inventory: do the 7 data sources ml-predictions needs exist
in S3 directly? If yes → rewrite fetch_all_data() to use S3 instead
of the dead api.justhodl.ai endpoint.

Required keys (from ml-predictions raw.get() calls):
  fed-liquidity, enhanced-repo, cross-currency, volatility-monitor,
  bond-indices, ai-prediction, global-liquidity

Search:
  - At root level: <key>.json
  - Under data/ prefix: data/<key>.json
  - Any wildcard match
"""
import json
from datetime import datetime, timezone
from ops_report import report
import boto3

s3 = boto3.client("s3", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"

REQUIRED_KEYS = [
    "fed-liquidity", "enhanced-repo", "cross-currency",
    "volatility-monitor", "bond-indices", "ai-prediction", "global-liquidity",
]


def find_keys_matching(stem):
    """Search S3 for any key that matches the stem, in common locations."""
    candidates = [
        f"{stem}.json",
        f"data/{stem}.json",
        f"{stem}",
    ]
    found = []
    for c in candidates:
        try:
            obj = s3.head_object(Bucket=BUCKET, Key=c)
            age_h = (datetime.now(timezone.utc) - obj["LastModified"]).total_seconds() / 3600
            found.append((c, obj["ContentLength"], age_h))
        except Exception:
            pass
    # Also list with prefix-search
    try:
        resp = s3.list_objects_v2(Bucket=BUCKET, Prefix=stem.split("-")[0], MaxKeys=50)
        for obj in resp.get("Contents", []):
            if stem in obj["Key"] and obj["Key"] not in [f[0] for f in found]:
                age_h = (datetime.now(timezone.utc) - obj["LastModified"]).total_seconds() / 3600
                found.append((obj["Key"], obj["Size"], age_h))
    except Exception:
        pass
    return found


with report("ml_predictions_data_inventory") as r:
    r.heading("ml-predictions: where do its 7 data sources live in S3?")

    found_count = 0
    not_found = []
    for stem in REQUIRED_KEYS:
        r.section(f"Looking for: {stem}")
        results = find_keys_matching(stem)
        if results:
            for key, size, age_h in results:
                marker = "✓" if age_h < 24 else "⚠" if age_h < 168 else "✗"
                r.log(f"  {marker} {key:50} {size:>10} bytes  ({age_h:.1f}h old)")
            found_count += 1
        else:
            r.warn(f"  Not found in S3 (checked {stem}.json, data/{stem}.json, etc.)")
            not_found.append(stem)

    r.section("Summary")
    r.log(f"  Sources with S3 representation: {found_count}/{len(REQUIRED_KEYS)}")
    r.log(f"  Sources missing: {not_found}")
    r.log("")
    r.log("  If most sources exist as fresh S3 keys, rewrite fetch_all_data()")
    r.log("  to read from S3 directly. If sources are missing or stale, the")
    r.log("  source-of-truth Lambdas (e.g. fed-liquidity-agent) themselves")
    r.log("  may also be broken, which would be a much bigger investigation.")

    r.kv(found=found_count, missing=len(not_found), total=len(REQUIRED_KEYS))
    r.log("Done")
