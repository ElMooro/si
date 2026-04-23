#!/usr/bin/env python3
"""
Fix S3 bucket policy so public dashboards can read their data files.

Current policy only grants public s3:GetObject on:
  - report.json (bucket root — stale, we write to data/report.json now)
  - screener/*
  - sentiment/*

What's actually fetched by dashboards:
  - data/report.json, data/secretary-latest.json, data/fred-cache.json
  - flow-data.json (bucket root — written by justhodl-options-flow)
  - crypto-intel.json (bucket root — written by justhodl-crypto-intel)
  - data/intelligence-report.json

Adds two new Allow statements:
  - data/* (catches data/report.json, secretary-latest, cache, etc.)
  - Specific bucket-root files (flow-data.json, crypto-intel.json)
    explicitly rather than * to keep the blast radius narrow

Also removes the now-useless PublicReadReportJson statement since
nothing reads the orphan report.json at bucket root anymore (we moved
to data/report.json in Phase 3b).

Probe each file post-deploy to verify 200 OK.
"""

import json
import ssl
import urllib.request
import urllib.error
from ops_report import report
import boto3

BUCKET = "justhodl-dashboard-live"
s3 = boto3.client("s3", region_name="us-east-1")


NEW_POLICY = {
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "PublicReadDataDir",
            "Effect": "Allow",
            "Principal": "*",
            "Action": "s3:GetObject",
            "Resource": f"arn:aws:s3:::{BUCKET}/data/*",
        },
        {
            "Sid": "PublicReadScreener",
            "Effect": "Allow",
            "Principal": "*",
            "Action": "s3:GetObject",
            "Resource": f"arn:aws:s3:::{BUCKET}/screener/*",
        },
        {
            "Sid": "PublicReadSentiment",
            "Effect": "Allow",
            "Principal": "*",
            "Action": "s3:GetObject",
            "Resource": f"arn:aws:s3:::{BUCKET}/sentiment/*",
        },
        {
            "Sid": "PublicReadRootDashboardFiles",
            "Effect": "Allow",
            "Principal": "*",
            "Action": "s3:GetObject",
            "Resource": [
                f"arn:aws:s3:::{BUCKET}/flow-data.json",
                f"arn:aws:s3:::{BUCKET}/crypto-intel.json",
            ],
        },
    ],
}


TEST_FILES = [
    "data/report.json",
    "data/secretary-latest.json",
    "data/fred-cache.json",
    "flow-data.json",
    "crypto-intel.json",
]


def head(url, timeout=8):
    ctx = ssl.create_default_context()
    try:
        req = urllib.request.Request(url, method="HEAD")
        with urllib.request.urlopen(req, timeout=timeout, context=ctx) as resp:
            return resp.status, None
    except urllib.error.HTTPError as e:
        return e.code, e.reason
    except Exception as e:
        return None, str(e)[:60]


with report("fix_s3_public_access") as r:
    r.heading("Update bucket policy to expose data/* + flow/crypto JSONs")

    # Show current for the record
    r.section("1. Current bucket policy")
    try:
        cur = s3.get_bucket_policy(Bucket=BUCKET)
        r.log(f"  Current: {cur['Policy'][:300]}")
    except Exception as e:
        r.log(f"  (no current policy: {e})")

    # Apply new policy
    r.section("2. Apply new policy")
    try:
        s3.put_bucket_policy(
            Bucket=BUCKET,
            Policy=json.dumps(NEW_POLICY),
        )
        r.ok("  New policy applied")
    except Exception as e:
        r.fail(f"  put_bucket_policy failed: {e}")
        raise SystemExit(1)

    # Probe the critical files
    r.section("3. Verify public HTTPS access")
    results = {}
    for key in TEST_FILES:
        url = f"https://{BUCKET}.s3.amazonaws.com/{key}"
        status, err = head(url)
        symbol = "✓" if status == 200 else "✗"
        r.log(f"  {symbol} {key}: {status} {err or ''}")
        results[key] = status

    success = sum(1 for v in results.values() if v == 200)
    r.kv(task="s3-policy", success=success, total=len(TEST_FILES))

    if success == len(TEST_FILES):
        r.ok(f"  ALL {success}/{len(TEST_FILES)} files now publicly accessible")
    else:
        r.warn(f"  Only {success}/{len(TEST_FILES)} accessible — investigate individually")

    r.log("Done")
