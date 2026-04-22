#!/usr/bin/env python3
"""
Quick diagnostic — read data.json via boto3 (authenticated), inspect shape,
and confirm whether public access is broken too.
"""

import json
import urllib.request
import urllib.error
from ops_report import report
import boto3

BUCKET = "justhodl-dashboard-live"
KEY    = "data.json"
URL_REST    = f"https://{BUCKET}.s3.amazonaws.com/{KEY}"
URL_WEBSITE = f"http://{BUCKET}.s3-website-us-east-1.amazonaws.com/{KEY}"

s3 = boto3.client("s3", region_name="us-east-1")

with report("data_json_fetch_check") as r:
    r.heading("data.json reachability + shape")

    # 1. Authenticated boto3 read
    r.section("Authenticated S3 read (boto3)")
    try:
        obj = s3.get_object(Bucket=BUCKET, Key=KEY)
        body = obj["Body"].read().decode("utf-8")
        r.ok(f"Read {len(body)} bytes via boto3 — timestamp: {obj['LastModified'].isoformat()}")
        try:
            data = json.loads(body)
            if isinstance(data, dict):
                r.ok(f"Parsed as JSON. Top-level keys ({len(data)}):")
                for k in sorted(data.keys()):
                    v = data[k]
                    if isinstance(v, dict):
                        subkeys = sorted(v.keys())[:8]
                        r.log(f"  `{k}` → dict with subkeys: {subkeys}{'…' if len(v) > 8 else ''}")
                        r.kv(key=k, type="dict", size=len(v), sample=", ".join(subkeys))
                    elif isinstance(v, list):
                        r.log(f"  `{k}` → list (length {len(v)})")
                        r.kv(key=k, type="list", size=len(v), sample="")
                    elif isinstance(v, (int, float)):
                        r.log(f"  `{k}` → number: {v}")
                        r.kv(key=k, type="number", size=0, sample=str(v))
                    elif isinstance(v, str):
                        preview = v[:50]
                        r.log(f"  `{k}` → string: {preview}")
                        r.kv(key=k, type="string", size=len(v), sample=preview)
                    else:
                        r.log(f"  `{k}` → {type(v).__name__}: {v}")
                        r.kv(key=k, type=type(v).__name__, size=0, sample=str(v)[:40])
            else:
                r.warn(f"Not a dict: {type(data).__name__}")
        except json.JSONDecodeError as e:
            r.fail(f"Not valid JSON: {e}")
            r.log(f"First 200 chars: {body[:200]}")
    except Exception as e:
        r.fail(f"boto3 read failed: {e}")

    # 2. Anonymous public HTTPS read
    r.section("Public HTTPS read (what browsers/Workers see)")
    try:
        with urllib.request.urlopen(URL_REST, timeout=10) as resp:
            public_body = resp.read().decode("utf-8")
        r.ok(f"Public HTTPS read OK ({len(public_body)} bytes)")
    except urllib.error.HTTPError as e:
        r.fail(f"Public HTTPS read failed: HTTP {e.code} — {e.reason}")
        r.log(f"This means browser-based dashboards (edge.html, valuations.html, etc.) can't read data.json anymore")

    # 3. Bucket policy snapshot
    r.section("Current bucket policy + ACL")
    try:
        policy = s3.get_bucket_policy(Bucket=BUCKET)
        r.log(f"Bucket policy present ({len(policy['Policy'])} bytes):")
        # Parse and pretty-print it for the report
        p = json.loads(policy["Policy"])
        for stmt in p.get("Statement", []):
            r.log(f"  - Effect: {stmt.get('Effect')} | Principal: {stmt.get('Principal')} | Action: {stmt.get('Action')}")
    except s3.exceptions.from_code("NoSuchBucketPolicy"):
        r.warn("No bucket policy set — falling back to bucket ACL")
    except Exception as e:
        r.warn(f"Couldn't read bucket policy: {e}")

    try:
        pab = s3.get_public_access_block(Bucket=BUCKET)
        r.log(f"Public access block: {pab['PublicAccessBlockConfiguration']}")
    except Exception as e:
        r.log(f"Public access block: not set or inaccessible ({e})")

    r.log("Done")
