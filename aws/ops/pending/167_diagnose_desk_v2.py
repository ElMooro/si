#!/usr/bin/env python3
"""
Diagnose desk-v2.html "no data" — figure out which fetches are failing
and why. Three possible causes:
  1. CORS not configured on bucket → browser blocks but server returns 200
  2. Keys don't exist (404)
  3. Bucket is private (403)

This step:
  A. List S3 bucket CORS configuration
  B. For each key desk-v2 fetches: HEAD it from inside AWS, report status
  C. Check the bucket policy / public access settings
"""
import json
from ops_report import report
import boto3

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"

s3 = boto3.client("s3", region_name=REGION)

KEYS = [
    "regime/current.json",
    "divergence/current.json",
    "cot/extremes/current.json",
    "risk/recommendations.json",
    "opportunities/asymmetric-equity.json",
    "portfolio/pnl-daily.json",
    "investor-debate/_index.json",
    "intelligence-report.json",
    "crypto-intel.json",
]

with report("diagnose_desk_v2_no_data") as r:
    r.heading("Diagnose desk-v2.html 'no data' issue")

    # ─── A. CORS ─────────────────────────────────────────────────────────
    r.section("A. Bucket CORS configuration")
    try:
        cors = s3.get_bucket_cors(Bucket=BUCKET)
        rules = cors.get("CORSRules", [])
        r.log(f"  CORS rules: {len(rules)}")
        for i, rule in enumerate(rules):
            r.log(f"  Rule #{i+1}:")
            r.log(f"    AllowedOrigins: {rule.get('AllowedOrigins')}")
            r.log(f"    AllowedMethods: {rule.get('AllowedMethods')}")
            r.log(f"    AllowedHeaders: {rule.get('AllowedHeaders', [])}")
            r.log(f"    ExposeHeaders:  {rule.get('ExposeHeaders', [])}")
            r.log(f"    MaxAgeSeconds:  {rule.get('MaxAgeSeconds')}")
    except s3.exceptions.ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchCORSConfiguration':
            r.fail(f"  ❌ NO CORS CONFIGURED — this is the bug")
        else:
            r.warn(f"  CORS read error: {e}")

    # ─── B. Bucket public access settings ────────────────────────────────
    r.section("B. Bucket public access block")
    try:
        pab = s3.get_public_access_block(Bucket=BUCKET)
        cfg = pab.get("PublicAccessBlockConfiguration", {})
        r.log(f"  BlockPublicAcls:       {cfg.get('BlockPublicAcls')}")
        r.log(f"  IgnorePublicAcls:      {cfg.get('IgnorePublicAcls')}")
        r.log(f"  BlockPublicPolicy:     {cfg.get('BlockPublicPolicy')}")
        r.log(f"  RestrictPublicBuckets: {cfg.get('RestrictPublicBuckets')}")
    except s3.exceptions.ClientError as e:
        if 'NoSuchPublicAccessBlock' in str(e):
            r.log(f"  No public access block (default — public)")
        else:
            r.warn(f"  read PAB: {e}")

    # ─── C. Bucket policy ────────────────────────────────────────────────
    r.section("C. Bucket policy (controls public read)")
    try:
        pol = s3.get_bucket_policy(Bucket=BUCKET)
        policy = json.loads(pol.get("Policy", "{}"))
        for stmt in policy.get("Statement", []):
            r.log(f"  {stmt.get('Sid', '?')}:")
            r.log(f"    Effect:    {stmt.get('Effect')}")
            r.log(f"    Principal: {stmt.get('Principal')}")
            r.log(f"    Action:    {stmt.get('Action')}")
            res = stmt.get('Resource', '')
            if isinstance(res, list):
                for x in res: r.log(f"    Resource:  {x}")
            else:
                r.log(f"    Resource:  {res}")
    except s3.exceptions.ClientError as e:
        if 'NoSuchBucketPolicy' in str(e):
            r.warn(f"  ❌ NO BUCKET POLICY — bucket likely private")
        else:
            r.warn(f"  read policy: {e}")

    # ─── D. Key existence + sizes ────────────────────────────────────────
    r.section("D. Per-key status (do they exist?)")
    found = 0
    missing = 0
    for key in KEYS:
        try:
            head = s3.head_object(Bucket=BUCKET, Key=key)
            size = head['ContentLength']
            mod = head['LastModified'].strftime("%Y-%m-%d %H:%M")
            r.log(f"  ✅ {key:50} {size:>8}B  {mod}")
            found += 1
        except s3.exceptions.ClientError as e:
            code = e.response['Error']['Code']
            r.warn(f"  ❌ {key:50} {code}")
            missing += 1

    r.section("E. Diagnosis")
    r.log(f"  Found: {found} / {len(KEYS)} keys")
    if found < len(KEYS):
        r.warn(f"  Some keys are missing — Lambdas haven't run yet, OR")
        r.warn(f"  the path is different from what desk-v2 fetches.")

    r.kv(
        keys_found=found,
        keys_missing=missing,
        keys_total=len(KEYS),
    )
    r.log("Done")
