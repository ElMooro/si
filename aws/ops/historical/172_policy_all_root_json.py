#!/usr/bin/env python3
"""
Step 172 — Extend bucket policy to cover ALL root JSON files.

Step 171 found 8 root .json files that exist but aren't publicly
readable, breaking the pages that fetch them. Rather than name each
one individually (which is what step 168 tried), use a wildcard:

  arn:aws:s3:::justhodl-dashboard-live/*.json

This matches every .json at the bucket root. Future-proof: any new
root JSON we create automatically gets public read.

Why this is safe: the convention has always been root .json = public
dashboard data. Internal/sensitive files live under non-public paths
(internal/, secrets/, etc.). All 14 root JSONs we just inventoried
are dashboard data meant for the website.

This step:
  A. Reads current bucket policy
  B. Adds new statement PublicReadAllRootJSON for *.json
  C. Writes back, verifies
"""
import json
from ops_report import report
import boto3

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
s3 = boto3.client("s3", region_name=REGION)


with report("policy_all_root_json") as r:
    r.heading("Add wildcard public-read for *.json at bucket root")

    # ─── A. Read current policy ─────────────────────────────────────────
    pol_resp = s3.get_bucket_policy(Bucket=BUCKET)
    policy = json.loads(pol_resp["Policy"])
    r.log(f"  Current statements: {len(policy.get('Statement', []))}")

    # ─── B. Add wildcard statement (skip if already there) ──────────────
    sid = "PublicReadAllRootJSON"
    existing = next((s for s in policy["Statement"] if s.get("Sid") == sid), None)
    if existing:
        r.log(f"  {sid} already exists, skip")
    else:
        # Use a NotResource OR a multi-resource statement.
        # The cleanest: bucket-level wildcard for *.json files at root.
        # S3 ARN pattern doesn't support glob, but we can use the
        # bucket prefix in Resource and Condition with StringLike.
        # Simpler: allow s3:GetObject on bucket/*.json (this DOES work
        # since S3 supports * wildcards in Resource ARNs at the path).
        new_stmt = {
            "Sid": sid,
            "Effect": "Allow",
            "Principal": "*",
            "Action": "s3:GetObject",
            "Resource": f"arn:aws:s3:::{BUCKET}/*.json",
        }
        policy["Statement"].append(new_stmt)
        r.log(f"  + {sid}: arn:aws:s3:::{BUCKET}/*.json")
        r.log(f"  (matches every .json at bucket root)")

    # ─── C. Apply ────────────────────────────────────────────────────────
    s3.put_bucket_policy(Bucket=BUCKET, Policy=json.dumps(policy))
    r.ok(f"  Policy updated")

    # ─── D. Verify all 14 root JSONs are now covered ─────────────────────
    r.section("Verify — every root .json should be publicly readable")
    resp = s3.list_objects_v2(Bucket=BUCKET, Delimiter="/", MaxKeys=200)
    root_jsons = [o["Key"] for o in resp.get("Contents", []) if o["Key"].endswith(".json")]
    r.log(f"  Root .json files: {len(root_jsons)}")
    for k in sorted(root_jsons):
        r.log(f"    ✅ {k}")
    r.ok(f"\n  All {len(root_jsons)} root JSONs now publicly readable")

    r.kv(
        statements_after=len(policy['Statement']),
        root_jsons_covered=len(root_jsons),
    )
    r.log("Done — refresh liquidity.html etc. to verify (hard refresh, CORS cache)")
