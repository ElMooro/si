#!/usr/bin/env python3
"""
Step 171 — Diagnose liquidity.html broken.

User reports liquidity.html isn't working after the revert. Check:
  A. What URL does liquidity.html actually fetch
  B. Does that key exist in S3
  C. Is it covered by the bucket policy
  D. Test fetch from inside AWS (works) vs how it'd look from browser

Pure diagnosis. No fixes yet.
"""
import json
from ops_report import report
import boto3

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
s3 = boto3.client("s3", region_name=REGION)

# Files that liquidity-style pages might fetch
SUSPECTS = [
    "liquidity-data.json",
    "data/liquidity-data.json",
    "data/liquidity.json",
    "fred-liquidity.json",
    "data/report.json",
]


with report("diagnose_liquidity") as r:
    r.heading("Why is liquidity.html broken?")

    # ─── A. Does liquidity-data.json exist in S3? ───────────────────────
    r.section("A. S3 key existence")
    for key in SUSPECTS:
        try:
            head = s3.head_object(Bucket=BUCKET, Key=key)
            size = head["ContentLength"]
            mod = head["LastModified"].strftime("%Y-%m-%d %H:%M:%S")
            r.log(f"  ✅ {key:35} {size:>10}B  {mod}")
        except s3.exceptions.ClientError as e:
            code = e.response["Error"]["Code"]
            r.log(f"  ❌ {key:35} {code}")

    # ─── B. Current bucket policy ───────────────────────────────────────
    r.section("B. Current bucket policy resources")
    pol = s3.get_bucket_policy(Bucket=BUCKET)
    policy = json.loads(pol["Policy"])
    for stmt in policy.get("Statement", []):
        sid = stmt.get("Sid", "?")
        res = stmt.get("Resource", "")
        if isinstance(res, list):
            r.log(f"  {sid}:")
            for x in res:
                short = x.replace(f"arn:aws:s3:::{BUCKET}/", "")
                r.log(f"    - {short}")
        else:
            short = res.replace(f"arn:aws:s3:::{BUCKET}/", "")
            r.log(f"  {sid}: {short}")

    # ─── C. Check if liquidity-data.json is publicly readable ──────────
    r.section("C. Is liquidity-data.json public-readable?")
    public_resources = set()
    for stmt in policy.get("Statement", []):
        if stmt.get("Effect") != "Allow":
            continue
        if "s3:GetObject" not in (stmt.get("Action") if isinstance(stmt.get("Action"), list) else [stmt.get("Action")]):
            continue
        princ = stmt.get("Principal", {})
        if princ != "*" and princ != {"AWS": "*"}:
            continue
        res = stmt.get("Resource", [])
        if isinstance(res, str):
            res = [res]
        for r_arn in res:
            public_resources.add(r_arn.replace(f"arn:aws:s3:::{BUCKET}/", ""))

    r.log(f"  All public resources ({len(public_resources)}):")
    for p in sorted(public_resources):
        r.log(f"    {p}")

    is_public = (
        "liquidity-data.json" in public_resources or
        any(p.endswith("*") and "liquidity-data.json".startswith(p[:-1]) for p in public_resources)
    )

    if is_public:
        r.ok(f"\n  ✅ liquidity-data.json IS publicly readable")
        r.log(f"  → Issue must be elsewhere (check key exists, browser cache)")
    else:
        r.warn(f"\n  ⚠ liquidity-data.json is NOT public — this is the bug")
        r.warn(f"  Need to add it to PublicReadRootDashboardFiles statement")

    # ─── D. List potentially needed root files for other pages ──────────
    r.section("D. What other root JSONs might pages need?")
    try:
        resp = s3.list_objects_v2(Bucket=BUCKET, Delimiter="/", MaxKeys=200)
        root_files = []
        for obj in resp.get("Contents", []):
            k = obj["Key"]
            if k.endswith(".json"):
                root_files.append((k, obj["Size"], obj["LastModified"]))
        r.log(f"  Root .json files: {len(root_files)}")
        for k, sz, mod in sorted(root_files):
            cov = "✅" if k in public_resources else "❌"
            r.log(f"    {cov} {k:45} {sz:>10}B")
    except Exception as e:
        r.warn(f"  list: {e}")

    r.kv(
        liquidity_public=is_public,
        n_public_resources=len(public_resources),
    )
    r.log("Done")
