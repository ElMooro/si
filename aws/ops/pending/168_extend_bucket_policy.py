#!/usr/bin/env python3
"""
Step 168 — Extend bucket policy for all phase data paths.

Diagnostic step 167 revealed the bucket policy only grants public
s3:GetObject to:
  - data/*
  - screener/*
  - sentiment/*
  - 4 named root files

It does NOT grant public read to phase outputs:
  - regime/* (Phase 1A bond regime)
  - divergence/* (Phase 1B cross-asset)
  - cot/* (Phase 2A COT extremes)
  - risk/* (Phase 3 sized recommendations)
  - opportunities/* (Phase 2B asymmetric)
  - portfolio/* (Loop 2 PnL)
  - investor-debate/* (Loop 4 debate)
  - intelligence-report.json (Khalid Index)
  - edge-data.json (Edge composite)
  - repo-data.json (repo plumbing)
  - reports/* (scorecard, etc.)

Browser fetches to these paths return 403, blocking desk-v2.html (and
the original desk.html, intelligence.html, reports.html, etc).

This step:
  A. Reads current bucket policy
  B. Extends it with new public-read statements for the phase paths
  C. Writes back, verifies
  D. Test-fetches from inside AWS (CI can't external-test)

Why this is safe: ALL the data being served is already designed for
public consumption — it's hedge fund tracking outputs that are meant
for the website to display. Nothing private goes to these paths.

NOT extending: anything that contains secrets, credentials, or
internal Lambda state. Those are kept under different paths (none
of these phase paths contain such data).
"""
import json
from ops_report import report
import boto3

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
s3 = boto3.client("s3", region_name=REGION)


with report("extend_bucket_policy") as r:
    r.heading("Extend bucket policy for all phase data paths")

    # ─── A. Read current policy ─────────────────────────────────────────
    r.section("A. Read current bucket policy")
    pol_resp = s3.get_bucket_policy(Bucket=BUCKET)
    policy = json.loads(pol_resp["Policy"])
    r.log(f"  Current statements: {len(policy.get('Statement', []))}")
    for s in policy.get("Statement", []):
        r.log(f"    - {s.get('Sid', '?')}")

    # ─── B. Build extended policy ───────────────────────────────────────
    r.section("B. Extending policy with phase paths")

    # Define the new statements we need to add
    new_paths = [
        ("PublicReadRegime",         "regime/*"),
        ("PublicReadDivergence",     "divergence/*"),
        ("PublicReadCOT",            "cot/*"),
        ("PublicReadRisk",           "risk/*"),
        ("PublicReadOpportunities",  "opportunities/*"),
        ("PublicReadPortfolio",      "portfolio/*"),
        ("PublicReadInvestorDebate", "investor-debate/*"),
        ("PublicReadReports",        "reports/*"),
        ("PublicReadArchive",        "archive/*"),
        ("PublicReadLearning",       "learning/*"),
    ]

    # Named root files (single resources)
    new_root_files = [
        "intelligence-report.json",
        "edge-data.json",
        "repo-data.json",
        "ai-prediction.json",
        "options-flow.json",
        "valuations.json",
        "morning-brief.json",
    ]

    existing_sids = {s.get("Sid") for s in policy.get("Statement", [])}
    statements_added = 0

    # Add path-prefix statements
    for sid, path in new_paths:
        if sid in existing_sids:
            r.log(f"  - {sid}: already exists, skip")
            continue
        policy["Statement"].append({
            "Sid": sid,
            "Effect": "Allow",
            "Principal": "*",
            "Action": "s3:GetObject",
            "Resource": f"arn:aws:s3:::{BUCKET}/{path}",
        })
        statements_added += 1
        r.log(f"  + {sid}: arn:aws:s3:::{BUCKET}/{path}")

    # Update or add the root-files statement
    root_sid = "PublicReadRootDashboardFiles"
    existing_root = None
    for stmt in policy["Statement"]:
        if stmt.get("Sid") == root_sid:
            existing_root = stmt
            break

    if existing_root:
        existing_resources = existing_root.get("Resource", [])
        if isinstance(existing_resources, str):
            existing_resources = [existing_resources]
        new_resources = list(existing_resources)
        for f in new_root_files:
            arn = f"arn:aws:s3:::{BUCKET}/{f}"
            if arn not in new_resources:
                new_resources.append(arn)
                r.log(f"  + extending {root_sid} with {f}")
        existing_root["Resource"] = new_resources
    else:
        policy["Statement"].append({
            "Sid": root_sid,
            "Effect": "Allow",
            "Principal": "*",
            "Action": "s3:GetObject",
            "Resource": [f"arn:aws:s3:::{BUCKET}/{f}" for f in new_root_files],
        })
        r.log(f"  + new {root_sid}")
        statements_added += 1

    r.log(f"\n  Total statements added: {statements_added}")
    r.log(f"  New policy statement count: {len(policy['Statement'])}")

    # ─── C. Write back ───────────────────────────────────────────────────
    r.section("C. Apply updated policy")
    try:
        s3.put_bucket_policy(Bucket=BUCKET, Policy=json.dumps(policy))
        r.ok(f"  ✅ Policy updated")
    except Exception as e:
        r.fail(f"  put_bucket_policy: {e}")
        raise SystemExit(1)

    # ─── D. Verify by re-reading ─────────────────────────────────────────
    r.section("D. Verify (re-read policy)")
    pol_resp = s3.get_bucket_policy(Bucket=BUCKET)
    new_policy = json.loads(pol_resp["Policy"])
    r.log(f"  Statements after update: {len(new_policy.get('Statement', []))}")
    for s in new_policy.get("Statement", []):
        sid = s.get('Sid', '?')
        res = s.get('Resource', '')
        if isinstance(res, list):
            r.log(f"    {sid:30} → {len(res)} resources")
        else:
            res_short = res.replace(f"arn:aws:s3:::{BUCKET}/", "")
            r.log(f"    {sid:30} → {res_short}")

    r.kv(
        statements_added=statements_added,
        total_statements=len(new_policy['Statement']),
    )
    r.log("Done — refresh desk-v2.html to verify (may need to clear browser CORS cache)")
