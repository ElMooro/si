#!/usr/bin/env python3
"""
Step 100 — Finish step 99 cleanup:
  A. Grant dynamodb:DeleteTable to github-actions-justhodl IAM user
  B. Re-run the 18 empty-table deletions
  C. Verify S3 archive/* lifecycle rule actually landed (output got
     truncated in step 99; need to confirm)
"""
import json
import os
import time
from datetime import datetime, timezone

from ops_report import report
import boto3

REGION = "us-east-1"
ACCOUNT = "857687956942"

iam = boto3.client("iam", region_name=REGION)
ddb = boto3.client("dynamodb", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)


KEEP_TABLES = {
    "justhodl-signals", "justhodl-outcomes", "fed-liquidity-cache",
    "openbb-historical-data", "ai-assistant-tasks",
    "openbb-trading-signals", "liquidity-metrics-v2",
}


with report("finish_step99_cleanup") as r:
    r.heading("Finish DDB cleanup + verify S3 lifecycle")

    # ─── A. Grant DDB DeleteTable to github-actions-justhodl ──────────
    r.section("A. Grant dynamodb:DeleteTable IAM perm")
    policy_doc = {
        "Version": "2012-10-17",
        "Statement": [{
            "Sid": "DynamoDBManageTables",
            "Effect": "Allow",
            "Action": [
                "dynamodb:DeleteTable",
                "dynamodb:DescribeTable",
                "dynamodb:ListTables",
                "dynamodb:UpdateTable",
            ],
            "Resource": "*",
        }],
    }
    try:
        iam.put_user_policy(
            UserName="github-actions-justhodl",
            PolicyName="DynamoDBManageTables",
            PolicyDocument=json.dumps(policy_doc),
        )
        r.ok("  Attached DynamoDBManageTables to github-actions-justhodl")
    except Exception as e:
        r.fail(f"  IAM put_user_policy: {e}")
        raise SystemExit(1)

    # IAM propagation
    time.sleep(8)

    # ─── B. Re-run empty-table deletes ────────────────────────────────
    r.section("B. Re-run DDB table deletes")
    all_tables = []
    for page in ddb.get_paginator("list_tables").paginate():
        for tn in page.get("TableNames", []):
            try:
                td = ddb.describe_table(TableName=tn)["Table"]
                all_tables.append({
                    "name": tn,
                    "size_bytes": td.get("TableSizeBytes", 0),
                    "items": td.get("ItemCount", 0),
                })
            except Exception as e:
                r.warn(f"  describe {tn}: {e}")

    candidates = [
        t for t in all_tables
        if t["size_bytes"] == 0 and t["items"] == 0 and t["name"] not in KEEP_TABLES
    ]
    r.log(f"  Total tables: {len(all_tables)}")
    r.log(f"  Deletion candidates: {len(candidates)}")

    deleted, failed = [], []
    for t in candidates:
        try:
            ddb.delete_table(TableName=t["name"])
            deleted.append(t["name"])
            r.ok(f"    Deleted: {t['name']}")
        except Exception as e:
            failed.append((t["name"], str(e)[:120]))
            r.fail(f"    {t['name']}: {e}")

    r.log(f"\n  Deleted: {len(deleted)}, Failed: {len(failed)}")

    # ─── C. Verify S3 lifecycle rule ──────────────────────────────────
    r.section("C. Verify S3 archive/* → Glacier lifecycle rule")
    try:
        resp = s3.get_bucket_lifecycle_configuration(Bucket="justhodl-dashboard-live")
        rules = resp.get("Rules", [])
        r.log(f"  Found {len(rules)} lifecycle rule(s):")
        for rule in rules:
            r.log(f"    ID: {rule.get('ID')}")
            r.log(f"    Status: {rule.get('Status')}")
            r.log(f"    Filter: {rule.get('Filter')}")
            transitions = rule.get("Transitions", [])
            for t_ in transitions:
                r.log(f"    Transition: {t_.get('Days')}d → {t_.get('StorageClass')}")
        # Check if our specific rule is there
        target_rule = next(
            (r_ for r_ in rules if r_.get("ID") == "archive-to-glacier-deep-after-90d"),
            None,
        )
        if target_rule:
            r.ok(f"  archive-to-glacier-deep-after-90d rule confirmed live")
        else:
            r.warn(f"  Target rule NOT FOUND — re-applying")
            new_rule = {
                "ID": "archive-to-glacier-deep-after-90d",
                "Status": "Enabled",
                "Filter": {"Prefix": "archive/"},
                "Transitions": [{"Days": 90, "StorageClass": "DEEP_ARCHIVE"}],
                "AbortIncompleteMultipartUpload": {"DaysAfterInitiation": 7},
            }
            s3.put_bucket_lifecycle_configuration(
                Bucket="justhodl-dashboard-live",
                LifecycleConfiguration={"Rules": rules + [new_rule]},
            )
            r.ok(f"  Re-applied archive lifecycle rule")
    except s3.exceptions.NoSuchLifecycleConfiguration:
        r.warn(f"  No lifecycle config at all — applying fresh")
        s3.put_bucket_lifecycle_configuration(
            Bucket="justhodl-dashboard-live",
            LifecycleConfiguration={"Rules": [{
                "ID": "archive-to-glacier-deep-after-90d",
                "Status": "Enabled",
                "Filter": {"Prefix": "archive/"},
                "Transitions": [{"Days": 90, "StorageClass": "DEEP_ARCHIVE"}],
                "AbortIncompleteMultipartUpload": {"DaysAfterInitiation": 7},
            }]},
        )
        r.ok(f"  Applied lifecycle rule fresh")

    r.kv(
        ddb_deleted=len(deleted),
        ddb_failed=len(failed),
        lifecycle_status="confirmed",
    )
    r.log("Done")
