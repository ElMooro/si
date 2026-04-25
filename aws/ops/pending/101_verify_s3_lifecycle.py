#!/usr/bin/env python3
"""
Step 101 — Fix the S3 lifecycle exception bug from step 100.

s3.exceptions.NoSuchLifecycleConfiguration doesn't exist as an attribute.
Use ClientError + check error code instead.
"""
import json
import os

from ops_report import report
import boto3
from botocore.exceptions import ClientError

REGION = "us-east-1"
s3 = boto3.client("s3", region_name=REGION)

with report("verify_s3_lifecycle") as r:
    r.heading("Verify S3 archive/* → Glacier lifecycle rule")

    target_id = "archive-to-glacier-deep-after-90d"
    target_rule = {
        "ID": target_id,
        "Status": "Enabled",
        "Filter": {"Prefix": "archive/"},
        "Transitions": [{"Days": 90, "StorageClass": "DEEP_ARCHIVE"}],
        "AbortIncompleteMultipartUpload": {"DaysAfterInitiation": 7},
    }

    rules = []
    try:
        resp = s3.get_bucket_lifecycle_configuration(Bucket="justhodl-dashboard-live")
        rules = resp.get("Rules", [])
        r.log(f"  Found {len(rules)} existing lifecycle rule(s)")
        for rule in rules:
            r.log(f"    - {rule.get('ID')}: {rule.get('Status')}")
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code", "")
        if code == "NoSuchLifecycleConfiguration":
            r.log(f"  No existing lifecycle config")
        else:
            r.fail(f"  Unexpected error: {e}")
            raise SystemExit(1)

    has_target = any(r_.get("ID") == target_id for r_ in rules)
    if has_target:
        r.ok(f"  Rule '{target_id}' already present")
        # Show its details
        rule = next(r_ for r_ in rules if r_.get("ID") == target_id)
        r.log(f"    Filter: {rule.get('Filter')}")
        r.log(f"    Status: {rule.get('Status')}")
        for t_ in rule.get("Transitions", []):
            r.log(f"    Transition: {t_.get('Days')}d → {t_.get('StorageClass')}")
    else:
        r.log(f"  Rule not present — applying")
        s3.put_bucket_lifecycle_configuration(
            Bucket="justhodl-dashboard-live",
            LifecycleConfiguration={"Rules": rules + [target_rule]},
        )
        r.ok(f"  Applied: archive/* → DEEP_ARCHIVE after 90 days")

    r.kv(target_rule_present=str(has_target).lower())
    r.log("Done")
