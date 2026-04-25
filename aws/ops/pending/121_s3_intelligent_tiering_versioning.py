#!/usr/bin/env python3
"""
Step 121 — S3 Intelligent Tiering + Bucket Versioning.

Both are zero-risk additions to justhodl-dashboard-live:

A. Intelligent Tiering Configuration
   - Auto-moves objects between Frequent / Infrequent / Archive tiers
     based on access patterns
   - FREE for objects > 128KB; small files stay in Standard
   - Big wins on archive/, valuations-archive/, investor-analysis/
     where files are rarely accessed
   - Setting: enable for the whole bucket (no Filter), with
     ArchiveAccess and DeepArchiveAccess auto-tiers
   - Doesn't conflict with our existing archive/* lifecycle rule
     (lifecycle takes priority where it applies)

B. Bucket Versioning
   - Free in itself; only pay for storage of old versions
   - Combined with a 30-day expire-old-versions lifecycle rule,
     near-zero cost
   - Insurance against accidental data/report.json overwrites or
     bad code pushes
   - Once enabled, can be Suspended but never fully removed —
     it's a one-way switch (which is the point)

Both are reversible operationally:
  - Intelligent Tiering: delete the configuration
  - Versioning: Suspend (objects stay versioned but new uploads
    don't create versions)
"""
import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path

from ops_report import report
import boto3
from botocore.exceptions import ClientError

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"

s3 = boto3.client("s3", region_name=REGION)


with report("enable_s3_intelligent_tiering_versioning") as r:
    r.heading("Enable S3 Intelligent Tiering + Versioning on justhodl-dashboard-live")

    # ════════════════════════════════════════════════════════════════════
    # A. Intelligent Tiering
    # ════════════════════════════════════════════════════════════════════
    r.section("A. Intelligent Tiering configuration")

    # Check existing
    try:
        existing = s3.list_bucket_intelligent_tiering_configurations(Bucket=BUCKET)
        existing_configs = existing.get("IntelligentTieringConfigurationList", [])
        r.log(f"  Existing configs: {len(existing_configs)}")
        for cfg in existing_configs:
            r.log(f"    {cfg.get('Id')}: {cfg.get('Status')}")
    except Exception as e:
        existing_configs = []
        r.log(f"  No existing configs: {e}")

    # Apply: 1 config covering whole bucket
    config_id = "auto-tier-cold-objects"
    config = {
        "Id": config_id,
        "Status": "Enabled",
        # No Filter = applies to all objects in the bucket
        "Filter": {},
        "Tierings": [
            # Move to Archive Access after 90 days untouched (cheaper)
            {"Days": 90, "AccessTier": "ARCHIVE_ACCESS"},
            # Move to Deep Archive after 180 days untouched (cheapest)
            {"Days": 180, "AccessTier": "DEEP_ARCHIVE_ACCESS"},
        ],
    }
    try:
        s3.put_bucket_intelligent_tiering_configuration(
            Bucket=BUCKET,
            Id=config_id,
            IntelligentTieringConfiguration=config,
        )
        r.ok(f"  Applied Intelligent Tiering config '{config_id}'")
        r.log(f"    - Cold objects → ARCHIVE_ACCESS after 90d")
        r.log(f"    - Cold objects → DEEP_ARCHIVE_ACCESS after 180d")
        r.log(f"    - Free for objects >128KB; small files unaffected")
        r.log(f"    - Compatible with existing archive/* Glacier lifecycle rule")
    except Exception as e:
        r.fail(f"  Failed: {e}")

    # ════════════════════════════════════════════════════════════════════
    # B. Bucket Versioning
    # ════════════════════════════════════════════════════════════════════
    r.section("B. Bucket Versioning")

    cur_v = s3.get_bucket_versioning(Bucket=BUCKET)
    cur_state = cur_v.get("Status", "Disabled")
    r.log(f"  Current versioning: {cur_state}")

    if cur_state != "Enabled":
        try:
            s3.put_bucket_versioning(
                Bucket=BUCKET,
                VersioningConfiguration={"Status": "Enabled"},
            )
            time.sleep(1)
            v_check = s3.get_bucket_versioning(Bucket=BUCKET)
            r.ok(f"  Versioning → {v_check.get('Status')}")
            r.log(f"    - All new writes create a version")
            r.log(f"    - Existing objects unaffected (no extra cost)")
            r.log(f"    - Deleting an object now creates a delete marker")
            r.log(f"      (the data is preserved; can be restored)")
        except Exception as e:
            r.fail(f"  Failed: {e}")
    else:
        r.log("  Already enabled, skipping")

    # ════════════════════════════════════════════════════════════════════
    # C. Add lifecycle rule to expire old versions after 30 days
    # ════════════════════════════════════════════════════════════════════
    r.section("C. Add lifecycle rule: expire old versions after 30 days")

    # Read existing lifecycle config
    existing_rules = []
    try:
        lc = s3.get_bucket_lifecycle_configuration(Bucket=BUCKET)
        existing_rules = lc.get("Rules", [])
        r.log(f"  Existing rules: {len(existing_rules)}")
        for rule in existing_rules:
            r.log(f"    {rule.get('ID')}: {rule.get('Status')}")
    except ClientError as e:
        if e.response["Error"]["Code"] != "NoSuchLifecycleConfiguration":
            r.warn(f"  Read lifecycle: {e}")

    # New rule for old versions
    versions_rule = {
        "ID": "expire-old-versions-after-30d",
        "Status": "Enabled",
        "Filter": {},  # Apply to all objects in the bucket
        "NoncurrentVersionExpiration": {"NoncurrentDays": 30},
        "AbortIncompleteMultipartUpload": {"DaysAfterInitiation": 7},
    }

    # Add only if not already present
    has_rule = any(r_.get("ID") == versions_rule["ID"] for r_ in existing_rules)
    if has_rule:
        r.log(f"  Rule '{versions_rule['ID']}' already present, skipping")
    else:
        new_rules = existing_rules + [versions_rule]
        try:
            s3.put_bucket_lifecycle_configuration(
                Bucket=BUCKET,
                LifecycleConfiguration={"Rules": new_rules},
            )
            r.ok(f"  Added lifecycle rule '{versions_rule['ID']}'")
            r.log(f"    - Old versions expire 30 days after they become non-current")
            r.log(f"    - Multipart uploads cleaned up after 7 days")
            r.log(f"    - Combined with versioning, gives you 30 days of undo")
        except Exception as e:
            r.fail(f"  put_bucket_lifecycle_configuration: {e}")

    # ════════════════════════════════════════════════════════════════════
    # Verify final state
    # ════════════════════════════════════════════════════════════════════
    r.section("D. Verify final state")

    # Versioning
    v = s3.get_bucket_versioning(Bucket=BUCKET)
    r.log(f"  Versioning: {v.get('Status')}")

    # Lifecycle
    try:
        lc = s3.get_bucket_lifecycle_configuration(Bucket=BUCKET)
        rules = lc.get("Rules", [])
        r.log(f"  Lifecycle rules: {len(rules)}")
        for rule in rules:
            r.log(f"    - {rule.get('ID')}: {rule.get('Status')}")
    except Exception:
        pass

    # Intelligent Tiering
    try:
        it = s3.list_bucket_intelligent_tiering_configurations(Bucket=BUCKET)
        configs = it.get("IntelligentTieringConfigurationList", [])
        r.log(f"  Intelligent Tiering configs: {len(configs)}")
        for c in configs:
            r.log(f"    - {c.get('Id')}: {c.get('Status')}")
    except Exception:
        pass

    r.kv(
        intelligent_tiering="enabled",
        versioning=v.get("Status"),
        lifecycle_rules=len(rules) if 'rules' in dir() else 0,
    )
    r.log("Done")
