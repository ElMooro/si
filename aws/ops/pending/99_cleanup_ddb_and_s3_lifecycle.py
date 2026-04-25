#!/usr/bin/env python3
"""
Step 99 — Hygiene cleanup:

  A. Delete 18 empty DynamoDB tables that have been confirmed unused.
     Pay-per-request billing means $0 cost while keeping them, but
     deletion reduces console clutter and prevents accidental writes.

  B. Add S3 lifecycle policy: archive/* → Glacier Deep Archive after
     90 days. archive/ has 1,665 files, 29MB. Tiny direct savings
     (~$0.01/mo) but good hygiene + protects against future archive
     growth costs.

Both are reversible:
  A. DDB tables can be recreated (we save names + key schemas first
     so they CAN be re-created if needed).
  B. S3 lifecycle policies can be removed/edited via put_lifecycle_configuration.

Safety: the script saves a JSON snapshot of each DDB table's metadata
to S3 _audit/ddb_pre_delete.json BEFORE deleting, so we have a record
if anything was accidentally still in use.
"""
import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path

from ops_report import report
import boto3

REGION = "us-east-1"
REPO_ROOT = Path(os.environ.get("GITHUB_WORKSPACE", os.getcwd()))

ddb = boto3.client("dynamodb", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)


# Tables we KNOW are in use (NEVER touch these)
KEEP_TABLES = {
    "justhodl-signals",
    "justhodl-outcomes",
    "fed-liquidity-cache",
    # Below: small/tiny but presence in inventory step 79 suggests possible niche use
    "openbb-historical-data",     # 1 item — verify before delete
    "ai-assistant-tasks",         # 6 items — verify before delete
    "openbb-trading-signals",     # 2 items
    "liquidity-metrics-v2",       # 1 item
}


with report("cleanup_ddb_and_s3_lifecycle") as r:
    r.heading("Cleanup: empty DDB tables + S3 archive lifecycle")

    # ────────────────────────────────────────────────────────────────────
    # A. Find empty DDB tables + delete safely
    # ────────────────────────────────────────────────────────────────────
    r.section("A. List all DDB tables, identify empty ones")
    all_tables = []
    paginator = ddb.get_paginator("list_tables")
    for page in paginator.paginate():
        for tn in page.get("TableNames", []):
            try:
                td = ddb.describe_table(TableName=tn)["Table"]
                all_tables.append({
                    "name": tn,
                    "size_bytes": td.get("TableSizeBytes", 0),
                    "items": td.get("ItemCount", 0),
                    "billing": td.get("BillingModeSummary", {}).get("BillingMode"),
                    "key_schema": td.get("KeySchema"),
                    "attribute_definitions": td.get("AttributeDefinitions"),
                    "creation_date": str(td.get("CreationDateTime")),
                    "status": td.get("TableStatus"),
                })
            except Exception as e:
                r.warn(f"  describe {tn}: {e}")

    r.log(f"  Total tables: {len(all_tables)}")

    # Identify deletion candidates: empty AND not in KEEP_TABLES
    candidates = [
        t for t in all_tables
        if t["size_bytes"] == 0 and t["items"] == 0 and t["name"] not in KEEP_TABLES
    ]
    r.log(f"  Empty + not in KEEP set (deletion candidates): {len(candidates)}")
    for t in candidates:
        r.log(f"    {t['name']:50}  created={t['creation_date'][:10]}")

    # Snapshot to S3 BEFORE deleting (rollback capability)
    snapshot = {
        "snapshot_at": datetime.now(timezone.utc).isoformat(),
        "purpose": "Pre-deletion record of empty DDB tables. Can be used to recreate if needed.",
        "tables_to_delete": candidates,
        "all_tables_at_snapshot_time": all_tables,
    }
    s3.put_object(
        Bucket="justhodl-dashboard-live",
        Key=f"_audit/ddb_pre_delete_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.json",
        Body=json.dumps(snapshot, indent=2, default=str).encode(),
        ContentType="application/json",
    )
    r.ok(f"  Snapshot saved to S3 _audit/ddb_pre_delete_*.json (rollback ready)")

    # Delete each candidate
    r.section("A.2 Delete empty tables")
    deleted = []
    failed = []
    for t in candidates:
        name = t["name"]
        try:
            ddb.delete_table(TableName=name)
            deleted.append(name)
            r.ok(f"    Deleted: {name}")
        except Exception as e:
            failed.append((name, str(e)[:100]))
            r.fail(f"    {name}: {e}")

    r.log(f"\n  Deleted: {len(deleted)}, Failed: {len(failed)}")

    # ────────────────────────────────────────────────────────────────────
    # B. Add S3 lifecycle policy: archive/* → Glacier after 90 days
    # ────────────────────────────────────────────────────────────────────
    r.section("B. Add S3 lifecycle policy for archive/* → Glacier")

    # Read existing lifecycle policy if any
    existing_rules = []
    try:
        resp = s3.get_bucket_lifecycle_configuration(Bucket="justhodl-dashboard-live")
        existing_rules = resp.get("Rules", [])
        r.log(f"  Existing lifecycle rules: {len(existing_rules)}")
        for rule in existing_rules:
            r.log(f"    - {rule.get('ID', '?')}: {rule.get('Status')}")
    except s3.exceptions.NoSuchLifecycleConfiguration:
        r.log(f"  No existing lifecycle policy")
    except Exception as e:
        r.warn(f"  get_bucket_lifecycle_configuration: {e}")

    new_rule = {
        "ID": "archive-to-glacier-deep-after-90d",
        "Status": "Enabled",
        "Filter": {"Prefix": "archive/"},
        "Transitions": [{
            "Days": 90,
            "StorageClass": "DEEP_ARCHIVE",
        }],
        "AbortIncompleteMultipartUpload": {"DaysAfterInitiation": 7},
    }

    # Add rule if not already present
    rule_exists = any(r_.get("ID") == new_rule["ID"] for r_ in existing_rules)
    if rule_exists:
        r.log(f"  Lifecycle rule '{new_rule['ID']}' already exists; skipping")
    else:
        all_rules = existing_rules + [new_rule]
        try:
            s3.put_bucket_lifecycle_configuration(
                Bucket="justhodl-dashboard-live",
                LifecycleConfiguration={"Rules": all_rules},
            )
            r.ok(f"  Added lifecycle rule: archive/* → DEEP_ARCHIVE after 90 days")
        except Exception as e:
            r.fail(f"  put_bucket_lifecycle_configuration: {e}")

    r.kv(
        ddb_tables_deleted=len(deleted),
        ddb_failed=len(failed),
        s3_lifecycle_rule="archive-to-glacier-deep-after-90d",
    )
    r.log("Done")
