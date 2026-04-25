#!/usr/bin/env python3
"""
System inventory — collect raw facts about every component for the
canonical architecture doc.

Outputs structured JSON to s3://justhodl-dashboard-live/_audit/inventory.json
plus a markdown report.

Sections:
  A. All Lambda functions (name, runtime, schedule, last_modified, env_keys,
     triggers, code_size_bytes)
  B. All S3 keys at top level + first-level prefixes with size + last_modified
  C. All DynamoDB tables (name, item_count, size_bytes, billing_mode)
  D. All SSM parameters under /justhodl/* (name, type, last_modified, value_length)
  E. All EventBridge rules (name, schedule, target, enabled)

This is read-only. No changes to live infrastructure.
"""
import json
import os
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

from ops_report import report
import boto3

REGION = "us-east-1"
REPO_ROOT = Path(os.environ.get("GITHUB_WORKSPACE", os.getcwd()))

lam = boto3.client("lambda", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)
ddb = boto3.client("dynamodb", region_name=REGION)
ssm = boto3.client("ssm", region_name=REGION)
eb = boto3.client("events", region_name=REGION)


def all_lambdas():
    """Paginate through all Lambda functions in the account."""
    out = []
    paginator = lam.get_paginator("list_functions")
    for page in paginator.paginate():
        out.extend(page.get("Functions", []))
    return out


def all_s3_keys(bucket="justhodl-dashboard-live"):
    """List all keys, capped at 5000."""
    out = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket):
        out.extend(page.get("Contents", []))
        if len(out) >= 5000:
            break
    return out


def all_ddb_tables():
    out = []
    paginator = ddb.get_paginator("list_tables")
    for page in paginator.paginate():
        for tn in page.get("TableNames", []):
            try:
                td = ddb.describe_table(TableName=tn)["Table"]
                out.append({
                    "name": tn,
                    "item_count": td.get("ItemCount"),
                    "size_bytes": td.get("TableSizeBytes"),
                    "billing_mode": td.get("BillingModeSummary", {}).get("BillingMode", "PROVISIONED"),
                    "status": td.get("TableStatus"),
                    "key_schema": [k.get("AttributeName") for k in td.get("KeySchema", [])],
                    "ttl_enabled": False,  # Could query separately but skip for now
                })
            except Exception as e:
                out.append({"name": tn, "error": str(e)})
    return out


def all_ssm_params(prefix="/justhodl/"):
    out = []
    paginator = ssm.get_paginator("describe_parameters")
    for page in paginator.paginate(
        ParameterFilters=[{"Key": "Name", "Option": "BeginsWith", "Values": [prefix]}]
    ):
        out.extend(page.get("Parameters", []))
    return out


def all_eb_rules():
    out = []
    paginator = eb.get_paginator("list_rules")
    for page in paginator.paginate():
        for rule in page.get("Rules", []):
            try:
                targets = eb.list_targets_by_rule(Rule=rule["Name"]).get("Targets", [])
                out.append({
                    "name": rule.get("Name"),
                    "state": rule.get("State"),
                    "schedule": rule.get("ScheduleExpression"),
                    "description": rule.get("Description", "")[:200],
                    "target_count": len(targets),
                    "targets": [{
                        "id": t.get("Id"),
                        "arn": t.get("Arn", "")[-80:],
                    } for t in targets[:3]],
                })
            except Exception as e:
                out.append({"name": rule.get("Name"), "error": str(e)})
    return out


with report("system_inventory_full") as r:
    r.heading("Full system inventory — for canonical architecture doc")

    # ─── A. Lambdas ───
    r.section("A. Lambda functions")
    lambdas = all_lambdas()
    r.log(f"  Total: {len(lambdas)}")

    # Get triggers for each via EB rules-by-target lookup is expensive;
    # we'll cross-reference with EB rules below
    lam_data = []
    for fn in lambdas:
        env = (fn.get("Environment") or {}).get("Variables") or {}
        lam_data.append({
            "name": fn["FunctionName"],
            "runtime": fn.get("Runtime"),
            "memory_mb": fn.get("MemorySize"),
            "timeout_s": fn.get("Timeout"),
            "code_size_bytes": fn.get("CodeSize"),
            "last_modified": fn.get("LastModified"),
            "description": (fn.get("Description") or "")[:300],
            "handler": fn.get("Handler"),
            "env_keys": sorted(list(env.keys())),
            "role_arn_tail": (fn.get("Role") or "")[-50:],
        })

    # Bucket by naming convention for the doc
    by_prefix = defaultdict(list)
    for l in lam_data:
        prefix = l["name"].split("-")[0] if "-" in l["name"] else l["name"]
        by_prefix[prefix].append(l["name"])
    r.log(f"  Naming clusters:")
    for p, names in sorted(by_prefix.items(), key=lambda x: -len(x[1])):
        r.log(f"    {p}: {len(names)} (e.g. {names[0]})")

    # ─── B. S3 keys ───
    r.section("B. S3 keys (justhodl-dashboard-live)")
    keys = all_s3_keys()
    r.log(f"  Total objects (capped): {len(keys)}")

    # Bucket by first prefix
    by_dir = defaultdict(list)
    root_files = []
    for k in keys:
        path = k["Key"]
        if "/" in path:
            top = path.split("/")[0]
            by_dir[top].append(k)
        else:
            root_files.append(k)
    r.log(f"  Top-level directories:")
    for d, items in sorted(by_dir.items(), key=lambda x: -len(x[1])):
        total_size = sum(i["Size"] for i in items)
        newest = max(items, key=lambda i: i["LastModified"])
        newest_age_h = (datetime.now(timezone.utc) - newest["LastModified"]).total_seconds() / 3600
        r.log(f"    {d}/ — {len(items)} files, {total_size/1024:.0f}KB, newest {newest_age_h:.1f}h old")
    r.log(f"  Root-level files: {len(root_files)}")
    for f in sorted(root_files, key=lambda x: x["LastModified"], reverse=True)[:30]:
        age_h = (datetime.now(timezone.utc) - f["LastModified"]).total_seconds() / 3600
        r.log(f"    {f['Key']:45} {f['Size']:>10} bytes  ({age_h:>6.1f}h)")

    # ─── C. DynamoDB ───
    r.section("C. DynamoDB tables")
    tables = all_ddb_tables()
    for t in sorted(tables, key=lambda x: x.get("size_bytes") or 0, reverse=True):
        r.log(f"  {t.get('name'):40} items={t.get('item_count'):>10} size={(t.get('size_bytes') or 0)/1024:>10.0f}KB billing={t.get('billing_mode')}")

    # ─── D. SSM parameters ───
    r.section("D. SSM parameters under /justhodl/")
    ssm_params = all_ssm_params()
    r.log(f"  Total: {len(ssm_params)}")
    for p in ssm_params:
        lm = p.get("LastModifiedDate")
        age_h = ((datetime.now(timezone.utc) - lm).total_seconds() / 3600) if lm else None
        r.log(f"  {p.get('Name'):60} type={p.get('Type'):10} age={age_h:>5.1f}h" if age_h else f"  {p.get('Name')}")

    # ─── E. EventBridge rules ───
    r.section("E. EventBridge rules")
    rules = all_eb_rules()
    r.log(f"  Total: {len(rules)}")
    enabled = sum(1 for r2 in rules if r2.get("state") == "ENABLED")
    r.log(f"  Enabled: {enabled}, disabled: {len(rules) - enabled}")
    # Group by target Lambda
    by_target = defaultdict(list)
    for rule in rules:
        for t in rule.get("targets", []):
            tail = t["arn"].split(":")[-1] if ":" in t["arn"] else t["arn"]
            by_target[tail].append(rule)

    r.log(f"\n  Schedule summary (top 30 by count):")
    for tgt, rules_for_tgt in sorted(by_target.items(), key=lambda x: -len(x[1]))[:30]:
        schedules = [r2.get("schedule", "?") for r2 in rules_for_tgt]
        r.log(f"    {tgt:50} {len(rules_for_tgt)} rule(s): {schedules[:2]}")

    # ─── Save full structured JSON to S3 ───
    inventory = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "lambdas": lam_data,
        "s3_keys": [{"key": k["Key"], "size": k["Size"], "last_modified": k["LastModified"].isoformat()} for k in keys],
        "ddb_tables": tables,
        "ssm_params": [{"name": p.get("Name"), "type": p.get("Type"), "last_modified": p.get("LastModifiedDate").isoformat() if p.get("LastModifiedDate") else None} for p in ssm_params],
        "eb_rules": rules,
    }
    s3.put_object(
        Bucket="justhodl-dashboard-live",
        Key="_audit/inventory_2026-04-25.json",
        Body=json.dumps(inventory, indent=2, default=str),
        ContentType="application/json",
    )
    r.ok(f"  Saved structured inventory to s3://justhodl-dashboard-live/_audit/inventory_2026-04-25.json")

    # Also save a copy in the repo for direct reference
    out_path = REPO_ROOT / "aws/ops/audit/inventory_2026-04-25.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(inventory, indent=2, default=str))
    r.ok(f"  Saved to repo: aws/ops/audit/inventory_2026-04-25.json")

    r.kv(
        lambdas=len(lambdas),
        s3_objects=len(keys),
        ddb_tables=len(tables),
        ssm_params=len(ssm_params),
        eb_rules=len(rules),
    )
    r.log("Done")
