#!/usr/bin/env python3
"""Step 280 — Delete orphan CloudWatch log groups identified in step 279.

Reads aws/ops/reports/279_log_group_audit.json for the list of orphans.
Deletes all 52 Lambda orphan log groups + the 4 OpenBB AppRunner/ECS
log groups (services retired earlier).

Safeguards:
  - Re-verifies each Lambda log group has no parent Lambda (defense
    in depth — in case a Lambda was created between 279 and now)
  - Re-verifies each Lambda log group is small (<10 MB) before
    deletion — protects against accidentally nuking a recently-created
    log group with real data
  - Per-deletion error capture so a single failure doesn't kill the run
"""
import json
import os
import time
from datetime import datetime, timezone

import boto3
from botocore.exceptions import ClientError

REGION = "us-east-1"
AUDIT_REPORT = "aws/ops/reports/279_log_group_audit.json"
REPORT_PATH = "aws/ops/reports/280_log_group_cleanup.json"
SIZE_LIMIT_MB = 10  # safeguard

logs = boto3.client("logs", region_name=REGION)
lam = boto3.client("lambda", region_name=REGION)


def lambda_exists(name: str) -> bool:
    try:
        lam.get_function(FunctionName=name)
        return True
    except ClientError as e:
        if e.response["Error"]["Code"] == "ResourceNotFoundException":
            return False
        raise


def main():
    started = time.time()
    out = {"started": datetime.now(timezone.utc).isoformat()}
    try:
        # Read prior audit
        with open(AUDIT_REPORT) as f:
            audit = json.load(f)

        candidates = audit.get("all_orphan_log_group_names") or []
        # Add the OpenBB groups identified in non_lambda_groups_sample
        openbb_groups = [
            "/aws/apprunner/openbb-api/1ccdfbc8a3ab43cca282e6a6fd10a72f/application",
            "/aws/apprunner/openbb-api/1ccdfbc8a3ab43cca282e6a6fd10a72f/service",
            "//ecs//openbb-api",
            "/ecs/openbb-api",
            "/ecs/openbb-websocket-api",
        ]
        # Don't auto-include /aws/sagemaker — Khalid may still use Studio

        out["n_candidates"] = len(candidates) + len(openbb_groups)
        out["lambda_orphan_count"] = len(candidates)
        out["openbb_count"] = len(openbb_groups)

        deleted = []
        skipped = []
        errors = []

        # Lambda orphans — re-verify each before deletion
        for log_group_name in candidates:
            lambda_name = log_group_name.replace("/aws/lambda/", "", 1)
            try:
                # Defense in depth — re-check parent existence
                if lambda_exists(lambda_name):
                    skipped.append({"group": log_group_name,
                                    "reason": "parent Lambda exists now (created since audit)"})
                    continue

                # Re-check size
                desc = logs.describe_log_groups(logGroupNamePrefix=log_group_name)
                groups = desc.get("logGroups", [])
                matching = [g for g in groups if g["logGroupName"] == log_group_name]
                if not matching:
                    skipped.append({"group": log_group_name, "reason": "no longer exists"})
                    continue
                size = matching[0].get("storedBytes", 0)
                if size > SIZE_LIMIT_MB * 1024 * 1024:
                    skipped.append({"group": log_group_name,
                                    "reason": f"size {size} > {SIZE_LIMIT_MB}MB safeguard"})
                    continue

                logs.delete_log_group(logGroupName=log_group_name)
                deleted.append({"group": log_group_name, "size_bytes": size})
            except Exception as e:
                errors.append({"group": log_group_name, "err": str(e)[:200]})

        # OpenBB groups — these are AppRunner/ECS, no Lambda parent check
        for log_group_name in openbb_groups:
            try:
                desc = logs.describe_log_groups(logGroupNamePrefix=log_group_name)
                groups = desc.get("logGroups", [])
                matching = [g for g in groups if g["logGroupName"] == log_group_name]
                if not matching:
                    skipped.append({"group": log_group_name, "reason": "no longer exists"})
                    continue
                size = matching[0].get("storedBytes", 0)
                if size > SIZE_LIMIT_MB * 1024 * 1024:
                    skipped.append({"group": log_group_name,
                                    "reason": f"size {size} > {SIZE_LIMIT_MB}MB safeguard"})
                    continue

                logs.delete_log_group(logGroupName=log_group_name)
                deleted.append({"group": log_group_name, "size_bytes": size,
                                "category": "openbb_retired"})
            except Exception as e:
                errors.append({"group": log_group_name, "err": str(e)[:200]})

        out["deleted_count"] = len(deleted)
        out["skipped_count"] = len(skipped)
        out["errors_count"] = len(errors)
        out["deleted"] = deleted
        out["skipped"] = skipped
        out["errors"] = errors
        out["total_freed_bytes"] = sum(d.get("size_bytes", 0) for d in deleted)
        out["duration_s"] = round(time.time() - started, 1)

    except Exception as e:
        import traceback
        out["fatal_error"] = str(e)
        out["traceback"] = traceback.format_exc()

    os.makedirs(os.path.dirname(REPORT_PATH), exist_ok=True)
    with open(REPORT_PATH, "w") as f:
        json.dump(out, f, indent=2, default=str)
    # Truncated print since deleted list can be long
    summary = {k: v for k, v in out.items() if k not in ("deleted", "skipped", "errors")}
    summary["sample_deleted"] = (out.get("deleted") or [])[:5]
    summary["sample_skipped"] = (out.get("skipped") or [])[:3]
    summary["sample_errors"] = (out.get("errors") or [])[:3]
    print(json.dumps(summary, indent=2, default=str)[:3000])
    return 0 if "fatal_error" not in out else 1


if __name__ == "__main__":
    raise SystemExit(main())
