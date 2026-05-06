#!/usr/bin/env python3
"""Step 279 — Audit CloudWatch log groups vs deployed Lambdas.

Every Lambda creates /aws/lambda/<name> log group automatically. When the
Lambda is later deleted, the log group stays behind. These accumulate
forever with non-zero storage cost.

This script:
  1. Lists all CloudWatch log groups
  2. Lists all currently-deployed Lambdas
  3. Identifies orphan log groups (group exists but no parent Lambda)
  4. For each orphan, computes:
     - Age (last event time)
     - Size in bytes
     - Whether retention is set (unbounded retention = paying forever)
  5. Reports orphans for manual deletion (deletion is non-recoverable)
"""
import json
import os
import time
from datetime import datetime, timezone

import boto3
from botocore.exceptions import ClientError

REGION = "us-east-1"
REPORT_PATH = "aws/ops/reports/279_log_group_audit.json"

logs = boto3.client("logs", region_name=REGION)
lam = boto3.client("lambda", region_name=REGION)


def list_all_log_groups():
    out = []
    paginator = logs.get_paginator("describe_log_groups")
    for page in paginator.paginate():
        out.extend(page.get("logGroups", []))
    return out


def list_all_lambdas():
    out = set()
    paginator = lam.get_paginator("list_functions")
    for page in paginator.paginate():
        for f in page.get("Functions", []):
            out.add(f["FunctionName"])
    return out


def main():
    started = time.time()
    out = {"started": datetime.now(timezone.utc).isoformat()}
    try:
        all_groups = list_all_log_groups()
        all_lambdas = list_all_lambdas()
        out["n_log_groups"] = len(all_groups)
        out["n_deployed_lambdas"] = len(all_lambdas)

        # Bucket groups by category
        lambda_groups = []
        non_lambda_groups = []
        for g in all_groups:
            name = g["logGroupName"]
            if name.startswith("/aws/lambda/"):
                lambda_groups.append(g)
            else:
                non_lambda_groups.append(g)

        out["n_lambda_log_groups"] = len(lambda_groups)
        out["n_non_lambda_groups"] = len(non_lambda_groups)

        # Identify orphans
        orphans = []
        for g in lambda_groups:
            name = g["logGroupName"]
            lambda_name = name.replace("/aws/lambda/", "", 1)
            if lambda_name not in all_lambdas:
                orphans.append({
                    "log_group_name": name,
                    "lambda_name": lambda_name,
                    "size_bytes": g.get("storedBytes", 0),
                    "retention_days": g.get("retentionInDays"),  # None = unbounded
                    "created_ms": g.get("creationTime"),
                    "created_iso": (
                        datetime.fromtimestamp(g["creationTime"]/1000, tz=timezone.utc).isoformat()
                        if g.get("creationTime") else None
                    ),
                })

        # Aggregate by retention status
        unbounded = [o for o in orphans if o["retention_days"] is None]
        with_retention = [o for o in orphans if o["retention_days"] is not None]

        # Sort by size for prioritization
        orphans.sort(key=lambda o: -o["size_bytes"])

        out["n_orphan_log_groups"] = len(orphans)
        out["n_orphan_unbounded_retention"] = len(unbounded)
        out["n_orphan_with_retention"] = len(with_retention)
        out["total_orphan_bytes"] = sum(o["size_bytes"] for o in orphans)
        out["total_orphan_mb"] = round(out["total_orphan_bytes"] / (1024*1024), 2)

        # Top 20 by size
        out["top_20_by_size"] = orphans[:20]

        # All orphan names (compact list for cleanup script)
        out["all_orphan_log_group_names"] = [o["log_group_name"] for o in orphans]

        # Estimate cost (rough): CloudWatch Logs storage is ~$0.03/GB/month
        out["estimated_monthly_cost_usd"] = round(
            out["total_orphan_mb"] / 1024 * 0.03, 4
        )

        # Also sample non-Lambda groups (RDS, ECS, etc.) to flag potential
        # other orphans — but don't auto-classify them
        out["non_lambda_groups_sample"] = [
            {"name": g["logGroupName"], "size_bytes": g.get("storedBytes", 0),
             "retention_days": g.get("retentionInDays")}
            for g in sorted(non_lambda_groups, key=lambda g: -g.get("storedBytes", 0))[:10]
        ]

        out["duration_s"] = round(time.time() - started, 1)

    except Exception as e:
        import traceback
        out["fatal_error"] = str(e)
        out["traceback"] = traceback.format_exc()

    os.makedirs(os.path.dirname(REPORT_PATH), exist_ok=True)
    with open(REPORT_PATH, "w") as f:
        json.dump(out, f, indent=2, default=str)
    print(json.dumps(out, indent=2, default=str)[:5000])
    return 0 if "fatal_error" not in out else 1


if __name__ == "__main__":
    raise SystemExit(main())
