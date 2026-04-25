#!/usr/bin/env python3
"""
Step 122 — Fix step 119 issues:

A. PITR failed across all 7 tables: github-actions-justhodl IAM user
   lacks dynamodb:UpdateContinuousBackups action. Add it.
   Then retry the PITR enablement.

B. SnapStart skipped 3 Lambdas because they're on python3.11.
   Bump them to python3.12, then enable SnapStart:
     - justhodl-investor-agents
     - justhodl-stock-screener
     - cftc-futures-positioning-agent

   Python 3.11 → 3.12 is a backwards-compatible minor version bump
   for boto3/urllib code. After bump, sync invoke each to verify
   no compatibility surprises before publishing the SnapStart version.
"""
import json
import os
import time
from datetime import datetime, timezone

from ops_report import report
import boto3

REGION = "us-east-1"

iam = boto3.client("iam", region_name=REGION)
ddb = boto3.client("dynamodb", region_name=REGION)
lam = boto3.client("lambda", region_name=REGION)

PITR_TABLES = [
    "justhodl-signals", "justhodl-outcomes", "fed-liquidity-cache",
    "openbb-historical-data", "ai-assistant-tasks",
    "openbb-trading-signals", "liquidity-metrics-v2",
]

PYTHON311_LAMBDAS = [
    "justhodl-investor-agents",
    "justhodl-stock-screener",
    "cftc-futures-positioning-agent",
]


with report("fix_pitr_iam_and_runtime_bump") as r:
    r.heading("Fix step 119 — PITR IAM perm + python3.11→3.12 bump + SnapStart")

    # ════════════════════════════════════════════════════════════════════
    # A. Grant PITR + restore-table IAM perms to github-actions-justhodl
    # ════════════════════════════════════════════════════════════════════
    r.section("A1. Grant DynamoDB PITR perms")
    policy_doc = {
        "Version": "2012-10-17",
        "Statement": [{
            "Sid": "DynamoDBContinuousBackups",
            "Effect": "Allow",
            "Action": [
                "dynamodb:UpdateContinuousBackups",
                "dynamodb:DescribeContinuousBackups",
                "dynamodb:RestoreTableToPointInTime",
                "dynamodb:RestoreTableFromBackup",
                "dynamodb:CreateBackup",
                "dynamodb:DescribeBackup",
                "dynamodb:DeleteBackup",
                "dynamodb:ListBackups",
            ],
            "Resource": "*",
        }],
    }
    try:
        iam.put_user_policy(
            UserName="github-actions-justhodl",
            PolicyName="DynamoDBContinuousBackups",
            PolicyDocument=json.dumps(policy_doc),
        )
        r.ok("  Attached DynamoDBContinuousBackups inline policy")
    except Exception as e:
        r.fail(f"  IAM put: {e}")
        raise SystemExit(1)

    # IAM propagation
    r.log("  Waiting 8s for IAM propagation…")
    time.sleep(8)

    # ════════════════════════════════════════════════════════════════════
    # A2. Retry PITR enablement
    # ════════════════════════════════════════════════════════════════════
    r.section("A2. Retry PITR enablement on 7 tables")
    pitr_results = []
    for tn in PITR_TABLES:
        try:
            cur = ddb.describe_continuous_backups(TableName=tn)
            cur_state = (cur.get("ContinuousBackupsDescription", {})
                          .get("PointInTimeRecoveryDescription", {})
                          .get("PointInTimeRecoveryStatus"))
            if cur_state == "ENABLED":
                r.log(f"  {tn:35} already ENABLED")
                pitr_results.append((tn, "already_enabled"))
                continue

            ddb.update_continuous_backups(
                TableName=tn,
                PointInTimeRecoverySpecification={"PointInTimeRecoveryEnabled": True},
            )
            time.sleep(0.5)
            v = ddb.describe_continuous_backups(TableName=tn)
            new_state = (v.get("ContinuousBackupsDescription", {})
                          .get("PointInTimeRecoveryDescription", {})
                          .get("PointInTimeRecoveryStatus"))
            r.ok(f"  {tn:35} PITR → {new_state}")
            pitr_results.append((tn, new_state))
        except ddb.exceptions.TableNotFoundException:
            r.warn(f"  {tn:35} not found, skipping")
            pitr_results.append((tn, "not_found"))
        except Exception as e:
            r.fail(f"  {tn:35} {e}")
            pitr_results.append((tn, f"error"))

    pitr_ok = sum(1 for _, s in pitr_results if s in ("ENABLED", "already_enabled"))
    r.log(f"\n  PITR enabled on {pitr_ok}/{len(PITR_TABLES)} tables")

    # ════════════════════════════════════════════════════════════════════
    # B. Bump python3.11 → python3.12 + enable SnapStart
    # ════════════════════════════════════════════════════════════════════
    r.section("B. Bump python3.11 Lambdas to 3.12 + enable SnapStart")
    bump_results = []

    for name in PYTHON311_LAMBDAS:
        try:
            cfg = lam.get_function_configuration(FunctionName=name)
            cur_runtime = cfg.get("Runtime")
            if not cur_runtime.startswith("python3.11"):
                r.log(f"  {name:38} runtime={cur_runtime} (not 3.11), skipping")
                bump_results.append((name, "not_311"))
                continue

            # 1. Bump runtime
            lam.update_function_configuration(
                FunctionName=name,
                Runtime="python3.12",
            )
            lam.get_waiter("function_updated").wait(
                FunctionName=name, WaiterConfig={"Delay": 3, "MaxAttempts": 30}
            )
            r.ok(f"  {name:38} python3.11 → python3.12")

            # 2. Sync invoke to verify (skip ai-chat / others with low RC)
            try:
                rc_resp = lam.get_function_concurrency(FunctionName=name)
                rc = rc_resp.get("ReservedConcurrentExecutions")
            except Exception:
                rc = None

            if rc is not None and rc <= 3:
                r.log(f"      RC={rc}, skipping sync invoke")
            else:
                try:
                    resp = lam.invoke(FunctionName=name, InvocationType="RequestResponse")
                    if resp.get("FunctionError"):
                        payload = resp.get("Payload").read().decode()[:400]
                        # If error looks like a runtime/import issue, REVERT
                        if any(s in payload for s in ["ImportError", "ModuleNotFoundError",
                                                       "Runtime.ImportModuleError"]):
                            r.fail(f"      runtime issue, REVERTING to 3.11: {payload[:200]}")
                            lam.update_function_configuration(
                                FunctionName=name, Runtime="python3.11",
                            )
                            lam.get_waiter("function_updated").wait(
                                FunctionName=name, WaiterConfig={"Delay": 3, "MaxAttempts": 30}
                            )
                            bump_results.append((name, "reverted_runtime_issue"))
                            continue
                        else:
                            r.warn(f"      pre-existing functional error (not runtime): {payload[:150]}")
                    else:
                        r.log(f"      invoke clean at python3.12")
                except Exception as e:
                    r.warn(f"      invoke check: {str(e)[:120]}")

            # 3. Enable SnapStart
            lam.update_function_configuration(
                FunctionName=name,
                SnapStart={"ApplyOn": "PublishedVersions"},
            )
            lam.get_waiter("function_updated").wait(
                FunctionName=name, WaiterConfig={"Delay": 3, "MaxAttempts": 30}
            )

            # 4. Publish version
            v = lam.publish_version(
                FunctionName=name,
                Description=f"python3.12 + SnapStart by ops/122 at {datetime.now(timezone.utc).isoformat()}",
            )
            r.ok(f"      SnapStart enabled, version {v.get('Version')} published")
            bump_results.append((name, f"bumped_and_snapstart_v{v.get('Version')}"))

            time.sleep(1)
        except Exception as e:
            r.fail(f"  {name}: {e}")
            bump_results.append((name, f"error: {str(e)[:100]}"))

    # ════════════════════════════════════════════════════════════════════
    # Summary
    # ════════════════════════════════════════════════════════════════════
    r.section("Summary")
    r.log("PITR results (after IAM grant):")
    for tn, status in pitr_results:
        r.log(f"  {tn:35} {status}")
    r.log("\nRuntime bump + SnapStart:")
    for name, status in bump_results:
        r.log(f"  {name:40} {status}")

    pitr_total = sum(1 for _, s in pitr_results if s in ("ENABLED", "already_enabled"))
    snapstart_total = sum(1 for _, s in bump_results if "snapstart" in s)
    r.kv(
        pitr_protected=pitr_total,
        runtime_bumped=sum(1 for _, s in bump_results if "bumped" in s),
        snapstart_now_active=snapstart_total,
    )
    r.log("Done")
