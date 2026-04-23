#!/usr/bin/env python3
"""
Phase 2b final cleanup — 3 targeted actions.

1. Delete two EventBridge rules that warm enhanced-openbb-handler:
     - lambda-warmer-system3
     - lambda-warmer-system3-frequent
   (Remove targets first, then remove Lambda permission, then delete rule.)

2. Delete enhanced-openbb-handler Lambda itself + CloudWatch log group.
   (No callers, no API GW, no Function URL — 30k/mo invocations are
   all from the warmers we're deleting.)

3. Delete duplicate EventBridge rule DailyEmailReportsV2_8AMET.
   Keep DailyEmailReportsV2 (same cron, simpler name).
   This stops Khalid from getting the same daily email twice.

Every deletion is wrapped in try/except so one failure doesn't abort
the others.
"""

import os
from datetime import datetime, timezone

from ops_report import report
import boto3
from botocore.exceptions import ClientError

REGION = "us-east-1"

lam  = boto3.client("lambda", region_name=REGION)
ev   = boto3.client("events", region_name=REGION)
logs = boto3.client("logs", region_name=REGION)


def delete_eb_rule(r, rule_name: str, target_function: str = None):
    """Remove targets, revoke invoke permission, delete rule. Return True on success."""
    # Step 1: list + remove targets
    try:
        targets = ev.list_targets_by_rule(Rule=rule_name).get("Targets", [])
        if targets:
            ids = [t["Id"] for t in targets]
            ev.remove_targets(Rule=rule_name, Ids=ids)
            r.log(f"    removed {len(ids)} target(s) from rule")
    except ClientError as e:
        r.warn(f"    remove_targets failed: {e}")

    # Step 2: revoke Lambda permission (so the rule can no longer invoke the function)
    if target_function:
        try:
            policy_raw = lam.get_policy(FunctionName=target_function).get("Policy", "{}")
            import json as _json
            policy = _json.loads(policy_raw)
            for stmt in policy.get("Statement", []):
                sid = stmt.get("Sid", "")
                # Any statement whose Condition references this rule by ARN
                cond_arn = stmt.get("Condition", {}).get("ArnLike", {}).get("AWS:SourceArn", "")
                if rule_name in cond_arn or rule_name in sid:
                    try:
                        lam.remove_permission(FunctionName=target_function, StatementId=sid)
                        r.log(f"    removed Lambda permission statement '{sid}'")
                    except ClientError as e:
                        r.warn(f"    remove_permission '{sid}' failed: {e}")
        except lam.exceptions.ResourceNotFoundException:
            pass  # function might already be gone
        except ClientError as e:
            r.warn(f"    get_policy failed (rule {rule_name}): {e}")

    # Step 3: delete rule
    try:
        ev.delete_rule(Name=rule_name)
        return True
    except ClientError as e:
        r.fail(f"    delete_rule({rule_name}) failed: {e}")
        return False


def delete_lambda(r, fn_name: str) -> bool:
    try:
        lam.delete_function(FunctionName=fn_name)
        r.ok(f"  Lambda {fn_name} deleted")
        ok = True
    except lam.exceptions.ResourceNotFoundException:
        r.warn(f"  Lambda {fn_name} not found (already deleted?)")
        ok = False
    except ClientError as e:
        r.fail(f"  Lambda {fn_name} delete failed: {e}")
        return False

    try:
        logs.delete_log_group(logGroupName=f"/aws/lambda/{fn_name}")
        r.log(f"    Log group /aws/lambda/{fn_name} deleted")
    except logs.exceptions.ResourceNotFoundException:
        r.log(f"    Log group /aws/lambda/{fn_name} did not exist")
    except ClientError as e:
        r.warn(f"    Log group delete warning: {e}")

    return ok


with report("phase_2b_final_cleanup") as r:
    r.heading("Phase 2b final cleanup — warmers + handler + dup email rule")

    # ─────────────────────────────────────────────────
    # Action 1: Delete the two warmer rules
    # ─────────────────────────────────────────────────
    r.section("Action 1: Delete enhanced-openbb-handler warmer rules")
    for rule_name in ["lambda-warmer-system3", "lambda-warmer-system3-frequent"]:
        r.log(f"  Rule: {rule_name}")
        ok = delete_eb_rule(r, rule_name, target_function="enhanced-openbb-handler")
        if ok:
            r.ok(f"    Rule {rule_name} deleted")
            r.kv(action="delete-rule", target=rule_name, status="deleted")
        else:
            r.kv(action="delete-rule", target=rule_name, status="failed")

    # ─────────────────────────────────────────────────
    # Action 2: Delete enhanced-openbb-handler itself
    # ─────────────────────────────────────────────────
    r.section("Action 2: Delete enhanced-openbb-handler Lambda")
    ok = delete_lambda(r, "enhanced-openbb-handler")
    r.kv(action="delete-lambda", target="enhanced-openbb-handler",
         status="deleted" if ok else "failed")

    # ─────────────────────────────────────────────────
    # Action 3: Delete the duplicate email rule
    # ─────────────────────────────────────────────────
    r.section("Action 3: Delete duplicate email rule (DailyEmailReportsV2_8AMET)")
    r.log("  Keeping: DailyEmailReportsV2  (same cron — fires once, not twice)")
    ok = delete_eb_rule(r, "DailyEmailReportsV2_8AMET", target_function="justhodl-email-reports-v2")
    if ok:
        r.ok("    Rule DailyEmailReportsV2_8AMET deleted — daily email now fires once")
        r.kv(action="delete-rule", target="DailyEmailReportsV2_8AMET", status="deleted")
    else:
        r.kv(action="delete-rule", target="DailyEmailReportsV2_8AMET", status="failed")

    # ─────────────────────────────────────────────────
    # Verification: re-check that kept items are still alive
    # ─────────────────────────────────────────────────
    r.section("Verification")
    try:
        ev.describe_rule(Name="DailyEmailReportsV2")
        r.ok("  DailyEmailReportsV2 still exists (the one email rule we want to keep)")
    except ClientError as e:
        r.fail(f"  DailyEmailReportsV2 NOT found: {e}")

    try:
        cfg = lam.get_function_configuration(FunctionName="justhodl-email-reports-v2")
        r.ok(f"  justhodl-email-reports-v2 still exists (LastModified: {cfg['LastModified']})")
    except ClientError as e:
        r.fail(f"  justhodl-email-reports-v2 NOT found: {e}")

    # Total Lambda count after cleanup
    total = 0
    paginator = lam.get_paginator("list_functions")
    for page in paginator.paginate():
        total += len(page.get("Functions", []))
    r.log("")
    r.log(f"  Total Lambdas remaining in account: {total}")
    r.kv(metric="total-lambdas", value=total)

    r.log("Done")
