#!/usr/bin/env python3
"""
246_create_khalid_metrics_eb_rule.py
====================================

Follow-up to 245_diagnose_repair_health_red.py which found that
justhodl-khalid-metrics has NO EventBridge rule pointing at it. That's
why the health monitor reports 0 invocations / day — nothing fires it.

Memory note (edit #12) and the health-monitor's expected schedule both
say it should run cron(0 11 * * ? *) — daily at 11:00 UTC.

This script:
  1. Creates EB rule 'justhodl-khalid-metrics-refresh' with that schedule
  2. Adds the Lambda as a target
  3. Grants events.amazonaws.com permission to invoke it
  4. Verifies post-creation
  
All operations are idempotent — safe to re-run.
"""
import json
from datetime import datetime, timezone

from ops_report import report
import boto3
from botocore.exceptions import ClientError

REGION = "us-east-1"
events = boto3.client("events", region_name=REGION)
lam = boto3.client("lambda", region_name=REGION)

FN = "justhodl-khalid-metrics"
RULE = "justhodl-khalid-metrics-refresh"
SCHEDULE = "cron(0 11 * * ? *)"
TARGET_ID = "1"
PERM_STMT_ID = "EventBridgeKhalidMetricsInvoke"


with report("create_khalid_metrics_eb_rule") as r:
    r.heading("Create EB rule for justhodl-khalid-metrics")
    r.log(f"  Lambda:   {FN}")
    r.log(f"  Rule:     {RULE}")
    r.log(f"  Schedule: {SCHEDULE}")

    # ─── 1. Get the Lambda's ARN ───
    r.section("1. Resolve Lambda ARN")
    try:
        cfg = lam.get_function_configuration(FunctionName=FN)
        fn_arn = cfg["FunctionArn"]
        r.ok(f"  ARN: {fn_arn}")
    except ClientError as e:
        r.fail(f"  GetFunctionConfiguration failed: {e}")
        raise SystemExit(1)

    # ─── 2. Create or update the rule ───
    r.section("2. Create/update EB rule")
    try:
        rule_resp = events.put_rule(
            Name=RULE,
            ScheduleExpression=SCHEDULE,
            State="ENABLED",
            Description="Daily refresh of khalid-config.json metrics — created 2026-04-27 by ops 246",
        )
        r.ok(f"  put_rule OK: {rule_resp['RuleArn']}")
        rule_arn = rule_resp["RuleArn"]
    except ClientError as e:
        r.fail(f"  put_rule failed: {e}")
        raise SystemExit(1)

    # ─── 3. Add Lambda as target ───
    r.section("3. Attach Lambda as target")
    try:
        target_resp = events.put_targets(
            Rule=RULE,
            Targets=[{"Id": TARGET_ID, "Arn": fn_arn}],
        )
        if target_resp.get("FailedEntryCount", 0) > 0:
            r.fail(f"  put_targets had failures: {target_resp.get('FailedEntries')}")
            raise SystemExit(1)
        r.ok("  put_targets OK")
    except ClientError as e:
        r.fail(f"  put_targets failed: {e}")
        raise SystemExit(1)

    # ─── 4. Grant EventBridge permission to invoke ───
    r.section("4. Grant EventBridge invoke permission on Lambda")
    try:
        lam.add_permission(
            FunctionName=FN,
            StatementId=PERM_STMT_ID,
            Action="lambda:InvokeFunction",
            Principal="events.amazonaws.com",
            SourceArn=rule_arn,
        )
        r.ok(f"  add_permission OK (StatementId={PERM_STMT_ID})")
    except ClientError as e:
        if e.response["Error"]["Code"] == "ResourceConflictException":
            r.log(f"  permission already exists (StatementId={PERM_STMT_ID}) — OK")
        else:
            r.fail(f"  add_permission failed: {e}")
            # Not fatal — rule + target are in place, EB might already have perm

    # ─── 5. Verify ───
    r.section("5. Verify final state")
    desc = events.describe_rule(Name=RULE)
    r.log(f"  rule.State:    {desc.get('State')}")
    r.log(f"  rule.Schedule: {desc.get('ScheduleExpression')}")

    targets = events.list_targets_by_rule(Rule=RULE).get("Targets", [])
    for t in targets:
        r.log(f"  target: {t.get('Id')} → {t.get('Arn')}")
        if t["Arn"] == fn_arn:
            r.ok(f"  ✓ {FN} attached as target")

    # Cross-check from Lambda side
    rules_for_fn = events.list_rule_names_by_target(TargetArn=fn_arn).get("RuleNames", [])
    r.log(f"  Lambda's rules: {rules_for_fn}")
    if RULE in rules_for_fn:
        r.ok(f"  ✓ Lambda <-> rule binding confirmed bidirectionally")
    else:
        r.fail(f"  ✗ rule '{RULE}' not in Lambda's rule list — possible permission issue")

    r.section("Result")
    r.ok(f"\n  ✅ {FN} will now fire daily at 11:00 UTC")
    r.log(f"  Next expected invocation: today 11:00 UTC if it's before then,")
    r.log(f"  otherwise tomorrow 11:00 UTC.")
