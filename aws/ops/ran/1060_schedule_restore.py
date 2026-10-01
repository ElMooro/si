"""
Ops 1060: restore the missing EventBridge trigger for justhodl-etf-issuer-holdings.

Root cause (deploy run 36855464673, commit 8fefa5c):
  config.json declares schedule {name: justhodl-etf-issuer-holdings-daily,
  expression: cron(0 4 * * ? *)} (classic rule). The deploy guard
  (scripts/check_existing_schedule.py) calls events.describe_rule for that
  name and gets ResourceNotFoundException -> configured_binding_missing_
  use_original_reference -> deploy FAILS, so the lambda still runs stale code.

This script performs the reviewed schedule operation the guard demands:
  1. put_rule for justhodl-etf-issuer-holdings-daily, cron(0 4 * * ? *),
     preserving existing State/EventPattern/RoleArn if the rule reappears.
  2. add_permission for events.amazonaws.com -> lambda (idempotent).
  3. put_targets binding the rule to the lambda (preserves other targets).
  4. verify via describe_rule + list_targets_by_rule.

Idempotent: safe to re-run. Never raises; writes a capped report.
Writes: aws/ops/reports/1060_schedule_restore.json (committed back by run-ops).
"""
from __future__ import annotations

import json
import os
import time

import boto3
from botocore.exceptions import ClientError

OUT_PATH = os.path.join("aws", "ops", "reports", "1060_schedule_restore.json")
REGION = "us-east-1"
ACCOUNT = "857687956942"
FN = "justhodl-etf-issuer-holdings"
RULE = "justhodl-etf-issuer-holdings-daily"
CRON = "cron(0 4 * * ? *)"
DESC = "23:00 ET daily — issuer holdings after market close"
CAP = 1200


def cap(obj):
    """Truncate long strings. Never raises."""
    try:
        if isinstance(obj, str):
            return obj[:CAP] + ("..." if len(obj) > CAP else "")
        return obj
    except Exception:
        return "<unserializable>"


def main():
    """Create the rule, permission, and target; verify; write the report."""
    events = boto3.client("events", region_name=REGION)
    lam = boto3.client("lambda", region_name=REGION)
    fn_arn = f"arn:aws:lambda:{REGION}:{ACCOUNT}:function:{FN}"
    rule_arn = f"arn:aws:events:{REGION}:{ACCOUNT}:rule/{RULE}"
    steps = {}
    ok = True

    # 1. Rule (create or confirm).
    try:
        existing = events.describe_rule(Name=RULE)
        steps["rule"] = {"action": "already_exists",
                         "state": existing.get("State"),
                         "expression": existing.get("ScheduleExpression")}
        if existing.get("ScheduleExpression") != CRON:
            steps["rule"]["expression_mismatch"] = True
            ok = False
    except ClientError as e:
        if e.response.get("Error", {}).get("Code") == "ResourceNotFoundException":
            try:
                events.put_rule(Name=RULE, ScheduleExpression=CRON,
                                State="ENABLED", Description=DESC)
                steps["rule"] = {"action": "created", "expression": CRON}
            except ClientError as e2:
                steps["rule"] = {"action": "create_failed",
                                 "error": e2.response.get("Error", {}).get("Code")}
                ok = False
        else:
            steps["rule"] = {"action": "describe_failed",
                             "error": e.response.get("Error", {}).get("Code")}
            ok = False

    # 2. Lambda invoke permission for the rule (idempotent).
    if ok:
        try:
            lam.add_permission(FunctionName=FN, StatementId=f"EventBridge-{RULE}",
                               Action="lambda:InvokeFunction",
                               Principal="events.amazonaws.com",
                               SourceArn=rule_arn)
            steps["permission"] = {"action": "added"}
        except ClientError as e:
            code = e.response.get("Error", {}).get("Code")
            steps["permission"] = {"action": "already_exists" if code == "ResourceConflictException"
                                   else "add_failed", "code": code}
            if code != "ResourceConflictException":
                ok = False

    # 3. Target binding (preserve unrelated targets).
    if ok:
        try:
            current = events.list_targets_by_rule(Rule=RULE).get("Targets", [])
            bound = [t for t in current if t.get("Arn") in
                     (fn_arn, fn_arn + ":$LATEST", fn_arn + ":live")]
            if bound:
                steps["target"] = {"action": "already_bound",
                                   "target_arn": bound[0].get("Arn")}
            else:
                targets = list(current) + [{"Id": f"audit-{FN}", "Arn": fn_arn}]
                resp = events.put_targets(Rule=RULE, Targets=targets)
                failed = resp.get("FailedEntryCount", 0)
                steps["target"] = {"action": "bound" if not failed else "put_failed",
                                   "failed_entries": failed}
                ok = not failed
        except ClientError as e:
            steps["target"] = {"action": "failed",
                               "error": e.response.get("Error", {}).get("Code")}
            ok = False

    # 4. Verify.
    try:
        rule = events.describe_rule(Name=RULE)
        targets = events.list_targets_by_rule(Rule=RULE).get("Targets", [])
        steps["verify"] = {
            "rule_state": rule.get("State"),
            "expression": rule.get("ScheduleExpression"),
            "targets": [{"id": t.get("Id"), "arn": cap(t.get("Arn"))} for t in targets],
            "function_bound": any(t.get("Arn") == fn_arn for t in targets),
        }
        ok = ok and steps["verify"]["function_bound"] \
            and rule.get("ScheduleExpression") == CRON
    except ClientError as e:
        steps["verify"] = {"error": e.response.get("Error", {}).get("Code")}
        ok = False

    report = {"script": "1060_schedule_restore",
              "read_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
              "function": FN, "rule": RULE, "expression": CRON,
              "overall": "PASS" if ok else "FAIL", "steps": steps}
    os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)
    with open(OUT_PATH, "w") as f:
        json.dump(report, f, indent=2)
    print(json.dumps(report, indent=2)[:8000])


if __name__ == "__main__":
    main()
