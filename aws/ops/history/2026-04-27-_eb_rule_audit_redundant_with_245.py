"""
Step ___ — Audit and repair EventBridge rules for 0-invocation Lambdas.

The 2026-04-27 health monitor flagged these Lambdas as "0 invocations / 24h"
despite having scheduled EventBridge rules in the (possibly-stale) config.json:

  - justhodl-email-reports-v2     (cron 0 12 * * ? *,    ~1/day)
  - justhodl-khalid-metrics       (cron 0 11 * * ? *,    ~1/day)
  - justhodl-data-collector       (rate(1 hour),         ~24/day)

Possible root causes (which this script identifies and fixes where safe):

  1. Rule exists but is in DISABLED state               → re-enable
  2. Rule has no targets                                → log, manual fix
  3. Rule has a target but the target's permissions
     don't allow EB to invoke (Lambda resource policy
     missing the events.amazonaws.com Principal)       → add permission
  4. Rule was deleted entirely                          → log, propose recreation

This script ONLY makes safe, reversible changes (enable disabled rules,
add missing invoke permissions). Anything destructive (re-creating deleted
rules) is logged for manual follow-up.

Idempotent. Safe to re-run.
"""
from __future__ import annotations
import json
import time
from datetime import datetime, timezone, timedelta

from ops_report import report
import boto3
from botocore.exceptions import ClientError

REGION = "us-east-1"
ACCOUNT = "857687956942"

eb = boto3.client("events", region_name=REGION)
lam = boto3.client("lambda", region_name=REGION)
cw = boto3.client("cloudwatch", region_name=REGION)

# Lambdas the dashboard reported as 0-invocation, plus a couple we recently
# stubbed/repointed so we can verify their EB rules survived.
TARGETS = [
    "justhodl-email-reports-v2",
    "justhodl-khalid-metrics",
    "justhodl-data-collector",
    "scrapeMacroData",            # post-stub verification
    "fmp-stock-picks-agent",      # had 0/24h + 100% errors over 7d
    "news-sentiment-agent",       # post-rename verification
    "justhodl-intelligence",      # under-rate verification (now 4 threshold)
    "justhodl-repo-monitor",      # under-rate verification (now 6 threshold)
]


def find_rules_targeting(lambda_name: str) -> list[dict]:
    """Return list of EB rules whose targets include this Lambda's ARN."""
    arn = f"arn:aws:lambda:{REGION}:{ACCOUNT}:function:{lambda_name}"
    paginator = eb.get_paginator("list_rule_names_by_target")
    rules = []
    try:
        for page in paginator.paginate(TargetArn=arn):
            for name in page.get("RuleNames", []):
                desc = eb.describe_rule(Name=name)
                rules.append(desc)
    except ClientError as e:
        # list_rule_names_by_target may fail if Lambda doesn't exist
        return []
    return rules


def get_invocations_24h(lambda_name: str) -> tuple[int, int]:
    """Return (invocations, errors) over the last 24h via CloudWatch."""
    end = datetime.now(timezone.utc)
    start = end - timedelta(hours=24)
    inv = err = 0
    for metric, key in (("Invocations", "inv"), ("Errors", "err")):
        try:
            r = cw.get_metric_statistics(
                Namespace="AWS/Lambda",
                MetricName=metric,
                Dimensions=[{"Name": "FunctionName", "Value": lambda_name}],
                StartTime=start,
                EndTime=end,
                Period=86400,
                Statistics=["Sum"],
            )
            dp = r.get("Datapoints") or [{}]
            val = int(dp[0].get("Sum", 0))
            if key == "inv":
                inv = val
            else:
                err = val
        except ClientError:
            pass
    return inv, err


def has_invoke_permission(lambda_name: str, rule_arn: str) -> bool:
    """Check if Lambda's resource policy allows the rule to invoke."""
    try:
        policy = lam.get_policy(FunctionName=lambda_name)
    except ClientError as e:
        if e.response["Error"]["Code"] == "ResourceNotFoundException":
            return False
        raise
    p = json.loads(policy["Policy"])
    for stmt in p.get("Statement", []):
        if stmt.get("Principal", {}).get("Service") == "events.amazonaws.com":
            arn = stmt.get("Condition", {}).get("ArnLike", {}).get("AWS:SourceArn")
            if arn in (rule_arn, "*"):
                return True
            # Sometimes recorded as ArnEquals or no condition at all
            if not arn and stmt.get("Effect") == "Allow":
                return True
    return False


def add_invoke_permission(lambda_name: str, rule_name: str, rule_arn: str) -> str:
    """Add lambda:InvokeFunction permission for an EB rule. Returns SID used."""
    sid = f"AllowEB-{rule_name[:32]}-{int(time.time())}"[:64]
    lam.add_permission(
        FunctionName=lambda_name,
        StatementId=sid,
        Action="lambda:InvokeFunction",
        Principal="events.amazonaws.com",
        SourceArn=rule_arn,
    )
    return sid


def main():
    with report("eb_rule_audit_for_zero_invocation_lambdas") as r:
        r.heading("EB rule audit — 0-invocation Lambdas")
        r.log(f"Targets: {', '.join(TARGETS)}")
        r.log("")

        summary = []
        for fn in TARGETS:
            r.section(fn)

            # Lambda exists?
            try:
                lam.get_function_configuration(FunctionName=fn)
            except ClientError as e:
                if e.response["Error"]["Code"] == "ResourceNotFoundException":
                    r.warn(f"  Lambda does not exist in AWS — skipping")
                    summary.append({"fn": fn, "status": "missing"})
                    continue
                raise

            # Invocation reality
            inv, err = get_invocations_24h(fn)
            r.log(f"  CloudWatch 24h: invocations={inv}, errors={err}")

            # Rules targeting it
            rules = find_rules_targeting(fn)
            if not rules:
                r.warn(f"  No EB rules target this Lambda")
                summary.append({"fn": fn, "status": "no_rules", "inv_24h": inv, "err_24h": err})
                continue

            for rule in rules:
                rname = rule["Name"]
                rarn = rule["Arn"]
                state = rule.get("State", "?")
                schedule = rule.get("ScheduleExpression", rule.get("EventPattern", "?"))
                r.log(f"  Rule '{rname}' state={state} schedule={schedule}")

                actions_taken = []

                # Re-enable disabled rules
                if state == "DISABLED":
                    try:
                        eb.enable_rule(Name=rname)
                        actions_taken.append("enabled")
                        r.ok(f"    ✓ enabled disabled rule")
                    except ClientError as e:
                        r.fail(f"    ✗ enable_rule failed: {e}")

                # Verify Lambda has the invoke permission
                if not has_invoke_permission(fn, rarn):
                    try:
                        sid = add_invoke_permission(fn, rname, rarn)
                        actions_taken.append(f"added-permission:{sid}")
                        r.ok(f"    ✓ added lambda:InvokeFunction permission ({sid})")
                    except ClientError as e:
                        if e.response["Error"]["Code"] == "ResourceConflictException":
                            actions_taken.append("permission-already-exists")
                            r.log(f"    permission already exists (different SID)")
                        else:
                            r.fail(f"    ✗ add_permission failed: {e}")

                # Verify rule has the right target
                targets = eb.list_targets_by_rule(Rule=rname).get("Targets", [])
                target_arns = [t.get("Arn", "") for t in targets]
                expected = f"arn:aws:lambda:{REGION}:{ACCOUNT}:function:{fn}"
                if expected not in target_arns:
                    r.warn(f"    ⚠ rule has targets {target_arns} — missing {fn}")

                summary.append({
                    "fn": fn,
                    "rule": rname,
                    "state_before": state,
                    "actions": actions_taken,
                    "inv_24h": inv,
                    "err_24h": err,
                })

        r.section("Summary")
        for s in summary:
            r.kv(**s)

        r.log("")
        r.log("Re-run health-monitor invocation in 15+ min to see updated CloudWatch stats.")


if __name__ == "__main__":
    main()
