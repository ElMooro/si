"""
Ops 1063: provision the missing EventBridge triggers for upgrade lambdas.

Root cause (from 1054/1061): the upgrade lambdas were deployed but their
configs use cadence-only strings or null schedules, so the deploy pipeline
never created any trigger (CONFIG_CADENCE_ONLY / PRESERVE_EXISTING).
Result: zero invocations, missing S3 artifacts.

This script idempotently creates classic EventBridge rules (the 1060 pattern)
for each function that has NO existing binding. If a rule or Scheduler
schedule already targets the function, it is left untouched and reported.

Writes: aws/ops/reports/1063_schedules.json (committed back by run-ops).
Reviewed operation: creates EventBridge rules, Lambda permissions, targets.
Never raises.
"""
from __future__ import annotations

import json
import os
import time

import boto3
from botocore.exceptions import ClientError

OUT_PATH = os.path.join("aws", "ops", "reports", "1063_schedules.json")
ACCOUNT = "857687956942"
REGION = "us-east-1"

# (rule_name, cron_expression, function_name, description)
SCHEDULES = [
    ("justhodl-sec-8k-enrich-schedule", "cron(10,40 * * * ? *)",
     "justhodl-sec-8k-enrich", "8-K enrichment twice hourly (2/10)"),
    ("justhodl-xbrl-fundamentals-schedule", "cron(0 6 ? * SUN *)",
     "justhodl-xbrl-fundamentals", "XBRL fundamentals weekly Sunday (4/10)"),
    ("justhodl-cboe-options-chain-schedule", "cron(5,35 * * * ? *)",
     "justhodl-cboe-options-chain", "CBOE options chain twice hourly (5/10)"),
    ("justhodl-corporate-actions-schedule", "cron(30 6 ? * * *)",
     "justhodl-corporate-actions", "Corporate actions daily (7/10)"),
    ("justhodl-price-redundancy-15min", "cron(0/15 * * * ? *)",
     "justhodl-price-redundancy", "Price redundancy every 15 min (8/10)"),
    ("justhodl-microcap-float-squeeze-daily", "cron(0 22 ? * MON-FRI *)",
     "justhodl-microcap-float-squeeze",
     "Microcap squeeze screener weekdays after close (3/10)"),
    ("justhodl-transcript-query-weekly", "cron(15 7 ? * MON *)",
     "justhodl-transcript-query", "Transcript query weekly Monday (R2)"),
]


def fn_arn(fn):
    """Build the lambda ARN. Pure."""
    return f"arn:aws:lambda:{REGION}:{ACCOUNT}:function:{fn}"


def existing_binding(events, scheduler, fn):
    """Return a description of any existing trigger for fn, else None.

    Checks classic rules (prefix scan) and Scheduler schedules. Never raises.
    """
    arn = fn_arn(fn)
    try:
        rules = events.list_rules(NamePrefix="justhodl-").get("Rules", [])
        for r in rules:
            try:
                tgts = events.list_targets_by_rule(Rule=r["Name"]).get("Targets", [])
            except ClientError:
                continue
            if any(t.get("Arn") == arn for t in tgts):
                return f"classic rule {r['Name']} ({r.get('ScheduleExpression')})"
    except ClientError:
        pass
    try:
        token = None
        while True:
            kw = {"MaxResults": 100}
            if token:
                kw["NextToken"] = token
            page = scheduler.list_schedules(**kw)
            for s in page.get("Schedules", []):
                try:
                    desc = scheduler.get_schedule(Name=s["Name"])
                except ClientError:
                    continue
                if (desc.get("Target") or {}).get("Arn") == arn:
                    return f"scheduler {s['Name']} ({desc.get('ScheduleExpression')})"
            token = page.get("NextToken")
            if not token:
                break
    except ClientError:
        pass
    return None


def ensure_rule(events, lam, rule, expr, fn, desc):
    """Create/verify one rule + permission + target. Returns step dict. Never raises."""
    steps = {}
    arn = fn_arn(fn)
    try:
        try:
            events.describe_rule(Name=rule)
            steps["rule"] = {"action": "exists"}
        except ClientError as e:
            if e.response.get("Error", {}).get("Code") == "ResourceNotFoundException":
                events.put_rule(Name=rule, ScheduleExpression=expr,
                                State="ENABLED", Description=desc[:500])
                steps["rule"] = {"action": "created"}
            else:
                raise
        try:
            lam.add_permission(FunctionName=fn, StatementId=f"{rule}-invoke",
                               Action="lambda:InvokeFunction",
                               Principal="events.amazonaws.com",
                               SourceArn=f"arn:aws:events:{REGION}:{ACCOUNT}:rule/{rule}")
            steps["permission"] = {"action": "added"}
        except ClientError as e:
            if e.response.get("Error", {}).get("Code") == "ResourceConflictException":
                steps["permission"] = {"action": "exists"}
            else:
                raise
        try:
            current = events.list_targets_by_rule(Rule=rule).get("Targets", [])
        except ClientError:
            current = []
        kept = [t for t in current if t.get("Arn") != arn]
        events.put_targets(Rule=rule, Targets=kept + [{"Id": f"audit-{fn}", "Arn": arn}])
        steps["target"] = {"action": "bound",
                           "preserved_unrelated": len(kept)}
        verify = events.describe_rule(Name=rule)
        targets = events.list_targets_by_rule(Rule=rule).get("Targets", [])
        steps["verify"] = {
            "rule_state": verify.get("State"),
            "expression": verify.get("ScheduleExpression"),
            "function_bound": any(t.get("Arn") == arn for t in targets),
        }
        steps["ok"] = bool(steps["verify"]["function_bound"]
                           and steps["verify"]["rule_state"] == "ENABLED"
                           and steps["verify"]["expression"] == expr)
    except ClientError as e:
        steps["ok"] = False
        steps["error"] = e.response.get("Error", {}).get("Code", type(e).__name__)
    except Exception as e:  # defensive; never raise out of ops
        steps["ok"] = False
        steps["error"] = type(e).__name__
    return steps


def main():
    """Provision missing schedules, write the report, print it."""
    events = boto3.client("events", region_name=REGION)
    lam = boto3.client("lambda", region_name=REGION)
    scheduler = boto3.client("scheduler", region_name=REGION)
    results = {}
    for rule, expr, fn, desc in SCHEDULES:
        bound = existing_binding(events, scheduler, fn)
        if bound:
            results[fn] = {"skipped": True, "existing_binding": bound}
            continue
        results[fn] = ensure_rule(events, lam, rule, expr, fn, desc)
    report = {"script": "1063_schedules",
              "read_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
              "results": results,
              "overall": "PASS" if all(
                  r.get("ok") or r.get("skipped") for r in results.values()
              ) else "FAIL"}
    os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)
    with open(OUT_PATH, "w") as f:
        json.dump(report, f, indent=2)
    print(json.dumps(report, indent=2)[:12000])


if __name__ == "__main__":
    main()
