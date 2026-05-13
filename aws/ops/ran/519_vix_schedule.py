#!/usr/bin/env python3
"""519 — Scan all EventBridge rules to find vix/vol/squeeze schedules + ensure scheduling."""
import json, os
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/519_vix_schedule.json"
eb = boto3.client("events", region_name="us-east-1")
lam = boto3.client("lambda", region_name="us-east-1")


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}

    # Scan all rules to find ones touching vix/vol/squeeze
    all_rules = []
    paginator = eb.get_paginator("list_rules")
    for page in paginator.paginate():
        for r in page.get("Rules", []):
            try:
                targets = eb.list_targets_by_rule(Rule=r["Name"])["Targets"]
                target_fns = [t["Arn"].split(":function:")[1] if ":function:" in t.get("Arn", "") else "?"
                              for t in targets]
                all_rules.append({
                    "name": r["Name"],
                    "schedule": r.get("ScheduleExpression"),
                    "state": r.get("State"),
                    "targets": target_fns,
                })
            except: pass

    # Filter to vix/vol related
    relevant = [r for r in all_rules
                 if any(kw in r["name"].lower() or any(kw in (t or "").lower() for t in r.get("targets", []))
                          for kw in ["vix", "vol-regime", "volatility-squeeze"])]
    out["relevant_rules"] = relevant
    out["total_rules_scanned"] = len(all_rules)

    # Ensure each Lambda has a schedule
    plans = []
    targets_to_schedule = [
        ("justhodl-vix-curve", "rate(4 hours)"),
        ("justhodl-vol-regime", "rate(2 hours)"),
        ("justhodl-volatility-squeeze-hunter", "cron(0 13 * * ? *)"),  # daily 9AM ET
    ]
    for name, schedule in targets_to_schedule:
        existing = next((r for r in all_rules if name in (r.get("targets") or [])), None)
        if existing:
            plans.append({"name": name, "action": "already_scheduled",
                           "rule": existing["name"],
                           "schedule": existing["schedule"],
                           "state": existing["state"]})
        else:
            rule_name = f"{name}-auto"
            try:
                eb.put_rule(Name=rule_name, ScheduleExpression=schedule,
                             State="ENABLED",
                             Description=f"Auto-scheduled by ops 519")
                arn = lam.get_function(FunctionName=name)["Configuration"]["FunctionArn"]
                eb.put_targets(Rule=rule_name, Targets=[{"Id": "1", "Arn": arn}])
                try:
                    lam.add_permission(
                        FunctionName=name, StatementId=f"{rule_name}-perm",
                        Action="lambda:InvokeFunction",
                        Principal="events.amazonaws.com",
                        SourceArn=eb.describe_rule(Name=rule_name)["Arn"],
                    )
                except lam.exceptions.ResourceConflictException: pass
                plans.append({"name": name, "action": "scheduled",
                               "rule": rule_name, "schedule": schedule})
            except Exception as e:
                plans.append({"name": name, "action": "err", "err": str(e)[:200]})

    out["schedule_plans"] = plans
    out["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f: json.dump(out, f, indent=2, default=str)


if __name__ == "__main__":
    main()
