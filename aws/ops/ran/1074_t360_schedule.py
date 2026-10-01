"""
Ops 1074: create ticker-360 schedule (controlled write).

Creates/enables the EventBridge rule for justhodl-ticker-360 per its
config.json schedule (cron(20 6,18 * * ? *) — twice daily), bound to the
lambda. Idempotent: skips if the rule already exists and is enabled.

Writes: aws/ops/reports/1074_t360_schedule.json. Never raises.
"""
from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timezone

OUT_PATH = os.path.join("aws", "ops", "reports", "1074_t360_schedule.json")
REGION = "us-east-1"
ACCT = "857687956942"
FN = "justhodl-ticker-360"
RULE = "justhodl-ticker-360-schedule"
SCHEDULE = "cron(20 6,18 * * ? *)"

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                "..", "..", "shared"))
import boto3  # noqa: E402
from botocore.exceptions import ClientError  # noqa: E402

ev = boto3.client("events", region_name=REGION)
lam = boto3.client("lambda", region_name=REGION)


def main():
    """Create/enable the schedule rule. Write the report."""
    rep = {"script": "1074_t360_schedule",
           "read_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
           "rule": RULE, "schedule": SCHEDULE, "actions": []}
    try:
        try:
            cur = ev.describe_rule(Name=RULE)
            rep["pre_existing"] = {"state": cur.get("State"),
                                   "expression": cur.get("ScheduleExpression")}
        except ClientError as e:
            if e.response.get("Error", {}).get("Code") == "ResourceNotFoundException":
                rep["pre_existing"] = None
            else:
                raise
        arn = "arn:aws:lambda:%s:%s:function:%s" % (REGION, ACCT, FN)
        ev.put_rule(Name=RULE, ScheduleExpression=SCHEDULE,
                    State="ENABLED",
                    Description="ticker-360 cross-engine enrichment, twice daily")
        rep["actions"].append("put_rule")
        ev.put_targets(Rule=RULE, Targets=[{"Id": "t360",
                                            "Arn": arn}])
        rep["actions"].append("put_targets")
        try:
            lam.add_permission(FunctionName=FN, StatementId="ev-t360-sched",
                               Action="lambda:InvokeFunction",
                               Principal="events.amazonaws.com",
                               SourceArn="arn:aws:events:%s:%s:rule/%s" % (REGION, ACCT, RULE))
            rep["actions"].append("add_permission")
        except ClientError as e:
            if e.response.get("Error", {}).get("Code") != "ResourceConflictException":
                raise
            rep["actions"].append("permission_already_exists")
        cur = ev.describe_rule(Name=RULE)
        rep["final"] = {"state": cur.get("State"),
                        "expression": cur.get("ScheduleExpression"),
                        "ok": cur.get("State") == "ENABLED"}
    except Exception as e:  # noqa: BLE001
        rep["error"] = str(e)[:200]
        rep["final"] = {"ok": False}
    os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)
    with open(OUT_PATH, "w") as f:
        json.dump(rep, f, indent=2)
    print(json.dumps(rep, indent=2)[:2000])


if __name__ == "__main__":
    main()
