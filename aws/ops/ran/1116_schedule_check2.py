"""Ops 1116: Check EventBridge schedules for heartbeat and ticker-360."""
from __future__ import annotations
import json, os, sys
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "shared"))
import boto3
from botocore.exceptions import ClientError

sched = boto3.client("scheduler", region_name="us-east-1")
evb = boto3.client("events", region_name="us-east-1")

def main():
    out = {"schedules": {}, "eventbridge_rules": {}}
    # Check EventBridge Scheduler
    for name in ["justhodl-feed-heartbeat-schedule", "justhodl-ticker-360-schedule"]:
        try:
            r = sched.get_schedule(Name=name)
            out["schedules"][name] = {
                "exists": True,
                "state": r.get("State"),
                "expression": r.get("ScheduleExpression"),
            }
        except ClientError as e:
            out["schedules"][name] = {"exists": False, "error": str(e)[:80]}
    # Check EventBridge Rules (legacy)
    for prefix in ["justhodl-feed-heartbeat", "justhodl-ticker-360"]:
        try:
            r = evb.list_rules(NamePrefix=prefix)
            rules = r.get("Rules", [])
            out["eventbridge_rules"][prefix] = [
                {"name": x["Name"], "state": x["State"], "expr": x.get("ScheduleExpression")}
                for x in rules
            ]
        except ClientError as e:
            out["eventbridge_rules"][prefix] = {"error": str(e)[:80]}
    print(json.dumps(out, indent=2))
    # Write report
    os.makedirs("aws/ops/reports", exist_ok=True)
    with open("aws/ops/reports/1116_schedule_check.json", "w") as f:
        json.dump(out, f, indent=2)

if __name__ == "__main__":
    main()
