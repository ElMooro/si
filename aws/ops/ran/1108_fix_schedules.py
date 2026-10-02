"""Ops 1108: recreate missing ticker-360 schedule and check sec-8k."""
from __future__ import annotations
import json, os, sys
from datetime import datetime, timezone
OUT = os.path.join("aws", "ops", "reports", "1108_fix_schedules.json")
REGION = "us-east-1"
ACCOUNT = "857687956942"
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "shared"))
import boto3
from botocore.exceptions import ClientError
sched = boto3.client("scheduler", region_name=REGION)
lam = boto3.client("lambda", region_name=REGION)

def get_role():
    try:
        paginator = sched.get_paginator("list_schedules")
        for page in paginator.paginate():
            for s in page.get("Schedules", []):
                if "justhodl" in s.get("Name", "").lower():
                    try:
                        det = sched.get_schedule(Name=s["Name"])
                        role = det.get("Target", {}).get("RoleArn")
                        if role:
                            return role
                    except Exception:
                        continue
    except Exception:
        pass
    return None

def main():
    rep = {"read_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
           "actions": []}
    role = get_role()
    if not role:
        rep["error"] = "No IAM role found"
    else:
        # Recreate ticker-360 schedule
        try:
            sched.create_schedule(
                Name="justhodl-ticker-360-schedule",
                ScheduleExpression="cron(20 6,18 * * ? *)",
                Target={
                    "Arn": f"arn:aws:lambda:{REGION}:{ACCOUNT}:function:justhodl-ticker-360",
                    "RoleArn": role},
                FlexibleTimeWindow={"Mode": "OFF"},
                State="ENABLED",
                Description="Ticker-360 twice daily")
            rep["actions"].append({"schedule": "justhodl-ticker-360-schedule",
                                   "status": "recreated"})
        except ClientError as e:
            if "already exists" in str(e).lower() or "ConflictException" in str(e):
                # Try to enable it
                try:
                    det = sched.get_schedule(Name="justhodl-ticker-360-schedule")
                    sched.update_schedule(
                        Name="justhodl-ticker-360-schedule",
                        ScheduleExpression=det["ScheduleExpression"],
                        Target=det["Target"],
                        FlexibleTimeWindow={"Mode": "OFF"},
                        State="ENABLED")
                    rep["actions"].append({"schedule": "justhodl-ticker-360-schedule",
                                           "status": "enabled"})
                except Exception as e2:
                    rep["actions"].append({"schedule": "justhodl-ticker-360-schedule",
                                           "status": "failed", "error": str(e2)[:80]})
            else:
                rep["actions"].append({"schedule": "justhodl-ticker-360-schedule",
                                       "status": "failed", "error": str(e)[:80]})
        except Exception as e:
            rep["actions"].append({"schedule": "justhodl-ticker-360-schedule",
                                   "status": "failed", "error": str(e)[:80]})
    # Trigger sec-8k manually to test
    try:
        r = lam.invoke(FunctionName="justhodl-sec-8k-enrich",
                       InvocationType="Event",
                       Payload=json.dumps({"source": "ops-1108", "trigger": "stale-fix"}))
        rep["sec8k_triggered"] = r["StatusCode"] == 202
    except Exception as e:
        rep["sec8k_triggered"] = False
        rep["sec8k_error"] = str(e)[:80]
    os.makedirs(os.path.dirname(OUT), exist_ok=True)
    open(OUT,"w").write(json.dumps(rep,indent=2))
    print(json.dumps(rep, indent=2)[:2000])

if __name__=="__main__":
    main()
