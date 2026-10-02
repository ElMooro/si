"""Ops 1117: Directly check if heartbeat schedule exists now."""
from __future__ import annotations
import json, os, sys
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "shared"))
import boto3
from botocore.exceptions import ClientError
sched = boto3.client("scheduler", region_name="us-east-1")
def main():
    try:
        r = sched.get_schedule(Name="justhodl-feed-heartbeat-schedule")
        print(json.dumps({"exists": True, "state": r.get("State"), "expr": r.get("ScheduleExpression")}))
    except ClientError as e:
        print(json.dumps({"exists": False, "error": type(e).__name__}))
if __name__ == "__main__":
    main()
