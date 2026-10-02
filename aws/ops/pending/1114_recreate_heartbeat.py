"""Ops 1114: Recreate missing feed-heartbeat schedule."""
from __future__ import annotations
import json, os, sys
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "shared"))
import boto3

sched = boto3.client("scheduler", region_name="us-east-1")

def main():
    # Get Lambda ARN
    lam = boto3.client("lambda", region_name="us-east-1")
    fn = lam.get_function(FunctionName="justhodl-feed-heartbeat")
    arn = fn["Configuration"]["FunctionArn"]
    # Get role ARN from existing ticker-360 schedule
    t360 = sched.get_schedule(Name="justhodl-ticker-360-schedule")
    role = t360["Target"]["RoleArn"]
    # Create heartbeat schedule (every 15 minutes)
    sched.create_schedule(
        Name="justhodl-feed-heartbeat-schedule",
        ScheduleExpression="rate(15 minutes)",
        FlexibleTimeWindow={"Mode": "OFF"},
        Target={
            "Arn": arn,
            "RoleArn": role,
            "Input": json.dumps({"ops": "1114", "reason": "recreate vanished schedule"}),
        },
        State="ENABLED",
        Description="Feed heartbeat every 15 min (recreated ops 1114)",
    )
    print(json.dumps({"created": True, "schedule": "justhodl-feed-heartbeat-schedule"}))

if __name__ == "__main__":
    main()
