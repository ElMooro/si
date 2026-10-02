"""Ops 1119: Check schedules for stale feeds."""
from __future__ import annotations
import json, os, sys
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "shared"))
import boto3
from botocore.exceptions import ClientError
sched = boto3.client("scheduler", region_name="us-east-1")
def main():
    out = {}
    for name in ["justhodl-sec-8k-enrich-schedule", "justhodl-master-ranker-schedule",
                 "justhodl-conviction-engine-schedule", "justhodl-sec-8k-schedule"]:
        try:
            r = sched.get_schedule(Name=name)
            out[name] = {"exists": True, "state": r.get("State")}
        except ClientError:
            out[name] = {"exists": False}
    print(json.dumps(out, indent=2))
if __name__ == "__main__":
    main()
