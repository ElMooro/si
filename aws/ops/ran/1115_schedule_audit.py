"""Ops 1115: Full schedule audit - compare expected vs actual.
Lists all Lambda functions and checks for their schedules.
"""
from __future__ import annotations
import json, os, sys
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "shared"))
import boto3
from botocore.exceptions import ClientError

sched = boto3.client("scheduler", region_name="us-east-1")
lam = boto3.client("lambda", region_name="us-east-1")

def main():
    out = {"schedules": [], "lambdas_without_schedules": [], "expected_missing": []}
    # List all schedules
    paginator = sched.get_paginator("list_schedules")
    all_scheds = []
    for page in paginator.paginate():
        all_scheds.extend(page.get("Schedules", []))
    out["total_schedules"] = len(all_scheds)
    for s in all_scheds:
        if "justhodl" in s["Name"].lower():
            out["schedules"].append({
                "name": s["Name"],
                "state": s.get("State"),
                "expression": s.get("ScheduleExpression", "")[:50],
            })
    # List all justhodl lambdas
    paginator = lam.get_paginator("list_functions")
    lambdas = []
    for page in paginator.paginate():
        for fn in page.get("Functions", []):
            n = fn["FunctionName"]
            if n.startswith("justhodl-"):
                lambdas.append(n)
    out["total_lambdas"] = len(lambdas)
    # Find lambdas without schedules
    sched_names = {s["name"] for s in out["schedules"]}
    for ln in sorted(lambdas):
        # Expected schedule name pattern
        expected = f"{ln}-schedule"
        if expected not in sched_names and ln not in str(sched_names):
            # Check if any schedule targets this lambda
            has_sched = any(ln in s["name"] for s in out["schedules"])
            if not has_sched:
                out["lambdas_without_schedules"].append(ln)
    print(json.dumps(out, indent=2))
    os.makedirs("aws/ops/reports", exist_ok=True)
    with open("aws/ops/reports/1115_schedule_audit.json", "w") as f:
        json.dump(out, f, indent=2)

if __name__ == "__main__":
    main()
