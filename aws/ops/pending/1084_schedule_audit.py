"""
Ops 1084: diagnose missing EventBridge schedules.

Lists all schedules to see what exists vs what we expect.

Writes: aws/ops/reports/1084_schedule_audit.json. Never raises.
"""
from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timezone

OUT_PATH = os.path.join("aws", "ops", "reports", "1084_schedule_audit.json")
REGION = "us-east-1"

EXPECTED = [
    "justhodl-sec-8k-enrich-schedule",
    "justhodl-xbrl-fundamentals-schedule",
    "justhodl-cboe-options-chain-schedule",
    "justhodl-corporate-actions-schedule",
    "justhodl-etf-issuer-holdings-daily",
    "justhodl-microcap-float-squeeze-daily",
    "justhodl-transcript-query-weekly",
    "justhodl-ticker-360-schedule",
]

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                "..", "..", "shared"))
import boto3  # noqa: E402
from botocore.exceptions import ClientError  # noqa: E402

sched = boto3.client("scheduler", region_name=REGION)


def main():
    """Audit schedules. Write report."""
    rep = {"script": "1084_schedule_audit",
           "read_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
           "expected": {},
           "all_justhodl": []}
    try:
        # List all schedules
        paginator = sched.get_paginator("list_schedules")
        all_scheds = []
        for page in paginator.paginate():
            all_scheds.extend(page.get("Schedules", []))

        # Filter justhodl ones
        for s in all_scheds:
            name = s.get("Name", "")
            if "justhodl" in name.lower():
                rep["all_justhodl"].append({
                    "name": name,
                    "state": s.get("State"),
                    "expression": s.get("ScheduleExpression")})

        # Check each expected
        existing_names = {s.get("Name") for s in all_scheds}
        for name in EXPECTED:
            if name in existing_names:
                # Get details
                try:
                    det = sched.get_schedule(Name=name)
                    rep["expected"][name] = {
                        "exists": True,
                        "state": det.get("State"),
                        "expression": det.get("ScheduleExpression")}
                except Exception as e:  # noqa: BLE001
                    rep["expected"][name] = {"exists": True, "error": str(e)[:60]}
            else:
                rep["expected"][name] = {"exists": False}

        rep["total_schedules"] = len(all_scheds)
        rep["justhodl_count"] = len(rep["all_justhodl"])
        rep["status"] = "COMPLETE"
    except Exception as e:  # noqa: BLE001
        rep["status"] = "ERROR"
        rep["error"] = str(e)[:200]

    os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)
    with open(OUT_PATH, "w") as f:
        json.dump(rep, f, indent=2)
    print(json.dumps(rep, indent=2)[:5000])


if __name__ == "__main__":
    main()
