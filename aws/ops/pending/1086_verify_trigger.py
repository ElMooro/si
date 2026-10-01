"""
Ops 1086: verify recreated schedules are enabled and trigger test invocations.

For each of the five units:
 - Confirm schedule exists and is ENABLED
 - Invoke Lambda once to verify it runs (async, don't wait for completion)
 - Check for immediate errors

Writes: aws/ops/reports/1086_verify_trigger.json. Never raises.
"""
from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timezone

OUT_PATH = os.path.join("aws", "ops", "reports", "1086_verify_trigger.json")
REGION = "us-east-1"

UNITS = [
    {"schedule": "justhodl-sec-8k-enrich-schedule",
     "lambda": "justhodl-sec-8k-enrich"},
    {"schedule": "justhodl-xbrl-fundamentals-schedule",
     "lambda": "justhodl-xbrl-fundamentals"},
    {"schedule": "justhodl-corporate-actions-schedule",
     "lambda": "justhodl-corporate-actions"},
    {"schedule": "justhodl-etf-issuer-holdings-daily",
     "lambda": "justhodl-etf-issuer-holdings"},
    {"schedule": "justhodl-short-interest-schedule",
     "lambda": "justhodl-short-interest"},
]

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                "..", "..", "shared"))
import boto3  # noqa: E402
from botocore.exceptions import ClientError  # noqa: E402

sched = boto3.client("scheduler", region_name=REGION)
lam = boto3.client("lambda", region_name=REGION)


def main():
    """Verify and trigger. Write report."""
    rep = {"script": "1086_verify_trigger",
           "read_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
           "units": []}
    for u in UNITS:
        res = {"lambda": u["lambda"], "schedule": u["schedule"]}

        # Check schedule
        try:
            det = sched.get_schedule(Name=u["schedule"])
            res["schedule_state"] = det.get("State")
            res["schedule_cron"] = det.get("ScheduleExpression")
            res["schedule_ok"] = det.get("State") == "ENABLED"
        except Exception as e:  # noqa: BLE001
            res["schedule_state"] = "NOT FOUND"
            res["schedule_ok"] = False
            res["schedule_error"] = str(e)[:80]

        # Trigger Lambda (async Event invocation)
        try:
            inv = lam.invoke(
                FunctionName=u["lambda"],
                InvocationType="Event",
                Payload=json.dumps({"source": "ops-1086-verify",
                                    "trigger": "manual-test"}))
            res["invoke_status"] = inv["StatusCode"]
            res["invoke_ok"] = inv["StatusCode"] == 202
        except Exception as e:  # noqa: BLE001
            res["invoke_ok"] = False
            res["invoke_error"] = str(e)[:120]

        rep["units"].append(res)

    ok_sched = sum(1 for r in rep["units"] if r.get("schedule_ok"))
    ok_inv = sum(1 for r in rep["units"] if r.get("invoke_ok"))
    rep["summary"] = {"schedules_enabled": f"{ok_sched}/5",
                      "invocations_triggered": f"{ok_inv}/5"}
    rep["status"] = "COMPLETE"

    os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)
    with open(OUT_PATH, "w") as f:
        json.dump(rep, f, indent=2)
    print(json.dumps(rep, indent=2)[:4000])


if __name__ == "__main__":
    main()
