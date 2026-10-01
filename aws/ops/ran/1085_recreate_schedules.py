"""
Ops 1085: recreate the five missing EventBridge schedules.

From 1084 audit: all expected schedules are MISSING (not disabled, not found).
Recreate:
 1. justhodl-sec-8k-enrich-schedule -> justhodl-sec-8k-enrich, cron(10,40 * * *? *)
 2. justhodl-xbrl-fundamentals-schedule -> justhodl-xbrl-fundamentals, cron(0 6 ? * SUN *)
 3. justhodl-corporate-actions-schedule -> justhodl-corporate-actions, cron(30 6 ? * * *)
 4. justhodl-etf-issuer-holdings-daily -> justhodl-etf-issuer-holdings, cron(0 4 * * ? *)
 5. short-interest: find existing schedule or create justhodl-short-interest-schedule

Uses the IAM role from an existing justhodl schedule as template.
Writes: aws/ops/reports/1085_recreate_schedules.json. Never raises.
"""
from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timezone

OUT_PATH = os.path.join("aws", "ops", "reports", "1085_recreate_schedules.json")
REGION = "us-east-1"
ACCOUNT = "857687956942"

SCHEDULES = [
    {"name": "justhodl-sec-8k-enrich-schedule",
     "lambda": "justhodl-sec-8k-enrich",
     "cron": "cron(10,40 * * * ? *)",
     "desc": "SEC 8-K enrichment twice hourly"},
    {"name": "justhodl-xbrl-fundamentals-schedule",
     "lambda": "justhodl-xbrl-fundamentals",
     "cron": "cron(0 6 ? * SUN *)",
     "desc": "XBRL fundamentals weekly Sunday"},
    {"name": "justhodl-corporate-actions-schedule",
     "lambda": "justhodl-corporate-actions",
     "cron": "cron(30 6 ? * * *)",
     "desc": "Corporate actions daily"},
    {"name": "justhodl-etf-issuer-holdings-daily",
     "lambda": "justhodl-etf-issuer-holdings",
     "cron": "cron(0 4 * * ? *)",
     "desc": "ETF holdings daily 4am UTC"},
    {"name": "justhodl-short-interest-schedule",
     "lambda": "justhodl-short-interest",
     "cron": "cron(15 7 ? * * *)",
     "desc": "Short interest daily"},
]

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                "..", "..", "shared"))
import boto3  # noqa: E402
from botocore.exceptions import ClientError  # noqa: E402

sched = boto3.client("scheduler", region_name=REGION)


def get_template_role():
    """Get IAM role from an existing justhodl schedule. Never raises."""
    try:
        paginator = sched.get_paginator("list_schedules")
        for page in paginator.paginate():
            for s in page.get("Schedules", []):
                if "justhodl" in s.get("Name", "").lower():
                    try:
                        det = sched.get_schedule(Name=s["Name"])
                        target = det.get("Target", {})
                        role = target.get("RoleArn")
                        if role:
                            return role
                    except Exception:  # noqa: BLE001
                        continue
        return None
    except Exception:  # noqa: BLE001
        return None


def create_schedule(name, lambda_fn, cron_expr, role_arn, desc):
    """Create a schedule. Returns dict with status. Never raises."""
    fn_arn = f"arn:aws:lambda:{REGION}:{ACCOUNT}:function:{lambda_fn}"
    try:
        # Check if exists first
        try:
            existing = sched.get_schedule(Name=name)
            if existing.get("State") == "ENABLED":
                return {"name": name, "status": "already_exists_enabled"}
            # Exists but disabled - enable it
            sched.update_schedule(
                Name=name,
                ScheduleExpression=existing["ScheduleExpression"],
                Target=existing["Target"],
                FlexibleTimeWindow={"Mode": "OFF"},
                State="ENABLED",
                Description=existing.get("Description", desc))
            return {"name": name, "status": "enabled_existing"}
        except ClientError as e:
            if e.response["Error"]["Code"] != "ResourceNotFoundException":
                raise

        # Create new
        sched.create_schedule(
            Name=name,
            ScheduleExpression=cron_expr,
            Target={
                "Arn": fn_arn,
                "RoleArn": role_arn,
            },
            FlexibleTimeWindow={"Mode": "OFF"},
            State="ENABLED",
            Description=desc)
        return {"name": name, "status": "created", "cron": cron_expr,
                "lambda": lambda_fn}
    except Exception as e:  # noqa: BLE001
        return {"name": name, "status": "failed", "error": str(e)[:200]}


def main():
    """Recreate schedules. Write report."""
    rep = {"script": "1085_recreate_schedules",
           "read_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
           "results": []}

    role = get_template_role()
    rep["role_arn_found"] = bool(role)
    if role:
        # Don't log full ARN, just confirm we have one
        rep["role_prefix"] = role.split("/")[-1][:30]

    if not role:
        rep["status"] = "ABORTED"
        rep["error"] = "No template IAM role found from existing schedules"
    else:
        for s in SCHEDULES:
            result = create_schedule(s["name"], s["lambda"], s["cron"],
                                     role, s["desc"])
            rep["results"].append(result)

        created = sum(1 for r in rep["results"] if r["status"] == "created")
        enabled = sum(1 for r in rep["results"]
                      if r["status"] in ("already_exists_enabled",
                                         "enabled_existing"))
        failed = sum(1 for r in rep["results"] if r["status"] == "failed")
        rep["summary"] = {"created": created, "already_ok": enabled,
                          "failed": failed}
        rep["status"] = "COMPLETE" if failed == 0 else "PARTIAL"

    os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)
    with open(OUT_PATH, "w") as f:
        json.dump(rep, f, indent=2)
    print(json.dumps(rep, indent=2)[:4000])


if __name__ == "__main__":
    main()
