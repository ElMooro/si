"""
ops_4271 -- reconcile options-flow's declared schedules with reality.

4270's triage found BOTH declared Scheduler entries missing
(justhodl-options-flow-sched, justhodl-options-flow-30m) -- the same
EventBridge-saturation casualty pattern as risk-regime and the
calibration snapshotter. Recreate both exactly as declared in
config/schedule-manifest.json; the manifest is the contract.
"""
import json, sys
from datetime import datetime, timezone
import boto3
from ops_report import report

REGION = "us-east-1"
sch = boto3.client("scheduler", region_name=REGION)

WANT = [
    ("justhodl-options-flow-sched", "cron(25 20 ? * MON-FRI *)"),
    ("justhodl-options-flow-30m", "rate(30 minutes)"),
]
TARGET = ("arn:aws:lambda:us-east-1:857687956942:"
          "function:justhodl-options-flow")

def donor_role():
    for pg in sch.get_paginator("list_schedules").paginate(
            GroupName="default"):
        for it in pg.get("Schedules", []):
            try:
                d = sch.get_schedule(Name=it["Name"], GroupName="default")
                ra = (d.get("Target") or {}).get("RoleArn")
                if ra:
                    return ra
            except Exception:
                continue
    return None

fails = []
with report("4271_options_schedules") as r:
    r.heading("ops 4271 -- options-flow schedules reconciled")
    ra = donor_role()
    if not ra:
        fails.append("no donor RoleArn")
    for name, expr in WANT:
        try:
            d = sch.get_schedule(Name=name, GroupName="default")
            r.ok("%s already present: %s %s"
                 % (name, d.get("State"), d.get("ScheduleExpression")))
            continue
        except Exception:
            pass
        if not ra:
            continue
        try:
            sch.create_schedule(
                Name=name, GroupName="default",
                ScheduleExpression=expr,
                FlexibleTimeWindow={"Mode": "OFF"},
                State="ENABLED",
                Target={"Arn": TARGET, "RoleArn": ra, "Input": "{}"})
            r.ok("CREATED %s %s -> options-flow" % (name, expr))
        except Exception as e:
            fails.append("%s: %s" % (name, str(e)[:110]))
    r.section("RESULT")
    if fails:
        for f in fails:
            r.fail("  %s" % f)
    else:
        r.ok("OPS 4271 PASS -- declarations true; the alias + both "
             "cadences keep options-flow.json living")
if fails:
    sys.exit(1)
