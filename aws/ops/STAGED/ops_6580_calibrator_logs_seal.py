"""ops 6580 - three small brakes (Khalid, 2026-10-20: "do what you think is best ... seal anything that might go rogue").

1. justhodl-calibrator gets its own daily schedule (22:05 UTC, after the outcome-checker at 21:29): it was the one
   engine that only ran from the event bus, and the bus coordinator stays under its $50 cap - so without this the
   calibration weights never update again.  Added to config/schedule-manifest.json (committed with this script) and to
   the deployed S3 copy (same two-edit sync as ops 6565).
2. CloudWatch Logs: every log group with no retention gets 30 days (a loop that logs is bounded; storage stops growing
   forever); groups that already have a retention keep it.
3. An account-level log-ingestion alarm (Metrics Insights: SUM(IncomingBytes) over AWS/Logs > 2 GB in an hour -> the
   fleet-alerts topic): a chatty loop costs $0.50/GB of ingestion and nothing watched that line.
"""
from pathlib import Path
import json
import sys
import time

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
from ops_report import report  # noqa: E402

REGION, ACCOUNT = "us-east-1", "857687956942"
LIVE, PRIVATE = "justhodl-dashboard-live", "justhodl-ops-private-%s" % ACCOUNT
TOPIC = "arn:aws:sns:us-east-1:%s:justhodl-fleet-alerts" % ACCOUNT
SCHED = {"name": "justhodl-calibrator-daily", "function": "justhodl-calibrator", "expr": "cron(5 22 * * ? *)",
         "description": "Signal calibration weights - daily 22:05 UTC after the outcome-checker (21:29). Was bus-only (ops 6580, 2026-10-20)."}
CFG = Config(retries={"max_attempts": 10, "mode": "standard"}, connect_timeout=10, read_timeout=120)


def c(name):
    return boto3.client(name, region_name=REGION, config=CFG)


def calibrator_schedule(r):
    scheduler, iam, lam = c("scheduler"), c("iam"), c("lambda")
    arn = lam.get_function_configuration(FunctionName=SCHED["function"])["FunctionArn"]
    try:
        old = scheduler.get_schedule(Name=SCHED["name"], GroupName="default")
        assert old["Target"]["Arn"] == arn
        r.log("schedule %s exists: %s %s" % (SCHED["name"], old["ScheduleExpression"], old["State"]))
        return
    except scheduler.exceptions.ResourceNotFoundException:
        pass
    role = iam.get_role(RoleName="justhodl-scheduler-role")["Role"]["Arn"]
    scheduler.create_schedule(Name=SCHED["name"], GroupName="default", ScheduleExpression=SCHED["expr"], ScheduleExpressionTimezone="UTC",
                              State="ENABLED", Description=SCHED["description"], FlexibleTimeWindow={"Mode": "OFF"},
                              Target={"Arn": arn, "RoleArn": role, "RetryPolicy": {"MaximumEventAgeInSeconds": 3600, "MaximumRetryAttempts": 0}})
    chk = scheduler.get_schedule(Name=SCHED["name"], GroupName="default")
    assert chk["State"] == "ENABLED" and chk["ScheduleExpression"] == SCHED["expr"]
    r.ok("schedule %s created: %s UTC" % (SCHED["name"], SCHED["expr"]))


def sync_manifest(r, s3, stamp):
    key = "config/schedule-manifest.json"
    old = s3.get_object(Bucket=LIVE, Key=key)["Body"].read().decode("utf-8")
    doc = json.loads(old)
    if not any(x["name"] == SCHED["name"] for x in doc["schedules"]):
        doc["schedules"].append({"expr": SCHED["expr"], "group": "default", "kind": "scheduler", "name": SCHED["name"], "state": "ENABLED", "timezone": "UTC",
                                 "targets": [{"arn": "arn:aws:lambda:%s:%s:function:%s" % (REGION, ACCOUNT, SCHED["function"]), "input": None, "path": None}]})
        doc["schedules"].sort(key=lambda x: x["name"])
        new = json.dumps(doc, indent=1) + "\n"
        s3.put_object(Bucket=PRIVATE, Key="backups/schedule-manifest/%s.json" % stamp, Body=old.encode("utf-8"), ServerSideEncryption="AES256")
        s3.put_object(Bucket=LIVE, Key=key, Body=new.encode("utf-8"), ContentType="application/json", CacheControl="no-cache")
        r.log("S3 schedule manifest: calibrator schedule added")
    repo = json.loads((ROOT / key).read_text(encoding="utf-8"))
    same = {k: v for k, v in doc.items() if k != "generated_at"} == {k: v for k, v in repo.items() if k != "generated_at"}
    r.log("S3 schedule manifest %s the committed file" % ("equals" if same else "DIFFERS from"))


def log_retention(r):
    logs = c("logs")
    n_set, n_kept, groups = 0, 0, 0
    for page in logs.get_paginator("describe_log_groups").paginate():
        for g in page.get("logGroups", []):
            groups += 1
            if g.get("retentionInDays"):
                n_kept += 1
                continue
            logs.put_retention_policy(logGroupName=g["logGroupName"], retentionInDays=30)
            n_set += 1
    r.ok("log groups: %d; retention set to 30 days on %d that had none; %d kept their own" % (groups, n_set, n_kept))
    return n_set


def ingestion_alarm(r):
    cw = c("cloudwatch")
    name = "justhodl-logs-ingestion-2gb-hour"
    cw.put_metric_alarm(AlarmName=name, AlarmDescription="CloudWatch Logs ingestion over 2 GB in an hour, account-wide ($0.50/GB: a chatty loop). ops 6580.",
                        Metrics=[{"Id": "ingest", "Expression": "SELECT SUM(IncomingBytes) FROM SCHEMA(\"AWS/Logs\", LogGroupName)", "Period": 3600, "ReturnData": True}],
                        EvaluationPeriods=1, DatapointsToAlarm=1, Threshold=2.0 * 1024 ** 3, ComparisonOperator="GreaterThanThreshold",
                        TreatMissingData="notBreaching", ActionsEnabled=True, AlarmActions=[TOPIC], OKActions=[TOPIC])
    st = cw.describe_alarms(AlarmNames=[name])["MetricAlarms"][0]["StateValue"]
    r.ok("alarm %s: %s" % (name, st))


def main():
    s3 = c("s3")
    with report("ops_6580_calibrator_logs_seal") as r:
        r.heading("ops 6580 - calibrator daily schedule; 30-day log retention; log-ingestion alarm")
        stamp = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
        calibrator_schedule(r)
        sync_manifest(r, s3, stamp)
        n = log_retention(r)
        ingestion_alarm(r)
        r.kv(calibrator_schedule=SCHED["expr"], retention_set=n, alarm="justhodl-logs-ingestion-2gb-hour")
        r.ok("done")


if __name__ == "__main__":
    main()
