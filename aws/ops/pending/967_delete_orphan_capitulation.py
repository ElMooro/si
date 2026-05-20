"""
ops 967 -- delete orphan justhodl-capitulation Lambda
=======================================================

This Lambda was the predecessor of Edge #1 (justhodl-vix-backwardation-trigger).
It writes to data/capitulation.json which no production page consumes -- the
vix-capitulation.html page is wired to data/vix-backwardation-trigger.json
(produced by the new properly-named Lambda).

Steps:
  1. Confirm justhodl-vix-backwardation-trigger exists and is producing output
  2. Check what EventBridge rules/schedulers target justhodl-capitulation
  3. Disassociate those targets (don't delete the rules; they may be reused)
  4. Delete justhodl-capitulation
  5. Optionally also delete the orphan S3 object data/capitulation.json
     (leave the history file alone -- harmless and small)
"""
import datetime as dt
import json
import os

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

REGION = "us-east-1"
S3_BUCKET = "justhodl-dashboard-live"

lam = boto3.client("lambda", region_name=REGION,
                   config=Config(read_timeout=60, connect_timeout=10))
s3 = boto3.client("s3", region_name=REGION)
events = boto3.client("events", region_name=REGION)
scheduler = boto3.client("scheduler", region_name=REGION)

OLD_FN = "justhodl-capitulation"
NEW_FN = "justhodl-vix-backwardation-trigger"
OLD_S3_KEY = "data/capitulation.json"

CHECKS = []


def add(name, ok, detail=""):
    CHECKS.append({"name": name, "passed": ok, "detail": str(detail)[:280]})


def main():
    print(f"ops 967 -- delete orphan {OLD_FN} at {dt.datetime.utcnow().isoformat()}Z")

    # 1. Verify successor exists + has fresh output
    try:
        info = lam.get_function(FunctionName=NEW_FN)
        mod = info["Configuration"].get("LastModified", "")
        add("successor.exists", True, f"{NEW_FN} mod={mod[:19]}")
    except ClientError as e:
        add("successor.exists", False, str(e)[:200])
        print("ABORT: successor Lambda not deployed -- refusing to delete predecessor")
        write_report()
        return

    try:
        h = s3.head_object(Bucket=S3_BUCKET, Key="data/vix-backwardation-trigger.json")
        age_h = (dt.datetime.now(dt.timezone.utc) - h["LastModified"]).total_seconds() / 3600
        add("successor.s3_fresh", h["ContentLength"] > 500 and age_h < 168,
            f"size={h['ContentLength']}B age_h={round(age_h, 1)}")
    except ClientError as e:
        add("successor.s3_fresh", False, str(e)[:200])
        print("ABORT: successor has no S3 output -- not safe to delete predecessor")
        write_report()
        return

    # 2. Find EventBridge rules targeting OLD_FN
    rules_removed = []
    try:
        # rules-by-target requires the lambda ARN
        old_arn = lam.get_function(FunctionName=OLD_FN)["Configuration"]["FunctionArn"]
        add("orphan.arn", True, old_arn)

        # ListRuleNamesByTarget
        try:
            r = events.list_rule_names_by_target(TargetArn=old_arn)
            rules = r.get("RuleNames", [])
            add("orphan.eb_rules_found", True, f"n={len(rules)} rules={rules[:5]}")
            for rule in rules:
                try:
                    # Get target IDs
                    tt = events.list_targets_by_rule(Rule=rule)
                    target_ids = [t["Id"] for t in tt.get("Targets", [])
                                  if t.get("Arn") == old_arn]
                    if target_ids:
                        events.remove_targets(Rule=rule, Ids=target_ids)
                        rules_removed.append(rule)
                        print(f"  removed targets {target_ids} from rule {rule}")
                except ClientError as e:
                    print(f"  rule cleanup failed {rule}: {e}")
        except ClientError as e:
            add("orphan.eb_rules_found", False, str(e)[:150])

        add("orphan.eb_targets_removed", True, f"n={len(rules_removed)} rules={rules_removed[:5]}")
    except ClientError as e:
        # If get_function fails it means already deleted -- proceed
        add("orphan.arn", False, str(e)[:200])
        if "ResourceNotFoundException" in str(e):
            print("Lambda already deleted -- nothing to do")
            write_report()
            return

    # 3. Find EventBridge Scheduler schedules targeting OLD_FN
    sched_removed = []
    try:
        paginator = scheduler.get_paginator("list_schedules")
        for page in paginator.paginate():
            for s in page.get("Schedules", []):
                try:
                    sd = scheduler.get_schedule(Name=s["Name"])
                    target_arn = sd.get("Target", {}).get("Arn", "")
                    if OLD_FN in target_arn:
                        scheduler.delete_schedule(Name=s["Name"])
                        sched_removed.append(s["Name"])
                        print(f"  deleted scheduler schedule {s['Name']}")
                except ClientError as e:
                    print(f"  scheduler check failed {s['Name']}: {e}")
        add("orphan.scheduler_schedules_removed", True,
            f"n={len(sched_removed)} names={sched_removed[:5]}")
    except ClientError as e:
        add("orphan.scheduler_schedules_removed", False, str(e)[:200])

    # 4. Delete the Lambda
    try:
        lam.delete_function(FunctionName=OLD_FN)
        add("orphan.deleted", True, f"{OLD_FN} delete_function ok")
    except ClientError as e:
        if "ResourceNotFoundException" in str(e):
            add("orphan.deleted", True, "already deleted")
        else:
            add("orphan.deleted", False, str(e)[:200])

    # 5. Delete the orphan S3 object (keep history file)
    try:
        s3.delete_object(Bucket=S3_BUCKET, Key=OLD_S3_KEY)
        add("orphan.s3_object_deleted", True, OLD_S3_KEY)
    except ClientError as e:
        add("orphan.s3_object_deleted", False, str(e)[:200])

    write_report()


def write_report():
    rep = {
        "ops": 967,
        "title": "delete orphan justhodl-capitulation Lambda (superseded by Edge #1)",
        "run_at": dt.datetime.utcnow().isoformat() + "Z",
        "checks": CHECKS,
        "summary": {"total": len(CHECKS),
                    "passed": sum(1 for c in CHECKS if c["passed"]),
                    "failed": sum(1 for c in CHECKS if not c["passed"])},
        "overall_ok": all(c["passed"] for c in CHECKS),
    }
    os.makedirs("aws/ops/reports", exist_ok=True)
    with open("aws/ops/reports/967_delete_orphan_capitulation.json", "w") as f:
        json.dump(rep, f, indent=2)
    print(f"\n=== {rep['summary']['passed']}/{rep['summary']['total']} ===")
    for c in CHECKS:
        flag = "OK " if c["passed"] else "FAIL"
        print(f"  [{flag}] {c['name']:35} {c['detail'][:140]}")


if __name__ == "__main__":
    main()
