"""ops/821 - EventBridge Scheduler migration foundation.

THE PROBLEM (proven by ops 820):
  The AWS account is at 300/300 classic EventBridge rules - the hard default
  cap. Every new per-Lambda schedule rule now fails with LimitExceededException.
  ops 820 band-aided justhodl-dividend-growth onto the shared capital-return
  -daily rule as a second target. That works today, but it is a latent bug:
  deploy-lambdas.yml hardcodes the EventBridge target Id as "target1", so the
  next time dividend-growth's source is deployed the workflow runs
    put-targets --rule capital-return-daily --targets Id=target1,Arn=...dividend-growth
  which OVERWRITES target1 (currently justhodl-capital-return) and silently
  kills capital-return's own schedule. Shared multi-target rules are not safe
  with this workflow.

THE FIX (institutional, permanent):
  Migrate to Amazon EventBridge Scheduler - a separate service with a
  1,000,000-schedule quota (vs the 300-rule cap on classic EventBridge Rules).
  AWS built Scheduler specifically for the run-N-cron-jobs-at-scale pattern.
  This script:
    1. Audits the classic-rule saturation.
    2. Creates a dedicated IAM role  justhodl-scheduler-role  (trusted by
       scheduler.amazonaws.com, may InvokeFunction any justhodl-* Lambda).
    3. Creates an EventBridge Scheduler schedule for justhodl-dividend-growth.
    4. Verifies that schedule is ENABLED and correctly targeted.
    5. ONLY THEN removes dividend-growth from the capital-return-daily rule
       (disarms the time bomb) and rewrites its config.json to the new
       eventbridge_scheduler form.
  If the Scheduler path cannot be established (e.g. an IAM permission gap),
  the band-aid is left intact and the blocker is reported - the system is
  never left worse off.

GO-FORWARD: every new justhodl engine schedules via EventBridge Scheduler.
No engine touches the classic 300-rule pool again.
"""
import json
import os
import time
from datetime import datetime, timezone

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

cfg = Config(read_timeout=60, connect_timeout=20, retries={"max_attempts": 4})
REGION = "us-east-1"
ACCT = "857687956942"
lam = boto3.client("lambda", region_name=REGION, config=cfg)
events = boto3.client("events", region_name=REGION, config=cfg)
scheduler = boto3.client("scheduler", region_name=REGION, config=cfg)
iam = boto3.client("iam", config=cfg)

FN = "justhodl-dividend-growth"
ROLE_NAME = "justhodl-scheduler-role"
ROLE_ARN = f"arn:aws:iam::{ACCT}:role/{ROLE_NAME}"
SCHED_NAME = "justhodl-dividend-growth-daily"
CRON = "cron(45 13 * * ? *)"
HOST_RULE = "capital-return-daily"
CONFIG_PATH = f"aws/lambdas/{FN}/config.json"

report = {
    "ops": 821,
    "ts": datetime.now(timezone.utc).isoformat(),
    "subject": "Migrate scheduled invocation to EventBridge Scheduler "
               "(remove the 300-rule cap blocker)",
}

# --- 1. audit classic-rule saturation ----------------------------------
rules, tok = [], None
while True:
    kw = {"Limit": 100}
    if tok:
        kw["NextToken"] = tok
    resp = events.list_rules(**kw)
    rules += resp.get("Rules", [])
    tok = resp.get("NextToken")
    if not tok:
        break
report["classic_rule_audit"] = {
    "total_classic_rules": len(rules),
    "default_cap": 300,
    "saturated": len(rules) >= 300,
}

sched_existing, stok = [], None
while True:
    kw = {"MaxResults": 100}
    if stok:
        kw["NextToken"] = stok
    resp = scheduler.list_schedules(**kw)
    sched_existing += resp.get("Schedules", [])
    stok = resp.get("NextToken")
    if not stok:
        break
report["scheduler_audit"] = {
    "existing_scheduler_schedules": len(sched_existing),
    "scheduler_quota": "1,000,000 (default) - effectively uncapped",
}

# --- 2. create / ensure the Scheduler IAM role -------------------------
TRUST = {
    "Version": "2012-10-17",
    "Statement": [{
        "Effect": "Allow",
        "Principal": {"Service": "scheduler.amazonaws.com"},
        "Action": "sts:AssumeRole",
        "Condition": {"StringEquals": {"aws:SourceAccount": ACCT}},
    }],
}
INVOKE_POLICY = {
    "Version": "2012-10-17",
    "Statement": [{
        "Effect": "Allow",
        "Action": "lambda:InvokeFunction",
        "Resource": [
            f"arn:aws:lambda:{REGION}:{ACCT}:function:justhodl-*",
        ],
    }],
}
role_status = "unknown"
try:
    try:
        iam.create_role(
            RoleName=ROLE_NAME,
            AssumeRolePolicyDocument=json.dumps(TRUST),
            Description="Lets EventBridge Scheduler invoke justhodl-* "
                        "Lambdas. Created by ops 821.",
            MaxSessionDuration=3600,
        )
        role_status = "created"
    except iam.exceptions.EntityAlreadyExistsException:
        iam.update_assume_role_policy(
            RoleName=ROLE_NAME, PolicyDocument=json.dumps(TRUST))
        role_status = "already-existed (trust refreshed)"
    iam.put_role_policy(
        RoleName=ROLE_NAME,
        PolicyName="invoke-justhodl-lambdas",
        PolicyDocument=json.dumps(INVOKE_POLICY),
    )
except ClientError as e:
    role_status = f"ERROR {e.response['Error']['Code']}: {str(e)[:160]}"
report["scheduler_role"] = {"name": ROLE_NAME, "status": role_status}

role_ok = role_status.startswith(("created", "already-existed"))

# --- 3. create the EventBridge Scheduler schedule ----------------------
fn_arn = None
try:
    fn_arn = lam.get_function_configuration(FunctionName=FN)["FunctionArn"]
except ClientError as e:
    report["fn_arn_error"] = str(e)[:160]

schedule_status = "skipped"
if role_ok and fn_arn:
    target = {
        "Arn": fn_arn,
        "RoleArn": ROLE_ARN,
        "Input": "{}",
        "RetryPolicy": {"MaximumRetryAttempts": 2,
                        "MaximumEventAgeInSeconds": 3600},
    }
    params = dict(
        Name=SCHED_NAME,
        GroupName="default",
        ScheduleExpression=CRON,
        ScheduleExpressionTimezone="UTC",
        FlexibleTimeWindow={"Mode": "OFF"},
        State="ENABLED",
        Description="justhodl-dividend-growth - daily 13:45 UTC. Migrated "
                    "off the saturated 300-rule pool to EventBridge "
                    "Scheduler by ops 821.",
        Target=target,
    )
    # IAM role can take a few seconds to become assumable - retry.
    last_err = None
    for attempt in range(6):
        try:
            try:
                scheduler.create_schedule(**params)
                schedule_status = "created"
            except scheduler.exceptions.ConflictException:
                scheduler.update_schedule(**params)
                schedule_status = "updated"
            last_err = None
            break
        except ClientError as e:
            last_err = f"{e.response['Error']['Code']}: {str(e)[:140]}"
            time.sleep(8)
    if last_err:
        schedule_status = f"ERROR {last_err}"
report["scheduler_schedule"] = {"name": SCHED_NAME, "status": schedule_status}

# --- 4. verify the schedule -------------------------------------------
schedule_verified = False
if schedule_status in ("created", "updated"):
    try:
        gs = scheduler.get_schedule(Name=SCHED_NAME, GroupName="default")
        tgt = gs.get("Target", {}) or {}
        schedule_verified = (
            gs.get("State") == "ENABLED"
            and tgt.get("Arn") == fn_arn
            and tgt.get("RoleArn") == ROLE_ARN
        )
        report["schedule_verify"] = {
            "state": gs.get("State"),
            "expression": gs.get("ScheduleExpression"),
            "timezone": gs.get("ScheduleExpressionTimezone"),
            "target_arn": tgt.get("Arn"),
            "role_arn": tgt.get("RoleArn"),
            "verified": schedule_verified,
        }
    except ClientError as e:
        report["schedule_verify"] = {"error": str(e)[:160]}

# --- 5. disarm the band-aid (ONLY if the new schedule is proven) ------
deband = "not-attempted"
config_rewritten = False
host_targets_after = None
if schedule_verified:
    try:
        tlist = events.list_targets_by_rule(Rule=HOST_RULE).get("Targets", [])
        kill_ids = [t["Id"] for t in tlist
                    if t.get("Arn", "").endswith(f":function:{FN}")]
        if kill_ids:
            events.remove_targets(Rule=HOST_RULE, Ids=kill_ids)
            deband = (f"removed {len(kill_ids)} dividend-growth target(s) "
                      f"from {HOST_RULE}")
        else:
            deband = f"no dividend-growth target found on {HOST_RULE}"
        after = events.list_targets_by_rule(Rule=HOST_RULE).get("Targets", [])
        host_targets_after = sorted({
            t["Arn"].split(":function:")[-1] for t in after
            if ":function:" in t["Arn"]})
    except ClientError as e:
        deband = f"ERROR {str(e)[:140]}"

    # rewrite config.json to the EventBridge Scheduler form so the classic
    # deploy-lambdas.yml schedule path (.schedule) is no longer triggered.
    try:
        conf = json.load(open(CONFIG_PATH))
        conf.pop("schedule", None)
        conf["eventbridge_scheduler"] = {
            "schedule_name": SCHED_NAME,
            "cron": CRON,
            "timezone": "UTC",
            "role_arn": ROLE_ARN,
            "description": "Daily 13:45 UTC via Amazon EventBridge Scheduler. "
                           "The account hit the classic EventBridge 300-rule "
                           "cap; Scheduler (1M-schedule quota) is the "
                           "go-forward scheduling path for all justhodl "
                           "engines. Provisioned by ops 821.",
        }
        with open(CONFIG_PATH, "w") as f:
            json.dump(conf, f, indent=2)
            f.write("\n")
        config_rewritten = True
    except Exception as e:
        report["config_rewrite_error"] = str(e)[:160]
else:
    deband = ("SKIPPED - Scheduler path not proven; band-aid left intact so "
              "dividend-growth keeps auto-updating. See scheduler_role / "
              "scheduler_schedule for the blocker.")

report["deband_aid"] = deband
report["host_rule_targets_after"] = host_targets_after
report["config_rewritten"] = config_rewritten

# --- 6. verdict --------------------------------------------------------
checks = {
    "rule_saturation_confirmed": report["classic_rule_audit"]["saturated"],
    "scheduler_role_ready": role_ok,
    "schedule_created": schedule_status in ("created", "updated"),
    "schedule_verified_enabled": schedule_verified,
    "band_aid_disarmed": schedule_verified and deband.startswith(
        ("removed", "no dividend-growth")),
    "config_migrated": config_rewritten,
}
report["checks"] = checks
report["all_pass"] = all(checks.values())
report["verdict"] = (
    "EVENTBRIDGE SCHEDULER LIVE - justhodl-dividend-growth now runs daily "
    "13:45 UTC via EventBridge Scheduler (uncapped). The capital-return-daily "
    "band-aid is removed, disarming the target1-overwrite bug. Scheduler is "
    "the go-forward scheduling path; the 300-rule cap no longer blocks new "
    "engines."
    if report["all_pass"] else
    "REVIEW - Scheduler migration incomplete; see checks[]. The band-aid "
    "schedule is left intact where the new path was not proven, so "
    "dividend-growth still auto-updates - but the underlying blocker "
    "(likely an IAM permission gap for the ops user) must be resolved.")

print(json.dumps(report, indent=2, default=str))
os.makedirs("aws/ops/reports", exist_ok=True)
with open("aws/ops/reports/821_scheduler_migration.json", "w") as f:
    json.dump(report, f, indent=2, default=str)
print("[ok] wrote aws/ops/reports/821_scheduler_migration.json")
