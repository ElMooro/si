"""ops 6523 - arm justhodl-spend-governor (deployed by deploy-lambdas from the same push) and prove it live.

1. Wait until the deployed governor is the code in this commit (by content: governor_core.VERSION and the
   ledger/levers modules inside the live package) and runs as justhodl-spend-governor-role.
2. Reserved concurrency 1 on the governor (single writer of its state); never throttled by itself.
3. Wire the Budgets topic justhodl-spend-governor -> governor (subscription + invoke permission) and make
   sure the hourly Scheduler tick exists and its role may invoke the governor.
4. Live proofs, in order: selftest on the canary (throttle -> invoke refused -> exact restore),
   status (fresh Cost Explorer read), plan FREEZE and plan BRAKE (read-only), one tick in monitor mode.
5. Flip control.json to mode=enforce, tick again, run the runaway fuse once.
6. Dead-man alarms to justhodl-fleet-alerts: no heartbeat for 3 hours, or any governor error.
"""

from datetime import datetime, timezone, timedelta
from pathlib import Path
import io
import json
import sys
import time
import urllib.request
import zipfile

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
from ops_report import report  # noqa: E402

REGION = "us-east-1"
ACCOUNT = "857687956942"
FN = "justhodl-spend-governor"
CANARY = "justhodl-spend-governor-canary"
ROLE_ARN = "arn:aws:iam::%s:role/justhodl-spend-governor-role" % ACCOUNT
SCHED_ROLE = "justhodl-scheduler-role"
SCHEDULE = "justhodl-spend-governor-tick"
GOV_TOPIC_ARN = "arn:aws:sns:%s:%s:justhodl-spend-governor" % (REGION, ACCOUNT)
ALERT_TOPIC_ARN = "arn:aws:sns:%s:%s:justhodl-fleet-alerts" % (REGION, ACCOUNT)
STATE_BUCKET = "justhodl-ops-private-%s" % ACCOUNT
VERSION = '"1.0.0"'
OUT_JSON = ROOT / "aws" / "ops" / "reports" / "6523.json"

lam = boto3.client("lambda", region_name=REGION, config=Config(retries={"max_attempts": 8, "mode": "adaptive"}))
lam_sync = boto3.client("lambda", region_name=REGION,
                        config=Config(retries={"max_attempts": 0}, read_timeout=910, connect_timeout=20))
s3 = boto3.client("s3", region_name=REGION)
sns = boto3.client("sns", region_name=REGION)
sch = boto3.client("scheduler", region_name=REGION)
cw = boto3.client("cloudwatch", region_name=REGION)
iam = boto3.client("iam")
out = {"op": 6523, "ran_at": datetime.now(timezone.utc).isoformat()}


def live_package(fn):
    cfg = lam.get_function(FunctionName=fn)
    with urllib.request.urlopen(cfg["Code"]["Location"], timeout=60) as r:
        return cfg["Configuration"], zipfile.ZipFile(io.BytesIO(r.read()))


def wait_deployed(r, deadline_min=28):
    deadline = time.time() + deadline_min * 60
    last = None
    while time.time() < deadline:
        try:
            conf, z = live_package(FN)
            names = set(z.namelist())
            core = z.read("governor_core.py").decode() if "governor_core.py" in names else ""
            ok = (conf.get("State") == "Active" and conf.get("LastUpdateStatus") == "Successful"
                  and conf.get("Role") == ROLE_ARN and ("VERSION = %s" % VERSION) in core
                  and {"lambda_function.py", "governor_levers.py"} <= names)
            last = {"state": conf.get("State"), "update": conf.get("LastUpdateStatus"), "role": conf.get("Role"),
                    "version_ok": ("VERSION = %s" % VERSION) in core}
            if ok:
                r.ok("governor live by content: %s" % json.dumps(last))
                return True
        except ClientError as e:
            last = e.response["Error"]["Code"]
        time.sleep(20)
    r.fail("governor not live after %d min: %s" % (deadline_min, last))
    out["deploy_last_seen"] = last
    return False


def invoke(payload, tries=6):
    for i in range(tries):
        try:
            res = lam_sync.invoke(FunctionName=FN, InvocationType="RequestResponse", Payload=json.dumps(payload).encode())
            body = json.loads(res["Payload"].read() or b"null")
            if res.get("FunctionError"):
                return {"function_error": res["FunctionError"], "body": body}
            return body
        except ClientError as e:
            if e.response["Error"]["Code"] == "TooManyRequestsException" and i < tries - 1:
                time.sleep(30)  # the hourly tick holds the single slot
                continue
            raise


def wire_sns(r):
    fn_arn = lam.get_function(FunctionName=FN)["Configuration"]["FunctionArn"]
    try:
        lam.add_permission(FunctionName=FN, StatementId="budgets-sns-6523", Action="lambda:InvokeFunction",
                           Principal="sns.amazonaws.com", SourceArn=GOV_TOPIC_ARN)
    except ClientError as e:
        if e.response["Error"]["Code"] != "ResourceConflictException":
            raise
    subs = sns.list_subscriptions_by_topic(TopicArn=GOV_TOPIC_ARN).get("Subscriptions", [])
    if not any(s["Protocol"] == "lambda" and s["Endpoint"] == fn_arn for s in subs):
        sns.subscribe(TopicArn=GOV_TOPIC_ARN, Protocol="lambda", Endpoint=fn_arn)
    subs = sns.list_subscriptions_by_topic(TopicArn=GOV_TOPIC_ARN).get("Subscriptions", [])
    ok = any(s["Protocol"] == "lambda" and s["Endpoint"] == fn_arn for s in subs)
    (r.ok if ok else r.fail)("Budgets topic -> governor subscription %s" % ("confirmed" if ok else "missing"))
    return ok


def ensure_schedule(r):
    fn_arn = lam.get_function(FunctionName=FN)["Configuration"]["FunctionArn"]
    role_arn = "arn:aws:iam::%s:role/%s" % (ACCOUNT, SCHED_ROLE)
    try:
        s = sch.get_schedule(Name=SCHEDULE, GroupName="default")
        r.log("schedule exists: %s %s -> %s" % (s["ScheduleExpression"], s["State"], s["Target"]["Arn"].split(":")[-1]))
    except ClientError as e:
        if e.response["Error"]["Code"] != "ResourceNotFoundException":
            raise
        sch.create_schedule(Name=SCHEDULE, GroupName="default", ScheduleExpression="cron(7 * * * ? *)",
                            ScheduleExpressionTimezone="UTC", FlexibleTimeWindow={"Mode": "OFF"}, State="ENABLED",
                            Description="Hourly spend-cap tick (ops 6523)",
                            Target={"Arn": fn_arn, "RoleArn": role_arn, "Input": json.dumps({"action": "tick"}),
                                    "RetryPolicy": {"MaximumRetryAttempts": 2, "MaximumEventAgeInSeconds": 3600}})
        r.ok("schedule %s created" % SCHEDULE)
        s = sch.get_schedule(Name=SCHEDULE, GroupName="default")
    sim = iam.simulate_principal_policy(PolicySourceArn=role_arn, ActionNames=["lambda:InvokeFunction"],
                                       ResourceArns=[fn_arn])["EvaluationResults"][0]["EvalDecision"]
    if sim != "allowed":
        iam.put_role_policy(RoleName=SCHED_ROLE, PolicyName="invoke-justhodl-spend-governor", PolicyDocument=json.dumps({
            "Version": "2012-10-17", "Statement": [{"Effect": "Allow", "Action": "lambda:InvokeFunction",
                                                    "Resource": [fn_arn, fn_arn + ":*"]}]}))
        r.ok("scheduler role granted invoke on the governor (was %s)" % sim)
    ok = s["State"] == "ENABLED" and s["Target"]["Arn"].split(":function:")[-1].split(":")[0] == FN
    (r.ok if ok else r.fail)("hourly tick schedule %s (%s)" % ("verified" if ok else "WRONG", s["ScheduleExpression"]))
    return {"expr": s["ScheduleExpression"], "state": s["State"], "role_sim": sim}


def set_mode(mode):
    key = "spend-governor/control.json"
    cur = json.loads(s3.get_object(Bucket=STATE_BUCKET, Key=key)["Body"].read())
    cur["mode"] = mode
    cur["mode_set_by"] = "ops 6523"
    cur["mode_set_at"] = datetime.now(timezone.utc).isoformat()
    s3.put_object(Bucket=STATE_BUCKET, Key=key, Body=json.dumps(cur, indent=1).encode(),
                  ContentType="application/json", ServerSideEncryption="AES256")
    return cur


def alarms(r):
    cw.put_metric_alarm(
        AlarmName="justhodl-spend-governor-heartbeat", Namespace="JustHodl/SpendGovernor", MetricName="Heartbeat",
        Statistic="SampleCount", Period=3600, EvaluationPeriods=3, DatapointsToAlarm=3, Threshold=1,
        ComparisonOperator="LessThanThreshold", TreatMissingData="breaching", ActionsEnabled=True,
        AlarmActions=[ALERT_TOPIC_ARN], OKActions=[ALERT_TOPIC_ARN],
        AlarmDescription="The $500 spend-cap governor has not ticked for 3 hours - the cap is NOT being enforced.")
    cw.put_metric_alarm(
        AlarmName="justhodl-spend-governor-errors", Namespace="AWS/Lambda", MetricName="Errors",
        Dimensions=[{"Name": "FunctionName", "Value": FN}], Statistic="Sum", Period=3600, EvaluationPeriods=1,
        Threshold=1, ComparisonOperator="GreaterThanOrEqualToThreshold", TreatMissingData="notBreaching",
        ActionsEnabled=True, AlarmActions=[ALERT_TOPIC_ARN],
        AlarmDescription="justhodl-spend-governor raised an error - check its logs; the cap may not be enforced.")
    got = cw.describe_alarms(AlarmNames=["justhodl-spend-governor-heartbeat", "justhodl-spend-governor-errors"])["MetricAlarms"]
    r.ok("alarms: %s" % [(a["AlarmName"], a["StateValue"]) for a in got])
    return [(a["AlarmName"], a["StateValue"]) for a in got]


def main():
    with report("ops_6523_spend_governor_arm") as r:
        r.heading("ops 6523 - arm the spend governor")
        failed = []
        if not wait_deployed(r):
            OUT_JSON.write_text(json.dumps(out, indent=1, default=str))
            sys.exit(1)
        lam.put_function_concurrency(FunctionName=FN, ReservedConcurrentExecutions=1)
        r.ok("governor reserved concurrency 1")
        if not wire_sns(r):
            failed.append("sns")
        out["schedule"] = ensure_schedule(r)
        for _ in range(30):  # canary deployed by the previous push
            try:
                if lam.get_function(FunctionName=CANARY)["Configuration"].get("State") == "Active":
                    break
            except ClientError:
                pass
            time.sleep(20)

        r.section("Live proofs")
        st = invoke({"action": "selftest"})
        out["selftest"] = st
        (r.ok if st.get("ok") else r.fail)("selftest on canary: %s" % json.dumps(st))
        if not st.get("ok"):
            failed.append("selftest")
        status = invoke({"action": "status", "refresh": True})
        out["status"] = status
        th = status.get("thresholds") or {}
        r.log("cap $%s (%s) | MTD $%s | brake $%s | freeze $%s | baseline $%s/day | residual $%s/day | projected $%s" % (
            status.get("cap"), status.get("cap_source"), th.get("mtd"), th.get("brake_at"), th.get("freeze_at"),
            th.get("baseline_usd_day"), th.get("residual_usd_day"), th.get("projected_eom")))
        for stage in ("FREEZE", "BRAKE"):
            p = invoke({"action": "plan", "stage": stage})
            out["plan_" + stage.lower()] = p
            r.log("plan %s would act on %s; logs %s" % (stage, json.dumps(p.get("would_act_on")), (p.get("logs") or [])[:3]))
            if stage == "FREEZE":
                r.log("exempt: %s" % p.get("exempt"))
        t1 = invoke({"action": "tick", "source": "ops"})
        out["tick_monitor"] = t1
        r.log("tick (monitor): decision=%s stage=%s reason=%s" % (t1.get("decision"), t1.get("stage"), t1.get("reason")))
        if t1.get("function_error"):
            failed.append("tick_monitor")

        r.section("Enforce")
        ctl = set_mode("enforce")
        r.ok("control.json mode=enforce (overrides: %s)" % json.dumps(ctl.get("cap_overrides")))
        t2 = invoke({"action": "tick", "source": "ops"})
        out["tick_enforce"] = t2
        (r.fail if t2.get("function_error") else r.ok)("tick (enforce): decision=%s stage=%s reason=%s" % (
            t2.get("decision"), t2.get("stage"), t2.get("reason")))
        if t2.get("function_error"):
            failed.append("tick_enforce")
        fz = invoke({"action": "fuse_check"})
        out["fuse"] = fz
        r.log("fuse: %s" % json.dumps({k: fz.get(k) for k in ("checked", "firing", "runaway", "new")}))
        time.sleep(5)
        out["alarms"] = alarms(r)
        out["governor_reserved"] = lam.get_function_concurrency(FunctionName=FN).get("ReservedConcurrentExecutions")
        OUT_JSON.write_text(json.dumps(out, indent=1, default=str))
        r.ok("wrote %s" % OUT_JSON.relative_to(ROOT))
        if failed:
            r.fail("failed: %s" % failed)
            sys.exit(1)
    sys.exit(0)


if __name__ == "__main__":
    main()
