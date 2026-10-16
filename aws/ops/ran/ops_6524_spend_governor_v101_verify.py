"""ops 6524 - verify justhodl-spend-governor v1.0.1 live (hardening after ops 6523) and time every action.

v1.0.1: standard (not adaptive) boto retries so an expected throttle can never rate-limit the shared client,
a separate no-retry client for the canary probe, a time-aware concurrency snapshot (Lambda's control plane
is ~15 requests/s account-wide) and threaded drift checks.

FIRST it releases the two false-positive fuse holds ops 6523 created: its forced fuse check looked back 2 hours,
a window that still held the 40 pre-quarantine storm minutes, so it re-throttled justhodl-miss-calibrator and
justhodl-event-coordinator although both were already fixed and idle.  v1.0.1's fuse looks back 1 hour, requires
invocations in the last 20 minutes, and needs two consecutive hot checks 0.8-3 h apart.

Re-asserts everything ops 6523 wired (idempotent): reserved concurrency 1, Budgets SNS subscription,
hourly schedule, heartbeat/error alarms, control.json mode=enforce.  Then times selftest, status,
plan FREEZE (the worst-case discovery: every function's concurrency), and one tick, and fails if any
action comes within 2 minutes of the 900-second Lambda limit.
"""

from datetime import datetime, timezone
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
ROLE_ARN = "arn:aws:iam::%s:role/justhodl-spend-governor-role" % ACCOUNT
GOV_TOPIC_ARN = "arn:aws:sns:%s:%s:justhodl-spend-governor" % (REGION, ACCOUNT)
STATE_BUCKET = "justhodl-ops-private-%s" % ACCOUNT
WANT = 'VERSION = "1.0.1"'
OUT_JSON = ROOT / "aws" / "ops" / "reports" / "6524.json"
lam = boto3.client("lambda", region_name=REGION, config=Config(retries={"max_attempts": 8, "mode": "standard"}))
lam_sync = boto3.client("lambda", region_name=REGION, config=Config(retries={"max_attempts": 0}, read_timeout=910))
out = {"op": 6524, "ran_at": datetime.now(timezone.utc).isoformat(), "timings_s": {}}


def live_version_ok():
    cfg = lam.get_function(FunctionName=FN)
    conf = cfg["Configuration"]
    with urllib.request.urlopen(cfg["Code"]["Location"], timeout=60) as r:
        z = zipfile.ZipFile(io.BytesIO(r.read()))
    core = z.read("governor_core.py").decode()
    levers = z.read("governor_levers.py").decode()
    lf = z.read("lambda_function.py").decode()
    return (conf.get("State") == "Active" and conf.get("LastUpdateStatus") == "Successful" and conf.get("Role") == ROLE_ARN
            and WANT in core and '"deferred"' in levers and '"lambda-probe"' in lf and '"mode": "standard"' in lf)


def invoke(payload, label):
    for i in range(6):
        t = time.time()
        try:
            res = lam_sync.invoke(FunctionName=FN, InvocationType="RequestResponse", Payload=json.dumps(payload).encode())
            body = json.loads(res["Payload"].read() or b"null")
            out["timings_s"][label] = round(time.time() - t, 1)
            return {"function_error": res["FunctionError"], "body": body} if res.get("FunctionError") else body
        except ClientError as e:
            if e.response["Error"]["Code"] == "TooManyRequestsException" and i < 5:
                time.sleep(30)
                continue
            raise


def main():
    with report("ops_6524_spend_governor_v101_verify") as r:
        r.heading("ops 6524 - spend governor v1.0.1 live verification")
        failed = []
        FALSE_POSITIVES = ["justhodl-miss-calibrator", "justhodl-event-coordinator"]
        rel = invoke({"action": "restore", "include_fuse": True, "only_ids": FALSE_POSITIVES,
                      "reason": "ops 6523 fuse false positive (2 h window held pre-quarantine storm)"}, "release_false_positive")
        out["release_false_positive"] = rel
        now_res = {fn: lam.get_function_concurrency(FunctionName=fn).get("ReservedConcurrentExecutions") for fn in FALSE_POSITIVES}
        out["after_release"] = now_res
        ok_rel = all(v is None for v in now_res.values())
        (r.ok if ok_rel else r.fail)("released 6523 fuse false positives: %s -> %s" % (json.dumps(rel)[:300], now_res))
        if not ok_rel:
            failed.append("release")
        deadline = time.time() + 25 * 60
        while time.time() < deadline:
            try:
                if live_version_ok():
                    break
            except ClientError:
                pass
            time.sleep(20)
        else:
            r.fail("v1.0.1 not live by content after 25 min")
            sys.exit(1)
        r.ok("v1.0.1 live by content")

        # idempotent re-assertion of the 6523 wiring
        lam.put_function_concurrency(FunctionName=FN, ReservedConcurrentExecutions=1)
        sns = boto3.client("sns", region_name=REGION)
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
            r.warn("Budgets topic subscription was missing - re-created")
        sched = boto3.client("scheduler", region_name=REGION).get_schedule(Name="justhodl-spend-governor-tick", GroupName="default")
        out["schedule"] = {"expr": sched["ScheduleExpression"], "state": sched["State"]}
        s3 = boto3.client("s3", region_name=REGION)
        ctl = json.loads(s3.get_object(Bucket=STATE_BUCKET, Key="spend-governor/control.json")["Body"].read())
        if ctl.get("mode") != "enforce":
            ctl.update(mode="enforce", mode_set_by="ops 6524", mode_set_at=datetime.now(timezone.utc).isoformat())
            s3.put_object(Bucket=STATE_BUCKET, Key="spend-governor/control.json", Body=json.dumps(ctl, indent=1).encode(),
                          ContentType="application/json", ServerSideEncryption="AES256")
            r.warn("control.json was %s - set to enforce" % ctl.get("mode"))
        alarms = boto3.client("cloudwatch", region_name=REGION).describe_alarms(
            AlarmNames=["justhodl-spend-governor-heartbeat", "justhodl-spend-governor-errors"])["MetricAlarms"]
        out["alarms"] = [(a["AlarmName"], a["StateValue"]) for a in alarms]
        r.log("wiring: reserved=1, schedule %s %s, alarms %s, mode enforce" % (out["schedule"]["expr"], out["schedule"]["state"], out["alarms"]))
        if len(alarms) != 2:
            failed.append("alarms")

        r.section("Timed live actions")
        st = invoke({"action": "selftest"}, "selftest")
        out["selftest"] = st
        (r.ok if st.get("ok") else r.fail)("selftest %.1fs: %s" % (out["timings_s"]["selftest"], json.dumps(st)))
        if not st.get("ok"):
            failed.append("selftest")
        status = invoke({"action": "status"}, "status")
        out["status"] = status
        th = status.get("thresholds") or {}
        r.log("status %.1fs: mode=%s stage=%s cap=$%s (%s) MTD=$%s brake=$%s freeze=$%s baseline=$%s/d residual=$%s/d projected=$%s" % (
            out["timings_s"]["status"], status.get("mode"), status.get("stage"), status.get("cap"), status.get("cap_source"),
            th.get("mtd"), th.get("brake_at"), th.get("freeze_at"), th.get("baseline_usd_day"), th.get("residual_usd_day"),
            th.get("projected_eom")))
        p = invoke({"action": "plan", "stage": "FREEZE"}, "plan_freeze")
        out["plan_freeze"] = {k: p.get(k) for k in ("would_act_on", "exempt", "logs", "function_error", "body")} if isinstance(p, dict) else p
        r.log("plan FREEZE %.1fs: %s" % (out["timings_s"]["plan_freeze"], json.dumps(out["plan_freeze"])[:600]))
        t = invoke({"action": "tick", "source": "ops"}, "tick")
        out["after_tick_false_positives"] = {fn: lam.get_function_concurrency(FunctionName=fn).get("ReservedConcurrentExecutions")
                                             for fn in FALSE_POSITIVES}
        if any(v == 0 for v in out["after_tick_false_positives"].values()):
            failed.append("fuse re-throttled fixed engines")
        out["tick"] = {k: t.get(k) for k in ("decision", "stage", "reason", "fuse", "mode", "ledger_size", "logs", "function_error", "body")} if isinstance(t, dict) else t
        r.log("tick %.1fs: %s" % (out["timings_s"]["tick"], json.dumps(out["tick"])[:700]))
        slow = {k: v for k, v in out["timings_s"].items() if v > 780}
        if slow:
            failed.append("near-timeout %s" % slow)
        for k in ("plan_freeze", "tick"):
            if isinstance(out.get(k), dict) and out[k].get("function_error"):
                failed.append(k)
        OUT_JSON.write_text(json.dumps(out, indent=1, default=str))
        r.ok("timings: %s" % json.dumps(out["timings_s"]))
        if failed:
            r.fail("failed: %s" % failed)
            sys.exit(1)
    sys.exit(0)


if __name__ == "__main__":
    main()
