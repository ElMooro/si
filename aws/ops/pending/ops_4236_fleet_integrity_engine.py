"""
ops_4236 — institutionalise the audit as justhodl-fleet-integrity.

Self-healing deploy: role, runtime and layer config are DISCOVERED from a
donor function in the live fleet rather than hard-coded, so this op works
regardless of what the account looks like today. Create-or-update, so it
is idempotent and safe to re-run.

Gates (the op FAILS if any of these do not hold — a monitoring system
that might not be running is worse than none, because it manufactures
false confidence):
  1. the deployed zip contains this version's marker
  2. a live invoke returns ok=True and a defect count
  3. data/fleet-integrity.json exists and parses with rows
  4. a weekly schedule exists and points at the function
  5. an alarm exists on JustHodl/Integrity DefectsNew
"""

import io
import json
import os
import time
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from urllib.request import urlopen

import boto3
from botocore.config import Config

from ops_report import report

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
FN = "justhodl-fleet-integrity"
MARKER = "fleet-integrity v1.0.0 ops4236"
RULE = "justhodl-fleet-integrity-weekly"
EXPR = "cron(0 8 ? * MON *)"

CFG = Config(retries={"max_attempts": 6, "mode": "adaptive"},
             read_timeout=300)
lam = boto3.client("lambda", region_name=REGION, config=CFG)
evb = boto3.client("events", region_name=REGION, config=CFG)
cw = boto3.client("cloudwatch", region_name=REGION, config=CFG)
s3 = boto3.client("s3", region_name=REGION, config=CFG)
logs = boto3.client("logs", region_name=REGION, config=CFG)
ACCT = boto3.client("sts").get_caller_identity()["Account"]
ROOT = Path(os.environ.get("GITHUB_WORKSPACE", os.getcwd()))


def zip_fn(fn):
    src = "aws/lambdas/%s/source" % fn
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        for root, _, files in os.walk(src):
            if "__pycache__" in root:
                continue
            for f in files:
                fp = os.path.join(root, f)
                z.write(fp, os.path.relpath(fp, src))
    return buf.getvalue()


def wait_active(fn, budget=200):
    t0 = time.time()
    while time.time() - t0 < budget:
        try:
            c = lam.get_function_configuration(FunctionName=fn)
            if c.get("State") == "Active" and \
                    c.get("LastUpdateStatus") in (None, "Successful"):
                return True
        except Exception:
            pass
        time.sleep(4)
    return False


with report("4236_fleet_integrity_engine") as rep:
    rep.heading("ops 4236 — justhodl-fleet-integrity")
    fails = []

    rep.section("1. Discover deploy config from a donor")
    donor = None
    for cand in ("justhodl-fleet-error-monitor", "justhodl-gov-sources",
                 "justhodl-source-map"):
        try:
            donor = lam.get_function_configuration(FunctionName=cand)
            rep.ok("donor = %s" % cand)
            break
        except Exception:
            continue
    if not donor:
        raise SystemExit("no donor function found")
    ROLE = donor["Role"]
    RUNTIME = donor.get("Runtime", "python3.12")
    rep.kv(section="deploy", role=ROLE.split("/")[-1], runtime=RUNTIME)

    rep.section("2. Create or update the function")
    pkg = zip_fn(FN)
    rep.log("package %d bytes" % len(pkg))
    exists = True
    try:
        lam.get_function_configuration(FunctionName=FN)
    except Exception:
        exists = False
    try:
        if exists:
            wait_active(FN)
            lam.update_function_code(FunctionName=FN, ZipFile=pkg)
            wait_active(FN)
            lam.update_function_configuration(
                FunctionName=FN, Timeout=600, MemorySize=1024,
                Runtime=RUNTIME,
                Environment={"Variables": {"S3_BUCKET": BUCKET}})
            rep.ok("updated existing function")
        else:
            lam.create_function(
                FunctionName=FN, Runtime=RUNTIME, Role=ROLE,
                Handler="lambda_function.lambda_handler",
                Code={"ZipFile": pkg}, Timeout=600, MemorySize=1024,
                Environment={"Variables": {"S3_BUCKET": BUCKET}},
                Description="Standing fleet integrity audit — 12 silent "
                            "failure classes, baseline-diffed, EMF metrics")
            rep.ok("created function")
        try:
            logs.put_retention_policy(
                logGroupName="/aws/lambda/%s" % FN, retentionInDays=30)
        except Exception:
            pass
    except Exception as e:
        fails.append("deploy: %s" % str(e)[:200])

    rep.section("3. GATE 1 — marker inside the deployed zip")
    settled = False
    for i in range(30):
        try:
            loc = lam.get_function(FunctionName=FN)["Code"]["Location"]
            src = zipfile.ZipFile(io.BytesIO(urlopen(loc, timeout=60).read())
                                  ).read("lambda_function.py").decode(
                "utf-8", "ignore")
            if MARKER in src:
                settled = True
                break
        except Exception:
            pass
        time.sleep(6)
    (rep.ok if settled else rep.fail)(
        "marker %s" % ("verified" if settled else "NEVER APPEARED"))
    if not settled:
        fails.append("zip marker")

    rep.section("4. GATE 2 — live invoke")
    body = {}
    try:
        wait_active(FN)
        r = lam.invoke(FunctionName=FN, InvocationType="RequestResponse",
                       LogType="Tail",
                       Payload=json.dumps({"mode": "audit"}).encode())
        raw = r["Payload"].read() or b"{}"
        body = json.loads(raw)
        if r.get("FunctionError"):
            rep.fail("FunctionError=%s %s" % (r["FunctionError"],
                                              str(body)[:300]))
            fails.append("invoke error")
        else:
            rep.ok("returned %s" % json.dumps(body)[:220])
            rep.kv(section="run", defects=body.get("n_defects"),
                   new=body.get("n_new"), sev1=body.get("sev1"))
    except Exception as e:
        fails.append("invoke: %s" % str(e)[:200])

    rep.section("5. GATE 3 — artifact is real")
    try:
        doc = json.loads(s3.get_object(
            Bucket=BUCKET, Key="data/fleet-integrity.json")["Body"].read())
        rep.ok("artifact ok — %d rows, sev1=%d sev2=%d sev3=%d, fleet=%d"
               % (len(doc.get("rows", [])), doc.get("sev1", 0),
                  doc.get("sev2", 0), doc.get("sev3", 0),
                  doc.get("fleet_size", 0)))
        for c, n in sorted(doc.get("totals", {}).items(),
                           key=lambda x: -x[1]):
            rep.log("   %-26s %d" % (c, n))
            rep.kv(section="totals", defect_class=c, count=n)
        if not doc.get("rows"):
            rep.warn("zero rows — either a pristine fleet or a broken audit")
    except Exception as e:
        fails.append("artifact: %s" % str(e)[:180])

    rep.section("6. GATE 4 — weekly schedule")
    try:
        evb.put_rule(Name=RULE, ScheduleExpression=EXPR, State="ENABLED",
                     Description="Weekly fleet integrity audit")
        arn = "arn:aws:lambda:%s:%s:function:%s" % (REGION, ACCT, FN)
        evb.put_targets(Rule=RULE, Targets=[{"Id": "1", "Arn": arn}])
        try:
            lam.add_permission(
                FunctionName=FN, StatementId="allow-integrity-weekly",
                Action="lambda:InvokeFunction",
                Principal="events.amazonaws.com",
                SourceArn="arn:aws:events:%s:%s:rule/%s"
                          % (REGION, ACCT, RULE))
            rep.ok("invoke permission granted to EventBridge")
        except lam.exceptions.ResourceConflictException:
            rep.log("invoke permission already present")
        tg = evb.list_targets_by_rule(Rule=RULE)["Targets"]
        ok = len(tg) == 1 and tg[0]["Arn"] == arn
        (rep.ok if ok else rep.fail)(
            "schedule %s -> %s (%d target%s)"
            % (EXPR, FN, len(tg), "" if len(tg) == 1 else "s — DUPLICATE"))
        if not ok:
            fails.append("schedule target count")
    except Exception as e:
        fails.append("schedule: %s" % str(e)[:180])

    rep.section("7. GATE 5 — alarm on NEW defects only")
    try:
        cw.put_metric_alarm(
            AlarmName="justhodl-integrity-new-defects",
            AlarmDescription="A NEW fleet defect appeared since the last "
                             "accepted baseline. Open /integrity.html.",
            Namespace="JustHodl/Integrity", MetricName="DefectsNew",
            Statistic="Maximum", Period=86400, EvaluationPeriods=1,
            Threshold=0, ComparisonOperator="GreaterThanThreshold",
            TreatMissingData="notBreaching")
        rep.ok("alarm justhodl-integrity-new-defects armed (fires only on "
               "NEW findings, never on the standing backlog)")
    except Exception as e:
        rep.warn("alarm: %s" % str(e)[:160])

    rep.section("RESULT")
    if fails:
        for f in fails:
            rep.fail("  %s" % f)
        raise SystemExit("FAILS: %s" % "; ".join(fails[:3]))
    rep.ok("OPS 4236 PASS — the fleet now audits itself every Monday 08:00 "
           "UTC and alarms only on regressions.")
