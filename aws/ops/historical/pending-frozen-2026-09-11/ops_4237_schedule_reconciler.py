"""
ops_4237 — declarative schedule control.

Step 1 SNAPSHOT. Live AWS is captured into config/schedule-manifest.json.
  Live is a legitimate starting point ONLY because ops 4229-4232 just
  cleaned it: 13 monitor cadences retuned, 26 duplicate schedules
  disabled, 90 double-fire targets removed. Snapshotting a dirty fleet
  would enshrine the mess as desired state, which is the classic way
  these systems get worse instead of better.

  Duplicate targets are dropped during capture so the manifest can never
  declare a double-fire, and DISABLED rules are captured with their
  disabled state so the reconciler does not resurrect them.

Step 2 DEPLOY the reconciler in AUDIT mode. It changes nothing until
  SSM /justhodl/schedules/mode is set to "enforce".

Step 3 PROVE THE SNAPSHOT. The reconciler runs immediately and MUST
  report drift == 0. A non-zero result means the capture is not a
  faithful representation of live, and enforcing an unfaithful manifest
  would break running engines. The op fails rather than shipping that.

Step 4 SCHEDULE it daily, and record the operating doctrine in the
  manifest itself so the next session sees it.
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
FN = "justhodl-schedule-reconciler"
MARKER = "schedule-reconciler v1.0.0 ops4237"
RULE = "justhodl-schedule-reconciler-daily"
EXPR = "cron(30 7 * * ? *)"
MANIFEST_KEY = "config/schedule-manifest.json"

CFG = Config(retries={"max_attempts": 6, "mode": "adaptive"},
             read_timeout=300)
lam = boto3.client("lambda", region_name=REGION, config=CFG)
evb = boto3.client("events", region_name=REGION, config=CFG)
sch = boto3.client("scheduler", region_name=REGION, config=CFG)
s3 = boto3.client("s3", region_name=REGION, config=CFG)
ssm = boto3.client("ssm", region_name=REGION, config=CFG)
logs = boto3.client("logs", region_name=REGION, config=CFG)
ACCT = boto3.client("sts").get_caller_identity()["Account"]
ROOT = Path(os.environ.get("GITHUB_WORKSPACE", os.getcwd()))


def tsig(t):
    return json.dumps({"arn": t.get("arn"), "input": t.get("input"),
                       "path": t.get("path")}, sort_keys=True)


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


with report("4237_schedule_reconciler") as rep:
    rep.heading("ops 4237 — declarative schedule reconciliation")
    fails = []

    # ------------------------------------------------------------ step 1
    rep.section("1. Snapshot live AWS into the authoritative manifest")
    rules, scheds, n_dup_dropped = [], [], 0
    for page in evb.get_paginator("list_rules").paginate():
        for r in page["Rules"]:
            if not r.get("ScheduleExpression"):
                continue
            try:
                tg = evb.list_targets_by_rule(Rule=r["Name"])["Targets"]
            except Exception:
                tg = []
            targets, seen = [], set()
            for t in tg:
                row = {"id": t.get("Id"), "arn": t.get("Arn"),
                       "input": t.get("Input"), "path": t.get("InputPath")}
                s_ = tsig(row)
                if s_ in seen:
                    n_dup_dropped += 1
                    continue
                seen.add(s_)
                targets.append(row)
            rules.append({"kind": "events", "name": r["Name"],
                          "expr": r["ScheduleExpression"].strip(),
                          "state": r.get("State", "ENABLED"),
                          "targets": targets})
    for page in sch.get_paginator("list_schedules").paginate():
        for s_ in page["Schedules"]:
            g = s_.get("GroupName", "default")
            try:
                d = sch.get_schedule(Name=s_["Name"], GroupName=g)
            except Exception:
                continue
            t = d.get("Target", {}) or {}
            scheds.append({"kind": "scheduler", "name": s_["Name"],
                           "group": g,
                           "expr": (d.get("ScheduleExpression") or "").strip(),
                           "state": d.get("State", "ENABLED"),
                           "targets": [{"arn": t.get("Arn"),
                                        "input": t.get("Input"),
                                        "path": None}]})
    n_enabled = (sum(1 for r in rules if r["state"] == "ENABLED")
                 + sum(1 for s_ in scheds if s_["state"] == "ENABLED"))
    rep.log("captured %d EventBridge rules + %d Scheduler schedules "
            "(%d enabled)" % (len(rules), len(scheds), n_enabled))
    rep.log("duplicate targets dropped during capture: %d" % n_dup_dropped)
    rep.kv(section="manifest", rules=len(rules), schedules=len(scheds),
           enabled=n_enabled, dupes_dropped=n_dup_dropped)

    manifest = {
        "version": 1,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source": "ops_4237 snapshot of live AWS after the "
                  "ops 4229-4232 cleanup",
        "doctrine": [
            "This manifest is the SOURCE OF TRUTH for every scheduled "
            "invocation in the account.",
            "To add, change or remove a schedule: edit this file, upload "
            "it to s3://%s/%s, then run the reconciler in enforce mode. "
            "Do NOT call put_rule from ops scripts — that is how 90 "
            "double-fire targets and a five-way duplicate accumulated."
            % (BUCKET, MANIFEST_KEY),
            "Mode lives in SSM /justhodl/schedules/mode (audit|enforce) "
            "and takes effect without a redeploy.",
            "Enforcement DISABLES undeclared rules; it never deletes.",
        ],
        "rules": rules, "schedules": scheds,
    }
    (ROOT / "config").mkdir(exist_ok=True)
    (ROOT / "config" / "schedule-manifest.json").write_text(
        json.dumps(manifest, indent=1), encoding="utf-8")
    s3.put_object(Bucket=BUCKET, Key=MANIFEST_KEY,
                  Body=json.dumps(manifest).encode(),
                  ContentType="application/json")
    rep.ok("manifest written to repo config/ and s3://%s/%s"
           % (BUCKET, MANIFEST_KEY))

    # ------------------------------------------------------------ step 2
    rep.section("2. Deploy the reconciler (AUDIT mode)")
    try:
        ssm.put_parameter(Name="/justhodl/schedules/mode", Value="audit",
                          Type="String", Overwrite=True)
        rep.ok("SSM /justhodl/schedules/mode = audit")
    except Exception as e:
        rep.warn("ssm: %s" % str(e)[:130])

    donor = lam.get_function_configuration(
        FunctionName="justhodl-fleet-error-monitor")
    ROLE, RUNTIME = donor["Role"], donor.get("Runtime", "python3.12")
    pkg = zip_fn(FN)
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
                FunctionName=FN, Timeout=300, MemorySize=512,
                Environment={"Variables": {"S3_BUCKET": BUCKET}})
            rep.ok("updated")
        else:
            lam.create_function(
                FunctionName=FN, Runtime=RUNTIME, Role=ROLE,
                Handler="lambda_function.lambda_handler",
                Code={"ZipFile": pkg}, Timeout=300, MemorySize=512,
                Environment={"Variables": {"S3_BUCKET": BUCKET}},
                Description="Declarative schedule reconciliation — "
                            "manifest is desired state")
            rep.ok("created")
        try:
            logs.put_retention_policy(
                logGroupName="/aws/lambda/%s" % FN, retentionInDays=30)
        except Exception:
            pass
    except Exception as e:
        fails.append("deploy: %s" % str(e)[:180])

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
    (rep.ok if settled else rep.fail)("zip marker %s"
                                      % ("verified" if settled else "MISSING"))
    if not settled:
        fails.append("zip marker")

    # ------------------------------------------------------------ step 3
    rep.section("3. PROVE the snapshot — drift must be exactly 0")
    try:
        wait_active(FN)
        r = lam.invoke(FunctionName=FN, InvocationType="RequestResponse",
                       LogType="Tail",
                       Payload=json.dumps({"mode": "audit"}).encode())
        body = json.loads(r["Payload"].read() or b"{}")
        rep.log("reconciler -> %s" % json.dumps(body)[:300])
        if r.get("FunctionError"):
            fails.append("reconciler invoke error")
        elif body.get("drift_count", -1) != 0:
            rep.fail("DRIFT = %s against a manifest captured seconds ago. "
                     "The snapshot is not faithful; enforcing it could "
                     "break live engines." % body.get("drift_count"))
            for k, v in (body.get("by_class") or {}).items():
                rep.fail("   %s: %s" % (k, v))
            fails.append("snapshot not faithful (drift=%s)"
                         % body.get("drift_count"))
        else:
            rep.ok("drift = 0 — the manifest faithfully describes live AWS")
            rep.kv(section="proof", drift=0, live=body.get("drift_count"),
                   mode=body.get("mode"))
    except Exception as e:
        fails.append("reconciler probe: %s" % str(e)[:180])

    # ------------------------------------------------------------ step 4
    rep.section("4. Schedule the reconciler daily")
    try:
        evb.put_rule(Name=RULE, ScheduleExpression=EXPR, State="ENABLED",
                     Description="Daily schedule drift reconciliation")
        arn = "arn:aws:lambda:%s:%s:function:%s" % (REGION, ACCT, FN)
        evb.put_targets(Rule=RULE, Targets=[{"Id": "1", "Arn": arn}])
        try:
            lam.add_permission(
                FunctionName=FN, StatementId="allow-reconciler-daily",
                Action="lambda:InvokeFunction",
                Principal="events.amazonaws.com",
                SourceArn="arn:aws:events:%s:%s:rule/%s"
                          % (REGION, ACCT, RULE))
        except Exception:
            pass
        tg = evb.list_targets_by_rule(Rule=RULE)["Targets"]
        rep.ok("%s -> %s (%d target)" % (EXPR, FN, len(tg)))
        if len(tg) != 1:
            fails.append("reconciler schedule has %d targets" % len(tg))
        # the reconciler's own rule must be in the manifest it enforces,
        # or it would flag itself as UNDECLARED and disable itself.
        manifest["rules"].append(
            {"kind": "events", "name": RULE, "expr": EXPR,
             "state": "ENABLED",
             "targets": [{"id": "1", "arn": arn, "input": None,
                          "path": None}]})
        manifest["rules"].append(
            {"kind": "events", "name": "justhodl-fleet-integrity-weekly",
             "expr": "cron(0 8 ? * MON *)", "state": "ENABLED",
             "targets": [{"id": "1",
                          "arn": "arn:aws:lambda:%s:%s:function:"
                                 "justhodl-fleet-integrity" % (REGION, ACCT),
                          "input": None, "path": None}]})
        seen, ded = set(), []
        for rr in manifest["rules"]:
            if rr["name"] in seen:
                continue
            seen.add(rr["name"])
            ded.append(rr)
        manifest["rules"] = ded
        (ROOT / "config" / "schedule-manifest.json").write_text(
            json.dumps(manifest, indent=1), encoding="utf-8")
        s3.put_object(Bucket=BUCKET, Key=MANIFEST_KEY,
                      Body=json.dumps(manifest).encode(),
                      ContentType="application/json")
        rep.ok("manifest updated to include the two new control-plane rules")
    except Exception as e:
        fails.append("schedule: %s" % str(e)[:180])

    rep.section("RESULT")
    if fails:
        for f in fails:
            rep.fail("  %s" % f)
        raise SystemExit("FAILS: %s" % "; ".join(fails[:3]))
    rep.ok("OPS 4237 PASS — %d schedules are now declared in git. "
           "Reconciler runs daily in AUDIT mode; flip SSM "
           "/justhodl/schedules/mode to 'enforce' when you want it to "
           "converge automatically." % (len(rules) + len(scheds)))
