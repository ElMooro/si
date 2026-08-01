"""
ops_4235 — fix justhodl-deal-scanner's UnboundLocalError and silence a
legacy engine that fails 100% of the time.

A. deal-scanner: revenue_and_cap() bound rev_prev/rev_g only inside
   `if isinstance(inc, list) and inc:` but read rev_g unconditionally at
   the end. Any symbol with an empty FMP income-statement response —
   no filing yet, a rate-limited call, an ADR — raised UnboundLocalError
   and took the whole scan down. 222 of 312 runs (71.2%) died on it.
   Root cause is initialisation, not the API. Both names now default to
   None before the branch.

B. ultimate-multi-agent: ImportModuleError "No module named
   lambda_function" on 57 of 57 invocations — the deployment package is
   structurally broken (2025-era orchestrator). It has produced nothing
   for at least 14 days. Its schedules are DISABLED (reversible) rather
   than the function deleted, so the code stays available for salvage.
"""
import io, json, os, time, zipfile
from datetime import datetime, timezone
from pathlib import Path
from urllib.request import urlopen
import boto3
from botocore.config import Config
from ops_report import report

REGION = "us-east-1"
CFG = Config(retries={"max_attempts": 6, "mode": "adaptive"}, read_timeout=120)
lam = boto3.client("lambda", region_name=REGION, config=CFG)
evb = boto3.client("events", region_name=REGION, config=CFG)
sch = boto3.client("scheduler", region_name=REGION, config=CFG)
FN = "justhodl-deal-scanner"
MARK = "ops 4234/4235: rev_prev and rev_g were bound ONLY inside"

def zip_fn(fn):
    src = "aws/lambdas/%s/source" % fn
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        for root, _, files in os.walk(src):
            if "__pycache__" in root: continue
            for f in files:
                fp = os.path.join(root, f)
                z.write(fp, os.path.relpath(fp, src))
        if os.path.isdir("aws/shared"):
            for f in sorted(os.listdir("aws/shared")):
                if f.endswith(".py"):
                    z.write(os.path.join("aws/shared", f), f)
    return buf.getvalue()

def wait_active(fn, budget=180):
    t0 = time.time()
    while time.time() - t0 < budget:
        c = lam.get_function_configuration(FunctionName=fn)
        if c.get("State") == "Active" and c.get("LastUpdateStatus") in (None, "Successful"):
            return True
        time.sleep(4)
    return False

with report("4235_dealscanner_fix") as rep:
    rep.heading("ops 4235 — deal-scanner UnboundLocalError + legacy silence")
    fails = []

    rep.section("A. deploy deal-scanner")
    try:
        wait_active(FN)
        lam.update_function_code(FunctionName=FN, ZipFile=zip_fn(FN))
        ok = False
        for i in range(25):
            time.sleep(6)
            try:
                loc = lam.get_function(FunctionName=FN)["Code"]["Location"]
                src = zipfile.ZipFile(io.BytesIO(urlopen(loc, timeout=60).read())
                                      ).read("lambda_function.py").decode("utf-8", "ignore")
                if MARK in src:
                    ok = True
                    break
            except Exception:
                pass
        if ok:
            rep.ok("settled by marker inside the deployed zip")
        else:
            fails.append("marker never appeared")
    except Exception as e:
        fails.append("deploy: %s" % str(e)[:150])

    rep.section("A2. live probe")
    try:
        wait_active(FN)
        r = lam.invoke(FunctionName=FN, InvocationType="RequestResponse",
                       LogType="Tail")
        fe = r.get("FunctionError")
        body = (r["Payload"].read() or b"")[:400].decode("utf-8", "ignore")
        if fe:
            rep.fail("probe FunctionError=%s %s" % (fe, body[:250]))
            fails.append("probe still failing")
        else:
            rep.ok("probe clean — %s" % body[:200])
    except Exception as e:
        rep.warn("probe: %s" % str(e)[:160])

    rep.section("B. silence ultimate-multi-agent (100% ImportModuleError)")
    n = 0
    try:
        for page in evb.get_paginator("list_rules").paginate():
            for r in page["Rules"]:
                if r.get("State") != "ENABLED":
                    continue
                try:
                    tg = evb.list_targets_by_rule(Rule=r["Name"])
                except Exception:
                    continue
                if any("ultimate-multi-agent" in t.get("Arn", "")
                       for t in tg.get("Targets", [])):
                    evb.disable_rule(Name=r["Name"])
                    rep.ok("  disabled rule %s" % r["Name"])
                    n += 1
        for page in sch.get_paginator("list_schedules").paginate():
            for s_ in page["Schedules"]:
                if s_.get("State") != "ENABLED":
                    continue
                g = s_.get("GroupName", "default")
                d = sch.get_schedule(Name=s_["Name"], GroupName=g)
                if "ultimate-multi-agent" in (d.get("Target", {}).get("Arn") or ""):
                    sch.update_schedule(Name=s_["Name"], GroupName=g,
                                        ScheduleExpression=d["ScheduleExpression"],
                                        FlexibleTimeWindow=d["FlexibleTimeWindow"],
                                        Target=d["Target"], State="DISABLED")
                    rep.ok("  disabled schedule %s" % s_["Name"])
                    n += 1
        rep.log("schedules disabled: %d (function kept for salvage)" % n)
    except Exception as e:
        rep.warn("silence: %s" % str(e)[:150])

    if fails:
        raise SystemExit("FAILS: %s" % "; ".join(fails))
    rep.ok("OPS 4235 PASS")
