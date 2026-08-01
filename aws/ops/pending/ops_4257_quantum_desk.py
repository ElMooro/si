"""
ops_4257 -- quantum-desk: bring the PM meta-allocator live and prove it.

The engine (aws/lambdas/justhodl-quantum-desk) joins 12 sibling artifacts
into one blotter: regime consensus, risk-gate sizing, a six-leg asset-
class ladder (discount-below-200dma / asymmetry / strategic ER / regime
playbook / liquidity plumbing / crypto cycle), and a Khalid-fit money
map from best-setups. Deterministic, no LLM dependency (layer is down).

This op does not assume the deploy workflow won the race:
  1. ENSURE the function exists -- create from the committed source
     (+ aws/shared) with role/runtime/env discovered from the
     asset-compass donor if deploy-lambdas.yml hasn't landed it yet;
     otherwise wait for LastUpdateStatus=Successful.
  2. SCHEDULE via EventBridge Scheduler (classic rules SATURATED):
     quantum-desk-sched cron(55 23 * * ? *) -- after cycle-clock 23:30,
     role borrowed from an existing default-group schedule.
  3. RUN it sync (botocore read_timeout set) and require ok=True.
  4. VERIFY the artifact from S3: fresh generated_at, ladder non-empty
     OR an explicit abstain, data_health present, and print the actual
     regime / best class / top names into this report -- measured, not
     hoped.
  5. Edge-check the page (warn-only; CDN lag is not a failure).
"""
import io, json, os, sys, time, zipfile
from datetime import datetime, timezone
from pathlib import Path
from urllib.request import Request, urlopen

import boto3
from botocore.config import Config
from ops_report import report

REGION, BUCKET = "us-east-1", "justhodl-dashboard-live"
FN = "justhodl-quantum-desk"
DONOR = "justhodl-asset-compass"
ROOT = Path(__file__).resolve().parents[3]
SRC = ROOT / "aws" / "lambdas" / FN / "source"
SHARED = ROOT / "aws" / "shared"

lam = boto3.client("lambda", region_name=REGION,
                   config=Config(read_timeout=200, retries={"max_attempts": 2}))
s3 = boto3.client("s3", region_name=REGION)
sched = boto3.client("scheduler", region_name=REGION)

def build_zip():
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        for p in SRC.glob("*.py"):
            z.write(p, p.name)
        if SHARED.exists():
            for p in SHARED.glob("*.py"):
                z.write(p, p.name)
    return buf.getvalue()

def wait_ready(r, tries=40):
    for _ in range(tries):
        c = lam.get_function_configuration(FunctionName=FN)
        if c.get("State") == "Active" and \
           c.get("LastUpdateStatus") in (None, "Successful"):
            return c
        time.sleep(3)
    raise RuntimeError("function never settled: %s/%s"
                       % (c.get("State"), c.get("LastUpdateStatus")))

fails = []
with report("4257_quantum_desk") as r:
    r.heading("ops 4257 -- quantum-desk (PM meta-allocator) live + proven")

    r.section("1. ensure function")
    try:
        lam.get_function_configuration(FunctionName=FN)
        r.ok("function exists (deploy-lambdas.yml won the race)")
        wait_ready(r)
    except lam.exceptions.ResourceNotFoundException:
        d = lam.get_function_configuration(FunctionName=DONOR)
        env = (d.get("Environment") or {}).get("Variables") or {}
        env = {k: v for k, v in env.items()
               if k in ("S3_BUCKET",) or k.endswith("_KEY")}
        lam.create_function(
            FunctionName=FN, Runtime=d["Runtime"], Role=d["Role"],
            Handler="lambda_function.lambda_handler",
            Code={"ZipFile": build_zip()}, Timeout=120, MemorySize=512,
            Environment={"Variables": env},
            Description="Quantum Desk -- cross-engine PM meta-allocator "
                        "(ops 4257)")
        r.ok("created from committed source (donor role/runtime: %s)"
             % d["Runtime"])
        wait_ready(r)

    r.section("2. schedule (EventBridge Scheduler)")
    try:
        donor_role = None
        for pg in sched.get_paginator("list_schedules").paginate(
                GroupName="default"):
            for srow in pg.get("Schedules", []):
                try:
                    g = sched.get_schedule(Name=srow["Name"],
                                           GroupName="default")
                    donor_role = g["Target"]["RoleArn"]
                    break
                except Exception:
                    continue
            if donor_role:
                break
        arn = lam.get_function_configuration(FunctionName=FN)["FunctionArn"]
        body = dict(Name="quantum-desk-sched", GroupName="default",
                    ScheduleExpression="cron(55 23 * * ? *)",
                    FlexibleTimeWindow={"Mode": "OFF"},
                    State="ENABLED",
                    Target={"Arn": arn, "RoleArn": donor_role},
                    Description="Quantum Desk daily 23:55 UTC "
                                "(after cycle-clock 23:30)")
        try:
            sched.create_schedule(**body)
            r.ok("schedule created: cron(55 23 * * ? *)")
        except sched.exceptions.ConflictException:
            sched.update_schedule(**body)
            r.ok("schedule updated: cron(55 23 * * ? *)")
    except Exception as e:
        fails.append("schedule: %s" % str(e)[:150])
        r.fail("schedule: %s" % str(e)[:150])

    r.section("3. first run (sync)")
    try:
        resp = lam.invoke(FunctionName=FN, InvocationType="RequestResponse",
                          Payload=b"{}")
        body = json.loads(resp["Payload"].read() or b"{}")
        if resp.get("FunctionError") or not body.get("ok"):
            fails.append("invoke error: %s" % json.dumps(body)[:300])
            r.fail("invoke: %s" % json.dumps(body)[:300])
        else:
            r.ok("ran: regime=%s sources_ok=%s ladder=%s map=%s best=%s"
                 % (body.get("regime"), body.get("sources_ok"),
                    body.get("ladder"), body.get("money_map"),
                    body.get("best_class")))
    except Exception as e:
        fails.append("invoke: %s" % str(e)[:150])
        r.fail("invoke: %s" % str(e)[:150])

    r.section("4. artifact verification (measured, not hoped)")
    try:
        doc = json.loads(s3.get_object(
            Bucket=BUCKET, Key="data/quantum-desk.json")["Body"].read())
        gen = doc.get("generated_at", "")
        age_s = (datetime.now(timezone.utc)
                 - datetime.fromisoformat(gen)).total_seconds()
        assert age_s < 600, "artifact stale: %ss old" % int(age_s)
        for k in ("regime", "risk_gate", "asset_ladder", "money_map",
                  "data_health", "doctrine"):
            assert k in doc, "missing key %s" % k
        dh = doc["data_health"]
        r.ok("artifact fresh (%ds) v%s -- sources %s/%s live"
             % (age_s, doc.get("version"), dh.get("sources_ok"),
                dh.get("sources_total")))
        for nm, h in sorted(dh.get("detail", {}).items()):
            (r.ok if h["status"] == "ok" else r.warn)(
                "  %-16s %-8s age=%sh" % (nm, h["status"], h.get("age_h")))
        reg = doc["regime"]
        r.log("REGIME: %s (votes: %s)%s"
              % (reg.get("regime"),
                 ", ".join("%(source)s=%(regime)s" % v
                           for v in reg.get("votes", [])) or "none",
                 " -- SPLIT" if reg.get("disagreement") else ""))
        rk = doc["risk_gate"]
        r.log("RISK-GATE: posture=%s composite=%s sizing=x%s"
              % (rk.get("posture"), rk.get("composite"),
                 rk.get("sizing_multiplier")))
        lad = doc["asset_ladder"]
        if not lad:
            r.warn("ladder ABSTAINED (asset-compass unreadable) -- "
                   "honest empty, page shows abstain box")
        for row in lad[:6]:
            r.row(cls=row["class"], score=row["score"],
                  verdict=row["verdict"],
                  legs=",".join(row.get("legs_used", [])))
        mm = doc["money_map"]
        if not mm:
            r.warn("money map: %s" % doc.get("money_map_note"))
        for m in mm[:6]:
            r.row(ticker=m["ticker"], fit=m["khalid_fit"],
                  cls=m["class"], quadrant=m.get("flow_quadrant"),
                  size_x=m.get("size_hint_x"))
        if lad:
            top = lad[0]
            need = 8
            if dh.get("sources_ok", 0) < need:
                r.warn("only %s/12 sources live -- ladder stands but "
                       "flagged" % dh.get("sources_ok"))
            r.ok("BEST CLASS NOW: %s (score %s, %s)"
                 % (top["class"], top["score"], top["verdict"]))
    except AssertionError as e:
        fails.append("artifact: %s" % e)
        r.fail("artifact: %s" % e)
    except Exception as e:
        fails.append("artifact: %s" % str(e)[:200])
        r.fail("artifact: %s" % str(e)[:200])

    r.section("5. page edge check (warn-only, CDN lag tolerated)")
    live = False
    for i in range(3):
        try:
            html = urlopen(Request(
                "https://justhodl.ai/quantum-desk.html",
                headers={"User-Agent": "jh-ops-4257",
                         "Cache-Control": "no-cache"}),
                timeout=20).read().decode("utf-8", "replace")
            if "Quantum Desk" in html and "quantum-desk.json" in html:
                live = True
                break
        except Exception:
            pass
        time.sleep(25)
    (r.ok if live else r.warn)(
        "page %s" % ("LIVE on edge" if live else
                     "not on edge yet -- pages.yml/CDN lag; same push "
                     "carries it, recheck next session"))

    r.section("RESULT")
    if fails:
        for f in fails:
            r.fail("  %s" % f)
    else:
        r.ok("OPS 4257 PASS -- quantum-desk live, scheduled 23:55 UTC, "
             "artifact verified with real fleet data")

if fails:
    sys.exit(1)
