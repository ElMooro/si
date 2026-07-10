#!/usr/bin/env python3
"""ops 3032 -- earned FINAL. 3031 hit the same gate-class bug one section
earlier (ensure_fn age-gated an untouched fn); settled-only now.
Weights re-run not needed -- the 3030 file (11 LEARNED) is read from
S3 and asserted into the warroom earned view.

Prior: ops 3031 -- earned-view refresh. 3030 completed probes (all 4 DBnomics
IMF reserve ids LIVE; all Truflation free endpoints 404 -> M21 parked
DATA-GATED with evidence), grid v3.1 (Swiss 15.31/EA 26.3 LIVE),
weights v2 (11 LEARNED, CISS x1.323 best); only the section-3 gate
wrongly demanded a <12-min-old warroom on a push that did not touch
it. Settled-only gate here; asserts new weights flowed into the
earned view.

Prior: ops 3030 -- queue sweep (Khalid): weights v2 (CISS points-field fix,
CFTC socrata full-history crowding proxy ES/ZN/GC/CL, factor IWF/IWD+
IWM/SPY appetite) -> expect >=10 LEARNED; grid v3.1 Swiss+EA reserve
legs via DBnomics IMF candidates (SOFT until probe); probes for
DBnomics ids + Truflation free endpoints (M21, evidence before build);
mock-faithful visual + live methodology page.

Prior: ops 3029 -- earned weights CLOSE. 3028: plumbing-history indicators is a DICT keyed by id (its writer confirms), engine iterated it as a list of dicts -> str.get crash. Reader now handles both shapes.

Prior: ops 3028 -- EARNED WEIGHTS arc (Khalid go). Chain:
1. ensure justhodl-warroom-weights exists (deploy-lambdas create-branch is
   INTERMITTENT per doctrine -- verify get_function, fallback boto3
   create_function from this checkout, env copied from donor
   justhodl-confluence-meta);
2. sync-invoke it (event study ~1-3 min) -> assert data/warroom-weights
   .json: 12 mechanisms, >=6 LEARNED, all weights in [0.6,1.6], shrinkage
   evident (no weight pinned at clamp for every learned mech);
3. wait warroom fresh+settled -> invoke -> assert views.earned present,
   score numeric, differs from per_mechanism only via weights;
4. monthly EventBridge Scheduler schedule (classic rule cap SATURATED per
   doctrine) -- 1st of month 07:10 UTC;
5. live page check (warn-level on CDN lag)."""
import io
import json
import sys
import time
import zipfile
from datetime import datetime, timezone
from pathlib import Path

import boto3
from botocore.config import Config
from ops_report import report

REG = "us-east-1"
LAM = boto3.client("lambda", region_name=REG,
                   config=Config(read_timeout=420,
                                 retries={"max_attempts": 0}))
SCH = boto3.client("scheduler", region_name=REG)
S3 = boto3.client("s3", region_name=REG)
BUCKET = "justhodl-dashboard-live"
FN = "justhodl-warroom-weights"
ROLE = "arn:aws:iam::857687956942:role/lambda-execution-role"
SCHED_ROLE = "arn:aws:iam::857687956942:role/justhodl-scheduler-role"
AWS_DIR = Path(__file__).resolve().parents[2]
SRC = AWS_DIR / "lambdas" / FN / "source" / "lambda_function.py"


def s3_json(key):
    return json.loads(S3.get_object(Bucket=BUCKET, Key=key)["Body"].read())


def wait_settled(fn, max_min=8):
    for _ in range(int(max_min * 3)):
        try:
            c = LAM.get_function_configuration(FunctionName=fn)
            lm = datetime.fromisoformat(
                c["LastModified"].replace("+0000", "+00:00"))
            age = (datetime.now(timezone.utc) - lm).total_seconds() / 60.0
            if (age < 12 and c.get("LastUpdateStatus") in
                    (None, "Successful") and c.get("State") in
                    (None, "Active")):
                time.sleep(8)
                return age
        except Exception:
            pass
        time.sleep(20)
    return None


def ensure_fn(rep):
    try:
        c = LAM.get_function_configuration(FunctionName=FN)
        rep.kv(fn_exists=True, update_status=c.get("LastUpdateStatus"))
        return c.get("LastUpdateStatus") in (None, "Successful")
    except LAM.exceptions.ResourceNotFoundException:
        rep.kv(fn_exists=False, action="boto3 create_function fallback")
    donor = LAM.get_function_configuration(
        FunctionName="justhodl-confluence-meta")
    env = donor.get("Environment", {}).get("Variables", {}) or {}
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        z.writestr("lambda_function.py", SRC.read_text())
    buf.seek(0)
    LAM.create_function(
        FunctionName=FN, Runtime="python3.12", Role=ROLE,
        Handler="lambda_function.lambda_handler",
        Code={"ZipFile": buf.read()}, Timeout=600, MemorySize=1024,
        Description="Earned per-mechanism barometer weights (event-study "
                    "vs 8 crisis windows, shrunk to equal).",
        Environment={"Variables": env})
    return wait_settled(FN, 4) is not None


def main():
    fails, warns = [], []
    with report("3032_earned_final") as rep:
        rep.section("1. Ensure engine exists + settled")
        if not ensure_fn(rep):
            fails.append("warroom-weights fn not ready")
            _fin(rep, fails, warns, {})
            sys.exit(1)

        rep.section("2. Weights file (from 3030 run)")
        wj = s3_json("data/warroom-weights.json")
        mechs = wj.get("mechanisms") or {}
        learned = {k: v for k, v in mechs.items()
                   if v.get("status") == "LEARNED"}
        rep.kv(n_learned=len(learned),
               weights_asof=wj.get("generated_at"))
        if len(learned) < 11:
            fails.append("weights file learned=%d (<11)" % len(learned))

        rep.section("3. Warroom earned view")
        try:
            c = LAM.get_function_configuration(
                FunctionName="justhodl-canary-warroom")
            if c.get("LastUpdateStatus") not in (None, "Successful"):
                fails.append("warroom mid-update")
        except Exception as e:
            fails.append("warroom config: %s" % str(e)[:80])
        r = LAM.invoke(FunctionName="justhodl-canary-warroom",
                       InvocationType="RequestResponse", Payload=b"{}")
        rep.kv(warroom_invoke=json.dumps(
            json.loads(r["Payload"].read() or b"{}"))[:200])
        d = s3_json("data/canary-warroom.json")
        V = (d.get("barometer") or {}).get("views") or {}
        ew, pmv = V.get("earned") or {}, V.get("per_mechanism") or {}
        rep.kv(earned_score=ew.get("score"), earned_band5=ew.get("band5"),
               pm_score=pmv.get("score"),
               weights_asof=ew.get("weights_asof"),
               weighted_mechs=json.dumps({k: v.get("weight") for k, v in
                                          (pmv.get("by_mechanism") or
                                           {}).items()}))
        if ew.get("score") is None:
            fails.append("earned view missing")
        if ew.get("weights_asof") is None:
            fails.append("earned view not reading weights file")
        if (ew.get("n_learned") or 0) < 11:
            fails.append("earned view n_learned=%s (<11 -- stale weights)"
                         % ew.get("n_learned"))

        rep.section("4. Monthly schedule (EventBridge Scheduler)")
        try:
            SCH.get_schedule(Name="justhodl-warroom-weights-monthly")
            rep.kv(schedule="exists")
        except Exception:
            SCH.create_schedule(
                Name="justhodl-warroom-weights-monthly",
                ScheduleExpression="cron(10 7 1 * ? *)",
                FlexibleTimeWindow={"Mode": "OFF"},
                Target={"Arn": LAM.get_function_configuration(
                    FunctionName=FN)["FunctionArn"],
                    "RoleArn": SCHED_ROLE},
                Description="Monthly earned-weights event study")
            rep.kv(schedule="created cron(10 7 1 * ? *)")

        rep.section("5. Live page (warn-level)")
        try:
            import urllib.request
            req = urllib.request.Request(
                "https://justhodl.ai/canaries.html?v=%d" % time.time(),
                headers={"User-Agent": "Mozilla/5.0 ops-3028"})
            page = urllib.request.urlopen(req, timeout=25).read().decode(
                "utf-8", "replace")
            ok = "HOW THE BAROMETER WAS BUILT" in page
            rep.kv(page_earned_toggle=ok)
            if not ok:
                warns.append("page not propagated yet")
        except Exception as e:
            warns.append("page check: %s" % str(e)[:100])

        rep.section("verdict")
        _fin(rep, fails, warns, {"earned": ew.get("score"),
                                 "per_mechanism": pmv.get("score"),
                                 "n_learned": len(learned)})
        if fails:
            for f in fails:
                rep.log("FAIL: %s" % f)
            sys.exit(1)
        rep.log("PASS -- earned %s vs per-mech %s; %d learned"
                % (ew.get("score"), pmv.get("score"), len(learned)))


def _fin(rep, fails, warns, extra):
    payload = {"ops": 3032, "fails": fails, "warns": warns,
               "verdict": "FAIL" if fails else "PASS",
               "ts": datetime.now(timezone.utc).isoformat()}
    payload.update(extra)
    (AWS_DIR / "ops" / "reports" / "3032.json").write_text(
        json.dumps(payload, indent=1))
    rep.kv(verdict=payload["verdict"], n_fails=len(fails),
           n_warns=len(warns))


main()
sys.exit(0)
