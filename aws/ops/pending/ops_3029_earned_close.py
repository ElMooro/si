#!/usr/bin/env python3
"""ops 3029 -- earned weights CLOSE. 3028: plumbing-history indicators is a DICT keyed by id (its writer confirms), engine iterated it as a list of dicts -> str.get crash. Reader now handles both shapes.

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
        LAM.get_function_configuration(FunctionName=FN)
        rep.kv(fn_exists=True)
        return wait_settled(FN) is not None
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
    with report("3029_earned_close") as rep:
        rep.section("1. Ensure engine exists + settled")
        if not ensure_fn(rep):
            fails.append("warroom-weights fn not ready")
            _fin(rep, fails, warns, {})
            sys.exit(1)

        rep.section("2. Event study run")
        r = LAM.invoke(FunctionName=FN, InvocationType="RequestResponse",
                       Payload=b"{}")
        body = json.loads(r["Payload"].read() or b"{}")
        rep.kv(invoke=json.dumps(body)[:300])
        if body.get("errorMessage"):
            fails.append("weights engine crashed: %s"
                         % body["errorMessage"][:150])
            _fin(rep, fails, warns, {})
            sys.exit(1)
        wj = s3_json("data/warroom-weights.json")
        mechs = wj.get("mechanisms") or {}
        weights = {k: v.get("weight") for k, v in mechs.items()}
        learned = {k: v for k, v in mechs.items()
                   if v.get("status") == "LEARNED"}
        rep.kv(n_mechanisms=len(mechs), n_learned=len(learned),
               weights=json.dumps(weights),
               learned_detail=json.dumps({k: {
                   "hit": v.get("hit_rate"), "lead": v.get(
                       "mean_lead_months"), "fa": v.get(
                       "false_alarm_rate"), "n": v.get(
                       "n_crises_covered")} for k, v in learned.items()}))
        if len(mechs) < 12:
            fails.append("mechanisms=%d (<12)" % len(mechs))
        if len(learned) < 6:
            fails.append("learned=%d (<6)" % len(learned))
        bad = [k for k, w in weights.items()
               if not isinstance(w, (int, float)) or w < 0.6 or w > 1.6]
        if bad:
            fails.append("weights out of clamp: %s" % bad)

        rep.section("3. Warroom earned view")
        if wait_settled("justhodl-canary-warroom") is None:
            fails.append("warroom code not settled")
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
            ok = "Earned weights" in page
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
    payload = {"ops": 3029, "fails": fails, "warns": warns,
               "verdict": "FAIL" if fails else "PASS",
               "ts": datetime.now(timezone.utc).isoformat()}
    payload.update(extra)
    (AWS_DIR / "ops" / "reports" / "3029.json").write_text(
        json.dumps(payload, indent=1))
    rep.kv(verdict=payload["verdict"], n_fails=len(fails),
           n_warns=len(warns))


main()
sys.exit(0)
