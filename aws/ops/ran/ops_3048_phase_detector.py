#!/usr/bin/env python3
"""ops 3048 (env fix + verify) -- PHASE DETECTOR (Khalid): dated BEGIN/END of accumulation
and distribution per stock. Research-grounded (Wyckoff schematic +
climax volume signatures + SOS/SOW completion events, cited on-page);
new engine justhodl-phase-detector (~700-name Polygon universe, 2y daily
bars, state machine: trend gate -> climax -> range -> pressure ->
spring/UT events -> volume-confirmed phase END); page phases.html with
6 boards + searchable table + method citations. Chain: create fn ->
Event invoke + poll (long: 700 aggs fetches + segmentation) -> assert
segmentation credibility -> daily schedule -> page check."""
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
                   config=Config(read_timeout=920,
                                 retries={"max_attempts": 0}))
SCH = boto3.client("scheduler", region_name=REG)
S3 = boto3.client("s3", region_name=REG)
BUCKET = "justhodl-dashboard-live"
FN = "justhodl-phase-detector"
ROLE = "arn:aws:iam::857687956942:role/lambda-execution-role"
SCHED_ROLE = "arn:aws:iam::857687956942:role/justhodl-scheduler-role"
AWS_DIR = Path(__file__).resolve().parents[2]
SRC = AWS_DIR / "lambdas" / FN / "source" / "lambda_function.py"


def s3_json(key):
    return json.loads(S3.get_object(Bucket=BUCKET, Key=key)["Body"].read())


def _touched_this_push():
    """True if this push's diff modified the engine source (then we must
    wait for a POST-push deploy; else any settled deploy is fine).
    3046 lesson: an unconditional age<15min gate can never pass on a
    [skip-deploy] retry of an already-deployed fn."""
    import subprocess
    try:
        out = subprocess.run(
            ["git", "diff", "--name-only", "HEAD^", "HEAD"],
            capture_output=True, text=True, timeout=20,
            cwd=str(AWS_DIR.parent)).stdout
        return ("aws/lambdas/%s/" % FN) in out
    except Exception:
        return False


def ensure_fn(rep):
    t0 = datetime.now(timezone.utc)
    need_fresh = _touched_this_push()
    rep.kv(need_fresh_deploy=need_fresh)
    try:
        LAM.get_function_configuration(FunctionName=FN)
        rep.kv(fn_exists=True)
        for _ in range(24):
            c = LAM.get_function_configuration(FunctionName=FN)
            lm = datetime.fromisoformat(
                c["LastModified"].replace("+0000", "+00:00"))
            settled = (c.get("LastUpdateStatus") in (None, "Successful")
                       and c.get("State") in (None, "Active"))
            fresh_ok = (not need_fresh) or \
                lm >= t0 - __import__("datetime").timedelta(seconds=90)
            if settled and fresh_ok:
                time.sleep(8)
                rep.kv(code_age_min=round((datetime.now(timezone.utc)
                                           - lm).total_seconds() / 60.0,
                                          1))
                return True
            time.sleep(20)
        return False
    except LAM.exceptions.ResourceNotFoundException:
        rep.kv(fn_exists=False, action="boto3 create_function")
    donor = LAM.get_function_configuration(
        FunctionName="justhodl-confluence-meta")
    env = donor.get("Environment", {}).get("Variables", {}) or {}
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        z.writestr("lambda_function.py", SRC.read_text())
    buf.seek(0)
    try:
        LAM.create_function(
            FunctionName=FN, Runtime="python3.12", Role=ROLE,
            Handler="lambda_function.lambda_handler",
            Code={"ZipFile": buf.read()}, Timeout=900, MemorySize=2048,
            Description="A/D phase segmentation with dated BEGIN/END "
                        "(Wyckoff-grounded).",
            Environment={"Variables": env})
    except LAM.exceptions.ResourceConflictException:
        # deploy-lambdas create-branch won the race (3044 lesson):
        # fn exists now -- wait for it to settle instead of failing
        rep.kv(create_race="deploy-lambdas won; waiting settled")
        for _ in range(24):
            c = LAM.get_function_configuration(FunctionName=FN)
            if c.get("LastUpdateStatus") in (None, "Successful") and \
                    c.get("State") in (None, "Active"):
                time.sleep(8)
                return True
            time.sleep(15)
        return False
    time.sleep(12)
    return True


def main():
    fails, warns = [], []
    with report("3048_phase_detector") as rep:
        rep.section("1. Ensure engine")
        if not ensure_fn(rep):
            fails.append("fn not ready")
            _fin(rep, fails, warns, {})
            sys.exit(1)

        rep.section("1b. Repair env (deploy-lambdas created fn "
                    "WITHOUT donor env -- 3047 evidence: POLYGON key "
                    "missing, 3ms crash)")
        cfg = LAM.get_function_configuration(FunctionName=FN)
        env = (cfg.get("Environment") or {}).get("Variables") or {}
        if not any("POLYGON" in k for k in env):
            donor = LAM.get_function_configuration(
                FunctionName="justhodl-confluence-meta")
            denv = (donor.get("Environment") or {}).get("Variables") \
                or {}
            env.update(denv)
            poly_src = next((k for k in env if "POLYGON" in k), None)
            if poly_src and "POLYGON_KEY" not in env:
                env["POLYGON_KEY"] = env[poly_src]
            LAM.update_function_configuration(
                FunctionName=FN, Environment={"Variables": env})
            for _ in range(20):
                c = LAM.get_function_configuration(FunctionName=FN)
                if c.get("LastUpdateStatus") in (None, "Successful"):
                    break
                time.sleep(6)
            time.sleep(6)
            rep.kv(env_copied=len(denv),
                   polygon_vars=json.dumps(
                       [k for k in env if "POLYGON" in k]))
        else:
            rep.kv(env_ok=json.dumps(
                [k for k in env if "POLYGON" in k]))

        rep.section("2. Segment the market (SYNC + log tail)")
        import base64
        r = LAM.invoke(FunctionName=FN, InvocationType="RequestResponse",
                       LogType="Tail", Payload=b"{}")
        tail = base64.b64decode(r.get("LogResult", "")).decode(
            "utf-8", "replace")
        rep.log("engine log tail:\n" + tail[-2400:])
        if r.get("FunctionError"):
            fails.append("engine FunctionError: %s" %
                         r["Payload"].read()[:300].decode("utf-8",
                                                          "replace"))
            _fin(rep, fails, warns, {})
            sys.exit(1)
        d = None
        try:
            d = s3_json("data/phase-detector.json")
        except Exception:
            pass
        if not d:
            fails.append("sync run OK but no output JSON")
            _fin(rep, fails, warns, {})
            sys.exit(1)

        pc = d.get("phase_counts") or {}
        tk = d.get("tickers") or {}
        boards = d.get("boards") or {}
        rep.kv(universe=d.get("universe_n"),
               analyzed=d.get("analyzed_n"),
               phase_counts=json.dumps(pc),
               boards_sizes=json.dumps({k: len(v) for k, v in
                                        boards.items()}),
               duration_s=d.get("duration_s"))
        if (d.get("analyzed_n") or 0) < 450:
            fails.append("analyzed=%s (<450)" % d.get("analyzed_n"))
        for ph in ("ACCUMULATION", "DISTRIBUTION", "MARKUP", "MARKDOWN"):
            if not pc.get(ph):
                warns.append("no names in phase %s" % ph)
        if (pc.get("ACCUMULATION", 0) + pc.get("DISTRIBUTION", 0)) < 20:
            fails.append("ranged phases too thin: %s" % pc)
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        bad_dates = sane = with_events = with_hist = 0
        mega = {}
        for t, v in tk.items():
            b = v.get("begin")
            if v.get("phase") not in ("NEUTRAL",) and b:
                sane += 1
                if not ("2024-01-01" <= b <= today):
                    bad_dates += 1
                if v.get("events"):
                    with_events += 1
            for h in v.get("history") or []:
                with_hist += 1
                if h.get("end") and h.get("begin") and \
                        h["end"] < h["begin"]:
                    fails.append("%s history end<begin" % t)
                    break
            if t in ("AAPL", "MSFT", "NVDA", "GOOGL", "AMZN"):
                mega[t] = {"phase": v.get("phase"),
                           "begin": v.get("begin"),
                           "days": v.get("days_in_phase")}
        rep.kv(dated_names=sane, bad_dates=bad_dates,
               with_events=with_events, completed_segments=with_hist,
               mega=json.dumps(mega))
        if bad_dates:
            fails.append("%d begin dates out of range" % bad_dates)
        if with_hist < 100:
            warns.append("few completed segments: %d" % with_hist)
        acc_b = boards.get("accumulation_beginning") or []
        if acc_b and not all((r.get("pressure") or 0) > 0
                             for r in acc_b):
            fails.append("accumulation_beginning has non-positive "
                         "pressure rows")

        rep.section("3. Daily schedule")
        try:
            SCH.get_schedule(Name="justhodl-phase-detector-daily")
            rep.kv(schedule="exists")
        except Exception:
            SCH.create_schedule(
                Name="justhodl-phase-detector-daily",
                ScheduleExpression="cron(10 22 * * ? *)",
                FlexibleTimeWindow={"Mode": "OFF"},
                Target={"Arn": LAM.get_function_configuration(
                    FunctionName=FN)["FunctionArn"],
                    "RoleArn": SCHED_ROLE},
                Description="Daily A/D phase segmentation post-close")
            rep.kv(schedule="created cron(10 22 * * ? *)")

        rep.section("4. Live page (warn-level)")
        try:
            import urllib.request
            req = urllib.request.Request(
                "https://justhodl.ai/phases.html?v=%d" % time.time(),
                headers={"User-Agent": "Mozilla/5.0 ops-3048"})
            page = urllib.request.urlopen(req, timeout=25).read().decode(
                "utf-8", "replace")
            ok = "PHASE MAP" in page
            rep.kv(page_live=ok)
            if not ok:
                warns.append("pages not propagated yet")
        except Exception as e:
            warns.append("page: %s" % str(e)[:90])

        rep.section("verdict")
        _fin(rep, fails, warns,
             {"analyzed": d.get("analyzed_n"), "phase_counts": pc})
        if fails:
            for f in fails:
                rep.log("FAIL: %s" % f)
            sys.exit(1)
        rep.log("PASS -- %s analyzed, counts %s"
                % (d.get("analyzed_n"), pc))


def _fin(rep, fails, warns, extra):
    payload = {"ops": 3048, "fails": fails, "warns": warns,
               "verdict": "FAIL" if fails else "PASS",
               "ts": datetime.now(timezone.utc).isoformat()}
    payload.update(extra)
    (AWS_DIR / "ops" / "reports" / "3048.json").write_text(
        json.dumps(payload, indent=1))
    rep.kv(verdict=payload["verdict"], n_fails=len(fails),
           n_warns=len(warns))


main()
sys.exit(0)
