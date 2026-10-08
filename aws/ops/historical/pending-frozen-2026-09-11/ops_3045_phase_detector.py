#!/usr/bin/env python3
"""ops 3045 (retry of 3044) -- PHASE DETECTOR (Khalid): dated BEGIN/END of accumulation
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
                   config=Config(read_timeout=180,
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


def ensure_fn(rep):
    try:
        LAM.get_function_configuration(FunctionName=FN)
        rep.kv(fn_exists=True)
        for _ in range(24):
            c = LAM.get_function_configuration(FunctionName=FN)
            lm = datetime.fromisoformat(
                c["LastModified"].replace("+0000", "+00:00"))
            age = (datetime.now(timezone.utc)
                   - lm).total_seconds() / 60.0
            if age < 15 and c.get("LastUpdateStatus") in (None,
                                                          "Successful"):
                time.sleep(8)
                rep.kv(code_age_min=round(age, 1))
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
    with report("3045_phase_detector") as rep:
        rep.section("1. Ensure engine")
        if not ensure_fn(rep):
            fails.append("fn not ready")
            _fin(rep, fails, warns, {})
            sys.exit(1)

        rep.section("2. Segment the market (Event + poll, long)")
        prev = ""
        try:
            prev = s3_json("data/phase-detector.json").get(
                "generated_at", "")
        except Exception:
            pass
        LAM.invoke(FunctionName=FN, InvocationType="Event", Payload=b"{}")
        d = None
        for _ in range(45):
            time.sleep(20)
            try:
                cand = s3_json("data/phase-detector.json")
                if cand.get("generated_at", "") > prev:
                    d = cand
                    break
            except Exception:
                continue
        if not d:
            fails.append("no fresh phase-detector.json after 15min")
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
                headers={"User-Agent": "Mozilla/5.0 ops-3045"})
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
    payload = {"ops": 3045, "fails": fails, "warns": warns,
               "verdict": "FAIL" if fails else "PASS",
               "ts": datetime.now(timezone.utc).isoformat()}
    payload.update(extra)
    (AWS_DIR / "ops" / "reports" / "3045.json").write_text(
        json.dumps(payload, indent=1))
    rep.kv(verdict=payload["verdict"], n_fails=len(fails),
           n_warns=len(warns))


main()
sys.exit(0)
