#!/usr/bin/env python3
"""ops 3040 -- WHALES engine + page (Khalid). Audit found 5 smart-money
engines; the missing piece was per-STOCK $ flows (latest-vs-prior 13F
diff, NEW/EXIT, buyer/seller names, dollar boards) -- built as composer
justhodl-whales over the probe-proven FMP extract endpoint + validated
roster, banks flagged custodial. Chain: ensure fn (create fallback) ->
Event invoke + poll (35 filers x 2 quarters, minutes) -> assert output
credibility (Berkshire present, boards populated, flow math sane) ->
weekly schedule -> page live check."""
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
FN = "justhodl-whales"
ROLE = "arn:aws:iam::857687956942:role/lambda-execution-role"
SCHED_ROLE = "arn:aws:iam::857687956942:role/justhodl-scheduler-role"
AWS_DIR = Path(__file__).resolve().parents[2]
SRC = AWS_DIR / "lambdas" / FN / "source" / "lambda_function.py"


def s3_json(key):
    return json.loads(S3.get_object(Bucket=BUCKET, Key=key)["Body"].read())


def ensure_fn(rep):
    try:
        c = LAM.get_function_configuration(FunctionName=FN)
        rep.kv(fn_exists=True, status=c.get("LastUpdateStatus"))
        for _ in range(20):
            c = LAM.get_function_configuration(FunctionName=FN)
            if c.get("LastUpdateStatus") in (None, "Successful"):
                time.sleep(6)
                return True
            time.sleep(15)
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
    LAM.create_function(
        FunctionName=FN, Runtime="python3.12", Role=ROLE,
        Handler="lambda_function.lambda_handler",
        Code={"ZipFile": buf.read()}, Timeout=600, MemorySize=1024,
        Description="WHALES: per-stock 13F $ flows across the whale "
                    "roster, latest-vs-prior quarter.",
        Environment={"Variables": env})
    time.sleep(12)
    return True


def main():
    fails, warns = [], []
    with report("3040_whales") as rep:
        rep.section("1. Ensure engine")
        if not ensure_fn(rep):
            fails.append("fn not ready")
            _fin(rep, fails, warns, {})
            sys.exit(1)

        rep.section("2. Run the diff (Event + poll)")
        prev = ""
        try:
            prev = s3_json("data/whales.json").get("generated_at", "")
        except Exception:
            pass
        LAM.invoke(FunctionName=FN, InvocationType="Event", Payload=b"{}")
        d = None
        for _ in range(33):                    # up to 11 min
            time.sleep(20)
            try:
                cand = s3_json("data/whales.json")
                if cand.get("generated_at", "") > prev:
                    d = cand
                    break
            except Exception:
                continue
        if not d:
            fails.append("no fresh whales.json after 11min")
            _fin(rep, fails, warns, {})
            sys.exit(1)

        boards = d.get("boards") or {}
        whales = d.get("whales") or []
        berk = next((w for w in whales
                     if "Berkshire" in w.get("name", "")), None)
        infl = boards.get("whale_inflow_leaders") or []
        outf = boards.get("whale_outflow_leaders") or []
        fresh = boards.get("fresh_accumulation") or []
        rep.kv(quarter=d.get("quarter"), whales_ok=d.get("n_whales_ok"),
               failed=d.get("failed"),
               stocks_moved=d.get("n_stocks_moved"),
               berkshire=(json.dumps({k: berk.get(k) for k in
                          ("quarter", "n_positions", "total_value_usd",
                           "n_moves")}) if berk else None),
               top_inflow=json.dumps([{r["symbol"]:
                                       r["conviction_flow_usd"]}
                                      for r in infl[:5]]),
               top_outflow=json.dumps([{r["symbol"]:
                                        r["conviction_flow_usd"]}
                                       for r in outf[:5]]),
               fresh_n=len(fresh))
        if (d.get("n_whales_ok") or 0) < 22:
            fails.append("whales_ok=%s (<22)" % d.get("n_whales_ok"))
        if not berk:
            fails.append("Berkshire missing")
        if (d.get("n_stocks_moved") or 0) < 150:
            fails.append("stocks_moved=%s (<150)"
                         % d.get("n_stocks_moved"))
        if not infl or not outf:
            fails.append("flow boards empty")
        if infl and not (infl[0].get("buyers")
                         and infl[0]["conviction_flow_usd"] > 0):
            fails.append("inflow leader lacks buyers/positive flow")
        banks = [w for w in whales if w.get("custodial")]
        if not banks:
            warns.append("no bank-tier filers resolved")

        rep.section("3. Weekly schedule")
        try:
            SCH.get_schedule(Name="justhodl-whales-weekly")
            rep.kv(schedule="exists")
        except Exception:
            SCH.create_schedule(
                Name="justhodl-whales-weekly",
                ScheduleExpression="cron(10 13 ? * MON *)",
                FlexibleTimeWindow={"Mode": "OFF"},
                Target={"Arn": LAM.get_function_configuration(
                    FunctionName=FN)["FunctionArn"],
                    "RoleArn": SCHED_ROLE},
                Description="Weekly whales 13F diff (amendments)")
            rep.kv(schedule="created cron(10 13 ? * MON *)")

        rep.section("4. Live page (warn-level)")
        try:
            import urllib.request
            req = urllib.request.Request(
                "https://justhodl.ai/whales.html?v=%d" % time.time(),
                headers={"User-Agent": "Mozilla/5.0 ops-3040"})
            page = urllib.request.urlopen(req, timeout=25).read().decode(
                "utf-8", "replace")
            ok = "WHALES ARE HOLDING" in page
            rep.kv(page_live=ok)
            if not ok:
                warns.append("pages not propagated yet")
        except Exception as e:
            warns.append("page: %s" % str(e)[:90])

        rep.section("verdict")
        _fin(rep, fails, warns,
             {"quarter": d.get("quarter"),
              "whales_ok": d.get("n_whales_ok"),
              "stocks_moved": d.get("n_stocks_moved")})
        if fails:
            for f in fails:
                rep.log("FAIL: %s" % f)
            sys.exit(1)
        rep.log("PASS -- %s: %s whales, %s stocks moved"
                % (d.get("quarter"), d.get("n_whales_ok"),
                   d.get("n_stocks_moved")))


def _fin(rep, fails, warns, extra):
    payload = {"ops": 3040, "fails": fails, "warns": warns,
               "verdict": "FAIL" if fails else "PASS",
               "ts": datetime.now(timezone.utc).isoformat()}
    payload.update(extra)
    (AWS_DIR / "ops" / "reports" / "3040.json").write_text(
        json.dumps(payload, indent=1))
    rep.kv(verdict=payload["verdict"], n_fails=len(fails),
           n_warns=len(warns))


main()
sys.exit(0)
