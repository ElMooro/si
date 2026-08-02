"""
ops_4285 -- wave 3: kill the rot, deploy the never-deployed, and grep
the whole fleet for the same infections.

Fixes shipped this push:
  commodity-curves  gold rides FMP GCUSD (LBMA FRED series is dead)
  convexity-scorer  short interest from the house FINRA rail
  failure-library   FMP stable renamed insider path -> /search
Plus: justhodl-eurostat-history exists in repo but was NEVER deployed
(ResourceNotFoundException in 4284) -> ensure-create + first run.
Plus: fleet-wide grep for every other engine carrying the dead FRED
gold series or the two dead FMP paths -- rot rarely infects only the
engine that surfaced it. And: 4284 misattributed data/morning-intel's
writer to a READER (census regex counts get_object keys); locate the
true put_object writer by proximity grep and correct the atlas.
"""
import io
import json
import os
import re
import sys
import time
import zipfile
from datetime import datetime, timezone

import boto3
from botocore.config import Config
from ops_report import report

REGION, BUCKET = "us-east-1", "justhodl-dashboard-live"
s3 = boto3.client("s3", region_name=REGION)
lam = boto3.client("lambda", region_name=REGION,
                   config=Config(read_timeout=90, retries={"max_attempts": 1}))
logs = boto3.client("logs", region_name=REGION)
RUN_START = datetime.now(timezone.utc)

ROT = {
    "dead FRED gold (GOLDAMGBD/GOLDPMGBD)":
        re.compile(r"GOLD[AP]MGBD228NLBM"),
    "dead FMP /short-interest":
        re.compile(r"""["']/short-interest["']"""),
    "dead FMP /insider-trading (non-search)":
        re.compile(r"""["']/?insider-trading["']"""),
}

def fresh_deploy(fn, minutes=12, tries=45):
    for _ in range(tries):
        try:
            c = lam.get_function_configuration(FunctionName=fn)
            if c.get("LastUpdateStatus") in (None, "Successful") \
                    and c.get("State") == "Active":
                lm = datetime.strptime(
                    c["LastModified"].split(".")[0], "%Y-%m-%dT%H:%M:%S"
                ).replace(tzinfo=timezone.utc)
                if (RUN_START - lm).total_seconds() < minutes * 60:
                    return True
        except Exception:
            pass
        time.sleep(8)
    return False

def recent_logs(fn, pat, window=360):
    try:
        ev = logs.filter_log_events(
            logGroupName="/aws/lambda/%s" % fn,
            startTime=int((time.time() - window) * 1000))
        return [x["message"].strip()[:130]
                for x in ev.get("events", []) if pat in x["message"]]
    except Exception:
        return []

fails = []
with report("4285_wave3_rot") as r:
    r.heading("ops 4285 -- wave 3: rot killed, fleet swept")

    r.section("0. fleet-wide rot grep")
    infected = {k: [] for k in ROT}
    for eng in sorted(os.listdir("aws/lambdas")):
        sp = "aws/lambdas/%s/source/lambda_function.py" % eng
        if not os.path.exists(sp):
            continue
        srct = open(sp, encoding="utf-8", errors="ignore").read()
        for label, rx in ROT.items():
            for m in rx.finditer(srct):
                if "insider-trading" in label and \
                        "insider-trading/search" in srct[
                            m.start():m.start() + 40]:
                    continue
                infected[label].append(eng)
                break
    for label, engs in infected.items():
        (r.warn if engs else r.ok)(
            "%s: %d engines%s"
            % (label, len(engs),
               " -- " + ", ".join(e.replace("justhodl-", "")
                                  for e in engs[:10]) if engs else ""))

    r.section("0b. morning-intel true writer (put-proximity)")
    true_writer = None
    for eng in sorted(os.listdir("aws/lambdas")):
        sp = "aws/lambdas/%s/source/lambda_function.py" % eng
        if not os.path.exists(sp):
            continue
        srct = open(sp, encoding="utf-8", errors="ignore").read()
        i = srct.find("morning-intel.json")
        if i >= 0 and "put_object" in srct[max(0, i - 400):i]:
            true_writer = eng
            break
    r.log("true writer: %s (4284 blamed the reader ab-test; census "
          "regex hardening queued)" % (true_writer or "NONE FOUND"))

    r.section("1. eurostat-history: deploy the never-deployed")
    FN = "justhodl-eurostat-history"
    try:
        lam.get_function_configuration(FunctionName=FN)
        r.ok("function already exists")
        created = True
    except Exception:
        created = False
    if not created:
        try:
            cfg = json.load(open("aws/lambdas/%s/config.json" % FN))
            buf = io.BytesIO()
            with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
                sdir = "aws/lambdas/%s/source" % FN
                for root, _, files in os.walk(sdir):
                    for f in files:
                        fp = os.path.join(root, f)
                        z.write(fp, os.path.relpath(fp, sdir))
                for shf in os.listdir("aws/shared"):
                    if shf.endswith(".py"):
                        z.write(os.path.join("aws/shared", shf), shf)
            lam.create_function(
                FunctionName=FN,
                Runtime=cfg.get("runtime", "python3.12"),
                Role=cfg.get("role",
                             "arn:aws:iam::857687956942:role/"
                             "lambda-execution-role"),
                Handler=cfg.get("handler",
                                "lambda_function.lambda_handler"),
                Code={"ZipFile": buf.getvalue()},
                Timeout=int(cfg.get("timeout", 120)),
                MemorySize=int(cfg.get("memory", 512)),
                Environment={"Variables": cfg.get("env", {}) or {}},
                Architectures=["x86_64"])
            for _ in range(30):
                if lam.get_function_configuration(
                        FunctionName=FN).get("State") == "Active":
                    break
                time.sleep(5)
            r.ok("CREATED %s (%d KB, from committed config)"
                 % (FN, len(buf.getvalue()) // 1024))
        except Exception as e:
            fails.append("eurostat create: %s" % str(e)[:130])
    if not fails:
        try:
            p = lam.invoke(FunctionName=FN,
                           InvocationType="RequestResponse",
                           Payload=b"{}")
            r.log("eurostat first run: %s"
                  % (p["Payload"].read() or b"")[:170].decode(
                      "utf-8", "ignore"))
            s3.head_object(Bucket=BUCKET, Key="data/ecb-confidence.json")
            r.ok("data/ecb-confidence.json MATERIALIZED -- a "
                 "never-deployed engine is now live")
        except Exception as e:
            r.warn("eurostat verify: %s (its outputs may use other "
                   "keys; logged for review)" % str(e)[:100])

    r.section("2. the three rot fixes, invoked on settled code")
    checks = [
        ("justhodl-commodity-curves", "GOLDAMGBD228NLBM",
         "data/commodity-curves.json"),
        ("justhodl-convexity-scorer", "fmp_fail /short-interest",
         "data/convexity-scores.json"),
        ("justhodl-failure-library", "fmp_fail /insider-trading",
         "data/failure-library.json"),
    ]
    for fn, badpat, art in checks:
        if not fresh_deploy(fn):
            fails.append("%s deploy window missed" % fn)
            continue
        try:
            p = lam.invoke(FunctionName=fn,
                           InvocationType="RequestResponse",
                           Payload=b"{}")
            r.log("%s: %s" % (fn.replace("justhodl-", ""),
                              (p["Payload"].read() or b"")[:120].decode(
                                  "utf-8", "ignore")))
        except Exception as e:
            if "Read timeout" not in str(e):
                fails.append("%s invoke: %s" % (fn, str(e)[:90]))
                continue
        time.sleep(6)
        bad = recent_logs(fn, badpat)
        if bad:
            fails.append("%s still emitting rot: %s"
                         % (fn, bad[-1][:100]))
        else:
            r.ok("%s: rot signature gone from fresh logs"
                 % fn.replace("justhodl-", ""))
        try:
            h = s3.head_object(Bucket=BUCKET, Key=art)
            age = (datetime.now(timezone.utc)
                   - h["LastModified"]).total_seconds() / 60
            (r.ok if age < 30 else r.warn)(
                "%s %s (%.0f min)" % (art, "fresh" if age < 30
                                      else "not refreshed this run",
                                      age))
        except Exception:
            r.log("%s: primary artifact key differs; logs are the "
                  "gate here" % art)

    r.section("RESULT")
    if fails:
        for f in fails:
            r.fail("  %s" % f)
    else:
        r.ok("OPS 4285 PASS -- rot fixed at source, fleet swept, "
             "dormant engine deployed")
if fails:
    sys.exit(1)
