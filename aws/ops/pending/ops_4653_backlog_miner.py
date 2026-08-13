"""ops 4653 — backlog-miner r3 v1.0.2 (read-time derive: belts on cached too): SEC-primary backlog levels +
QoQ/YoY per ticker (anchors + screener candidates), warm 7d,
honest NOT_DISCLOSED. Create-capable deploy, hourly, truth table.
"""
import io
import json
import sys
import time
import urllib.request
import zipfile

import boto3
from botocore.config import Config

from ops_report import report

FN = "justhodl-backlog-miner"
B = "justhodl-dashboard-live"
ROLE = "arn:aws:iam::857687956942:role/justhodl-scheduler-role"
ARN = "arn:aws:lambda:us-east-1:857687956942:function:" + FN
lam = boto3.client("lambda", region_name="us-east-1",
                   config=Config(read_timeout=900,
                                 retries={"max_attempts": 1}))
s3 = boto3.client("s3", region_name="us-east-1")
sch = boto3.client("scheduler", region_name="us-east-1")


def contract(r, name, cond, why):
    if cond:
        r.ok("  [%s] %s" % (name, why))
        return 0
    r.fail("  [%s] CONTRACT MISS — %s" % (name, why))
    return 1


def http_get(url, timeout=45):
    req = urllib.request.Request(url,
                                 headers={"User-Agent": "ops-4653"})
    with urllib.request.urlopen(req, timeout=timeout) as h:
        return h.read()


def main():
    misses = 0
    with report("4653_backlog_miner") as r:
        r.heading("ops 4653 — backlog miner (SEC primary)")

        r.section("deploy (create-capable) + settle + schedule")
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w",
                             zipfile.ZIP_DEFLATED) as z:
            z.write("aws/lambdas/justhodl-backlog-miner/source/"
                    "lambda_function.py", "lambda_function.py")
        created = False
        try:
            lam.get_function(FunctionName=FN)
        except Exception:
            sc = lam.get_function_configuration(
                FunctionName="justhodl-stock-buying")
            lam.create_function(
                FunctionName=FN, Runtime=sc["Runtime"],
                Role=sc["Role"],
                Handler="lambda_function.lambda_handler",
                Timeout=840, MemorySize=1024,
                Code={"ZipFile": buf.getvalue()},
                Environment=sc.get("Environment")
                or {"Variables": {}})
            created = True
        if not created:
            for att in range(10):
                try:
                    st = lam.get_function_configuration(
                        FunctionName=FN)
                    if st.get("LastUpdateStatus") \
                            == "InProgress":
                        time.sleep(15)
                        continue
                    lam.update_function_code(
                        FunctionName=FN, ZipFile=buf.getvalue())
                    break
                except Exception as e:
                    if "ResourceConflict" in str(e):
                        time.sleep(15)
                        continue
                    raise
        settled = False
        for att in range(12):
            try:
                gf = lam.get_function(FunctionName=FN)
                zb = http_get(gf["Code"]["Location"], 60)
                src = zipfile.ZipFile(io.BytesIO(zb)).read(
                    "lambda_function.py").decode("utf-8",
                                                 "replace")
                if "justhodl-backlog-miner v1.0.2" in src:
                    settled = True
                    break
            except Exception:
                pass
            time.sleep(20)
        misses += contract(r, "deploy", settled,
                           "v1.0.2 live (created=%s)" % created)
        if not settled:
            sys.exit(1)
        try:
            sch.get_schedule(Name=FN)
        except Exception:
            try:
                sch.create_schedule(
                    Name=FN,
                    ScheduleExpression="rate(2 hours)",
                    FlexibleTimeWindow={"Mode": "OFF"},
                    Target={"Arn": ARN, "RoleArn": ROLE})
                r.log("2-hourly schedule created")
            except Exception as e:
                r.warn("schedule: %s" % str(e)[:90])

        r.section("run + mined truth")
        inv = lam.invoke(FunctionName=FN,
                         InvocationType="RequestResponse")
        r.kv(fn_error=inv.get("FunctionError"))
        pl = json.loads(s3.get_object(
            Bucket=B, Key="data/backlog-mined.json")
            ["Body"].read())
        by = pl.get("by_ticker") or {}
        r.kv(targets=pl.get("n_targets"),
             mined=pl.get("n_mined"),
             not_disclosed=pl.get("n_not_disclosed"),
             budget_left=pl.get("budget_left"))
        for tk in ("BA", "CAT", "LMT", "GD", "DE", "GE"):
            x = by.get(tk) or {}
            r.log("%-4s %-13s lvl=%-14s qoq=%-6s yoy=%-6s %s"
                  % (tk, x.get("status"),
                     ("%.1fB" % (x["backlog_usd"] / 1e9))
                     if x.get("backlog_usd") else None,
                     x.get("backlog_qoq_pct"),
                     x.get("backlog_yoy_pct"),
                     str(x.get("asof"))[:10]))
        plaus = [t for t, x in by.items()
                 if x.get("status") == "MINED"
                 and 1e8 <= (x.get("backlog_usd") or 0)
                 <= 8e11]
        misses += contract(r, "mined",
                           (pl.get("n_mined") or 0) >= 6
                           and len(plaus) >= 6,
                           "%s mined, %d plausible levels "
                           "(coverage compounds via warm cache)"
                           % (pl.get("n_mined"), len(plaus)))
        chg = [t for t, x in by.items()
               if x.get("backlog_qoq_pct") is not None]
        crazy = [t2 for t2, x in by.items()
                 if abs(x.get("backlog_qoq_pct") or 0) > 300
                 or abs(x.get("backlog_yoy_pct") or 0) > 300]
        misses += contract(r, "sanity",
                           not crazy,
                           "no >300%% deltas (guards): %s"
                           % (crazy or "clean"))
        misses += contract(r, "deltas",
                           len(chg) >= 3,
                           "%d tickers carry QoQ%% (YoY fills "
                           "as filings accumulate)" % len(chg))

        r.section("verdict")
        if misses:
            r.fail("backlog-miner: %d red" % misses)
            sys.exit(1)
        r.ok("BACKLOG MINER LIVE — %s/%s mined from SEC primary "
             "text · store data/backlog-mined.json"
             % (pl.get("n_mined"), pl.get("n_targets")))


if __name__ == "__main__":
    main()
