"""ops 4654 — CATALYST engine v1.0.0: Khalid's revaluation-forcer
taxonomy fused from in-fleet primaries (PR tape, XBRL backlog/EPS,
census buyback/debt/margin, industry boom, commodity/rate macro).
Create-capable, hourly, evidence-truth table, stock-buying join.
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

FN = "justhodl-catalyst"
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
                                 headers={"User-Agent": "ops-4654"})
    with urllib.request.urlopen(req, timeout=timeout) as h:
        return h.read()


def main():
    misses = 0
    with report("4654_catalyst") as r:
        r.heading("ops 4654 — catalyst engine")

        r.section("deploy (create-capable) + settle + schedule")
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w",
                             zipfile.ZIP_DEFLATED) as z:
            z.write("aws/lambdas/justhodl-catalyst/source/"
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
                Timeout=300, MemorySize=768,
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
                        FunctionName=FN,
                        ZipFile=buf.getvalue())
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
                if "justhodl-catalyst v1.0.0" in src:
                    settled = True
                    break
            except Exception:
                pass
            time.sleep(20)
        misses += contract(r, "deploy", settled,
                           "v1.0.0 live (created=%s)" % created)
        if not settled:
            sys.exit(1)
        try:
            sch.get_schedule(Name=FN)
        except Exception:
            try:
                sch.create_schedule(
                    Name=FN, ScheduleExpression="rate(1 hour)",
                    FlexibleTimeWindow={"Mode": "OFF"},
                    Target={"Arn": ARN, "RoleArn": ROLE})
                r.log("hourly schedule created")
            except Exception as e:
                r.warn("schedule: %s" % str(e)[:90])

        r.section("run + catalyst truth")
        inv = lam.invoke(FunctionName=FN,
                         InvocationType="RequestResponse")
        r.kv(fn_error=inv.get("FunctionError"))
        pl = json.loads(s3.get_object(
            Bucket=B, Key="data/catalyst.json")["Body"].read())
        by = pl.get("by_ticker") or {}
        cc = pl.get("class_census") or {}
        r.kv(n_tickers=pl.get("n_tickers"),
             n_classes=len(cc),
             macro=json.dumps(pl.get("macro") or {})[:140])
        r.log("class census: %s"
              % json.dumps(sorted(cc.items(),
                                  key=lambda kv: -kv[1])[:12]))
        top = sorted(by.items(),
                     key=lambda kv: -(kv[1].get("score") or 0)
                     )[:8]
        for t, e in top:
            c0 = (e.get("catalysts") or [{}])[0]
            r.log("%-5s s=%-5s %-20s %s"
                  % (t, e.get("score"),
                     str(c0.get("class"))[:20],
                     str(c0.get("evidence"))[:70]))
        misses += contract(r, "coverage",
                           (pl.get("n_tickers") or 0) >= 40,
                           "%s tickers carry catalysts"
                           % pl.get("n_tickers"))
        misses += contract(r, "taxonomy-breadth",
                           len(cc) >= 6,
                           "%d distinct classes live: %s"
                           % (len(cc),
                              list(cc.keys())[:8]))
        ev_ok = all((c.get("evidence") and c.get("src"))
                    for _, e in top
                    for c in e.get("catalysts") or [])
        misses += contract(r, "evidence",
                           ev_ok,
                           "every top catalyst carries "
                           "evidence + source")

        r.section("stock-buying join")
        lam.invoke(FunctionName="justhodl-stock-buying",
                   InvocationType="RequestResponse")
        sb = json.loads(s3.get_object(
            Bucket=B, Key="data/stock-buying.json")
            ["Body"].read())
        jn = sb.get("catalyst_join_n")
        rows = sb.get("top") or []
        with_c = sum(1 for x in rows
                     if x.get("catalysts"))
        r.kv(catalyst_join_n=jn, top_rows_with=with_c)
        misses += contract(r, "join",
                           (jn or 0) >= 30 and with_c >= 20,
                           "join_n=%s, %d shipped rows carry "
                           "catalyst classes" % (jn, with_c))

        r.section("verdict")
        if misses:
            r.fail("catalyst: %d red" % misses)
            sys.exit(1)
        r.ok("CATALYST LIVE — %s tickers, %d classes, macro %s "
             "· data/catalyst.json · joined into stock-buying"
             % (pl.get("n_tickers"), len(cc),
                json.dumps(pl.get("macro") or {})[:60]))


if __name__ == "__main__":
    main()
