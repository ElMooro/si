"""ops 4650 — STOCK BUYING accumulation screener (Khalid's
institutional spec): gates + 9-factor composite + catalysts +
double-bottom + why.html deep links. Pre-dumps every input
store's live row keys so the tolerant getters bind to truth.
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

FN = "justhodl-stock-buying"
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
    req = urllib.request.Request(url, headers={"User-Agent": "ops-4650"})
    with urllib.request.urlopen(req, timeout=timeout) as h:
        return h.read()


def s3j(key):
    try:
        return json.loads(s3.get_object(Bucket=B,
                                        Key=key)["Body"].read())
    except Exception:
        return None


def main():
    misses = 0
    with report("4650_stock_buying") as r:
        r.heading("ops 4650 — stock-buying screener")

        r.section("input-store key evidence")
        cen = s3j("data/fundamental-census.json") or {}
        crows = cen.get("rows") or cen.get("companies") or []
        r.kv(census_rows=len(crows))
        if crows:
            r.log("census row keys: %s"
                  % sorted(list(crows[0].keys()))[:44])
        clo = s3j("data/_ma200/closes.json") or {}
        r.kv(closes_tickers=len(clo.get("series") or {}),
             has_SPY="SPY" in (clo.get("series") or {}))
        dl = s3j("data/deal-scanner.json") or {}
        dr = dl.get("rows") or dl.get("events") or []
        if dr:
            r.log("deal row keys: %s"
                  % sorted(list(dr[0].keys()))[:20])
        bm = s3j("data/industry-boom.json") or {}
        br = bm.get("rows") or bm.get("industries") or []
        if br:
            r.log("boom row keys: %s"
                  % sorted(list(br[0].keys()))[:14])

        r.section("deploy (create-capable) + settle + schedule")
        import zipfile as zf2
        buf = io.BytesIO()
        with zf2.ZipFile(buf, "w", zf2.ZIP_DEFLATED) as z:
            z.write("aws/lambdas/justhodl-stock-buying/source/"
                    "lambda_function.py", "lambda_function.py")
        created = False
        try:
            lam.get_function(FunctionName=FN)
        except Exception:
            sc = lam.get_function_configuration(
                FunctionName="justhodl-liquidity-reversal")
            lam.create_function(
                FunctionName=FN, Runtime=sc["Runtime"],
                Role=sc["Role"],
                Handler="lambda_function.lambda_handler",
                Timeout=600, MemorySize=768,
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
        for att in range(14):
            try:
                gf = lam.get_function(FunctionName=FN)
                zb = http_get(gf["Code"]["Location"], 60)
                src = zf2.ZipFile(io.BytesIO(zb)).read(
                    "lambda_function.py").decode("utf-8",
                                                 "replace")
                if "justhodl-stock-buying v1.0.0" in src:
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

        r.section("run + screener truth")
        inv = lam.invoke(FunctionName=FN,
                         InvocationType="RequestResponse")
        r.kv(fn_error=inv.get("FunctionError"))
        pl = s3j("data/stock-buying.json") or {}
        gc = pl.get("gate_census") or {}
        rows = pl.get("rows") or []
        r.kv(gate_census=json.dumps(gc),
             missing=json.dumps(
                 pl.get("missing_factor_counts") or {})[:200],
             candidates=pl.get("n_candidates"))
        for x in rows[:10]:
            r.log("%5.1f %-6s dSMA=%-6s rsi=%-5s eps=%-6s "
                  "peg=%-5s roic=%-5s db=%-9s cat=%s"
                  % (x.get("score"), x.get("ticker"),
                     x.get("sma250_gap_pct"), x.get("rsi14"),
                     x.get("eps_yoy_pct"), x.get("peg"),
                     x.get("roic_pct"),
                     str(x.get("double_bottom")),
                     ",".join(x.get("catalysts") or [])[:22]))
        misses += contract(r, "pipeline",
                           (gc.get("universe") or 0) >= 300
                           and gc.get("passed_all") is not None,
                           "universe %s -> under-SMA %s -> "
                           "RSI %s -> passed %s"
                           % (gc.get("universe"),
                              gc.get("under_sma250"),
                              gc.get("rsi_lt_35"),
                              gc.get("passed_all")))
        misses += contract(r, "rows",
                           pl.get("n_candidates") is not None
                           and (not rows or (
                               isinstance(rows[0].get("score"),
                                          (int, float))
                               and str(rows[0].get("why", ""))
                               .startswith("why.html?ticker="))),
                           "%s candidates; top row scored + "
                           "why-linked" % pl.get("n_candidates"))

        r.section("edge")
        ok = False
        for att in range(9):
            try:
                pg = http_get("https://justhodl.ai/"
                              "stock-buying.html?cb=%d"
                              % time.time()).decode("utf-8",
                                                    "replace")
                jd = json.loads(http_get(
                    "https://justhodl.ai/data/"
                    "stock-buying.json?cb=%d" % time.time()))
                if "data/stock-buying.json" in pg \
                        and jd.get("gate_census"):
                    ok = True
                    break
            except Exception as e:
                r.log("edge %d: %s" % (att + 1, str(e)[:70]))
            time.sleep(20)
        misses += contract(r, "edge", ok,
                           "page + payload at the edge")

        r.section("verdict")
        if misses:
            r.fail("stock-buying: %d red" % misses)
            sys.exit(1)
        r.ok("STOCK BUYING LIVE — funnel %s -> %s candidates · "
             "https://justhodl.ai/stock-buying.html"
             % (json.dumps(gc), pl.get("n_candidates")))


if __name__ == "__main__":
    main()
