"""ops 4651 r5 (ping; their v1.0.3 restored after my overwrite) — cols|metrics alias, v1.0.3 settled rerun; columnar loader (consumer-verified shape) + scalar fallbacks.

FMP key injected from fmp-fundamentals-agent env; create-capable
deploy; hourly schedule; invoke; truth table with pillar scores;
CF purge; structural edge; dblclick->why.html verified in HTML.
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
    req = urllib.request.Request(url, headers={"User-Agent": "ops"})
    with urllib.request.urlopen(req, timeout=timeout) as h:
        return h.read()


def main():
    misses = 0
    with report("4651_stock_buying") as r:
        r.heading("ops 4651 — stock-buying flagship")

        r.section("FMP key donor -> engine env")
        fmpk = None
        for donor in ("fmp-fundamentals-agent",
                      "justhodl-data-census",
                      "census-economic-agent"):
            try:
                ev = (lam.get_function_configuration(
                    FunctionName=donor).get("Environment")
                    or {}).get("Variables") or {}
                for kk in ("FMP_API_KEY", "FMP_KEY", "FMP"):
                    if ev.get(kk):
                        fmpk = ev[kk]
                        r.log("key from %s.%s (len=%d)"
                              % (donor, kk, len(fmpk)))
                        break
            except Exception:
                continue
            if fmpk:
                break
        if not fmpk:
            r.warn("no FMP key found in donors — revisions "
                   "pillar will read n/a")

        r.section("deploy (create-capable) + schedule")
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w",
                             zipfile.ZIP_DEFLATED) as z:
            z.write("aws/lambdas/justhodl-stock-buying/source/"
                    "lambda_function.py", "lambda_function.py")
        created = False
        try:
            lam.get_function(FunctionName=FN)
        except Exception:
            sc = lam.get_function_configuration(
                FunctionName="justhodl-liquidity-reversal")
            env = {"Variables": {}}
            if fmpk:
                env["Variables"]["FMP_API_KEY"] = fmpk
            lam.create_function(
                FunctionName=FN, Runtime=sc["Runtime"],
                Role=sc["Role"],
                Handler="lambda_function.lambda_handler",
                Timeout=780, MemorySize=768,
                Code={"ZipFile": buf.getvalue()},
                Environment=env)
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
            if fmpk:
                for att in range(10):
                    try:
                        st = lam.get_function_configuration(
                            FunctionName=FN)
                        if st.get("LastUpdateStatus") \
                                == "InProgress":
                            time.sleep(12)
                            continue
                        ev = (st.get("Environment")
                              or {}).get("Variables") or {}
                        if ev.get("FMP_API_KEY") != fmpk:
                            ev["FMP_API_KEY"] = fmpk
                            lam.update_function_configuration(
                                FunctionName=FN,
                                Environment={"Variables": ev})
                        break
                    except Exception as e:
                        if "ResourceConflict" in str(e):
                            time.sleep(12)
                            continue
                        break
        settled = False
        for att in range(14):
            try:
                gf = lam.get_function(FunctionName=FN)
                zb = http_get(gf["Code"]["Location"], 60)
                src = zipfile.ZipFile(io.BytesIO(zb)).read(
                    "lambda_function.py").decode("utf-8",
                                                 "replace")
                if "justhodl-stock-buying v1.0.3" in src:
                    settled = True
                    break
            except Exception:
                pass
            time.sleep(20)
        misses += contract(r, "deploy", settled,
                           "v1.0.3 live (created=%s)" % created)
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

        r.section("matrix probe (runner-side)")
        try:
            mx = json.loads(s3.get_object(
                Bucket=B,
                Key="data/fundamental-census-matrix.json")
                ["Body"].read())
            r.log("top_keys=%s n_tickers=%s n_cols=%s"
                  % (list(mx.keys())[:10],
                     len(mx.get("tickers") or []),
                     len(mx.get("cols") or {})))
            r.log("first cols: %s"
                  % list((mx.get("cols") or {}).keys())[:20])
        except Exception as e:
            r.log("matrix read ERR: %s" % str(e)[:110])

        r.section("run + institutional truth")
        inv = lam.invoke(FunctionName=FN,
                         InvocationType="RequestResponse")
        r.kv(fn_error=inv.get("FunctionError"))
        if inv.get("FunctionError"):
            time.sleep(10)
            try:
                logs = boto3.client("logs",
                                    region_name="us-east-1")
                evs = logs.filter_log_events(
                    logGroupName="/aws/lambda/" + FN,
                    startTime=int((time.time() - 600) * 1000))
                tail = "".join(e["message"] for e in
                               evs.get("events") or [])[-2500:]
                for ln in tail.splitlines():
                    if "Error" in ln or "line " in ln \
                            or "File" in ln:
                        r.log("CW| " + ln.strip()[:160])
            except Exception:
                pass
        pl = json.loads(s3.get_object(
            Bucket=B, Key="data/stock-buying.json")
            ["Body"].read())
        r.log("engine matrix_probe: %s"
              % json.dumps(pl.get("matrix_probe"))[:200])
        r.kv(census=pl.get("census_source"),
             mode=pl.get("census_mode"),
             universe=pl.get("n_universe"),
             scored=pl.get("n_scored"),
             fmp=pl.get("fmp_key"),
             tiers=json.dumps(pl.get("tiers")),
             gates=json.dumps(pl.get("gates_summary")))
        r.log("census fields: %s"
              % (pl.get("census_fields_sample") or [])[:36])
        top = pl.get("top") or []
        for x in top[:12]:
            p = x.get("pillars") or {}
            r.log("%-6s %-16s %-9s sc=%-5s bt=%-5s ac=%-5s "
                  "fcf=%-5s val=%-5s cat=%-5s peg=%-5s %s"
                  % (x["symbol"], x["tier"][:16],
                     "DB" if (x.get("double_bottom")
                              or {}).get("formed") else "--",
                     x["score"], p.get("revisions_beats"),
                     p.get("accel"), p.get("fcf_growth"),
                     p.get("valuation_vs_growth"),
                     p.get("catalyst_rs"), x.get("peg"),
                     (x.get("catalysts") or [{}])[0].get(
                         "class", "")[:18]))
        misses += contract(r, "universe",
                           (pl.get("n_universe") or 0) >= 300,
                           "%s companies in census universe"
                           % pl.get("n_universe"))
        misses += contract(r, "scored",
                           (pl.get("n_scored") or 0) >= 200,
                           "%s scored rows" % pl.get("n_scored"))
        t1 = top[0] if top else {}
        misses += contract(r, "row-integrity",
                           bool(t1.get("why", "").startswith(
                               "why.html?symbol="))
                           and isinstance(t1.get("pillars"),
                                          dict)
                           and isinstance(t1.get("gates"),
                                          dict),
                           "top row carries pillars+gates+why "
                           "link (%s)" % t1.get("symbol"))
        tiers = pl.get("tiers") or {}
        misses += contract(r, "tiers",
                           sum(tiers.values()) ==
                           pl.get("n_scored"),
                           "tier partition sums: %s" % tiers)

        r.section("edge (CF purge + structural)")
        try:
            import os as _os
            cf = _os.environ.get("CLOUDFLARE_API_TOKEN", "")
            if cf:
                zreq = urllib.request.Request(
                    "https://api.cloudflare.com/client/v4/"
                    "zones?name=justhodl.ai",
                    headers={"Authorization": "Bearer " + cf})
                with urllib.request.urlopen(zreq,
                                            timeout=20) as h:
                    zid = json.loads(
                        h.read())["result"][0]["id"]
                preq = urllib.request.Request(
                    "https://api.cloudflare.com/client/v4/"
                    "zones/" + zid + "/purge_cache",
                    method="POST",
                    headers={"Authorization": "Bearer " + cf,
                             "Content-Type":
                                 "application/json"},
                    data=json.dumps({"files": [
                        "https://justhodl.ai/"
                        "stock-buying.html",
                        "https://justhodl.ai/data/"
                        "stock-buying.json"]}).encode())
                urllib.request.urlopen(preq, timeout=20)
                r.log("CF purge issued")
                time.sleep(5)
            else:
                r.warn("CF token absent — probing without purge")
        except Exception as e:
            r.warn("purge: %s" % str(e)[:80])
        page_ok = pay_ok = dbl_ok = False
        for att in range(9):
            try:
                pg = http_get("https://justhodl.ai/"
                              "stock-buying.html?cb=%d"
                              % time.time()).decode("utf-8",
                                                    "replace")
                page_ok = "data/stock-buying.json" in pg
                dbl_ok = "ondblclick" in pg and \
                    "why.html?symbol=" in pg
                jd = json.loads(http_get(
                    "https://justhodl.ai/data/"
                    "stock-buying.json?cb=%d" % time.time()))
                pay_ok = bool((jd.get("top") or [{}])[0]
                              .get("symbol"))
                if page_ok and pay_ok and dbl_ok:
                    break
            except Exception as e:
                r.log("edge %d: %s" % (att + 1, str(e)[:70]))
            time.sleep(20)
        misses += contract(r, "edge",
                           page_ok and pay_ok and dbl_ok,
                           "page structural + dblclick->why "
                           "+ payload at edge")

        r.section("verdict")
        if misses:
            r.fail("stock-buying: %d red" % misses)
            sys.exit(1)
        r.ok("STOCK BUYING LIVE — universe %s, tiers %s · "
             "https://justhodl.ai/stock-buying.html"
             % (pl.get("n_universe"),
                json.dumps(pl.get("tiers"))))


if __name__ == "__main__":
    main()
