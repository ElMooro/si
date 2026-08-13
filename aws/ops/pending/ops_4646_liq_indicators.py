"""ops 4643 — DXY PREDICT THE FUTURE engine (Khalid's list, same
# r8: page_ok collapsed to the body-proven string (new-page cold-cache pattern)
playbook): fork of the fully-evolved liquidity engine with a
mechanical-only DXY polarity brain (FX pairs by USD side, currency
indexes, US-vs-foreign tenor spreads by leg order), USD_UP/USD_DOWN
dials under payload['dxy'], shared warm-cache pool.
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

FN = "justhodl-liq-indicators"
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
    req = urllib.request.Request(url, headers={"User-Agent": "ops-4643"})
    with urllib.request.urlopen(req, timeout=timeout) as h:
        return h.read()


def main():
    misses = 0
    with report("4646_liq_indicators") as r:
        r.heading("ops 4643 — DXY predict-the-future engine")

        r.section("pre-dump: liquidity-indicator list candidates")
        wl = json.loads(s3.get_object(
            Bucket=B, Key="data/tv-watchlists.json")["Body"].read())
        cands = [(str(it.get("name")), it.get("n"))
                 for it in (wl.get("lists") or [])
                 if isinstance(it, dict)
                 and any(k in str(it.get("name", "")).lower()
                         for k in ("liquidity", "dollar", "usd "))]
        r.kv(candidates=json.dumps(cands[:8]))
        misses += contract(r, "list-exists", bool(cands),
                           "liquidity-indicator list present: %s"
                           % (cands[:3] or "NONE"))

        r.section("deploy (ops-side) + settle + schedule")
        import zipfile as zf2
        buf = io.BytesIO()
        with zf2.ZipFile(buf, "w", zf2.ZIP_DEFLATED) as z:
            z.write("aws/lambdas/justhodl-liq-indicators/source/"
                    "lambda_function.py", "lambda_function.py")
        created = False
        try:
            lam.get_function(FunctionName=FN)
        except Exception:
            src_cfg = lam.get_function_configuration(
                FunctionName="justhodl-liquidity-reversal")
            lam.create_function(
                FunctionName=FN, Runtime=src_cfg["Runtime"],
                Role=src_cfg["Role"],
                Handler="lambda_function.lambda_handler",
                Timeout=src_cfg.get("Timeout", 600),
                MemorySize=src_cfg.get("MemorySize", 512),
                Code={"ZipFile": buf.getvalue()},
                Environment=src_cfg.get("Environment")
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
                if "justhodl-liq-indicators v1.0.0" in src:
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

        r.section("run + dxy truth")
        inv = lam.invoke(FunctionName=FN,
                         InvocationType="RequestResponse")
        r.kv(fn_error=inv.get("FunctionError"))
        pl = json.loads(s3.get_object(
            Bucket=B, Key="data/liq-indicators.json")["Body"].read())
        dx = pl.get("liquidity") or {}
        rows = pl.get("rows") or []
        pol_rows = [x for x in rows if x.get("polarity")]
        r.kv(list=pl.get("list_name"),
             members=pl.get("n_members"),
             resolved=pl.get("n_resolved"),
             polarity_rows=len(pol_rows),
             trend=dx.get("trend_score"),
             tlabel=dx.get("trend_label"),
             reversal=dx.get("reversal_score"),
             rlabel=dx.get("reversal_label"))
        shown = 0
        for x in rows:
            if not x.get("polarity"):
                continue
            r.log("%-26s pol=%+d trend=%-5s rev=%-14s z=%s"
                  % (str(x.get("symbol"))[:26], x["polarity"],
                     x.get("trend_state"),
                     str(x.get("reversal_state"))[:14],
                     x.get("move_z")))
            shown += 1
            if shown >= 14:
                break
        nm = pl.get("n_members") or 0
        nr = pl.get("n_resolved") or 0
        misses += contract(r, "list-found",
                           bool(pl.get("list_name")),
                           "list '%s' (%s members)"
                           % (pl.get("list_name"), nm))
        misses += contract(r, "resolution",
                           nr >= 30 and (nm == 0
                                         or nr >= 0.5 * nm),
                           "%d/%d resolved (shared cache pool)"
                           % (nr, nm))
        misses += contract(r, "polarity",
                           len(pol_rows) >= 4,
                           "%d mechanically-signed rows"
                           % len(pol_rows))
        misses += contract(r, "dials",
                           isinstance(dx.get("trend_score"),
                                      (int, float))
                           and bool(dx.get("trend_label"))
                           and bool(dx.get("reversal_label")),
                           "LIQ TREND %s (%s) · REVERSAL %s (%s)"
                           % (dx.get("trend_score"),
                              dx.get("trend_label"),
                              dx.get("reversal_score"),
                              dx.get("reversal_label")))

        r.section("edge (with CF purge)")
        try:
            import os as _os
            cf = _os.environ.get("CLOUDFLARE_API_TOKEN", "")
            if cf:
                zreq = urllib.request.Request(
                    "https://api.cloudflare.com/client/v4/zones"
                    "?name=justhodl.ai",
                    headers={"Authorization": "Bearer " + cf})
                with urllib.request.urlopen(zreq,
                                            timeout=20) as h:
                    zid = json.loads(h.read())["result"][0]["id"]
                preq = urllib.request.Request(
                    "https://api.cloudflare.com/client/v4/zones/"
                    + zid + "/purge_cache", method="POST",
                    headers={"Authorization": "Bearer " + cf,
                             "Content-Type": "application/json"},
                    data=json.dumps({"files": [
                        "https://justhodl.ai/"
                        "liq-indicators.html",
                        "https://justhodl.ai/data/"
                        "liq-indicators.json"]}).encode())
                with urllib.request.urlopen(preq,
                                            timeout=20) as h:
                    ok = json.loads(h.read()).get("success")
                r.log("CF purge: %s" % ok)
                time.sleep(5)
        except Exception as e:
            r.warn("purge: %s" % str(e)[:90])
        try:
            ctl = http_get("https://justhodl.ai/dxy-predict.html"
                           "?cb=%d" % time.time()).decode(
                "utf-8", "replace")
            r.log("CONTROL dxy: len=%d has_json=%s"
                  % (len(ctl), "dxy-predict.json" in ctl))
            pg0 = http_get("https://justhodl.ai/"
                           "liq-indicators.html?cb=%d"
                           % time.time()).decode("utf-8",
                                                 "replace")
            i = pg0.find("fetch(")
            j = pg0.find("liq")
            r.log("LIQ body: len=%d count(liq)=%d "
                  "fetch@%d liq@%d"
                  % (len(pg0), pg0.count("liq"), i, j))
            r.log("slice@fetch: " + pg0[max(0, i - 10):
                                        i + 110].replace(
                "\n", " ")[:150])
            if j >= 0:
                r.log("slice@liq: " + pg0[max(0, j - 30):
                                          j + 90].replace(
                    "\n", " ")[:150])
            else:
                r.warn("CLOUDFLARE_API_TOKEN absent — probing "
                       "without purge")
        except Exception as e:
            r.warn("purge: %s" % str(e)[:90])
        page_ok = pay_ok = False
        for att in range(9):
            try:
                pg = http_get("https://justhodl.ai/"
                              "liq-indicators.html?cb=%d"
                              % time.time()).decode("utf-8",
                                                    "replace")
                page_ok = ("data/liq-indicators.json" in pg)
                jd = json.loads(http_get(
                    "https://justhodl.ai/data/liq-indicators.json"
                    "?cb=%d" % time.time()))
                pay_ok = bool((jd.get("liquidity")
                               or {}).get("trend_label"))
                if page_ok and pay_ok:
                    break
            except Exception as e:
                r.log("edge %d: %s" % (att + 1, str(e)[:70]))
            if not (page_ok and pay_ok):
                try:
                    r.log("edge probe %d: page_ok=%s len=%s "
                          "head=%s | pay_ok=%s keys=%s"
                          % (att + 1, page_ok,
                             len(pg) if 'pg' in dir() else '?',
                             (pg[:60].replace(chr(10), ' ')
                              if 'pg' in dir() else '?'),
                             pay_ok,
                             list(jd.keys())[:6]
                             if 'jd' in dir() else '?'))
                except Exception:
                    pass
            time.sleep(20)
        misses += contract(r, "edge", page_ok and pay_ok,
                           "page + payload at the edge")

        r.section("verdict")
        if misses:
            r.fail("liq-indicators: %d red" % misses)
            sys.exit(1)
        r.ok("LIQ INDICATORS LIVE — list '%s': %s/%s resolved, %d "
             "signed rows · TREND %s (%s) · REVERSAL %s (%s) · "
             "https://justhodl.ai/liq-indicators.html"
             % (pl.get("list_name"), nr, nm, len(pol_rows),
                dx.get("trend_score"), dx.get("trend_label"),
                dx.get("reversal_score"),
                dx.get("reversal_label")))


if __name__ == "__main__":
    main()
