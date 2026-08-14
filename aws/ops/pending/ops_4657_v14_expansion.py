"""ops 4657 — stock-buying v1.4.0 + page v5 expansion gate:
SMA ladder (20/50/100/200/250 + golden cross), warnings
(dilution/rev/eps contraction/double-top), census binds
(net-bb-yield, P/S, FCF yield/sh/CAGR3, inventory), catalyst
6-class badges, 13F institutional/whale join, engine dropdown.
Evidence dumps: 13F 't' sample + options-store discovery.
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
lam = boto3.client("lambda", region_name="us-east-1",
                   config=Config(read_timeout=900,
                                 retries={"max_attempts": 1}))
s3 = boto3.client("s3", region_name="us-east-1")


def contract(r, name, cond, why):
    if cond:
        r.ok("  [%s] %s" % (name, why))
        return 0
    r.fail("  [%s] CONTRACT MISS — %s" % (name, why))
    return 1


def http_get(url, timeout=45):
    req = urllib.request.Request(url,
                                 headers={"User-Agent": "ops-4657"})
    with urllib.request.urlopen(req, timeout=timeout) as h:
        return h.read()


def main():
    misses = 0
    with report("4657_v14_expansion") as r:
        r.heading("ops 4657 — v1.4.0 expansion")

        r.section("evidence: 13F 't' sample + options discovery")
        try:
            f13 = json.loads(s3.get_object(
                Bucket=B, Key="data/13f-flows-by-ticker.json")
                ["Body"].read())
            tmap = f13.get("t") or {}
            k0 = "AAPL" if "AAPL" in tmap else \
                sorted(tmap.keys())[0]
            r.log("13F t[%s] = %s"
                  % (k0, json.dumps(tmap[k0])[:300]))
            r.kv(f13_n=len(tmap),
                 whale_rule=str(f13.get("whale_rule"))[:80])
        except Exception as e:
            r.warn("13F: %s" % str(e)[:80])
        try:
            kw = {"Bucket": B, "Prefix": "data/"}
            opt = []
            while True:
                pg = s3.list_objects_v2(**kw)
                for o in pg.get("Contents") or []:
                    kl = o["Key"].lower()
                    if "option" in kl or "putcall" in kl \
                            or "put-call" in kl:
                        opt.append(o["Key"])
                if not pg.get("IsTruncated"):
                    break
                kw["ContinuationToken"] = \
                    pg["NextContinuationToken"]
            r.log("options stores: %s" % opt[:8])
        except Exception as e:
            r.warn("opt list: %s" % str(e)[:60])

        r.section("deploy + settle")
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w",
                             zipfile.ZIP_DEFLATED) as z:
            z.write("aws/lambdas/justhodl-stock-buying/source/"
                    "lambda_function.py", "lambda_function.py")
        for att in range(10):
            try:
                st = lam.get_function_configuration(
                    FunctionName=FN)
                if st.get("LastUpdateStatus") == "InProgress":
                    time.sleep(15)
                    continue
                lam.update_function_code(FunctionName=FN,
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
                if "justhodl-stock-buying v1.4.2" in src:
                    settled = True
                    break
            except Exception:
                pass
            time.sleep(20)
        misses += contract(r, "deploy", settled, "v1.4.2 live")
        if not settled:
            sys.exit(1)

        r.section("run + expansion truth")
        inv = None
        for att in range(6):
            try:
                inv = lam.invoke(FunctionName=FN,
                                 InvocationType="RequestResponse")
                break
            except Exception as e:
                if "ResourceConflict" in str(e) \
                        or "Pending" in str(e):
                    time.sleep(15)
                    continue
                raise
        r.kv(fn_error=(inv or {}).get("FunctionError"))
        pl = json.loads(s3.get_object(
            Bucket=B, Key="data/stock-buying.json")
            ["Body"].read())
        rows = pl.get("top") or []
        cen = [x for x in rows if x.get("lane") != "BROAD"]

        def nn(key, sub=None):
            n = 0
            for x in cen:
                v = x.get(key)
                if sub and isinstance(v, dict):
                    v = v.get(sub)
                if v not in (None, "", []):
                    n += 1
            return n
        lad_n = sum(1 for x in cen
                    if ((x.get("sma") or {}).get("ladder")
                        or {}).get("below200") is not None)
        gc_n = sum(1 for x in cen
                   if ((x.get("sma") or {}).get("ladder")
                       or {}).get("golden"))
        warn_n = sum(1 for x in rows if x.get("warnings"))
        r.kv(ladder=lad_n, golden=gc_n, warned=warn_n,
             nbb=nn("net_bb_yield_pct"), ps=nn("ps"),
             fcfy=nn("fcf_yield_pct"),
             fc3=nn("fcf_cagr3_pct"),
             inv_rev=nn("inv_rev_pct"),
             f13=pl.get("f13_join_n"))
        for x in [y for y in rows if y.get("warnings")][:5]:
            r.log("WARN %-5s %s" % (x.get("symbol"),
                                    x.get("warnings")))
        misses += contract(r, "ladder",
                           lad_n >= 240,
                           "%d/300-lane rows carry SMA "
                           "ladder; %d golden" % (lad_n, gc_n))
        misses += contract(r, "binds",
                           min(nn("net_bb_yield_pct"),
                               nn("ps"), nn("fcf_yield_pct"),
                               nn("fcf_cagr3_pct"),
                               nn("inv_rev_pct")) >= 200,
                           "census binds live (nbb/ps/fcfy/"
                           "fc3/inv >= 200 each)")
        misses += contract(r, "warnings",
                           warn_n >= 3,
                           "%d rows carry red-flag warnings"
                           % warn_n)
        misses += contract(r, "f13-join",
                           (pl.get("f13_join_n") or 0) >= 80,
                           "13F flows joined on %s rows"
                           % pl.get("f13_join_n"))

        r.section("edge (page v5)")
        ok = False
        for att in range(9):
            try:
                pg = http_get("https://justhodl.ai/"
                              "stock-buying.html?cb=%d"
                              % time.time()).decode("utf-8",
                                                    "replace")
                if "ADD ENGINE COLUMNS" in pg \
                        and "SMAs" in pg and "WARN" in pg:
                    ok = True
                    break
            except Exception:
                pass
            time.sleep(20)
        misses += contract(r, "page", ok,
                           "v5 structural tokens at edge")

        r.section("verdict")
        if misses:
            r.fail("v1.4 expansion: %d red" % misses)
            sys.exit(1)
        r.ok("V1.4 EXPANSION LIVE — ladder+GC, warnings, "
             "census binds, cat6 badges, 13F join, engine "
             "dropdown · stock-buying.html")


if __name__ == "__main__":
    main()
