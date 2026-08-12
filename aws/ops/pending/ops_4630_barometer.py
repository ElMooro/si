"""ops 4630 — BLACKSWAN BAROMETER + TE join + ffill composites.

Khalid: one barometer summarizing the strip, and pull the missing
rows from his engines/providers. v1.4.0: (1) 0-100 tail-stress
barometer — 45% breadth of >=2-sigma shocks + 40% breadth of 1y
range extremes + 15% stretched, with components + top extremes;
(2) te-feed join: ECONOMICS:* codes map bare to Khalid's paid
Trading Economics prices{} (value+asof+unit); (3) composite legs
forward-filled — SOFR-FEDFUNDS class finally z-based. Signal v2.1.3
carries barometer on the physical board; page shows the dial.
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

BFN = "justhodl-blackswan-watch"
PFN = "justhodl-physical-econ"
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
    req = urllib.request.Request(url, headers={"User-Agent": "ops-4630"})
    with urllib.request.urlopen(req, timeout=timeout) as h:
        return h.read()


def main():
    misses = 0
    with report("4630_barometer") as r:
        r.heading("ops 4630 — barometer + TE join")

        r.section("deploy-settle")
        ok_b = ok_p = False
        for att in range(16):
            try:
                gf = lam.get_function(FunctionName=BFN)
                zb = http_get(gf["Code"]["Location"], 60)
                src = zipfile.ZipFile(io.BytesIO(zb)).read(
                    "lambda_function.py").decode("utf-8", "replace")
                if "justhodl-blackswan-watch v1.4.0" in src:
                    ok_b = True
                    break
            except Exception as e:
                r.log("settle %d: %s" % (att + 1, str(e)[:60]))
            time.sleep(30)
        for att in range(6):
            try:
                gf = lam.get_function(FunctionName=PFN)
                zb = http_get(gf["Code"]["Location"], 60)
                src = zipfile.ZipFile(io.BytesIO(zb)).read(
                    "lambda_function.py").decode("utf-8", "replace")
                if "v2.1.3" in src:
                    ok_p = True
                    break
            except Exception:
                pass
            time.sleep(30)
        misses += contract(r, "deploy", ok_b and ok_p,
                           "blackswan v1.4.0 + signal v2.1.3")
        if not (ok_b and ok_p):
            sys.exit(1)

        r.section("run + barometer truth")
        lam.invoke(FunctionName=BFN, InvocationType="RequestResponse")
        pl = json.loads(s3.get_object(
            Bucket=B, Key="data/blackswan-watch.json")["Body"].read())
        bm = pl.get("barometer") or {}
        comp = bm.get("components") or {}
        rows = {x["symbol"]: x for x in pl.get("rows") or []}
        r.kv(barometer=bm.get("value"), label=bm.get("label"),
             shock=comp.get("shock_breadth_pct"),
             extreme=comp.get("range_extreme_pct"),
             stretched=comp.get("range_stretched_pct"),
             resolved=pl.get("n_resolved"),
             top_extremes=json.dumps(bm.get("top_extremes")
                                     or [])[:200])
        misses += contract(r, "barometer",
                           isinstance(bm.get("value"), (int, float))
                           and 0 <= bm["value"] <= 100
                           and bm.get("label") in (
                               "QUIET", "WATCHFUL", "ELEVATED",
                               "HIGH", "CRITICAL"),
                           "barometer %s (%s)"
                           % (bm.get("value"), bm.get("label")))
        te_rows = [x["symbol"] for x in pl.get("rows") or []
                   if x.get("via") == "TE latest"]
        r.log("TE-joined: %s" % te_rows[:12])
        misses += contract(r, "te-join", len(te_rows) >= 15,
                           "%d ECONOMICS rows via Trading Economics"
                           % len(te_rows))
        sf = rows.get("FRED:SOFR-FRED:FEDFUNDS") or {}
        misses += contract(r, "ffill-composite",
                           sf.get("move_z") is not None,
                           "SOFR-FEDFUNDS z-based: z=%s %s"
                           % (sf.get("move_z"),
                              str(sf.get("chg_str"))[:26]))
        misses += contract(r, "resolution",
                           (pl.get("n_resolved") or 0) >= 385,
                           "%s/500 resolved" % pl.get("n_resolved"))

        r.section("board + edge")
        time.sleep(3)
        lam.invoke(FunctionName=PFN, InvocationType="RequestResponse")
        pe = json.loads(s3.get_object(
            Bucket=B,
            Key="data/physical-economy.json")["Body"].read())
        cb = (pe.get("canaries") or {}).get("blackswan_strip") or {}
        misses += contract(r, "canary-barometer",
                           cb.get("barometer") == bm.get("value"),
                           "board carries barometer %s (%s)"
                           % (cb.get("barometer"),
                              cb.get("barometer_label")))
        fresh = False
        for att in range(8):
            try:
                jd = json.loads(http_get(
                    "https://justhodl.ai/data/blackswan-watch.json"
                    "?cb=%d" % time.time()))
                if (jd.get("barometer") or {}).get(
                        "value") is not None:
                    fresh = True
                    break
            except Exception as e:
                r.log("edge %d: %s" % (att + 1, str(e)[:70]))
            time.sleep(20)
        misses += contract(r, "edge", fresh,
                           "edge serves the barometer")

        r.section("verdict")
        if misses:
            r.fail("barometer: %d red" % misses)
            sys.exit(1)
        r.ok("BAROMETER LIVE — %s (%s): shock %s%% / extreme %s%% "
             "/ stretched %s%% · %s/500 resolved (+TE join, ffill "
             "composites) · on the physical board and the page"
             % (bm.get("value"), bm.get("label"),
                comp.get("shock_breadth_pct"),
                comp.get("range_extreme_pct"),
                comp.get("range_stretched_pct"),
                pl.get("n_resolved")))


if __name__ == "__main__":
    main()
