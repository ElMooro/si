"""ops 4630 — BLACKSWAN BAROMETER + TE join + ffill composites.

Khalid: one barometer summarizing the strip ops 4633 — v1.6.0 census attack r3 (v1.6.1): cache-first, 429-safe, poisoned-miss purge + FX inversion + FRED twins + negative-cached heuristic.
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
    with report("4633_census_attack") as r:
        r.heading("ops 4630 — barometer + TE join")

        r.section("deploy-settle")
        ok_b = ok_p = False
        for att in range(16):
            try:
                gf = lam.get_function(FunctionName=BFN)
                zb = http_get(gf["Code"]["Location"], 60)
                src = zipfile.ZipFile(io.BytesIO(zb)).read(
                    "lambda_function.py").decode("utf-8", "replace")
                if "justhodl-blackswan-watch v1.6.1" in src:
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
                           "blackswan v1.6.1 + signal v2.1.3")
        if not (ok_b and ok_p):
            sys.exit(1)

        r.section("purge poisoned negative caches")
        purged = 0
        try:
            pag = s3.get_paginator("list_objects_v2")
            for pg in pag.paginate(Bucket=B,
                                   Prefix="data/warm/blackswan/"
                                          "yh_"):
                for ob in pg.get("Contents") or []:
                    if ob["Size"] < 200:  # miss stubs are tiny
                        try:
                            doc = json.loads(s3.get_object(
                                Bucket=B, Key=ob["Key"])
                                ["Body"].read())
                            if doc.get("miss"):
                                s3.delete_object(Bucket=B,
                                                 Key=ob["Key"])
                                purged += 1
                        except Exception:
                            pass
        except Exception as e:
            r.warn("purge: %s" % str(e)[:80])
        r.log("purged %d poisoned miss stubs" % purged)

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
        r.section("UNRESOLVED CENSUS (the 125, named)")
        unres = [x["symbol"] for x in pl.get("rows") or []
                 if not x.get("resolved")]
        from collections import Counter
        pref = Counter(u.split(":", 1)[0] if ":" in u else "?"
                       for u in unres)
        r.kv(n_unresolved=len(unres),
             by_prefix=json.dumps(pref.most_common(12)))
        for i in range(0, min(len(unres), 120), 6):
            r.log(" · ".join(unres[i:i + 6]))
        misses += contract(r, "census-shrunk", len(unres) <= 100,
                           "%d unresolved (was 125)" % len(unres))
        for sym2 in ("KRX:KOSPI200", "INDEX:FTSEMIB",
                     "EURONEXT:N100", "CBOT:ZB2!", "CME:SR32!",
                     "FX_IDC:INRUSD", "FX_IDC:MXNJPY",
                     "ECONOMICS:USJO"):
            x2 = rows.get(sym2) or {}
            r.log("%-18s %-9s z=%-5s n=%-4s %s"
                  % (sym2, x2.get("move_state", "?"),
                     x2.get("move_z"), x2.get("n_obs", "-"),
                     str(x2.get("via") or "")[:30]))
        cls_ok = sum(1 for sym2 in ("KRX:KOSPI200",
                                    "INDEX:FTSEMIB",
                                    "EURONEXT:N100", "CBOT:ZB2!",
                                    "CME:SR32!", "FX_IDC:INRUSD",
                                    "ECONOMICS:USJO")
                     if (rows.get(sym2) or {}).get("move_z")
                     is not None)
        misses += contract(r, "class-routes", cls_ok >= 5,
                           "%d/7 per-class spot-checks z-based"
                           % cls_ok)
        for sym2 in ("CBOT:ZB1!", "FX_IDC:KRWUSD",
                     "ECONOMICS:CHUR", "NASDAQ:VXUS",
                     "CBOE:VXEEM", "AMEX:VEA"):
            x2 = rows.get(sym2) or {}
            r.log("%-16s %-9s z=%-5s n=%-4s %s"
                  % (sym2, x2.get("move_state", "?"),
                     x2.get("move_z"), x2.get("n_obs", "-"),
                     str(x2.get("via") or "")[:24]))
        alias_z = sum(1 for sym2 in ("CBOT:ZB1!",
                                     "FX_IDC:KRWUSD",
                                     "ECONOMICS:CHUR",
                                     "NASDAQ:VXUS", "AMEX:VEA")
                      if (rows.get(sym2) or {}).get("move_z")
                      is not None)
        misses += contract(r, "alias-z", alias_z >= 3,
                           "%d/5 alias spot-checks on z-basis"
                           % alias_z)
        nh = pl.get("n_with_history") or 0
        misses += contract(r, "history-depth", nh >= 245,
                           "%d rows on statistical basis (steady-state ~330 as the hourly cache compounds)" % nh)
        sf = rows.get("FRED:SOFR-FRED:FEDFUNDS") or {}
        misses += contract(r, "ffill-composite",
                           sf.get("move_z") is not None,
                           "SOFR-FEDFUNDS z-based: z=%s %s"
                           % (sf.get("move_z") or sf.get("detail"),
                              str(sf.get("chg_str"))[:26]))
        misses += contract(r, "resolution",
                           (pl.get("n_resolved") or 0) >= 400,
                           "%s/500 — census-attacked; residue enumerated below" % pl.get("n_resolved"))

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
