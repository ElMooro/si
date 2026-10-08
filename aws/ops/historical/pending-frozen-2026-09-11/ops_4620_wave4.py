"""ops 4620 — WAVE 4: DTS wages/customs, EIA depth, weather
adjustment, WEI replication benchmark.

Collector v1.3.0 + signal v2.1.0. New scored legs: withheld taxes
(daily wage read), customs duties (imports proxy), US exports,
Cushing, NG storage vs 5y, gas burn, core capex, inventories/sales,
manufacturing hours, construction spending, US-48 EX-WEATHER
(regression residual). New canaries: cushing_squeeze,
withheld_stall, wei_divergence. FBX + WEI observed-only. Contracts:
new tier-1 legs >=8 OK, ex-weather computed, canaries armed,
coverage >=30, all sub-pillars regress, edge.
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

CFN = "justhodl-real-economy-collector"
PFN = "justhodl-physical-econ"
B = "justhodl-dashboard-live"
lam = boto3.client("lambda", region_name="us-east-1",
                   config=Config(read_timeout=600,
                                 retries={"max_attempts": 1}))
s3 = boto3.client("s3", region_name="us-east-1")


def contract(r, name, cond, why):
    if cond:
        r.ok("  [%s] %s" % (name, why))
        return 0
    r.fail("  [%s] CONTRACT MISS — %s" % (name, why))
    return 1


def http_get(url, timeout=45):
    req = urllib.request.Request(url, headers={"User-Agent": "ops-4620"})
    with urllib.request.urlopen(req, timeout=timeout) as h:
        return h.read()


def settle(r, fn, marker):
    for att in range(16):
        try:
            gf = lam.get_function(FunctionName=fn)
            zb = http_get(gf["Code"]["Location"], 60)
            src = zipfile.ZipFile(io.BytesIO(zb)).read(
                "lambda_function.py").decode("utf-8", "replace")
            if marker in src:
                r.log("%s carries %s" % (fn, marker))
                return True
        except Exception as e:
            r.log("%s attempt %d: %s" % (fn, att + 1, str(e)[:80]))
        time.sleep(30)
    return False


def main():
    misses = 0
    with report("4620_wave4") as r:
        r.heading("ops 4620 — Wave 4 depth build")

        r.section("deploy-settle")
        ok_c = settle(r, CFN, "v1.3.0")
        ok_p = settle(r, PFN, "v2.1.0")
        misses += contract(r, "deploy", ok_c and ok_p,
                           "collector v1.3.0 + signal v2.1.0")
        if not (ok_c and ok_p):
            sys.exit(1)

        r.section("collector run + new-leg truth table")
        lam.invoke(FunctionName=CFN, InvocationType="RequestResponse")
        newlegs = ["dts_withheld", "dts_customs", "eia_cushing",
                   "eia_ng_storage", "eia_exports", "eia930_gasburn",
                   "fred_wei", "fred_neworder", "fred_isratio",
                   "fred_awhman", "fred_ttlcons",
                   "noaa_degree_days", "fbx"]
        status = {}
        for lid in newlegs:
            try:
                env = json.loads(s3.get_object(
                    Bucket=B, Key="data/warm/real-economy/%s.json"
                    % lid)["Body"].read())
                status[lid] = env.get("status")
                r.log("%-16s %-9s n=%-4s %s"
                      % (lid, env.get("status"),
                         env.get("n_obs", "-"),
                         str(env.get("detail"))[:85]))
            except Exception as e:
                status[lid] = "ABSENT"
                r.log("%-16s ABSENT %s" % (lid, str(e)[:60]))
        t1_new = ["dts_withheld", "dts_customs", "eia_cushing",
                  "eia_ng_storage", "eia_exports", "eia930_gasburn",
                  "fred_wei", "fred_neworder", "fred_isratio",
                  "fred_awhman", "fred_ttlcons"]
        n_ok = sum(1 for x in t1_new if status.get(x) == "OK")
        misses += contract(r, "new-tier1", n_ok >= 8,
                           "new tier-1 legs OK: %d/%d"
                           % (n_ok, len(t1_new)))
        misses += contract(r, "noaa-series",
                           status.get("noaa_degree_days") == "OK",
                           "NOAA daily CDD series landed")

        r.section("signal v2.1.0 + contracts")
        time.sleep(3)
        lam.invoke(FunctionName=PFN, InvocationType="RequestResponse")
        pl = json.loads(s3.get_object(
            Bucket=B,
            Key="data/physical-economy.json")["Body"].read())
        rows = {x["leg_id"]: x for x in pl.get("legs") or []}
        xw = rows.get("us48_exweather") or {}
        misses += contract(r, "ex-weather",
                           xw.get("expansion_0_100") is not None,
                           "ex-weather leg: %s · %s"
                           % (xw.get("expansion_0_100"),
                              str(xw.get("detail"))[:90]))
        cans = pl.get("canaries") or {}
        for cn in ("cushing_squeeze", "withheld_stall",
                   "wei_divergence"):
            misses += contract(r, "canary-" + cn, cn in cans,
                               "%s: %s"
                               % (cn, json.dumps(cans.get(cn)
                                                 or {})[:130]))
        misses += contract(r, "coverage",
                           (pl.get("n_live_legs") or 0) >= 30,
                           "%s live legs" % pl.get("n_live_legs"))
        subs = pl.get("sub_pillars") or {}
        for sp in ("energy", "trade_transport", "materials",
                   "labor", "construction"):
            d = subs.get(sp) or {}
            misses += contract(r, "sub-" + sp,
                               d.get("score") is not None,
                               "%s %s (%s/%s live)"
                               % (sp, d.get("score"),
                                  d.get("n_live"), d.get("n_total")))
        comp = pl.get("composite_score")
        misses += contract(r, "composite",
                           comp is not None and 0 <= comp <= 100,
                           "composite %s (%s)"
                           % (comp, pl.get("composite_label")))
        bad = [x["leg_id"] for x in pl.get("legs") or []
               if x.get("status") not in ("OK",)
               and x.get("expansion_0_100") is not None]
        misses += contract(r, "bug-gate", not bad,
                           "no non-OK leg scored (%s)"
                           % (bad or "none"))
        r.kv(subs=json.dumps({k: (v or {}).get("score")
                              for k, v in subs.items()}),
             canaries=json.dumps({k: (v or {}).get("state")
                                  for k, v in cans.items()}))

        r.section("edge")
        fresh = False
        for att in range(8):
            try:
                jd = json.loads(http_get(
                    "https://justhodl.ai/data/physical-economy.json"
                    "?cb=%d" % time.time()))
                if (jd.get("n_live_legs") or 0) >= 30:
                    fresh = True
                    break
            except Exception as e:
                r.log("edge %d: %s" % (att + 1, str(e)[:70]))
            time.sleep(20)
        misses += contract(r, "edge", fresh, "edge shows >=30 legs")

        r.section("verdict")
        if misses:
            r.fail("wave 4: %d red (truth table above)" % misses)
            sys.exit(1)
        r.ok("WAVE 4 LIVE — %s legs, composite %s (%s), ex-weather "
             "computed, cushing/withheld/WEI canaries armed"
             % (pl.get("n_live_legs"), comp,
                pl.get("composite_label")))


if __name__ == "__main__":
    main()
