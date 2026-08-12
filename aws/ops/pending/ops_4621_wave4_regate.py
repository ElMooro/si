"""ops 4621 — Wave-4 regate: NOAA pipe-split + DTS page depth.

4620: 11/11 new tier-1 OK, but the NOAA parser took only the LAST
pipe column of the US row (1 value, no series) and DTS paginated at
900 rows (~15 days when YoY needs ~380; fiscaldata max is 10000).
v1.3.1 fixes both; ex-weather and withheld_stall auto-heal.
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
    req = urllib.request.Request(url, headers={"User-Agent": "ops-4621"})
    with urllib.request.urlopen(req, timeout=timeout) as h:
        return h.read()


def main():
    misses = 0
    with report("4621_wave4_regate") as r:
        r.heading("ops 4621 — Wave-4 regate")

        r.section("deploy-settle")
        settled = False
        for att in range(16):
            try:
                gf = lam.get_function(FunctionName=CFN)
                zb = http_get(gf["Code"]["Location"], 60)
                src = zipfile.ZipFile(io.BytesIO(zb)).read(
                    "lambda_function.py").decode("utf-8", "replace")
                if "v1.3.1" in src:
                    settled = True
                    r.log("v1.3.1 live (attempt %d)" % (att + 1))
                    break
            except Exception as e:
                r.log("attempt %d: %s" % (att + 1, str(e)[:80]))
            time.sleep(30)
        misses += contract(r, "deploy", settled, "collector v1.3.1")
        if not settled:
            sys.exit(1)

        r.section("collector + healed legs")
        lam.invoke(FunctionName=CFN, InvocationType="RequestResponse")
        for lid in ("noaa_degree_days", "dts_withheld",
                    "dts_customs"):
            env = json.loads(s3.get_object(
                Bucket=B, Key="data/warm/real-economy/%s.json"
                % lid)["Body"].read())
            r.log("%-16s %-9s n=%-4s %s"
                  % (lid, env.get("status"), env.get("n_obs", "-"),
                     str(env.get("detail"))[:80]))
            if lid == "noaa_degree_days":
                misses += contract(r, "noaa",
                                   env.get("status") == "OK"
                                   and (env.get("n_obs") or 0) >= 60,
                                   "daily CDD series n=%s"
                                   % env.get("n_obs"))
            if lid == "dts_withheld":
                misses += contract(r, "dts-depth",
                                   (env.get("n_obs") or 0) >= 240,
                                   "withheld series n=%s (YoY-able)"
                                   % env.get("n_obs"))

        r.section("signal + healed contracts")
        time.sleep(3)
        lam.invoke(FunctionName=PFN, InvocationType="RequestResponse")
        pl = json.loads(s3.get_object(
            Bucket=B,
            Key="data/physical-economy.json")["Body"].read())
        rows = {x["leg_id"]: x for x in pl.get("legs") or []}
        xw = rows.get("us48_exweather") or {}
        wh = rows.get("dts_withheld") or {}
        cans = pl.get("canaries") or {}
        misses += contract(r, "ex-weather",
                           xw.get("expansion_0_100") is not None,
                           "ex-weather %s · %s"
                           % (xw.get("expansion_0_100"),
                              str(xw.get("detail"))[:90]))
        misses += contract(r, "withheld-leg",
                           wh.get("expansion_0_100") is not None,
                           "withheld leg %s · %s"
                           % (wh.get("expansion_0_100"),
                              str(wh.get("detail"))[:80]))
        misses += contract(r, "withheld-canary",
                           "withheld_stall" in cans,
                           "withheld_stall: %s"
                           % json.dumps(cans.get("withheld_stall")
                                        or {})[:120])
        misses += contract(r, "coverage",
                           (pl.get("n_live_legs") or 0) >= 32,
                           "%s live legs" % pl.get("n_live_legs"))
        r.kv(composite=pl.get("composite_score"),
             label=pl.get("composite_label"),
             subs=json.dumps({k: (v or {}).get("score") for k, v in
                              (pl.get("sub_pillars") or {}).items()}),
             canaries=json.dumps({k: (v or {}).get("state")
                                  for k, v in cans.items()}))

        r.section("edge")
        fresh = False
        for att in range(8):
            try:
                jd = json.loads(http_get(
                    "https://justhodl.ai/data/physical-economy.json"
                    "?cb=%d" % time.time()))
                rr = {x["leg_id"]: x
                      for x in jd.get("legs") or []}
                if (rr.get("us48_exweather") or {}).get(
                        "expansion_0_100") is not None:
                    fresh = True
                    break
            except Exception as e:
                r.log("edge %d: %s" % (att + 1, str(e)[:70]))
            time.sleep(20)
        misses += contract(r, "edge", fresh,
                           "edge serves the ex-weather leg")

        r.section("verdict")
        if misses:
            r.fail("regate: %d red" % misses)
            sys.exit(1)
        r.ok("WAVE 4 COMPLETE — %s legs, composite %s (%s); "
             "ex-weather live, wage canary armed"
             % (pl.get("n_live_legs"), pl.get("composite_score"),
                pl.get("composite_label")))


if __name__ == "__main__":
    main()
