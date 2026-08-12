"""ops 4615 — collector v1.1.0 regate; supersedes 4614's single red.

Per the 4614 truth table: EIA-930 moved to the native daily route
(hourly form returned 0), chokepoints gained date-format negotiation
(the port-cargo lesson), copper got a FRED PCOPPUSDM fallback, NOAA
parser now records its miss reason. Contracts: chokepoints +
eia930 x2 OK, tier-1 >=12/13, chokepoint_shock canary armed,
coverage >=20 live legs.
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
    req = urllib.request.Request(url, headers={"User-Agent": "ops-4615"})
    with urllib.request.urlopen(req, timeout=timeout) as h:
        return h.read()


def main():
    misses = 0
    with report("4615_collector_fix") as r:
        r.heading("ops 4615 — collector v1.1.0 regate")

        r.section("deploy-settle")
        settled = False
        for att in range(16):
            try:
                gf = lam.get_function(FunctionName=CFN)
                zb = http_get(gf["Code"]["Location"], 60)
                src = zipfile.ZipFile(io.BytesIO(zb)).read(
                    "lambda_function.py").decode("utf-8", "replace")
                if "v1.1.0" in src and "native daily route" in src:
                    settled = True
                    r.log("v1.1.0 live (attempt %d)" % (att + 1))
                    break
            except Exception as e:
                r.log("attempt %d: %s" % (att + 1, str(e)[:80]))
            time.sleep(30)
        misses += contract(r, "deploy", settled, "collector v1.1.0")
        if not settled:
            sys.exit(1)

        r.section("collector run + fixed-leg contracts")
        lam.invoke(FunctionName=CFN, InvocationType="RequestResponse")
        summ = json.loads(s3.get_object(
            Bucket=B,
            Key="data/warm/real-economy/_summary.json")["Body"].read())
        legs = summ.get("legs") or {}
        for lid in ("chokepoints", "eia930_us48", "eia930_ercot",
                    "copper", "noaa_degree_days"):
            env = json.loads(s3.get_object(
                Bucket=B, Key="data/warm/real-economy/%s.json"
                % lid)["Body"].read())
            r.log("%-16s %-9s %s" % (lid, env.get("status"),
                                     str(env.get("detail"))[:110]))
        misses += contract(r, "chokepoints",
                           legs.get("chokepoints") == "OK",
                           "chokepoints OK")
        misses += contract(r, "eia930",
                           legs.get("eia930_us48") == "OK"
                           and legs.get("eia930_ercot") == "OK",
                           "US48 + ERCOT daily demand OK")
        t1 = [lid for lid in legs
              if lid.startswith(("eia", "wti", "fred", "chokepoints",
                                 "indeed"))]
        t1_ok = sum(1 for lid in t1 if legs[lid] == "OK")
        misses += contract(r, "tier1", t1_ok >= 12,
                           "tier-1 OK: %d/%d" % (t1_ok, len(t1)))
        r.kv(counts=json.dumps(summ.get("counts")))

        r.section("signal + canary + coverage")
        time.sleep(3)
        lam.invoke(FunctionName=PFN, InvocationType="RequestResponse")
        pl = json.loads(s3.get_object(
            Bucket=B,
            Key="data/physical-economy.json")["Body"].read())
        cans = pl.get("canaries") or {}
        misses += contract(r, "chokepoint-canary",
                           "chokepoint_shock" in cans,
                           "chokepoint_shock: %s"
                           % json.dumps(cans.get("chokepoint_shock")
                                        or {})[:120])
        misses += contract(r, "coverage",
                           (pl.get("n_live_legs") or 0) >= 20,
                           "%s live legs" % pl.get("n_live_legs"))
        subs = pl.get("sub_pillars") or {}
        r.kv(composite=pl.get("composite_score"),
             label=pl.get("composite_label"),
             subs=json.dumps({k: v.get("score")
                              for k, v in subs.items()}))
        misses += contract(r, "energy-depth",
                           (subs.get("energy") or {}).get(
                               "n_live", 0) >= 8,
                           "energy sub-pillar %s/%s live"
                           % ((subs.get("energy") or {}).get("n_live"),
                              (subs.get("energy") or {}).get(
                                  "n_total")))

        r.section("edge")
        fresh = False
        for att in range(8):
            try:
                jd = json.loads(http_get(
                    "https://justhodl.ai/data/physical-economy.json"
                    "?cb=%d" % time.time()))
                if (jd.get("n_live_legs") or 0) >= 20:
                    fresh = True
                    break
            except Exception as e:
                r.log("edge %d: %s" % (att + 1, str(e)[:70]))
            time.sleep(20)
        misses += contract(r, "edge", fresh, "edge shows >=20 legs")

        r.section("verdict")
        if misses:
            r.fail("collector fix: %d red" % misses)
            sys.exit(1)
        r.ok("REAL ECONOMY COMPLETE — %s legs live, composite %s "
             "(%s), all doctrine canaries armed"
             % (pl.get("n_live_legs"), pl.get("composite_score"),
                pl.get("composite_label")))


if __name__ == "__main__":
    main()
