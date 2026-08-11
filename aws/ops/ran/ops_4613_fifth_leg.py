"""ops 4613 — fifth leg closes: port-cargo exact-field join.

4612 healed the upstream (fetch OK, 2065 ports, 4d age) but the
physical join still missed: the discovery regex had pct_ch where the
field is total_chg_pct. v1.0.2 joins by exact fields —
seasonal_chg_pct (same-week vs prior years) first, total_chg_pct
fallback — gated on fetch_status OK. Contracts the full 5-leg signal.
"""
import io
import json
import os
import sys
import time
import urllib.request
import zipfile

import boto3
from botocore.config import Config

from ops_report import report

PFN = "justhodl-physical-econ"
MFN = "justhodl-market-machine"
B = "justhodl-dashboard-live"
lam = boto3.client("lambda", region_name="us-east-1",
                   config=Config(read_timeout=300,
                                 retries={"max_attempts": 1}))
s3 = boto3.client("s3", region_name="us-east-1")


def contract(r, name, cond, why):
    if cond:
        r.ok("  [%s] %s" % (name, why))
        return 0
    r.fail("  [%s] CONTRACT MISS — %s" % (name, why))
    return 1


def http_get(url, timeout=45):
    req = urllib.request.Request(url, headers={"User-Agent": "ops-4613"})
    with urllib.request.urlopen(req, timeout=timeout) as h:
        return h.read()


def main():
    misses = 0
    with report("4613_fifth_leg") as r:
        r.heading("ops 4613 — fifth leg: port-cargo exact join")

        r.section("deploy-settle v1.0.2")
        settled = False
        for att in range(16):
            try:
                gf = lam.get_function(FunctionName=PFN)
                zb = http_get(gf["Code"]["Location"], 60)
                src = zipfile.ZipFile(io.BytesIO(zb)).read(
                    "lambda_function.py").decode("utf-8", "replace")
                if "v1.0.2" in src and "seasonal_chg_pct" in src:
                    settled = True
                    r.log("v1.0.2 live (attempt %d)" % (att + 1))
                    break
            except Exception as e:
                r.log("attempt %d: %s" % (att + 1, str(e)[:80]))
            time.sleep(30)
        misses += contract(r, "deploy", settled,
                           "physical-econ v1.0.2")
        if not settled:
            sys.exit(1)

        r.section("invoke + 5-leg contracts")
        inv = lam.invoke(FunctionName=PFN,
                         InvocationType="RequestResponse")
        misses += contract(r, "invoke",
                           inv.get("StatusCode") == 200,
                           "physical-econ invoked")
        pe = json.loads(s3.get_object(
            Bucket=B,
            Key="data/physical-economy.json")["Body"].read())
        comps = pe.get("components") or []
        names = [x.get("name", "") for x in comps]
        port = next((x for x in comps if "Port" in x.get("name", "")),
                    None)
        misses += contract(r, "legs", len(comps) >= 5,
                           "%d legs: %s" % (len(comps),
                                            json.dumps(names)[:260]))
        misses += contract(r, "port-leg",
                           port is not None
                           and port.get("expansion_0_100") is not None,
                           "port leg live: %s"
                           % json.dumps(port or {})[:200])
        sig = pe.get("trade_signal") or {}
        misses += contract(r, "confidence",
                           sig.get("confidence") == "HIGH",
                           "signal %s at %s · composite %s"
                           % (sig.get("signal"),
                              sig.get("confidence"),
                              pe.get("composite_score")))
        time.sleep(3)
        lam.invoke(FunctionName=MFN, InvocationType="RequestResponse")
        mm = json.loads(s3.get_object(
            Bucket=B, Key="data/market-machine.json")["Body"].read())
        p1 = ((mm.get("pillars") or {}).get("profits") or {})
        misses += contract(r, "machine",
                           (p1.get("n_contributors") or 0) >= 4,
                           "machine P1 n=%s score=%s · composite %s"
                           % (p1.get("n_contributors"),
                              p1.get("score"),
                              mm.get("composite_score")))

        r.section("edge")
        fresh = False
        for att in range(8):
            try:
                jd = json.loads(http_get(
                    "https://justhodl.ai/data/physical-economy.json"
                    "?cb=%d" % time.time()))
                if (jd.get("n_components") or 0) >= 5:
                    fresh = True
                    break
            except Exception as e:
                r.log("edge %d: %s" % (att + 1, str(e)[:70]))
            time.sleep(20)
        misses += contract(r, "edge", fresh, "edge shows 5 legs")

        r.section("verdict")
        if misses:
            r.fail("fifth leg: %d red" % misses)
            sys.exit(1)
        r.ok("PHYSICAL ECONOMY COMPLETE — all 5 legs live (%s, %s "
             "conf, composite %s); port tonnage joined on its exact "
             "seasonal basis"
             % (sig.get("signal"), sig.get("confidence"),
                pe.get("composite_score")))


if __name__ == "__main__":
    main()
