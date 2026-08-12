"""ops 4618 — copper calibration + Destatis GENESIS guest path.

4617 evidence: the copper basis fix worked (monthly window, $/tonne)
but a real +23.1%/3m move saturated k=2.5 to 100 — calibration, not
data. v2.0.2 sets monthly-commodity k=1.2. Destatis EXDAT pages carry
no CSV href, so v1.2.1 tries the documented GENESIS guest API
(table 42191-0001) first; if the guest quota refuses, DEGRADED stays
honest. Contracts: copper in (5,95) with tonne label, materials <=90,
edge fresh, coverage >=23; destatis soft-checked with evidence.
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
    req = urllib.request.Request(url, headers={"User-Agent": "ops-4618"})
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
    with report("4618_calibration") as r:
        r.heading("ops 4618 — copper k=1.2 + Destatis GENESIS")

        r.section("deploy-settle")
        ok_c = settle(r, CFN, "v1.2.1")
        ok_p = settle(r, PFN, "v2.0.2")
        misses += contract(r, "deploy", ok_c and ok_p,
                           "collector v1.2.1 + signal v2.0.2")
        if not (ok_c and ok_p):
            sys.exit(1)

        r.section("invoke chain")
        lam.invoke(FunctionName=CFN, InvocationType="RequestResponse")
        de = json.loads(s3.get_object(
            Bucket=B,
            Key="data/warm/real-economy/destatis_toll.json")
            ["Body"].read())
        r.log("destatis: %s via %s — %s"
              % (de.get("status"), de.get("source"),
                 str(de.get("detail"))[:110]))
        if de.get("status") == "OK":
            r.ok("  [destatis] GENESIS guest path LIVE (%s obs)"
                 % de.get("n_obs"))
        else:
            r.warn("destatis still degraded (guest quota or table "
                   "shape) — honest, observed-only impact")
        time.sleep(3)
        lam.invoke(FunctionName=PFN, InvocationType="RequestResponse")
        pl = json.loads(s3.get_object(
            Bucket=B,
            Key="data/physical-economy.json")["Body"].read())
        rows = {x["leg_id"]: x for x in pl.get("legs") or []}
        cu = rows.get("copper") or {}
        misses += contract(r, "copper",
                           cu.get("expansion_0_100") is not None
                           and 5 <= cu["expansion_0_100"] <= 95
                           and "tonne" in str(cu.get("detail")),
                           "copper %s · %s"
                           % (cu.get("expansion_0_100"),
                              str(cu.get("detail"))[:80]))
        mat = (pl.get("sub_pillars") or {}).get("materials") or {}
        misses += contract(r, "materials",
                           mat.get("score") is not None
                           and mat["score"] <= 90,
                           "materials %s" % mat.get("score"))
        misses += contract(r, "coverage",
                           (pl.get("n_live_legs") or 0) >= 23,
                           "%s live legs" % pl.get("n_live_legs"))
        r.kv(composite=pl.get("composite_score"),
             label=pl.get("composite_label"),
             subs=json.dumps({k: (v or {}).get("score") for k, v in
                              (pl.get("sub_pillars") or {}).items()}))

        r.section("edge")
        fresh = False
        for att in range(8):
            try:
                jd = json.loads(http_get(
                    "https://justhodl.ai/data/physical-economy.json"
                    "?cb=%d" % time.time()))
                rr = {x["leg_id"]: x for x in jd.get("legs") or []}
                ce = rr.get("copper") or {}
                if (ce.get("expansion_0_100") is not None
                        and ce["expansion_0_100"] <= 95):
                    fresh = True
                    break
            except Exception as e:
                r.log("edge %d: %s" % (att + 1, str(e)[:70]))
            time.sleep(20)
        misses += contract(r, "edge", fresh,
                           "edge serves calibrated copper")

        r.section("verdict")
        if misses:
            r.fail("calibration: %d red" % misses)
            sys.exit(1)
        r.ok("CALIBRATED — copper %s (real +23%%/3m move, sane "
             "scale), materials %s, composite %s (%s), %s legs; "
             "destatis %s"
             % (cu.get("expansion_0_100"), mat.get("score"),
                pl.get("composite_score"), pl.get("composite_label"),
                pl.get("n_live_legs"), de.get("status")))


if __name__ == "__main__":
    main()
