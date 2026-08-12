"""ops 4617 — pattern-not-found legs fixed + copper basis bug.

Khalid flagged the DEGRADED tier-3 legs. Root fixes: link-follow
resolution (AAR news listing -> weekly release page, BTS-via-FRED
guaranteed fallback; Destatis CSV href harvested off the page), ACC
CAB URL candidates, stooq mirror. And an owned malfunction from the
pasted page: the copper FRED fallback ($/tonne MONTHLY) ran through
the daily window, mislabeled "1w vs 1m", and pinned the leg at 100 —
v2.0.1 is basis-aware. Rail + toll promoted to scored trade legs.
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
    req = urllib.request.Request(url, headers={"User-Agent": "ops-4617"})
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
    with report("4617_t3_fix") as r:
        r.heading("ops 4617 — pattern-not-found fixes + copper basis")

        r.section("deploy-settle")
        ok_c = settle(r, CFN, "v1.2.0")
        ok_p = settle(r, PFN, "v2.0.1")
        misses += contract(r, "deploy", ok_c and ok_p,
                           "collector v1.2.0 + signal v2.0.1")
        if not (ok_c and ok_p):
            sys.exit(1)

        r.section("collector run + fixed-leg truth")
        lam.invoke(FunctionName=CFN, InvocationType="RequestResponse")
        legs = {}
        for lid in ("aar_rail", "destatis_toll", "acc_cab", "copper"):
            env = json.loads(s3.get_object(
                Bucket=B, Key="data/warm/real-economy/%s.json"
                % lid)["Body"].read())
            legs[lid] = env
            r.log("%-14s %-9s %-38s %s"
                  % (lid, env.get("status"),
                     str(env.get("source"))[:38],
                     str(env.get("detail"))[:90]))
        misses += contract(r, "aar", legs["aar_rail"].get(
            "status") == "OK",
            "rail OK via %s" % legs["aar_rail"].get("source"))
        de = legs["destatis_toll"]
        misses += contract(r, "destatis-resolved",
                           "pattern not found" not in str(
                               de.get("detail")),
                           "toll resolver ran (status=%s, %s)"
                           % (de.get("status"),
                              str(de.get("detail"))[:80]))
        if de.get("status") != "OK":
            r.warn("destatis still degraded — detail above is the "
                   "next-patch evidence")
        misses += contract(r, "copper-src",
                           legs["copper"].get("status") == "OK",
                           "copper OK via %s"
                           % legs["copper"].get("source"))

        r.section("signal v2.0.1 + basis-bug contracts")
        time.sleep(3)
        lam.invoke(FunctionName=PFN, InvocationType="RequestResponse")
        pl = json.loads(s3.get_object(
            Bucket=B,
            Key="data/physical-economy.json")["Body"].read())
        rows = {x["leg_id"]: x for x in pl.get("legs") or []}
        cu = rows.get("copper") or {}
        misses += contract(r, "copper-basis",
                           cu.get("expansion_0_100") is not None
                           and cu["expansion_0_100"] <= 95
                           and ("tonne" in str(cu.get("detail"))
                                or "/lb" in str(cu.get("detail"))),
                           "copper leg sane: %s · %s"
                           % (cu.get("expansion_0_100"),
                              str(cu.get("detail"))[:80]))
        mat = (pl.get("sub_pillars") or {}).get("materials") or {}
        misses += contract(r, "materials-sane",
                           mat.get("score") is not None
                           and mat["score"] <= 90,
                           "materials %s (was 81.7 on the bugged "
                           "basis)" % mat.get("score"))
        rl = rows.get("aar_rail") or {}
        misses += contract(r, "rail-leg",
                           rl.get("status") in ("OK", "NO_SIGNAL"),
                           "rail leg %s: %s · %s"
                           % (rl.get("status"),
                              rl.get("expansion_0_100"),
                              str(rl.get("detail"))[:70]))
        tt = (pl.get("sub_pillars") or {}).get("trade_transport") or {}
        misses += contract(r, "trade-depth",
                           (tt.get("n_total") or 0) >= 7,
                           "trade sub-pillar %s/%s"
                           % (tt.get("n_live"), tt.get("n_total")))
        misses += contract(r, "coverage",
                           (pl.get("n_live_legs") or 0) >= 22,
                           "%s live legs" % pl.get("n_live_legs"))
        r.kv(composite=pl.get("composite_score"),
             label=pl.get("composite_label"),
             subs=json.dumps({k: (v or {}).get("score") for k, v in
                              (pl.get("sub_pillars")
                               or {}).items()}))

        r.section("edge")
        fresh = False
        for att in range(8):
            try:
                jd = json.loads(http_get(
                    "https://justhodl.ai/data/physical-economy.json"
                    "?cb=%d" % time.time()))
                rr = {x["leg_id"]: x for x in jd.get("legs") or []}
                cue = rr.get("copper") or {}
                if (cue.get("expansion_0_100") is not None
                        and cue["expansion_0_100"] <= 95):
                    fresh = True
                    break
            except Exception as e:
                r.log("edge %d: %s" % (att + 1, str(e)[:70]))
            time.sleep(20)
        misses += contract(r, "edge", fresh,
                           "edge serves the corrected copper basis")

        r.section("verdict")
        if misses:
            r.fail("t3 fix: %d red" % misses)
            sys.exit(1)
        r.ok("FIXED — rail via %s, toll resolver live, copper "
             "basis-aware (%s), composite %s (%s), %s legs"
             % (legs["aar_rail"].get("source"), cu.get("detail"),
                pl.get("composite_score"), pl.get("composite_label"),
                pl.get("n_live_legs")))


if __name__ == "__main__":
    main()
