"""ops 4611 — physical wiring regate; supersedes 4610's two reds.

Root causes fixed: (1) scheduled pjm-grid run collided with the test
invoke on PJM's ~6/min non-member limit and republished with an empty
lmp block — v1.0.1 keeps the prior run's block flagged stale, and the
physical join now falls back to the canaries block for the shock leg;
(2) port-cargo metrics sit deeper than depth-1 — regex-deep discovery
added; (3) grid-queue exposes stocks not momentum — the leg is now
executed-IA share of the headline queue, a true 0-100 ratio.
Deep (depth-2) shape-dump included for any final tightening.
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
GFN = "justhodl-pjm-grid"
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
    req = urllib.request.Request(url, headers={"User-Agent": "ops-4611"})
    with urllib.request.urlopen(req, timeout=timeout) as h:
        return h.read()


def shape(obj, depth=0):
    if depth > 2:
        return type(obj).__name__
    if isinstance(obj, dict):
        return {k: shape(v, depth + 1)
                for k, v in list(obj.items())[:20]}
    if isinstance(obj, list):
        return ["list[%d]" % len(obj),
                shape(obj[0], depth + 1) if obj else "empty"]
    return (repr(obj)[:24] if isinstance(obj, str)
            else type(obj).__name__)


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
    with report("4611_physical_regate") as r:
        r.heading("ops 4611 — physical wiring regate")

        r.section("deep shape-dump (depth 2)")
        for k in ("data/port-cargo.json", "data/grid-queue.json"):
            try:
                d = json.loads(s3.get_object(
                    Bucket=B, Key=k)["Body"].read())
                r.log("%s → %s" % (k, json.dumps(shape(d))[:900]))
            except Exception as e:
                r.log("%s → ABSENT (%s)" % (k, str(e)[:70]))

        r.section("deploy-settle")
        ok_g = settle(r, GFN, "justhodl-pjm-grid v1.0.1")
        ok_p = settle(r, PFN, "justhodl-physical-econ v1.0.1")
        misses += contract(r, "deploy-pjm", ok_g, "pjm-grid v1.0.1")
        misses += contract(r, "deploy-phys", ok_p,
                           "physical-econ v1.0.1")
        if not (ok_g and ok_p):
            sys.exit(1)

        r.section("invoke chain (spaced for the PJM rate limit)")
        inv = lam.invoke(FunctionName=GFN,
                         InvocationType="RequestResponse")
        gb = {}
        try:
            gb = json.loads(json.loads(
                inv["Payload"].read().decode()).get("body") or "{}")
        except Exception:
            pass
        r.kv(pjm_invoke=json.dumps(gb)[:220])
        misses += contract(r, "pjm-ok", bool(gb.get("ok")),
                           "pjm-grid ok:true (lmp shock=%s)"
                           % gb.get("lmp_shock"))
        time.sleep(5)
        inv = lam.invoke(FunctionName=PFN,
                         InvocationType="RequestResponse")
        pb = {}
        try:
            pb = json.loads(json.loads(
                inv["Payload"].read().decode()).get("body") or "{}")
        except Exception:
            pass
        misses += contract(r, "phys-ok", bool(pb.get("ok")),
                           "physical-econ ok:true")

        pl = json.loads(s3.get_object(
            Bucket=B, Key="data/physical-economy.json")["Body"].read())
        comps = pl.get("components") or []
        names = [x.get("name", "") for x in comps]
        pjm_legs = sum(1 for n in names if "PJM" in n)
        misses += contract(r, "components", len(comps) >= 4,
                           "%d of 5 legs: %s"
                           % (len(comps), json.dumps(names)[:260]))
        misses += contract(r, "pjm-legs", pjm_legs >= 2,
                           "both PJM legs (found %d)" % pjm_legs)
        sig = pl.get("trade_signal") or {}
        misses += contract(r, "signal",
                           sig.get("signal") in
                           ("EXPANSION", "NEUTRAL", "CONTRACTION")
                           and sig.get("confidence") in
                           ("MEDIUM", "HIGH"),
                           "signal %s at %s confidence · composite %s"
                           % (sig.get("signal"), sig.get("confidence"),
                              pl.get("composite_score")))
        time.sleep(3)
        inv = lam.invoke(FunctionName=MFN,
                         InvocationType="RequestResponse")
        mm = json.loads(s3.get_object(
            Bucket=B, Key="data/market-machine.json")["Body"].read())
        p1 = ((mm.get("pillars") or {}).get("profits") or {})
        misses += contract(r, "machine-p1",
                           (p1.get("n_contributors") or 0) >= 4,
                           "machine profits pillar n=%s score=%s"
                           % (p1.get("n_contributors"),
                              p1.get("score")))
        r.kv(components=json.dumps(
                 [{"n": x["name"][:34], "v": x["expansion_0_100"]}
                  for x in comps])[:500],
             machine_composite=mm.get("composite_score"))

        r.section("edge")
        fresh = False
        for att in range(8):
            try:
                jd = json.loads(http_get(
                    "https://justhodl.ai/data/physical-economy.json"
                    "?cb=%d" % time.time()))
                if (jd.get("n_components") or 0) >= 4:
                    fresh = True
                    break
            except Exception as e:
                r.log("edge %d: %s" % (att + 1, str(e)[:70]))
            time.sleep(20)
        misses += contract(r, "edge", fresh,
                           "edge payload shows >=4 components")

        r.section("verdict")
        if misses:
            r.fail("regate: %d red" % misses)
            sys.exit(1)
        r.ok("PHYSICAL SIGNAL COMPLETE — %s (%s), %d legs live, both "
             "PJM legs joined, machine consuming"
             % (sig.get("signal"), sig.get("confidence"), len(comps)))


if __name__ == "__main__":
    main()
