"""ops 4608 — forced pillar hardened; supersedes 4607's single red.

4607: 14/15 green, machine live at 74.4 STRONG TAILWIND, but pillar 4
(what traders are FORCED to do) found only VIX — all four fleet joins
(vol-target-unwind, capital-flow-radar, spx-ma, risk-gate) missed on
key or shape. v1.1.0 widens to multi-key discovery + broader field
candidates and adds two direct FRED forced reads (VIX 1m/3m term
structure, SPX vs 200dma CTA trigger) so the pillar always has >=3
real legs. This op also SHAPE-DUMPS the six missed artifacts into the
report so any next tightening is evidence-based, not guessed.
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

FN = "justhodl-market-machine"
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
    req = urllib.request.Request(url, headers={"User-Agent": "ops-4608"})
    with urllib.request.urlopen(req, timeout=timeout) as h:
        return h.read()


def shape(obj, depth=0):
    if depth > 1:
        return type(obj).__name__
    if isinstance(obj, dict):
        return {k: shape(v, depth + 1)
                for k, v in list(obj.items())[:14]}
    if isinstance(obj, list):
        return ["list[%d]" % len(obj),
                shape(obj[0], depth + 1) if obj else "empty"]
    return (repr(obj)[:28] if isinstance(obj, str)
            else type(obj).__name__)


def main():
    misses = 0
    with report("4608_machine_forced") as r:
        r.heading("ops 4608 — forced pillar hardened (supersedes 4607)")

        r.section("shape-dump of the six missed artifacts")
        for k in ("data/vol-target-unwind.json",
                  "data/capital-flow-radar.json", "data/spx-ma.json",
                  "data/risk-gate.json", "data/etf-true-flows.json",
                  "data/rotation-dashboard.json"):
            try:
                d = json.loads(s3.get_object(
                    Bucket=B, Key=k)["Body"].read())
                r.log("%s → %s" % (k, json.dumps(shape(d))[:420]))
            except Exception as e:
                r.log("%s → ABSENT (%s)" % (k, str(e)[:70]))

        r.section("deploy-settle on v1.1.0")
        settled = False
        for att in range(16):
            try:
                gf = lam.get_function(FunctionName=FN)
                zb = http_get(gf["Code"]["Location"], 60)
                src = zipfile.ZipFile(io.BytesIO(zb)).read(
                    "lambda_function.py").decode("utf-8", "replace")
                if "v1.1.0" in src and "s3_json_multi" in src:
                    settled = True
                    r.log("v1.1.0 live (attempt %d)" % (att + 1))
                    break
            except Exception as e:
                r.log("attempt %d: %s" % (att + 1, str(e)[:90]))
            time.sleep(30)
        misses += contract(r, "deploy", settled, "zip carries v1.1.0")
        if not settled:
            sys.exit(1)

        r.section("invoke + contracts (forced >=3)")
        inv = lam.invoke(FunctionName=FN,
                         InvocationType="RequestResponse")
        raw = inv["Payload"].read().decode("utf-8", "replace")
        ok = False
        try:
            ok = bool(json.loads(json.loads(raw).get("body")
                                 or "{}").get("ok"))
        except Exception:
            pass
        misses += contract(r, "invoke",
                           inv.get("StatusCode") == 200 and ok,
                           "engine ok:true")
        pl = json.loads(s3.get_object(
            Bucket=B, Key="data/market-machine.json")["Body"].read())
        detail = {}
        for pid, floor in (("profits", 2), ("rates", 2), ("flow", 2),
                           ("forced", 3)):
            p = (pl.get("pillars") or {}).get(pid) or {}
            n = p.get("n_contributors") or 0
            detail[pid] = {"score": p.get("score"), "n": n,
                           "found": [x["name"][:38] for x in
                                     p.get("contributors") or []]}
            misses += contract(r, "pillar-" + pid,
                               p.get("score") is not None
                               and n >= floor,
                               "%s >=%d live (score=%s n=%d)"
                               % (pid, floor, p.get("score"), n))
        comp = pl.get("composite_score")
        misses += contract(r, "composite",
                           comp is not None and 0 <= comp <= 100,
                           "composite %s (%s)"
                           % (comp, pl.get("composite_label")))
        r.kv(machine_verdict=str(pl.get("machine_verdict"))[:180],
             pillar_detail=json.dumps(detail)[:900])

        r.section("edge")
        fresh = False
        for att in range(8):
            try:
                jd = json.loads(http_get(
                    "https://justhodl.ai/data/market-machine.json"
                    "?cb=%d" % time.time()))
                f = ((jd.get("pillars") or {}).get("forced")
                     or {}).get("n_contributors") or 0
                if f >= 3:
                    fresh = True
                    break
            except Exception as e:
                r.log("edge %d: %s" % (att + 1, str(e)[:70]))
            time.sleep(20)
        misses += contract(r, "edge", fresh,
                           "edge payload shows forced pillar >=3")

        r.section("verdict")
        if misses:
            r.fail("forced pillar: %d red" % misses)
            sys.exit(1)
        r.ok("FOUR PILLARS FULLY LIVE — composite=%s (%s) · %s"
             % (comp, pl.get("composite_label"),
                str(pl.get("machine_verdict"))[:150]))


if __name__ == "__main__":
    main()
