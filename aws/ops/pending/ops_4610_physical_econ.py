"""ops 4610 — PHYSICAL ECONOMY trade signal: the ops-4559 wiring job.

New engine justhodl-physical-econ joins pjm-grid (load momentum + LMP
shock), port-cargo, grid-queue, and freight-pulse into one 0-100
physical-expansion composite with a cyclical trade signal
(EXPANSION / NEUTRAL / CONTRACTION + confidence). market-machine
v1.2.0 consumes it as a profits-pillar contributor (real activity
leads earnings).

This op: shape-dump the three legacy artifacts (evidence for any
tightening), settle BOTH lambdas, schedule physical-econ hourly,
invoke chain physical -> machine, contracts (>=3 of 5 components
found incl. both PJM legs; machine P1 gains the physical contributor,
n>=4), purge, edge asserts on both payloads.
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
ROLE = "arn:aws:iam::857687956942:role/justhodl-scheduler-role"
PARN = "arn:aws:lambda:us-east-1:857687956942:function:" + PFN
lam = boto3.client("lambda", region_name="us-east-1",
                   config=Config(read_timeout=300,
                                 retries={"max_attempts": 1}))
s3 = boto3.client("s3", region_name="us-east-1")
sch = boto3.client("scheduler", region_name="us-east-1")


def contract(r, name, cond, why):
    if cond:
        r.ok("  [%s] %s" % (name, why))
        return 0
    r.fail("  [%s] CONTRACT MISS — %s" % (name, why))
    return 1


def http_get(url, timeout=45):
    req = urllib.request.Request(url, headers={"User-Agent": "ops-4610"})
    with urllib.request.urlopen(req, timeout=timeout) as h:
        return h.read()


def cf(path, method="GET", data=None):
    tok = os.environ.get("CLOUDFLARE_API_TOKEN", "")
    if not tok:
        return None, "no token"
    req = urllib.request.Request(
        "https://api.cloudflare.com/client/v4" + path,
        data=json.dumps(data).encode() if data else None, method=method,
        headers={"Authorization": "Bearer " + tok,
                 "Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=30) as h:
            return json.loads(h.read()), None
    except Exception as e:
        return None, str(e)[:100]


def shape(obj, depth=0):
    if depth > 1:
        return type(obj).__name__
    if isinstance(obj, dict):
        return {k: shape(v, depth + 1)
                for k, v in list(obj.items())[:14]}
    if isinstance(obj, list):
        return ["list[%d]" % len(obj),
                shape(obj[0], depth + 1) if obj else "empty"]
    return (repr(obj)[:26] if isinstance(obj, str)
            else type(obj).__name__)


def settle(r, fn, marker):
    for att in range(16):
        try:
            gf = lam.get_function(FunctionName=fn)
            zb = http_get(gf["Code"]["Location"], 60)
            src = zipfile.ZipFile(io.BytesIO(zb)).read(
                "lambda_function.py").decode("utf-8", "replace")
            if marker in src:
                r.log("%s carries %s (attempt %d)"
                      % (fn, marker, att + 1))
                return True
        except lam.exceptions.ResourceNotFoundException:
            r.log("%s attempt %d: not created yet" % (fn, att + 1))
        except Exception as e:
            r.log("%s attempt %d: %s" % (fn, att + 1, str(e)[:80]))
        time.sleep(30)
    return False


def main():
    misses = 0
    with report("4610_physical_econ") as r:
        r.heading("ops 4610 — Physical Economy trade signal")

        r.section("shape-dump: the three legacy physical artifacts")
        for k in ("data/port-cargo.json", "data/grid-queue.json",
                  "data/freight-pulse.json"):
            try:
                d = json.loads(s3.get_object(
                    Bucket=B, Key=k)["Body"].read())
                r.log("%s → %s" % (k, json.dumps(shape(d))[:420]))
            except Exception as e:
                r.log("%s → ABSENT (%s)" % (k, str(e)[:70]))

        r.section("deploy-settle (both functions)")
        ok_p = settle(r, PFN, "justhodl-physical-econ v1.0.0")
        ok_m = settle(r, MFN, "v1.2.0")
        misses += contract(r, "deploy-physical", ok_p,
                           "physical-econ live v1.0.0")
        misses += contract(r, "deploy-machine", ok_m,
                           "market-machine live v1.2.0")
        if not (ok_p and ok_m):
            sys.exit(1)

        r.section("physical-econ config + schedule")
        cfg = lam.get_function_configuration(FunctionName=PFN)
        if cfg["Timeout"] < 90 or cfg["MemorySize"] < 512:
            lam.update_function_configuration(
                FunctionName=PFN, Timeout=max(cfg["Timeout"], 90),
                MemorySize=max(cfg["MemorySize"], 512))
            for _ in range(20):
                stt = lam.get_function_configuration(FunctionName=PFN)
                if stt.get("LastUpdateStatus") == "Successful":
                    break
                time.sleep(5)
        sched_ok = False
        try:
            sch.get_schedule(Name=PFN)
            sched_ok = True
        except Exception:
            try:
                sch.create_schedule(
                    Name=PFN, ScheduleExpression="rate(1 hour)",
                    FlexibleTimeWindow={"Mode": "OFF"},
                    Target={"Arn": PARN, "RoleArn": ROLE})
                sched_ok = True
            except Exception as e:
                r.warn("schedule: %s" % str(e)[:110])
        misses += contract(r, "schedule", sched_ok,
                           "hourly schedule set for physical-econ")

        r.section("invoke chain: physical -> machine")
        inv = lam.invoke(FunctionName=PFN,
                         InvocationType="RequestResponse")
        body = {}
        try:
            body = json.loads(json.loads(
                inv["Payload"].read().decode()).get("body") or "{}")
        except Exception:
            pass
        r.kv(physical_invoke=json.dumps(body)[:240])
        misses += contract(r, "invoke-physical",
                           inv.get("StatusCode") == 200
                           and bool(body.get("ok")),
                           "physical-econ ok:true")

        pl = json.loads(s3.get_object(
            Bucket=B, Key="data/physical-economy.json")["Body"].read())
        comps = pl.get("components") or []
        names = [x.get("name", "") for x in comps]
        pjm_legs = sum(1 for n in names if "PJM" in n)
        misses += contract(r, "components", len(comps) >= 3,
                           "%d of 5 components found: %s"
                           % (len(comps), json.dumps(names)[:220]))
        misses += contract(r, "pjm-legs", pjm_legs >= 2,
                           "both PJM legs joined (momentum + LMP "
                           "shock canary), found %d" % pjm_legs)
        misses += contract(r, "signal",
                           (pl.get("trade_signal") or {}).get("signal")
                           in ("EXPANSION", "NEUTRAL", "CONTRACTION"),
                           "trade signal %s (%s confidence) · "
                           "composite %s"
                           % ((pl.get("trade_signal") or {})
                              .get("signal"),
                              (pl.get("trade_signal") or {})
                              .get("confidence"),
                              pl.get("composite_score")))

        inv2 = lam.invoke(FunctionName=MFN,
                          InvocationType="RequestResponse")
        mb = {}
        try:
            mb = json.loads(json.loads(
                inv2["Payload"].read().decode()).get("body") or "{}")
        except Exception:
            pass
        misses += contract(r, "invoke-machine",
                           inv2.get("StatusCode") == 200
                           and bool(mb.get("ok")),
                           "market-machine ok:true")
        mm = json.loads(s3.get_object(
            Bucket=B, Key="data/market-machine.json")["Body"].read())
        p1 = ((mm.get("pillars") or {}).get("profits") or {})
        p1n = [x.get("name", "") for x in p1.get("contributors") or []]
        has_phys = any("Physical economy" in n for n in p1n)
        misses += contract(r, "machine-p1", has_phys
                           and (p1.get("n_contributors") or 0) >= 4,
                           "profits pillar carries the physical pulse "
                           "(n=%s, score=%s)"
                           % (p1.get("n_contributors"),
                              p1.get("score")))
        r.kv(machine_composite=mm.get("composite_score"),
             machine_verdict=str(mm.get("machine_verdict"))[:150])

        r.section("purge + edge (both payloads)")
        zj, _ = cf("/zones?name=justhodl.ai")
        zid = (((zj or {}).get("result") or [{}])[0] or {}).get("id")
        if zid:
            cf("/zones/%s/purge_cache" % zid, "POST",
               {"files": [
                   "https://justhodl.ai/physical-economy.html",
                   "https://justhodl.ai/data/physical-economy.json",
                   "https://justhodl.ai/data/market-machine.json",
                   "https://justhodl.ai/market-machine.html"]})
        page_ok = pay_ok = False
        for att in range(10):
            try:
                pg = http_get("https://justhodl.ai/"
                              "physical-economy.html?cb=%d"
                              % time.time()).decode("utf-8", "replace")
                page_ok = "PHYSICAL ECONOMY" in pg
                jd = json.loads(http_get(
                    "https://justhodl.ai/data/physical-economy.json"
                    "?cb=%d" % time.time()))
                pay_ok = jd.get("schema_version") == "1.0"
                if page_ok and pay_ok:
                    break
            except Exception as e:
                r.log("edge %d: %s" % (att + 1, str(e)[:70]))
            time.sleep(25)
        misses += contract(r, "edge-page", page_ok,
                           "physical-economy.html live")
        misses += contract(r, "edge-payload", pay_ok,
                           "physical-economy.json serving 1.0")

        r.section("verdict")
        if misses:
            r.fail("physical wiring: %d red" % misses)
            sys.exit(1)
        sig = pl.get("trade_signal") or {}
        r.ok("PHYSICAL ECONOMY SIGNAL LIVE — %s (%s conf, composite "
             "%s, %d legs) wired into the Market Machine profits "
             "pillar · https://justhodl.ai/physical-economy.html"
             % (sig.get("signal"), sig.get("confidence"),
                pl.get("composite_score"), len(comps)))


if __name__ == "__main__":
    main()
