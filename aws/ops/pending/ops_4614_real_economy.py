"""ops 4614 — REAL ECONOMY BUILD: collector + physical-econ v2.0.0.

Khalid: GO — ship every metric, institutional style, and check for
bugs. Architecture: justhodl-real-economy-collector (20 isolated leg
fetchers, tiered evidence, uniform envelopes to
data/warm/real-economy/) feeding physical-econ v2.0.0 (five weighted
sub-pillars, staleness gates, tier discipline: tier1 load-bearing,
tier2 half-weight, tier3 observed-only, declared not-public list).

This op: settle both, inject keys, schedule collector, invoke chain,
per-leg status table, hard contracts on tier-1 coverage, doctrine
canaries (oil backwardation / chokepoint shock / claims spike), a
BUG-CHECK section (weights, NaN leaks, staleness-gate enforcement,
schema), machine regress, purge, edge.
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

CFN = "justhodl-real-economy-collector"
PFN = "justhodl-physical-econ"
MFN = "justhodl-market-machine"
B = "justhodl-dashboard-live"
ROLE = "arn:aws:iam::857687956942:role/justhodl-scheduler-role"
CARN = "arn:aws:lambda:us-east-1:857687956942:function:" + CFN
lam = boto3.client("lambda", region_name="us-east-1",
                   config=Config(read_timeout=600,
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
    req = urllib.request.Request(url, headers={"User-Agent": "ops-4614"})
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
        except lam.exceptions.ResourceNotFoundException:
            r.log("%s attempt %d: not created yet" % (fn, att + 1))
        except Exception as e:
            r.log("%s attempt %d: %s" % (fn, att + 1, str(e)[:80]))
        time.sleep(30)
    return False


def main():
    misses = 0
    with report("4614_real_economy") as r:
        r.heading("ops 4614 — Real Economy institutional build")

        r.section("deploy-settle (both)")
        ok_c = settle(r, CFN, "justhodl-real-economy-collector v1.0.0")
        ok_p = settle(r, PFN, "justhodl-physical-econ v2.0.0")
        misses += contract(r, "deploy-collector", ok_c, "collector "
                           "v1.0.0")
        misses += contract(r, "deploy-signal", ok_p, "signal v2.0.0")
        if not (ok_c and ok_p):
            sys.exit(1)

        r.section("keys + config + schedule")
        eia = os.environ.get("EIA_API_KEY", "")
        fred = os.environ.get("FRED_API_KEY", "")
        misses += contract(r, "eia-secret", bool(eia),
                           "EIA_API_KEY in runner env (len=%d)"
                           % len(eia))
        cfg = lam.get_function_configuration(FunctionName=CFN)
        envv = (cfg.get("Environment") or {}).get("Variables") or {}
        envv["EIA_API_KEY"] = eia
        if fred:
            envv["FRED_API_KEY"] = fred
        envv.setdefault("S3_BUCKET", B)
        lam.update_function_configuration(
            FunctionName=CFN, Timeout=max(cfg["Timeout"], 240),
            MemorySize=max(cfg["MemorySize"], 1024),
            Environment={"Variables": envv})
        for _ in range(20):
            stc = lam.get_function_configuration(FunctionName=CFN)
            if stc.get("LastUpdateStatus") == "Successful":
                break
            time.sleep(5)
        pc = lam.get_function_configuration(FunctionName=PFN)
        if pc["Timeout"] < 120 or pc["MemorySize"] < 512:
            lam.update_function_configuration(
                FunctionName=PFN, Timeout=max(pc["Timeout"], 120),
                MemorySize=max(pc["MemorySize"], 512))
            for _ in range(20):
                stp = lam.get_function_configuration(FunctionName=PFN)
                if stp.get("LastUpdateStatus") == "Successful":
                    break
                time.sleep(5)
        sched_ok = False
        try:
            sch.get_schedule(Name=CFN)
            sched_ok = True
        except Exception:
            try:
                sch.create_schedule(
                    Name=CFN, ScheduleExpression="rate(1 hour)",
                    FlexibleTimeWindow={"Mode": "OFF"},
                    Target={"Arn": CARN, "RoleArn": ROLE})
                sched_ok = True
            except Exception as e:
                r.warn("schedule: %s" % str(e)[:110])
        misses += contract(r, "schedule", sched_ok,
                           "collector hourly schedule set")

        r.section("collector run + per-leg truth table")
        inv = lam.invoke(FunctionName=CFN,
                         InvocationType="RequestResponse")
        cb = {}
        try:
            cb = json.loads(json.loads(
                inv["Payload"].read().decode()).get("body") or "{}")
        except Exception:
            pass
        r.kv(collector=json.dumps(cb)[:200])
        summ = json.loads(s3.get_object(
            Bucket=B,
            Key="data/warm/real-economy/_summary.json")["Body"].read())
        legs = summ.get("legs") or {}
        for lid in sorted(legs):
            env = json.loads(s3.get_object(
                Bucket=B, Key="data/warm/real-economy/%s.json"
                % lid)["Body"].read())
            r.log("%-18s %-6s %-9s %s" % (
                lid, env.get("tier"), env.get("status"),
                str(env.get("detail"))[:90]))
        t1 = [lid for lid in legs
              if lid.startswith(("eia", "wti", "fred", "chokepoints",
                                 "indeed"))]
        t1_ok = sum(1 for lid in t1 if legs[lid] == "OK")
        misses += contract(r, "tier1", t1_ok >= 10,
                           "tier-1 legs OK: %d/%d (hard SLO >=10)"
                           % (t1_ok, len(t1)))
        t2_ok = sum(1 for lid, v in legs.items()
                    if lid in ("tsa", "aisi_steel", "copper",
                               "noaa_degree_days") and v == "OK")
        if t2_ok == 0:
            r.warn("all tier-2 scrapes degraded this run (soft SLO)")
        r.kv(counts=json.dumps(summ.get("counts")))

        r.section("signal v2 + contracts")
        inv = lam.invoke(FunctionName=PFN,
                         InvocationType="RequestResponse")
        pb = {}
        try:
            pb = json.loads(json.loads(
                inv["Payload"].read().decode()).get("body") or "{}")
        except Exception:
            pass
        misses += contract(r, "signal-ok", bool(pb.get("ok")),
                           "signal ok:true (%s)" % json.dumps(pb)[:140])
        pl = json.loads(s3.get_object(
            Bucket=B,
            Key="data/physical-economy.json")["Body"].read())
        misses += contract(r, "schema",
                           pl.get("schema_version") == "2.0",
                           "schema 2.0")
        subs = pl.get("sub_pillars") or {}
        for sp in ("energy", "trade_transport", "materials", "labor",
                   "construction"):
            d = subs.get(sp) or {}
            misses += contract(r, "sub-" + sp,
                               d.get("score") is not None
                               and (d.get("n_live") or 0) >= 1,
                               "%s scoring (%s, %s/%s live)"
                               % (sp, d.get("score"), d.get("n_live"),
                                  d.get("n_total")))
        misses += contract(r, "coverage",
                           (pl.get("n_live_legs") or 0) >= 12,
                           "%s live legs fleet-wide"
                           % pl.get("n_live_legs"))
        cans = pl.get("canaries") or {}
        for cn in ("oil_backwardation", "chokepoint_shock",
                   "claims_spike"):
            misses += contract(r, "canary-" + cn, cn in cans,
                               "%s: %s" % (cn,
                                           json.dumps(cans.get(cn)
                                                      or {})[:120]))

        r.section("bug-check (institutional self-test)")
        wsum = round(sum((pl.get("sub_pillar_weights")
                          or {}).values()), 4)
        misses += contract(r, "bug-weights", wsum == 1.0,
                           "sub-pillar weights sum to %s" % wsum)
        bad = [x["leg_id"] for x in pl.get("legs") or []
               if x.get("expansion_0_100") is not None
               and not (0 <= x["expansion_0_100"] <= 100)]
        misses += contract(r, "bug-range", not bad,
                           "all scored legs in [0,100] (violations: "
                           "%s)" % (bad or "none"))
        leak = [x["leg_id"] for x in pl.get("legs") or []
                if x.get("status") in ("STALE", "FAILED", "ABSENT",
                                       "DEGRADED")
                and x.get("expansion_0_100") is not None]
        misses += contract(r, "bug-gate", not leak,
                           "no non-OK leg leaked into scoring "
                           "(violations: %s)" % (leak or "none"))
        comp = pl.get("composite_score")
        misses += contract(r, "bug-composite",
                           comp is not None and 0 <= comp <= 100,
                           "composite %s (%s) confidence %s"
                           % (comp, pl.get("composite_label"),
                              (pl.get("trade_signal") or {})
                              .get("confidence")))

        r.section("machine regress + purge + edge")
        time.sleep(3)
        lam.invoke(FunctionName=MFN, InvocationType="RequestResponse")
        mm = json.loads(s3.get_object(
            Bucket=B, Key="data/market-machine.json")["Body"].read())
        p1 = ((mm.get("pillars") or {}).get("profits") or {})
        misses += contract(r, "machine",
                           (p1.get("n_contributors") or 0) >= 4,
                           "machine P1 n=%s · composite %s"
                           % (p1.get("n_contributors"),
                              mm.get("composite_score")))
        zj, _ = cf("/zones?name=justhodl.ai")
        zid = (((zj or {}).get("result") or [{}])[0] or {}).get("id")
        if zid:
            cf("/zones/%s/purge_cache" % zid, "POST",
               {"files": [
                   "https://justhodl.ai/physical-economy.html",
                   "https://justhodl.ai/data/physical-economy.json"]})
        page_ok = pay_ok = False
        for att in range(10):
            try:
                pg = http_get("https://justhodl.ai/"
                              "physical-economy.html?cb=%d"
                              % time.time()).decode("utf-8", "replace")
                page_ok = "INSTITUTIONAL SIGNAL" in pg
                jd = json.loads(http_get(
                    "https://justhodl.ai/data/physical-economy.json"
                    "?cb=%d" % time.time()))
                pay_ok = jd.get("schema_version") == "2.0"
                if page_ok and pay_ok:
                    break
            except Exception as e:
                r.log("edge %d: %s" % (att + 1, str(e)[:70]))
            time.sleep(25)
        misses += contract(r, "edge-page", page_ok, "v2 page live")
        misses += contract(r, "edge-payload", pay_ok,
                           "v2 payload at the edge")

        r.section("verdict")
        if misses:
            r.fail("real economy build: %d red (per-leg table above "
                   "is the ground truth)" % misses)
            sys.exit(1)
        r.ok("REAL ECONOMY INSTITUTIONAL SIGNAL LIVE — composite %s "
             "(%s), %s live legs across 5 sub-pillars, doctrine "
             "canaries armed, bug-check clean · "
             "https://justhodl.ai/physical-economy.html"
             % (comp, pl.get("composite_label"),
                pl.get("n_live_legs")))


if __name__ == "__main__":
    main()
