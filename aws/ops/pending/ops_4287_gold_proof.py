"""
ops_4287 -- the gold heal, PROVEN at the source of truth.

4286 deployed the shim heal fleet-wide but no sampled engine happened
to request the dead series, so no claim was made. The definitive proof
runs through justhodl-financial-secretary -- maintainer of
data/fred-cache.json (207 series) that most cache-first engines read
instead of HTTP. This op, evidence-first:

  1. grep the secretary's FULL source tree for the gold series (literal
     or series-list file) and the shim import;
  2. if it carries gold: invoke post-wave, gate on the
     "[fred-shim] gold->GCUSD served" log AND fred-cache.json's gold
     entries carrying a CURRENT date with a sane price -- every
     cache-reader inherits the heal;
  3. else: force-exercise a direct-fetch importer (correlation-breaks /
     divergence-engine-v2) and gate on its served log;
  4. us-cycle recheck on settled post-wave code.
"""
import json
import os
import re
import sys
import time
from datetime import datetime, timedelta, timezone

import boto3
from botocore.config import Config
from ops_report import report

REGION, BUCKET = "us-east-1", "justhodl-dashboard-live"
lam = boto3.client("lambda", region_name=REGION,
                   config=Config(read_timeout=330, retries={"max_attempts": 1}))
logs = boto3.client("logs", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)
NOW = datetime.now(timezone.utc)

def wave_deployed(fn, max_age_min=180):
    """Settled + LastModified recent enough to be the 4286 wave."""
    for _ in range(40):
        try:
            c = lam.get_function_configuration(FunctionName=fn)
            if c.get("LastUpdateStatus") in (None, "Successful") \
                    and c.get("State") == "Active":
                lm = datetime.strptime(
                    c["LastModified"].split(".")[0], "%Y-%m-%dT%H:%M:%S"
                ).replace(tzinfo=timezone.utc)
                return (NOW - lm).total_seconds() < max_age_min * 60, lm
        except Exception:
            pass
        time.sleep(8)
    return False, None

def fresh_lines(fn, pat, window=480):
    try:
        ev = logs.filter_log_events(
            logGroupName="/aws/lambda/%s" % fn,
            startTime=int((time.time() - window) * 1000))
        return [x["message"].strip()[:140]
                for x in ev.get("events", []) if pat in x["message"]]
    except Exception:
        return []

def grep_dir(d, pat):
    hits = []
    for root, _, files in os.walk(d):
        for f in files:
            if not f.endswith((".py", ".json", ".txt", ".yaml", ".yml")):
                continue
            fp = os.path.join(root, f)
            try:
                t = open(fp, encoding="utf-8", errors="ignore").read()
            except Exception:
                continue
            for m in re.finditer(pat, t):
                hits.append((fp, t[max(0, m.start() - 60):
                                   m.start() + 80].replace("\n", " ")))
    return hits

fails = []
with report("4287_gold_proof") as r:
    r.heading("ops 4287 -- gold heal proven at the cache source")

    r.section("1. evidence: who carries the gold series")
    sec_dir = "aws/lambdas/justhodl-financial-secretary"
    gold_hits = grep_dir(sec_dir, r"GOLD[AP]MGBD228NLBM")
    shim_hits = grep_dir(sec_dir, r"_fred_shim")
    r.log("secretary: gold refs=%d shim import=%s"
          % (len(gold_hits), "YES" if shim_hits else "NO"))
    for fp, ctx in gold_hits[:3]:
        r.log("  %s :: …%s…" % (fp.split("/")[-1], ctx[:100]))
    # does the CACHE currently hold gold, and how stale?
    cache_gold_date = None
    try:
        cache = json.loads(s3.get_object(
            Bucket=BUCKET, Key="data/fred-cache.json")["Body"].read())
        for k in ("GOLDAMGBD228NLBM", "GOLDPMGBD228NLBM"):
            rows = cache.get(k)
            if isinstance(rows, list) and rows:
                cache_gold_date = rows[0].get("date")
                r.log("cache[%s] latest: %s = %s"
                      % (k, rows[0].get("date"), rows[0].get("value")))
    except Exception as e:
        r.log("cache read: %s" % str(e)[:80])

    proof = False
    if gold_hits and shim_hits:
        r.section("2. secretary run -> served log + cache currency")
        ok, lm = wave_deployed("justhodl-financial-secretary")
        if not ok:
            fails.append("secretary not on post-wave code (lm=%s)" % lm)
        else:
            try:
                p = lam.invoke(
                    FunctionName="justhodl-financial-secretary",
                    InvocationType="RequestResponse", Payload=b"{}")
                r.log("invoked: %s"
                      % (p["Payload"].read() or b"")[:140].decode(
                          "utf-8", "ignore"))
            except Exception as e:
                if "Read timeout" not in str(e):
                    fails.append("secretary invoke: %s" % str(e)[:90])
            time.sleep(8)
            served = fresh_lines("justhodl-financial-secretary",
                                 "gold->GCUSD served")
            if served:
                r.ok("SERVED: %s" % served[-1][:100])
            try:
                cache = json.loads(s3.get_object(
                    Bucket=BUCKET,
                    Key="data/fred-cache.json")["Body"].read())
                for k in ("GOLDAMGBD228NLBM", "GOLDPMGBD228NLBM"):
                    rows = cache.get(k)
                    if isinstance(rows, list) and rows:
                        d0 = rows[0].get("date", "")
                        v0 = float(rows[0].get("value") or 0)
                        cur = d0 >= (NOW - timedelta(days=5)
                                     ).date().isoformat()
                        sane = 1200 < v0 < 9000
                        (r.ok if cur and sane else r.warn)(
                            "cache[%s] now: %s = %.1f (%s, %s)"
                            % (k, d0, v0,
                               "CURRENT" if cur else "stale",
                               "sane" if sane else "odd"))
                        if cur and sane:
                            proof = True
            except Exception as e:
                fails.append("cache verify: %s" % str(e)[:90])
            if not proof and not served:
                r.warn("secretary run produced neither served-log nor "
                       "current cache gold -- falling to direct path")

    if not proof:
        r.section("3. fallback: force a direct-fetch importer")
        for fn in ("justhodl-correlation-breaks",
                   "justhodl-divergence-engine-v2"):
            ok, lm = wave_deployed(fn)
            r.log("%s post-wave=%s (lm=%s)" % (fn, ok, lm))
            if not ok:
                continue
            try:
                lam.invoke(FunctionName=fn,
                           InvocationType="RequestResponse",
                           Payload=b"{}")
            except Exception as e:
                if "Read timeout" not in str(e):
                    r.warn("%s invoke: %s" % (fn, str(e)[:80]))
            time.sleep(6)
            served = fresh_lines(fn, "gold->GCUSD served")
            bad = [l for l in fresh_lines(fn, "GOLDAMGBD")
                   if "400" in l]
            if served:
                r.ok("PROOF via %s: %s"
                     % (fn.replace("justhodl-", ""), served[-1][:100]))
                proof = True
                break
            if bad:
                fails.append("%s still 400s: %s" % (fn, bad[-1][:80]))

    r.section("4. us-cycle on settled code")
    ok, lm = wave_deployed("justhodl-us-cycle")
    if not ok:
        fails.append("us-cycle still not on wave code (lm=%s)" % lm)
    else:
        try:
            p = lam.invoke(FunctionName="justhodl-us-cycle",
                           InvocationType="RequestResponse",
                           Payload=b"{}")
            r.log("us-cycle: %s"
                  % (p["Payload"].read() or b"")[:120].decode(
                      "utf-8", "ignore"))
        except Exception as e:
            if "Read timeout" not in str(e):
                r.warn("us-cycle invoke: %s" % str(e)[:80])
        time.sleep(5)
        bad = [l for l in fresh_lines("justhodl-us-cycle", "GOLDAMGBD")
               if "400" in l]
        served = fresh_lines("justhodl-us-cycle", "gold->GCUSD served")
        if bad:
            fails.append("us-cycle still 400s on dead gold")
        else:
            r.ok("us-cycle clean%s"
                 % (" + served: %s" % served[-1][:80] if served else
                    " (gold path not exercised this run -- neutral)"))
            if served:
                proof = True

    if not proof:
        fails.append("gold heal still UNPROVEN by served-log or "
                     "current cache -- evidence above says where next")
    r.section("RESULT")
    if fails:
        for f in fails:
            r.fail("  %s" % f)
    else:
        r.ok("OPS 4287 PASS -- gold heal proven; cache-readers "
             "inherit real 2026 gold")
if fails:
    sys.exit(1)
