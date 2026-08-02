"""
ops_4284 -- dormant switch-on wave 2: the remaining true-dormants.

Wave 1 went 10/10, proving most dormancy is never-invoked, not broken.
Wave 2: head-check every remaining MAIN_PATH target (schedules may
have lit some since), fire the next batch of UNFIRED writers (cap 16;
client read-timeouts count as fired -- 4283 proved server-side
completion), then head everything again. Outcomes are classified
honestly: LIT; CONDITIONAL_PATH (writer already ran in wave 1 yet this
key stayed absent -- a conditional branch, wave-3 code-read item); or
DEFECT with the FunctionError/log evidence attached. Atlas updated
with per-target results.
"""
import json
import sys
import time
from datetime import datetime, timezone

import boto3
from botocore.config import Config
from ops_report import report

REGION, BUCKET = "us-east-1", "justhodl-dashboard-live"
s3 = boto3.client("s3", region_name=REGION)
lam = boto3.client("lambda", region_name=REGION,
                   config=Config(read_timeout=55, retries={"max_attempts": 1}))
logs = boto3.client("logs", region_name=REGION)

WAVE1_WRITERS = {
    "justhodl-convergence-radar", "justhodl-alert-sentinel",
    "justhodl-prepump-alerts-router", "justhodl-trade-ticket-monitor",
    "justhodl-altseason", "justhodl-backtest-harness",
    "justhodl-theme-classifier", "justhodl-crisis-canaries",
    "justhodl-accumulation-radar", "justhodl-signal-harvester",
}

def exists(key):
    try:
        s3.head_object(Bucket=BUCKET, Key=key)
        return True
    except Exception:
        return False

fails = []
with report("4284_wave2") as r:
    r.heading("ops 4284 -- switch-on wave 2")
    atlas = json.loads(s3.get_object(
        Bucket=BUCKET, Key="data/alpha-atlas.json")["Body"].read())
    triage = atlas.get("dormant_triage") or []
    remaining = [t for t in triage
                 if t.get("class") == "MAIN_PATH"
                 and t.get("result") != "MATERIALIZED"
                 and not exists(t["target"])]
    already = [t for t in triage
               if t.get("class") == "MAIN_PATH"
               and t.get("result") != "MATERIALIZED"
               and exists(t["target"])]
    for t in already:
        t["result"] = "MATERIALIZED_BY_SCHEDULE"
    r.ok("remaining true-dormant after re-head: %d (%d lit by "
         "schedules since wave 1)" % (len(remaining), len(already)))

    to_fire, seen = [], set()
    for t in remaining:
        w = t.get("writer")
        if not w or w in WAVE1_WRITERS or w in seen:
            continue
        seen.add(w)
        to_fire.append(w)
        if len(to_fire) >= 16:
            break
    r.log("firing %d unfired writers: %s"
          % (len(to_fire), [w.replace("justhodl-", "")
                            for w in to_fire]))
    fired_err = {}
    for w in to_fire:
        try:
            p = lam.invoke(FunctionName=w,
                           InvocationType="RequestResponse",
                           Payload=b"{}")
            if p.get("FunctionError"):
                fired_err[w] = (p["Payload"].read() or b"")[:160].decode(
                    "utf-8", "ignore")
        except Exception as e:
            msg = str(e)
            if "Read timeout" in msg:
                r.log("%s: client timeout (server-side likely "
                      "completing)" % w)
            else:
                fired_err[w] = msg[:140]
    time.sleep(10)

    lit, cond, defects = [], [], []
    for t in remaining:
        if exists(t["target"]):
            t["result"] = "MATERIALIZED_WAVE2"
            lit.append(t)
        elif t.get("writer") in WAVE1_WRITERS:
            t["result"] = "CONDITIONAL_PATH"
            cond.append(t)
        elif t.get("writer") in seen:
            ev = fired_err.get(t["writer"], "")
            if not ev:
                try:
                    lg = logs.filter_log_events(
                        logGroupName="/aws/lambda/%s" % t["writer"],
                        startTime=int((time.time() - 420) * 1000))
                    lines = [x["message"].strip()[:120]
                             for x in lg.get("events", [])
                             if "Error" in x["message"]
                             or "Traceback" in x["message"]][-2:]
                    ev = " | ".join(lines)
                except Exception:
                    pass
            t["result"] = "DEFECT"
            t["evidence"] = ev[:200] or "ran clean, key absent -- " \
                "conditional or different-name write"
            defects.append(t)
        else:
            t["result"] = "QUEUED_WAVE3"

    for t in lit[:20]:
        r.ok("LIT: %s (by %s)" % (t["target"],
                                  (t.get("writer") or "?")
                                  .replace("justhodl-", "")))
    for t in cond[:12]:
        r.log("CONDITIONAL: %s (writer %s ran wave 1, key needs a "
              "branch)" % (t["target"],
                           (t.get("writer") or "?")
                           .replace("justhodl-", "")))
    for t in defects[:12]:
        r.warn("DEFECT: %s (%s) -- %s"
               % (t["target"],
                  (t.get("writer") or "?").replace("justhodl-", ""),
                  t.get("evidence", "")[:120]))
    queued = [t for t in remaining if t.get("result") == "QUEUED_WAVE3"]
    r.ok("wave 2: %d lit, %d conditional, %d defects with evidence, "
         "%d queued for wave 3"
         % (len(lit), len(cond), len(defects), len(queued)))

    tot = atlas.get("totals") or {}
    tot.update(wave2_lit=len(lit) + len(already),
               dormant_conditional=len(cond),
               dormant_defects=len(defects),
               dormant_true_remaining=len(cond) + len(defects)
               + len(queued))
    atlas.update(totals=tot, dormant_triage=triage,
                 generated_at=datetime.now(
                     timezone.utc).isoformat(timespec="seconds"))
    s3.put_object(Bucket=BUCKET, Key="data/alpha-atlas.json",
                  Body=json.dumps(atlas, separators=(",", ":"),
                                  default=str).encode(),
                  ContentType="application/json",
                  CacheControl="public, max-age=1800")
    r.ok("atlas updated with wave-2 results")
    if not lit and not already:
        fails.append("wave 2 lit nothing")
    r.section("RESULT")
    if fails:
        for f in fails:
            r.fail("  %s" % f)
    else:
        r.ok("OPS 4284 PASS")
if fails:
    sys.exit(1)
