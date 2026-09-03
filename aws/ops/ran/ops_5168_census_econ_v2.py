"""ops_5168 -- census-econ lane v2: verify the deploy, wake the stuck shard, restore cadence.

Why (from ops 5164/5167 evidence): shard 12 sat in DRAIN with 151 datasets
queued and zero progress in a 6-minute bracket while 11 COMPLETE shards each
re-downloaded the Census data.json catalog on every dispatch (~60s x 11 x
every tick). Reading the source showed three structural faults:
  1. no intra-entry cursor -- a heavy dataset-vintage that exceeded the
     780s budget restarted from geo 0 / chunk 0 on the next run, re-PUTting
     the same files and never finishing;
  2. no lease -- under the old 5-minute dispatch, overlapping runs of the
     same shard overwrote each other's state document (rows_total went
     DOWN inside the bracket);
  3. the dispatcher invoked every shard every tick, COMPLETE or not.
The Lambda source in this same commit fixes all three (econ-v2). This op:
  A. waits for the deploy (description marker), reads shard states,
  B. invokes the dispatcher synchronously and checks it skips COMPLETE
     shards (first call after deploy recatalogs once -- expected),
  C. brackets shard 11 progress over ~4 minutes with the lease visible,
  D. sets the dispatcher back to rate(15 minutes): only shards with work
     run, the lease forbids overlap, so the 151 datasets drain fast and
     the lane costs nothing once COMPLETE.
"""
import json
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
from ops_report import report  # noqa: E402

FN = "justhodl-census-us"
BUCKET = "justhodl-dashboard-live"
MARK = "econ-v2 (ops 5168)"
CFG = Config(retries={"max_attempts": 6, "mode": "adaptive"}, read_timeout=900)
lam = boto3.client("lambda", region_name="us-east-1", config=CFG)
s3 = boto3.client("s3", region_name="us-east-1", config=CFG)
logs = boto3.client("logs", region_name="us-east-1", config=CFG)
sch = boto3.client("scheduler", region_name="us-east-1", config=CFG)
NOW = datetime.now(timezone.utc)
FAILS = []


def jget(key):
    try:
        return json.loads(s3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
    except Exception:
        return None


def shard_summary(k):
    st = jget("data/_state/census-econ-s%d.json" % k) or {}
    q = st.get("queue") or []
    head = ("%s@%s" % (q[0].get("ds"), q[0].get("vintage"))) if q else "-"
    return {"phase": st.get("phase"), "queue_left": len(q), "n_done": st.get("n_done"),
            "n_total": st.get("n_total"), "rows": st.get("rows_total"), "head": head,
            "attempts_head": (st.get("attempts") or {}).get(head), "cursor": st.get("cursor"),
            "lease": st.get("lease_until"), "failures": len(st.get("failures") or {}),
            "budget_breaks": st.get("budget_breaks"), "updated": st.get("updated_at"),
            "version": st.get("version")}


with report("ops_5168_census_econ_v2") as R:
    R.heading("ops 5168 -- census-econ v2: deploy verify, wake shard 11, cadence")

    R.section("A. Deploy + shard states")
    ok = False
    for _ in range(45):
        c = lam.get_function_configuration(FunctionName=FN)
        if MARK in (c.get("Description") or "") and c.get("LastUpdateStatus") in (None, "Successful"):
            ok = True
            break
        time.sleep(20)
    if ok:
        R.ok("   deployed: %s (%s)" % (c.get("Description")[:90], c.get("LastModified")))
    else:
        R.warn("   deploy marker not observed after 15 min -- continuing with whatever is live")
    before = {}
    for k in range(12):
        before[k] = shard_summary(k)
        s_ = before[k]
        R.log("   s%02d %-9s left %4s done %4s/%4s rows %11s fails %3s cursor=%s lease=%s head=%s"
              % (k, s_["phase"], s_["queue_left"], s_["n_done"], s_["n_total"], "{:,}".format(s_["rows"] or 0),
                 s_["failures"], (s_["cursor"] or {}).get("gi") if s_["cursor"] else "-",
                 (s_["lease"] or "-")[11:19], s_["head"][:50]))
    live_shards = [k for k, v in before.items() if v["queue_left"] or v["phase"] != "COMPLETE"]
    R.log("   shards with work: %s" % live_shards)
    if live_shards:
        st11 = jget("data/_state/census-econ-s%d.json" % live_shards[0]) or {}
        fails = st11.get("failures") or {}
        R.log("   shard %d failures (%d): %s" % (live_shards[0], len(fails),
              json.dumps(dict(list(fails.items())[:6]))[:400]))
        heads = [("%s@%s" % (e.get("ds"), e.get("vintage"))) for e in (st11.get("queue") or [])[:5]]
        R.log("   shard %d queue head: %s" % (live_shards[0], heads))

    R.section("B. Dispatcher: synchronous call -- must skip COMPLETE shards")
    try:
        resp = lam.invoke(FunctionName=FN, InvocationType="RequestResponse",
                          Payload=json.dumps({"mode": "econ_dispatch", "shards": 12}).encode())
        out = json.loads(resp["Payload"].read() or b"{}")
        R.log("   dispatcher -> %s" % json.dumps(out)[:300])
        if out.get("recatalog"):
            R.log("   (first dispatch after deploy re-catalogs every shard once; daily after that)")
        elif out.get("invoked", 99) <= len(live_shards):
            R.ok("   only %d shard(s) invoked, %d COMPLETE skipped" % (out.get("invoked"), len(out.get("skipped_complete") or [])))
        else:
            R.warn("   dispatcher invoked %s shards; expected %d" % (out.get("invoked"), len(live_shards)))
    except Exception as e:
        FAILS.append("dispatch: %s" % str(e)[:140])

    R.section("C. Bracket: lease visible, cursor/progress on the working shard")
    time.sleep(240)
    after = {k: shard_summary(k) for k in range(12)}
    for k in live_shards:
        b, a = before[k], after[k]
        R.log("   s%02d before: left %s done %s rows %s | after: left %s done %s rows %s cursor=%s lease=%s breaks=%s version=%s"
              % (k, b["queue_left"], b["n_done"], b["rows"], a["queue_left"], a["n_done"], a["rows"],
                 json.dumps(a["cursor"])[:60] if a["cursor"] else None, (a["lease"] or "-")[11:19],
                 a["budget_breaks"], a["version"]))
        moved = (a["n_done"] or 0) > (b["n_done"] or 0) or (a["rows"] or 0) > (b["rows"] or 0) or bool(a["cursor"]) or a["lease"]
        (R.ok if moved else R.warn)("   shard %d %s" % (k, "is moving (lease/cursor/progress observed)" if moved else "shows no movement yet"))
    try:
        ev = logs.filter_log_events(logGroupName="/aws/lambda/" + FN,
                                    startTime=int((NOW - timedelta(minutes=2)).timestamp() * 1000),
                                    filterPattern="?Traceback ?ERROR", limit=5)
        errs = ev.get("events", [])
        if errs:
            for e_ in errs:
                R.warn("   log: %s" % e_["message"].strip()[:200])
        else:
            R.ok("   no Traceback/ERROR in the log since the dispatch")
    except Exception as e:
        R.warn("   log read: %s" % str(e)[:80])
    dsp = jget("data/_state/census-econ-dispatch.json")
    R.log("   dispatcher state: %s" % json.dumps(dsp)[:200])

    R.section("D. Cadence: dispatcher back to rate(15 minutes) -- safe now (skip-complete + lease + cursor)")
    try:
        d = sch.get_schedule(Name="justhodl-census-econ-dispatch", GroupName="default")
        if d.get("ScheduleExpression") != "rate(15 minutes)":
            sch.update_schedule(Name="justhodl-census-econ-dispatch", GroupName="default",
                                ScheduleExpression="rate(15 minutes)", ScheduleExpressionTimezone="UTC",
                                FlexibleTimeWindow=d["FlexibleTimeWindow"], Target=d["Target"], State="ENABLED",
                                Description="census econ dispatcher v2: only shards with work run; lease forbids overlap (ops 5168)")
            R.ok("   justhodl-census-econ-dispatch %s -> rate(15 minutes)" % d.get("ScheduleExpression"))
        else:
            R.ok("   already rate(15 minutes)")
    except Exception as e:
        FAILS.append("schedule: %s" % str(e)[:120])

    if FAILS:
        for f in FAILS:
            R.fail(f)
        sys.exit(1)
    R.ok("ops 5168 complete")
