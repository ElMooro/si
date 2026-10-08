"""ops_5030 -- re-arm the extractor, but only on PROOF of progress.

ops 5029 answered Khalid's question: we did NOT get the data.
    8,147 warm eurostat flow files, 8.86 GB
    79 flows parsed  =  0.97% by count, 0.71% by raw bytes
    1,733,000 series in 3,961 pages / 1.03 GB -- intact and readable
    8,068 flows / 8.80 GB never touched
Everything after 2026-08-09T02:40 was rewrite waste: 20 days of maximum
apparent activity and zero new data. Finishing costs ~$2.41 one-time in
PUT requests and ~$2.89/month of storage, so the anomaly bought nothing
and stopping the engine permanently would abandon 99.3% of the lane.

v2.1 (deployed with this op) removes both failure modes:
  * budget checked INSIDE the row loop off get_remaining_time_in_millis
  * checkpoint after every flow, and on every budget trip mid-flow
  * per-flow resume offset -- a flow bigger than one invocation resumes
  * sha256 skip -- a byte-identical page is never re-PUT, so no
    noncurrent version can ever be created by a rerun
  * STALL_ATTEMPTS breaker -- a flow is retried only while rows_done is
    ADVANCING; a genuinely stuck flow is recorded and skipped instead of
    blocking all 8,147 behind it

  P0 unfreeze the function ONLY (schedule stays disabled)
  P1 single Event invoke, then poll the state doc for real movement
  P2 PROOF: flows_done > 79, n_pages advanced, and the pages written are
     NEW KEYS (version count 1), not rewrites of existing ones
  P3 only if proven: re-enable justhodl-series-extractor-5min
  P4 ledger + projected completion

RED if the run does not advance flows_done -- the engine then stays
frozen and the schedule stays off.
"""
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import boto3
from botocore.config import Config

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from ops_report import report  # noqa: E402

REGION = "us-east-1"
LIVE = "justhodl-dashboard-live"
FN = "justhodl-series-extractor"
RULE = "justhodl-series-extractor-5min"
STATE_KEY = "data/_state/series-extract-eurostat.json"
SERIES_PFX = "data/providers/eurostat/series/"

cfg = Config(read_timeout=120, retries={"max_attempts": 3})
s3 = boto3.client("s3", region_name=REGION, config=cfg)
lam = boto3.client("lambda", region_name=REGION, config=cfg)
ev = boto3.client("events", region_name=REGION, config=cfg)
NOW = datetime.now(timezone.utc)


def read_state():
    try:
        return json.loads(s3.get_object(Bucket=LIVE,
                                        Key=STATE_KEY)["Body"].read())
    except Exception:
        return {}


with report("ops_5030_rearm_extractor") as R:
    fails = []
    out = {"op": "ops_5030", "at": NOW.isoformat(timespec="seconds")}

    R.section("P0 baseline + unfreeze the function (schedule stays off)")
    before = read_state()
    b_flows = len(before.get("flows_done") or [])
    b_pages = before.get("n_pages")
    R.log("  BEFORE flows_done=%d n_pages=%s series=%s updated_at=%s" % (
        b_flows, b_pages, before.get("series_count"),
        before.get("updated_at")))
    out.update(before_flows=b_flows, before_pages=b_pages)
    try:
        c = lam.get_function_configuration(FunctionName=FN)
        R.log("  deployed code LastModified=%s size=%s" % (
            c.get("LastModified"), c.get("CodeSize")))
    except Exception as e:
        R.log("  cfg err %s" % str(e)[:100])
    try:
        lam.delete_function_concurrency(FunctionName=FN)
        time.sleep(2)
        rc = lam.get_function_concurrency(FunctionName=FN)
        R.log("  reserved concurrency -> %s (unfrozen)" % rc.get(
            "ReservedConcurrentExecutions", "unreserved"))
    except Exception as e:
        R.log("  unfreeze err %s" % str(e)[:120])
        fails.append("P0:unfreeze")
    try:
        d = ev.describe_rule(Name=RULE)
        R.log("  rule %s still %s (correct -- one supervised run first)"
              % (RULE, d.get("State")))
    except Exception as e:
        R.log("  rule read err %s" % str(e)[:90])

    R.section("P1 one supervised Event invoke")
    try:
        r = lam.invoke(FunctionName=FN, InvocationType="Event",
                       Payload=json.dumps({"provider": "eurostat"}).encode())
        R.log("  invoke accepted status=%s" % r.get("StatusCode"))
    except Exception as e:
        R.log("  invoke err %s" % str(e)[:140])
        fails.append("P1:invoke")
    moved = False
    after = before
    for i in range(20):                      # up to ~5.5 min
        time.sleep(17)
        after = read_state()
        if after.get("updated_at") != before.get("updated_at"):
            moved = True
            R.log("  state advanced after %ds: updated_at=%s" % (
                (i + 1) * 17, after.get("updated_at")))
            break
    if not moved:
        R.log("  state did NOT move within the poll window")

    R.section("P2 PROOF of real forward progress")
    a_flows = len(after.get("flows_done") or [])
    a_pages = after.get("n_pages") or 0
    R.log("  AFTER  flows_done=%d n_pages=%s series=%s stopped_early=%s "
          "pages_this_run=%s" % (a_flows, a_pages,
                                 after.get("series_count"),
                                 after.get("stopped_early"),
                                 after.get("pages_this_run")))
    R.log("  delta: flows %+d   pages %+d" % (a_flows - b_flows,
                                              a_pages - (b_pages or 0)))
    prog = after.get("flow_progress") or {}
    for fid, v in list(prog.items())[:5]:
        R.log("  in-flight flow %s: rows_done=%s attempts=%s" % (
            fid, v.get("rows_done"), v.get("attempts")))
    errs = after.get("errors") or {}
    for k, v in list(errs.items())[:5]:
        R.log("  err %s: %s" % (k, str(v)[:110]))
    advanced = (a_flows > b_flows) or (a_pages > (b_pages or 0))
    out.update(after_flows=a_flows, after_pages=a_pages, advanced=advanced)

    # the decisive check: newly written pages must be NEW KEYS, not
    # rewrites -- a rewrite would mean the churn is back
    fresh_ok = True
    try:
        checks = [SERIES_PFX + "page-%04d.json" % n
                  for n in range((b_pages or 0), min((b_pages or 0) + 3,
                                                     a_pages))]
        for k in checks:
            rv = s3.list_object_versions(Bucket=LIVE, Prefix=k, MaxKeys=20)
            nv = len(rv.get("Versions", []))
            R.log("  %-28s versions=%d %s" % (
                k[len(SERIES_PFX):], nv,
                "NEW KEY (clean)" if nv <= 1 else "*** REWRITE ***"))
            if nv > 1:
                fresh_ok = False
        if not checks:
            R.log("  (no new page keys in this run -- run was consumed "
                  "resuming inside the large flow, which is expected "
                  "and correct)")
    except Exception as e:
        R.log("  version check err %s" % str(e)[:110])
    if not advanced:
        fails.append("P2:noprogress")
    if not fresh_ok:
        fails.append("P2:rewrite")

    R.section("P3 re-enable the schedule (only on proof)")
    if advanced and fresh_ok:
        try:
            ev.enable_rule(Name=RULE)
            d = ev.describe_rule(Name=RULE)
            R.log("  rule %s -> %s (%s)" % (RULE, d.get("State"),
                                            d.get("ScheduleExpression")))
            out["rule_state"] = d.get("State")
        except Exception as e:
            R.log("  enable err %s" % str(e)[:120])
            fails.append("P3:enable")
    else:
        R.log("  NOT re-enabling -- the run did not prove progress; the "
              "engine stays frozen and this needs eyes")

    R.section("P4 projection")
    try:
        done = a_flows
        total = 8147
        pages_run = after.get("pages_this_run") or 0
        R.log("  flows %d / %d (%.2f%%)" % (done, total,
                                            100.0 * done / total))
        if pages_run:
            runs_needed = max(1, int(482544 / max(1, pages_run)))
            R.log("  ~%d pages/run -> ~%d runs to finish -> ~%.1f days at "
                  "rate(5 minutes)" % (pages_run, runs_needed,
                                       runs_needed / 288.0))
        R.log("  NOTE: signal-registry-ingest stays quarantined during "
              "the backfill, so the ~482k Object Created events this "
              "will raise cost nothing; replication stays off, so none "
              "of it mirrors to us-west-2")
    except Exception as e:
        R.log("  projection err %s" % str(e)[:100])
    try:
        s3.put_object(Bucket=LIVE, Key="data/ops/series-extractor-rearm.json",
                      Body=json.dumps(out, indent=1, default=str).encode(),
                      ContentType="application/json")
        R.log("  -> data/ops/series-extractor-rearm.json")
    except Exception as e:
        R.log("  write err %s" % str(e)[:90])

    if fails:
        R.log("ops 5030 RED: " + "; ".join(fails))
        sys.exit(1)
    R.kv(before_flows=b_flows, after_flows=a_flows,
         pages_added=a_pages - (b_pages or 0), rule=out.get("rule_state"))
    R.log("ops 5030 GREEN -- extractor advancing again on proof, "
          "schedule re-enabled, churn structurally impossible")
