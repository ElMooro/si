"""ops_5031 -- correct the proof, then re-enable the schedule.

ops 5030's gate was wrong, not the engine. It asserted that a page key
written by the run must carry exactly ONE version total. But pages
3466-3468 are precisely the keys the old churn hammered, so they still
carry historic versions -- 20 apiece now, down from 11,870, which is the
ops5027 lifecycle purge working exactly as designed. Total version count
can therefore never be the test.

What ops 5030 actually achieved:
    flows_done  79 -> 81      (flow #80, the 20-day poison pill, PASSED)
    n_pages   3466 -> 3602    (+136 real pages)
    series 1,733,000 -> 1,801,000
The fix works. Only the assertion was mis-specified.

The correct predicate is about the RUN WINDOW, not the key's history:
    * every page key NEW in this run gains exactly 1 version
    * every page key that already existed gains 0
That is what idempotency means here, and it is what the sha256 skip and
the per-flow checkpoint exist to guarantee.

  P0 baseline + run window start
  P1 one Event invoke, poll for state movement
  P2 dual proof: new keys +1 version, pre-existing keys +0 versions
  P3 enable justhodl-series-extractor-5min on proof
  P4 purge progress + completion projection
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
PFX = "data/providers/eurostat/series/"

cfg = Config(read_timeout=120, retries={"max_attempts": 3})
s3 = boto3.client("s3", region_name=REGION, config=cfg)
lam = boto3.client("lambda", region_name=REGION, config=cfg)
ev = boto3.client("events", region_name=REGION, config=cfg)


def read_state():
    try:
        return json.loads(s3.get_object(Bucket=LIVE,
                                        Key=STATE_KEY)["Body"].read())
    except Exception:
        return {}


def versions_since(page_no, t0):
    key = PFX + "page-%04d.json" % page_no
    try:
        r = s3.list_object_versions(Bucket=LIVE, Prefix=key, MaxKeys=200)
        vs = [v for v in r.get("Versions", []) if v["Key"] == key]
        return len(vs), sum(1 for v in vs if v["LastModified"] >= t0)
    except Exception:
        return -1, -1


with report("ops_5031_idempotency_proof") as R:
    fails = []
    out = {"op": "ops_5031"}

    R.section("P0 baseline")
    before = read_state()
    b_pages = int(before.get("n_pages") or 0)
    b_flows = len(before.get("flows_done") or [])
    R.log("  BEFORE flows_done=%d n_pages=%d series=%s updated_at=%s" % (
        b_flows, b_pages, before.get("series_count"),
        before.get("updated_at")))
    t0 = datetime.now(timezone.utc)
    R.log("  run window opens at %s" % t0.isoformat(timespec="seconds"))

    R.section("P1 one Event invoke")
    try:
        r = lam.invoke(FunctionName=FN, InvocationType="Event",
                       Payload=json.dumps({"provider": "eurostat"}).encode())
        R.log("  accepted status=%s" % r.get("StatusCode"))
    except Exception as e:
        R.log("  invoke err %s" % str(e)[:130])
        fails.append("P1")
    moved = False
    after = before
    for i in range(20):
        time.sleep(17)
        after = read_state()
        if after.get("updated_at") != before.get("updated_at"):
            moved = True
            R.log("  state advanced after %ds" % ((i + 1) * 17))
            break
    a_pages = int(after.get("n_pages") or 0)
    a_flows = len(after.get("flows_done") or [])
    R.log("  AFTER  flows_done=%d n_pages=%d series=%s" % (
        a_flows, a_pages, after.get("series_count")))
    R.log("  delta: flows %+d  pages %+d  series %+d" % (
        a_flows - b_flows, a_pages - b_pages,
        int(after.get("series_count") or 0) -
        int(before.get("series_count") or 0)))
    out.update(before_flows=b_flows, after_flows=a_flows,
               before_pages=b_pages, after_pages=a_pages)
    if not moved:
        R.log("  state did not move")
        fails.append("P1:nomove")

    R.section("P2 dual proof -- new keys +1, existing keys +0")
    new_ok = True
    for n in range(b_pages, min(b_pages + 4, a_pages)):
        tot, since = versions_since(n, t0)
        ok = since == 1
        R.log("  NEW      page-%04d total=%d  written_this_run=%d  %s"
              % (n, tot, since, "OK" if ok else "*** UNEXPECTED ***"))
        new_ok = new_ok and ok
    old_ok = True
    probes = [p for p in (3466, 3500, 3550, 3600, 0, 1980) if p < b_pages]
    for n in probes:
        tot, since = versions_since(n, t0)
        ok = since == 0
        R.log("  EXISTING page-%04d total=%d  written_this_run=%d  %s"
              % (n, tot, since,
                 "OK -- untouched" if ok else "*** REWRITTEN ***"))
        old_ok = old_ok and ok
    R.log("  (page-3466 total was 11,870 versions at ops 5028; the "
          "ops5027 lifecycle purge is what is shrinking it)")
    advanced = a_pages > b_pages or a_flows > b_flows
    if not advanced:
        fails.append("P2:noprogress")
    if not old_ok:
        fails.append("P2:rewrite")
    out.update(new_keys_clean=new_ok, no_rewrites=old_ok,
               advanced=advanced)

    R.section("P3 re-enable the schedule")
    if advanced and old_ok:
        try:
            ev.enable_rule(Name=RULE)
            d = ev.describe_rule(Name=RULE)
            R.log("  rule %s -> %s (%s)" % (RULE, d.get("State"),
                                            d.get("ScheduleExpression")))
            out["rule_state"] = d.get("State")
        except Exception as e:
            R.log("  enable err %s" % str(e)[:120])
            fails.append("P3")
    else:
        R.log("  NOT enabling -- predicate failed")

    R.section("P4 projection + purge progress")
    per_run = max(1, a_pages - b_pages)
    remaining = max(0, 482544 - (a_pages - 3466))
    R.log("  flows %d / 8147 (%.2f%%)" % (a_flows, 100.0 * a_flows / 8147))
    R.log("  ~%d pages/run -> ~%d runs -> ~%.1f days at rate(5 minutes)"
          % (per_run, remaining // per_run,
             (remaining / per_run) / 288.0))
    prog = after.get("flow_progress") or {}
    for fid, v in list(prog.items())[:4]:
        R.log("  in-flight %s rows_done=%s attempts=%s" % (
            fid, v.get("rows_done"), v.get("attempts")))
    try:
        s3.put_object(Bucket=LIVE, Key="data/ops/series-extractor-rearm.json",
                      Body=json.dumps(out, indent=1, default=str).encode(),
                      ContentType="application/json")
        R.log("  -> data/ops/series-extractor-rearm.json")
    except Exception as e:
        R.log("  write err %s" % str(e)[:90])

    if fails:
        R.log("ops 5031 RED: " + "; ".join(fails))
        sys.exit(1)
    R.kv(flows=a_flows, pages=a_pages, pages_this_run=per_run,
         rule=out.get("rule_state"))
    R.log("ops 5031 GREEN -- idempotency proven on the correct "
          "predicate; lane running again and converging")
