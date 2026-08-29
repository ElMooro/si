"""ops_5036 -- land the import, audit it, and stand the lane down.

State at ops 5035 (14:47Z): 6,272 pages/min, 3.14M series/min, pages
145,697, series 72.8M, 0 write errors, 0 holes, ETA ~54 min.

This op waits for the lane to finish, then does the three things that
must not be skipped when a backfill lands:

  P0 poll to completion (up to ~35 min, well inside the 90-min runner)
  P1 INTEGRITY -- the page counter is a claim, not evidence. Count the
     objects actually present under the series prefix and reconcile
     against n_pages. Any gap means the manifest promises pages that do
     not exist, which would break the reader silently.
  P2 EXCEPTIONS -- failed_flows and missing_pages. Note the trap: once
     every flow is in flows_done, `todo` is empty, so the engine will
     NEVER revisit a hole on its own. Holes must be named here and
     repaired deliberately, by removing those flows from flows_done so
     the next run re-parses exactly them.
  P3 STAND DOWN -- rate(2 minutes) at 10GB is a backfill posture, not a
     steady-state one. sdmx-walker keeps adding warm flows, so the lane
     stays armed but drops to an hourly cadence. The reserved
     concurrency interlock STAYS at 1 permanently: it is what makes two
     runs sharing one page counter impossible, which is the failure
     this entire arc was about.
  P4 closeout ledger + the honest coverage number
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
MANIFEST = "data/providers/eurostat/series-manifest.json"
WARM = "data/warm/eurostat/data/"

cfg = Config(read_timeout=120, retries={"max_attempts": 4})
s3 = boto3.client("s3", region_name=REGION, config=cfg)
lam = boto3.client("lambda", region_name=REGION, config=cfg)
ev = boto3.client("events", region_name=REGION, config=cfg)


def read_state():
    try:
        return json.loads(s3.get_object(Bucket=LIVE,
                                        Key=STATE_KEY)["Body"].read())
    except Exception:
        return {}


def count_prefix(prefix):
    n, byts = 0, 0
    kw = {"Bucket": LIVE, "Prefix": prefix, "MaxKeys": 1000}
    while True:
        r = s3.list_objects_v2(**kw)
        for o in r.get("Contents", []):
            n += 1
            byts += o["Size"]
        if not r.get("IsTruncated"):
            break
        kw["ContinuationToken"] = r.get("NextContinuationToken")
    return n, byts


with report("ops_5036_land_and_standdown") as R:
    fails = []
    out = {"op": "ops_5036"}

    R.section("P0 wait for the lane to finish")
    flows_total = 0
    try:
        flows_total, _ = count_prefix(WARM)
        R.log("  denominator: %d warm eurostat flow files" % flows_total)
    except Exception as e:
        R.log("  warm count err %s" % str(e)[:110])
        flows_total = 8147
    st = read_state()
    prev = -1
    complete = False
    for i in range(8):                       # up to ~35 min
        st = read_state()
        f = len(st.get("flows_done") or [])
        p = int(st.get("n_pages") or 0)
        s = int(st.get("series_count") or 0)
        R.log("  t+%2dmin flows=%d/%d (%.1f%%) pages=%d series=%d "
              "stopped_early=%s" % (i * 4, f, flows_total,
                                    100.0 * f / max(1, flows_total), p, s,
                                    st.get("stopped_early")))
        if f >= flows_total:
            complete = True
            R.log("  COMPLETE -- every warm flow is parsed")
            break
        if f == prev and i >= 2:
            R.log("  no flow movement in the last interval (large flow "
                  "in progress, or stalled -- exceptions checked below)")
        prev = f
        time.sleep(240)
    f1 = len(st.get("flows_done") or [])
    p1 = int(st.get("n_pages") or 0)
    s1 = int(st.get("series_count") or 0)
    out.update(flows=f1, flows_total=flows_total, n_pages=p1, series=s1,
               complete=complete)

    R.section("P1 integrity -- counted objects vs the page counter")
    try:
        n_obj, byts = count_prefix(PFX)
        R.log("  objects present under %s : %d  (%.2f GB)" % (
            PFX, n_obj, byts / 1e9))
        R.log("  state n_pages claims      : %d" % p1)
        gap = p1 - n_obj
        R.log("  gap: %d  %s" % (gap, "CLEAN" if abs(gap) <= 2 else
                                 "*** pages promised that do not exist ***"))
        out.update(objects_present=n_obj, bytes=byts, gap=gap)
        if abs(gap) > 2:
            fails.append("P1:gap")
        if n_obj:
            R.log("  avg page %.0f KB -> ~%.0f series/GB" % (
                byts / n_obj / 1024, s1 / max(1, byts / 1e9)))
    except Exception as e:
        R.log("  count err %s" % str(e)[:130])
        fails.append("P1")

    R.section("P2 exceptions")
    holes = st.get("missing_pages") or []
    failed = st.get("failed_flows") or []
    errs = st.get("errors") or {}
    R.log("  write errors last run: %s" % st.get("write_errors_this_run"))
    R.log("  missing_pages (holes): %d" % len(holes))
    for h in holes[:10]:
        R.log("    %s" % str(h)[:120])
    R.log("  retired failed_flows : %d" % len(failed))
    for x in failed[:10]:
        R.log("    %-28s %s" % (str(x)[:28], str(errs.get(x, ""))[:90]))
    R.log("  errors recorded      : %d" % len(errs))
    if holes:
        R.log("  NOTE: with every flow in flows_done, todo is empty and "
              "the engine will not revisit these on its own -- repairing "
              "them means dropping their flows from flows_done so the "
              "next run re-parses exactly those")
    out.update(holes=len(holes), failed_flows=failed[:20],
               errors=len(errs))

    R.section("P3 stand down to steady state")
    if complete:
        try:
            ev.put_rule(Name=RULE, ScheduleExpression="rate(1 hour)",
                        State="ENABLED")
            d = ev.describe_rule(Name=RULE)
            t = ev.list_targets_by_rule(Rule=RULE).get("Targets", [])
            R.log("  cadence -> %s (%s), targets=%d" % (
                d.get("ScheduleExpression"), d.get("State"), len(t)))
            if not t:
                fails.append("P3:notarget")
            out["schedule"] = d.get("ScheduleExpression")
        except Exception as e:
            R.log("  rule err %s" % str(e)[:130])
            fails.append("P3:rule")
        R.log("  memory/timeout LEFT AT 10240MB/900s -- sdmx-walker keeps "
              "adding flows and the next large one should not have to "
              "wait for another incident to get the headroom")
    else:
        R.log("  NOT standing down -- the lane has not finished; cadence "
              "stays at rate(2 minutes) so it keeps importing")
    try:
        rc = lam.get_function_concurrency(FunctionName=FN)
        n = rc.get("ReservedConcurrentExecutions")
        R.log("  reserved concurrency = %s %s" % (
            n, "(permanent race interlock)" if n == 1 else "*** NOT 1 ***"))
        if n != 1:
            fails.append("P3:interlock")
    except Exception as e:
        R.log("  concurrency err %s" % str(e)[:110])

    R.section("P4 closeout")
    try:
        man = json.loads(s3.get_object(Bucket=LIVE,
                                       Key=MANIFEST)["Body"].read())
        R.log("  manifest: flows_total=%s flows_parsed=%s "
              "series_extracted=%s n_pages=%s page_size=%s updated_at=%s"
              % (man.get("flows_total"), man.get("flows_parsed"),
                 man.get("series_extracted"), man.get("n_pages"),
                 man.get("page_size"), man.get("updated_at")))
        out["manifest_series"] = man.get("series_extracted")
    except Exception as e:
        R.log("  manifest err %s" % str(e)[:110])
    R.log("  COVERAGE: %d / %d flows (%.2f%%)  vs 79 / 8147 (0.97%%) "
          "before this arc" % (f1, flows_total,
                               100.0 * f1 / max(1, flows_total)))
    R.log("  series held: %d  (was 1,733,000)" % s1)
    try:
        s3.put_object(Bucket=LIVE,
                      Key="data/ops/eurostat-backfill-progress.json",
                      Body=json.dumps(out, indent=1, default=str).encode(),
                      ContentType="application/json")
        R.log("  -> data/ops/eurostat-backfill-progress.json")
    except Exception as e:
        R.log("  write err %s" % str(e)[:90])

    if fails:
        R.log("ops 5036 RED: " + "; ".join(fails))
        sys.exit(1)
    R.kv(complete=complete, flows=f1, flows_total=flows_total,
         pages=p1, objects=out.get("objects_present"), series=s1,
         holes=len(holes))
    R.log("ops 5036 GREEN -- %s" % (
        "import landed, integrity reconciled, lane at steady state"
        if complete else "import still running; measured and left alone"))
