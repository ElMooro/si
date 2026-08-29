"""ops_5035 -- verify the rule wiring, then watch the import land.

ops 5034 delivered the throughput but went RED on its own verification
call: list_targets_by_rule takes Rule=, not Name=. The put_rule itself
succeeded -- describe_rule read back rate(2 minutes)/ENABLED -- so the
cadence is live and the measured rate proves work is flowing. Only the
target read-back was malformed, and a rule with no target would be a
silent dead lane, so it gets checked properly here.

Measured after v3:
    3,874 pages/min   1,937,014 series/min   (12.5x)
    flows 331/8147, pages 52,769/~486,000, 0 write errors, 0 holes
    ETA ~1.9 hours

  P0 rule wiring: schedule, state, and the ACTUAL target list
  P1 runtime: memory, timeout, concurrency interlock
  P2 observe ~11 minutes, recompute rate and ETA
  P3 integrity: write errors, holes, retired flows, and a read-back of a
     freshly written page to prove the parallel writer emits valid docs
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
FLOWS_TOTAL = 8147
TARGET_PAGES = 486000

cfg = Config(read_timeout=120, retries={"max_attempts": 3})
s3 = boto3.client("s3", region_name=REGION, config=cfg)
lam = boto3.client("lambda", region_name=REGION, config=cfg)
ev = boto3.client("events", region_name=REGION, config=cfg)


def snap():
    try:
        st = json.loads(s3.get_object(Bucket=LIVE,
                                      Key=STATE_KEY)["Body"].read())
    except Exception:
        st = {}
    return (len(st.get("flows_done") or []), int(st.get("n_pages") or 0),
            int(st.get("series_count") or 0), st)


with report("ops_5035_verify_and_track") as R:
    fails = []
    out = {"op": "ops_5035"}

    R.section("P0 rule wiring")
    try:
        d = ev.describe_rule(Name=RULE)
        R.log("  %s state=%s schedule=%s" % (RULE, d.get("State"),
                                             d.get("ScheduleExpression")))
        if d.get("State") != "ENABLED":
            fails.append("P0:disabled")
        t = ev.list_targets_by_rule(Rule=RULE).get("Targets", [])
        R.log("  targets: %d" % len(t))
        for x in t:
            R.log("    id=%s arn=%s input=%s" % (
                x.get("Id"), x.get("Arn"),
                str(x.get("Input"))[:60]))
        if not any(FN in (x.get("Arn") or "") for x in t):
            R.log("  *** the rule has no target on %s -- dead lane ***" % FN)
            fails.append("P0:notarget")
        else:
            R.log("  target intact -- put_rule preserved the wiring")
        out["schedule"] = d.get("ScheduleExpression")
        out["targets"] = len(t)
    except Exception as e:
        R.log("  rule err %s" % str(e)[:150])
        fails.append("P0")

    R.section("P1 runtime")
    try:
        c = lam.get_function_configuration(FunctionName=FN)
        R.log("  mem=%s MB timeout=%ss lastmod=%s" % (
            c.get("MemorySize"), c.get("Timeout"), c.get("LastModified")))
        rc = lam.get_function_concurrency(FunctionName=FN)
        n = rc.get("ReservedConcurrentExecutions")
        R.log("  reserved concurrency = %s %s" % (
            n, "(interlock holding)" if n == 1 else "*** NOT 1 ***"))
        if n != 1:
            fails.append("P1:interlock")
        out.update(memory=c.get("MemorySize"), timeout=c.get("Timeout"))
    except Exception as e:
        R.log("  runtime err %s" % str(e)[:120])

    R.section("P2 observe")
    f0, p0, s0, _ = snap()
    t0 = datetime.now(timezone.utc)
    R.log("  window opens: flows=%d pages=%d series=%d" % (f0, p0, s0))
    last = (f0, p0, s0)
    done_early = False
    for i in range(3):
        time.sleep(220)
        f, p, s, stx = snap()
        el = int((datetime.now(timezone.utc) - t0).total_seconds())
        R.log("  t+%4ds flows=%d (+%d) pages=%d (+%d) series=%d (+%d)"
              % (el, f, f - f0, p, p - p0, s, s - s0))
        last = (f, p, s)
        if f >= FLOWS_TOTAL:
            R.log("  ALL FLOWS PARSED")
            done_early = True
            break
    f1, p1, s1 = last
    el = max(1, int((datetime.now(timezone.utc) - t0).total_seconds()))
    ppm = (p1 - p0) * 60.0 / el
    spm = (s1 - s0) * 60.0 / el
    R.log("  rate: %.0f pages/min  %.0f series/min" % (ppm, spm))
    R.log("  flows %d / %d (%.2f%%)   pages %d (%.1f%% of ~%d)" % (
        f1, FLOWS_TOTAL, 100.0 * f1 / FLOWS_TOTAL, p1,
        100.0 * p1 / TARGET_PAGES, TARGET_PAGES))
    if ppm > 0 and not done_early:
        R.log("  ETA: ~%.1f hours (~%.0f min)" % (
            (TARGET_PAGES - p1) / ppm / 60.0, (TARGET_PAGES - p1) / ppm))
        out["eta_min"] = round((TARGET_PAGES - p1) / ppm)
    if p1 <= p0 and not done_early:
        fails.append("P2:stalled")
    out.update(flows=f1, pages=p1, series=s1, pages_per_min=round(ppm),
               series_per_min=round(spm))

    R.section("P3 integrity of what the parallel writer produced")
    _, _, _, st1 = snap()
    holes = st1.get("missing_pages") or []
    R.log("  write errors this run=%s  holes=%d  retired flows=%d" % (
        st1.get("write_errors_this_run"), len(holes),
        len(st1.get("failed_flows") or [])))
    for w in (st1.get("write_errors") or [])[:5]:
        R.log("    %s" % str(w)[:140])
    if holes:
        R.log("  holes: %s (a later run rewrites exactly these)"
              % holes[:6])
    probe = PFX + "page-%04d.json" % max(0, p1 - 5)
    try:
        doc = json.loads(s3.get_object(Bucket=LIVE,
                                       Key=probe)["Body"].read())
        rows = doc.get("rows") or []
        R.log("  read-back %s: page=%s count=%s rows=%d" % (
            probe[len(PFX):], doc.get("page"), doc.get("count"),
            len(rows)))
        if rows:
            r0 = rows[0]
            R.log("    sample id=%s flow=%s geo=%s last_obs=%s "
                  "last_value=%s" % (str(r0.get("id"))[:46],
                                     r0.get("flow"), r0.get("geo"),
                                     r0.get("last_obs"),
                                     r0.get("last_value")))
            R.log("    fields: %s" % sorted(r0.keys()))
        if len(rows) != doc.get("count"):
            fails.append("P3:corrupt")
    except Exception as e:
        R.log("  read-back FAILED %s" % str(e)[:120])
        fails.append("P3:readback")
    try:
        s3.put_object(Bucket=LIVE,
                      Key="data/ops/eurostat-backfill-progress.json",
                      Body=json.dumps(out, indent=1, default=str).encode(),
                      ContentType="application/json")
        R.log("  -> data/ops/eurostat-backfill-progress.json")
    except Exception as e:
        R.log("  write err %s" % str(e)[:90])

    if fails:
        R.log("ops 5035 RED: " + "; ".join(fails))
        sys.exit(1)
    R.kv(flows=f1, pages=p1, series=s1, pages_per_min=round(ppm),
         eta_min=out.get("eta_min"))
    R.log("ops 5035 GREEN -- wiring verified, import landing, pages "
          "valid")
