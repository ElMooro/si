"""ops_4986 -- COST DOWNSHIFT (Khalid: imports done, stop the waste).

Evidence: AWS recursive-loop auto-remediation (our MIDAS chains --
retired now that drains are COMPLETE) + $179 S3 Requests-Tier1
anomaly (drain-era state churn) + a us-west-2 SIA line to expose.

  P1 schedule census -> downshift every high-frequency schedule:
       <=2h  -> rate(24 hours)   (drain restarters; drains done)
       30m   -> rate(6 hours)    (gdelt live edge, 48->4 runs/day)
       6h    -> rate(24 hours)   (imf/finra deltas)
       12h   -> rate(24 hours)   (mirror refreshers)
     weekly redrains and daily+ cadences stay untouched
  P2 recursion-config census on the chaining fleet (what AWS
     flagged); worldbank exempt from chain-kill until COMPLETE
  P3 bucket/region census -> name the us-west-2 requester
  P4 worldbank still progresses (tail ~1.5k finishes on ticks)
"""
import gzip
import json
import sys
import time
from pathlib import Path

import boto3

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from ops_report import report  # noqa: E402

REGION = "us-east-1"
B = "justhodl-dashboard-live"
sch = boto3.client("scheduler", region_name=REGION)
lam = boto3.client("lambda", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)
s3g = boto3.client("s3")


def gj(key, default=None):
    try:
        raw = s3.get_object(Bucket=B, Key=key)["Body"].read()
        if raw[:2] == b"\x1f\x8b":
            raw = gzip.decompress(raw)
        return json.loads(raw)
    except Exception:
        return default


MAP = {"rate(30 minutes)": "rate(6 hours)",
       "rate(1 hour)": "rate(24 hours)",
       "rate(2 hours)": "rate(24 hours)",
       "rate(6 hours)": "rate(24 hours)",
       "rate(12 hours)": "rate(24 hours)"}
KEEP = {"justhodl-worldbank-full-2h"}   # until COMPLETE

with report("ops_4986_cost_downshift") as R:
    fails = []
    R.section("P1 schedule downshift")
    changed, kept = 0, 0
    names = []
    pag = sch.get_paginator("list_schedules")
    for pg in pag.paginate(GroupName="default"):
        names += [x["Name"] for x in pg.get("Schedules", [])]
    wb_state = gj("data/warm/worldbank-full/_state/state.json") \
        or {}
    wb_left = len(wb_state.get("queue") or [])
    for nm in sorted(names):
        try:
            g = sch.get_schedule(GroupName="default", Name=nm)
            expr = g.get("ScheduleExpression")
            tgt = MAP.get(expr)
            if nm in KEEP and wb_left > 0:
                R.log("  KEEP  %-38s %s (wb queue=%d)" % (
                    nm, expr, wb_left))
                kept += 1
                continue
            if not tgt:
                kept += 1
                continue
            sch.update_schedule(
                GroupName="default", Name=nm,
                ScheduleExpression=tgt,
                FlexibleTimeWindow=g.get("FlexibleTimeWindow")
                or {"Mode": "OFF"},
                Target=g["Target"], State="ENABLED")
            R.log("  SHIFT %-38s %s -> %s" % (nm, expr, tgt))
            changed += 1
        except Exception as e:
            R.log("  ERR   %-38s %s" % (nm, str(e)[:70]))
            fails.append("P1:" + nm)
    R.log("  P1: %d downshifted, %d kept, %d total" % (
        changed, kept, len(names)))
    if changed < 5:
        fails.append("P1-thin")

    R.section("P2 recursion-config census")
    flagged = []
    for fn in ["justhodl-gdelt-full", "justhodl-imf-full",
               "justhodl-worldbank-full", "justhodl-polygon-full",
               "justhodl-finra-full", "justhodl-fiscaldata-full",
               "justhodl-bls-full", "justhodl-census-full"]:
        try:
            rc = lam.get_function_recursion_config(
                FunctionName=fn)
            R.log("  %-28s RecursiveLoop=%s" % (
                fn, rc.get("RecursiveLoop")))
            if rc.get("RecursiveLoop") == "Terminate":
                flagged.append(fn)
        except Exception as e:
            R.log("  %-28s %s" % (fn, str(e)[:60]))
    R.log("  AWS-terminating self-invokes on: %s" %
          (flagged or "none (default Allow)"))

    R.section("P3 bucket/region census (us-west-2 mystery)")
    try:
        for bkt in s3g.list_buckets().get("Buckets", []):
            nm = bkt["Name"]
            try:
                loc = s3g.get_bucket_location(Bucket=nm).get(
                    "LocationConstraint") or "us-east-1"
            except Exception as e:
                loc = "err:" + str(e)[:40]
            mark = "  <-- us-west-2!" if "us-west-2" in str(loc) \
                else ""
            R.log("  %-42s %s%s" % (nm, loc, mark))
    except Exception as e:
        R.log("  bucket census err %s" % str(e)[:80])

    R.section("P4 worldbank tail")
    b0 = wb_state.get("n_banked") or \
        len(wb_state.get("have") or {})
    R.log("  banked=%s queue=%d (finishes on kept 2h ticks; "
          "schedule downshifts after COMPLETE next hygiene pass)"
          % (b0, wb_left))

    if fails:
        R.log("ops 4986 RED: " + "; ".join(fails))
        sys.exit(1)
    R.kv(downshifted=changed, kept=kept,
         wb_queue=wb_left, aws_flagged=len(flagged))
    R.log("ops 4986 GREEN -- steady-state cadence: ~90%% fewer "
          "invocations/day; S3 request burn collapses with the "
          "drains; chains retired by completion")
