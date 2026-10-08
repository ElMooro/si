"""ops_5034 -- run the backfill at speed.

Khalid: "fill up the data fast, im not worried about the budget."

Measured ceiling before this op: 311 pages/min. The cause was not AWS --
it was the engine writing pages SERIALLY. At ~40ms per S3 round trip a
single-threaded writer tops out near 4 pages/s, and the parser sat idle
between puts. On top of that MAX_PAGES_PER_RUN=1200 ended each run after
roughly a minute of a 220-second budget, so the function was idle most
of every 5-minute tick.

v3 (deployed with this op, all four properties proven offline first):
  * 32-thread write pool -- the ceiling moves onto CPU where it belongs
  * page writes are collected explicitly: series_count and page_hashes
    advance ONLY for pages that actually landed; a dropped write is
    named in state["missing_pages"] and a later run rewrites exactly
    those holes and skips everything else
  * MAX_PAGES_PER_RUN retired -- a run is bounded by TIME, not a count

This op then reshapes the runtime around it:
  memory   3008 -> 10240 MB   (~6 vCPU, and the top network tier)
  timeout   280 -> 900 s      (BUDGET_S 840, 60s reserved to drain)
  cadence  rate(5 min) -> rate(2 min)
  reserved concurrency = 1

The concurrency reservation is the safety interlock, not a throttle:
the cadence is now shorter than the timeout, so without it two runs
could execute together, both read the same n_pages, and both write the
same page keys -- which is precisely the version churn this whole arc
existed to kill. With a reservation of 1, an overlapping tick is
throttled and retried by Lambda instead, so the worker is kept busy
continuously and can never race itself.

  P0 wait for v3, apply memory/timeout, set concurrency 1, set cadence
  P1 kick, then measure over ~11 minutes
  P2 new rate, write-error count, holes, ETA
"""
import json
import sys
import time
from datetime import datetime, timedelta, timezone
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
FLOWS_TOTAL = 8147
TARGET_PAGES = 486000          # 241M series / 500 per page

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


with report("ops_5034_throughput_unlock") as R:
    fails = []
    out = {"op": "ops_5034"}
    NOW = datetime.now(timezone.utc)

    R.section("P0 reshape the runtime")
    for i in range(18):
        try:
            c = lam.get_function_configuration(FunctionName=FN)
            lm = (c.get("LastModified") or "")[:19]
            if lm >= (NOW - timedelta(minutes=12)).strftime(
                    "%Y-%m-%dT%H:%M:%S"):
                R.log("  v3 code present (LastModified=%s)" % lm)
                break
        except Exception:
            pass
        time.sleep(20)
    try:
        lam.update_function_configuration(FunctionName=FN,
                                          MemorySize=10240, Timeout=900)
        for _ in range(20):
            time.sleep(4)
            c = lam.get_function_configuration(FunctionName=FN)
            if c.get("LastUpdateStatus") == "Successful":
                break
        R.log("  runtime: mem=%s MB timeout=%ss status=%s" % (
            c.get("MemorySize"), c.get("Timeout"),
            c.get("LastUpdateStatus")))
        out.update(memory=c.get("MemorySize"), timeout=c.get("Timeout"))
    except Exception as e:
        R.log("  runtime update err %s" % str(e)[:140])
        fails.append("P0:runtime")
    # interlock FIRST, cadence second -- never the other way round
    try:
        lam.put_function_concurrency(FunctionName=FN,
                                     ReservedConcurrentExecutions=1)
        time.sleep(2)
        rc = lam.get_function_concurrency(FunctionName=FN)
        R.log("  reserved concurrency = %s (serialisation interlock)"
              % rc.get("ReservedConcurrentExecutions"))
        if rc.get("ReservedConcurrentExecutions") != 1:
            fails.append("P0:interlock")
    except Exception as e:
        R.log("  concurrency err %s" % str(e)[:130])
        fails.append("P0:interlock")
    try:
        d0 = ev.describe_rule(Name=RULE)
        ev.put_rule(Name=RULE, ScheduleExpression="rate(2 minutes)",
                    State="ENABLED")
        d = ev.describe_rule(Name=RULE)
        R.log("  cadence %s -> %s (%s)" % (d0.get("ScheduleExpression"),
                                           d.get("ScheduleExpression"),
                                           d.get("State")))
        t = ev.list_targets_by_rule(Name=RULE).get("Targets", [])
        R.log("  targets intact: %s" % [x.get("Arn", "").rsplit(":", 1)[-1]
                                        for x in t])
        if not t:
            fails.append("P0:targets")
    except Exception as e:
        R.log("  rule err %s" % str(e)[:130])
        fails.append("P0:rule")

    R.section("P1 measure")
    f0, p0, s0, _ = snap()
    R.log("  window opens: flows=%d pages=%d series=%d" % (f0, p0, s0))
    try:
        lam.invoke(FunctionName=FN, InvocationType="Event",
                   Payload=json.dumps({"provider": "eurostat"}).encode())
        R.log("  kick sent")
    except Exception as e:
        R.log("  kick err %s" % str(e)[:100])
    t0 = datetime.now(timezone.utc)
    last = (f0, p0, s0)
    for i in range(3):
        time.sleep(220)
        f, p, s, _ = snap()
        el = int((datetime.now(timezone.utc) - t0).total_seconds())
        R.log("  t+%4ds flows=%d (+%d) pages=%d (+%d) series=%d (+%d)"
              % (el, f, f - f0, p, p - p0, s, s - s0))
        last = (f, p, s)
    f1, p1, s1 = last
    el = max(1, int((datetime.now(timezone.utc) - t0).total_seconds()))
    ppm = (p1 - p0) * 60.0 / el
    spm = (s1 - s0) * 60.0 / el

    R.section("P2 new rate + ETA")
    R.log("  BEFORE (ops 5033): 311 pages/min, 155,500 series/min")
    R.log("  NOW              : %.0f pages/min, %.0f series/min  "
          "(%.1fx)" % (ppm, spm, ppm / 311.0 if ppm else 0))
    _, _, _, st1 = snap()
    R.log("  flows %d / %d (%.2f%%)  pages %d / ~%d (%.1f%%)" % (
        f1, FLOWS_TOTAL, 100.0 * f1 / FLOWS_TOTAL, p1, TARGET_PAGES,
        100.0 * p1 / TARGET_PAGES))
    R.log("  write errors this run: %s   holes recorded: %d" % (
        st1.get("write_errors_this_run"),
        len(st1.get("missing_pages") or [])))
    for w in (st1.get("write_errors") or [])[:5]:
        R.log("    %s" % str(w)[:140])
    failed = st1.get("failed_flows") or []
    R.log("  retired failed flows: %d %s" % (len(failed), failed[:6]))
    if ppm > 0:
        hrs = (TARGET_PAGES - p1) / ppm / 60.0
        R.log("  ETA to a complete Eurostat series universe: ~%.1f hours"
              % hrs)
        out["eta_hours"] = round(hrs, 1)
    out.update(pages_per_min=round(ppm), series_per_min=round(spm),
               flows=f1, pages=p1, series=s1,
               holes=len(st1.get("missing_pages") or []))
    if p1 <= p0:
        R.log("  NO PAGE MOVEMENT -- investigate before leaving it")
        fails.append("P2:stalled")
    try:
        s3.put_object(Bucket=LIVE,
                      Key="data/ops/eurostat-backfill-progress.json",
                      Body=json.dumps(out, indent=1, default=str).encode(),
                      ContentType="application/json")
        R.log("  -> data/ops/eurostat-backfill-progress.json")
    except Exception as e:
        R.log("  write err %s" % str(e)[:90])

    if fails:
        R.log("ops 5034 RED: " + "; ".join(fails))
        sys.exit(1)
    R.kv(pages_per_min=round(ppm), series_per_min=round(spm),
         pages=p1, eta_hours=out.get("eta_hours"))
    R.log("ops 5034 GREEN -- backfill running at speed")
