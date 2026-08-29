"""ops_5033 -- unblock AVIA_GOEXAC and let the backfill run.

ops 5032 finally produced a real failure instead of a measurement
artifact: with the schedule ENABLED the lane sat at flows_done=129 for a
full 11 minutes, and the state named why --
    AVIA_GOEXAC   MemoryError   attempts=6   rows_done=0

Two independent defects, both fixed in v2.2 (deployed with this op):

 1. extract_eurostat did gzip.decompress(gz_bytes).decode(...) -- the
    entire dataset materialised twice, once as bytes and once as str. A
    42MB .gz of Eurostat TSV expands to several hundred MB, so a 1536MB
    Lambda died on it every time. Now streamed through GzipFile +
    TextIOWrapper: peak memory is one line. Parse equivalence was proven
    offline against a synthetic header + rows before shipping.
 2. the except branch recorded the error and retried the SAME flow
    forever, so one poisoned dataset blocked the other 8,018 behind it
    -- structurally the identical failure to the Aug-09 bug, just via
    the error path instead of the timeout path. v2.2 retires a flow
    after ERROR_ATTEMPTS=3 into state["failed_flows"] and moves on, and
    STALL_ATTEMPTS drops 40 -> 3.

Memory also goes 1536 -> 3008 MB here (config.json alone may not
re-apply to an existing function, so this op sets it explicitly).

  P0 raise memory, wait for the v2.2 code to land
  P1 clear the stuck in-flight record for AVIA_GOEXAC so the retry
     counters start clean against the fixed parser
  P2 invoke once, then observe ~11 minutes under the live schedule
  P3 report progress, failed flows, and the completion projection
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
STUCK = "AVIA_GOEXAC"
FLOWS_TOTAL = 8147

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


def snap():
    st = read_state()
    return (len(st.get("flows_done") or []), int(st.get("n_pages") or 0),
            int(st.get("series_count") or 0), st)


with report("ops_5033_unblock_and_backfill") as R:
    fails = []
    out = {"op": "ops_5033"}
    NOW = datetime.now(timezone.utc)

    R.section("P0 memory + wait for the v2.2 code")
    try:
        c = lam.get_function_configuration(FunctionName=FN)
        R.log("  before: mem=%s timeout=%s lastmod=%s" % (
            c.get("MemorySize"), c.get("Timeout"), c.get("LastModified")))
    except Exception as e:
        R.log("  cfg err %s" % str(e)[:100])
    landed = False
    for i in range(18):                       # up to 6 min
        try:
            c = lam.get_function_configuration(FunctionName=FN)
            lm = c.get("LastModified") or ""
            fresh = lm[:19] >= (NOW - timedelta(minutes=12)).strftime(
                "%Y-%m-%dT%H:%M:%S")
            if fresh and c.get("LastUpdateStatus", "Successful") == \
                    "Successful":
                landed = True
                R.log("  v2.2 code present (LastModified=%s) after %ds"
                      % (lm, i * 20))
                break
        except Exception:
            pass
        time.sleep(20)
    if not landed:
        R.log("  code freshness not confirmed -- proceeding anyway; the "
              "5-min schedule will pick it up regardless")
    try:
        lam.update_function_configuration(FunctionName=FN,
                                          MemorySize=3008)
        for _ in range(15):
            time.sleep(4)
            c = lam.get_function_configuration(FunctionName=FN)
            if c.get("LastUpdateStatus") == "Successful":
                break
        R.log("  memory now %s MB (status %s)" % (
            c.get("MemorySize"), c.get("LastUpdateStatus")))
        out["memory"] = c.get("MemorySize")
    except Exception as e:
        R.log("  memory update err %s" % str(e)[:130])

    R.section("P1 clear the stuck in-flight record")
    f0, p0, s0, st0 = snap()
    R.log("  flows_done=%d n_pages=%d series=%d errors=%d" % (
        f0, p0, s0, len(st0.get("errors") or {})))
    prog = st0.get("flow_progress") or {}
    if STUCK in prog:
        R.log("  %s before: %s" % (STUCK, json.dumps(prog[STUCK],
                                                     default=str)[:140]))
        prog.pop(STUCK, None)
        (st0.get("errors") or {}).pop(STUCK, None)
        st0["flow_progress"] = prog
        try:
            s3.put_object(Bucket=LIVE, Key=STATE_KEY,
                          Body=json.dumps(st0, default=str).encode(),
                          ContentType="application/json")
            R.log("  cleared -- retry counters start clean against the "
                  "streaming parser")
        except Exception as e:
            R.log("  state write err %s" % str(e)[:110])
            fails.append("P1")
    else:
        R.log("  %s not in flight (already retired or moved on)" % STUCK)

    R.section("P2 run and observe")
    try:
        d = ev.describe_rule(Name=RULE)
        R.log("  rule %s: %s" % (RULE, d.get("State")))
        if d.get("State") != "ENABLED":
            ev.enable_rule(Name=RULE)
            R.log("  re-enabled")
    except Exception as e:
        R.log("  rule err %s" % str(e)[:100])
    try:
        r = lam.invoke(FunctionName=FN, InvocationType="Event",
                       Payload=json.dumps({"provider": "eurostat"}).encode())
        R.log("  kick invoke status=%s" % r.get("StatusCode"))
    except Exception as e:
        R.log("  invoke err %s" % str(e)[:110])
    t0 = datetime.now(timezone.utc)
    f0, p0, s0, _ = snap()
    R.log("  window opens at flows=%d pages=%d series=%d" % (f0, p0, s0))
    last = (f0, p0, s0)
    for i in range(3):
        time.sleep(220)
        f, p, s, stx = snap()
        el = int((datetime.now(timezone.utc) - t0).total_seconds())
        R.log("  t+%4ds flows=%d (+%d) pages=%d (+%d) series=%d (+%d)"
              % (el, f, f - f0, p, p - p0, s, s - s0))
        last = (f, p, s)
    f1, p1, s1 = last
    el = max(1, int((datetime.now(timezone.utc) - t0).total_seconds()))
    fpm = (f1 - f0) * 60.0 / el
    ppm = (p1 - p0) * 60.0 / el
    R.log("  RATE %.2f flows/min  %.0f pages/min  %.0f series/min" % (
        fpm, ppm, (s1 - s0) * 60.0 / el))
    out.update(start_flows=f0, end_flows=f1, start_pages=p0, end_pages=p1,
               flows_per_min=round(fpm, 2))
    if f1 <= f0:
        R.log("  STILL NOT MOVING -- read the state errors below")
        fails.append("P2:stalled")

    R.section("P3 state after the window")
    _, _, _, st1 = snap()
    errs = st1.get("errors") or {}
    failed = st1.get("failed_flows") or []
    R.log("  errors=%d  retired failed_flows=%d" % (len(errs), len(failed)))
    for k, v in list(errs.items())[:10]:
        R.log("    %-26s %s" % (k[:26], str(v)[:110]))
    prog = st1.get("flow_progress") or {}
    for fid, v in list(prog.items())[:5]:
        R.log("  in-flight %-24s rows_done=%s attempts=%s errs=%s" % (
            fid[:24], v.get("rows_done"), v.get("attempts"),
            v.get("error_count")))
    R.log("  flows %d / %d  (%.2f%%)" % (f1, FLOWS_TOTAL,
                                         100.0 * f1 / FLOWS_TOTAL))
    if fpm > 0:
        hrs = (FLOWS_TOTAL - f1) / fpm / 60.0
        R.log("  ETA to a complete Eurostat series universe: ~%.1f hours "
              "(~%.1f days) at the measured rate" % (hrs, hrs / 24.0))
        out["eta_hours"] = round(hrs, 1)
    out.update(errors=len(errs), failed_flows=failed[:20])
    try:
        s3.put_object(Bucket=LIVE,
                      Key="data/ops/eurostat-backfill-progress.json",
                      Body=json.dumps(out, indent=1, default=str).encode(),
                      ContentType="application/json")
        R.log("  -> data/ops/eurostat-backfill-progress.json")
    except Exception as e:
        R.log("  write err %s" % str(e)[:90])

    if fails:
        R.log("ops 5033 RED: " + "; ".join(fails))
        sys.exit(1)
    R.kv(flows=f1, pct=round(100.0 * f1 / FLOWS_TOTAL, 2), pages=p1,
         flows_per_min=round(fpm, 2), eta_hours=out.get("eta_hours"))
    R.log("ops 5033 GREEN -- AVIA_GOEXAC unblocked, lane importing")
