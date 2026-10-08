"""ops_5032 -- enable the lane and measure the real backfill rate.

ops 5031 went RED on "noprogress" -- a measurement artifact, for the
third time in this arc. Its poll broke out of the wait as soon as
state.updated_at changed, but v2 checkpoints after EVERY flow, so the
first change fires seconds into a 280s invocation that is still running.
It measured the run's opening checkpoint and called it the result.

What the two supervised invokes actually did:
    flows_done   79 -> 129     (the Aug-09 poison pill cleared)
    n_pages    3466 -> 3961    (+495 real pages)
    series 1,733,000 -> 1,980,500
and the idempotency predicate PASSED outright: pages 0000, 1980, 3466,
3500, 3550, 3600 all showed written_this_run=0. Nothing is rewritten.
The engine is correct; only the gate was wrong.

  P0 current truth: flows, pages, series, errors, in-flight flows
  P1 ENABLE justhodl-series-extractor-5min
  P2 observe three samples across ~11 minutes -- real flows/min and
     pages/min under the live schedule, not a single-invoke snapshot
  P3 idempotency under load: sample pre-existing keys and prove they
     gained zero versions across the whole observation window
  P4 project completion + write a progress ledger the next op can read

GREEN = the lane is enabled and measurably converging.
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


def versions_since(page_no, t0):
    key = PFX + "page-%04d.json" % page_no
    try:
        r = s3.list_object_versions(Bucket=LIVE, Prefix=key, MaxKeys=300)
        vs = [v for v in r.get("Versions", []) if v["Key"] == key]
        return len(vs), sum(1 for v in vs if v["LastModified"] >= t0)
    except Exception:
        return -1, -1


with report("ops_5032_enable_and_measure") as R:
    fails = []
    out = {"op": "ops_5032"}

    R.section("P0 current truth")
    f0, p0, s0, st0 = snap()
    R.log("  flows_done=%d / %d (%.2f%%)  n_pages=%d  series=%d" % (
        f0, FLOWS_TOTAL, 100.0 * f0 / FLOWS_TOTAL, p0, s0))
    R.log("  updated_at=%s" % st0.get("updated_at"))
    errs = st0.get("errors") or {}
    R.log("  errored flows: %d" % len(errs))
    for k, v in list(errs.items())[:10]:
        R.log("    %-28s %s" % (k[:28], str(v)[:110]))
    prog = st0.get("flow_progress") or {}
    R.log("  in-flight flows: %d" % len(prog))
    for fid, v in list(prog.items())[:6]:
        R.log("    %-28s rows_done=%s attempts=%s" % (
            fid[:28], v.get("rows_done"), v.get("attempts")))
    out.update(start_flows=f0, start_pages=p0, start_series=s0,
               errors=len(errs))
    t0 = datetime.now(timezone.utc)

    R.section("P1 enable the schedule")
    try:
        ev.enable_rule(Name=RULE)
        d = ev.describe_rule(Name=RULE)
        R.log("  rule %s -> %s (%s)" % (RULE, d.get("State"),
                                        d.get("ScheduleExpression")))
        out["rule_state"] = d.get("State")
        if d.get("State") != "ENABLED":
            fails.append("P1:rule")
    except Exception as e:
        R.log("  enable err %s" % str(e)[:130])
        fails.append("P1")
    try:
        rc = lam.get_function_concurrency(FunctionName=FN)
        R.log("  reserved concurrency: %s" % rc.get(
            "ReservedConcurrentExecutions", "unreserved"))
    except Exception as e:
        R.log("  conc err %s" % str(e)[:90])

    R.section("P2 observe -- three samples across ~11 minutes")
    samples = [(0, f0, p0, s0)]
    for i in range(3):
        time.sleep(220)
        f, p, s, _ = snap()
        el = int((datetime.now(timezone.utc) - t0).total_seconds())
        R.log("  t+%4ds  flows=%d (+%d)  pages=%d (+%d)  series=%d (+%d)"
              % (el, f, f - f0, p, p - p0, s, s - s0))
        samples.append((el, f, p, s))
    f1, p1, s1 = samples[-1][1], samples[-1][2], samples[-1][3]
    elapsed = max(1, samples[-1][0])
    fpm = (f1 - f0) * 60.0 / elapsed
    ppm = (p1 - p0) * 60.0 / elapsed
    R.log("  RATE: %.1f flows/min, %.0f pages/min, %.0f series/min" % (
        fpm, ppm, (s1 - s0) * 60.0 / elapsed))
    out.update(end_flows=f1, end_pages=p1, end_series=s1,
               flows_per_min=round(fpm, 2), pages_per_min=round(ppm, 1))
    if f1 <= f0 and p1 <= p0:
        R.log("  NO MOVEMENT across the whole window -- the lane is not "
              "converging and needs eyes")
        fails.append("P2:stalled")

    R.section("P3 idempotency under the live schedule")
    clean = True
    for n in (0, 1980, 3466, 3500, 3600, 3900):
        if n >= p0:
            continue
        tot, since = versions_since(n, t0)
        ok = since == 0
        clean = clean and ok
        R.log("  page-%04d total_versions=%d  written_in_window=%d  %s" % (
            n, tot, since, "OK" if ok else "*** REWRITTEN ***"))
    R.log("  (page-3466 held 11,870 versions at ops 5028 -- the ops5027 "
          "purge is sweeping them; totals here should keep falling)")
    if not clean:
        fails.append("P3:rewrite")
    out["no_rewrites"] = clean

    R.section("P4 projection")
    remaining_flows = FLOWS_TOTAL - f1
    if fpm > 0:
        hrs = remaining_flows / fpm / 60.0
        R.log("  %d flows left at %.1f flows/min -> ~%.1f hours (~%.1f "
              "days) to a complete Eurostat series universe" % (
                  remaining_flows, fpm, hrs, hrs / 24.0))
        out["eta_hours"] = round(hrs, 1)
    R.log("  NOTE for the cost inbox: this backfill legitimately writes "
          "at a rate close to the anomaly's, because the anomaly was "
          "this engine doing the same work and throwing it away. Expect "
          "one more Cost Anomaly email covering the import window; it "
          "ends when flows_done reaches %d." % FLOWS_TOTAL)
    R.log("  reader justhodl-signal-registry-ingest stays quarantined, "
          "so the Object Created events cost nothing; replication stays "
          "off, so nothing mirrors to us-west-2")
    try:
        s3.put_object(Bucket=LIVE, Key="data/ops/eurostat-backfill-progress.json",
                      Body=json.dumps(out, indent=1, default=str).encode(),
                      ContentType="application/json")
        R.log("  -> data/ops/eurostat-backfill-progress.json")
    except Exception as e:
        R.log("  write err %s" % str(e)[:90])

    if fails:
        R.log("ops 5032 RED: " + "; ".join(fails))
        sys.exit(1)
    R.kv(flows=f1, pct=round(100.0 * f1 / FLOWS_TOTAL, 2), pages=p1,
         flows_per_min=round(fpm, 2), eta_hours=out.get("eta_hours"))
    R.log("ops 5032 GREEN -- lane enabled and converging on measured "
          "throughput")
