"""ops_5037 -- correct the gap check, tighten the duty cycle, re-project.

Two things came out of ops 5036, one my error and one that matters.

MY ERROR. It counted 500,120 objects against a claimed n_pages of
476,156 and called the -23,964 difference "pages promised that do not
exist". The opposite is true: MORE pages exist than the counter claimed,
because the counter was read at 15:58 and the object count finished at
16:04 while the lane was writing ~6,000 pages/min. Six minutes of
writing is ~36,000 pages; the observed difference is 23,964. It is
measurement skew on a moving target, not corruption -- the third time in
this arc a gate has measured a running system as though it were still.
The correct predicate for a live lane brackets the count:
    n_pages(before)  <=  objects_counted  <=  n_pages(after)
That is what runs here. holes=0, failed_flows=0, write_errors=0
throughout, so nothing was ever actually wrong with the data.

WHAT MATTERS. My scale projection was wrong by ~5.5x, and it was wrong
because I extrapolated off the only 79 flows that existed -- the
alphabetical head of the catalogue, all small. Reality at 17.83%:
    1,453 / 8,147 flows  ->  240,883,000 series, 481,766 pages, 139.6 GB
    ~166k series and ~333 pages per flow, avg page 273 KB
Linear to the full universe that is roughly 1.35 BILLION series, ~2.7M
page objects and ~780 GB -- not the 241M / 126 GB I quoted. Khalid has
said budget is not the constraint, so the import continues; but he
should have the real number, and it is this one.

  P0 integrity on the bracket predicate (correct for a live lane)
  P1 duty cycle from CloudWatch -- how much wall clock the single
     serialised worker is actually executing, and where the gap goes
  P2 tighten cadence rate(2 min) -> rate(1 minute) so a finished run is
     picked up sooner; throttled ticks are harmless behind the interlock
  P3 re-project scale and ETA off measured per-flow reality
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
PFX = "data/providers/eurostat/series/"
FLOWS_TOTAL = 8147

cfg = Config(read_timeout=120, retries={"max_attempts": 4})
s3 = boto3.client("s3", region_name=REGION, config=cfg)
lam = boto3.client("lambda", region_name=REGION, config=cfg)
ev = boto3.client("events", region_name=REGION, config=cfg)
cw = boto3.client("cloudwatch", region_name=REGION, config=cfg)


def state():
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


with report("ops_5037_bracket_and_tighten") as R:
    fails = []
    out = {"op": "ops_5037"}

    R.section("P0 integrity on the BRACKET predicate")
    st_a = state()
    p_before = int(st_a.get("n_pages") or 0)
    t_a = datetime.now(timezone.utc)
    R.log("  n_pages BEFORE count: %d  (%s)" % (
        p_before, t_a.strftime("%H:%M:%S")))
    n_obj, byts = count_prefix(PFX)
    t_b = datetime.now(timezone.utc)
    st_b = state()
    p_after = int(st_b.get("n_pages") or 0)
    R.log("  objects counted     : %d  (%.2f GB) over %ds" % (
        n_obj, byts / 1e9, int((t_b - t_a).total_seconds())))
    R.log("  n_pages AFTER count : %d  (%s)" % (
        p_after, t_b.strftime("%H:%M:%S")))
    ok = (p_before - 2) <= n_obj <= (p_after + 2)
    R.log("  bracket %d <= %d <= %d : %s" % (
        p_before, n_obj, p_after,
        "CLEAN -- every claimed page exists" if ok else "*** REAL GAP ***"))
    R.log("  lane wrote %d pages during the count itself" % (
        p_after - p_before))
    if not ok:
        fails.append("P0:gap")
    out.update(objects=n_obj, bytes=byts, n_pages_before=p_before,
               n_pages_after=p_after, bracket_clean=ok)
    R.log("  holes=%d failed_flows=%d write_errors_last_run=%s" % (
        len(st_b.get("missing_pages") or []),
        len(st_b.get("failed_flows") or []),
        st_b.get("write_errors_this_run")))

    R.section("P1 duty cycle of the serialised worker")
    try:
        end = datetime.now(timezone.utc)
        start = end - timedelta(hours=1)
        res = {}
        for m, stat in (("Duration", "Sum"), ("Invocations", "Sum"),
                        ("Throttles", "Sum"), ("Errors", "Sum")):
            r = cw.get_metric_statistics(
                Namespace="AWS/Lambda", MetricName=m,
                Dimensions=[{"Name": "FunctionName", "Value": FN}],
                StartTime=start, EndTime=end, Period=3600,
                Statistics=[stat])
            res[m] = sum(p[stat] for p in r.get("Datapoints", []))
        duty = res["Duration"] / 1000.0 / 3600.0 * 100.0
        R.log("  last hour: invocations=%.0f  errors=%.0f  throttles=%.0f"
              % (res["Invocations"], res["Errors"], res["Throttles"]))
        R.log("  execution time=%.0fs of 3600s  ->  DUTY CYCLE %.1f%%"
              % (res["Duration"] / 1000.0, duty))
        R.log("  (throttles are expected and harmless -- they are ticks "
              "arriving while the single worker is busy, which is the "
              "interlock doing its job)")
        out.update(duty_pct=round(duty, 1),
                   throttles=int(res["Throttles"]))
    except Exception as e:
        R.log("  metric err %s" % str(e)[:120])

    R.section("P2 tighten the cadence")
    try:
        d0 = ev.describe_rule(Name=RULE)
        ev.put_rule(Name=RULE, ScheduleExpression="rate(1 minute)",
                    State="ENABLED")
        d = ev.describe_rule(Name=RULE)
        t = ev.list_targets_by_rule(Rule=RULE).get("Targets", [])
        R.log("  cadence %s -> %s (%s), targets=%d %s" % (
            d0.get("ScheduleExpression"), d.get("ScheduleExpression"),
            d.get("State"), len(t),
            [x.get("Arn", "").rsplit(":", 1)[-1] for x in t]))
        if not t:
            fails.append("P2:notarget")
        rc = lam.get_function_concurrency(FunctionName=FN)
        R.log("  reserved concurrency = %s (must stay 1 -- it is what "
              "makes a tighter cadence safe)" % rc.get(
                  "ReservedConcurrentExecutions"))
        if rc.get("ReservedConcurrentExecutions") != 1:
            fails.append("P2:interlock")
    except Exception as e:
        R.log("  rule err %s" % str(e)[:130])
        fails.append("P2:rule")

    R.section("P3 measured re-projection")
    f0 = len(st_b.get("flows_done") or [])
    s0 = int(st_b.get("series_count") or 0)
    per_flow_series = s0 / max(1, f0)
    per_flow_pages = p_after / max(1, f0)
    per_page_bytes = byts / max(1, n_obj)
    proj_series = per_flow_series * FLOWS_TOTAL
    proj_pages = per_flow_pages * FLOWS_TOTAL
    proj_gb = proj_pages * per_page_bytes / 1e9
    R.log("  measured: %.0f series and %.1f pages per flow, %.0f KB/page"
          % (per_flow_series, per_flow_pages, per_page_bytes / 1024))
    R.log("  NOW  : %d/%d flows (%.2f%%)  %d pages  %.0f series  %.1f GB"
          % (f0, FLOWS_TOTAL, 100.0 * f0 / FLOWS_TOTAL, p_after, s0,
             byts / 1e9))
    R.log("  FULL : ~%.2fB series  ~%.1fM pages  ~%.0f GB" % (
        proj_series / 1e9, proj_pages / 1e6, proj_gb))
    R.log("  (my earlier 241M/126GB came from extrapolating the 79 "
          "alphabetical-head flows, which are small -- this projection "
          "is off 1,400+ real flows)")
    R.log("  storage at $0.023/GB-mo: ~$%.0f/month once complete; "
          "one-time PUT at $0.005/1k: ~$%.0f" % (
              proj_gb * 0.023, proj_pages / 1000 * 0.005))
    out.update(flows=f0, series=s0, projected_series=int(proj_series),
               projected_pages=int(proj_pages),
               projected_gb=round(proj_gb))

    t0 = datetime.now(timezone.utc)
    base_p, base_s = p_after, s0
    for i in range(2):
        time.sleep(230)
        stx = state()
        p = int(stx.get("n_pages") or 0)
        s = int(stx.get("series_count") or 0)
        el = int((datetime.now(timezone.utc) - t0).total_seconds())
        R.log("  t+%4ds pages=%d (+%d) series=%d (+%d)" % (
            el, p, p - base_p, s, s - base_s))
    el = max(1, int((datetime.now(timezone.utc) - t0).total_seconds()))
    ppm = (p - base_p) * 60.0 / el
    R.log("  rate after the cadence change: %.0f pages/min" % ppm)
    if ppm > 0:
        hrs = (proj_pages - p) / ppm / 60.0
        R.log("  ETA to the FULL Eurostat series universe: ~%.1f hours"
              % hrs)
        out["eta_hours"] = round(hrs, 1)
    out["pages_per_min"] = round(ppm)
    try:
        s3.put_object(Bucket=LIVE,
                      Key="data/ops/eurostat-backfill-progress.json",
                      Body=json.dumps(out, indent=1, default=str).encode(),
                      ContentType="application/json")
        R.log("  -> data/ops/eurostat-backfill-progress.json")
    except Exception as e:
        R.log("  write err %s" % str(e)[:90])

    if fails:
        R.log("ops 5037 RED: " + "; ".join(fails))
        sys.exit(1)
    R.kv(bracket_clean=ok, objects=n_obj, gb=round(byts / 1e9, 1),
         duty_pct=out.get("duty_pct"), projected_gb=out.get("projected_gb"),
         eta_hours=out.get("eta_hours"))
    R.log("ops 5037 GREEN -- data intact, lane tightened, real scale "
          "on the record")
