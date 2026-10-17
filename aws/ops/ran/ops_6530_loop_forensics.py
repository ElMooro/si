"""ops 6530 - READ-ONLY forensics of the Oct 12-16 cost anomaly, for Khalid's "explain thoroughly what caused it".

Already established (ops 6519/6520, git history):
  * the loop: justhodl-miss-calibrator emits one near_miss.extreme event per signal type with >= 50 near-misses;
    justhodl-event-coordinator routes near_miss.extreme back to justhodl-miss-calibrator (audit=True, notify=True)
  * what armed it: commit 42bedb1c (2026-10-07 22:04 UTC): the coordinator's duplicate filter now keys on the
    EventBridge event id, so a repeated near_miss.extreme with identical content is no longer dropped
  * miss-calibrator runs on its own only Mondays 09:15 UTC -> first run after that deploy = Mon 2026-10-12

This op measures (no mutation, ~1-2 cents of API calls):
  1. Cost Explorer daily by service Oct 1-17, usage types Oct 12-17, forecast
  2. BEFORE: did previous Mondays (Sep 14 - Oct 5) start laps that the old filter killed?
  3. START: per-minute invocations around Oct 12 09:15 UTC
  4. TRAJECTORY: hourly invocations/concurrency Oct 12-17, async queue age/drops, account-wide throttles
  5. LOGS (Logs Insights, cents): first lap on Oct 12, a 10-minute steady-state sample on Oct 15,
     the Oct 5 Monday for comparison
  6. BRANCHING: data/miss-summary.json history - how many signal types were >= 50 each week
"""

from datetime import datetime, timezone
from pathlib import Path
import json
import sys
import time

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
from ops_report import report  # noqa: E402

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
CAL, COORD = "justhodl-miss-calibrator", "justhodl-event-coordinator"
OUT_JSON = ROOT / "aws" / "ops" / "reports" / "6530.json"
CFG = Config(retries={"max_attempts": 8, "mode": "standard"})
ce = boto3.client("ce", region_name=REGION, config=CFG)
cw = boto3.client("cloudwatch", region_name=REGION, config=CFG)
logs = boto3.client("logs", region_name=REGION, config=CFG)
s3 = boto3.client("s3", region_name=REGION, config=CFG)
out = {"op": 6530, "ran_at": datetime.now(timezone.utc).isoformat(), "errors": {}}
UTC = timezone.utc


def T(*a):
    return datetime(*a, tzinfo=UTC)


def guard(name):
    def deco(fn):
        def run(*a, **k):
            try:
                return fn(*a, **k)
            except Exception as e:  # noqa: BLE001
                out["errors"][name] = "%s: %s" % (type(e).__name__, str(e)[:300])
        return run
    return deco


def series(fn, metric, stat, start, end, period, dims=True):
    kw = {"Namespace": "AWS/Lambda", "MetricName": metric, "StartTime": start, "EndTime": end,
          "Period": period, "Statistics": [stat]}
    if dims and fn:
        kw["Dimensions"] = [{"Name": "FunctionName", "Value": fn}]
    r = cw.get_metric_statistics(**kw)
    return [(p["Timestamp"].strftime("%m-%d %H:%M"), round(p[stat], 1))
            for p in sorted(r.get("Datapoints", []), key=lambda p: p["Timestamp"])]


def total(points):
    return round(sum(v for _, v in points), 1)


def peak(points):
    return max((v for _, v in points), default=0)


# 1 ─────────────────────────────────────────────────────────────────────────────────────────────
@guard("cost")
def cost(r):
    resp = ce.get_cost_and_usage(TimePeriod={"Start": "2026-10-01", "End": "2026-10-18"}, Granularity="DAILY",
                                 Metrics=["UnblendedCost"], GroupBy=[{"Type": "DIMENSION", "Key": "SERVICE"}])
    days = {}
    for t in resp["ResultsByTime"]:
        svc = {g["Keys"][0]: round(float(g["Metrics"]["UnblendedCost"]["Amount"]), 2) for g in t.get("Groups", [])}
        days[t["TimePeriod"]["Start"]] = {"total": round(sum(svc.values()), 2), "estimated": t.get("Estimated"),
                                           "by_service": dict(sorted(svc.items(), key=lambda kv: -kv[1])[:8])}
    out["daily"] = days
    out["mtd"] = round(sum(v["total"] for v in days.values()), 2)
    pre = [v["total"] for d, v in days.items() if d <= "2026-10-11"]
    out["oct1_11_avg"] = round(sum(pre) / max(1, len(pre)), 2)
    r.log("Oct 1-11 average: $%.2f/day" % out["oct1_11_avg"])
    for d in sorted(days):
        if d >= "2026-10-09":
            top = ", ".join("%s $%.0f" % (k.replace("Amazon ", "").replace("AWS ", ""), v)
                            for k, v in list(days[d]["by_service"].items())[:4])
            r.log("%s $%8.2f%s  %s" % (d, days[d]["total"], " (est)" if days[d]["estimated"] else "", top))
    r.log("October month-to-date per Cost Explorer now: $%.2f" % out["mtd"])
    try:
        f = ce.get_cost_forecast(TimePeriod={"Start": datetime.now(UTC).date().isoformat(), "End": "2026-11-01"},
                                 Metric="UNBLENDED_COST", Granularity="MONTHLY")
        out["forecast_rest"] = round(float(f["Total"]["Amount"]), 2)
        r.log("Cost Explorer forecast for the rest of October (it does not know about the freeze): $%.2f"
              % out["forecast_rest"])
    except Exception as e:  # noqa: BLE001
        out["errors"]["forecast"] = "%s: %s" % (type(e).__name__, str(e)[:120])
    usage = ce.get_cost_and_usage(TimePeriod={"Start": "2026-10-12", "End": "2026-10-18"}, Granularity="MONTHLY",
                                  Metrics=["UnblendedCost", "UsageQuantity"],
                                  GroupBy=[{"Type": "DIMENSION", "Key": "USAGE_TYPE"}])
    ut = sorted(((g["Keys"][0], round(float(g["Metrics"]["UnblendedCost"]["Amount"]), 2),
                  round(float(g["Metrics"]["UsageQuantity"]["Amount"]), 1))
                 for t in usage["ResultsByTime"] for g in t.get("Groups", [])), key=lambda kv: -kv[1])
    out["oct12_17_usage_types"] = ut[:20]
    for name, usd, qty in ut[:12]:
        r.log("  Oct 12-17 %-40s $%9.2f  qty %s" % (name, usd, qty))


# 2 ─────────────────────────────────────────────────────────────────────────────────────────────
@guard("before")
def before(r):
    daily = {fn: series(fn, "Invocations", "Sum", T(2026, 9, 10), T(2026, 10, 12), 86400) for fn in (CAL, COORD)}
    out["before_daily"] = daily
    r.log("calibrator invocations per day Sep 10 - Oct 11 (non-zero): %s" % [p for p in daily[CAL] if p[1]])
    r.log("coordinator invocations per day Sep 10 - Oct 11: total %s, max day %s" % (total(daily[COORD]),
                                                                                 peak(daily[COORD])))
    mondays = {}
    for d in (T(2026, 9, 14), T(2026, 9, 21), T(2026, 9, 28), T(2026, 10, 5), T(2026, 10, 12)):
        pts = series(CAL, "Invocations", "Sum", d.replace(hour=9), d.replace(hour=11), 300)
        cpts = series(COORD, "Invocations", "Sum", d.replace(hour=9), d.replace(hour=11), 300)
        mondays[d.strftime("%m-%d")] = {"calibrator_0900_1100": total(pts), "calibrator_5min": pts,
                                        "coordinator_0900_1100": total(cpts)}
        r.log("Monday %s 09:00-11:00 UTC: calibrator %s runs %s | coordinator %s runs" % (
            d.strftime("%m-%d"), total(pts), [p for p in pts if p[1]][:8], total(cpts)))
    out["mondays"] = mondays


# 3 ─────────────────────────────────────────────────────────────────────────────────────────────
@guard("start")
def start_minute(r):
    s, e = T(2026, 10, 12, 9, 0), T(2026, 10, 12, 11, 0)
    for fn in (CAL, COORD):
        pts = [p for p in series(fn, "Invocations", "Sum", s, e, 60) if p[1]]
        out.setdefault("start_minutes", {})[fn] = pts[:60]
        r.log("%s invocations per minute from 09:00 UTC Oct 12 (first 30 non-empty minutes): %s" % (fn, pts[:30]))
    conc = [p for p in series(CAL, "ConcurrentExecutions", "Maximum", s, e, 60) if p[1]]
    out["start_concurrency_cal"] = conc[:60]
    r.log("calibrator concurrent executions per minute: %s" % conc[:30])


# 4 ─────────────────────────────────────────────────────────────────────────────────────────────
@guard("trajectory")
def trajectory(r):
    s, e = T(2026, 10, 12), T(2026, 10, 17, 6)
    traj = {}
    for fn in (CAL, COORD):
        traj[fn] = {"invocations_h": series(fn, "Invocations", "Sum", s, e, 3600),
                    "concurrency_h": series(fn, "ConcurrentExecutions", "Maximum", s, e, 3600),
                    "duration_avg_d": series(fn, "Duration", "Average", s, e, 86400),
                    "errors_d": series(fn, "Errors", "Sum", s, e, 86400),
                    "throttles_d": series(fn, "Throttles", "Sum", s, e, 86400),
                    "async_received_d": series(fn, "AsyncEventsReceived", "Sum", s, e, 86400),
                    "async_age_max_d": series(fn, "AsyncEventAge", "Maximum", s, e, 86400),
                    "async_dropped_d": series(fn, "AsyncEventsDropped", "Sum", s, e, 86400)}
        t = traj[fn]
        by_day = {}
        for ts, v in t["invocations_h"]:
            by_day[ts[:5]] = by_day.get(ts[:5], 0) + v
        t["invocations_d"] = {k: round(v) for k, v in by_day.items()}
        r.log("%s invocations per day: %s" % (fn, t["invocations_d"]))
        r.log("%s peak concurrency per day: %s" % (fn, {k: peak([p for p in t["concurrency_h"] if p[0][:5] == k])
                                                          for k in by_day}))
        r.log("%s avg duration ms/day %s | errors %s | throttles %s" % (fn, t["duration_avg_d"], t["errors_d"],
                                                                       t["throttles_d"]))
        r.log("%s async queue: received %s | oldest event age s %s | dropped %s" % (
            fn, t["async_received_d"], t["async_age_max_d"], t["async_dropped_d"]))
    out["trajectory"] = traj
    acct = {}
    for metric, stat in (("Throttles", "Sum"), ("ConcurrentExecutions", "Maximum"), ("Invocations", "Sum"),
                         ("UnreservedConcurrentExecutions", "Maximum")):
        acct[metric] = series(None, metric, stat, T(2026, 10, 5), T(2026, 10, 17, 6), 86400, dims=False)
    out["account_daily"] = acct
    for k, v in acct.items():
        r.log("account-wide %s per day: %s" % (k, v))


# 5 ─────────────────────────────────────────────────────────────────────────────────────────────
def insights(group, query, start, end, limit=50):
    qid = logs.start_query(logGroupName=group, startTime=int(start.timestamp()), endTime=int(end.timestamp()),
                           queryString=query, limit=limit)["queryId"]
    for _ in range(120):
        res = logs.get_query_results(queryId=qid)
        if res["status"] in ("Complete", "Failed", "Cancelled", "Timeout"):
            rows = [{f["field"]: f["value"] for f in row} for row in res.get("results", [])]
            return {"status": res["status"], "rows": rows,
                    "scanned_mb": round((res.get("statistics") or {}).get("bytesScanned", 0) / 1e6, 1)}
        time.sleep(2)
    return {"status": "pending"}


@guard("logs")
def log_evidence(r):
    for fn in (CAL, COORD):
        try:
            g = logs.describe_log_groups(logGroupNamePrefix="/aws/lambda/" + fn)["logGroups"]
            out.setdefault("log_groups", {})[fn] = [{"name": x["logGroupName"], "retention_days": x.get("retentionInDays"),
                                                     "stored_gb": round(x.get("storedBytes", 0) / 1e9, 1)} for x in g]
        except Exception as e:  # noqa: BLE001
            out["errors"]["log_groups_" + fn] = str(e)[:150]
    r.log("log groups: %s" % out.get("log_groups"))
    first_s, first_e = T(2026, 10, 12, 9, 10), T(2026, 10, 12, 9, 50)
    q_first = ("fields @timestamp, @message | filter @message like /miss-cal|REPORT|event publish/ "
               "| sort @timestamp asc | limit 60")
    out["first_lap_calibrator"] = insights("/aws/lambda/" + CAL, q_first, first_s, first_e, 60)
    for row in out["first_lap_calibrator"].get("rows", [])[:20]:
        r.log("  CAL %s %s" % (row.get("@timestamp"), (row.get("@message") or "").strip()[:160]))
    q_first_c = ("fields @timestamp, @message | filter @message like /routed|Downstream|REPORT|deduped/ "
                 "| sort @timestamp asc | limit 40")
    out["first_lap_coordinator"] = insights("/aws/lambda/" + COORD, q_first_c, first_s, first_e, 40)
    for row in out["first_lap_coordinator"].get("rows", [])[:16]:
        r.log("  COORD %s %s" % (row.get("@timestamp"), (row.get("@message") or "").strip()[:160]))
    q_rep = ("filter @type = \"REPORT\" | stats count(*) as runs, avg(@duration) as avg_ms, "
             "pct(@duration, 50) as p50_ms, pct(@duration, 95) as p95_ms, max(@maxMemoryUsed)/1000000 as max_mem_mb, "
             "max(@memorySize)/1000000 as mem_mb")
    q_kinds = ("filter @message like /ssm put fail|ThrottlingException|Rate exceeded|event publish|telegram|"
               "Downstream invocation not accepted|audit|TooManyRequests|SlowDown|routed event|proposals,/ "
               "| parse @message /(?<kind>ssm put fail|ThrottlingException|Rate exceeded|event publish|telegram|"
               "Downstream invocation not accepted|audit|TooManyRequests|SlowDown|routed event=[a-z_.]+|proposals,)/ "
               "| stats count(*) as n by kind")
    windows = {"oct15_1200": (T(2026, 10, 15, 12, 0), T(2026, 10, 15, 12, 10)),
               "oct05_monday": (T(2026, 10, 5, 9, 0), T(2026, 10, 5, 11, 0))}
    for label, (s, e) in windows.items():
        for fn in (CAL, COORD):
            rep = insights("/aws/lambda/" + fn, q_rep, s, e)
            kinds = insights("/aws/lambda/" + fn, q_kinds, s, e)
            out.setdefault("logs_" + label, {})[fn] = {"report": rep, "kinds": kinds}
            r.log("%s %s: %s | kinds %s (scanned %s+%s MB)" % (label, fn, rep.get("rows"), kinds.get("rows"),
                                                              rep.get("scanned_mb"), kinds.get("scanned_mb")))


# 6 ─────────────────────────────────────────────────────────────────────────────────────────────
def extreme_of(doc):
    by = doc.get("near_misses_by_signal") or {}
    ext = {}
    for k, v in by.items():
        try:
            if int(v) >= 50:
                ext[k] = int(v)
        except (TypeError, ValueError):
            continue
    return by, ext


@guard("branching")
def branching(r):
    key = "data/miss-summary.json"
    doc = json.loads(s3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
    by, ext = extreme_of(doc)
    head = s3.head_object(Bucket=BUCKET, Key=key)
    out["miss_summary_now"] = {"last_modified": head["LastModified"].isoformat(), "n_signal_types": len(by),
                               "extreme_ge_50": ext}
    r.log("now (written %s): %d signal types, %d with >= 50 near-misses: %s" % (
        head["LastModified"].isoformat(), len(by), len(ext), ext))
    versions, kw = [], {"Bucket": BUCKET, "Prefix": key}
    for _ in range(20):
        page = s3.list_object_versions(**kw)
        versions += [v for v in page.get("Versions", []) if v["Key"] == key]
        if not page.get("IsTruncated"):
            break
        kw.update(KeyMarker=page["NextKeyMarker"], VersionIdMarker=page["NextVersionIdMarker"])
    versions.sort(key=lambda v: v["LastModified"])
    picked, last_day = [], None
    for v in versions:  # one version per day is enough
        day = v["LastModified"].strftime("%m-%d")
        if v["LastModified"] < T(2026, 9, 1) or day == last_day:
            continue
        last_day = day
        picked.append(v)
    hist = []
    for v in picked[-50:]:
        try:
            d = json.loads(s3.get_object(Bucket=BUCKET, Key=key, VersionId=v["VersionId"])["Body"].read())
            _, e = extreme_of(d)
            hist.append((v["LastModified"].strftime("%m-%d %H:%M"), len(e), max(e.values()) if e else 0))
        except Exception as ex:  # noqa: BLE001
            hist.append((v["LastModified"].strftime("%m-%d %H:%M"), "err", str(ex)[:60]))
    out["miss_summary_history"] = {"n_versions": len(versions), "daily": hist}
    r.log("%d stored versions; per day (time, signal types >= 50, max count): %s" % (len(versions), hist))


def main():
    with report("ops_6530_loop_forensics") as r:
        r.heading("ops 6530 - forensics of the Oct 12-16 anomaly (read-only)")
        r.section("1. Cost")
        cost(r)
        r.section("2. Before the change: previous Mondays")
        before(r)
        r.section("3. Start minute, Oct 12")
        start_minute(r)
        r.section("4. Trajectory Oct 12-17 + collateral")
        trajectory(r)
        r.section("5. Logs")
        log_evidence(r)
        r.section("6. Branching factor (miss-summary history)")
        branching(r)
        if out["errors"]:
            r.section("Section errors")
            for k, v in out["errors"].items():
                r.warn("%s: %s" % (k, v))
        OUT_JSON.write_text(json.dumps(out, indent=1, default=str))
        r.ok("wrote %s" % OUT_JSON.relative_to(ROOT))
        if "cost" in out["errors"] and "start" in out["errors"]:
            r.fail("both core sections failed - no forensics produced")
            sys.exit(1)
    sys.exit(0)


if __name__ == "__main__":
    main()
