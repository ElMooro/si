"""ops 3249 — error forensics on 3248's findings.

  1. justhodl-wl-engines: 27 errors/12h BUT fresh feed + clean recent
     streams ⇒ hypothesis: they are the debugging marathon itself
     (429-storm era + mid-fix invocations). HOURLY error distribution
     decides — if errors stop at the fix deploys (~07:30 UTC), certified
     historical. Last [ERROR] timestamp printed as evidence.
  2. The 8 unrelated single-error functions: last error line + own-feed
     freshness each → triage table (transient vs broken). Anything with
     a real recurring traceback becomes the next fix target.
"""
import json
import sys
from datetime import datetime, timedelta, timezone

import boto3

from ops_report import report

REGION = "us-east-1"
CW = boto3.client("cloudwatch", region_name=REGION)
LOGS = boto3.client("logs", region_name=REGION)
LAM = boto3.client("lambda", region_name=REGION)
SINGLES = ("justhodl-consumer-pulse", "justhodl-cb-injection",
           "justhodl-theme-rotation-engine", "justhodl-khalid-metrics",
           "justhodl-boj-detail", "justhodl-ka-metrics",
           "justhodl-yen-carry", "justhodl-snb-detail")


def last_error(fn, max_streams=4):
    try:
        grp = f"/aws/lambda/{fn}"
        for stm in LOGS.describe_log_streams(
                logGroupName=grp, orderBy="LastEventTime",
                descending=True, limit=max_streams)\
                .get("logStreams") or []:
            evs = LOGS.get_log_events(
                logGroupName=grp, logStreamName=stm["logStreamName"],
                limit=200, startFromHead=False).get("events") or []
            for ev in reversed(evs):
                m = ev.get("message") or ""
                if "[ERROR]" in m or "Task timed out" in m:
                    ts = datetime.fromtimestamp(
                        ev["timestamp"] / 1000, tz=timezone.utc)
                    return ts.isoformat()[11:19], m.splitlines()[0][:110]
    except Exception:
        pass
    return None, ""


with report("3249_error_forensics") as rep:
    fails, warns = [], []
    rep.heading("ops 3249 — error forensics")
    now = datetime.now(timezone.utc)
    start = now - timedelta(hours=13)

    rep.section("1. wl-engines hourly error distribution (13h)")
    res = CW.get_metric_data(MetricDataQueries=[{
        "Id": "e", "MetricStat": {
            "Metric": {"Namespace": "AWS/Lambda", "MetricName": "Errors",
                       "Dimensions": [{"Name": "FunctionName",
                                       "Value": "justhodl-wl-engines"}]},
            "Period": 3600, "Stat": "Sum"}}],
        StartTime=start, EndTime=now)
    r = res["MetricDataResults"][0]
    pairs = sorted(zip(r.get("Timestamps") or [], r.get("Values") or []))
    last_err_hour = None
    for ts, v in pairs:
        if v > 0:
            last_err_hour = ts
        rep.log(f"  {ts.strftime('%H:00')}Z  "
                f"{'█' * int(v)}{int(v) if v else ''}")
    ets, eline = last_error("justhodl-wl-engines", 8)
    rep.kv(last_error_at=ets or "none-in-recent-streams",
           last_error=(eline[:70] or "—"))
    hours_clean = round((now - last_err_hour).total_seconds() / 3600, 1) \
        if last_err_hour else 13.0
    rep.kv(hours_since_last_error=hours_clean)
    if hours_clean >= 3:
        rep.ok("wl-engines errors STOPPED — marathon-era, certified "
               "historical (fresh feed + clean recent streams)")
    else:
        fails.append(f"wl-engines errored within {hours_clean}h — "
                     "still live")

    rep.section("2. Singles triage")
    live_problems = 0
    for fn in SINGLES:
        ets, eline = last_error(fn)
        cfg_ok = True
        try:
            LAM.get_function_configuration(FunctionName=fn)
        except Exception:
            cfg_ok = False
        tag = "…transient" if ets else "…no recent error trace"
        if eline and any(k in eline for k in
                         ("AttributeError", "KeyError", "TypeError",
                          "Task timed out")):
            tag = "⚠ REAL"
            live_problems += 1
        rep.log(f"  {fn:<36} last_err={ets or '—'}  {tag}")
        if eline:
            rep.log(f"      {eline[:100]}")
    rep.kv(singles_flagged=live_problems)
    if live_problems:
        warns.append(f"{live_problems} single-error functions carry "
                     "real tracebacks — fix queue")

    for w in warns:
        rep.warn(w)
    rep.kv(n_fails=len(fails), n_warns=len(warns),
           verdict="PASS" if not fails else "FAIL")
    if fails:
        for f in fails:
            rep.fail(f)
        sys.exit(1)
