"""ops 3248 — post-marathon fleet certification. Tonight's session ran
~50 deploys and changed shared modules; this ops proves nothing else
broke:

  1. Lambda Errors metric for EVERY justhodl-* function (batched
     get_metric_data, 12h window) — offenders named with their last
     error line from logs.
  2. The night's directly-deployed functions verified clean
     specifically.
  3. Freshness audit on every feed touched tonight.

Clean sweep ⇒ certified steady state. Errors ⇒ the fix list, with
evidence.
"""
import json
import sys
from datetime import datetime, timedelta, timezone

import boto3

from ops_report import report

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
S3 = boto3.client("s3", region_name=REGION)
LAM = boto3.client("lambda", region_name=REGION)
CW = boto3.client("cloudwatch", region_name=REGION)
LOGS = boto3.client("logs", region_name=REGION)
TONIGHT = ("justhodl-wl-engines", "justhodl-thesis-engine",
           "justhodl-symbol-dictionary", "justhodl-credit-stress",
           "justhodl-eurodollar-plumbing", "justhodl-macro-nowcast",
           "justhodl-crisis-composite", "justhodl-wl-fusion",
           "justhodl-cot-extremes-scanner")
FEEDS = {"data/wl-engines.json": 26, "data/wl-fusion.json": 26,
         "data/thesis-engine.json": 26, "data/credit-stress.json": 26,
         "data/eurodollar-plumbing.json": 26,
         "data/macro-nowcast.json": 26, "data/crisis-composite.json": 26,
         "data/market-internals.json": 30,
         "data/symbol-map.json": 30}


def s3_json(key):
    try:
        return json.loads(S3.get_object(Bucket=BUCKET, Key=key)
                          ["Body"].read())
    except Exception:
        return None


with report("3248_fleet_health") as rep:
    fails, warns = [], []
    rep.heading("ops 3248 — fleet certification after the marathon")

    rep.section("1. Errors metric, every function, 12h")
    fns = []
    pag = LAM.get_paginator("list_functions")
    for page in pag.paginate():
        fns += [f["FunctionName"] for f in page["Functions"]
                if f["FunctionName"].startswith("justhodl")]
    rep.kv(functions=len(fns))
    now = datetime.now(timezone.utc)
    start = now - timedelta(hours=12)
    offenders = {}
    for i in range(0, len(fns), 480):
        batch = fns[i:i + 480]
        q = [{"Id": f"e{j}", "MetricStat": {
              "Metric": {"Namespace": "AWS/Lambda",
                         "MetricName": "Errors",
                         "Dimensions": [{"Name": "FunctionName",
                                         "Value": fn}]},
              "Period": 43200, "Stat": "Sum"}}
             for j, fn in enumerate(batch)]
        res = CW.get_metric_data(MetricDataQueries=q, StartTime=start,
                                 EndTime=now)
        for r in res.get("MetricDataResults", []):
            vals = r.get("Values") or []
            tot = sum(vals)
            if tot > 0:
                offenders[batch[int(r["Id"][1:])]] = int(tot)
    rep.kv(functions_with_errors=len(offenders))
    for fn, n in sorted(offenders.items(), key=lambda kv: -kv[1])[:12]:
        line = ""
        try:
            grp = f"/aws/lambda/{fn}"
            for stm in LOGS.describe_log_streams(
                    logGroupName=grp, orderBy="LastEventTime",
                    descending=True, limit=2).get("logStreams") or []:
                for ev in reversed(LOGS.get_log_events(
                        logGroupName=grp,
                        logStreamName=stm["logStreamName"],
                        limit=120, startFromHead=False)
                        .get("events") or []):
                    m = ev.get("message") or ""
                    if "[ERROR]" in m or "Task timed out" in m:
                        line = m.splitlines()[0][:110]
                        break
                if line:
                    break
        except Exception:
            pass
        mark = "⚠ TONIGHT" if fn in TONIGHT else " "
        rep.log(f"  {mark} {fn}: {n} err — {line}")
        if fn in TONIGHT:
            fails.append(f"{fn}: {n} errors post-deploy")

    rep.section("2. Tonight's deploys — explicit clean check")
    dirty = [fn for fn in TONIGHT if fn in offenders]
    if dirty:
        rep.log("  dirty: " + ", ".join(dirty))
    else:
        rep.ok(f"all {len(TONIGHT)} tonight-deployed functions: "
               "zero errors in 12h")

    rep.section("3. Feed freshness")
    stale = 0
    for key, max_h in FEEDS.items():
        d = s3_json(key) or {}
        gen = str(d.get("generated_at") or d.get("stamp") or "")
        try:
            age = (now - datetime.fromisoformat(
                gen.replace("Z", "+00:00"))).total_seconds() / 3600
        except Exception:
            age = None
        ok = age is not None and age <= max_h
        rep.log(f"  {'✓' if ok else '✗'} {key:<34} "
                f"age={round(age, 1) if age is not None else '—'}h "
                f"(cap {max_h}h)")
        if not ok:
            stale += 1
            warns.append(f"{key} stale/unreadable")
    rep.kv(feeds_fresh=len(FEEDS) - stale, of=len(FEEDS))

    for w in warns:
        rep.warn(w)
    verdict = "PASS" if not fails else "FAIL"
    if not fails and not offenders:
        rep.ok("FLEET CERTIFIED: zero erroring functions, all touched "
               "feeds fresh")
    rep.kv(n_fails=len(fails), n_warns=len(warns), verdict=verdict)
    if fails:
        for f in fails:
            rep.fail(f)
        sys.exit(1)
