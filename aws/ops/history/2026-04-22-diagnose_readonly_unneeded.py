#!/usr/bin/env python3
"""
Read-only diagnosis of justhodl-daily-report-v3.

No invocation. No side effects. Just reads config, metrics, schedules,
logs, and code — quickly — so we don't hit the 15-min workflow wall
waiting on a hung sync invoke.
"""

import base64
import io
import json
import os
import re
import sys
import urllib.request
import zipfile
from datetime import datetime, timedelta, timezone

from ops_report import report
import boto3
from botocore.exceptions import ClientError

REGION = "us-east-1"
FN_NAME = "justhodl-daily-report-v3"
BUCKET = "justhodl-dashboard-live"

lam  = boto3.client("lambda", region_name=REGION)
cw   = boto3.client("cloudwatch", region_name=REGION)
ev   = boto3.client("events", region_name=REGION)
sch  = boto3.client("scheduler", region_name=REGION)
logs = boto3.client("logs", region_name=REGION)
s3   = boto3.client("s3", region_name=REGION)


with report("daily_report_readonly") as r:
    r.heading(f"Read-only diagnosis: {FN_NAME}")

    # 1. Config
    r.section("Function configuration")
    cfg = lam.get_function_configuration(FunctionName=FN_NAME)
    arn = cfg["FunctionArn"]
    r.log(f"  Runtime: {cfg['Runtime']} · Handler: {cfg['Handler']}")
    r.log(f"  Memory: {cfg['MemorySize']} MB · Timeout: {cfg['Timeout']}s")
    r.log(f"  Code LastModified: {cfg['LastModified']}")
    r.log(f"  CodeSize: {cfg['CodeSize']} bytes")
    env = (cfg.get("Environment") or {}).get("Variables", {})
    r.log(f"  Environment keys: {sorted(env.keys())}")
    r.kv(
        prop="config",
        runtime=cfg["Runtime"],
        memory=cfg["MemorySize"],
        timeout=cfg["Timeout"],
        code_last_modified=cfg["LastModified"],
        env_keys=",".join(sorted(env.keys())),
    )

    # 2. Metrics (90d)
    r.section("Invocation metrics, last 90 days")
    end = datetime.now(timezone.utc)
    start = end - timedelta(days=90)
    def metric(name):
        resp = cw.get_metric_statistics(
            Namespace="AWS/Lambda",
            MetricName=name,
            Dimensions=[{"Name": "FunctionName", "Value": FN_NAME}],
            StartTime=start, EndTime=end,
            Period=86400, Statistics=["Sum", "Maximum"],
        )
        by_day = {dp["Timestamp"].strftime("%Y-%m-%d"): (int(dp["Sum"]), dp.get("Maximum", 0)) for dp in resp.get("Datapoints", [])}
        return by_day

    inv = metric("Invocations")
    err = metric("Errors")
    dur = metric("Duration")
    throt = metric("Throttles")

    total_inv = sum(v[0] for v in inv.values())
    total_err = sum(v[0] for v in err.values())
    total_throt = sum(v[0] for v in throt.values())
    r.log(f"  90d totals → Invocations: {total_inv} · Errors: {total_err} · Throttles: {total_throt}")

    if inv:
        active_days = sorted(inv.keys(), reverse=True)
        r.log(f"  Most recent invocation day: {active_days[0]} ({inv[active_days[0]][0]} invoked)")
        r.log(f"  Oldest invocation day (in 90d window): {active_days[-1]}")
        r.log(f"  Last 15 days with any activity:")
        for d in active_days[:15]:
            e_count = err.get(d, (0, 0))[0]
            max_dur = int(dur.get(d, (0, 0))[1])
            r.log(f"    {d}  inv={inv[d][0]:>3}  err={e_count:>2}  maxDuration={max_dur}ms")
            r.kv(day=d, invocations=inv[d][0], errors=e_count, max_duration_ms=max_dur)
    else:
        r.warn("  ZERO invocations in 90d — function is dormant")

    # 3. Classic EventBridge Rules
    r.section("EventBridge Rules targeting this function")
    rule_names = ev.list_rule_names_by_target(TargetArn=arn).get("RuleNames", [])
    if rule_names:
        for rn in rule_names:
            rule = ev.describe_rule(Name=rn)
            r.log(f"  - `{rn}` | State={rule.get('State')} | Schedule={rule.get('ScheduleExpression', '—')}")
            r.kv(source="EB Rule", name=rn, state=rule.get("State", ""), schedule=rule.get("ScheduleExpression", ""))
    else:
        r.warn("  ✗ NO EventBridge Rules target this function")

    # 4. EventBridge Scheduler
    r.section("EventBridge Scheduler schedules targeting this function")
    scheduler_hits = []
    try:
        for page in sch.get_paginator("list_schedules").paginate():
            for s in page.get("Schedules", []):
                if s.get("Target", {}).get("Arn", "").startswith(arn):
                    scheduler_hits.append(s)
    except Exception as e:
        r.warn(f"  Scheduler enum failed: {e}")

    if scheduler_hits:
        for s in scheduler_hits:
            r.log(f"  - `{s['Name']}` | State={s.get('State')} | Expression={s.get('ScheduleExpression')}")
            r.kv(source="Scheduler", name=s["Name"], state=s.get("State", ""), schedule=s.get("ScheduleExpression", ""))
    else:
        r.log("  (none found)")

    # 5. Recent error-ish log lines (last 7 days) — bounded to not hang
    r.section("Recent errors/warnings from CloudWatch Logs (last 7 days)")
    log_group = f"/aws/lambda/{FN_NAME}"
    error_events = []
    try:
        # Use one call, no paginator, hard limit
        resp = logs.filter_log_events(
            logGroupName=log_group,
            startTime=int((end - timedelta(days=7)).timestamp() * 1000),
            filterPattern='?ERROR ?Exception ?Traceback ?Timeout ?failed ?"Task timed out"',
            limit=50,
        )
        for evt in resp.get("events", []):
            ts = datetime.fromtimestamp(evt["timestamp"] / 1000, timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
            error_events.append((ts, evt["message"].strip()))
    except logs.exceptions.ResourceNotFoundException:
        r.warn(f"  Log group {log_group} does not exist")
    except Exception as e:
        r.warn(f"  filter_log_events error: {e}")

    if error_events:
        r.log(f"  Found {len(error_events)} matching events (showing most recent 10):")
        for ts, msg in error_events[:10]:
            r.log(f"    [{ts}] {msg[:250]}")
    else:
        r.log("  (no error-pattern matches in last 7 days)")

    # 6. Latest 20 log events, any pattern (useful if function is erroring silently or returning early)
    r.section("Last 20 log events (any content, last 7 days)")
    try:
        resp = logs.filter_log_events(
            logGroupName=log_group,
            startTime=int((end - timedelta(days=7)).timestamp() * 1000),
            limit=20,
        )
        events = resp.get("events", [])
        if events:
            for evt in events[-20:]:
                ts = datetime.fromtimestamp(evt["timestamp"] / 1000, timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
                r.log(f"    [{ts}] {evt['message'].strip()[:200]}")
        else:
            r.warn(f"  No log events in last 7 days (confirms function hasn't been running)")
    except Exception as e:
        r.warn(f"  Can't read last events: {e}")

    # 7. Code scan — what S3 keys + bucket does it write to?
    r.section("Code scan — what does the function write?")
    try:
        code_url = lam.get_function(FunctionName=FN_NAME)["Code"]["Location"]
        with urllib.request.urlopen(code_url, timeout=15) as resp_:
            zbytes = resp_.read()
        put_keys = set()
        buckets = set()
        json_refs = set()
        with zipfile.ZipFile(io.BytesIO(zbytes)) as zf:
            for entry in zf.namelist():
                if not entry.endswith(".py"):
                    continue
                src = zf.read(entry).decode("utf-8", errors="ignore")
                for m in re.finditer(r"put_object\s*\([^)]*Key\s*=\s*['\"]([^'\"]+)['\"]", src):
                    put_keys.add(m.group(1))
                for m in re.finditer(r"put_object\s*\([^)]*Bucket\s*=\s*['\"]([^'\"]+)['\"]", src):
                    buckets.add(m.group(1))
                for m in re.finditer(r"['\"]([a-z0-9_\-/\.]+\.json)['\"]", src, re.I):
                    json_refs.add(m.group(1))
        r.log(f"  put_object Bucket=s seen: {sorted(buckets)}")
        r.log(f"  put_object Key=s seen:    {sorted(put_keys)}")
        r.log(f"  All JSON refs in code:    {sorted(json_refs)[:20]}"
              f"{' (+' + str(len(json_refs) - 20) + ' more)' if len(json_refs) > 20 else ''}")
    except Exception as e:
        r.warn(f"  Code scan failed: {e}")

    # 8. S3: latest state of data.json
    r.section("Current data.json object")
    try:
        head = s3.head_object(Bucket=BUCKET, Key="data.json")
        age_days = (datetime.now(timezone.utc) - head["LastModified"]).days
        r.log(f"  LastModified: {head['LastModified'].isoformat()}  Age: {age_days} days  Size: {head['ContentLength']} bytes")
    except Exception as e:
        r.fail(f"  HEAD failed: {e}")

    r.log("Read-only diagnosis complete")
