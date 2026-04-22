#!/usr/bin/env python3
"""
Why is data.json 62 days stale?

Investigates justhodl-daily-report-v3:
  1. Is it still on a schedule? (EventBridge Rules + Scheduler)
  2. When was it last invoked? (CloudWatch metrics, 90d lookback)
  3. What were the recent errors? (CloudWatch logs last 7 days)
  4. If it did run recently — what happened? Did it write to a different S3 key?
  5. Manually invoke it RIGHT NOW and capture the output

Read + one test invocation. Does NOT redeploy code.
"""

import json
import os
import sys
from datetime import datetime, timedelta, timezone

from ops_report import report
import boto3
from botocore.exceptions import ClientError

REGION = "us-east-1"
FN_NAME = "justhodl-daily-report-v3"
BUCKET = "justhodl-dashboard-live"

lam = boto3.client("lambda", region_name=REGION)
cw  = boto3.client("cloudwatch", region_name=REGION)
ev  = boto3.client("events", region_name=REGION)
sch = boto3.client("scheduler", region_name=REGION)
logs = boto3.client("logs", region_name=REGION)
s3   = boto3.client("s3", region_name=REGION)


with report("daily_report_diagnosis") as r:
    r.heading(f"Why is data.json stale? Diagnosing {FN_NAME}")

    # 1. Function config
    r.section("Function configuration")
    try:
        cfg = lam.get_function_configuration(FunctionName=FN_NAME)
        r.log(f"  Runtime: {cfg['Runtime']}")
        r.log(f"  Handler: {cfg['Handler']}")
        r.log(f"  Memory:  {cfg['MemorySize']} MB · Timeout: {cfg['Timeout']}s")
        r.log(f"  LastModified: {cfg['LastModified']}")
        arn = cfg["FunctionArn"]
    except Exception as e:
        r.fail(f"Can't fetch config: {e}")
        sys.exit(1)

    # 2. Invocation metrics 90d
    r.section("Invocation history")
    end = datetime.now(timezone.utc)
    start = end - timedelta(days=90)
    inv = cw.get_metric_statistics(
        Namespace="AWS/Lambda",
        MetricName="Invocations",
        Dimensions=[{"Name": "FunctionName", "Value": FN_NAME}],
        StartTime=start, EndTime=end,
        Period=86400, Statistics=["Sum"],
    )
    err = cw.get_metric_statistics(
        Namespace="AWS/Lambda",
        MetricName="Errors",
        Dimensions=[{"Name": "FunctionName", "Value": FN_NAME}],
        StartTime=start, EndTime=end,
        Period=86400, Statistics=["Sum"],
    )
    inv_by_day = {dp["Timestamp"].strftime("%Y-%m-%d"): int(dp["Sum"]) for dp in inv.get("Datapoints", [])}
    err_by_day = {dp["Timestamp"].strftime("%Y-%m-%d"): int(dp["Sum"]) for dp in err.get("Datapoints", [])}
    total_inv = sum(inv_by_day.values())
    total_err = sum(err_by_day.values())
    r.log(f"  90d totals: {total_inv} invocations, {total_err} errors")
    if inv_by_day:
        recent_days = sorted(inv_by_day.keys(), reverse=True)[:10]
        r.log(f"  Last 10 active days:")
        for d in recent_days:
            r.log(f"    {d}: {inv_by_day[d]} invocations, {err_by_day.get(d, 0)} errors")
    else:
        r.warn("  NO INVOCATIONS in the last 90 days — function is dormant")

    # 3. EventBridge Rules (classic)
    r.section("EventBridge Rules targeting this function")
    try:
        rule_names = ev.list_rule_names_by_target(TargetArn=arn).get("RuleNames", [])
    except ClientError as e:
        r.warn(f"  Can't list rules: {e}")
        rule_names = []

    if rule_names:
        for rn in rule_names:
            try:
                rule = ev.describe_rule(Name=rn)
                r.log(f"  - `{rn}` State={rule.get('State')} Schedule={rule.get('ScheduleExpression', '—')}")
                r.kv(source="EB Rule", name=rn, state=rule.get("State"), schedule=rule.get("ScheduleExpression", ""))
            except Exception as e:
                r.warn(f"  - `{rn}` — couldn't describe: {e}")
    else:
        r.warn("  No EventBridge Rules target this function")

    # 4. EventBridge Scheduler (newer service)
    r.section("EventBridge Scheduler schedules targeting this function")
    scheduler_hits = []
    try:
        for page in sch.get_paginator("list_schedules").paginate():
            for s in page.get("Schedules", []):
                tgt = s.get("Target", {}).get("Arn", "")
                if tgt.startswith(arn):
                    scheduler_hits.append(s)
    except Exception as e:
        r.warn(f"  Scheduler list failed: {e}")

    if scheduler_hits:
        for s in scheduler_hits:
            r.log(f"  - `{s['Name']}` State={s.get('State')} Expression={s.get('ScheduleExpression')}")
            r.kv(source="Scheduler", name=s["Name"], state=s.get("State"), schedule=s.get("ScheduleExpression", ""))
    else:
        r.log("  No Scheduler schedules target this function")

    # 5. Recent errors from CloudWatch Logs (last 7 days)
    r.section("Recent error lines from CloudWatch Logs (last 7 days)")
    log_group = f"/aws/lambda/{FN_NAME}"
    error_lines = []
    try:
        paginator = logs.get_paginator("filter_log_events")
        for page in paginator.paginate(
            logGroupName=log_group,
            startTime=int((end - timedelta(days=7)).timestamp() * 1000),
            filterPattern='?"ERROR" ?"Exception" ?"Traceback" ?"failed" ?"timeout"',
            limit=200,
        ):
            for evt in page.get("events", []):
                error_lines.append((
                    datetime.fromtimestamp(evt["timestamp"] / 1000, timezone.utc).strftime("%Y-%m-%d %H:%M"),
                    evt["message"].strip()[:300],
                ))
    except logs.exceptions.ResourceNotFoundException:
        r.warn(f"  Log group {log_group} doesn't exist — function has never produced logs")
    except Exception as e:
        r.warn(f"  Log filter failed: {e}")

    if error_lines:
        # Show the 15 most recent
        for ts, msg in error_lines[-15:]:
            r.log(f"  [{ts}] {msg[:200]}")
    else:
        r.log("  (no errors found in last 7 days — but also likely no invocations)")

    # 6. Check what keys the function writes to S3 — look in the code
    r.section("What S3 keys does the function reference in its code?")
    try:
        import urllib.request, zipfile, io, re
        code_url = lam.get_function(FunctionName=FN_NAME)["Code"]["Location"]
        with urllib.request.urlopen(code_url) as resp:
            zbytes = resp.read()
        keys = set()
        with zipfile.ZipFile(io.BytesIO(zbytes)) as zf:
            for entry in zf.namelist():
                if entry.endswith(".py"):
                    src = zf.read(entry).decode("utf-8", errors="ignore")
                    # Find put_object calls with Key=...
                    for m in re.finditer(r"Key\s*=\s*['\"]([^'\"]+)['\"]", src):
                        keys.add(m.group(1))
                    for m in re.finditer(r"['\"]([a-z0-9_\-/]+\.json)['\"]", src, re.I):
                        keys.add(m.group(1))
        r.log(f"  JSON keys / S3 objects referenced in code:")
        for k in sorted(keys):
            r.log(f"    - {k}")
    except Exception as e:
        r.warn(f"  Code scan failed: {e}")

    # 7. When was data.json actually last modified?
    r.section("data.json in S3")
    try:
        head = s3.head_object(Bucket=BUCKET, Key="data.json")
        age_days = (datetime.now(timezone.utc) - head["LastModified"]).days
        r.log(f"  data.json LastModified: {head['LastModified'].isoformat()}")
        r.log(f"  Age: {age_days} days")
        r.log(f"  Size: {head['ContentLength']} bytes")
        r.kv(object="data.json", last_modified=head["LastModified"].isoformat(), age_days=age_days, size=head["ContentLength"])
    except Exception as e:
        r.fail(f"  Can't HEAD data.json: {e}")

    # 8. Manual invocation RIGHT NOW, capture result
    r.section(f"Manually invoking {FN_NAME} (RequestResponse, timeout 300s)")
    try:
        resp = lam.invoke(
            FunctionName=FN_NAME,
            InvocationType="RequestResponse",
            LogType="Tail",
            Payload=b"{}",
        )
        status = resp["StatusCode"]
        fn_error = resp.get("FunctionError")
        payload = resp["Payload"].read().decode("utf-8", errors="ignore")

        # Tail of log (last 4KB of execution)
        import base64
        log_tail = base64.b64decode(resp.get("LogResult", "")).decode("utf-8", errors="ignore") if resp.get("LogResult") else ""

        r.log(f"  StatusCode: {status}")
        if fn_error:
            r.fail(f"  FunctionError: {fn_error}")
        else:
            r.ok(f"  No FunctionError — function completed normally")

        r.log(f"  Response payload (first 500 chars):")
        r.log(f"    {payload[:500]}")

        if log_tail:
            r.log(f"  Last part of execution log (tail):")
            for line in log_tail.splitlines()[-25:]:
                r.log(f"    {line[:200]}")

        # After invocation — check if data.json got updated
        try:
            head2 = s3.head_object(Bucket=BUCKET, Key="data.json")
            new_age_sec = (datetime.now(timezone.utc) - head2["LastModified"]).total_seconds()
            if new_age_sec < 120:
                r.ok(f"  data.json WAS updated just now (age {int(new_age_sec)}s) — function works, just wasn't scheduled")
            else:
                r.warn(f"  data.json is still {(datetime.now(timezone.utc) - head2['LastModified']).days} days old — function didn't write it")
        except Exception as e:
            r.warn(f"  HEAD after invoke failed: {e}")

    except Exception as e:
        r.fail(f"  Invoke failed: {e}")

    r.log("Diagnosis complete")
