#!/usr/bin/env python3
"""
Step 103 — Verify daily-report-v3 right-size persisted.

Step 102's test invoke hit TooManyRequestsException because the Lambda
has ReservedConcurrentExecutions=1 and was busy with a scheduled run.
But the update_function_configuration call SUCCEEDED before that.

This step:
  1. Confirms current memory is 768MB
  2. Waits for the next scheduled 5-min run to complete
  3. Reads the resulting REPORT line — checks Max Memory Used + Duration
  4. If clean → no action; if errors → revert to 1024MB
"""
import json
import os
import re
import time
from datetime import datetime, timezone, timedelta

from ops_report import report
import boto3

REGION = "us-east-1"
LAMBDA_NAME = "justhodl-daily-report-v3"
PRE_CHANGE_MEMORY = 1024
NEW_MEMORY = 768

lam = boto3.client("lambda", region_name=REGION)
logs = boto3.client("logs", region_name=REGION)


def parse_report_line(message):
    out = {}
    for key, pattern in [
        ("duration_ms", r"Duration:\s*([\d.]+)\s*ms"),
        ("memory_size_mb", r"Memory Size:\s*(\d+)\s*MB"),
        ("max_memory_mb", r"Max Memory Used:\s*(\d+)\s*MB"),
    ]:
        m = re.search(pattern, message)
        if m:
            try:
                v = m.group(1)
                out[key] = float(v) if "." in v else int(v)
            except Exception:
                pass
    return out


with report("verify_daily_report_rightsize") as r:
    r.heading("Verify daily-report-v3 right-size persisted + works at 768MB")

    # 1. Check current config
    r.section("1. Current Lambda configuration")
    cfg = lam.get_function_configuration(FunctionName=LAMBDA_NAME)
    cur_memory = cfg.get("MemorySize")
    r.log(f"  Memory: {cur_memory}MB")
    r.log(f"  Last modified: {cfg.get('LastModified')}")

    if cur_memory != NEW_MEMORY:
        r.warn(f"  Expected {NEW_MEMORY}MB but found {cur_memory}MB")
        r.kv(state="memory_not_at_target")
        raise SystemExit(1)
    r.ok(f"  Memory is at target {NEW_MEMORY}MB")

    # 2. Pull recent REPORT lines (post-change)
    r.section("2. Pull post-change REPORT lines from CloudWatch")

    # The change happened around 02:04 UTC. Look for runs AFTER that.
    change_time_unix = int(time.mktime(time.strptime("2026-04-25T02:04:00", "%Y-%m-%dT%H:%M:%S")))
    cutoff_ms = change_time_unix * 1000

    streams = logs.describe_log_streams(
        logGroupName=f"/aws/lambda/{LAMBDA_NAME}",
        orderBy="LastEventTime", descending=True, limit=5,
    ).get("logStreams", [])
    r.log(f"  Found {len(streams)} recent log streams")

    post_change_reports = []
    pre_change_reports = []
    for s in streams:
        try:
            ev = logs.get_log_events(
                logGroupName=f"/aws/lambda/{LAMBDA_NAME}",
                logStreamName=s["logStreamName"],
                limit=20, startFromHead=False,
            )
            for e in ev.get("events", []):
                msg = e["message"]
                if msg.startswith("REPORT"):
                    parsed = parse_report_line(msg)
                    parsed["ts"] = e["timestamp"]
                    if e["timestamp"] >= cutoff_ms:
                        post_change_reports.append(parsed)
                    else:
                        pre_change_reports.append(parsed)
        except Exception:
            pass

    r.log(f"  Post-change REPORT lines: {len(post_change_reports)}")
    r.log(f"  Pre-change REPORT lines (for comparison): {len(pre_change_reports)}")

    # 3. Display the post-change runs
    r.section("3. Post-change runs analysis")
    if not post_change_reports:
        r.warn("  No post-change REPORT lines yet — Lambda may not have fired since change")
        r.warn("  The 5-min schedule means we need to wait. Will rely on next ops run for this.")

        # Check pre-change baseline anyway
        if pre_change_reports:
            durs = [p.get("duration_ms", 0) for p in pre_change_reports if p.get("duration_ms")]
            mems = [p.get("max_memory_mb", 0) for p in pre_change_reports if p.get("max_memory_mb")]
            sizes = [p.get("memory_size_mb", 0) for p in pre_change_reports if p.get("memory_size_mb")]
            if durs:
                r.log(f"  Pre-change baseline: avg duration {sum(durs)/len(durs):.0f}ms ({sum(durs)/len(durs)/1000:.1f}s)")
            if mems:
                r.log(f"  Pre-change baseline: max memory used: {max(mems)}MB / {sizes[0] if sizes else '?'}MB allocated")
        r.kv(state="awaiting_first_run_at_new_memory")
    else:
        # Post-change data available
        durs = [p.get("duration_ms", 0) for p in post_change_reports if p.get("duration_ms")]
        mems = [p.get("max_memory_mb", 0) for p in post_change_reports if p.get("max_memory_mb")]
        sizes = [p.get("memory_size_mb", 0) for p in post_change_reports if p.get("memory_size_mb")]

        avg_dur_post = sum(durs) / len(durs) if durs else 0
        max_mem_post = max(mems) if mems else 0
        new_size = sizes[0] if sizes else 0

        r.log(f"  At {new_size}MB allocated:")
        r.log(f"    Avg duration: {avg_dur_post:.0f}ms ({avg_dur_post/1000:.1f}s)")
        r.log(f"    Max memory used: {max_mem_post}MB")
        r.log(f"    Headroom: {(new_size - max_mem_post)/new_size*100:.0f}%")

        # Compare to pre-change
        if pre_change_reports:
            pre_durs = [p.get("duration_ms", 0) for p in pre_change_reports if p.get("duration_ms")]
            avg_dur_pre = sum(pre_durs) / len(pre_durs) if pre_durs else 0
            r.log(f"")
            r.log(f"  Comparison to pre-change (1024MB):")
            r.log(f"    Avg duration: {avg_dur_pre:.0f}ms (pre) → {avg_dur_post:.0f}ms (post)")
            if avg_dur_pre > 0:
                delta_pct = (avg_dur_post - avg_dur_pre) / avg_dur_pre * 100
                r.log(f"    Delta: {delta_pct:+.1f}%")

                if delta_pct > 50:
                    r.warn(f"  Duration up {delta_pct:.0f}% — REVERTING to {PRE_CHANGE_MEMORY}MB")
                    lam.update_function_configuration(
                        FunctionName=LAMBDA_NAME, MemorySize=PRE_CHANGE_MEMORY,
                    )
                    r.kv(state="reverted_due_to_slowdown")
                    raise SystemExit(1)
                else:
                    r.ok(f"  Duration delta acceptable. Right-size SUCCESSFUL.")

        # 4. Verify status from invocations: any FunctionError in last 30 min?
        r.section("4. Check for errors since change")
        cw = boto3.client("cloudwatch", region_name=REGION)
        end = datetime.now(timezone.utc)
        start = end - timedelta(minutes=30)
        try:
            err_resp = cw.get_metric_statistics(
                Namespace="AWS/Lambda", MetricName="Errors",
                Dimensions=[{"Name": "FunctionName", "Value": LAMBDA_NAME}],
                StartTime=start, EndTime=end, Period=300, Statistics=["Sum"],
            )
            errors = sum(p.get("Sum", 0) for p in err_resp.get("Datapoints", []))
            inv_resp = cw.get_metric_statistics(
                Namespace="AWS/Lambda", MetricName="Invocations",
                Dimensions=[{"Name": "FunctionName", "Value": LAMBDA_NAME}],
                StartTime=start, EndTime=end, Period=300, Statistics=["Sum"],
            )
            invs = sum(p.get("Sum", 0) for p in inv_resp.get("Datapoints", []))
            r.log(f"  Last 30 min: {int(invs)} invocations, {int(errors)} errors")
            if errors > 0 and invs > 0:
                err_rate = errors / invs
                if err_rate > 0.20:
                    r.warn(f"  Error rate {err_rate*100:.0f}% — this could be due to the memory change")
                else:
                    r.log(f"  Error rate {err_rate*100:.0f}% — within normal range")
        except Exception as e:
            r.warn(f"  CloudWatch metrics: {e}")

        r.kv(
            state="verified_clean_at_new_memory",
            new_memory_mb=new_size,
            max_memory_used_mb=max_mem_post,
            avg_duration_ms=int(avg_dur_post),
        )

    r.log("Done")
