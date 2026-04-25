#!/usr/bin/env python3
"""
Step 102 — Right-size justhodl-daily-report-v3 carefully.

This Lambda is the biggest cost driver (~$27/mo, 1.6M GB-seconds/30d).
Current config: 1024MB memory, 8762 invocations/30d, ~3 min avg runtime.

Strategy:
  1. Pull recent CloudWatch logs to see Max Memory Used per invocation.
     Lambda's REPORT line shows this at the end of every run.
  2. Pull duration metrics to see typical/p99 runtime.
  3. Decision tree:
     - If max_memory consistently < 60% of allocated → memory is overkill,
       reducing to 768MB or 512MB is safe (proportional cost reduction)
     - If max_memory consistently > 80% → DON'T reduce, you'll cause OOMs
     - If duration scales with memory (CPU bound) → may NOT save much
       because Lambda also scales CPU with memory
     - If duration is constant regardless of memory (I/O bound) → reducing
       memory is pure win
  4. After change: sync invoke once, compare new duration + max_memory
     against prior baseline. If duration increases >50%, revert.

This is conservative — small step (1024 → 768MB, 25% reduction), monitor,
then decide whether to go further next session.
"""
import json
import os
import time
from datetime import datetime, timezone, timedelta

from ops_report import report
import boto3

REGION = "us-east-1"

lam = boto3.client("lambda", region_name=REGION)
cw = boto3.client("cloudwatch", region_name=REGION)
logs = boto3.client("logs", region_name=REGION)

LAMBDA_NAME = "justhodl-daily-report-v3"
NEW_MEMORY = 768  # 25% reduction from 1024


def parse_report_line(message):
    """Parse a Lambda REPORT line for Duration, MaxMemoryUsed, Memory."""
    out = {}
    # REPORT RequestId: ... Duration: 12345.67 ms Billed Duration: 12346 ms
    # Memory Size: 1024 MB Max Memory Used: 432 MB Init Duration: 123 ms
    import re
    for key, pattern in [
        ("duration_ms", r"Duration:\s*([\d.]+)\s*ms"),
        ("billed_duration_ms", r"Billed Duration:\s*(\d+)\s*ms"),
        ("memory_size_mb", r"Memory Size:\s*(\d+)\s*MB"),
        ("max_memory_mb", r"Max Memory Used:\s*(\d+)\s*MB"),
        ("init_duration_ms", r"Init Duration:\s*([\d.]+)\s*ms"),
    ]:
        m = re.search(pattern, message)
        if m:
            try:
                out[key] = float(m.group(1)) if "." in m.group(1) else int(m.group(1))
            except Exception:
                pass
    return out


def gather_recent_reports(name, n_streams=10, events_per_stream=20):
    """Gather REPORT metrics from the last N log streams."""
    reports = []
    try:
        streams = logs.describe_log_streams(
            logGroupName=f"/aws/lambda/{name}",
            orderBy="LastEventTime", descending=True, limit=n_streams,
        ).get("logStreams", [])
    except Exception as e:
        return [], f"log stream fetch: {e}"

    for s in streams:
        try:
            ev = logs.get_log_events(
                logGroupName=f"/aws/lambda/{name}",
                logStreamName=s["logStreamName"],
                limit=events_per_stream, startFromHead=False,
            )
            for e in ev.get("events", []):
                msg = e["message"]
                if msg.startswith("REPORT"):
                    parsed = parse_report_line(msg)
                    if parsed.get("max_memory_mb"):
                        reports.append(parsed)
        except Exception:
            pass

    return reports, None


with report("rightsize_daily_report_v3") as r:
    r.heading("Step 102 — Right-size justhodl-daily-report-v3 carefully")

    # ─── 1. Capture current config ─────────────────────────────────────
    r.section("1. Current configuration")
    cfg = lam.get_function_configuration(FunctionName=LAMBDA_NAME)
    cur_memory = cfg.get("MemorySize")
    cur_timeout = cfg.get("Timeout")
    r.log(f"  Memory: {cur_memory}MB")
    r.log(f"  Timeout: {cur_timeout}s")
    r.log(f"  Last modified: {cfg.get('LastModified')}")

    if cur_memory == NEW_MEMORY:
        r.log(f"  Already at target {NEW_MEMORY}MB; nothing to do")
        raise SystemExit(0)
    if cur_memory < NEW_MEMORY:
        r.warn(f"  Already lower ({cur_memory}MB < target {NEW_MEMORY}MB); skipping")
        raise SystemExit(0)

    # ─── 2. Analyze recent invocation memory usage ─────────────────────
    r.section("2. Analyze recent Max Memory Used (REPORT lines)")
    reports, err = gather_recent_reports(LAMBDA_NAME, n_streams=8, events_per_stream=15)
    if err:
        r.warn(f"  {err}")
    r.log(f"  Captured {len(reports)} REPORT lines")

    if not reports:
        r.fail("  No reports — cannot make safe right-sizing decision")
        raise SystemExit(1)

    max_mems = [r_.get("max_memory_mb", 0) for r_ in reports]
    durations = [r_.get("duration_ms", 0) for r_ in reports if r_.get("duration_ms")]

    max_mem_observed = max(max_mems)
    avg_max_mem = sum(max_mems) / len(max_mems)
    p95_max_mem = sorted(max_mems)[int(len(max_mems) * 0.95)] if len(max_mems) > 1 else max_mems[0]
    avg_duration = sum(durations) / len(durations) if durations else 0
    max_duration = max(durations) if durations else 0

    r.log(f"  Max memory observed:      {max_mem_observed}MB")
    r.log(f"  P95 max memory:           {p95_max_mem}MB")
    r.log(f"  Avg max memory:           {avg_max_mem:.0f}MB")
    r.log(f"  Avg duration:             {avg_duration:.0f}ms ({avg_duration/1000:.1f}s)")
    r.log(f"  Max duration:             {max_duration:.0f}ms ({max_duration/1000:.1f}s)")
    r.log(f"  Current allocation:       {cur_memory}MB")
    headroom_pct = (cur_memory - max_mem_observed) / cur_memory * 100
    r.log(f"  Current headroom:         {headroom_pct:.0f}%")

    # ─── 3. Safety check: is downsizing safe? ─────────────────────────
    r.section("3. Safety check")
    safety_buffer_pct = 25  # Want at least 25% headroom after downsize
    new_headroom = (NEW_MEMORY - max_mem_observed) / NEW_MEMORY * 100
    r.log(f"  Headroom at new {NEW_MEMORY}MB: {new_headroom:.0f}%")

    if max_mem_observed > NEW_MEMORY * 0.85:
        r.fail(f"  ABORT: max observed ({max_mem_observed}MB) too close to new target ({NEW_MEMORY}MB)")
        r.log(f"  Risk of OOM. Skipping right-size.")
        r.kv(decision="skipped", reason="max_memory_too_close_to_target")
        raise SystemExit(0)

    if new_headroom < safety_buffer_pct:
        r.fail(f"  ABORT: only {new_headroom:.0f}% headroom at {NEW_MEMORY}MB; want ≥{safety_buffer_pct}%")
        r.kv(decision="skipped", reason="insufficient_headroom")
        raise SystemExit(0)

    r.ok(f"  Safety check PASSED — proceeding with {cur_memory}MB → {NEW_MEMORY}MB")

    # ─── 4. Apply the change ───────────────────────────────────────────
    r.section("4. Apply memory change")
    try:
        lam.update_function_configuration(
            FunctionName=LAMBDA_NAME,
            MemorySize=NEW_MEMORY,
        )
        lam.get_waiter("function_updated").wait(
            FunctionName=LAMBDA_NAME, WaiterConfig={"Delay": 3, "MaxAttempts": 30}
        )
        r.ok(f"  Memory: {cur_memory}MB → {NEW_MEMORY}MB")
    except Exception as e:
        r.fail(f"  update_function_configuration: {e}")
        raise SystemExit(1)

    # ─── 5. Test invoke ────────────────────────────────────────────────
    r.section("5. Sync test invoke at new memory")
    time.sleep(5)
    invoke_start = time.time()
    try:
        resp = lam.invoke(FunctionName=LAMBDA_NAME, InvocationType="RequestResponse")
        elapsed = time.time() - invoke_start
        if resp.get("FunctionError"):
            payload = resp.get("Payload").read().decode()[:500]
            r.fail(f"  Invoke FAILED at {NEW_MEMORY}MB: {payload}")
            r.log(f"  REVERTING memory to {cur_memory}MB")
            lam.update_function_configuration(FunctionName=LAMBDA_NAME, MemorySize=cur_memory)
            lam.get_waiter("function_updated").wait(
                FunctionName=LAMBDA_NAME, WaiterConfig={"Delay": 3, "MaxAttempts": 30}
            )
            r.ok(f"  Reverted to {cur_memory}MB")
            r.kv(decision="reverted", reason="invoke_error_at_new_memory")
            raise SystemExit(1)
        else:
            r.ok(f"  Invoke clean at {NEW_MEMORY}MB ({elapsed:.1f}s wall time)")
    except Exception as e:
        r.fail(f"  Invoke exception: {e}")
        raise SystemExit(1)

    # ─── 6. Check duration delta ───────────────────────────────────────
    r.section("6. Check post-change duration vs baseline")
    time.sleep(3)
    post_reports, _ = gather_recent_reports(LAMBDA_NAME, n_streams=2, events_per_stream=5)
    if post_reports:
        new_durations = [r_.get("duration_ms", 0) for r_ in post_reports if r_.get("duration_ms")]
        new_max_mems = [r_.get("max_memory_mb", 0) for r_ in post_reports if r_.get("max_memory_mb")]
        if new_durations:
            new_avg = sum(new_durations) / len(new_durations)
            duration_delta_pct = (new_avg - avg_duration) / avg_duration * 100 if avg_duration else 0
            r.log(f"  Pre-change avg duration:  {avg_duration:.0f}ms")
            r.log(f"  Post-change avg duration: {new_avg:.0f}ms")
            r.log(f"  Delta: {duration_delta_pct:+.1f}%")
            if duration_delta_pct > 50:
                r.warn(f"  Duration increased >50% — REVERTING")
                lam.update_function_configuration(FunctionName=LAMBDA_NAME, MemorySize=cur_memory)
                r.kv(decision="reverted", reason="duration_increased_too_much")
                raise SystemExit(1)
        if new_max_mems:
            new_max = max(new_max_mems)
            r.log(f"  New max memory used: {new_max}MB / {NEW_MEMORY}MB allocated")
            new_pct = new_max / NEW_MEMORY * 100
            r.log(f"  Memory utilization: {new_pct:.0f}%")

    # ─── 7. Estimated savings ──────────────────────────────────────────
    r.section("7. Estimated cost savings")
    pct_reduction = (cur_memory - NEW_MEMORY) / cur_memory
    # Old cost: ~$27/mo for daily-report-v3
    estimated_savings = 27 * pct_reduction
    r.log(f"  Memory reduced by {pct_reduction*100:.0f}%")
    r.log(f"  Lambda cost is linear with memory")
    r.log(f"  Estimated savings: ~${estimated_savings:.2f}/mo (was ~$27/mo)")
    r.log(f"  New estimated cost: ~${27 - estimated_savings:.2f}/mo")

    r.kv(
        decision="applied",
        old_memory_mb=cur_memory,
        new_memory_mb=NEW_MEMORY,
        max_observed_mb=max_mem_observed,
        estimated_monthly_savings=f"${estimated_savings:.2f}",
    )
    r.log("Done")
