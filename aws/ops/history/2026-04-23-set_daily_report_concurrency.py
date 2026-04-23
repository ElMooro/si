#!/usr/bin/env python3
"""
Set reserved concurrency = 1 on daily-report-v3.

Root cause of v3.2 'fetch 233, cache never written' bug:

  Up to FOUR concurrent invocations of daily-report-v3 were running
  simultaneously. Each hit FRED with 233 requests. 4 × 233 = 932
  simultaneous requests slamming an API with a 120 req/min limit.
  Every run got 429-hosed. Cache never reached the 70% threshold.

Causes of the pileup:
  a) EventBridge rule fires every 5 min
  b) My ops scripts triggered async invokes for testing
  c) Some scans take 200-240s, so a scan at t=0 is still running
     when t=300 fires a fresh scan

Fix: put_function_concurrency(ReservedConcurrentExecutions=1).

This:
  - Caps daily-report at 1 concurrent invocation
  - Extra invocations get throttled (TooManyRequests — harmless, since
    EventBridge just waits for next cycle)
  - Ensures one scan completes (or times out at 900s) before the next
    one starts
  - Eliminates concurrent FRED hammering

Side effect: my ops scripts that async-invoke will be throttled if a
scheduled run is in-flight. That's fine — the scheduled every-5-min
run gets the data fresh anyway.
"""
import os
from ops_report import report
import boto3

lam = boto3.client("lambda", region_name="us-east-1")


with report("set_daily_report_concurrency") as r:
    r.heading("Set ReservedConcurrentExecutions=1 on daily-report-v3")

    fn = "justhodl-daily-report-v3"

    # Show current
    try:
        cur = lam.get_function_concurrency(FunctionName=fn)
        r.log(f"  Current reserved concurrency: {cur.get('ReservedConcurrentExecutions', 'not set')}")
    except Exception as e:
        r.log(f"  Current: not set ({e})")

    # Apply
    try:
        lam.put_function_concurrency(
            FunctionName=fn,
            ReservedConcurrentExecutions=1,
        )
        r.ok(f"  Set ReservedConcurrentExecutions=1")
    except Exception as e:
        r.fail(f"  Failed: {e}")
        raise SystemExit(1)

    # Verify
    cur = lam.get_function_concurrency(FunctionName=fn)
    r.log(f"  Verified: {cur.get('ReservedConcurrentExecutions')}")
    r.kv(reserved_concurrency=cur.get("ReservedConcurrentExecutions"))

    r.log("Done — next scheduled run (within 5 min) will have exclusive FRED access")
