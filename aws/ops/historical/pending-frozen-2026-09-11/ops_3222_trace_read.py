"""ops 3222 — read the trace that 3221's 8-second race missed. The 05:59
run DID execute the tracer (its counts moved: Europe Liquidity 3→5
z-scorable); CloudWatch ingestion just lagged the read. Logs-only."""
import sys

import boto3

from ops_report import report

LOGS = boto3.client("logs", region_name="us-east-1")

with report("3222_trace_read") as rep:
    fails = []
    rep.heading("ops 3222 — the trace, actually")
    shown = 0
    grp = "/aws/lambda/justhodl-wl-engines"
    try:
        for st in LOGS.describe_log_streams(
                logGroupName=grp, orderBy="LastEventTime",
                descending=True, limit=5).get("logStreams") or []:
            for e in LOGS.get_log_events(
                    logGroupName=grp, logStreamName=st["logStreamName"],
                    limit=400, startFromHead=False).get("events") or []:
                m = (e.get("message") or "").strip()
                if "[trace]" in m or "pull FAIL" in m:
                    rep.log("  " + m[:170])
                    shown += 1
            if shown >= 12:
                break
    except Exception as e:
        fails.append(f"logs: {str(e)[:80]}")
    rep.kv(trace_lines=shown)
    if not shown:
        fails.append("still no trace lines — tracer genuinely absent")
    rep.kv(n_fails=len(fails), verdict="PASS" if not fails else "FAIL")
    if fails:
        for f in fails:
            rep.fail(f)
        sys.exit(1)
