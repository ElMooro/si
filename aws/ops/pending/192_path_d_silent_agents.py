#!/usr/bin/env python3
"""
Step 192 — Path D: deep audit of silent agents.

We have 48 justhodl-* Lambdas + 18 raw-data-API Lambdas. Many run
on schedule producing data nobody surfaces on the website.

For each justhodl-* Lambda, determine:
  A. Does it write to S3? Where?
  B. Is the output fresh or zombie?
  C. Does any page already read that output?
  D. What's the schedule?

Method:
  - Pull CloudWatch logs for each (last 50 events) and grep for S3
    PUT operations / s3.put_object calls / output keys
  - Cross-reference with S3 inventory + page-source-URL inventory
    from step 190.

Output: a 4-column matrix of [Lambda, S3 output, Freshness, Surfaced?]
that lets us pick the next features to surface.
"""
import json
import re
import time
from datetime import datetime, timezone, timedelta
from collections import defaultdict
from ops_report import report
import boto3
from botocore.exceptions import ClientError

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"

lam = boto3.client("lambda", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)
logs = boto3.client("logs", region_name=REGION)
events = boto3.client("events", region_name=REGION)


# Lambdas already known to be "surfaced" (have a page)
SURFACED_LAMBDAS = {
    "justhodl-stock-screener",      # /screener/
    "justhodl-stock-analyzer",      # /stock/ (on-demand)
    "justhodl-stock-ai-research",   # /stock/ AI tab (on-demand)
    "justhodl-ai-chat",             # ai chat (on-demand via api.justhodl.ai)
    "justhodl-edge-engine",         # /edge.html
    "justhodl-options-flow",        # /flow.html
    "justhodl-daily-report-v3",     # / and /intelligence.html (KhalidIndex source)
    "justhodl-intelligence",        # /intelligence.html
    "justhodl-morning-intelligence",# (briefs go to Telegram)
    "justhodl-pnl-tracker",         # / desk pnl card
    "justhodl-risk-sizer",          # /risk.html
    "justhodl-asymmetric-scorer",   # / today's setups
    "justhodl-divergence-scanner",  # / divergences card
    "justhodl-bond-regime-detector",# / regime cell
    "justhodl-cot-extremes-scanner",# / cot card
    "justhodl-valuations-agent",    # /valuations.html
    "justhodl-crypto-intel",        # / and /crypto/
    "justhodl-dex-scanner",         # /dex.html
    "justhodl-telegram-bot",        # (Telegram alerts)
    "justhodl-signal-logger",       # (Loop 1, internal DDB)
    "justhodl-outcome-checker",     # (Loop 1, internal DDB)
    "justhodl-calibrator",          # (Loop 1, internal SSM)
    "justhodl-khalid-metrics",      # /khalid/
    "justhodl-liquidity-agent",     # /liquidity.html
    "justhodl-ml-predictions",      # /ml-predictions.html
    "justhodl-reports-builder",     # /reports.html
    "justhodl-prompt-iterator",     # (Loop 3, weekly internal)
    "justhodl-watchlist-debate",    # (Loop 4 — output not yet surfaced!)
    "justhodl-investor-agents",     # (on-demand — NO PAGE!)
    "justhodl-fred-proxy",          # /fred.html
    "justhodl-ecb-proxy",           # /ecb.html
    "justhodl-treasury-proxy",      # /treasury-auctions.html
}


def find_recent_s3_writes(name, hours=72):
    """Grep last 1000 log events for s3.put_object output keys."""
    keys = set()
    log_group = f"/aws/lambda/{name}"
    try:
        streams = logs.describe_log_streams(
            logGroupName=log_group,
            orderBy="LastEventTime",
            descending=True,
            limit=3,
        ).get("logStreams", [])
    except logs.exceptions.ResourceNotFoundException:
        return None  # no logs at all
    if not streams:
        return None

    cutoff = time.time() * 1000 - hours * 3600 * 1000
    for stream in streams:
        if stream.get("lastEventTimestamp", 0) < cutoff:
            continue
        try:
            evts = logs.get_log_events(
                logGroupName=log_group,
                logStreamName=stream["logStreamName"],
                startFromHead=False,
                limit=200,
            ).get("events", [])
        except Exception:
            continue
        for ev in evts:
            msg = ev.get("message", "")
            # match s3 keys
            for m in re.findall(r'(?:Key=|Key:|put_object.*Key.*[\'"])([a-zA-Z0-9_/.\-]+\.json)', msg):
                keys.add(m)
            for m in re.findall(r's3://justhodl-dashboard-live/([a-zA-Z0-9_/.\-]+\.json)', msg):
                keys.add(m)
            for m in re.findall(r'put_object.*key[\'"]?\s*[=:]\s*[\'"]([a-zA-Z0-9_/.\-]+)[\'"]', msg):
                keys.add(m)
            for m in re.findall(r'wrote\s+([a-zA-Z0-9_/.\-]+\.json)', msg, re.I):
                keys.add(m)
            for m in re.findall(r'uploaded\s+to\s+s3://[^/]+/([a-zA-Z0-9_/.\-]+)', msg, re.I):
                keys.add(m)
    return keys


with report("path_d_silent_agents") as r:
    r.heading("Path D — what does each silent Lambda produce?")

    # ─── A. List all justhodl-* Lambdas ────────────────────────────────
    r.section("A. justhodl-* Lambda inventory")
    all_fns = []
    paginator = lam.get_paginator("list_functions")
    for page in paginator.paginate():
        all_fns.extend(page.get("Functions", []))
    justhodl = sorted([f for f in all_fns if f["FunctionName"].startswith("justhodl")],
                      key=lambda f: f["FunctionName"])
    r.log(f"  {len(justhodl)} justhodl-* Lambdas total")

    # ─── B. EventBridge schedule lookup ─────────────────────────────────
    r.section("B. Schedule lookup")
    schedules_by_lambda = defaultdict(list)
    next_token = None
    while True:
        kwargs = {"Limit": 100}
        if next_token: kwargs["NextToken"] = next_token
        rules_resp = events.list_rules(**kwargs)
        for rule in rules_resp.get("Rules", []):
            if not rule.get("ScheduleExpression"): continue
            if rule.get("State") != "ENABLED": continue
            try:
                tgts = events.list_targets_by_rule(Rule=rule["Name"])
                for t in tgts.get("Targets", []):
                    arn = t.get("Arn", "")
                    if ":lambda:" in arn:
                        ln = arn.split(":")[-1]
                        schedules_by_lambda[ln].append(rule["ScheduleExpression"])
            except Exception:
                continue
        next_token = rules_resp.get("NextToken")
        if not next_token: break

    # ─── C. S3 freshness map ────────────────────────────────────────────
    r.section("C. S3 inventory")
    s3_keys = {}
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=BUCKET):
        for o in page.get("Contents", []):
            s3_keys[o["Key"]] = {
                "size": o["Size"],
                "mod": o["LastModified"],
                "age_h": (datetime.now(timezone.utc) - o["LastModified"]).total_seconds() / 3600,
            }
    r.log(f"  {len(s3_keys)} S3 objects total")

    # ─── D. Per-Lambda: find S3 writes ─────────────────────────────────
    r.section("D. Per-Lambda audit")
    matrix = []  # (lambda, schedule, s3_keys, fresh, stale, surfaced)
    for f in justhodl:
        name = f["FunctionName"]
        sched = schedules_by_lambda.get(name, [])
        sched_str = "; ".join(sched) if sched else "ON-DEMAND"

        # Find S3 writes
        s3_writes = find_recent_s3_writes(name)
        if s3_writes is None:
            log_status = "NO LOGS"
            keys_in_s3 = []
        else:
            log_status = f"{len(s3_writes)} keys logged"
            keys_in_s3 = []
            for k in sorted(s3_writes):
                if k in s3_keys:
                    age_h = s3_keys[k]["age_h"]
                    keys_in_s3.append((k, age_h))

        surfaced = "✓" if name in SURFACED_LAMBDAS else "✗"
        matrix.append((name, sched_str, keys_in_s3, log_status, surfaced))

        if not keys_in_s3 and not sched:
            continue  # boring on-demand without S3 traces — skip detail
        r.log(f"\n  ── {name}  [{surfaced}]")
        r.log(f"     schedule: {sched_str}")
        r.log(f"     log status: {log_status}")
        for k, age_h in keys_in_s3[:5]:
            mark = "🟢" if age_h < 24 else "🟡" if age_h < 168 else "🔴"
            r.log(f"     {mark} {k:50} {age_h:>6.1f}h ago")

    # ─── E. Coverage gap report ─────────────────────────────────────────
    r.section("E. Coverage gaps — Lambdas producing data NOT on website")
    gaps = []
    for name, sched_str, keys_in_s3, log_status, surfaced in matrix:
        if surfaced == "✓": continue
        if "ON-DEMAND" in sched_str and not keys_in_s3:
            continue  # truly on-demand, no data
        # Has schedule or has S3 output but not surfaced
        if keys_in_s3:
            gaps.append((name, sched_str, keys_in_s3))

    r.log(f"\n  {len(gaps)} Lambdas producing untouched data:")
    for name, sched, keys in sorted(gaps):
        r.log(f"\n    🔍 {name}  ({sched})")
        for k, age_h in keys[:5]:
            mark = "🟢" if age_h < 24 else "🟡" if age_h < 168 else "🔴"
            r.log(f"        {mark} {k:50} {age_h:>6.1f}h ago")

    # ─── F. Summary ────────────────────────────────────────────────────
    r.section("F. Summary")
    n_total = len(matrix)
    n_surfaced = sum(1 for m in matrix if m[4] == "✓")
    n_with_data = sum(1 for m in matrix if m[2])
    r.log(f"\n  Total justhodl-* Lambdas:        {n_total}")
    r.log(f"  Surfaced on website:             {n_surfaced}/{n_total}")
    r.log(f"  Producing S3 data (audited):     {n_with_data}")
    r.log(f"  Coverage gaps (have data, no UI): {len(gaps)}")

    r.log("Done")
