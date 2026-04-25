#!/usr/bin/env python3
"""
Step 178 — Diagnose the AAPL fetch failure in stock-ai-research Lambda.

Smoke test in step 177 returned 404 'ticker AAPL not found at FMP'. My
gather_facts() returns None for `name` if FMP profile response isn't a
list. Investor-agents Lambda handles BOTH list and dict shapes via:
    return d[0] if isinstance(d, list) and d else (d if isinstance(d, dict) else default)

This step:
  A. Pull CloudWatch logs from the recent invocation — see if fmp()
     logged any errors (which would mean non-200 responses)
  B. Invoke the Lambda again WITH explicit debug output added to a
     companion endpoint — actually no, simpler: just look at logs.
  C. If logs are inconclusive, do a manual-direct boto3 invoke with
     custom event to a test_fmp_shape endpoint via temporary code update.

Pure read-only first; if logs are silent I'll add prints to gather_facts.
"""
import time
from collections import Counter
from ops_report import report
import boto3

REGION = "us-east-1"
LOG_GROUP = "/aws/lambda/justhodl-stock-ai-research"

logs = boto3.client("logs", region_name=REGION)


with report("diagnose_aapl_fetch") as r:
    r.heading("Why did stock-ai-research return 404 for AAPL?")

    # ─── A. Find recent log streams ─────────────────────────────────────
    r.section("A. Recent log streams")
    try:
        streams = logs.describe_log_streams(
            logGroupName=LOG_GROUP,
            orderBy="LastEventTime",
            descending=True,
            limit=3,
        ).get("logStreams", [])
    except logs.exceptions.ResourceNotFoundException:
        r.fail(f"  Log group {LOG_GROUP} not found yet — Lambda may not have been invoked")
        raise SystemExit(1)

    if not streams:
        r.fail("  No log streams")
        raise SystemExit(1)

    for s in streams:
        last = s.get("lastEventTimestamp", 0)
        last_str = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(last/1000)) if last else "?"
        r.log(f"  {s['logStreamName']:60} last={last_str}")

    stream_name = streams[0]["logStreamName"]
    r.log(f"\n  Using: {stream_name}")

    # ─── B. Pull events ─────────────────────────────────────────────────
    r.section("B. All log events from most recent stream")
    events = []
    next_token = None
    while True:
        kwargs = {
            "logGroupName": LOG_GROUP,
            "logStreamName": stream_name,
            "startFromHead": True,
            "limit": 10000,
        }
        if next_token: kwargs["nextToken"] = next_token
        resp = logs.get_log_events(**kwargs)
        evts = resp.get("events", [])
        if not evts: break
        events.extend(evts)
        new_token = resp.get("nextForwardToken")
        if new_token == next_token: break
        next_token = new_token

    r.log(f"  Pulled {len(events)} events")
    r.log(f"")
    r.log(f"  All non-empty messages:")
    for ev in events:
        msg = (ev.get("message") or "").strip()
        if not msg: continue
        r.log(f"    {msg[:200]}")

    # ─── C. Tally key markers ───────────────────────────────────────────
    r.section("C. Key markers")
    has_start = any("AI RESEARCH" in (e.get("message") or "") for e in events)
    has_fmp_err = any("FMP" in (e.get("message") or "") and "error" in (e.get("message") or "") for e in events)
    has_response = any("Anthropic" in (e.get("message") or "") for e in events)
    has_init_err = any("[ERROR]" in (e.get("message") or "") or "Task timed out" in (e.get("message") or "") for e in events)

    r.log(f"  Got '=== AI RESEARCH ===' marker: {has_start}")
    r.log(f"  FMP error logs:                 {has_fmp_err}")
    r.log(f"  Anthropic call logged:          {has_response}")
    r.log(f"  Lambda init/timeout error:      {has_init_err}")

    if has_start and not has_fmp_err and not has_response:
        r.warn(f"\n  → Lambda started, FMP didn't log errors, Anthropic wasn't called")
        r.warn(f"  → gather_facts() ran successfully, but profile.name was None")
        r.warn(f"  → Most likely: FMP returned a dict (not list), my parser dropped it")
        r.warn(f"  → Fix: use safe() helper from investor-agents to handle both shapes")
    elif has_fmp_err:
        r.warn(f"\n  → fmp() got non-200 responses — see error messages above")
    else:
        r.warn(f"\n  → Inconclusive; need targeted test")

    r.kv(
        n_events=len(events),
        had_start=has_start,
        had_fmp_err=has_fmp_err,
        had_anthropic=has_response,
    )
    r.log("Done")
