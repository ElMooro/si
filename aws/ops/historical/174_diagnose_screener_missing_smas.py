#!/usr/bin/env python3
"""
Step 174 — Pull CloudWatch logs from screener run to diagnose missing SMA data.

Step 173 ran the screener but only 147/503 stocks got sma50 (29% coverage).
Runtime was 17.3s — suspiciously fast for 2515 FMP calls with 5 workers.

Two hypotheses:
  A. FMP /stable historical-price-eod/full silently rate-limiting (returns
     empty/error for ~70% of requests after some threshold)
  B. FMP returns a shape my parser doesn't handle for some tickers

The fmp() helper logs "ERR historical-price-eod/full: <reason>" on errors.
Pull the log group, count the error patterns by reason, and we'll know.

This step:
  A. Find the most recent invocation log stream
  B. Pull all events from it
  C. Tally errors by pattern
  D. Sample error messages by category
"""
import re
import time
from collections import Counter
from ops_report import report
import boto3

REGION = "us-east-1"
LOG_GROUP = "/aws/lambda/justhodl-stock-screener"

logs = boto3.client("logs", region_name=REGION)


with report("diagnose_screener_missing_smas") as r:
    r.heading("Diagnose: why are 70% of stocks missing SMA data?")

    # ─── A. Find most recent invocation log stream ─────────────────────
    r.section("A. Find most recent log stream")
    streams = logs.describe_log_streams(
        logGroupName=LOG_GROUP,
        orderBy="LastEventTime",
        descending=True,
        limit=3,
    )
    streams_list = streams.get("logStreams", [])
    if not streams_list:
        r.fail("  No log streams found")
        raise SystemExit(1)

    for s in streams_list:
        last = s.get("lastEventTimestamp", 0)
        last_str = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(last/1000)) if last else "?"
        r.log(f"  {s['logStreamName']:60} last={last_str}")

    most_recent = streams_list[0]
    stream_name = most_recent["logStreamName"]
    r.log(f"\n  Using: {stream_name}")

    # ─── B. Pull all events from this stream ───────────────────────────
    r.section("B. Pull events")
    events = []
    next_token = None
    page = 0
    while True:
        kwargs = {
            "logGroupName": LOG_GROUP,
            "logStreamName": stream_name,
            "startFromHead": True,
            "limit": 10000,
        }
        if next_token:
            kwargs["nextToken"] = next_token
        resp = logs.get_log_events(**kwargs)
        evts = resp.get("events", [])
        if not evts:
            break
        events.extend(evts)
        new_token = resp.get("nextForwardToken")
        if new_token == next_token:
            break
        next_token = new_token
        page += 1
        if page > 50:  # safety
            break
    r.log(f"  Pulled {len(events)} log events across {page+1} pages")

    # ─── C. Tally error patterns ───────────────────────────────────────
    r.section("C. Error patterns")

    err_counter = Counter()
    err_samples = {}  # pattern -> first 3 example messages

    err_pattern = re.compile(r"ERR\s+(\S+):\s*(.*)")
    histeod_pattern = re.compile(r"ERR\s+historical-price-eod/full:\s*(.*)")

    histeod_errors = []
    other_errors = []
    all_errors = []

    for ev in events:
        msg = ev.get("message", "")
        m = err_pattern.search(msg)
        if not m:
            continue
        endpoint, reason = m.group(1), m.group(2).strip()
        all_errors.append((endpoint, reason))
        err_counter[endpoint] += 1

        if endpoint == "historical-price-eod/full":
            histeod_errors.append(reason)

        # sample
        if endpoint not in err_samples:
            err_samples[endpoint] = []
        if len(err_samples[endpoint]) < 3:
            err_samples[endpoint].append(reason)

    r.log(f"  Total ERR lines: {len(all_errors)}")
    r.log(f"")
    r.log(f"  By endpoint:")
    for endpoint, count in err_counter.most_common():
        r.log(f"    {count:>4}× {endpoint}")

    # ─── D. Histeod sample reasons ─────────────────────────────────────
    r.section("D. historical-price-eod/full error reasons (this is the key one)")
    if not histeod_errors:
        r.warn("  ⚠ No errors logged for historical-price-eod/full")
        r.warn("  → Means FMP returned 200 OK but with shape my parser couldn't handle")
        r.warn("  → Need to check what shape it actually returned")
    else:
        # Tally reasons
        reason_counter = Counter()
        for reason in histeod_errors:
            # Normalize e.g. HTTP 429 / timeout / connection reset
            if "429" in reason: reason_counter["HTTP 429 (rate limit)"] += 1
            elif "404" in reason: reason_counter["HTTP 404 (not found)"] += 1
            elif "403" in reason: reason_counter["HTTP 403 (forbidden)"] += 1
            elif "500" in reason: reason_counter["HTTP 500"] += 1
            elif "timeout" in reason.lower() or "timed out" in reason.lower(): reason_counter["timeout"] += 1
            elif "connection" in reason.lower(): reason_counter["connection error"] += 1
            else: reason_counter[reason[:60]] += 1

        for cat, n in reason_counter.most_common(15):
            r.log(f"    {n:>4}× {cat}")

        r.log(f"\n  Sample raw error messages:")
        for ex in histeod_errors[:6]:
            r.log(f"    {ex[:200]}")

    # ─── E. Look for the "FAIL <symbol>" pattern from process() ────────
    r.section("E. Per-stock FAIL events (from process() catch-all)")
    fail_pattern = re.compile(r"FAIL\s+(\S+):\s*(.*)")
    fails = []
    for ev in events:
        msg = ev.get("message", "")
        m = fail_pattern.search(msg)
        if m:
            fails.append((m.group(1), m.group(2).strip()))
    r.log(f"  Total FAIL events: {len(fails)}")
    if fails:
        for sym, reason in fails[:10]:
            r.log(f"    {sym:8} {reason[:100]}")

    # ─── F. Diagnosis ───────────────────────────────────────────────────
    r.section("F. Diagnosis")
    histeod_n = err_counter.get("historical-price-eod/full", 0)
    if histeod_n == 0:
        r.log(f"  No fmp() errors for historical-price-eod/full")
        r.log(f"  → FMP is returning 200 OK but with an unexpected shape")
        r.log(f"  → Most likely: returning {{}} or {{'historical': []}} for some tickers")
        r.log(f"  → Or: returning a list-of-records but with no 'close' field")
    else:
        pct = round(100 * histeod_n / 503, 1)
        r.log(f"  {histeod_n} historical-price-eod/full errors out of 503 stocks ({pct}%)")
        if histeod_n > 200:
            r.warn(f"  → ~70% mismatch — confirms rate-limit OR endpoint issue")

    r.kv(
        n_events=len(events),
        n_errors=len(all_errors),
        n_histeod_errors=histeod_n,
        n_fails=len(fails),
    )
    r.log("Done")
