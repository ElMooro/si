#!/usr/bin/env python3
"""Step 239 — diagnose why correlation-breaks aligns 0 dates.

Symptoms (step 238):
  - Lambda invokes successfully in 1.3s (no FRED 429 retries fired)
  - Output: warming_up, n_dates: 0
  - That means align_returns() found 0 common dates across 10 instruments
  - Either one instrument returns no observations, or compute_returns
    drops all returns due to a series being all-None

Approach: tail CloudWatch logs of the latest invoke to see the
'series_lengths: {...}' diagnostic print, which shows per-instrument
observation count.
"""
import json
import time
from ops_report import report
import boto3

REGION = "us-east-1"
logs = boto3.client("logs", region_name=REGION)
lam = boto3.client("lambda", region_name=REGION)


with report("diagnose_corr_align") as r:
    r.heading("Diagnose correlation-breaks alignment failure")

    # Force a fresh invoke to get latest log output
    r.section("1. Force fresh invoke")
    t0 = time.time()
    resp = lam.invoke(FunctionName="justhodl-correlation-breaks", InvocationType="RequestResponse")
    payload = json.loads(resp["Payload"].read())
    dur = round(time.time() - t0, 1)
    r.log(f"  invoke {dur}s  payload: {json.dumps(payload)[:300]}")
    time.sleep(2)  # give CloudWatch a moment to capture

    # Tail logs
    r.section("2. CloudWatch tail")
    log_group = "/aws/lambda/justhodl-correlation-breaks"
    streams = logs.describe_log_streams(
        logGroupName=log_group, orderBy="LastEventTime", descending=True, limit=2
    )["logStreams"]
    for stream in streams[:1]:
        r.log(f"  stream: {stream['logStreamName']}")
        events = logs.get_log_events(
            logGroupName=log_group,
            logStreamName=stream["logStreamName"],
            limit=200,
            startFromHead=False,
        )["events"]
        # Print all relevant events from latest invoke
        # The invoke we just did should be at the end
        printed = 0
        for e in events[-150:]:
            msg = e["message"].rstrip()
            if any(k in msg for k in ("[fred]", "[correlation-breaks]", "INIT_START", "Error", "error")):
                r.log(f"    {msg[:400]}")
                printed += 1
            if printed > 60:
                break

    r.log("")
    r.log("Done")
