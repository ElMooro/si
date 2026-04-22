#!/usr/bin/env python3
"""
Smoke test the 6 migrated Lambdas that weren't explicitly tested
after today's data.json migration + deploy.

For each Lambda:
  - Invoke with a realistic payload
  - Check for FunctionError
  - Parse response and check for stale-data signals
  - Where relevant, check related S3 outputs were refreshed

Lambdas covered:
  - justhodl-morning-intelligence  — runs tomorrow 8am ET
  - justhodl-investor-agents        — 6-agent consensus (needs ticker input)
  - justhodl-bloomberg-v8           — terminal UI data
  - justhodl-chat-api               — second chat endpoint
  - justhodl-crypto-intel           — crypto dashboard (hourly)
  - justhodl-signal-logger          — learning system (6h)
"""

import base64
import json
import os
from datetime import datetime, timezone

from ops_report import report
import boto3

REGION = "us-east-1"
lam = boto3.client("lambda", region_name=REGION)
s3  = boto3.client("s3", region_name=REGION)

# Stale-data signals that should NOT appear in a healthy response
STALE_SIGNALS = [
    "[REGIME]", "[DATA]", "[SCORE]", "[KHALID_INDEX]",
    "49/100",           # specific stale khalid_index value from Feb 18
    "2026-02-18",        # specific stale timestamp
]

# Retired model names that would cause invocation failure
RETIRED_MODELS = [
    "claude-sonnet-4-20250514",
    "claude-3-haiku-20240307",
    "claude-3-5-sonnet-20241022",
]


def invoke(fn_name, payload):
    """Sync invoke with log tail. Returns dict with everything we need."""
    resp = lam.invoke(
        FunctionName=fn_name,
        InvocationType="RequestResponse",
        LogType="Tail",
        Payload=json.dumps(payload).encode() if isinstance(payload, dict) else payload,
    )
    body = resp["Payload"].read().decode("utf-8", errors="ignore")
    log_tail = ""
    if resp.get("LogResult"):
        try:
            log_tail = base64.b64decode(resp["LogResult"]).decode("utf-8", errors="ignore")
        except Exception:
            pass
    return {
        "status_code": resp["StatusCode"],
        "function_error": resp.get("FunctionError"),
        "payload": body,
        "log_tail": log_tail,
    }


def analyze_response(body_text):
    """Return list of stale signals found, and whether it looks like a Lambda 200."""
    found_stale = [s for s in STALE_SIGNALS if s in body_text]
    found_retired = [m for m in RETIRED_MODELS if m in body_text]

    looks_ok = True
    try:
        data = json.loads(body_text)
        if isinstance(data, dict):
            if data.get("errorMessage") or data.get("error"):
                looks_ok = False
            status = data.get("statusCode")
            if status and status >= 400:
                looks_ok = False
    except Exception:
        pass

    return found_stale, found_retired, looks_ok


with report("smoke_test_migrated_lambdas") as r:
    r.heading("Smoke test 6 migrated Lambdas")

    targets = [
        # (function name, invocation payload, notes)
        (
            "justhodl-morning-intelligence",
            {},  # scheduled invocation — empty event
            "Runs tomorrow at 8am ET. Writes briefing to S3.",
        ),
        (
            "justhodl-investor-agents",
            {"httpMethod": "POST", "body": json.dumps({"ticker": "AAPL"})},
            "6-agent consensus analysis. Uses Claude Haiku 4.5.",
        ),
        (
            "justhodl-bloomberg-v8",
            {"httpMethod": "GET", "path": "/", "queryStringParameters": None},
            "Terminal UI data source.",
        ),
        (
            "justhodl-chat-api",
            {"httpMethod": "POST", "body": json.dumps({"messages": [{"role": "user", "content": "hello"}]})},
            "Second chat endpoint. Hardcoded retired model — EXPECTED FAIL.",
        ),
        (
            "justhodl-crypto-intel",
            {},
            "Crypto dashboard data. Hardcoded retired model in analysis path — may partial-fail.",
        ),
        (
            "justhodl-signal-logger",
            {},
            "Learning system — logs signals to DynamoDB.",
        ),
    ]

    any_failed = False

    for fn_name, payload, note in targets:
        r.section(f"{fn_name}")
        r.log(f"  Note: {note}")

        try:
            result = invoke(fn_name, payload)
        except Exception as e:
            r.fail(f"  ✗ invoke failed: {type(e).__name__}: {e}")
            r.kv(lambda_name=fn_name, verdict="INVOKE_ERROR", detail=str(e)[:80])
            any_failed = True
            continue

        fn_err = result["function_error"]
        body = result["payload"]
        found_stale, found_retired, looks_ok = analyze_response(body)

        # Decide verdict
        if fn_err:
            verdict = "FUNCTION_ERROR"
            r.fail(f"  ✗ FunctionError={fn_err}")
            r.log(f"  Response preview: {body[:200]}")
            any_failed = True
        elif not looks_ok:
            verdict = "STATUS_ERROR"
            r.fail(f"  ✗ Response indicates error")
            r.log(f"  Body: {body[:300]}")
            any_failed = True
        elif found_retired:
            verdict = "RETIRED_MODEL_IN_OUTPUT"
            r.warn(f"  ⚠ Response contains retired model string: {found_retired}")
            r.log(f"  Body preview: {body[:200]}")
        elif found_stale:
            verdict = "STALE_SIGNAL"
            r.warn(f"  ⚠ Stale-data signals found: {found_stale}")
            r.log(f"  Body preview: {body[:400]}")
        else:
            verdict = "OK"
            r.ok(f"  Status {result['status_code']} · {len(body)} bytes · no stale signals")
            r.log(f"  Body preview: {body[:200]}")

        # Peek at log tail if there was an error
        if fn_err and result["log_tail"]:
            r.log(f"  Log tail (last 10 lines):")
            for line in result["log_tail"].splitlines()[-10:]:
                r.log(f"    {line[:200]}")

        r.kv(
            lambda_name=fn_name,
            status_code=result["status_code"],
            fn_error=fn_err or "—",
            verdict=verdict,
            stale=",".join(found_stale) if found_stale else "—",
            retired_model=",".join(found_retired) if found_retired else "—",
        )

    r.log("")
    if any_failed:
        r.warn("Some Lambdas have issues — see per-function section above")
    else:
        r.ok("All 6 Lambdas returned without fatal errors")

    r.log("Done")
