#!/usr/bin/env python3
"""
Zero-touch smoke test.

Runs three small checks and writes a structured report. Claude will read
the committed report via `git pull` after this workflow completes, without
needing the user to paste logs.

Checks:
  1. ai-chat auth token is readable from SSM
  2. The live ai-chat Lambda URL returns a real response (NVDA price)
  3. The justhodl-ai-proxy Worker URL also returns a real response
"""

import json
import os
import sys
import urllib.request
import urllib.error

from ops_report import report
import boto3

REGION = "us-east-1"
LAMBDA_URL = "https://zh3c6izcbzcqwcia4m6dmjnupy0dbnns.lambda-url.us-east-1.on.aws/"
WORKER_URL = "https://justhodl-ai-proxy.REDACTED.workers.dev/"
SSM_PARAM  = "/justhodl/ai-chat/auth-token"


def http_post(url, headers, body, timeout=30):
    req = urllib.request.Request(
        url, data=json.dumps(body).encode(), headers=headers, method="POST"
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, r.read().decode(errors="ignore")
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode(errors="ignore")


with report("zero_touch_smoke_test") as r:
    r.heading("Zero-Touch Smoke Test")
    r.log("Starting checks")

    # 1: SSM token
    try:
        ssm = boto3.client("ssm", region_name=REGION)
        token = ssm.get_parameter(Name=SSM_PARAM, WithDecryption=True)["Parameter"]["Value"]
        r.ok(f"SSM token readable (length: {len(token)})")
        r.kv(check="ssm-token", status="pass", detail=f"len={len(token)}")
    except Exception as e:
        r.fail(f"SSM token unreadable: {e}")
        r.kv(check="ssm-token", status="fail", detail=str(e))
        sys.exit(1)

    # 2: Direct Lambda URL with auth
    code, body = http_post(
        LAMBDA_URL,
        {"Content-Type": "application/json", "Origin": "https://justhodl.ai",
         "x-justhodl-token": token},
        {"message": "NVDA price in one short line"},
    )
    if code == 200:
        try:
            preview = json.loads(body).get("response", "")[:100]
            r.ok(f"Lambda direct: {preview}")
            r.kv(check="lambda-url-auth", status="pass", detail=preview)
        except Exception:
            r.ok(f"Lambda direct: status 200, raw body len={len(body)}")
            r.kv(check="lambda-url-auth", status="pass", detail=body[:100])
    else:
        r.fail(f"Lambda direct failed: HTTP {code} — {body[:150]}")
        r.kv(check="lambda-url-auth", status="fail", detail=f"HTTP {code}")

    # 3: Cloudflare Worker (no token needed — Worker adds it)
    code, body = http_post(
        WORKER_URL,
        {"Content-Type": "application/json", "Origin": "https://justhodl.ai"},
        {"message": "NVDA price in one short line"},
    )
    if code == 200:
        try:
            preview = json.loads(body).get("response", "")[:100]
            r.ok(f"Worker proxy: {preview}")
            r.kv(check="worker-proxy", status="pass", detail=preview)
        except Exception:
            r.ok(f"Worker proxy: status 200, raw body len={len(body)}")
            r.kv(check="worker-proxy", status="pass", detail=body[:100])
    else:
        r.fail(f"Worker proxy failed: HTTP {code} — {body[:150]}")
        r.kv(check="worker-proxy", status="fail", detail=f"HTTP {code}")

    r.log("All checks complete")
