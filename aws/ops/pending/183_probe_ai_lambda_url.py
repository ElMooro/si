#!/usr/bin/env python3
"""
Step 183 — Probe AI Lambda Function URL via public HTTPS.

Step 180 used boto3.invoke() which bypasses the Function URL entirely.
The user's browser DOES go through the Function URL (HTTPS endpoint).
We need to test the public path.

Approach: write a tiny inline script, invoke it as a one-shot Lambda
via the existing investor-agents Lambda's Layer/runtime by sending it
a special ?probe=URL payload. Actually simpler: use boto3 invoke to
hit a small probe utility we add to investor-agents' code path? That's
too invasive.

Cleanest: deploy a tiny temporary probe Lambda that just does
urllib.request to the URL and returns status+body. Build the zip
inline. Invoke. Read result.

Even simpler: use the screener Lambda's existing fmp() machinery
indirectly — modify its handler temporarily? No, too risky.

Simplest of all: add a probe endpoint to the SAME AI Lambda. Via
boto3 invoke we send {"probe_url": "..."} and the handler hits the URL
internally. But that doesn't test the public path either...

OK actually simplest: write a small inline urllib script and invoke
ANY existing Lambda with raw event that triggers it via temporary
code. NO — too invasive.

Right answer: create a tiny new ProbeLambda just for this check.
Build zip inline (urllib only, std lib). Create function. Invoke.
Read result. Tear down.

This step does that. The probe Lambda hits the Function URL via
HTTPS like the browser would, returns status + first 800 chars of
body + headers.
"""
import io
import json
import time
import zipfile
from ops_report import report
import boto3
from botocore.exceptions import ClientError

REGION = "us-east-1"
PROBE_NAME = "justhodl-tmp-url-probe"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
TARGET_URL = "https://obcsgkzlvicwc6htdmj5wg6yae0tfmya.lambda-url.us-east-1.on.aws/?ticker=AAPL"

lam = boto3.client("lambda", region_name=REGION)


PROBE_CODE = '''
import json
import urllib.request
import urllib.error

def lambda_handler(event, context):
    url = event.get("url", "")
    method = event.get("method", "GET")
    headers = event.get("headers", {})
    try:
        req = urllib.request.Request(url, method=method, headers=headers)
        with urllib.request.urlopen(req, timeout=60) as r:
            body = r.read().decode("utf-8", errors="replace")
            return {
                "ok": True,
                "status": r.status,
                "headers": dict(r.headers),
                "body_len": len(body),
                "body_preview": body[:1500],
            }
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", errors="replace") if hasattr(e, "read") else ""
        return {
            "ok": False,
            "kind": "HTTPError",
            "status": e.code,
            "headers": dict(e.headers) if e.headers else {},
            "body_preview": body[:1500],
        }
    except Exception as e:
        return {"ok": False, "kind": type(e).__name__, "error": str(e)}
'''


def build_probe_zip():
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("lambda_function.py", PROBE_CODE)
    buf.seek(0)
    return buf.read()


with report("probe_ai_lambda_url") as r:
    r.heading("Probe AI Lambda Function URL via public HTTPS")

    # ─── A. Create probe Lambda ─────────────────────────────────────────
    r.section("A. Create temporary probe Lambda")
    zip_bytes = build_probe_zip()
    try:
        lam.delete_function(FunctionName=PROBE_NAME)
        r.log(f"  Cleaned up old probe")
    except ClientError as e:
        if "ResourceNotFoundException" not in str(e):
            r.warn(f"  delete attempt: {e}")
    try:
        lam.create_function(
            FunctionName=PROBE_NAME,
            Runtime="python3.11",
            Role=ROLE_ARN,
            Handler="lambda_function.lambda_handler",
            Code={"ZipFile": zip_bytes},
            Timeout=70,
            MemorySize=256,
            Architectures=["x86_64"],
        )
        r.ok(f"  Created {PROBE_NAME}")
        time.sleep(3)  # let it warm up
    except Exception as e:
        r.fail(f"  create failed: {e}")
        raise SystemExit(1)

    # ─── B. Probe with browser-like headers ────────────────────────────
    r.section("B. Probe with browser-like Origin: justhodl.ai")
    try:
        resp = lam.invoke(
            FunctionName=PROBE_NAME,
            InvocationType="RequestResponse",
            Payload=json.dumps({
                "url": TARGET_URL,
                "method": "GET",
                "headers": {
                    "Origin": "https://justhodl.ai",
                    "Referer": "https://justhodl.ai/stock/",
                    "User-Agent": "Mozilla/5.0 (Test Probe)",
                },
            }),
        )
        result = json.loads(resp["Payload"].read())
        r.log(f"  Probe result: ok={result.get('ok')}")
        if result.get("ok"):
            r.log(f"  Status: {result.get('status')}")
            r.log(f"  Body length: {result.get('body_len')}")
            r.log(f"")
            r.log(f"  Response headers:")
            for k, v in (result.get("headers") or {}).items():
                r.log(f"    {k}: {v}")
            r.log(f"")
            r.log(f"  Body preview:")
            r.log(f"    {result.get('body_preview','')[:800]}")
        else:
            r.warn(f"  Probe failed: kind={result.get('kind')} status={result.get('status')}")
            r.log(f"  Error: {result.get('error','')}")
            if result.get("headers"):
                r.log(f"  Headers received:")
                for k, v in result["headers"].items():
                    r.log(f"    {k}: {v}")
            if result.get("body_preview"):
                r.log(f"  Body: {result['body_preview'][:800]}")
    except Exception as e:
        r.fail(f"  invoke failed: {e}")

    # ─── C. Probe OPTIONS preflight ────────────────────────────────────
    r.section("C. Probe OPTIONS preflight (what browser sends first)")
    try:
        resp = lam.invoke(
            FunctionName=PROBE_NAME,
            InvocationType="RequestResponse",
            Payload=json.dumps({
                "url": TARGET_URL,
                "method": "OPTIONS",
                "headers": {
                    "Origin": "https://justhodl.ai",
                    "Access-Control-Request-Method": "GET",
                    "Access-Control-Request-Headers": "content-type",
                },
            }),
        )
        result = json.loads(resp["Payload"].read())
        r.log(f"  OPTIONS preflight: ok={result.get('ok')} status={result.get('status')}")
        for k, v in (result.get("headers") or {}).items():
            r.log(f"    {k}: {v}")
    except Exception as e:
        r.warn(f"  OPTIONS probe failed: {e}")

    # ─── D. Cleanup ─────────────────────────────────────────────────────
    r.section("D. Cleanup probe Lambda")
    try:
        lam.delete_function(FunctionName=PROBE_NAME)
        r.ok(f"  Deleted {PROBE_NAME}")
    except Exception as e:
        r.warn(f"  delete failed: {e}")

    r.log("Done")
