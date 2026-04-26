#!/usr/bin/env python3
"""
Step 201 — End-to-end probe of every /agent/<key> route.

Tests Worker's new generic /agent/<key> proxy added in commit d309c7b.
Calls each route with browser User-Agent + Origin: https://justhodl.ai
(needed to pass the Worker's ALLOWED_ORIGINS check).

Verdict per agent:
  - 200 OK + JSON body  → page should work
  - 5xx                 → Worker config issue, fix
  - 502                 → Lambda upstream down
"""
import io, json, time, zipfile
from ops_report import report
import boto3
from botocore.exceptions import ClientError

REGION = "us-east-1"
PROBE_NAME = "justhodl-tmp-probe-201"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
lam = boto3.client("lambda", region_name=REGION)

UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"

AGENTS = ["volatility", "dollar", "bonds", "bea", "manufacturing",
          "banking", "trends", "sentiment", "secretary", "macro-brief"]

PROBE_CODE = '''
import json, urllib.request, urllib.error
def lambda_handler(event, context):
    try:
        req = urllib.request.Request(
            event["url"], method=event.get("method","GET"),
            headers=event.get("headers", {}),
            data=event["body"].encode() if event.get("body") else None,
        )
        with urllib.request.urlopen(req, timeout=30) as r:
            data = r.read()
            return {"ok": True, "status": r.status, "len": len(data),
                    "preview": data[:1500].decode("utf-8", errors="replace"),
                    "headers": dict(r.headers)}
    except urllib.error.HTTPError as e:
        b = e.read()[:300] if hasattr(e,"read") else b""
        return {"ok": False, "status": e.code,
                "body": b.decode("utf-8", errors="replace")}
    except Exception as e:
        return {"ok": False, "kind": type(e).__name__, "error": str(e)}
'''


def build_zip():
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("lambda_function.py", PROBE_CODE)
    buf.seek(0); return buf.read()


with report("e2e_probe_agents") as r:
    r.heading("E2E probe — /agent/<key> through Cloudflare Worker")

    try: lam.delete_function(FunctionName=PROBE_NAME)
    except ClientError: pass
    lam.create_function(
        FunctionName=PROBE_NAME, Runtime="python3.11",
        Role=ROLE_ARN, Handler="lambda_function.lambda_handler",
        Code={"ZipFile": build_zip()},
        Timeout=60, MemorySize=512, Architectures=["x86_64"],
    )
    time.sleep(3)

    results = {}
    for agent in AGENTS:
        r.section(f"📡 /agent/{agent}")
        url = f"https://api.justhodl.ai/agent/{agent}"
        try:
            resp = lam.invoke(
                FunctionName=PROBE_NAME, InvocationType="RequestResponse",
                Payload=json.dumps({
                    "url": url,
                    "method": "GET",
                    "headers": {
                        "Origin": "https://justhodl.ai",
                        "User-Agent": UA,
                        "Accept": "application/json",
                        "Referer": "https://justhodl.ai/",
                    },
                }),
            )
            result = json.loads(resp["Payload"].read())
            if result.get("ok"):
                r.log(f"  ✅ HTTP {result['status']} len={result['len']}")
                # Parse response
                try:
                    body = json.loads(result.get("preview","").rstrip())
                    if body.get("statusCode") and isinstance(body.get("body"), str):
                        # Wrapped Lambda response
                        try:
                            inner = json.loads(body["body"])
                            r.log(f"  inner keys: {list(inner.keys())[:8]}")
                        except: pass
                    else:
                        r.log(f"  top-level keys: {list(body.keys())[:8]}")
                    results[agent] = "OK"
                except:
                    r.log(f"  (preview not parseable JSON, len={result['len']}B)")
                    results[agent] = "OK-RAW"
            else:
                r.warn(f"  ✗ status={result.get('status')} {result.get('error','')[:200]}")
                r.log(f"  body: {result.get('body','')[:300]}")
                results[agent] = f"FAIL-{result.get('status', '?')}"
        except Exception as e:
            r.warn(f"  invoke fail: {e}")
            results[agent] = "INVOKE-FAIL"

    r.section("SUMMARY")
    for agent in AGENTS:
        v = results.get(agent, "?")
        mark = "🟢" if v.startswith("OK") else "🔴"
        r.log(f"  {mark} /agent/{agent:14} {v}")

    n_ok = sum(1 for v in results.values() if v.startswith("OK"))
    r.log(f"\n  {n_ok}/{len(AGENTS)} agents working through Worker")

    try: lam.delete_function(FunctionName=PROBE_NAME)
    except ClientError: pass
    r.log("Done")
