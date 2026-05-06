#!/usr/bin/env python3
"""
Step 195 — End-to-end probe of /investor page's runtime path.

Page calls: POST https://api.justhodl.ai/investor
  → Cloudflare Worker forwards to Lambda
  → Lambda returns 6 personas
  → Worker passes back to browser

Probe this exact path (with Origin: https://justhodl.ai header
to satisfy ALLOWED_ORIGINS check) using the same Lambda probe
pattern as step 194.
"""
import io, json, time, zipfile
from ops_report import report
import boto3
from botocore.exceptions import ClientError

REGION = "us-east-1"
PROBE_NAME = "justhodl-tmp-probe-195"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"

lam = boto3.client("lambda", region_name=REGION)

PROBE_CODE = '''
import json, urllib.request, urllib.error
def lambda_handler(event, context):
    try:
        req = urllib.request.Request(
            event["url"], method=event.get("method","GET"),
            headers=event.get("headers", {}),
            data=event["body"].encode() if event.get("body") else None,
        )
        with urllib.request.urlopen(req, timeout=120) as r:
            data = r.read()
            return {"ok": True, "status": r.status, "len": len(data),
                    "preview": data[:2500].decode("utf-8", errors="replace"),
                    "headers": dict(r.headers)}
    except urllib.error.HTTPError as e:
        b = e.read()[:500] if hasattr(e,"read") else b""
        return {"ok": False, "status": e.code,
                "body": b.decode("utf-8", errors="replace"),
                "headers": dict(e.headers) if hasattr(e,"headers") else {}}
    except Exception as e:
        return {"ok": False, "kind": type(e).__name__, "error": str(e)}
'''


def build_zip():
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("lambda_function.py", PROBE_CODE)
    buf.seek(0); return buf.read()


with report("e2e_probe_investor") as r:
    r.heading("End-to-end probe — Worker → Lambda → response")

    # Setup
    try: lam.delete_function(FunctionName=PROBE_NAME)
    except ClientError: pass
    lam.create_function(
        FunctionName=PROBE_NAME, Runtime="python3.11",
        Role=ROLE_ARN, Handler="lambda_function.lambda_handler",
        Code={"ZipFile": build_zip()},
        Timeout=130, MemorySize=512, Architectures=["x86_64"],
    )
    time.sleep(3)

    # Probe 1: api.justhodl.ai/investor with allowed Origin
    r.section("A. POST api.justhodl.ai/investor with Origin: https://justhodl.ai")
    resp = lam.invoke(
        FunctionName=PROBE_NAME, InvocationType="RequestResponse",
        Payload=json.dumps({
            "url": "https://api.justhodl.ai/investor",
            "method": "POST",
            "headers": {
                "Content-Type": "application/json",
                "Origin": "https://justhodl.ai",
            },
            "body": json.dumps({"ticker": "NVDA"}),
        }),
    )
    result = json.loads(resp["Payload"].read())
    if result.get("ok"):
        r.log(f"  ✅ HTTP {result['status']} len={result['len']}")
        # Headers — confirm CORS came through
        r.log(f"  CORS headers:")
        for k, v in (result.get("headers") or {}).items():
            if 'access-control' in k.lower() or 'content-type' in k.lower():
                r.log(f"    {k}: {v}")
        # Parse body
        try:
            data = json.loads(result.get("preview","").rstrip())
            r.log(f"\n  Top-level keys: {list(data.keys())}")
            r.log(f"  ticker: {data.get('ticker')}")
            r.log(f"  consensus.signal: {data.get('consensus',{}).get('signal')}")
            r.log(f"  agents: {len(data.get('agents',[]))}")
            for a in data.get("agents", [])[:3]:
                r.log(f"    {a.get('name')}: {a.get('signal')} (conv {a.get('conviction')})")
        except Exception as e:
            r.log(f"  preview (truncated): {result.get('preview','')[:1000]}")
    else:
        r.warn(f"  ✗ status={result.get('status')} {result.get('error','')[:200]}")
        r.log(f"  body: {result.get('body','')[:500]}")

    # Probe 2: confirm /research still works (no regression)
    r.section("B. Regression check — /research still works")
    resp = lam.invoke(
        FunctionName=PROBE_NAME, InvocationType="RequestResponse",
        Payload=json.dumps({
            "url": "https://api.justhodl.ai/research?ticker=AAPL",
            "method": "GET",
            "headers": {"Origin": "https://justhodl.ai"},
        }),
    )
    result = json.loads(resp["Payload"].read())
    if result.get("ok"):
        r.log(f"  ✅ /research HTTP {result['status']} len={result['len']}")
    else:
        r.warn(f"  ✗ status={result.get('status')} {result.get('error','')[:200]}")
        r.log(f"  body: {result.get('body','')[:300]}")

    # Cleanup
    try: lam.delete_function(FunctionName=PROBE_NAME)
    except ClientError: pass
    r.log("Done")
