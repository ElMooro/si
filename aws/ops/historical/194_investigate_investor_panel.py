#!/usr/bin/env python3
"""
Step 194 — Investigate Legendary Investor Panel for /investor.html.

  A. Full dump of investor-analysis/AAPL.json (file is 7KB,
     step 193 only sampled 'metrics' dict; might have personas)
  B. Probe justhodl-investor-agents Function URL to see response
     shape (similar to step 183 for AI Research)
  C. Decide: route via Cloudflare proxy or use directly?
"""
import io
import json
import time
import zipfile
from ops_report import report
import boto3
from botocore.exceptions import ClientError

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
PROBE_NAME = "justhodl-tmp-probe-194"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
INVESTOR_AGENTS_URL = "https://7qufoauxzhqwnrsmdjjwt46wy40zzdyp.lambda-url.us-east-1.on.aws/"

s3 = boto3.client("s3", region_name=REGION)
lam = boto3.client("lambda", region_name=REGION)


PROBE_CODE = '''
import json, urllib.request, urllib.error
def lambda_handler(event, context):
    url = event["url"]
    method = event.get("method", "GET")
    headers = event.get("headers", {})
    body = event.get("body")
    try:
        req = urllib.request.Request(url, method=method, headers=headers,
                                     data=body.encode() if body else None)
        with urllib.request.urlopen(req, timeout=90) as r:
            data = r.read()
            return {"ok": True, "status": r.status, "len": len(data),
                    "preview": data[:3000].decode("utf-8", errors="replace"),
                    "headers": dict(r.headers)}
    except urllib.error.HTTPError as e:
        b = e.read()[:500] if hasattr(e,"read") else b""
        return {"ok": False, "status": e.code,
                "body": b.decode("utf-8", errors="replace")}
    except Exception as e:
        return {"ok": False, "kind": type(e).__name__, "error": str(e)}
'''


def build_zip():
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("lambda_function.py", PROBE_CODE)
    buf.seek(0)
    return buf.read()


with report("investigate_investor_panel") as r:
    r.heading("Investigate Legendary Investor Panel")

    # ─── A. Full investor-analysis dump ─────────────────────────────────
    r.section("A. investor-analysis/AAPL.json — FULL contents")
    obj = s3.get_object(Bucket=BUCKET, Key="investor-analysis/AAPL.json")
    data = json.loads(obj["Body"].read())
    r.log(f"  Top-level keys ({len(data)}):")
    for k, v in data.items():
        if isinstance(v, dict):
            r.log(f"    {k}: dict({len(v)} keys)")
            for k2, v2 in list(v.items())[:8]:
                vs = json.dumps(v2)[:80] if v2 is not None else "null"
                r.log(f"      {k2}: {vs}")
        elif isinstance(v, list):
            r.log(f"    {k}: list[{len(v)}]")
            if v and isinstance(v[0], dict):
                r.log(f"      [0] keys: {list(v[0].keys())[:10]}")
        else:
            vs = json.dumps(v)[:120] if v is not None else "null"
            r.log(f"    {k}: {vs}")

    # Also show a slice of the raw JSON so I can see narrative text
    r.log(f"\n  RAW JSON preview (first 3000 chars):")
    raw = json.dumps(data, indent=2)
    for line in raw[:3000].split("\n"):
        r.log(f"    {line}")

    # ─── B. Setup probe Lambda ──────────────────────────────────────────
    r.section("B. Setup probe Lambda")
    try: lam.delete_function(FunctionName=PROBE_NAME)
    except ClientError: pass
    lam.create_function(
        FunctionName=PROBE_NAME, Runtime="python3.11",
        Role=ROLE_ARN, Handler="lambda_function.lambda_handler",
        Code={"ZipFile": build_zip()},
        Timeout=120, MemorySize=512, Architectures=["x86_64"],
    )
    time.sleep(3)
    r.ok("  probe ready")

    # ─── C. Probe investor-agents Function URL (GET first) ─────────────
    r.section("C. Probe investor-agents Function URL — GET")
    try:
        resp = lam.invoke(
            FunctionName=PROBE_NAME, InvocationType="RequestResponse",
            Payload=json.dumps({
                "url": INVESTOR_AGENTS_URL + "?ticker=AAPL",
                "method": "GET",
                "headers": {"Origin": "https://justhodl.ai"},
            }),
        )
        result = json.loads(resp["Payload"].read())
        if result.get("ok"):
            r.log(f"  ✅ HTTP {result['status']} len={result['len']}")
            r.log(f"  Headers:")
            for k, v in (result.get("headers") or {}).items():
                r.log(f"    {k}: {v}")
            r.log(f"\n  Preview (first 1500 chars):")
            for line in result.get("preview","")[:1500].split("\n"):
                r.log(f"    {line}")
        else:
            r.warn(f"  ✗ status={result.get('status')} {result.get('error','')[:200]}")
            r.log(f"  body: {result.get('body','')[:300]}")
    except Exception as e:
        r.warn(f"  invoke fail: {e}")

    # If GET fails, try POST with body
    r.section("D. Probe investor-agents — POST with ticker body")
    try:
        resp = lam.invoke(
            FunctionName=PROBE_NAME, InvocationType="RequestResponse",
            Payload=json.dumps({
                "url": INVESTOR_AGENTS_URL,
                "method": "POST",
                "headers": {"Content-Type": "application/json", "Origin": "https://justhodl.ai"},
                "body": json.dumps({"ticker": "AAPL"}),
            }),
        )
        result = json.loads(resp["Payload"].read())
        if result.get("ok"):
            r.log(f"  ✅ HTTP {result['status']} len={result['len']}")
            r.log(f"\n  Preview:")
            for line in result.get("preview","")[:1500].split("\n"):
                r.log(f"    {line}")
        else:
            r.warn(f"  ✗ status={result.get('status')} {result.get('error','')[:200]}")
            r.log(f"  body: {result.get('body','')[:300]}")
    except Exception as e:
        r.warn(f"  invoke fail: {e}")

    # ─── Cleanup ────────────────────────────────────────────────────────
    try: lam.delete_function(FunctionName=PROBE_NAME)
    except ClientError: pass
    r.log("Done")
