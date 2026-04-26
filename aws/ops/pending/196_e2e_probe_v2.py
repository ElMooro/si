#!/usr/bin/env python3
"""Step 196 — re-probe with browser User-Agent (CF blocked urllib)."""
import io, json, time, zipfile
from ops_report import report
import boto3
from botocore.exceptions import ClientError

REGION = "us-east-1"
PROBE_NAME = "justhodl-tmp-probe-196"
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
                "body": b.decode("utf-8", errors="replace")}
    except Exception as e:
        return {"ok": False, "kind": type(e).__name__, "error": str(e)}
'''


def build_zip():
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("lambda_function.py", PROBE_CODE)
    buf.seek(0); return buf.read()


# Real browser UA — Chrome on Mac
UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"

with report("e2e_probe_investor_v2") as r:
    r.heading("Re-probe Worker with Mozilla UA")

    try: lam.delete_function(FunctionName=PROBE_NAME)
    except ClientError: pass
    lam.create_function(
        FunctionName=PROBE_NAME, Runtime="python3.11",
        Role=ROLE_ARN, Handler="lambda_function.lambda_handler",
        Code={"ZipFile": build_zip()},
        Timeout=130, MemorySize=512, Architectures=["x86_64"],
    )
    time.sleep(3)

    r.section("A. POST api.justhodl.ai/investor with browser UA")
    resp = lam.invoke(
        FunctionName=PROBE_NAME, InvocationType="RequestResponse",
        Payload=json.dumps({
            "url": "https://api.justhodl.ai/investor",
            "method": "POST",
            "headers": {
                "Content-Type": "application/json",
                "Origin": "https://justhodl.ai",
                "User-Agent": UA,
                "Accept": "application/json",
                "Referer": "https://justhodl.ai/investor.html",
            },
            "body": json.dumps({"ticker": "NVDA"}),
        }),
    )
    result = json.loads(resp["Payload"].read())
    if result.get("ok"):
        r.log(f"  ✅ HTTP {result['status']} len={result['len']}")
        try:
            data = json.loads(result.get("preview","").rstrip())
            r.log(f"  Top-level keys: {list(data.keys())}")
            r.log(f"  ticker: {data.get('ticker')} ({data.get('name')})")
            r.log(f"  consensus.signal: {data.get('consensus',{}).get('signal')}")
            r.log(f"  agents: {len(data.get('agents',[]))}")
            for a in data.get("agents", [])[:6]:
                r.log(f"    {a.get('name'):20} {a.get('signal'):12} conv={a.get('conviction')}")
        except Exception as e:
            r.log(f"  preview: {result.get('preview','')[:600]}")
    else:
        r.warn(f"  ✗ status={result.get('status')} {result.get('error','')[:200]}")
        r.log(f"  body: {result.get('body','')[:500]}")

    r.section("B. /research GET with browser UA")
    resp = lam.invoke(
        FunctionName=PROBE_NAME, InvocationType="RequestResponse",
        Payload=json.dumps({
            "url": "https://api.justhodl.ai/research?ticker=AAPL",
            "method": "GET",
            "headers": {
                "Origin": "https://justhodl.ai",
                "User-Agent": UA,
                "Accept": "application/json",
                "Referer": "https://justhodl.ai/stock/",
            },
        }),
    )
    result = json.loads(resp["Payload"].read())
    if result.get("ok"):
        r.log(f"  ✅ /research HTTP {result['status']} len={result['len']}")
    else:
        r.warn(f"  ✗ status={result.get('status')} body={result.get('body','')[:200]}")

    r.section("C. /investor.html static page reachable from GitHub Pages")
    resp = lam.invoke(
        FunctionName=PROBE_NAME, InvocationType="RequestResponse",
        Payload=json.dumps({
            "url": "https://justhodl.ai/investor.html",
            "method": "GET",
            "headers": {"User-Agent": UA},
        }),
    )
    result = json.loads(resp["Payload"].read())
    if result.get("ok"):
        r.log(f"  ✅ /investor.html HTTP {result['status']} len={result['len']}")
        # Confirm it's the new page
        preview = result.get("preview", "")
        has_input = '"tickerInput"' in preview
        has_api = "api.justhodl.ai/investor" in preview
        r.log(f"  has tickerInput: {has_input}")
        r.log(f"  calls api.justhodl.ai/investor: {has_api}")
    else:
        r.warn(f"  ✗ status={result.get('status')} body={result.get('body','')[:200]}")

    try: lam.delete_function(FunctionName=PROBE_NAME)
    except ClientError: pass
    r.log("Done")
