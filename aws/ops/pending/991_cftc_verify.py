#!/usr/bin/env python3
"""Step 991 — Verify CFTC proxy route is live + page uses it."""
import io, json, os, time, zipfile
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/991_cftc_verify.json"
NAME = "justhodl-cftc-verify"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
lam = boto3.client("lambda", region_name="us-east-1")

DIAG = '''
import json, urllib.request, re

UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) Chrome/120 Safari/537.36"

def probe(url):
    try:
        req = urllib.request.Request(url, headers={
            "Origin": "https://justhodl.ai",
            "User-Agent": UA, "Accept": "application/json",
        })
        with urllib.request.urlopen(req, timeout=20) as r:
            body = r.read().decode("utf-8")
            try:
                d = json.loads(body)
                return {"status": r.status, "size": len(body),
                        "top_keys": list(d.keys())[:8] if isinstance(d, dict) else None}
            except Exception:
                return {"status": r.status, "size": len(body), "preview": body[:200]}
    except urllib.error.HTTPError as e:
        try: b = e.read().decode("utf-8")
        except: b = ""
        return {"http_err": e.code, "body": b[:200]}
    except Exception as e:
        return {"err": str(e)[:200]}

def lambda_handler(event, context):
    out = {}
    out["sig_proxy"]    = probe("https://api.justhodl.ai/agent/cftc-positioning/signals")
    out["cot_proxy"]    = probe("https://api.justhodl.ai/agent/cftc-positioning/cot/all")
    out["fut_proxy"]    = probe("https://api.justhodl.ai/agent/cftc-positioning/futures")
    
    # Confirm deployed positioning/index.html uses the proxy
    try:
        req = urllib.request.Request("https://justhodl.ai/positioning/",
            headers={"User-Agent": UA, "Cache-Control": "no-cache"})
        with urllib.request.urlopen(req, timeout=15) as r:
            html = r.read().decode("utf-8")
            api_match = re.search(r'API="([^"]+)"', html)
            out["page_api"] = api_match.group(1) if api_match else "<not found>"
            out["page_uses_proxy"] = "api.justhodl.ai/agent/cftc-positioning" in (api_match.group(1) if api_match else "")
    except Exception as e:
        out["page_err"] = str(e)[:200]
    
    return {"statusCode": 200, "body": json.dumps(out, default=str)}
'''


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("lambda_function.py", DIAG)
    zb = buf.getvalue()
    try:
        lam.create_function(FunctionName=NAME, Runtime="python3.12",
            Handler="lambda_function.lambda_handler", Role=ROLE_ARN,
            MemorySize=256, Timeout=120, Code={"ZipFile": zb})
        lam.get_waiter("function_active_v2").wait(FunctionName=NAME)
    except Exception:
        lam.update_function_code(FunctionName=NAME, ZipFile=zb)
        lam.get_waiter("function_updated").wait(FunctionName=NAME)
        lam.update_function_configuration(FunctionName=NAME, Timeout=120)
        lam.get_waiter("function_updated").wait(FunctionName=NAME)
    time.sleep(2)
    resp = lam.invoke(FunctionName=NAME, InvocationType="RequestResponse", Payload=b"{}")
    body = resp["Payload"].read().decode("utf-8")
    try:
        out["test"] = json.loads(json.loads(body).get("body", "{}"))
    except Exception:
        out["raw"] = body[:1500]
    try: lam.delete_function(FunctionName=NAME)
    except: pass
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f:
        json.dump(out, f, indent=2, default=str)


if __name__ == "__main__":
    main()
