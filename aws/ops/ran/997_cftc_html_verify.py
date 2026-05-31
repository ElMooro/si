#!/usr/bin/env python3
"""Step 997 — Verify /cftc.html serves + SW updated."""
import io, json, os, time, zipfile
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/997_cftc_html_verify.json"
NAME = "justhodl-cftc-final"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
lam = boto3.client("lambda", region_name="us-east-1")

DIAG = r'''
import json
import urllib.request

UA = "Mozilla/5.0 Chrome/120"

def probe(url):
    try:
        req = urllib.request.Request(url, headers={
            "User-Agent": UA, "Cache-Control": "no-cache"})
        with urllib.request.urlopen(req, timeout=15) as r:
            body = r.read().decode("utf-8", errors="replace")
            return {"status": r.status, "size": len(body),
                    "has_redir": "location.replace" in body and "/positioning/" in body,
                    "has_meta_refresh": "http-equiv" in body.lower() and "refresh" in body.lower()}
    except urllib.error.HTTPError as e:
        return {"http_err": e.code}
    except Exception as e:
        return {"err": str(e)[:200]}

def lambda_handler(event, context):
    out = {}
    out["cftc_html"]    = probe("https://justhodl.ai/cftc.html")
    out["cftc_dir"]     = probe("https://justhodl.ai/cftc/")
    out["positioning"]  = probe("https://justhodl.ai/positioning/")
    
    # Check SW version live
    try:
        req = urllib.request.Request("https://justhodl.ai/service-worker.js",
            headers={"User-Agent": UA, "Cache-Control": "no-cache"})
        with urllib.request.urlopen(req, timeout=15) as r:
            sw = r.read().decode("utf-8", errors="replace")
            for line in sw.splitlines():
                if "VERSION" in line and "=" in line:
                    out["sw_version_line"] = line.strip()[:80]
                    break
    except Exception as e:
        out["sw_err"] = str(e)[:200]
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
            MemorySize=256, Timeout=60, Code={"ZipFile": zb})
        lam.get_waiter("function_active_v2").wait(FunctionName=NAME)
    except Exception:
        lam.update_function_code(FunctionName=NAME, ZipFile=zb)
        lam.get_waiter("function_updated").wait(FunctionName=NAME)
    time.sleep(2)
    resp = lam.invoke(FunctionName=NAME, InvocationType="RequestResponse", Payload=b"{}")
    body = resp["Payload"].read().decode("utf-8")
    try:
        out["test"] = json.loads(json.loads(body).get("body", "{}"))
    except Exception:
        out["raw"] = body[:2000]
    try: lam.delete_function(FunctionName=NAME)
    except: pass
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f:
        json.dump(out, f, indent=2, default=str)


if __name__ == "__main__":
    main()
