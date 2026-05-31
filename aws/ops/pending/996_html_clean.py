#!/usr/bin/env python3
"""Step 996 — Clean inspection of deployed positioning page."""
import io, json, os, time, zipfile
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/996_html_clean.json"
NAME = "justhodl-html-clean"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
lam = boto3.client("lambda", region_name="us-east-1")

DIAG = r'''
import json
import re
import urllib.request

UA = "Mozilla/5.0 Chrome/120"

def probe(url):
    try:
        req = urllib.request.Request(url, headers={
            "Origin": "https://justhodl.ai",
            "Referer": "https://justhodl.ai/positioning/",
            "User-Agent": UA,
            "Accept": "*/*",
        })
        with urllib.request.urlopen(req, timeout=20) as r:
            body = r.read()
            return {"status": r.status, "size": len(body)}
    except urllib.error.HTTPError as e:
        return {"http_err": e.code}
    except Exception as e:
        return {"err": str(e)[:300]}

def lambda_handler(event, context):
    out = {}
    
    # Fetch deployed page
    try:
        req = urllib.request.Request(
            "https://justhodl.ai/positioning/",
            headers={"User-Agent": UA, "Cache-Control": "no-cache"})
        with urllib.request.urlopen(req, timeout=15) as r:
            html = r.read().decode("utf-8", errors="replace")
            out["html_size"] = len(html)
            out["html_cf_cache"] = r.getheader("Cf-Cache-Status", "")
            out["html_age"] = r.getheader("Age", "")
            out["html_etag"] = r.getheader("Etag", "")
    except Exception as e:
        out["html_err"] = str(e)[:300]
        return {"statusCode": 200, "body": json.dumps(out)}
    
    # Use string find instead of regex (avoid escape headaches)
    out["has_api_var"] = "var API=" in html or "var API =" in html or "const API=" in html
    
    # Find any occurrence of the API value
    idx = html.find("var API=")
    if idx >= 0:
        end = html.find(",", idx)
        out["api_line"] = html[idx:end+1]
    
    # Direct lambda urls?
    out["lambda_url_count"] = html.count("lambda-url.us-east-1.on.aws")
    out["proxy_url_count"] = html.count("api.justhodl.ai/agent/cftc-positioning")
    
    # Find every fetch( occurrence
    fetches = []
    pos = 0
    while True:
        idx = html.find("fetch(", pos)
        if idx < 0:
            break
        end = html.find(")", idx)
        if end > 0:
            fetches.append(html[idx:end+1][:200])
        pos = idx + 1
        if len(fetches) > 10:
            break
    out["fetch_calls"] = fetches
    
    # Test all 3 endpoints exactly as the page would
    base = "https://api.justhodl.ai/agent/cftc-positioning"
    for p in ("/signals", "/cot/all", "/futures"):
        out["test " + p] = probe(base + p + "?t=12345")
    
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
