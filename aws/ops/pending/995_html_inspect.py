#!/usr/bin/env python3
"""Step 995 — Inspect the deployed positioning/index.html for every URL it fetches.

If something says 'failed to fetch' the page must be calling a URL that fails.
Find every URL in the page and test each.
"""
import io, json, os, time, zipfile
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/995_html_inspect.json"
NAME = "justhodl-html-inspect"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
lam = boto3.client("lambda", region_name="us-east-1")

DIAG = '''
import json, urllib.request, re

UA = "Mozilla/5.0 Chrome/120"

def probe(url, origin="https://justhodl.ai"):
    try:
        req = urllib.request.Request(url, headers={
            "Origin": origin, "Referer": "https://justhodl.ai/positioning/",
            "User-Agent": UA, "Accept": "*/*",
            "Sec-Fetch-Mode": "cors",
        })
        with urllib.request.urlopen(req, timeout=20) as r:
            return {"status": r.status, "size": len(r.read())}
    except urllib.error.HTTPError as e:
        return {"http_err": e.code}
    except Exception as e:
        return {"err": str(e)[:200]}

def lambda_handler(event, context):
    out = {}
    
    # Fetch deployed page
    try:
        req = urllib.request.Request("https://justhodl.ai/positioning/",
            headers={"User-Agent": UA, "Cache-Control": "no-cache"})
        with urllib.request.urlopen(req, timeout=15) as r:
            html = r.read().decode("utf-8", errors="replace")
            out["html_size"] = len(html)
            out["html_etag"] = r.getheader("Etag", "")
            out["html_last_modified"] = r.getheader("Last-Modified", "")
            out["html_cache"] = r.getheader("Cf-Cache-Status", "")
    except Exception as e:
        out["html_err"] = str(e)[:200]
        return {"statusCode": 200, "body": json.dumps(out)}
    
    # Find ALL URLs the page fetches
    api_match = re.search(r'(?:var|const|let)\\s+API\\s*=\\s*["\']([^"\']+)["\']', html)
    out["api_var"] = api_match.group(1) if api_match else None
    
    # Find every fetch() call URL
    fetch_urls = re.findall(r'fetch\\([\'"]?([^\\)\'\"]+)', html)
    out["fetch_urls_in_html"] = fetch_urls[:10]
    
    # Find any literal Lambda URLs that snuck through
    lambda_urls = re.findall(r'https://[a-z0-9]+\\.lambda-url\\.us-east-1\\.on\\.aws[^\\s\'"<>)]*', html)
    out["lambda_urls_in_html"] = list(set(lambda_urls))[:10]
    
    # Test the 3 endpoints exactly as the page would
    if api_match:
        api = api_match.group(1)
        for path in ("/signals", "/cot/all", "/futures"):
            out["test " + path] = probe(api + path + "?t=12345")
    
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
