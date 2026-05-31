#!/usr/bin/env python3
"""Step 994 — Browser-fidelity probe of CFTC proxy."""
import io, json, os, time, zipfile
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/994_cftc_browser_probe.json"
NAME = "justhodl-cftc-browser"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
lam = boto3.client("lambda", region_name="us-east-1")

DIAG = '''
import json, urllib.request

UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36"

def probe(url, method="GET"):
    try:
        req = urllib.request.Request(url, method=method, headers={
            "Origin": "https://justhodl.ai",
            "Referer": "https://justhodl.ai/positioning/",
            "User-Agent": UA,
            "Accept": "*/*",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-site",
        })
        with urllib.request.urlopen(req, timeout=20) as r:
            body = r.read(2048).decode("utf-8", errors="replace")
            hdrs = dict(r.getheaders())
            cors = {k: v for k, v in hdrs.items() if "access" in k.lower() or "vary" in k.lower() or "cf-" in k.lower()}
            return {
                "status": r.status, "size_first2k": len(body),
                "preview": body[:200],
                "cors_headers": cors,
                "content_type": hdrs.get("Content-Type"),
                "cf_ray": hdrs.get("Cf-Ray", ""),
            }
    except urllib.error.HTTPError as e:
        return {"http_err": e.code, "body": e.read(500).decode("utf-8", errors="replace")}
    except Exception as e:
        return {"err": str(e)[:300]}

def probe_options(url):
    """CORS preflight."""
    try:
        req = urllib.request.Request(url, method="OPTIONS", headers={
            "Origin": "https://justhodl.ai",
            "Access-Control-Request-Method": "GET",
            "User-Agent": UA,
        })
        with urllib.request.urlopen(req, timeout=15) as r:
            hdrs = dict(r.getheaders())
            return {"status": r.status, "cors_headers": {k: v for k, v in hdrs.items() if "access" in k.lower()}}
    except urllib.error.HTTPError as e:
        return {"http_err": e.code}
    except Exception as e:
        return {"err": str(e)[:200]}

def lambda_handler(event, context):
    out = {}
    for path in ("/signals", "/cot/all", "/futures"):
        url = "https://api.justhodl.ai/agent/cftc-positioning" + path
        out["GET " + path] = probe(url)
        out["OPTIONS " + path] = probe_options(url)
    
    # Also fetch the deployed page itself to see what API it's using
    out["html"] = probe("https://justhodl.ai/positioning/")
    
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
