#!/usr/bin/env python3
"""Step 992 — Probe what justhodl.ai/cftc actually serves."""
import io, json, os, time, zipfile
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/992_cftc_url_probe.json"
NAME = "justhodl-cftc-url-probe"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
lam = boto3.client("lambda", region_name="us-east-1")

DIAG = '''
import json, urllib.request

UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) Chrome/120 Safari/537.36"

def probe(url):
    try:
        req = urllib.request.Request(url, headers={
            "User-Agent": UA, "Cache-Control": "no-cache",
            "Accept": "text/html,application/json,*/*"
        })
        with urllib.request.urlopen(req, timeout=15) as r:
            body = r.read().decode("utf-8", errors="replace")
            return {"status": r.status, "size": len(body),
                    "ct": r.getheader("Content-Type", ""),
                    "final_url": r.url,
                    "title_match": body[body.find("<title>"):body.find("</title>")+8] if "<title>" in body else "",
                    "preview": body[:400]}
    except urllib.error.HTTPError as e:
        try: b = e.read().decode("utf-8", errors="replace")
        except: b = ""
        return {"http_err": e.code, "body_preview": b[:400], "url": url,
                "final_url": e.url if hasattr(e, "url") else ""}
    except Exception as e:
        return {"err": str(e)[:300]}

def lambda_handler(event, context):
    out = {}
    for label, url in [
        ("/cftc",         "https://justhodl.ai/cftc"),
        ("/cftc/",        "https://justhodl.ai/cftc/"),
        ("/cftc.html",    "https://justhodl.ai/cftc.html"),
        ("/positioning/", "https://justhodl.ai/positioning/"),
    ]:
        out[label] = probe(url)
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
        out["raw"] = body[:1500]
    try: lam.delete_function(FunctionName=NAME)
    except: pass
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f:
        json.dump(out, f, indent=2, default=str)


if __name__ == "__main__":
    main()
