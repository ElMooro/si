#!/usr/bin/env python3
"""Step 990 — Diagnose CFTC positioning Lambda + proxy state.

positioning/index.html hits these endpoints directly:
  https://35t3serkv4gn2hk7utwvp7t2sa0flbum.lambda-url.us-east-1.on.aws/signals
  https://35t3serkv4gn2hk7utwvp7t2sa0flbum.lambda-url.us-east-1.on.aws/cot/all
  https://35t3serkv4gn2hk7utwvp7t2sa0flbum.lambda-url.us-east-1.on.aws/futures

We confirm:
  (1) Lambda itself responds correctly to all 3 paths (data is good)
  (2) api.justhodl.ai/agent/cftc-positioning/<path> returns 404
      (because the agent isn't in AGENT_LAMBDAS yet)
This proves the right fix is adding cftc-positioning to the CF Worker.
"""
import io, json, os, time, zipfile
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/990_cftc_diag.json"
NAME = "justhodl-cftc-diag"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
lam = boto3.client("lambda", region_name="us-east-1")

DIAG = '''
import json, urllib.request

LAMBDA = "https://35t3serkv4gn2hk7utwvp7t2sa0flbum.lambda-url.us-east-1.on.aws"
PROXY  = "https://api.justhodl.ai/agent/cftc-positioning"

def probe(label, url):
    try:
        req = urllib.request.Request(url, headers={
            "Origin": "https://justhodl.ai",
            "User-Agent": "Mozilla/5.0 (verify) Chrome/120",
        })
        with urllib.request.urlopen(req, timeout=20) as r:
            body = r.read().decode("utf-8")
            sample = body[:300]
            ok = False
            try:
                d = json.loads(body)
                ok = isinstance(d, dict)
            except Exception:
                d = None
            return {
                "status": r.status,
                "size": len(body),
                "json_ok": ok,
                "preview": sample,
                "top_keys": list(d.keys())[:10] if ok else None,
            }
    except urllib.error.HTTPError as e:
        try: body = e.read().decode("utf-8")
        except Exception: body = ""
        return {"http_err": e.code, "body": body[:300]}
    except Exception as e:
        return {"err": str(e)[:300]}

def lambda_handler(event, context):
    out = {}
    for label, base in [("direct", LAMBDA), ("proxy", PROXY)]:
        for path in ("/signals", "/cot/all", "/futures"):
            out[f"{label}{path}"] = probe(label + path, base + path)
    return {"statusCode": 200, "body": json.dumps(out, default=str)}
'''


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("lambda_function.py", DIAG)
    zb = buf.getvalue()
    try:
        lam.create_function(
            FunctionName=NAME, Runtime="python3.12",
            Handler="lambda_function.lambda_handler", Role=ROLE_ARN,
            MemorySize=256, Timeout=120, Code={"ZipFile": zb},
        )
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
    try:
        lam.delete_function(FunctionName=NAME)
    except Exception:
        pass
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f:
        json.dump(out, f, indent=2, default=str)


if __name__ == "__main__":
    main()
