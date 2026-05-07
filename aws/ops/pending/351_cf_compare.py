#!/usr/bin/env python3
"""Step 351 — Compare CF Worker response for known vs new agents."""
import io
import json
import os
import time
import zipfile
from datetime import datetime, timezone

import boto3

REPORT = "aws/ops/reports/351_cf_compare.json"
NAME = "justhodl-cf-compare"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"

lam = boto3.client("lambda", region_name="us-east-1")


DIAG_CODE = '''
import json, urllib.request

def lambda_handler(event, context):
    out = {}
    for label, url in [
        ("volatility_known", "https://api.justhodl.ai/agent/volatility"),
        ("dollar_known",     "https://api.justhodl.ai/agent/dollar"),
        ("chart_new",        "https://api.justhodl.ai/agent/chart-data?catalog=1"),
        ("invalid_control",  "https://api.justhodl.ai/agent/nonexistent"),
        ("worker_root",      "https://api.justhodl.ai/"),
    ]:
        try:
            req = urllib.request.Request(url, 
                headers={"Origin": "https://justhodl.ai", "User-Agent": "test/1.0"})
            with urllib.request.urlopen(req, timeout=20) as r:
                body = r.read().decode("utf-8")
                out[label] = {"status": r.status, "size": len(body),
                                "preview": body[:200],
                                "cf_ray": r.getheader("Cf-Ray", "")}
        except urllib.error.HTTPError as e:
            try:
                body = e.read().decode("utf-8")
            except Exception:
                body = ""
            out[label] = {"http_err": e.code, "body": body[:200]}
        except Exception as e:
            out[label] = {"err": str(e)[:200]}
    return {"statusCode": 200, "body": json.dumps(out, default=str)}
'''


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("lambda_function.py", DIAG_CODE)
    zb = buf.getvalue()
    try:
        lam.create_function(FunctionName=NAME, Runtime="python3.12",
            Handler="lambda_function.lambda_handler", Role=ROLE_ARN,
            MemorySize=256, Timeout=60, Code={"ZipFile": zb})
        lam.get_waiter("function_active_v2").wait(FunctionName=NAME)
    except Exception:
        try:
            lam.update_function_code(FunctionName=NAME, ZipFile=zb)
            lam.get_waiter("function_updated").wait(FunctionName=NAME)
        except Exception:
            pass
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
    print(json.dumps(out, indent=2, default=str)[:3500])


if __name__ == "__main__":
    main()
