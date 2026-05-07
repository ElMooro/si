#!/usr/bin/env python3
"""Step 348 — Verify chart-data CORS fix actually fixed it."""
import io
import json
import os
import time
import zipfile
from datetime import datetime, timezone

import boto3

REPORT = "aws/ops/reports/348_chart_verify.json"
NAME = "justhodl-diag-temp3"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"

lam = boto3.client("lambda", region_name="us-east-1")


DIAG_CODE = '''
import json
import urllib.request


def lambda_handler(event, context):
    out = {}
    chart_url = "https://zsgb72zf4ayw6ajw7phbyq6wzq0haobh.lambda-url.us-east-1.on.aws/?catalog=1&_v=2"
    
    # Test the GET with proper Origin header — show ALL response headers verbatim
    try:
        req = urllib.request.Request(chart_url,
            headers={"Origin": "https://justhodl.ai", "User-Agent": "diag/1.0"})
        with urllib.request.urlopen(req, timeout=15) as r:
            out["status"] = r.status
            out["body_size"] = len(r.read())
            # Get all headers as list of tuples (preserves duplicates and case)
            out["all_headers"] = [(k, v) for k, v in r.getheaders()]
    except Exception as e:
        out["err"] = str(e)[:300]
    
    return {"statusCode": 200, "body": json.dumps(out, default=str)}
'''


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}

    # Wait for Lambda code to deploy if needed
    time.sleep(15)

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("lambda_function.py", DIAG_CODE)
    zb = buf.getvalue()

    try:
        lam.create_function(
            FunctionName=NAME, Runtime="python3.12",
            Handler="lambda_function.lambda_handler", Role=ROLE_ARN,
            MemorySize=256, Timeout=30, Code={"ZipFile": zb},
            Description="Temporary verification — delete after use",
        )
        lam.get_waiter("function_active_v2").wait(FunctionName=NAME)
    except Exception:
        try:
            lam.update_function_code(FunctionName=NAME, ZipFile=zb)
            lam.get_waiter("function_updated").wait(FunctionName=NAME)
        except Exception as e:
            out["create_err"] = str(e)

    time.sleep(2)
    resp = lam.invoke(FunctionName=NAME, InvocationType="RequestResponse", Payload=b"{}")
    body = resp["Payload"].read().decode("utf-8")
    try:
        outer = json.loads(body)
        out["test"] = json.loads(outer.get("body", "{}"))
    except Exception:
        out["test_raw"] = body[:1500]

    try:
        lam.delete_function(FunctionName=NAME)
    except Exception:
        pass

    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f:
        json.dump(out, f, indent=2, default=str)
    print(json.dumps(out, indent=2, default=str)[:5000])


if __name__ == "__main__":
    main()
