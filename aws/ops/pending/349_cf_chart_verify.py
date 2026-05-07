#!/usr/bin/env python3
"""Step 349 — Verify api.justhodl.ai/agent/chart-data is reachable."""
import io
import json
import os
import time
import zipfile
from datetime import datetime, timezone

import boto3

REPORT = "aws/ops/reports/349_cf_chart_verify.json"
NAME = "justhodl-cf-test"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"

lam = boto3.client("lambda", region_name="us-east-1")


DIAG_CODE = '''
import json
import urllib.request


def lambda_handler(event, context):
    out = {}
    
    # Test 1: api.justhodl.ai/agent/chart-data?catalog=1 — main entrypoint
    try:
        req = urllib.request.Request(
            "https://api.justhodl.ai/agent/chart-data?catalog=1",
            headers={"Origin": "https://justhodl.ai", "User-Agent": "verify/1.0"}
        )
        with urllib.request.urlopen(req, timeout=20) as r:
            body = r.read().decode("utf-8")
            out["api_chart_data"] = {
                "status": r.status,
                "size": len(body),
                "preview": body[:200],
                "headers": {k: v for k, v in r.getheaders()},
            }
    except Exception as e:
        out["api_chart_data"] = {"err": str(e)[:300]}
    
    # Test 2: api.justhodl.ai/agent/chart-data?series=DGS10
    try:
        req = urllib.request.Request(
            "https://api.justhodl.ai/agent/chart-data?series=DGS10&from=2024-01-01",
            headers={"Origin": "https://justhodl.ai", "User-Agent": "verify/1.0"}
        )
        with urllib.request.urlopen(req, timeout=20) as r:
            body = r.read().decode("utf-8")
            d = json.loads(body)
            out["api_dgs10"] = {
                "status": r.status,
                "n_obs": d.get("n_obs"),
                "source": d.get("source"),
                "last": (d.get("data") or [{}])[-1] if d.get("data") else None,
            }
    except Exception as e:
        out["api_dgs10"] = {"err": str(e)[:300]}
    
    # Test 3: deployed chart-pro.html
    try:
        req = urllib.request.Request(
            "https://justhodl.ai/chart-pro.html",
            headers={"Cache-Control": "no-cache", "User-Agent": "verify/1.0"}
        )
        with urllib.request.urlopen(req, timeout=15) as r:
            html = r.read().decode("utf-8")
            for line in html.split("\\n"):
                if "CHART_API" in line and "=" in line and "//" not in line[:line.find("CHART_API")]:
                    out["html_chart_api_line"] = line.strip()[:200]
                    break
    except Exception as e:
        out["html_check"] = {"err": str(e)[:200]}
    
    return {"statusCode": 200, "body": json.dumps(out, default=str)}
'''


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("lambda_function.py", DIAG_CODE)
    zb = buf.getvalue()

    try:
        lam.create_function(
            FunctionName=NAME, Runtime="python3.12",
            Handler="lambda_function.lambda_handler", Role=ROLE_ARN,
            MemorySize=256, Timeout=60, Code={"ZipFile": zb},
            Description="Temp verify — delete after",
        )
        lam.get_waiter("function_active_v2").wait(FunctionName=NAME)
    except Exception:
        try:
            lam.update_function_code(FunctionName=NAME, ZipFile=zb)
            lam.get_waiter("function_updated").wait(FunctionName=NAME)
        except Exception as e:
            out["err"] = str(e)

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
