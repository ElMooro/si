#!/usr/bin/env python3
"""Step 347 — Diagnose chart-pro.html load failure."""
import io
import json
import os
import time
import zipfile
from datetime import datetime, timezone

import boto3

REPORT = "aws/ops/reports/347_chart_diag.json"
NAME = "justhodl-diag-temp2"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"

lam = boto3.client("lambda", region_name="us-east-1")
s3 = boto3.client("s3", region_name="us-east-1")


DIAG_CODE = '''
import json
import urllib.request


def lambda_handler(event, context):
    out = {}
    
    # 1. Fetch what's actually deployed at justhodl.ai/chart-pro.html
    try:
        req = urllib.request.Request("https://justhodl.ai/chart-pro.html",
                                       headers={"User-Agent": "diag/1.0",
                                                "Cache-Control": "no-cache"})
        with urllib.request.urlopen(req, timeout=15) as r:
            html = r.read().decode("utf-8")
            out["live_html"] = {
                "status": r.status,
                "bytes": len(html),
                "headers": dict(r.headers),
            }
            # Find the CHART_API line
            for line in html.split("\\n"):
                if "CHART_API" in line and "=" in line:
                    out["live_html"]["chart_api_line"] = line.strip()[:300]
                    break
    except Exception as e:
        out["live_html"] = {"err": str(e)[:200]}
    
    # 2. Test the chart-data Lambda URL directly
    chart_url = "https://zsgb72zf4ayw6ajw7phbyq6wzq0haobh.lambda-url.us-east-1.on.aws/?catalog=1"
    try:
        req = urllib.request.Request(chart_url,
                                       headers={"Origin": "https://justhodl.ai",
                                                "User-Agent": "diag/1.0"})
        with urllib.request.urlopen(req, timeout=15) as r:
            body = r.read().decode("utf-8")
            out["lambda_direct"] = {
                "status": r.status,
                "headers": dict(r.headers),
                "body_preview": body[:200],
                "body_size": len(body),
            }
    except Exception as e:
        out["lambda_direct"] = {"err": str(e)[:200]}
    
    # 3. Test OPTIONS (CORS preflight) to the chart-data Lambda
    try:
        opt_req = urllib.request.Request(
            chart_url,
            method="OPTIONS",
            headers={
                "Origin": "https://justhodl.ai",
                "Access-Control-Request-Method": "GET",
                "Access-Control-Request-Headers": "content-type",
                "User-Agent": "diag/1.0",
            },
        )
        with urllib.request.urlopen(opt_req, timeout=10) as r:
            out["preflight"] = {
                "status": r.status,
                "headers": dict(r.headers),
            }
    except Exception as e:
        out["preflight"] = {"err": str(e)[:200]}
    
    return {"statusCode": 200, "body": json.dumps(out, default=str)}
'''


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}

    # Get Lambda URL config
    try:
        r = lam.get_function_url_config(FunctionName="justhodl-chart-data")
        out["chart_data_url_config"] = {
            "url": r.get("FunctionUrl"),
            "auth": r.get("AuthType"),
            "cors": r.get("Cors"),
        }
    except Exception as e:
        out["chart_data_url_config"] = {"err": str(e)}

    # Build temp diagnostic Lambda
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("lambda_function.py", DIAG_CODE)
    zb = buf.getvalue()

    print("[diag] Creating temp Lambda…")
    try:
        lam.create_function(
            FunctionName=NAME, Runtime="python3.12",
            Handler="lambda_function.lambda_handler", Role=ROLE_ARN,
            MemorySize=256, Timeout=60, Code={"ZipFile": zb},
            Description="Temporary diagnostic — delete after use",
        )
        lam.get_waiter("function_active_v2").wait(FunctionName=NAME)
    except Exception:
        try:
            lam.update_function_code(FunctionName=NAME, ZipFile=zb)
            lam.get_waiter("function_updated").wait(FunctionName=NAME)
        except Exception as e:
            out["create_err"] = str(e)

    time.sleep(2)
    print("[diag] Invoking…")
    resp = lam.invoke(FunctionName=NAME, InvocationType="RequestResponse", Payload=b"{}")
    body = resp["Payload"].read().decode("utf-8")
    try:
        outer = json.loads(body)
        out["diag"] = json.loads(outer.get("body", "{}"))
    except Exception:
        out["diag_raw"] = body[:1500]

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
