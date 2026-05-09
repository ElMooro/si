#!/usr/bin/env python3
"""Step 379b — Check S3 bucket CORS config + simulate browser-style preflight."""
import io, json, os, time, zipfile
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/379b_s3_cors_check.json"
NAME = "justhodl-tmp-s3-cors"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
lam = boto3.client("lambda", region_name="us-east-1")

DIAG_CODE = '''
import json, urllib.request, urllib.error
import boto3

s3 = boto3.client("s3", region_name="us-east-1")

def lambda_handler(event, context):
    out = {}

    # 1. Bucket CORS config
    try:
        cors = s3.get_bucket_cors(Bucket="justhodl-dashboard-live")
        out["bucket_cors"] = cors.get("CORSRules", [])
    except Exception as e:
        out["bucket_cors"] = {"error": str(e), "msg": "NO CORS CONFIGURED"}

    # 2. Simulate browser-origin GET request to each S3 JSON
    files = ["data/report.json", "regime/current.json", "data/crisis-plumbing.json",
             "data/auction-crisis.json", "data/auction-tenor-signals.json"]
    out["browser_simulated_fetches"] = {}
    for k in files:
        url = f"https://justhodl-dashboard-live.s3.amazonaws.com/{k}"
        try:
            req = urllib.request.Request(url, headers={
                "Origin": "https://justhodl.ai",
                "User-Agent": "Mozilla/5.0",
            })
            with urllib.request.urlopen(req, timeout=10) as r:
                # Capture response headers — what would the browser see?
                hdrs = dict(r.getheaders())
                out["browser_simulated_fetches"][k] = {
                    "status": r.status,
                    "access_control_allow_origin": hdrs.get("Access-Control-Allow-Origin"),
                    "access_control_allow_methods": hdrs.get("Access-Control-Allow-Methods"),
                    "vary": hdrs.get("Vary"),
                    "content_type": hdrs.get("Content-Type"),
                    "size": int(hdrs.get("Content-Length") or 0),
                }
        except Exception as e:
            out["browser_simulated_fetches"][k] = {"error": str(e)[:200]}

    # 3. Simulate OPTIONS preflight
    try:
        req = urllib.request.Request(
            "https://justhodl-dashboard-live.s3.amazonaws.com/data/report.json",
            method="OPTIONS",
            headers={
                "Origin": "https://justhodl.ai",
                "Access-Control-Request-Method": "GET",
                "Access-Control-Request-Headers": "content-type",
            }
        )
        with urllib.request.urlopen(req, timeout=10) as r:
            hdrs = dict(r.getheaders())
            out["preflight"] = {
                "status": r.status,
                "access_control_allow_origin": hdrs.get("Access-Control-Allow-Origin"),
                "access_control_allow_methods": hdrs.get("Access-Control-Allow-Methods"),
            }
    except urllib.error.HTTPError as e:
        out["preflight"] = {"status": e.code, "error": e.reason}
    except Exception as e:
        out["preflight"] = {"error": str(e)}

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
        lam.update_function_code(FunctionName=NAME, ZipFile=zb)
        lam.get_waiter("function_updated").wait(FunctionName=NAME)
    time.sleep(2)
    resp = lam.invoke(FunctionName=NAME, InvocationType="RequestResponse", Payload=b"{}")
    body = resp["Payload"].read().decode("utf-8")
    try:
        parsed = json.loads(body)
        out["test"] = json.loads(parsed["body"]) if "body" in parsed else parsed
    except Exception:
        out["raw"] = body[:5000]
    try:
        lam.delete_function(FunctionName=NAME)
    except Exception:
        pass
    out["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f:
        json.dump(out, f, indent=2, default=str)


if __name__ == "__main__":
    main()
