#!/usr/bin/env python3
"""Step 425 — Stage 9 deploy:
  1) Update Lambda memory 512 → 1280 MB, timeout 600 → 900s
  2) Fire async force refresh
  3) Return immediately (Lambda runs ~9 min on its own with 14 endpoints/stock)"""
import io, json, os, time, zipfile
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/425_stage9_deploy.json"
NAME = "justhodl-tmp-s9-deploy"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
lam = boto3.client("lambda", region_name="us-east-1")

DIAG = '''
import json, time
import boto3
lam = boto3.client("lambda", region_name="us-east-1")

def lambda_handler(event, context):
    out = {}

    # 1. Check current config
    cfg_before = lam.get_function_configuration(FunctionName="justhodl-stock-screener")
    out["before"] = {
        "memory": cfg_before["MemorySize"],
        "timeout": cfg_before["Timeout"],
        "last_modified": cfg_before["LastModified"],
        "code_size": cfg_before["CodeSize"],
    }

    # 2. Update config — memory 1280, timeout 900s
    try:
        lam.update_function_configuration(
            FunctionName="justhodl-stock-screener",
            MemorySize=1280,
            Timeout=900,
        )
        # Wait for update to apply
        for _ in range(30):
            time.sleep(2)
            cfg = lam.get_function_configuration(FunctionName="justhodl-stock-screener")
            if cfg.get("LastUpdateStatus") == "Successful":
                break
        cfg_after = lam.get_function_configuration(FunctionName="justhodl-stock-screener")
        out["after"] = {
            "memory": cfg_after["MemorySize"],
            "timeout": cfg_after["Timeout"],
            "last_modified": cfg_after["LastModified"],
        }
    except Exception as e:
        out["update_err"] = str(e)[:300]

    # 3. Fire async refresh
    try:
        resp = lam.invoke(
            FunctionName="justhodl-stock-screener",
            InvocationType="Event",
            Payload=json.dumps({"force": True}).encode())
        out["invoke"] = {"status": resp.get("StatusCode")}
    except Exception as e:
        out["invoke"] = {"error": str(e)[:200]}

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
        parsed = json.loads(body)
        out["test"] = json.loads(parsed["body"]) if "body" in parsed and parsed["body"] else parsed
    except Exception:
        out["raw"] = body[:6000]
    try: lam.delete_function(FunctionName=NAME)
    except Exception: pass
    out["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f: json.dump(out, f, indent=2, default=str)


if __name__ == "__main__":
    main()
