#!/usr/bin/env python3
"""Step 437 — Check whether the workflow created a Function URL for the
M&A Lambda. If not, give us the diagnostic info to figure out why."""
import io, json, os, time, zipfile
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/437_check_fn_url.json"
NAME = "justhodl-tmp-437"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
lam = boto3.client("lambda", region_name="us-east-1")

DIAG = '''
import json
import boto3
import urllib.request
lam = boto3.client("lambda", region_name="us-east-1")

def lambda_handler(event, context):
    out = {}
    # Confirm Lambda config + last modified time
    try:
        cfg = lam.get_function_configuration(FunctionName="justhodl-ma-tracker")
        out["lambda"] = {
            "last_modified": cfg["LastModified"],
            "code_size": cfg["CodeSize"],
            "memory": cfg["MemorySize"],
            "timeout": cfg["Timeout"],
        }
    except Exception as e:
        out["lambda_err"] = str(e)[:300]

    # Try to get function URL via the boto3 client.
    # If lambda-execution-role lacks GetFunctionUrlConfig, this will 403
    # but the err message will tell us if a URL exists.
    try:
        url_cfg = lam.get_function_url_config(FunctionName="justhodl-ma-tracker")
        out["function_url"] = url_cfg["FunctionUrl"]
        out["auth_type"] = url_cfg.get("AuthType")
        out["cors"] = url_cfg.get("Cors")
        out["url_created"] = str(url_cfg.get("CreationTime"))
    except Exception as e:
        msg = str(e)
        out["url_err"] = msg[:400]
        # If error mentions ResourceNotFoundException → no URL exists
        # If AccessDenied → URL might exist but we cant read it
        if "ResourceNotFoundException" in msg or "URL configuration does not exist" in msg:
            out["url_state"] = "no_url_exists"
        elif "AccessDenied" in msg:
            out["url_state"] = "exists_but_cant_read"
        else:
            out["url_state"] = "unknown_error"

    # Try direct invoke to confirm Lambda itself works
    try:
        resp = lam.invoke(
            FunctionName="justhodl-ma-tracker",
            InvocationType="RequestResponse",
            Payload=b"{}")
        body = resp["Payload"].read().decode("utf-8")
        parsed = json.loads(body)
        if "body" in parsed:
            inner = json.loads(parsed["body"])
            out["direct_invoke"] = {
                "status_code": parsed.get("statusCode"),
                "n_deals": len(inner.get("deals") or []),
                "n_profiles": len(inner.get("profiles") or {}),
                "elapsed_seconds": inner.get("elapsed_seconds"),
                "summary": inner.get("summary"),
            }
        else:
            out["direct_invoke"] = {"raw": body[:500]}
    except Exception as e:
        out["invoke_err"] = str(e)[:200]

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
        out["raw"] = body[:8000]
    try: lam.delete_function(FunctionName=NAME)
    except Exception: pass
    out["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f: json.dump(out, f, indent=2, default=str)


if __name__ == "__main__":
    main()
