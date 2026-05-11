#!/usr/bin/env python3
"""Step 435 — Stage 12 deployment:
  1) Wait for deploy-lambdas to publish justhodl-ma-tracker
  2) Create a Function URL (no auth, CORS *)
  3) Add lambda:InvokeFunctionUrl permission for public invoke
  4) Patch ma/app.js with the actual Function URL (commit back to repo)
  5) Test invoke to populate the S3 cache
  6) Confirm end-to-end
"""
import io, json, os, time, zipfile
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/435_ma_tracker_setup.json"
NAME = "justhodl-tmp-ma-setup"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
lam = boto3.client("lambda", region_name="us-east-1")

DIAG = '''
import json
import boto3
import urllib.request
lam = boto3.client("lambda", region_name="us-east-1")

def lambda_handler(event, context):
    out = {}

    # 1. Confirm Lambda exists
    try:
        cfg = lam.get_function_configuration(FunctionName="justhodl-ma-tracker")
        out["lambda"] = {
            "last_modified": cfg["LastModified"],
            "code_size": cfg["CodeSize"],
            "memory": cfg["MemorySize"],
            "timeout": cfg["Timeout"],
            "env_keys": list((cfg.get("Environment") or {}).get("Variables", {}).keys()),
        }
    except Exception as e:
        out["lambda_err"] = str(e)[:300]
        return {"statusCode": 200, "body": json.dumps(out, default=str)}

    # 2. Try to get existing function URL or create a new one
    try:
        try:
            url_cfg = lam.get_function_url_config(FunctionName="justhodl-ma-tracker")
            out["function_url"] = url_cfg["FunctionUrl"]
            out["url_action"] = "existed"
        except lam.exceptions.ResourceNotFoundException:
            url_cfg = lam.create_function_url_config(
                FunctionName="justhodl-ma-tracker",
                AuthType="NONE",
                Cors={
                    "AllowOrigins": ["*"],
                    "AllowMethods": ["GET", "OPTIONS"],
                    "AllowHeaders": ["content-type"],
                    "MaxAge": 86400,
                },
            )
            out["function_url"] = url_cfg["FunctionUrl"]
            out["url_action"] = "created"
        # Add permission so the URL is publicly invokable
        try:
            lam.add_permission(
                FunctionName="justhodl-ma-tracker",
                StatementId="FunctionURLAllowPublicAccess",
                Action="lambda:InvokeFunctionUrl",
                Principal="*",
                FunctionUrlAuthType="NONE",
            )
            out["permission"] = "added"
        except lam.exceptions.ResourceConflictException:
            out["permission"] = "already_existed"
    except Exception as e:
        out["url_err"] = str(e)[:300]
        return {"statusCode": 200, "body": json.dumps(out, default=str)}

    # 3. Test invoke via direct HTTP to the Function URL
    try:
        url = out["function_url"]
        req = urllib.request.Request(url, headers={"User-Agent": "JustHodl-Test/1.0"})
        with urllib.request.urlopen(req, timeout=60) as r:
            body = r.read()
        payload = json.loads(body.decode("utf-8"))
        out["http_test"] = {
            "status": 200,
            "size_kb": round(len(body)/1024, 1),
            "generated_at": payload.get("generated_at"),
            "elapsed_seconds": payload.get("elapsed_seconds"),
            "n_deals": len((payload.get("deals") or [])),
            "n_profiles": len((payload.get("profiles") or {})),
            "summary_keys": list((payload.get("summary") or {}).keys()),
        }
        if payload.get("summary", {}).get("by_sector"):
            out["http_test"]["top_sectors"] = payload["summary"]["by_sector"][:5]
        if payload.get("summary", {}).get("top_acquirers"):
            out["http_test"]["top_acquirers"] = payload["summary"]["top_acquirers"][:5]
    except Exception as e:
        out["http_test_err"] = str(e)[:300]

    return {"statusCode": 200, "body": json.dumps(out, default=str)}
'''


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}
    # Wait for deploy-lambdas
    print("Waiting 90s for deploy-lambdas to publish justhodl-ma-tracker...")
    time.sleep(90)
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

    # 4. Patch ma/app.js with the actual Function URL
    function_url = (out.get("test") or {}).get("function_url")
    if function_url:
        app_js_path = "ma/app.js"
        try:
            with open(app_js_path) as f:
                content = f.read()
            patched = content.replace("__FUNCTION_URL_PLACEHOLDER__", function_url)
            if patched != content:
                with open(app_js_path, "w") as f:
                    f.write(patched)
                out["app_js_patched"] = True
            else:
                out["app_js_patched"] = "already_patched_or_no_placeholder"
        except Exception as e:
            out["app_js_err"] = str(e)[:200]

    try: lam.delete_function(FunctionName=NAME)
    except Exception: pass
    out["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f: json.dump(out, f, indent=2, default=str)


if __name__ == "__main__":
    main()
