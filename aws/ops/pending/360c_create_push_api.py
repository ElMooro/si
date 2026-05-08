#!/usr/bin/env python3
"""
Step 360c — Create justhodl-push-api Lambda (first-time only).

deploy-lambdas.yml only handles `aws lambda update-function-code` which
requires the function to already exist. For brand-new Lambdas we have
to call `create_function` once. After this script runs, future updates
to aws/lambdas/justhodl-push-api/source/** will be picked up by the
normal workflow.

Idempotent — if the Lambda already exists, this script just refreshes
the code and exits.
"""
import io
import json
import os
import time
import zipfile
from datetime import datetime, timezone
import boto3
from botocore.exceptions import ClientError

REPORT = "aws/ops/reports/360c_create_push_api.json"
FN_NAME = "justhodl-push-api"
SRC_DIR = "aws/lambdas/justhodl-push-api/source"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"

lam = boto3.client("lambda", region_name="us-east-1")

def build_zip():
    """Zip the source directory in-memory."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for root, _, files in os.walk(SRC_DIR):
            for f in files:
                full = os.path.join(root, f)
                arc = os.path.relpath(full, SRC_DIR)
                zf.write(full, arc)
    return buf.getvalue()

def main():
    out = {"started": datetime.now(timezone.utc).isoformat(), "fn": FN_NAME}
    if not os.path.isdir(SRC_DIR):
        out["status"] = "error"
        out["error"] = f"Source dir missing: {SRC_DIR}"
        _write(out)
        raise RuntimeError(out["error"])
    zip_bytes = build_zip()
    out["zip_bytes"] = len(zip_bytes)

    try:
        existing = lam.get_function(FunctionName=FN_NAME)
        out["preexisting"] = True
        # Refresh code
        lam.update_function_code(FunctionName=FN_NAME, ZipFile=zip_bytes)
        lam.get_waiter("function_updated").wait(FunctionName=FN_NAME)
        out["action"] = "code_updated"
    except lam.exceptions.ResourceNotFoundException:
        out["preexisting"] = False
        # Create
        resp = lam.create_function(
            FunctionName=FN_NAME,
            Runtime="python3.12",
            Handler="lambda_function.lambda_handler",
            Role=ROLE_ARN,
            Code={"ZipFile": zip_bytes},
            Description="Web Push subscription manager + sender (VAPID + DDB)",
            Timeout=30,
            MemorySize=256,
            Environment={"Variables": {"DDB_TABLE": "justhodl-push-subscriptions"}},
            Tags={"project": "justhodl", "feature": "pwa-push"},
        )
        # Wait for active
        lam.get_waiter("function_active_v2").wait(FunctionName=FN_NAME)
        out["action"] = "created"
        out["function_arn"] = resp.get("FunctionArn")
    except Exception as e:
        out["status"] = "error"
        out["error"] = f"{type(e).__name__}: {e}"
        _write(out)
        raise

    # Verify it works by invoking the GET / endpoint via lambda invoke
    try:
        test_event = {"requestContext": {"http": {"method": "GET"}}, "rawPath": "/"}
        resp = lam.invoke(
            FunctionName=FN_NAME,
            InvocationType="RequestResponse",
            Payload=json.dumps(test_event).encode("utf-8"),
        )
        body = resp["Payload"].read().decode("utf-8")
        try:
            parsed = json.loads(body)
            inner = json.loads(parsed.get("body", "{}"))
            out["test_invoke"] = {"statusCode": parsed.get("statusCode"), "service": inner.get("service"), "endpoints": inner.get("endpoints"), "vapid_configured": inner.get("vapid_configured")}
        except Exception:
            out["test_invoke_raw"] = body[:500]
    except Exception as e:
        out["test_invoke_error"] = str(e)

    out["status"] = "success"
    _write(out)
    print(f"[360c] push-api Lambda → {out.get('action')}")

def _write(out):
    out["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f:
        json.dump(out, f, indent=2, default=str)

if __name__ == "__main__":
    main()
