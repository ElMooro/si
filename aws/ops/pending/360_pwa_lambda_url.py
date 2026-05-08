#!/usr/bin/env python3
"""
Step 360 — Create Lambda Function URL for justhodl-push-api.
(retry — Lambda was created by 360c after deploy-lambdas couldn't update a non-existent function)
Auth: NONE (subscribe is public; admin endpoints check X-Justhodl-Admin-Token header).
CORS: justhodl.ai + www.justhodl.ai allowed; GET, POST, OPTIONS.
Idempotent — if URL already exists, returns existing config.
"""
import json
import os
from datetime import datetime, timezone
import boto3
from botocore.exceptions import ClientError

REPORT = "aws/ops/reports/360_pwa_lambda_url.json"
FN_NAME = "justhodl-push-api"

lam = boto3.client("lambda", region_name="us-east-1")

CORS_CONFIG = {
    "AllowOrigins": ["https://justhodl.ai", "https://www.justhodl.ai"],
    "AllowMethods": ["GET", "POST", "OPTIONS"],
    "AllowHeaders": ["Content-Type", "X-Justhodl-Admin-Token"],
    "ExposeHeaders": [],
    "MaxAge": 3600,
    "AllowCredentials": False,
}

def main():
    out = {"started": datetime.now(timezone.utc).isoformat(), "fn": FN_NAME}
    # Poll up to 180s for the function to appear (deploy-lambdas.yml may
    # still be running; we tolerate the race by retrying every 10s).
    import time as _time
    deadline = _time.time() + 180
    last_err = None
    while _time.time() < deadline:
        try:
            lam.get_function(FunctionName=FN_NAME)
            break
        except ClientError as e:
            last_err = e
            if e.response["Error"]["Code"] != "ResourceNotFoundException":
                out["status"] = "lambda_lookup_error"
                out["error"] = str(e)
                _write(out)
                raise
            _time.sleep(10)
    else:
        out["status"] = "function_not_deployed_in_180s"
        out["error"] = str(last_err)
        out["hint"] = "deploy-lambdas.yml took longer than 180s; rerun this op"
        _write(out)
        raise RuntimeError(out["status"])

    # Try create
    try:
        resp = lam.create_function_url_config(
            FunctionName=FN_NAME,
            AuthType="NONE",
            Cors=CORS_CONFIG,
            InvokeMode="BUFFERED",
        )
        out["created"] = True
        out["function_url"] = resp["FunctionUrl"]
    except lam.exceptions.ResourceConflictException:
        # Already exists — update CORS to make sure config is current
        resp = lam.get_function_url_config(FunctionName=FN_NAME)
        out["created"] = False
        out["function_url"] = resp["FunctionUrl"]
        try:
            lam.update_function_url_config(
                FunctionName=FN_NAME, AuthType="NONE", Cors=CORS_CONFIG, InvokeMode="BUFFERED",
            )
            out["cors_updated"] = True
        except Exception as e:
            out["cors_update_error"] = str(e)
    except Exception as e:
        out["status"] = "error"
        out["error"] = f"{type(e).__name__}: {e}"
        _write(out)
        raise

    # Add resource-based policy so public can invoke
    try:
        lam.add_permission(
            FunctionName=FN_NAME,
            StatementId="FunctionURLAllowPublicAccess",
            Action="lambda:InvokeFunctionUrl",
            Principal="*",
            FunctionUrlAuthType="NONE",
        )
        out["public_invoke_perm"] = "added"
    except lam.exceptions.ResourceConflictException:
        out["public_invoke_perm"] = "already_exists"
    except Exception as e:
        out["public_invoke_perm_error"] = str(e)

    out["status"] = "success"
    _write(out)
    print(f"[360] push-api URL: {out['function_url']}")

def _write(out):
    out["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f:
        json.dump(out, f, indent=2, default=str)

if __name__ == "__main__":
    main()
