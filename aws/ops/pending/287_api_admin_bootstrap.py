#!/usr/bin/env python3
"""Step 287 — Bootstrap the API key admin Lambda.

After step 286 created the DDB tables, this:
  1. Creates IAM policy granting api-keys-admin read/write on
     justhodl-api-keys table + read on /justhodl/api-admin/*
  2. Generates a fresh admin token (32 bytes random) and stores it
     in SSM /justhodl/api-admin/token (SecureString)
  3. Creates the Lambda function (or updates if it exists)
  4. Creates a Function URL with public access (auth is in the
     Lambda body, not the URL)
  5. Smoke test: create a FREE key, list it, revoke it
  6. Outputs the admin token (so Khalid can save it) and the
     Function URL for future use
"""
import io
import json
import os
import secrets
import time
import urllib.request
import zipfile
from datetime import datetime, timezone

import boto3
from botocore.exceptions import ClientError

REGION = "us-east-1"
ACCOUNT_ID = "857687956942"
LAMBDA_NAME = "justhodl-api-keys-admin"
SOURCE_DIR = f"aws/lambdas/{LAMBDA_NAME}/source"
ROLE_NAME = "lambda-execution-role"
ROLE_ARN = f"arn:aws:iam::{ACCOUNT_ID}:role/{ROLE_NAME}"
KEYS_TABLE = "justhodl-api-keys"
RATE_TABLE = "justhodl-api-rate"
ADMIN_TOKEN_SSM = "/justhodl/api-admin/token"
POLICY_NAME = "api-keys-admin-permissions"
REPORT_PATH = "aws/ops/reports/287_api_admin_bootstrap.json"

lam = boto3.client("lambda", region_name=REGION)
iam = boto3.client("iam", region_name=REGION)
ssm = boto3.client("ssm", region_name=REGION)


def ensure_iam_policy():
    """Inline policy on lambda-execution-role.

    Grants:
      - DDB read/write on justhodl-api-keys (admin operations)
      - DDB read/write on justhodl-api-rate (so any Lambda using
        api_auth.py from this role can rate-limit)
      - SSM read/write on /justhodl/api-admin/* (admin token)
    """
    policy_doc = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "ApiKeysTableReadWrite",
                "Effect": "Allow",
                "Action": [
                    "dynamodb:GetItem", "dynamodb:PutItem", "dynamodb:UpdateItem",
                    "dynamodb:DeleteItem", "dynamodb:Scan", "dynamodb:Query",
                ],
                "Resource": [
                    f"arn:aws:dynamodb:{REGION}:{ACCOUNT_ID}:table/{KEYS_TABLE}",
                    f"arn:aws:dynamodb:{REGION}:{ACCOUNT_ID}:table/{KEYS_TABLE}/index/*",
                ],
            },
            {
                "Sid": "RateTableReadWrite",
                "Effect": "Allow",
                "Action": [
                    "dynamodb:GetItem", "dynamodb:PutItem", "dynamodb:UpdateItem",
                ],
                "Resource": [
                    f"arn:aws:dynamodb:{REGION}:{ACCOUNT_ID}:table/{RATE_TABLE}",
                ],
            },
            {
                "Sid": "ApiAdminSsmReadWrite",
                "Effect": "Allow",
                "Action": ["ssm:GetParameter", "ssm:PutParameter"],
                "Resource": [
                    f"arn:aws:ssm:{REGION}:{ACCOUNT_ID}:parameter/justhodl/api-admin/*",
                ],
            },
        ],
    }
    iam.put_role_policy(
        RoleName=ROLE_NAME, PolicyName=POLICY_NAME,
        PolicyDocument=json.dumps(policy_doc),
    )
    return {"policy": POLICY_NAME, "action": "applied"}


def ensure_admin_token():
    """If /justhodl/api-admin/token doesn't exist, generate and store one.
    Returns the (potentially newly generated) token so the bootstrap
    can echo it once for Khalid to save."""
    try:
        resp = ssm.get_parameter(Name=ADMIN_TOKEN_SSM, WithDecryption=True)
        return {"status": "already_exists", "token": resp["Parameter"]["Value"]}
    except ClientError as e:
        if e.response["Error"]["Code"] != "ParameterNotFound":
            raise
    new_token = secrets.token_urlsafe(32)
    ssm.put_parameter(
        Name=ADMIN_TOKEN_SSM, Value=new_token,
        Type="SecureString", Overwrite=False,
        Description="Admin token for justhodl-api-keys-admin Lambda. Manage via the Function URL with this in 'Authorization: Bearer <token>'. Rotate via SSM put_parameter.",
    )
    return {"status": "created", "token": new_token}


def build_zip():
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for root, _d, files in os.walk(SOURCE_DIR):
            for fn in files:
                if fn.endswith(".pyc") or "__pycache__" in root:
                    continue
                fp = os.path.join(root, fn)
                zf.write(fp, os.path.relpath(fp, SOURCE_DIR))
    return buf.getvalue()


def deploy_lambda(zip_bytes):
    env_vars = {
        "JUSTHODL_API_KEYS_TABLE": KEYS_TABLE,
        "ADMIN_TOKEN_SSM": ADMIN_TOKEN_SSM,
    }
    try:
        lam.get_function(FunctionName=LAMBDA_NAME)
        # Update existing
        lam.update_function_code(FunctionName=LAMBDA_NAME, ZipFile=zip_bytes)
        lam.get_waiter("function_updated").wait(FunctionName=LAMBDA_NAME)
        lam.update_function_configuration(
            FunctionName=LAMBDA_NAME,
            Runtime="python3.12",
            Handler="lambda_function.lambda_handler",
            MemorySize=256,
            Timeout=30,
            Environment={"Variables": env_vars},
            Role=ROLE_ARN,
        )
        lam.get_waiter("function_updated").wait(FunctionName=LAMBDA_NAME)
        return {"action": "updated"}
    except ClientError as e:
        if e.response["Error"]["Code"] != "ResourceNotFoundException":
            raise
    # Create
    lam.create_function(
        FunctionName=LAMBDA_NAME,
        Runtime="python3.12",
        Handler="lambda_function.lambda_handler",
        Role=ROLE_ARN,
        MemorySize=256,
        Timeout=30,
        Code={"ZipFile": zip_bytes},
        Environment={"Variables": env_vars},
        Description="Admin Lambda for managing Public API keys: create, revoke, list, rotate.",
    )
    lam.get_waiter("function_active_v2").wait(FunctionName=LAMBDA_NAME)
    return {"action": "created"}


def ensure_function_url():
    """Create or fetch the Function URL. AuthType=NONE because auth is
    handled inside the Lambda (admin token). Reserved concurrency=2
    so even if someone DDoSes the URL they can't bring down the rest."""
    try:
        resp = lam.get_function_url_config(FunctionName=LAMBDA_NAME)
        return {"action": "already_exists", "url": resp["FunctionUrl"]}
    except ClientError as e:
        if e.response["Error"]["Code"] != "ResourceNotFoundException":
            raise
    resp = lam.create_function_url_config(
        FunctionName=LAMBDA_NAME,
        AuthType="NONE",
        Cors={
            "AllowMethods": ["POST", "OPTIONS"],
            "AllowHeaders": ["Content-Type", "Authorization"],
            "AllowOrigins": ["*"],
            "MaxAge": 300,
        },
    )
    # Add resource policy so Function URL is publicly invocable
    lam.add_permission(
        FunctionName=LAMBDA_NAME,
        StatementId="FunctionURLAllowPublicAccess",
        Action="lambda:InvokeFunctionUrl",
        Principal="*",
        FunctionUrlAuthType="NONE",
    )
    # Reserved concurrency
    try:
        lam.put_function_concurrency(
            FunctionName=LAMBDA_NAME,
            ReservedConcurrentExecutions=2,
        )
    except Exception:
        pass
    return {"action": "created", "url": resp["FunctionUrl"]}


def smoke_test(url, admin_token):
    """End-to-end test: create a FREE key, list keys, revoke, list again."""
    out = {}

    # Create a test key
    body = json.dumps({
        "action": "create",
        "tier": "FREE",
        "owner_email": "test@justhodl.ai",
        "label": "smoke-test-287",
    }).encode()
    req = urllib.request.Request(
        url, data=body, method="POST",
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {admin_token}",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=15) as r:
            out["create"] = {"status": r.status, "body": json.loads(r.read())}
    except urllib.error.HTTPError as e:
        out["create"] = {"status": e.code, "body": e.read().decode()[:500]}
    except Exception as e:
        out["create_err"] = str(e)[:300]
        return out

    test_key_hash = out["create"]["body"].get("key_hash") if isinstance(out["create"]["body"], dict) else None
    test_plain_key = out["create"]["body"].get("key") if isinstance(out["create"]["body"], dict) else None

    # List keys
    body = json.dumps({"action": "list"}).encode()
    req = urllib.request.Request(
        url, data=body, method="POST",
        headers={"Content-Type": "application/json",
                 "Authorization": f"Bearer {admin_token}"},
    )
    try:
        with urllib.request.urlopen(req, timeout=15) as r:
            list_resp = json.loads(r.read())
            out["list"] = {"status": r.status, "count": list_resp.get("count")}
    except Exception as e:
        out["list_err"] = str(e)[:300]

    # Revoke the test key
    if test_key_hash:
        body = json.dumps({"action": "revoke", "key_hash": test_key_hash}).encode()
        req = urllib.request.Request(
            url, data=body, method="POST",
            headers={"Content-Type": "application/json",
                     "Authorization": f"Bearer {admin_token}"},
        )
        try:
            with urllib.request.urlopen(req, timeout=15) as r:
                out["revoke"] = {"status": r.status, "body": json.loads(r.read())}
        except Exception as e:
            out["revoke_err"] = str(e)[:300]

    # Bad token check (should 403)
    req = urllib.request.Request(
        url, data=json.dumps({"action": "list"}).encode(), method="POST",
        headers={"Content-Type": "application/json",
                 "Authorization": "Bearer wrong_token"},
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            out["bad_token_check"] = {"status": r.status, "expected": 403}
    except urllib.error.HTTPError as e:
        out["bad_token_check"] = {"status": e.code, "expected": 403, "ok": e.code == 403}

    # Capture the test key hash so we can confirm cleanup
    out["test_key_hash"] = test_key_hash
    out["test_key_visible_only_here"] = (test_plain_key[:12] + "...") if test_plain_key else None
    return out


def main():
    started = time.time()
    out = {"started": datetime.now(timezone.utc).isoformat()}
    try:
        out["iam"] = ensure_iam_policy()
        time.sleep(2)

        token_info = ensure_admin_token()
        # Don't echo full token on update; only on first creation
        out["admin_token"] = {
            "ssm_path": ADMIN_TOKEN_SSM,
            "status": token_info["status"],
            "token_first_8": token_info["token"][:8],
            "token_for_save_now": token_info["token"] if token_info["status"] == "created" else "(already exists; retrieve via SSM)",
        }
        admin_token = token_info["token"]

        zip_bytes = build_zip()
        out["zip_size_bytes"] = len(zip_bytes)
        out["deploy"] = deploy_lambda(zip_bytes)

        out["function_url"] = ensure_function_url()

        # Wait for IAM propagation
        time.sleep(5)

        url = out["function_url"]["url"]
        out["smoke_test"] = smoke_test(url, admin_token)

        out["duration_s"] = round(time.time() - started, 1)

    except Exception as e:
        import traceback
        out["fatal_error"] = str(e)
        out["traceback"] = traceback.format_exc()

    # Don't write the admin token to the report (only print to logs)
    safe_out = json.loads(json.dumps(out, default=str))
    if isinstance(safe_out.get("admin_token"), dict):
        safe_out["admin_token"]["token_for_save_now"] = "(redacted in report — see workflow logs)"

    os.makedirs(os.path.dirname(REPORT_PATH), exist_ok=True)
    with open(REPORT_PATH, "w") as f:
        json.dump(safe_out, f, indent=2, default=str)

    # Print full out (with token) to logs only — visible in GH Actions but not committed
    print(json.dumps(out, indent=2, default=str)[:6000])
    return 0 if "fatal_error" not in out else 1


if __name__ == "__main__":
    raise SystemExit(main())
