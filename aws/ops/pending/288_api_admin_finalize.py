#!/usr/bin/env python3
"""Step 288 — Recover from step 287 partial failure.

Step 287 issues:
  1. PITR on keys table couldn't be enabled (table was still CREATING)
  2. Function URL CORS rejected 'OPTIONS' (AWS limit: ≤6 chars per
     allowed method; preflight is auto-handled by AWS anyway)

This step:
  1. Retry PITR enable on justhodl-api-keys
  2. Create Function URL with corrected CORS (only POST, no OPTIONS)
  3. Add resource policy for public InvokeFunctionUrl
  4. Run the end-to-end smoke test
"""
import json
import os
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone

import boto3
from botocore.exceptions import ClientError

REGION = "us-east-1"
ACCOUNT_ID = "857687956942"
LAMBDA_NAME = "justhodl-api-keys-admin"
KEYS_TABLE = "justhodl-api-keys"
ADMIN_TOKEN_SSM = "/justhodl/api-admin/token"
REPORT_PATH = "aws/ops/reports/288_api_admin_finalize.json"

ddb = boto3.client("dynamodb", region_name=REGION)
lam = boto3.client("lambda", region_name=REGION)
ssm = boto3.client("ssm", region_name=REGION)


def main():
    started = time.time()
    out = {"started": datetime.now(timezone.utc).isoformat()}
    try:
        # 1. Retry PITR enable
        try:
            ddb.update_continuous_backups(
                TableName=KEYS_TABLE,
                PointInTimeRecoverySpecification={"PointInTimeRecoveryEnabled": True},
            )
            out["pitr"] = {"status": "enabled"}
        except ClientError as e:
            if "already enabled" in str(e).lower() or "EnabledTrue" in str(e):
                out["pitr"] = {"status": "already_enabled"}
            else:
                out["pitr"] = {"status": "err", "err": str(e)[:200]}

        # 2. Create Function URL — corrected CORS
        try:
            existing = lam.get_function_url_config(FunctionName=LAMBDA_NAME)
            url = existing["FunctionUrl"]
            out["function_url"] = {"action": "already_exists", "url": url}
        except ClientError as e:
            if e.response["Error"]["Code"] != "ResourceNotFoundException":
                raise
            resp = lam.create_function_url_config(
                FunctionName=LAMBDA_NAME,
                AuthType="NONE",
                Cors={
                    # Only actual HTTP methods used; preflight OPTIONS is
                    # auto-handled by AWS based on these.
                    "AllowMethods": ["POST"],
                    "AllowHeaders": ["Content-Type", "Authorization"],
                    "AllowOrigins": ["*"],
                    "MaxAge": 300,
                },
            )
            url = resp["FunctionUrl"]
            out["function_url"] = {"action": "created", "url": url}

            # Resource policy for public access
            try:
                lam.add_permission(
                    FunctionName=LAMBDA_NAME,
                    StatementId="FunctionURLAllowPublicAccess",
                    Action="lambda:InvokeFunctionUrl",
                    Principal="*",
                    FunctionUrlAuthType="NONE",
                )
                out["function_url"]["resource_policy"] = "added"
            except ClientError as e:
                if e.response["Error"]["Code"] == "ResourceConflictException":
                    out["function_url"]["resource_policy"] = "already_exists"
                else:
                    raise

            # Reserved concurrency
            try:
                lam.put_function_concurrency(
                    FunctionName=LAMBDA_NAME,
                    ReservedConcurrentExecutions=2,
                )
                out["reserved_concurrency"] = 2
            except Exception as e:
                out["reserved_concurrency_err"] = str(e)[:200]

        # 3. Pull the admin token and run smoke test
        admin_token = ssm.get_parameter(
            Name=ADMIN_TOKEN_SSM, WithDecryption=True
        )["Parameter"]["Value"]
        out["admin_token_first_12"] = admin_token[:12] + "..."

        # Wait for Function URL DNS + IAM to propagate
        time.sleep(5)

        smoke = {}

        # Test 1: create FREE key
        body = json.dumps({
            "action": "create", "tier": "FREE",
            "owner_email": "test@justhodl.ai", "label": "smoke-test-288",
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
                smoke["create"] = {"status": r.status, "body": json.loads(r.read())}
        except urllib.error.HTTPError as e:
            smoke["create"] = {"status": e.code, "body": e.read().decode()[:500]}

        test_key_hash = None
        test_plain = None
        if smoke.get("create", {}).get("status") == 200:
            cb = smoke["create"]["body"]
            if isinstance(cb, dict):
                test_key_hash = cb.get("key_hash")
                test_plain = cb.get("key")

        # Test 2: list keys
        body = json.dumps({"action": "list"}).encode()
        req = urllib.request.Request(
            url, data=body, method="POST",
            headers={"Content-Type": "application/json",
                     "Authorization": f"Bearer {admin_token}"},
        )
        try:
            with urllib.request.urlopen(req, timeout=15) as r:
                rb = json.loads(r.read())
                smoke["list"] = {"status": r.status, "count": rb.get("count")}
        except Exception as e:
            smoke["list_err"] = str(e)[:200]

        # Test 3: revoke
        if test_key_hash:
            body = json.dumps({"action": "revoke", "key_hash": test_key_hash}).encode()
            req = urllib.request.Request(
                url, data=body, method="POST",
                headers={"Content-Type": "application/json",
                         "Authorization": f"Bearer {admin_token}"},
            )
            try:
                with urllib.request.urlopen(req, timeout=15) as r:
                    smoke["revoke"] = {"status": r.status}
            except Exception as e:
                smoke["revoke_err"] = str(e)[:200]

        # Test 4: bad token rejection
        req = urllib.request.Request(
            url, data=json.dumps({"action": "list"}).encode(), method="POST",
            headers={"Content-Type": "application/json",
                     "Authorization": "Bearer wrong_token_xyz"},
        )
        try:
            with urllib.request.urlopen(req, timeout=10) as r:
                smoke["bad_token_check"] = {"status": r.status, "expected": 403,
                                              "ok": False}
        except urllib.error.HTTPError as e:
            smoke["bad_token_check"] = {"status": e.code, "expected": 403,
                                          "ok": e.code == 403}

        # Test 5: missing token rejection
        req = urllib.request.Request(
            url, data=json.dumps({"action": "list"}).encode(), method="POST",
            headers={"Content-Type": "application/json"},
        )
        try:
            with urllib.request.urlopen(req, timeout=10) as r:
                smoke["no_token_check"] = {"status": r.status, "expected": 401,
                                             "ok": False}
        except urllib.error.HTTPError as e:
            smoke["no_token_check"] = {"status": e.code, "expected": 401,
                                         "ok": e.code == 401}

        smoke["test_key_hash"] = test_key_hash
        smoke["test_plain_first_12"] = test_plain[:12] + "..." if test_plain else None
        out["smoke_test"] = smoke

        out["duration_s"] = round(time.time() - started, 1)

    except Exception as e:
        import traceback
        out["fatal_error"] = str(e)
        out["traceback"] = traceback.format_exc()

    os.makedirs(os.path.dirname(REPORT_PATH), exist_ok=True)
    with open(REPORT_PATH, "w") as f:
        json.dump(out, f, indent=2, default=str)
    print(json.dumps(out, indent=2, default=str)[:4000])
    return 0 if "fatal_error" not in out else 1


if __name__ == "__main__":
    raise SystemExit(main())
