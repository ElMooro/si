#!/usr/bin/env python3
"""Step 289 — Bootstrap the public-api-demo Lambda + end-to-end PoC test.

This proves the full auth tier system works end-to-end:

  1. Create the demo Lambda
  2. Create its Function URL
  3. Issue a fresh FREE-tier key via the admin Lambda
  4. Call the demo with that key — should get 200 with tier info
  5. Call the demo with no key — should get 401
  6. Call the demo with bad key — should get 401
  7. Hammer the demo with valid key 110 times — should hit 429 after 100
     (FREE tier hourly limit)
  8. Revoke the key
  9. Call again with the now-revoked key — should get 403

If all 9 steps pass, Phase 1 is verified end-to-end.
"""
import io
import json
import os
import time
import urllib.error
import urllib.request
import zipfile
from datetime import datetime, timezone

import boto3
from botocore.exceptions import ClientError

REGION = "us-east-1"
ACCOUNT_ID = "857687956942"
LAMBDA_NAME = "justhodl-public-api-demo"
SOURCE_DIR = f"aws/lambdas/{LAMBDA_NAME}/source"
ROLE_ARN = f"arn:aws:iam::{ACCOUNT_ID}:role/lambda-execution-role"
ADMIN_LAMBDA = "justhodl-api-keys-admin"
ADMIN_TOKEN_SSM = "/justhodl/api-admin/token"
KEYS_TABLE = "justhodl-api-keys"
RATE_TABLE = "justhodl-api-rate"
REPORT_PATH = "aws/ops/reports/289_demo_e2e_test.json"

lam = boto3.client("lambda", region_name=REGION)
ssm = boto3.client("ssm", region_name=REGION)


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


def deploy_demo():
    env_vars = {
        "JUSTHODL_API_KEYS_TABLE": KEYS_TABLE,
        "JUSTHODL_API_RATE_TABLE": RATE_TABLE,
    }
    zip_bytes = build_zip()
    try:
        lam.get_function(FunctionName=LAMBDA_NAME)
        # Update
        lam.update_function_code(FunctionName=LAMBDA_NAME, ZipFile=zip_bytes)
        lam.get_waiter("function_updated").wait(FunctionName=LAMBDA_NAME)
        lam.update_function_configuration(
            FunctionName=LAMBDA_NAME,
            Runtime="python3.12",
            Handler="lambda_function.lambda_handler",
            MemorySize=256, Timeout=15,
            Environment={"Variables": env_vars},
            Role=ROLE_ARN,
        )
        lam.get_waiter("function_updated").wait(FunctionName=LAMBDA_NAME)
        return {"action": "updated", "zip_bytes": len(zip_bytes)}
    except ClientError as e:
        if e.response["Error"]["Code"] != "ResourceNotFoundException":
            raise
    lam.create_function(
        FunctionName=LAMBDA_NAME, Runtime="python3.12",
        Handler="lambda_function.lambda_handler", Role=ROLE_ARN,
        MemorySize=256, Timeout=15,
        Code={"ZipFile": zip_bytes},
        Environment={"Variables": env_vars},
        Description="Reference public API endpoint demonstrating api_auth.py.",
    )
    lam.get_waiter("function_active_v2").wait(FunctionName=LAMBDA_NAME)
    return {"action": "created", "zip_bytes": len(zip_bytes)}


def ensure_demo_url():
    try:
        r = lam.get_function_url_config(FunctionName=LAMBDA_NAME)
        return {"action": "already_exists", "url": r["FunctionUrl"]}
    except ClientError as e:
        if e.response["Error"]["Code"] != "ResourceNotFoundException":
            raise
    r = lam.create_function_url_config(
        FunctionName=LAMBDA_NAME, AuthType="NONE",
        Cors={
            "AllowMethods": ["GET", "POST"],   # OPTIONS auto-handled
            "AllowHeaders": ["Content-Type", "Authorization", "x-api-key"],
            "AllowOrigins": ["*"],
            "MaxAge": 300,
        },
    )
    try:
        lam.add_permission(
            FunctionName=LAMBDA_NAME,
            StatementId="FunctionURLAllowPublicAccess",
            Action="lambda:InvokeFunctionUrl",
            Principal="*", FunctionUrlAuthType="NONE",
        )
    except ClientError as e:
        if e.response["Error"]["Code"] != "ResourceConflictException":
            raise
    try:
        lam.put_function_concurrency(
            FunctionName=LAMBDA_NAME, ReservedConcurrentExecutions=5,
        )
    except Exception:
        pass
    return {"action": "created", "url": r["FunctionUrl"]}


def call(url, headers=None, body=None, timeout=10):
    """HTTP request returning (status, body_dict_or_str)."""
    data = json.dumps(body).encode() if body is not None else None
    method = "POST" if body is not None else "GET"
    req = urllib.request.Request(url, data=data, method=method,
                                  headers=headers or {})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            text = r.read().decode()
            try:
                return r.status, json.loads(text)
            except Exception:
                return r.status, text[:200]
    except urllib.error.HTTPError as e:
        text = e.read().decode()
        try:
            return e.code, json.loads(text)
        except Exception:
            return e.code, text[:200]


def main():
    started = time.time()
    out = {"started": datetime.now(timezone.utc).isoformat()}
    try:
        # 1+2. Deploy demo Lambda + Function URL
        out["deploy"] = deploy_demo()
        out["url"] = ensure_demo_url()
        demo_url = out["url"]["url"]
        time.sleep(5)   # IAM + URL DNS propagation

        # 3. Get admin token + admin URL
        admin_token = ssm.get_parameter(
            Name=ADMIN_TOKEN_SSM, WithDecryption=True
        )["Parameter"]["Value"]
        admin_url = lam.get_function_url_config(
            FunctionName=ADMIN_LAMBDA
        )["FunctionUrl"]

        # 4. Issue a fresh FREE-tier key via admin
        admin_status, admin_body = call(
            admin_url,
            headers={"Content-Type": "application/json",
                     "Authorization": f"Bearer {admin_token}"},
            body={"action": "create", "tier": "FREE",
                  "owner_email": "phase1-poc@justhodl.ai",
                  "label": "phase1-e2e-test"},
        )
        out["issue_key"] = {"status": admin_status,
                            "tier": admin_body.get("tier") if isinstance(admin_body, dict) else None,
                            "key_hash": admin_body.get("key_hash") if isinstance(admin_body, dict) else None}
        if admin_status != 200 or not isinstance(admin_body, dict):
            out["fatal_error"] = "key issuance failed"
            raise SystemExit(1)
        plain_key = admin_body["key"]
        key_hash = admin_body["key_hash"]
        out["test_key_first_12"] = plain_key[:12] + "..."

        # 5. Call demo with valid key — expect 200
        time.sleep(2)
        s, b = call(demo_url,
                    headers={"Authorization": f"Bearer {plain_key}"})
        out["call_with_key"] = {"status": s,
                                "tier": b.get("tier") if isinstance(b, dict) else None,
                                "owner": b.get("owner_email") if isinstance(b, dict) else None}

        # 6. Call demo with no key — expect 401
        s, b = call(demo_url)
        out["call_no_key"] = {"status": s, "expected": 401, "ok": s == 401,
                              "error_code": b.get("error") if isinstance(b, dict) else None}

        # 7. Call demo with bad key — expect 401
        s, b = call(demo_url, headers={"Authorization": "Bearer jhd_bogus"})
        out["call_bad_key"] = {"status": s, "expected": 401, "ok": s == 401,
                               "error_code": b.get("error") if isinstance(b, dict) else None}

        # 8. Hammer the demo to test rate limit
        # FREE tier: 100/hr. We just used 1 above. Hit 100 more — 99 should
        # succeed and the 100th should hit 429. Use sleep between bursts to
        # avoid the per_sec=5 burst limit.
        # Save time by only hitting up to the rate limit, not 110 times.
        rate_results = {"successes": 0, "first_429_at": None,
                        "429_status": None, "429_error_code": None}
        for i in range(110):
            if i % 4 == 0 and i > 0:
                time.sleep(1)   # avoid 5/sec burst limit
            s, b = call(demo_url,
                        headers={"Authorization": f"Bearer {plain_key}"})
            if s == 200:
                rate_results["successes"] += 1
            elif s == 429:
                rate_results["first_429_at"] = i + 2  # +2 because we already did call #1 (issue)+ #2 (call_with_key)
                rate_results["429_status"] = s
                rate_results["429_error_code"] = b.get("error") if isinstance(b, dict) else None
                rate_results["429_body"] = b
                break
            else:
                rate_results["unexpected_status"] = s
                rate_results["unexpected_body"] = b
                break
        out["rate_limit_test"] = rate_results

        # 9. Revoke the key
        s, b = call(admin_url,
                    headers={"Content-Type": "application/json",
                             "Authorization": f"Bearer {admin_token}"},
                    body={"action": "revoke", "key_hash": key_hash})
        out["revoke"] = {"status": s,
                         "revoked_at": b.get("revoked_at") if isinstance(b, dict) else None}

        # 10. Call again with revoked key — expect 403
        time.sleep(1)
        s, b = call(demo_url,
                    headers={"Authorization": f"Bearer {plain_key}"})
        out["call_after_revoke"] = {"status": s, "expected": 403, "ok": s == 403,
                                    "error_code": b.get("error") if isinstance(b, dict) else None}

        out["duration_s"] = round(time.time() - started, 1)

        # Overall pass/fail
        out["all_passed"] = (
            out.get("call_with_key", {}).get("status") == 200
            and out.get("call_no_key", {}).get("ok") is True
            and out.get("call_bad_key", {}).get("ok") is True
            and out.get("rate_limit_test", {}).get("first_429_at") is not None
            and out.get("call_after_revoke", {}).get("ok") is True
        )

    except Exception as e:
        import traceback
        out["fatal_error"] = str(e)
        out["traceback"] = traceback.format_exc()

    os.makedirs(os.path.dirname(REPORT_PATH), exist_ok=True)
    with open(REPORT_PATH, "w") as f:
        json.dump(out, f, indent=2, default=str)
    print(json.dumps(out, indent=2, default=str)[:5000])
    return 0 if out.get("all_passed") else 1


if __name__ == "__main__":
    raise SystemExit(main())
