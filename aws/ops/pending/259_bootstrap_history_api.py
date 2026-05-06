#!/usr/bin/env python3
"""Step 259 — Bootstrap justhodl-history-api with public Function URL.

  1. Create Lambda (or update existing) with role lambda-execution-role
  2. Set reserved concurrency to 5 (rate-limit abuse)
  3. Create Function URL with NONE auth + CORS
  4. Test invocations: GET /, GET /snapshot, GET /latest, GET /timestamps
  5. Persist Function URL to SSM /justhodl/history-api/url so audit.html
     can fetch it dynamically (no hard-coded secrets in HTML)
  6. Write report to aws/ops/reports/259_history_api_bootstrap.json
"""
import io
import json
import os
import time
import zipfile
from datetime import datetime, timezone

import boto3
from botocore.exceptions import ClientError

REGION = "us-east-1"
ACCOUNT_ID = "857687956942"
BUCKET = "justhodl-dashboard-live"
LAMBDA_NAME = "justhodl-history-api"
ROLE_ARN = f"arn:aws:iam::{ACCOUNT_ID}:role/lambda-execution-role"
SOURCE_DIR = f"aws/lambdas/{LAMBDA_NAME}/source"
SSM_URL_PARAM = "/justhodl/history-api/url"
REPORT_PATH = "aws/ops/reports/259_history_api_bootstrap.json"

lam = boto3.client("lambda", region_name=REGION)
ssm = boto3.client("ssm", region_name=REGION)


def build_zip():
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for root, _dirs, files in os.walk(SOURCE_DIR):
            for fn in files:
                fpath = os.path.join(root, fn)
                arcname = os.path.relpath(fpath, SOURCE_DIR)
                zf.write(fpath, arcname)
    return buf.getvalue()


def ensure_lambda():
    zip_bytes = build_zip()
    env = {"S3_BUCKET": BUCKET, "DDB_TABLE": "justhodl-history"}
    try:
        cur = lam.get_function(FunctionName=LAMBDA_NAME)
        print(f"[259] Lambda exists, updating ({len(zip_bytes):,}b)")
        lam.update_function_code(FunctionName=LAMBDA_NAME, ZipFile=zip_bytes, Publish=False)
        lam.get_waiter("function_updated").wait(FunctionName=LAMBDA_NAME)
        lam.update_function_configuration(
            FunctionName=LAMBDA_NAME,
            Runtime="python3.12",
            Handler="lambda_function.lambda_handler",
            MemorySize=256,
            Timeout=15,
            Environment={"Variables": env},
        )
        lam.get_waiter("function_updated").wait(FunctionName=LAMBDA_NAME)
        return {"created": False, "arn": cur["Configuration"]["FunctionArn"]}
    except ClientError as e:
        if e.response["Error"]["Code"] != "ResourceNotFoundException":
            raise
    print(f"[259] creating Lambda {LAMBDA_NAME}…")
    resp = lam.create_function(
        FunctionName=LAMBDA_NAME,
        Runtime="python3.12",
        Handler="lambda_function.lambda_handler",
        Role=ROLE_ARN,
        Code={"ZipFile": zip_bytes},
        Description=("Read-only API for justhodl-history DDB. Function URL "
                     "exposes /index, /snapshot, /latest, /timestamps. "
                     "Reserved concurrency=5."),
        MemorySize=256,
        Timeout=15,
        Environment={"Variables": env},
        Tags={"project": "justhodl", "purpose": "history-api"},
    )
    lam.get_waiter("function_active_v2").wait(FunctionName=LAMBDA_NAME)
    return {"created": True, "arn": resp["FunctionArn"]}


def ensure_reserved_concurrency():
    try:
        lam.put_function_concurrency(
            FunctionName=LAMBDA_NAME, ReservedConcurrentExecutions=5,
        )
        return {"reserved_concurrent_executions": 5}
    except Exception as e:
        return {"err": str(e)[:200]}


def ensure_function_url():
    try:
        existing = lam.get_function_url_config(FunctionName=LAMBDA_NAME)
        url = existing["FunctionUrl"]
        print(f"[259] Function URL exists: {url}")
        # Update CORS to be safe
        lam.update_function_url_config(
            FunctionName=LAMBDA_NAME,
            AuthType="NONE",
            Cors={
                "AllowOrigins": ["*"],
                "AllowMethods": ["GET", "OPTIONS"],
                "AllowHeaders": ["Content-Type"],
                "MaxAge": 3600,
            },
        )
        return {"url": url, "created": False}
    except ClientError as e:
        if e.response["Error"]["Code"] != "ResourceNotFoundException":
            raise
    resp = lam.create_function_url_config(
        FunctionName=LAMBDA_NAME,
        AuthType="NONE",
        Cors={
            "AllowOrigins": ["*"],
            "AllowMethods": ["GET", "OPTIONS"],
            "AllowHeaders": ["Content-Type"],
            "MaxAge": 3600,
        },
    )
    # Permission for public invoke
    try:
        lam.add_permission(
            FunctionName=LAMBDA_NAME,
            StatementId="FunctionURLAllowPublicAccess",
            Action="lambda:InvokeFunctionUrl",
            Principal="*",
            FunctionUrlAuthType="NONE",
        )
    except ClientError as e:
        if e.response["Error"]["Code"] != "ResourceConflictException":
            raise
    return {"url": resp["FunctionUrl"], "created": True}


def persist_url_to_ssm(url):
    out = {}
    try:
        ssm.put_parameter(
            Name=SSM_URL_PARAM, Value=url, Type="String", Overwrite=True,
            Description="Function URL for justhodl-history-api — used by /audit.html",
        )
        out["ssm_ok"] = True
    except Exception as e:
        out["ssm_err"] = str(e)[:200]

    # Also publish a small public pointer so the page can discover the
    # URL without an authenticated SSM call.
    try:
        s3 = boto3.client("s3", region_name=REGION)
        s3.put_object(
            Bucket=BUCKET, Key="data/history-api-url.json",
            Body=json.dumps({"url": url, "updated_at": datetime.now(timezone.utc).isoformat()}).encode(),
            ContentType="application/json",
            CacheControl="public, max-age=3600",
        )
        out["s3_pointer_ok"] = True
    except Exception as e:
        out["s3_pointer_err"] = str(e)[:200]
    return out


def smoke_test(url):
    """Hit each endpoint via direct Lambda invoke (since this sandbox can't
    reach the public Function URL host)."""
    cases = [
        ("/", {}),
        ("/timestamps", {"key": "data/report.json", "limit": "5"}),
        ("/latest", {"key": "data/report.json"}),
    ]
    out = []
    for path, qs in cases:
        event = {
            "rawPath": path,
            "rawQueryString": "&".join(f"{k}={v}" for k, v in qs.items()),
            "queryStringParameters": qs,
            "requestContext": {"http": {"method": "GET"}},
        }
        try:
            r = lam.invoke(FunctionName=LAMBDA_NAME, InvocationType="RequestResponse",
                           Payload=json.dumps(event).encode())
            payload = json.loads(r["Payload"].read())
            body = payload.get("body")
            try:
                parsed = json.loads(body) if isinstance(body, str) else body
            except Exception:
                parsed = body
            # Trim large payloads
            if isinstance(parsed, dict):
                trimmed = {k: v for k, v in parsed.items() if k != "feeds"}
                if "feeds" in parsed:
                    trimmed["n_feeds"] = parsed.get("n_feeds")
                    trimmed["feeds_sample"] = (parsed.get("feeds") or [])[:3]
                # Keep timestamps but trim
                if "timestamps" in trimmed and len(trimmed["timestamps"]) > 5:
                    trimmed["timestamps"] = trimmed["timestamps"][:5]
            else:
                trimmed = parsed
            out.append({
                "path": path, "qs": qs,
                "status": payload.get("statusCode"),
                "body": trimmed,
            })
        except Exception as e:
            out.append({"path": path, "qs": qs, "err": str(e)[:200]})
    return out


def main():
    started = time.time()
    out = {"started": datetime.now(timezone.utc).isoformat()}
    try:
        out["lambda"] = ensure_lambda()
        out["concurrency"] = ensure_reserved_concurrency()
        url_info = ensure_function_url()
        out["function_url"] = url_info
        out["ssm"] = persist_url_to_ssm(url_info["url"])
        time.sleep(2)
        out["smoke_test"] = smoke_test(url_info["url"])
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
