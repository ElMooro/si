#!/usr/bin/env python3
"""Step 270 — Direct deploy of justhodl-macro-nowcast (bypass deploy-lambdas.yml).

Steps 266-268 all waited for deploy-lambdas.yml to create the Lambda but
it never did — possibly the path matcher or the workflow itself didn't
fire. This script does the deployment directly via boto3 from the
ops runner (which already has AWS perms via OIDC).

  1. Build zip from aws/lambdas/justhodl-macro-nowcast/source/
  2. Create Lambda (or update if it somehow exists)
  3. Wait for Active state
  4. Create EB rule justhodl-macro-nowcast-6h with rate(6 hours)
  5. Add EB invocation permission + target
  6. Sync invoke + verify S3 output
  7. Persist report
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
LAMBDA_NAME = "justhodl-macro-nowcast"
RULE_NAME = "justhodl-macro-nowcast-6h"
ROLE_ARN = f"arn:aws:iam::{ACCOUNT_ID}:role/lambda-execution-role"
SOURCE_DIR = f"aws/lambdas/{LAMBDA_NAME}/source"
BUCKET = "justhodl-dashboard-live"
OUTPUT_KEY = "data/macro-nowcast.json"
REPORT_PATH = "aws/ops/reports/270_macro_nowcast_v2.json"

lam = boto3.client("lambda", region_name=REGION)
eb = boto3.client("events", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)


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


def deploy_lambda():
    zip_bytes = build_zip()
    env = {"S3_BUCKET": BUCKET}

    try:
        lam.get_function(FunctionName=LAMBDA_NAME)
        print(f"[270] Lambda exists, updating ({len(zip_bytes):,}b)")
        lam.update_function_code(FunctionName=LAMBDA_NAME, ZipFile=zip_bytes, Publish=False)
        lam.get_waiter("function_updated").wait(FunctionName=LAMBDA_NAME)
        lam.update_function_configuration(
            FunctionName=LAMBDA_NAME,
            Runtime="python3.12",
            Handler="lambda_function.lambda_handler",
            MemorySize=256,
            Timeout=60,
            Environment={"Variables": env},
        )
        lam.get_waiter("function_updated").wait(FunctionName=LAMBDA_NAME)
        return {"created": False, "zip_bytes": len(zip_bytes)}
    except ClientError as e:
        if e.response["Error"]["Code"] != "ResourceNotFoundException":
            raise

    print(f"[270] creating Lambda {LAMBDA_NAME} ({len(zip_bytes):,}b)…")
    lam.create_function(
        FunctionName=LAMBDA_NAME,
        Runtime="python3.12",
        Handler="lambda_function.lambda_handler",
        Role=ROLE_ARN,
        Code={"ZipFile": zip_bytes},
        Description=("Composite real-time macro nowcast — weighted z-score from 7 "
                     "FRED series in data/report.json. Runs every 6h."),
        MemorySize=256,
        Timeout=60,
        Environment={"Variables": env},
        Tags={"project": "justhodl", "purpose": "macro-nowcast"},
    )
    lam.get_waiter("function_active_v2").wait(FunctionName=LAMBDA_NAME)
    return {"created": True, "zip_bytes": len(zip_bytes)}


def ensure_eb_rule():
    rule_arn = f"arn:aws:events:{REGION}:{ACCOUNT_ID}:rule/{RULE_NAME}"
    lambda_arn = f"arn:aws:lambda:{REGION}:{ACCOUNT_ID}:function:{LAMBDA_NAME}"

    eb.put_rule(
        Name=RULE_NAME,
        ScheduleExpression="rate(6 hours)",
        State="ENABLED",
        Description="Compute composite macro nowcast every 6h from data/report.json",
    )

    perm_status = "added"
    try:
        lam.add_permission(
            FunctionName=LAMBDA_NAME,
            StatementId=f"{RULE_NAME}-invoke",
            Action="lambda:InvokeFunction",
            Principal="events.amazonaws.com",
            SourceArn=rule_arn,
        )
    except ClientError as e:
        if e.response["Error"]["Code"] == "ResourceConflictException":
            perm_status = "already_exists"
        else:
            raise

    eb.put_targets(Rule=RULE_NAME, Targets=[{"Id": "1", "Arn": lambda_arn}])
    return {"rule_name": RULE_NAME, "rate": "6 hours", "permission": perm_status}


def invoke_and_verify():
    print(f"[270] sync invoking {LAMBDA_NAME}…")
    started = time.time()
    inv = lam.invoke(FunctionName=LAMBDA_NAME, InvocationType="RequestResponse",
                     Payload=b"{}")
    payload = json.loads(inv["Payload"].read())
    elapsed = round(time.time() - started, 2)
    out = {
        "status": inv.get("StatusCode"),
        "func_err": inv.get("FunctionError"),
        "payload": payload,
        "elapsed_s": elapsed,
    }
    if inv.get("FunctionError"):
        return out

    time.sleep(2)
    try:
        body = json.loads(s3.get_object(Bucket=BUCKET, Key=OUTPUT_KEY)["Body"].read())
        out["output_summary"] = {
            "regime": body.get("regime"),
            "normalized_score": body.get("normalized_score"),
            "raw_score": body.get("raw_score"),
            "coverage_pct": body.get("coverage_pct"),
            "n_components_used": body.get("n_components_used"),
            "n_components_failed": body.get("n_components_failed"),
            "components": [
                {"fred_id": c["fred_id"], "label": c["label"],
                 "z": c.get("z"), "contribution": c.get("contribution"),
                 "raw_value": c.get("raw_value"), "error": c.get("error")}
                for c in (body.get("components") or [])
            ],
        }
    except Exception as e:
        out["output_read_err"] = str(e)[:200]
    return out


def main():
    started = time.time()
    out = {"started": datetime.now(timezone.utc).isoformat()}
    try:
        out["deploy"] = deploy_lambda()
        out["eb_rule"] = ensure_eb_rule()
        out["invoke"] = invoke_and_verify()
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
