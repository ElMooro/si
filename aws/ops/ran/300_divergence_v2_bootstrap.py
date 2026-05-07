#!/usr/bin/env python3
"""Step 300 — Bootstrap justhodl-divergence-engine-v2.

Creates the Lambda + EventBridge rule + invokes once for smoke test.
The Lambda ADDS to the existing divergence stack — does not modify
v1 (justhodl-divergence-scanner).

After this:
  - 32 new divergence pairs monitored every 2h
  - Output at s3://justhodl-dashboard-live/data/divergence-v2.json
  - Telegram alerts on extreme (>3σ) divergences only
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
LAMBDA_NAME = "justhodl-divergence-engine-v2"
SOURCE_DIR = f"aws/lambdas/{LAMBDA_NAME}/source"
ROLE_ARN = f"arn:aws:iam::{ACCOUNT_ID}:role/lambda-execution-role"
RULE_NAME = "divergence-v2-2hourly"
SCHEDULE = "rate(2 hours)"
REPORT_PATH = "aws/ops/reports/300_divergence_v2_bootstrap.json"

lam = boto3.client("lambda", region_name=REGION)
events = boto3.client("events", region_name=REGION)


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
        "S3_BUCKET": "justhodl-dashboard-live",
        "S3_KEY": "data/divergence-v2.json",
        "FRED_KEY": "2f057499936072679d8843d7fce99989",
        "FMP_KEY": "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb",
        "TIMEOUT_BUDGET_S": "260",
    }
    try:
        lam.get_function(FunctionName=LAMBDA_NAME)
        lam.update_function_code(FunctionName=LAMBDA_NAME, ZipFile=zip_bytes)
        lam.get_waiter("function_updated").wait(FunctionName=LAMBDA_NAME)
        lam.update_function_configuration(
            FunctionName=LAMBDA_NAME,
            Runtime="python3.12",
            Handler="lambda_function.lambda_handler",
            MemorySize=512, Timeout=300,
            Environment={"Variables": env_vars},
            Role=ROLE_ARN,
        )
        lam.get_waiter("function_updated").wait(FunctionName=LAMBDA_NAME)
        return {"action": "updated", "zip_bytes": len(zip_bytes)}
    except ClientError as e:
        if e.response["Error"]["Code"] != "ResourceNotFoundException":
            raise
    lam.create_function(
        FunctionName=LAMBDA_NAME,
        Runtime="python3.12",
        Handler="lambda_function.lambda_handler",
        Role=ROLE_ARN,
        MemorySize=512, Timeout=300,
        Code={"ZipFile": zip_bytes},
        Environment={"Variables": env_vars},
        Description="Phase 2 cross-asset divergence engine — 32 pairs covering labor, manufacturing, LEI, EM/frontier, micro caps, copper, eurodollar centers.",
    )
    lam.get_waiter("function_active_v2").wait(FunctionName=LAMBDA_NAME)
    return {"action": "created", "zip_bytes": len(zip_bytes)}


def ensure_eb_rule():
    """Create or update EventBridge rule + add Lambda target + permission."""
    try:
        events.describe_rule(Name=RULE_NAME)
        events.put_rule(Name=RULE_NAME, ScheduleExpression=SCHEDULE,
                        State="ENABLED",
                        Description="Run divergence-engine-v2 every 2 hours")
        rule_action = "updated"
    except ClientError as e:
        if e.response["Error"]["Code"] != "ResourceNotFoundException":
            raise
        events.put_rule(Name=RULE_NAME, ScheduleExpression=SCHEDULE,
                        State="ENABLED",
                        Description="Run divergence-engine-v2 every 2 hours")
        rule_action = "created"

    # Add Lambda as target
    events.put_targets(
        Rule=RULE_NAME,
        Targets=[{
            "Id": "1",
            "Arn": f"arn:aws:lambda:{REGION}:{ACCOUNT_ID}:function:{LAMBDA_NAME}",
        }],
    )

    # Grant EventBridge permission to invoke
    try:
        lam.add_permission(
            FunctionName=LAMBDA_NAME,
            StatementId=f"{RULE_NAME}-invoke",
            Action="lambda:InvokeFunction",
            Principal="events.amazonaws.com",
            SourceArn=f"arn:aws:events:{REGION}:{ACCOUNT_ID}:rule/{RULE_NAME}",
        )
    except ClientError as e:
        if e.response["Error"]["Code"] != "ResourceConflictException":
            raise

    return {"action": rule_action, "schedule": SCHEDULE, "rule_name": RULE_NAME}


def smoke_test():
    """Synchronously invoke the Lambda once to verify it works end-to-end."""
    print("[300] Smoke test — synchronous invoke…")
    resp = lam.invoke(
        FunctionName=LAMBDA_NAME,
        InvocationType="RequestResponse",
        LogType="Tail",
    )
    payload = resp["Payload"].read().decode("utf-8")
    try:
        body = json.loads(payload)
    except Exception:
        body = payload[:500]
    return {
        "status_code": resp.get("StatusCode"),
        "executed_version": resp.get("ExecutedVersion"),
        "function_error": resp.get("FunctionError"),
        "response": body,
    }


def main():
    started = time.time()
    out = {"started": datetime.now(timezone.utc).isoformat()}
    try:
        zip_bytes = build_zip()
        out["deploy"] = deploy_lambda(zip_bytes)
        time.sleep(3)  # brief settle
        out["eb_rule"] = ensure_eb_rule()
        time.sleep(2)
        out["smoke_test"] = smoke_test()
        out["duration_s"] = round(time.time() - started, 1)
    except Exception as e:
        import traceback
        out["fatal_error"] = str(e)
        out["traceback"] = traceback.format_exc()

    os.makedirs(os.path.dirname(REPORT_PATH), exist_ok=True)
    with open(REPORT_PATH, "w") as f:
        json.dump(out, f, indent=2, default=str)
    print(json.dumps(out, indent=2, default=str)[:5000])
    return 0 if "fatal_error" not in out else 1


if __name__ == "__main__":
    raise SystemExit(main())
