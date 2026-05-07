#!/usr/bin/env python3
"""Step 308 — Bootstrap justhodl-sector-tilt (Sprint 5).

Creates the Lambda + EB rule + sync invoke for smoke test.

ZERO-DETERIORATION GUARDRAILS:
  - Does NOT touch sector-rotation, allocator, or any upstream Lambda
  - Reads only data/macro-nowcast.json + data/sector-rotation.json
  - Writes only data/sector-tilt.json (new path)
  - New EventBridge rule, new Lambda role permissions reuse existing IAM
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
LAMBDA_NAME = "justhodl-sector-tilt"
SOURCE_DIR = f"aws/lambdas/{LAMBDA_NAME}/source"
ROLE_ARN = f"arn:aws:iam::{ACCOUNT_ID}:role/lambda-execution-role"
RULE_NAME = "sector-tilt-4hourly"
SCHEDULE = "cron(45 0/4 * * ? *)"  # 45 min past every 4h (after sector-rotation refresh)
REPORT = "aws/ops/reports/308_sector_tilt_bootstrap.json"

lam = boto3.client("lambda", region_name=REGION)
events = boto3.client("events", region_name=REGION)
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


def deploy_lambda(zip_bytes):
    env_vars = {
        "S3_BUCKET": "justhodl-dashboard-live",
        "S3_KEY_OUT": "data/sector-tilt.json",
        "S3_KEY_NOWCAST": "data/macro-nowcast.json",
        "S3_KEY_ROTATION": "data/sector-rotation.json",
    }
    try:
        lam.get_function(FunctionName=LAMBDA_NAME)
        lam.update_function_code(FunctionName=LAMBDA_NAME, ZipFile=zip_bytes)
        lam.get_waiter("function_updated").wait(FunctionName=LAMBDA_NAME)
        lam.update_function_configuration(
            FunctionName=LAMBDA_NAME,
            Runtime="python3.12",
            Handler="lambda_function.lambda_handler",
            MemorySize=128, Timeout=30,
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
        MemorySize=128, Timeout=30,
        Code={"ZipFile": zip_bytes},
        Environment={"Variables": env_vars},
        Description="Macro-Regime → Sector Tilt Engine. Reads macro-nowcast+sector-rotation, applies academic regime→sector playbook, surfaces ALIGNED vs MISALIGNED setups (the alpha).",
    )
    lam.get_waiter("function_active_v2").wait(FunctionName=LAMBDA_NAME)
    return {"action": "created", "zip_bytes": len(zip_bytes)}


def ensure_eb_rule():
    events.put_rule(
        Name=RULE_NAME, ScheduleExpression=SCHEDULE, State="ENABLED",
        Description="Run sector-tilt every 4h, 45min past (after sector-rotation refresh)",
    )
    events.put_targets(
        Rule=RULE_NAME,
        Targets=[{
            "Id": "1",
            "Arn": f"arn:aws:lambda:{REGION}:{ACCOUNT_ID}:function:{LAMBDA_NAME}",
        }],
    )
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


def smoke_test():
    print("[308] Smoke test: sync invoke…")
    resp = lam.invoke(
        FunctionName=LAMBDA_NAME,
        InvocationType="RequestResponse",
        LogType="Tail",
    )
    payload = resp["Payload"].read().decode("utf-8")
    try:
        body = json.loads(payload)
        if isinstance(body.get("body"), str):
            body["body"] = json.loads(body["body"])
    except Exception:
        body = payload[:500]

    # Pull the actual S3 output to confirm
    out_data = None
    try:
        obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="data/sector-tilt.json")
        out_data = json.loads(obj["Body"].read())
    except Exception as e:
        out_data = {"err": str(e)[:200]}

    # Compact summary
    summary = {}
    if isinstance(out_data, dict):
        summary = {
            "regime": out_data.get("regime"),
            "regime_raw": out_data.get("regime_raw"),
            "n_tilts": len(out_data.get("tilts", [])),
            "n_overweight": out_data.get("summary", {}).get("n_overweight"),
            "n_underweight": out_data.get("summary", {}).get("n_underweight"),
            "n_misaligned": out_data.get("summary", {}).get("n_misaligned"),
            "top_buys": out_data.get("summary", {}).get("top_buy_opportunities"),
            "top_fades": out_data.get("summary", {}).get("top_fade_opportunities"),
            "tilt_summary": [
                {
                    "ticker": t["ticker"],
                    "tilt": t["regime_tilt_label"],
                    "current": t["current_state"],
                    "rs_20d": t["rs_20d"],
                    "alignment": t["alignment"],
                    "implication": t["implication"],
                }
                for t in (out_data.get("tilts") or [])
            ],
        }

    return {
        "status_code": resp.get("StatusCode"),
        "function_error": resp.get("FunctionError"),
        "response": body,
        "s3_output_summary": summary,
    }


def main():
    started = time.time()
    out = {"started": datetime.now(timezone.utc).isoformat()}
    try:
        zip_bytes = build_zip()
        out["deploy"] = deploy_lambda(zip_bytes)
        time.sleep(3)
        ensure_eb_rule()
        out["eb_rule"] = {"name": RULE_NAME, "schedule": SCHEDULE}
        time.sleep(2)
        out["smoke_test"] = smoke_test()
        out["duration_s"] = round(time.time() - started, 1)
    except Exception as e:
        import traceback
        out["fatal_error"] = str(e)
        out["traceback"] = traceback.format_exc()

    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f:
        json.dump(out, f, indent=2, default=str)
    print(json.dumps(out, indent=2, default=str)[:6000])
    return 0 if "fatal_error" not in out else 1


if __name__ == "__main__":
    raise SystemExit(main())
