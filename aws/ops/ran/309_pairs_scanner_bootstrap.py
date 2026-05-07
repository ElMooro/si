#!/usr/bin/env python3
"""Step 309 — Bootstrap justhodl-pairs-scanner (Sprint 6).

Creates the Lambda + EB rule + sync invoke for smoke test.
Pairs scanner pulls Polygon prices for ~70 unique tickers across 36 pairs
in parallel. Expected duration: 8-15s for the full fetch + analysis.
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
LAMBDA_NAME = "justhodl-pairs-scanner"
SOURCE_DIR = f"aws/lambdas/{LAMBDA_NAME}/source"
ROLE_ARN = f"arn:aws:iam::{ACCOUNT_ID}:role/lambda-execution-role"
RULE_NAME = "pairs-scanner-6hourly"
SCHEDULE = "rate(6 hours)"
REPORT = "aws/ops/reports/309_pairs_scanner_bootstrap.json"

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
        "S3_KEY_OUT": "data/pairs-scanner.json",
        "POLYGON_KEY": "zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d",
    }
    try:
        lam.get_function(FunctionName=LAMBDA_NAME)
        lam.update_function_code(FunctionName=LAMBDA_NAME, ZipFile=zip_bytes)
        lam.get_waiter("function_updated").wait(FunctionName=LAMBDA_NAME)
        lam.update_function_configuration(
            FunctionName=LAMBDA_NAME,
            Runtime="python3.12",
            Handler="lambda_function.lambda_handler",
            MemorySize=256, Timeout=60,
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
        MemorySize=256, Timeout=60,
        Code={"ZipFile": zip_bytes},
        Environment={"Variables": env_vars},
        Description="Pairs trading scanner — 36 curated relative-value pairs across 13 categories. Outputs spread Z-score, half-life, and trade direction for mean-reversion candidates.",
    )
    lam.get_waiter("function_active_v2").wait(FunctionName=LAMBDA_NAME)
    return {"action": "created", "zip_bytes": len(zip_bytes)}


def ensure_eb_rule():
    events.put_rule(
        Name=RULE_NAME, ScheduleExpression=SCHEDULE, State="ENABLED",
        Description="Run pairs-scanner every 6h (4x daily, market session windows)",
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
    print("[309] Smoke test: sync invoke (8-15s for 70 ticker fetch + analysis)…")
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

    out_data = None
    try:
        obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="data/pairs-scanner.json")
        out_data = json.loads(obj["Body"].read())
    except Exception as e:
        out_data = {"err": str(e)[:200]}

    summary = {}
    if isinstance(out_data, dict):
        s = out_data.get("summary", {})
        summary = {
            "n_pairs": s.get("n_pairs"),
            "n_analyzed": s.get("n_analyzed"),
            "n_extreme": s.get("n_extreme"),
            "n_extended": s.get("n_extended"),
            "n_stretched": s.get("n_stretched"),
            "n_normal": s.get("n_normal"),
            "fetch_duration": out_data.get("fetch_stats", {}).get("fetch_duration_s"),
            "top_5": s.get("top_5_dislocations"),
            "top_10_pairs": [
                {
                    "name": p["name"],
                    "z": p.get("spread_z"),
                    "state": p.get("state"),
                    "trade": p.get("trade"),
                    "rr": p.get("rr_estimate"),
                    "hl": p.get("half_life_days"),
                    "corr": p.get("correlation_252d"),
                }
                for p in (out_data.get("pairs") or [])[:10]
            ],
        }

    return {
        "status_code": resp.get("StatusCode"),
        "function_error": resp.get("FunctionError"),
        "response": body,
        "s3_summary": summary,
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
    print(json.dumps(out, indent=2, default=str)[:6500])
    return 0 if "fatal_error" not in out else 1


if __name__ == "__main__":
    raise SystemExit(main())
