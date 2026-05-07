#!/usr/bin/env python3
"""Step 329 — Bootstrap justhodl-global-macro.

512MB / 240s — needs time for ~75 FRED + FMP API calls (15 countries × 5 dimensions).
Schedule rate(1 day) — daily refresh, low API budget.
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
LAMBDA_NAME = "justhodl-global-macro"
SOURCE_DIR = f"aws/lambdas/{LAMBDA_NAME}/source"
ROLE_ARN = f"arn:aws:iam::{ACCOUNT_ID}:role/lambda-execution-role"
RULE_NAME = "global-macro-daily"
SCHEDULE = "rate(1 day)"
REPORT = "aws/ops/reports/329_global_macro_bootstrap.json"

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


def deploy():
    zip_bytes = build_zip()
    env_vars = {
        "S3_BUCKET": "justhodl-dashboard-live",
        "S3_KEY_OUT": "data/global-macro.json",
        "FRED_API_KEY": "2f057499936072679d8843d7fce99989",
        "FMP_KEY": "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb",
    }
    try:
        lam.get_function(FunctionName=LAMBDA_NAME)
        action = "update"
        lam.update_function_code(FunctionName=LAMBDA_NAME, ZipFile=zip_bytes)
        lam.get_waiter("function_updated").wait(FunctionName=LAMBDA_NAME)
        lam.update_function_configuration(
            FunctionName=LAMBDA_NAME,
            Runtime="python3.12", Handler="lambda_function.lambda_handler",
            MemorySize=512, Timeout=240,
            Environment={"Variables": env_vars}, Role=ROLE_ARN,
        )
        lam.get_waiter("function_updated").wait(FunctionName=LAMBDA_NAME)
    except ClientError as e:
        if e.response["Error"]["Code"] != "ResourceNotFoundException":
            raise
        action = "create"
        lam.create_function(
            FunctionName=LAMBDA_NAME, Runtime="python3.12",
            Handler="lambda_function.lambda_handler", Role=ROLE_ARN,
            MemorySize=512, Timeout=240,
            Code={"ZipFile": zip_bytes},
            Environment={"Variables": env_vars},
            Description="Per-country economic regime aggregator. 15 countries × 5 dimensions (unemp, PMI, IP YoY, equity ETF, currency). Composite Health Score 0-100 + HOT/MIXED/COLD regime classification.",
        )
        lam.get_waiter("function_active_v2").wait(FunctionName=LAMBDA_NAME)
        try:
            lam.put_function_concurrency(FunctionName=LAMBDA_NAME, ReservedConcurrentExecutions=1)
        except Exception:
            pass
    return {"action": action, "zip_kb": round(len(zip_bytes)/1024, 1)}


def ensure_rule():
    events.put_rule(Name=RULE_NAME, ScheduleExpression=SCHEDULE, State="ENABLED",
                    Description="Daily global macro per-country aggregator")
    events.put_targets(Rule=RULE_NAME,
        Targets=[{"Id": "1", "Arn": f"arn:aws:lambda:{REGION}:{ACCOUNT_ID}:function:{LAMBDA_NAME}"}])
    try:
        lam.add_permission(
            FunctionName=LAMBDA_NAME, StatementId=f"{RULE_NAME}-invoke",
            Action="lambda:InvokeFunction", Principal="events.amazonaws.com",
            SourceArn=f"arn:aws:events:{REGION}:{ACCOUNT_ID}:rule/{RULE_NAME}",
        )
    except ClientError as e:
        if e.response["Error"]["Code"] != "ResourceConflictException":
            raise
    return {"rule": RULE_NAME, "schedule": SCHEDULE}


def smoke_test():
    print("[329] Sync invoke (15 countries × FRED + FMP, ~60-180s expected)…")
    started = time.time()
    resp = lam.invoke(FunctionName=LAMBDA_NAME, InvocationType="RequestResponse")
    out = {
        "status": resp.get("StatusCode"),
        "function_error": resp.get("FunctionError"),
        "duration_s": round(time.time() - started, 1),
    }
    body = resp["Payload"].read().decode("utf-8")
    try:
        out["response"] = json.loads(body)
    except Exception:
        out["response_raw"] = body[:400]
    time.sleep(2)
    try:
        obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="data/global-macro.json")
        data = json.loads(obj["Body"].read())
        out["s3_size_kb"] = round(obj["ContentLength"]/1024, 1)
        out["n_countries"] = data.get("n_countries")
        out["n_with_data"] = data.get("n_with_data")
        out["regime_counts"] = data.get("regime_counts")
        out["global_avg"] = data.get("global_avg_composite")
        out["global_regime"] = data.get("global_regime")
        out["rankings"] = data.get("rankings")
        out["countries_sample"] = [
            {
                "code": c.get("code"), "name": c.get("name"),
                "composite": c.get("composite_score"), "regime": c.get("regime"),
                "n_components": c.get("n_components"),
                "unemp": c.get("unemployment", {}).get("value"),
                "pmi": c.get("pmi", {}).get("value"),
                "ip_yoy": c.get("ip_yoy", {}).get("value"),
                "etf_3m": c.get("equity_3m", {}).get("return_pct"),
            }
            for c in (data.get("countries") or [])[:15]
        ]
    except Exception as e:
        out["s3_err"] = str(e)[:200]
    return out


def main():
    started = time.time()
    out = {"started": datetime.now(timezone.utc).isoformat()}
    try:
        out["deploy"] = deploy()
        time.sleep(3)
        out["rule"] = ensure_rule()
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
    print(json.dumps(out, indent=2, default=str)[:5500])


if __name__ == "__main__":
    main()
