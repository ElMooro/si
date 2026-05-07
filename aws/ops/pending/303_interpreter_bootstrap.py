#!/usr/bin/env python3
"""Step 303 — Bootstrap justhodl-divergence-interpreter (Phase B).

Creates the Lambda + EventBridge schedule + sync invoke for smoke test.

DOES NOT TOUCH any existing Lambda. Pure consumer of:
  data/divergence-v2.json    (from divergence-engine-v2)
  data/macro-nowcast.json    (from macro-nowcast)

Anthropic key resolution:
  1. Try SSM /justhodl/anthropic/api_key (SecureString)
  2. Fall back to Lambda env var ANTHROPIC_API_KEY
  3. If neither — discover from an existing Lambda's env vars and copy
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
LAMBDA_NAME = "justhodl-divergence-interpreter"
SOURCE_DIR = f"aws/lambdas/{LAMBDA_NAME}/source"
ROLE_ARN = f"arn:aws:iam::{ACCOUNT_ID}:role/lambda-execution-role"
RULE_NAME = "divergence-interpreter-4hourly"
SCHEDULE = "cron(30 0/4 * * ? *)"   # 30 min past every 4th hour
ANTHROPIC_KEY_SSM = "/justhodl/anthropic/api_key"
REPORT_PATH = "aws/ops/reports/303_interpreter_bootstrap.json"

lam = boto3.client("lambda", region_name=REGION)
events = boto3.client("events", region_name=REGION)
ssm = boto3.client("ssm", region_name=REGION)


def find_existing_anthropic_key():
    """Either fetch from SSM, or discover from an existing Lambda's env vars."""
    try:
        return ssm.get_parameter(Name=ANTHROPIC_KEY_SSM, WithDecryption=True)["Parameter"]["Value"]
    except ClientError as e:
        if e.response["Error"]["Code"] != "ParameterNotFound":
            raise

    # Discover from an existing Lambda known to use Anthropic
    candidates = ["justhodl-ai-chat", "justhodl-ai-brief", "justhodl-news-sentiment",
                  "justhodl-investor-agents", "justhodl-morning-intelligence"]
    for fn in candidates:
        try:
            cfg = lam.get_function_configuration(FunctionName=fn)
            env = cfg.get("Environment", {}).get("Variables", {})
            for var in ("ANTHROPIC_API_KEY", "ANTHROPIC_KEY", "CLAUDE_API_KEY"):
                if env.get(var):
                    print(f"[303] Found Anthropic key in {fn} env var {var}")
                    # Copy to SSM for future use
                    try:
                        ssm.put_parameter(
                            Name=ANTHROPIC_KEY_SSM,
                            Value=env[var],
                            Type="SecureString",
                            Description="Shared Anthropic API key for justhodl Lambdas (auto-imported)",
                        )
                        print(f"[303] Copied to SSM {ANTHROPIC_KEY_SSM}")
                    except ClientError as e:
                        if e.response["Error"]["Code"] != "ParameterAlreadyExists":
                            raise
                    return env[var]
        except ClientError:
            continue

    raise RuntimeError("No Anthropic API key found in SSM or any existing Lambda env var")


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
        "S3_KEY_OUT": "data/divergence-interpreted.json",
        "S3_KEY_DIVERGENCE": "data/divergence-v2.json",
        "S3_KEY_NOWCAST": "data/macro-nowcast.json",
        "S3_KEY_STATE": "data/divergence-interpreted-state.json",
        "ANTHROPIC_KEY_SSM": ANTHROPIC_KEY_SSM,
        "ANTHROPIC_MODEL": "claude-haiku-4-5-20251001",
    }
    try:
        lam.get_function(FunctionName=LAMBDA_NAME)
        lam.update_function_code(FunctionName=LAMBDA_NAME, ZipFile=zip_bytes)
        lam.get_waiter("function_updated").wait(FunctionName=LAMBDA_NAME)
        lam.update_function_configuration(
            FunctionName=LAMBDA_NAME,
            Runtime="python3.12",
            Handler="lambda_function.lambda_handler",
            MemorySize=256, Timeout=90,
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
        MemorySize=256, Timeout=90,
        Code={"ZipFile": zip_bytes},
        Environment={"Variables": env_vars},
        Description="Regime-conditional divergence interpreter — Claude analyzes 70 cross-asset divergences in context of macro nowcast regime.",
    )
    lam.get_waiter("function_active_v2").wait(FunctionName=LAMBDA_NAME)
    # Reserved concurrency 1 — DDoS protection on potentially expensive Claude calls
    try:
        lam.put_function_concurrency(
            FunctionName=LAMBDA_NAME,
            ReservedConcurrentExecutions=1,
        )
    except Exception:
        pass
    return {"action": "created", "zip_bytes": len(zip_bytes)}


def ensure_eb_rule():
    try:
        events.describe_rule(Name=RULE_NAME)
        events.put_rule(Name=RULE_NAME, ScheduleExpression=SCHEDULE,
                        State="ENABLED",
                        Description="Run divergence-interpreter every 4 hours (30 past)")
        rule_action = "updated"
    except ClientError as e:
        if e.response["Error"]["Code"] != "ResourceNotFoundException":
            raise
        events.put_rule(Name=RULE_NAME, ScheduleExpression=SCHEDULE,
                        State="ENABLED",
                        Description="Run divergence-interpreter every 4 hours (30 past)")
        rule_action = "created"

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
    return {"action": rule_action, "schedule": SCHEDULE}


def smoke_test():
    """Sync invoke to verify Claude call works + S3 output written."""
    print("[303] Smoke test: sync invoke (Claude call may take 5-30s)…")
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

    # Also pull the actual S3 output to confirm
    out_data = None
    try:
        s3 = boto3.client("s3", region_name=REGION)
        obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="data/divergence-interpreted.json")
        out_data = json.loads(obj["Body"].read())
    except Exception as e:
        out_data = {"err": str(e)[:200]}

    return {
        "status_code": resp.get("StatusCode"),
        "function_error": resp.get("FunctionError"),
        "response": body,
        "s3_output_check": {
            "regime": out_data.get("regime") if isinstance(out_data, dict) else None,
            "interp_chars": len(out_data.get("interpretation", "")) if isinstance(out_data, dict) else None,
            "interp_preview": (out_data.get("interpretation", "")[:300]
                                if isinstance(out_data, dict) else None),
            "alert_reasons": out_data.get("alert_reasons") if isinstance(out_data, dict) else None,
        },
    }


def main():
    started = time.time()
    out = {"started": datetime.now(timezone.utc).isoformat()}
    try:
        # 1. Ensure Anthropic key in SSM
        try:
            api_key = find_existing_anthropic_key()
            out["anthropic_key"] = {"status": "found", "first_8": api_key[:8] + "..."}
        except Exception as e:
            out["anthropic_key"] = {"status": "err", "err": str(e)[:200]}
            raise

        # 2. Deploy Lambda
        zip_bytes = build_zip()
        out["deploy"] = deploy_lambda(zip_bytes)
        time.sleep(3)

        # 3. EventBridge rule
        out["eb_rule"] = ensure_eb_rule()
        time.sleep(2)

        # 4. Smoke test
        out["smoke_test"] = smoke_test()
        out["duration_s"] = round(time.time() - started, 1)

    except Exception as e:
        import traceback
        out["fatal_error"] = str(e)
        out["traceback"] = traceback.format_exc()

    os.makedirs(os.path.dirname(REPORT_PATH), exist_ok=True)
    with open(REPORT_PATH, "w") as f:
        json.dump(out, f, indent=2, default=str)
    print(json.dumps(out, indent=2, default=str)[:5500])
    return 0 if "fatal_error" not in out else 1


if __name__ == "__main__":
    raise SystemExit(main())
