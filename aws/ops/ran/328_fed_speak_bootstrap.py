#!/usr/bin/env python3
"""Step 328 — Bootstrap justhodl-fed-speak.

256MB / 90s — needs time for RSS fetch + 0-8 Claude calls.
Schedule cron(15 11 * * ? *) — 11:15 UTC = 6:15 AM ET, after overnight Fed releases.
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
LAMBDA_NAME = "justhodl-fed-speak"
SOURCE_DIR = f"aws/lambdas/{LAMBDA_NAME}/source"
ROLE_ARN = f"arn:aws:iam::{ACCOUNT_ID}:role/lambda-execution-role"
RULE_NAME = "fed-speak-daily"
SCHEDULE = "cron(15 11 * * ? *)"
REPORT = "aws/ops/reports/328_fed_speak_bootstrap.json"

lam = boto3.client("lambda", region_name=REGION)
events = boto3.client("events", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)
ssm = boto3.client("ssm", region_name=REGION)


def get_anthropic_key():
    """Read Anthropic key from another Lambda's env (avoids hardcoding)."""
    try:
        cfg = lam.get_function_configuration(FunctionName="justhodl-ai-brief")
        env = cfg.get("Environment", {}).get("Variables", {})
        for k in ("ANTHROPIC_KEY", "ANTHROPIC_API_KEY"):
            if k in env:
                return env[k]
    except Exception as e:
        print(f"[328] couldn't read ai-brief env: {e}")
    # Fallback: try SSM
    try:
        p = ssm.get_parameter(Name="/justhodl/anthropic/api-key", WithDecryption=True)
        return p["Parameter"]["Value"]
    except Exception:
        return ""


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


def deploy(anthropic_key):
    zip_bytes = build_zip()
    env_vars = {
        "S3_BUCKET": "justhodl-dashboard-live",
        "S3_KEY_OUT": "data/fed-speak.json",
        "S3_KEY_STATE": "data/fed-speak-state.json",
        "ANTHROPIC_KEY": anthropic_key,
        "DAYS_BACK": "30",
        "MAX_NEW_PER_RUN": "8",
    }
    try:
        lam.get_function(FunctionName=LAMBDA_NAME)
        action = "update"
        lam.update_function_code(FunctionName=LAMBDA_NAME, ZipFile=zip_bytes)
        lam.get_waiter("function_updated").wait(FunctionName=LAMBDA_NAME)
        lam.update_function_configuration(
            FunctionName=LAMBDA_NAME,
            Runtime="python3.12", Handler="lambda_function.lambda_handler",
            MemorySize=256, Timeout=120,
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
            MemorySize=256, Timeout=120,
            Code={"ZipFile": zip_bytes},
            Environment={"Variables": env_vars},
            Description="Claude-powered Fed speech sentiment tracker. Pulls Fed RSS, classifies HAWKISH/NEUTRAL/DOVISH on -10/+10 scale via Claude haiku. Caches state to avoid re-classifying.",
        )
        lam.get_waiter("function_active_v2").wait(FunctionName=LAMBDA_NAME)
        try:
            lam.put_function_concurrency(FunctionName=LAMBDA_NAME, ReservedConcurrentExecutions=1)
        except Exception:
            pass
    return {"action": action, "zip_kb": round(len(zip_bytes)/1024, 1)}


def ensure_rule():
    events.put_rule(Name=RULE_NAME, ScheduleExpression=SCHEDULE, State="ENABLED",
                    Description="Daily Fed speak sentiment classifier (6:15 AM ET)")
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
    print("[328] Sync invoke (RSS + Claude classify, 30-90s expected)…")
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
        obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="data/fed-speak.json")
        data = json.loads(obj["Body"].read())
        out["s3_size_kb"] = round(obj["ContentLength"]/1024, 1)
        out["n_speeches_30d"] = data.get("n_speeches_30d")
        out["n_new_classified"] = data.get("n_new_classified_this_run")
        out["aggregate"] = data.get("aggregate")
        out["by_speaker_n"] = len(data.get("by_speaker", {}))
        out["timeline_first_3"] = [
            {
                "speaker": e.get("speaker"),
                "title": (e.get("title") or "")[:80],
                "date": e.get("pub_date","")[:10],
                "score": e.get("sentiment_score"),
                "classification": e.get("classification"),
                "rationale": (e.get("rationale") or "")[:120],
            }
            for e in (data.get("timeline") or [])[:3]
        ]
    except Exception as e:
        out["s3_err"] = str(e)[:200]
    return out


def main():
    started = time.time()
    out = {"started": datetime.now(timezone.utc).isoformat()}
    try:
        anthropic_key = get_anthropic_key()
        out["anthropic_key_present"] = bool(anthropic_key)
        out["deploy"] = deploy(anthropic_key)
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
