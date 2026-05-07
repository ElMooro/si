#!/usr/bin/env python3
"""Step 319 — Bootstrap justhodl-master-ranker.

Creates the Lambda (256MB / 60s — pure S3 reads, no expensive APIs),
schedules rate(1 hour) starting 5min past so it runs after compound-aggregator,
and sync-invokes for first data.
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
LAMBDA_NAME = "justhodl-master-ranker"
SOURCE_DIR = f"aws/lambdas/{LAMBDA_NAME}/source"
ROLE_ARN = f"arn:aws:iam::{ACCOUNT_ID}:role/lambda-execution-role"
RULE_NAME = "master-ranker-hourly"
SCHEDULE = "cron(5 * * * ? *)"  # 5min past every hour
REPORT = "aws/ops/reports/319_master_ranker_bootstrap.json"

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
        "S3_KEY_OUT": "data/master-ranker.json",
    }
    try:
        lam.get_function(FunctionName=LAMBDA_NAME)
        action = "update"
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
    except ClientError as e:
        if e.response["Error"]["Code"] != "ResourceNotFoundException":
            raise
        action = "create"
        lam.create_function(
            FunctionName=LAMBDA_NAME,
            Runtime="python3.12",
            Handler="lambda_function.lambda_handler",
            Role=ROLE_ARN,
            MemorySize=256, Timeout=60,
            Code={"ZipFile": zip_bytes},
            Environment={"Variables": env_vars},
            Description="Master signal ranker — unified Top-25 ticker conviction list across 14 systems + Top-10 macro signals (divergence, pairs, sector tilt, regime).",
        )
        lam.get_waiter("function_active_v2").wait(FunctionName=LAMBDA_NAME)
        try:
            lam.put_function_concurrency(FunctionName=LAMBDA_NAME, ReservedConcurrentExecutions=1)
        except Exception:
            pass
    return {"action": action, "zip_kb": round(len(zip_bytes) / 1024, 1)}


def ensure_rule():
    events.put_rule(
        Name=RULE_NAME, ScheduleExpression=SCHEDULE,
        State="ENABLED",
        Description="Master signal ranker — runs hourly at :05 (after compound-aggregator)",
    )
    events.put_targets(
        Rule=RULE_NAME,
        Targets=[{"Id": "1", "Arn": f"arn:aws:lambda:{REGION}:{ACCOUNT_ID}:function:{LAMBDA_NAME}"}],
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
    return {"rule": RULE_NAME, "schedule": SCHEDULE}


def smoke_test():
    print("[319] Sync invoke (S3 reads — should be ~5-15s)…")
    started = time.time()
    resp = lam.invoke(
        FunctionName=LAMBDA_NAME,
        InvocationType="RequestResponse",
        LogType="Tail",
    )
    out = {
        "status_code": resp.get("StatusCode"),
        "function_error": resp.get("FunctionError"),
        "duration_s": round(time.time() - started, 1),
    }
    body = resp["Payload"].read().decode("utf-8")
    try:
        out["response"] = json.loads(body)
    except Exception:
        out["response_raw"] = body[:500]

    # Pull S3 output for verification
    time.sleep(2)
    try:
        obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="data/master-ranker.json")
        data = json.loads(obj["Body"].read())
        out["s3_size_kb"] = round(obj["ContentLength"] / 1024, 1)
        out["regime"] = data.get("regime_context", {}).get("regime")
        out["n_tickers_total"] = data.get("n_tickers_total")
        out["n_tickers_top25"] = len(data.get("top_tickers", []))
        out["n_macro_signals"] = data.get("n_macro_signals")
        out["alerts"] = data.get("alerts")
        # Top 5 tickers preview
        out["top_5_tickers"] = [
            {
                "rank": i + 1,
                "ticker": t.get("ticker"),
                "score": t.get("score"),
                "n_systems": t.get("n_systems"),
                "rationale": (t.get("rationale") or "")[:100],
            }
            for i, t in enumerate(data.get("top_tickers", [])[:5])
        ]
        out["top_3_macro"] = [
            {
                "type": m.get("type"),
                "name": m.get("name"),
                "score": m.get("score"),
                "action": (m.get("action_hint") or "")[:80],
            }
            for m in (data.get("top_macro", []) or [])[:3]
        ]
        out["feed_health"] = data.get("feed_health")
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
