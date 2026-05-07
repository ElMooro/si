#!/usr/bin/env python3
"""Step 335 — Bootstrap all 5 new Lambdas (institutional-grade build).

Creates:
  1. justhodl-watchlist          (Lambda URL, no schedule, on-demand)
  2. justhodl-catalyst-calendar  (rate(1 hour))
  3. justhodl-vol-regime         (rate(1 hour) — rerun during market hours)
  4. justhodl-implied-prob       (rate(4 hours))
  5. justhodl-trade-journal      (Lambda URL + cron(0 22 * * ? *) for nightly MTM)

Sync-tests each. Returns Lambda URLs for the 2 API services.
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
ROLE_ARN = f"arn:aws:iam::{ACCOUNT_ID}:role/lambda-execution-role"
REPORT = "aws/ops/reports/335_5features_bootstrap.json"

lam = boto3.client("lambda", region_name=REGION)
events = boto3.client("events", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)
iam = boto3.client("iam", region_name=REGION)

# ─── Lambda specs ────────────────────────────────────────────────────────────
SPECS = [
    {
        "name": "justhodl-watchlist",
        "memory": 256, "timeout": 30,
        "env": {
            "S3_BUCKET": "justhodl-dashboard-live",
            "S3_KEY": "data/user-watchlist.json",
            "SSM_TOKEN_PATH": "/justhodl/api-admin/token",
        },
        "schedule": None,
        "needs_url": True,
        "description": "Personal watchlist API — GET (public read) + POST add/remove/replace (admin token).",
    },
    {
        "name": "justhodl-catalyst-calendar",
        "memory": 256, "timeout": 60,
        "env": {
            "S3_BUCKET": "justhodl-dashboard-live",
            "S3_KEY_OUT": "data/catalyst-calendar.json",
            "WINDOW_DAYS": "60",
        },
        "schedule": ("catalyst-calendar-hourly", "rate(1 hour)"),
        "needs_url": False,
        "description": "Forward catalyst calendar — FOMC + Treasury auctions + earnings + witching + index rebalance.",
    },
    {
        "name": "justhodl-vol-regime",
        "memory": 512, "timeout": 240,
        "env": {
            "S3_BUCKET": "justhodl-dashboard-live",
            "S3_KEY_OUT": "data/vol-regime.json",
            "POLY_KEY": "zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d",
        },
        "schedule": ("vol-regime-hourly", "rate(1 hour)"),
        "needs_url": False,
        "description": "Volatility regime — RV/IV/Skew/Term Structure for SPY/QQQ/etc + watchlist.",
    },
    {
        "name": "justhodl-implied-prob",
        "memory": 512, "timeout": 240,
        "env": {
            "S3_BUCKET": "justhodl-dashboard-live",
            "S3_KEY_OUT": "data/implied-prob.json",
            "FRED_API_KEY": "2f057499936072679d8843d7fce99989",
            "POLY_KEY": "zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d",
        },
        "schedule": ("implied-prob-4hourly", "rate(4 hours)"),
        "needs_url": False,
        "description": "Forward implied probability — Fed rate path + recession + SPY/QQQ/BTC implied moves.",
    },
    {
        "name": "justhodl-trade-journal",
        "memory": 256, "timeout": 60,
        "env": {
            "S3_BUCKET": "justhodl-dashboard-live",
            "S3_KEY_TRADES": "data/user-trades.json",
            "S3_KEY_STATS": "data/user-trades-stats.json",
            "SSM_TOKEN_PATH": "/justhodl/api-admin/token",
            "POLY_KEY": "zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d",
        },
        # Schedule for nightly MTM mark-to-market via aws.events trigger
        "schedule": ("trade-journal-mtm-nightly", "cron(0 22 * * ? *)"),
        "needs_url": True,
        "description": "Personal trade journal — GET (public read) + POST add/close/update/delete (admin) + nightly MTM.",
    },
]


def build_zip(lambda_name):
    src_dir = f"aws/lambdas/{lambda_name}/source"
    if not os.path.isdir(src_dir):
        raise FileNotFoundError(f"Source dir not found: {src_dir}")
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for root, _d, files in os.walk(src_dir):
            for fn in files:
                if fn.endswith(".pyc") or "__pycache__" in root:
                    continue
                fp = os.path.join(root, fn)
                zf.write(fp, os.path.relpath(fp, src_dir))
    return buf.getvalue()


def deploy_lambda(spec):
    name = spec["name"]
    zip_bytes = build_zip(name)
    try:
        lam.get_function(FunctionName=name)
        action = "update"
        lam.update_function_code(FunctionName=name, ZipFile=zip_bytes)
        lam.get_waiter("function_updated").wait(FunctionName=name)
        lam.update_function_configuration(
            FunctionName=name, Runtime="python3.12",
            Handler="lambda_function.lambda_handler",
            MemorySize=spec["memory"], Timeout=spec["timeout"],
            Environment={"Variables": spec["env"]}, Role=ROLE_ARN,
        )
        lam.get_waiter("function_updated").wait(FunctionName=name)
    except ClientError as e:
        if e.response["Error"]["Code"] != "ResourceNotFoundException":
            raise
        action = "create"
        lam.create_function(
            FunctionName=name, Runtime="python3.12",
            Handler="lambda_function.lambda_handler", Role=ROLE_ARN,
            MemorySize=spec["memory"], Timeout=spec["timeout"],
            Code={"ZipFile": zip_bytes},
            Environment={"Variables": spec["env"]},
            Description=spec["description"],
        )
        lam.get_waiter("function_active_v2").wait(FunctionName=name)
        try:
            lam.put_function_concurrency(FunctionName=name, ReservedConcurrentExecutions=1)
        except Exception:
            pass
    return {"action": action, "zip_kb": round(len(zip_bytes)/1024, 1)}


def ensure_url(name):
    """Create or update Lambda URL with public AuthType=NONE + CORS."""
    cors_cfg = {
        "AllowOrigins": ["https://justhodl.ai", "https://www.justhodl.ai", "*"],
        "AllowMethods": ["GET", "POST", "OPTIONS"],
        "AllowHeaders": ["content-type", "x-justhodl-token"],
        "MaxAge": 86400,
    }
    try:
        cur = lam.get_function_url_config(FunctionName=name)
        lam.update_function_url_config(
            FunctionName=name, AuthType="NONE", Cors=cors_cfg,
            InvokeMode="BUFFERED",
        )
        url = cur.get("FunctionUrl")
    except ClientError as e:
        if e.response["Error"]["Code"] != "ResourceNotFoundException":
            raise
        out = lam.create_function_url_config(
            FunctionName=name, AuthType="NONE", Cors=cors_cfg, InvokeMode="BUFFERED",
        )
        url = out.get("FunctionUrl")
    # Public invoke permission
    try:
        lam.add_permission(
            FunctionName=name, StatementId="public-url-invoke",
            Action="lambda:InvokeFunctionUrl", Principal="*",
            FunctionUrlAuthType="NONE",
        )
    except ClientError as e:
        if e.response["Error"]["Code"] != "ResourceConflictException":
            raise
    return url


def ensure_schedule(spec):
    if not spec.get("schedule"):
        return None
    rule_name, schedule_expr = spec["schedule"]
    name = spec["name"]
    events.put_rule(Name=rule_name, ScheduleExpression=schedule_expr,
                    State="ENABLED", Description=f"Schedule for {name}")
    target_arn = f"arn:aws:lambda:{REGION}:{ACCOUNT_ID}:function:{name}"
    target_input = json.dumps({"scheduled": True}) if name == "justhodl-trade-journal" else "{}"
    events.put_targets(Rule=rule_name,
        Targets=[{"Id": "1", "Arn": target_arn, "Input": target_input}])
    try:
        lam.add_permission(
            FunctionName=name, StatementId=f"{rule_name}-invoke",
            Action="lambda:InvokeFunction", Principal="events.amazonaws.com",
            SourceArn=f"arn:aws:events:{REGION}:{ACCOUNT_ID}:rule/{rule_name}",
        )
    except ClientError as e:
        if e.response["Error"]["Code"] != "ResourceConflictException":
            raise
    return {"rule": rule_name, "schedule": schedule_expr}


def smoke_test(spec):
    """Sync invoke each Lambda. For HTTP Lambdas, simulate a GET event."""
    name = spec["name"]
    is_http = spec.get("needs_url")
    if is_http:
        # Simulate Lambda URL GET request
        payload = {
            "requestContext": {"http": {"method": "GET"}},
            "rawPath": "/", "headers": {"origin": "https://justhodl.ai"}, "body": "",
        }
    else:
        payload = {}

    started = time.time()
    try:
        resp = lam.invoke(FunctionName=name, InvocationType="RequestResponse",
                          Payload=json.dumps(payload).encode("utf-8"))
        out = {
            "status": resp.get("StatusCode"),
            "function_error": resp.get("FunctionError"),
            "duration_s": round(time.time() - started, 1),
        }
        body = resp["Payload"].read().decode("utf-8")
        try:
            out["response"] = json.loads(body)
        except Exception:
            out["response_raw"] = body[:300]
    except Exception as e:
        out = {"err": str(e)[:200], "duration_s": round(time.time() - started, 1)}
    return out


def main():
    started = time.time()
    out = {"started": datetime.now(timezone.utc).isoformat(), "lambdas": {}}

    for spec in SPECS:
        name = spec["name"]
        print(f"\n══ {name} ══")
        info = {"name": name}
        try:
            info["deploy"] = deploy_lambda(spec)
            time.sleep(2)
            if spec.get("needs_url"):
                info["url"] = ensure_url(name)
            time.sleep(1)
            sched_info = ensure_schedule(spec)
            if sched_info:
                info["schedule"] = sched_info
            time.sleep(2)
            info["smoke_test"] = smoke_test(spec)
        except Exception as e:
            import traceback
            info["fatal_error"] = str(e)
            info["traceback"] = traceback.format_exc()[-1500:]
        out["lambdas"][name] = info
        # Brief pause between Lambdas to avoid throttling
        time.sleep(2)

    out["duration_s"] = round(time.time() - started, 1)
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f:
        json.dump(out, f, indent=2, default=str)
    print(json.dumps(out, indent=2, default=str)[:8000])


if __name__ == "__main__":
    main()
