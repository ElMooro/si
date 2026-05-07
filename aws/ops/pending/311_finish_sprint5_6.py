#!/usr/bin/env python3
"""Step 311 — Finish Sprint 5 + Sprint 6 deployment.

Sprint 5 — sector-tilt:
  * Lambda is deployed and producing data
  * But audit (ops 310) shows NO EventBridge schedule
  * Verify and (re)create the rule sector-tilt-4hourly
    cron(45 0/4 * * ? *)  — every 4h, 45min past

Sprint 6 — pairs-scanner:
  * Lambda code is in repo but Lambda not deployed to AWS
  * Bootstrap script 309 is still in pending/ (never ran or failed silently)
  * Build zip, create Lambda, create EB rule pairs-scanner-6hourly
    rate(6 hours)
  * Sync-invoke for first data

ZERO DETERIORATION
  * sector-tilt verify is idempotent — won't break the existing Active Lambda
  * Pairs-scanner is a brand-new Lambda — touches nothing existing
  * Both are pure consumers (sector-tilt) or producers (pairs) — no other
    Lambda's behavior changes
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
REPORT = "aws/ops/reports/311_finish_sprint5_6.json"

# Sprint 5 — sector-tilt
TILT_NAME = "justhodl-sector-tilt"
TILT_RULE = "sector-tilt-4hourly"
TILT_SCHEDULE = "cron(45 0/4 * * ? *)"

# Sprint 6 — pairs-scanner
PAIRS_NAME = "justhodl-pairs-scanner"
PAIRS_SOURCE_DIR = f"aws/lambdas/{PAIRS_NAME}/source"
PAIRS_RULE = "pairs-scanner-6hourly"
PAIRS_SCHEDULE = "rate(6 hours)"

lam = boto3.client("lambda", region_name=REGION)
events = boto3.client("events", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)


def list_eb_rules_for_lambda(fname):
    """Find any EB rules already targeting this Lambda."""
    out = []
    try:
        # List all rule sources for the Lambda
        arn = f"arn:aws:lambda:{REGION}:{ACCOUNT_ID}:function:{fname}"
        # Use list-rule-names-by-target
        paginator = events.get_paginator("list_rule_names_by_target")
        for page in paginator.paginate(TargetArn=arn):
            for n in page.get("RuleNames", []):
                r = events.describe_rule(Name=n)
                out.append({
                    "name": n,
                    "schedule": r.get("ScheduleExpression"),
                    "state": r.get("State"),
                })
    except Exception as e:
        out.append({"err": str(e)[:200]})
    return out


def ensure_rule_for_lambda(fname, rule_name, schedule, description):
    """Create or update an EB rule + target for this Lambda."""
    arn = f"arn:aws:lambda:{REGION}:{ACCOUNT_ID}:function:{fname}"

    # Create or update rule
    events.put_rule(
        Name=rule_name,
        ScheduleExpression=schedule,
        State="ENABLED",
        Description=description,
    )
    # Wire target
    events.put_targets(
        Rule=rule_name,
        Targets=[{"Id": "1", "Arn": arn}],
    )
    # Permission for events to invoke Lambda
    try:
        lam.add_permission(
            FunctionName=fname,
            StatementId=f"{rule_name}-invoke",
            Action="lambda:InvokeFunction",
            Principal="events.amazonaws.com",
            SourceArn=f"arn:aws:events:{REGION}:{ACCOUNT_ID}:rule/{rule_name}",
        )
    except ClientError as e:
        if e.response["Error"]["Code"] != "ResourceConflictException":
            raise
    return {"rule": rule_name, "schedule": schedule, "state": "ENABLED"}


def fix_sector_tilt():
    """Verify sector-tilt has its EB rule wired."""
    out = {"lambda_name": TILT_NAME}
    try:
        cfg = lam.get_function_configuration(FunctionName=TILT_NAME)
        out["lambda_state"] = cfg.get("State")
    except ClientError as e:
        out["lambda_err"] = e.response["Error"]["Code"]
        return out

    existing_rules = list_eb_rules_for_lambda(TILT_NAME)
    out["existing_rules_before"] = existing_rules

    # Check if any rule is targeting it; if none — wire it
    if not existing_rules or all("err" in r for r in existing_rules):
        out["action"] = "create_rule"
        out["rule_result"] = ensure_rule_for_lambda(
            TILT_NAME, TILT_RULE, TILT_SCHEDULE,
            "Sector-tilt every 4h at 45min past, after sector-rotation refresh"
        )
    else:
        # If existing but maybe disabled / wrong schedule — verify
        out["action"] = "rules_already_exist"
        out["rules"] = existing_rules

    return out


def build_pairs_zip():
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for root, _d, files in os.walk(PAIRS_SOURCE_DIR):
            for fn in files:
                if fn.endswith(".pyc") or "__pycache__" in root:
                    continue
                fp = os.path.join(root, fn)
                zf.write(fp, os.path.relpath(fp, PAIRS_SOURCE_DIR))
    return buf.getvalue()


def deploy_pairs_scanner():
    """Create or update Lambda + ensure EB rule + sync invoke."""
    out = {"lambda_name": PAIRS_NAME}

    env_vars = {
        "S3_BUCKET": "justhodl-dashboard-live",
        "S3_KEY_OUT": "data/pairs-scanner.json",
        "POLYGON_KEY": "zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d",
    }
    zip_bytes = build_pairs_zip()
    out["zip_size_kb"] = round(len(zip_bytes) / 1024, 1)

    try:
        lam.get_function(FunctionName=PAIRS_NAME)
        out["action"] = "update_existing"
        lam.update_function_code(FunctionName=PAIRS_NAME, ZipFile=zip_bytes)
        lam.get_waiter("function_updated").wait(FunctionName=PAIRS_NAME)
        lam.update_function_configuration(
            FunctionName=PAIRS_NAME,
            Runtime="python3.12",
            Handler="lambda_function.lambda_handler",
            MemorySize=256, Timeout=60,
            Environment={"Variables": env_vars},
            Role=ROLE_ARN,
        )
        lam.get_waiter("function_updated").wait(FunctionName=PAIRS_NAME)
    except ClientError as e:
        if e.response["Error"]["Code"] != "ResourceNotFoundException":
            raise
        out["action"] = "create_new"
        lam.create_function(
            FunctionName=PAIRS_NAME,
            Runtime="python3.12",
            Handler="lambda_function.lambda_handler",
            Role=ROLE_ARN,
            MemorySize=256, Timeout=60,
            Code={"ZipFile": zip_bytes},
            Environment={"Variables": env_vars},
            Description=("Relative-value pairs trading scanner — 36 curated pairs across "
                         "tech mega-cap, semis, banks, energy, healthcare, country ETFs, "
                         "style factors, bonds. Z-score, half-life, classification."),
        )
        lam.get_waiter("function_active_v2").wait(FunctionName=PAIRS_NAME)
        # Reserved concurrency — protect Polygon API rate limits
        lam.put_function_concurrency(
            FunctionName=PAIRS_NAME,
            ReservedConcurrentExecutions=1,
        )

    # Wire EB rule
    out["rule_result"] = ensure_rule_for_lambda(
        PAIRS_NAME, PAIRS_RULE, PAIRS_SCHEDULE,
        "Pairs-scanner every 6h — 36 curated pairs, Z-scores + half-life"
    )

    # Sync invoke for first data (Polygon fetch ~10s)
    time.sleep(3)
    print(f"[311] Sync invoke {PAIRS_NAME} — Polygon fetch ~8-15s…")
    started = time.time()
    resp = lam.invoke(
        FunctionName=PAIRS_NAME,
        InvocationType="RequestResponse",
        LogType="Tail",
    )
    out["smoke_invoke"] = {
        "status_code": resp.get("StatusCode"),
        "function_error": resp.get("FunctionError"),
        "duration_s": round(time.time() - started, 1),
    }
    body = resp["Payload"].read().decode("utf-8")
    try:
        out["smoke_invoke"]["response"] = json.loads(body)
    except Exception:
        out["smoke_invoke"]["response_raw"] = body[:500]

    # Verify S3 output
    time.sleep(2)
    try:
        obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="data/pairs-scanner.json")
        data = json.loads(obj["Body"].read())
        out["s3_output"] = {
            "size_kb": round(obj["ContentLength"] / 1024, 1),
            "n_pairs": len(data.get("pairs", [])),
            "n_extreme": data.get("summary", {}).get("n_extreme"),
            "n_extended": data.get("summary", {}).get("n_extended"),
            "categories": list((data.get("by_category") or {}).keys()),
        }
        # Capture top 5 most stretched pairs
        pairs = data.get("pairs", [])
        pairs_sorted = sorted(
            [p for p in pairs if p.get("z_score") is not None],
            key=lambda p: abs(p.get("z_score") or 0),
            reverse=True
        )
        out["top_5_stretched"] = [
            {
                "name": p.get("name"),
                "leg_a": p.get("leg_a"),
                "leg_b": p.get("leg_b"),
                "z_score": p.get("z_score"),
                "half_life_days": p.get("half_life_days"),
                "correlation_252d": p.get("correlation_252d"),
                "classification": p.get("classification"),
                "category": p.get("category"),
            }
            for p in pairs_sorted[:5]
        ]
    except Exception as e:
        out["s3_output"] = {"err": str(e)[:200]}

    return out


def main():
    started = time.time()
    out = {"started": datetime.now(timezone.utc).isoformat()}
    try:
        out["sprint5_fix"] = fix_sector_tilt()
        out["sprint6_deploy"] = deploy_pairs_scanner()
        out["duration_s"] = round(time.time() - started, 1)
    except Exception as e:
        import traceback
        out["fatal_error"] = str(e)
        out["traceback"] = traceback.format_exc()

    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f:
        json.dump(out, f, indent=2, default=str)
    print(json.dumps(out, indent=2, default=str)[:7000])


if __name__ == "__main__":
    main()
