#!/usr/bin/env python3
"""
Step 371 — Bootstrap stress-test simulator pipeline.

Creates two new Lambdas (neither existed before — both this commit's
greenfield work), wires them up, and schedules the loadings recompute.

Steps:
  1. Create Lambda justhodl-stress-simulator (real-time API)
  2. Create Lambda justhodl-stress-loadings (weekly recompute)
  3. Create Function URL on stress-simulator (public, CORS justhodl.ai)
  4. Public invoke permission for the Function URL
  5. EventBridge rule stress-loadings-weekly → stress-loadings Lambda
     (Sundays 14:00 UTC = roughly when markets are closed everywhere)
  6. Add Polygon API key to stress-loadings env vars
  7. Patch stress.html to inject the actual Function URL

Both Lambdas use lambda-execution-role (already has S3 + SSM read/write).
Idempotent — safe to re-run.
"""
import json
import os
import re
import time
import zipfile
import io
from datetime import datetime, timezone
import boto3
from botocore.exceptions import ClientError

REPORT = "aws/ops/reports/371_stress_setup.json"
ACCOUNT = "857687956942"
REGION = "us-east-1"
ROLE_ARN = f"arn:aws:iam::{ACCOUNT}:role/lambda-execution-role"
SIMULATOR_FN = "justhodl-stress-simulator"
LOADINGS_FN = "justhodl-stress-loadings"
SIM_SOURCE = "aws/lambdas/justhodl-stress-simulator/source/lambda_function.py"
LOAD_SOURCE = "aws/lambdas/justhodl-stress-loadings/source/lambda_function.py"

lam = boto3.client("lambda", region_name=REGION)
events = boto3.client("events", region_name=REGION)
ssm = boto3.client("ssm", region_name=REGION)


def _zip_source(path):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        with open(path) as f:
            zf.writestr("lambda_function.py", f.read())
    return buf.getvalue()


def _create_or_update(fn_name, source_path, env=None, memory=512, timeout=60):
    code = _zip_source(source_path)
    env_dict = env or {}
    try:
        info = lam.get_function(FunctionName=fn_name)
        # Update code + ensure runtime/handler/env are aligned
        try:
            lam.get_waiter("function_updated").wait(FunctionName=fn_name)
        except Exception:
            pass
        lam.update_function_code(FunctionName=fn_name, ZipFile=code)
        lam.get_waiter("function_updated").wait(FunctionName=fn_name)
        lam.update_function_configuration(
            FunctionName=fn_name,
            Runtime="python3.12",
            Handler="lambda_function.lambda_handler",
            Role=ROLE_ARN,
            Environment={"Variables": env_dict},
            MemorySize=memory,
            Timeout=timeout,
        )
        lam.get_waiter("function_updated").wait(FunctionName=fn_name)
        return "updated"
    except lam.exceptions.ResourceNotFoundException:
        lam.create_function(
            FunctionName=fn_name,
            Runtime="python3.12",
            Handler="lambda_function.lambda_handler",
            Role=ROLE_ARN,
            Code={"ZipFile": code},
            Environment={"Variables": env_dict},
            MemorySize=memory,
            Timeout=timeout,
            Tags={"project": "justhodl", "feature": "stress-simulator"},
        )
        lam.get_waiter("function_active_v2").wait(FunctionName=fn_name)
        return "created"


def step_simulator(out):
    out["simulator"] = {"action": _create_or_update(
        SIMULATOR_FN, SIM_SOURCE,
        env={"S3_BUCKET": "justhodl-dashboard-live"},
        memory=512, timeout=30,
    )}


def step_loadings(out):
    out["loadings"] = {"action": _create_or_update(
        LOADINGS_FN, LOAD_SOURCE,
        env={
            "S3_BUCKET": "justhodl-dashboard-live",
            "POLYGON_API_KEY": "zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d",
        },
        memory=1024, timeout=300,  # plenty for 10 Polygon fetches
    )}


def step_function_url(out):
    cors = {
        "AllowOrigins": ["https://justhodl.ai", "https://www.justhodl.ai"],
        "AllowMethods": ["GET", "POST"],
        "AllowHeaders": ["Content-Type", "X-Justhodl-Admin-Token"],
        "ExposeHeaders": [],
        "MaxAge": 3600,
        "AllowCredentials": False,
    }
    try:
        resp = lam.create_function_url_config(
            FunctionName=SIMULATOR_FN, AuthType="NONE", Cors=cors, InvokeMode="BUFFERED",
        )
        out["function_url"] = {"created": True, "url": resp["FunctionUrl"]}
    except lam.exceptions.ResourceConflictException:
        existing = lam.get_function_url_config(FunctionName=SIMULATOR_FN)
        out["function_url"] = {"created": False, "url": existing["FunctionUrl"]}
        try:
            lam.update_function_url_config(
                FunctionName=SIMULATOR_FN, AuthType="NONE", Cors=cors, InvokeMode="BUFFERED",
            )
        except Exception:
            pass

    # Public invoke perm
    try:
        lam.add_permission(
            FunctionName=SIMULATOR_FN, StatementId="FunctionURLAllowPublicAccess",
            Action="lambda:InvokeFunctionUrl", Principal="*",
            FunctionUrlAuthType="NONE",
        )
        out["public_perm"] = "added"
    except lam.exceptions.ResourceConflictException:
        out["public_perm"] = "already_exists"

    return out["function_url"]["url"]


def step_eventbridge(out):
    rule_name = "stress-loadings-weekly"
    schedule = "cron(0 14 ? * SUN *)"  # Sundays 14:00 UTC

    # Create / update rule
    events.put_rule(
        Name=rule_name,
        ScheduleExpression=schedule,
        State="ENABLED",
        Description="Weekly factor-loadings recompute for justhodl-stress-simulator",
    )

    fn_arn = lam.get_function(FunctionName=LOADINGS_FN)["Configuration"]["FunctionArn"]
    events.put_targets(
        Rule=rule_name,
        Targets=[{"Id": "1", "Arn": fn_arn}],
    )

    # Allow EventBridge to invoke the Lambda
    try:
        lam.add_permission(
            FunctionName=LOADINGS_FN,
            StatementId="eventbridge-stress-loadings-invoke",
            Action="lambda:InvokeFunction",
            Principal="events.amazonaws.com",
            SourceArn=f"arn:aws:events:{REGION}:{ACCOUNT}:rule/{rule_name}",
        )
        out["eventbridge"] = {"rule": rule_name, "schedule": schedule, "permission": "added"}
    except lam.exceptions.ResourceConflictException:
        out["eventbridge"] = {"rule": rule_name, "schedule": schedule, "permission": "already_exists"}


def step_save_url_in_ssm(out, url):
    ssm.put_parameter(
        Name="/justhodl/stress/simulator-url",
        Value=url,
        Type="String",
        Description="Public Function URL for justhodl-stress-simulator",
        Overwrite=True,
    )
    out["ssm"] = {"url_saved": True}


def step_patch_html(out, url):
    """Patch stress.html to use the real Function URL."""
    path = "stress.html"
    if not os.path.isfile(path):
        out["html_patched"] = "missing"
        return
    with open(path) as f:
        content = f.read()
    # url comes back as "https://...lambda-url.us-east-1.on.aws/" (with trailing slash)
    base = url.rstrip("/")
    new_content = re.sub(
        r'const API_URL = "https://[^"]*"',
        f'const API_URL = "{base}"',
        content,
    )
    if new_content != content:
        with open(path, "w") as f:
            f.write(new_content)
        out["html_patched"] = "yes"
    else:
        out["html_patched"] = "no_change_or_already_patched"


def step_initial_loadings_run(out):
    """Trigger one synchronous run of the loadings Lambda so the simulator
    has fresh betas right away (before the Sunday cron fires)."""
    try:
        resp = lam.invoke(FunctionName=LOADINGS_FN, InvocationType="RequestResponse",
                          Payload=b"{}")
        body = resp["Payload"].read().decode("utf-8")
        out["initial_loadings"] = {"status": resp.get("StatusCode"), "body": body[:300]}
    except Exception as e:
        out["initial_loadings"] = {"error": str(e)[:300]}


def main():
    out = {"started": datetime.now(timezone.utc).isoformat(), "steps": {}}
    try:
        step_simulator(out["steps"])
        step_loadings(out["steps"])
        url = step_function_url(out["steps"])
        step_eventbridge(out["steps"])
        step_save_url_in_ssm(out["steps"], url)
        step_patch_html(out["steps"], url)
        step_initial_loadings_run(out["steps"])
        out["status"] = "success"
    except Exception as e:
        out["status"] = "error"
        out["error"] = f"{type(e).__name__}: {e}"
        raise
    finally:
        out["finished"] = datetime.now(timezone.utc).isoformat()
        os.makedirs(os.path.dirname(REPORT), exist_ok=True)
        with open(REPORT, "w") as f:
            json.dump(out, f, indent=2, default=str)


if __name__ == "__main__":
    main()
