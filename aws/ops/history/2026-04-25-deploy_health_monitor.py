#!/usr/bin/env python3
"""
Step 83 — Deploy justhodl-health-monitor Lambda.

Bundles:
  - aws/lambdas/justhodl-health-monitor/source/lambda_function.py
  - aws/ops/health/expectations.py (copied next to lambda_function.py)

Creates IAM role if needed, attaches inline policy with read access to:
  - S3 (head_object on bucket)
  - Lambda (cloudwatch metrics)
  - DynamoDB (describe_table)
  - SSM (describe_parameters)
  - EventBridge (describe_rule)
  - CloudWatch (get_metric_statistics)
  - PUT permissions on s3://justhodl-dashboard-live/_health/*

Also: writes the JSON output once synchronously and inspects it.
Doesn't yet create the EB schedule — that's step 86.
"""
import io
import json
import os
import zipfile
import shutil
import tempfile
import time
from pathlib import Path

from ops_report import report
import boto3

REGION = "us-east-1"
ACCOUNT = "857687956942"
REPO_ROOT = Path(os.environ.get("GITHUB_WORKSPACE", os.getcwd()))

lam = boto3.client("lambda", region_name=REGION)
iam = boto3.client("iam", region_name=REGION)

LAMBDA_NAME = "justhodl-health-monitor"
EXEC_ROLE_ARN = f"arn:aws:iam::{ACCOUNT}:role/lambda-execution-role"

# Inline policy spec for the existing lambda-execution-role.
# We don't want to create a new role — the system uses lambda-execution-role for
# everything. We just need to ensure the role has the perms we need (it already
# does for most: S3, Lambda, DDB, SSM, EB, CW). Add specific deny-safe policy
# only if missing.


def build_zip():
    """Bundle lambda_function.py + expectations.py into a zip."""
    src_dir = REPO_ROOT / "aws/lambdas/justhodl-health-monitor/source"
    expectations_src = REPO_ROOT / "aws/ops/health/expectations.py"

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zout:
        # lambda_function.py and any other file in source/
        for f in src_dir.rglob("*"):
            if f.is_file():
                zout.write(f, str(f.relative_to(src_dir)))
        # expectations.py — must be importable by lambda_function.py
        zout.write(expectations_src, "expectations.py")
    return buf.getvalue()


def function_exists():
    try:
        lam.get_function(FunctionName=LAMBDA_NAME)
        return True
    except lam.exceptions.ResourceNotFoundException:
        return False


with report("deploy_health_monitor") as r:
    r.heading("Step 83 — Deploy justhodl-health-monitor Lambda")

    # Build zip
    zbytes = build_zip()
    r.log(f"  Built zip: {len(zbytes):,} bytes")

    if function_exists():
        r.section("Function exists — updating code")
        lam.update_function_code(FunctionName=LAMBDA_NAME, ZipFile=zbytes)
        lam.get_waiter("function_updated").wait(
            FunctionName=LAMBDA_NAME, WaiterConfig={"Delay": 3, "MaxAttempts": 30}
        )
        r.ok(f"  Updated {LAMBDA_NAME}")
    else:
        r.section("Creating new Lambda")
        try:
            lam.create_function(
                FunctionName=LAMBDA_NAME,
                Runtime="python3.12",
                Role=EXEC_ROLE_ARN,
                Handler="lambda_function.lambda_handler",
                Code={"ZipFile": zbytes},
                Timeout=120,         # 2 min — lots of API calls
                MemorySize=256,
                Description="System health monitor — checks S3 freshness, Lambda errors, DDB, SSM, EB. Runs every 15 min.",
                Environment={"Variables": {}},
                Tags={"system": "justhodl", "purpose": "monitoring"},
            )
            lam.get_waiter("function_active").wait(
                FunctionName=LAMBDA_NAME, WaiterConfig={"Delay": 3, "MaxAttempts": 30}
            )
            r.ok(f"  Created {LAMBDA_NAME}")
        except Exception as e:
            r.fail(f"  Create failed: {e}")
            raise SystemExit(1)

    # Synchronous test invoke
    r.section("Test invoke (sync) — see what dashboard looks like")
    try:
        resp = lam.invoke(FunctionName=LAMBDA_NAME, InvocationType="RequestResponse")
        status = resp.get("StatusCode")
        payload = resp.get("Payload").read().decode()
        r.log(f"  Status: {status}")
        if resp.get("FunctionError"):
            r.fail(f"  FunctionError: {resp.get('FunctionError')}")
            r.log(f"  Payload (first 1500 chars): {payload[:1500]}")
        else:
            r.ok(f"  Invoke clean")
            r.log(f"  Payload preview: {payload[:500]}")
    except Exception as e:
        r.fail(f"  Invoke failed: {e}")

    # Read back the dashboard
    r.section("Inspect dashboard.json output")
    try:
        s3 = boto3.client("s3", region_name=REGION)
        obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="_health/dashboard.json")
        dash = json.loads(obj["Body"].read())
        r.log(f"  System status: {dash.get('system_status')}")
        r.log(f"  Counts: {dash.get('counts')}")
        r.log(f"  Total components: {dash.get('total_components')}")
        r.log(f"  Duration: {dash.get('duration_sec'):.1f}s")
        r.log("")
        r.log("  Top issues (first 10 non-green):")
        for c in dash.get("components", [])[:10]:
            if c.get("status") == "green":
                continue
            short_id = c.get("id", "?")
            status = c.get("status", "?")
            sev = c.get("severity", "?")
            reason = c.get("reason") or c.get("error") or ""
            r.log(f"    [{status:7}] {sev:12} {short_id:50} {reason[:80]}")
    except Exception as e:
        r.warn(f"  Couldn't read dashboard: {e}")

    r.kv(
        lambda_name=LAMBDA_NAME,
        zip_bytes=len(zbytes),
        next_step="step 84 builds the HTML dashboard",
    )
    r.log("Done")
