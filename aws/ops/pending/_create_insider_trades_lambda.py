"""
Step ___ — Create/update justhodl-insider-trades Lambda + EB rule.

Re-runs:
  - 2026-04-27 — Patched atom feed parser (accession comes from <id>, not <link>).

deploy-lambdas.yml uses update-function-code which assumes the Lambda
already exists. For brand-new Lambdas we need create-function on first run.
This script is idempotent: creates if missing, updates if present, ensures
EB rule + permissions are in place either way.

Wires up:
  Lambda function:  justhodl-insider-trades
  EB rule:          justhodl-insider-trades-30min  (rate(30 minutes))
  Function URL:     for manual /backfill triggers
  Reserved concurrency: 1 (single-flight; SEC EDGAR doesn't like parallelism
                          and we don't want overlapping invocations stomping
                          on the rolling-window S3 file)
  Environment:      S3_BUCKET, MIN_BUY_VALUE_USD, etc. (defaults are fine)

After this runs successfully, future code changes deploy through the
standard CI/CD (deploy-lambdas.yml on push to source/**).
"""
from __future__ import annotations
import io
import json
import os
import time
import zipfile
from pathlib import Path

import boto3
from botocore.exceptions import ClientError

from ops_report import report

REGION = "us-east-1"
ACCOUNT = "857687956942"
FN_NAME = "justhodl-insider-trades"
EB_RULE_NAME = "justhodl-insider-trades-30min"
ROLE_ARN = f"arn:aws:iam::{ACCOUNT}:role/lambda-execution-role"
SOURCE_DIR = Path("aws/lambdas/justhodl-insider-trades/source")

ENV_VARS = {
    "S3_BUCKET":           "justhodl-dashboard-live",
    "S3_KEY":              "data/insider-trades.json",
    "SEC_USER_AGENT":      "JustHodl Research raafouis@gmail.com",
    "MIN_BUY_VALUE_USD":   "25000",
    "WINDOW_DAYS":         "30",
    "CLUSTER_WINDOW_DAYS": "14",
    "CLUSTER_MIN_INSIDERS": "3",
    "BIG_BUY_USD":         "1000000",
}

lam = boto3.client("lambda", region_name=REGION)
eb = boto3.client("events", region_name=REGION)


def build_zip() -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        for f in SOURCE_DIR.rglob("*"):
            if f.is_file() and "__pycache__" not in str(f) and not f.name.endswith(".pyc"):
                z.write(f, str(f.relative_to(SOURCE_DIR)))
    return buf.getvalue()


def function_exists() -> bool:
    try:
        lam.get_function(FunctionName=FN_NAME)
        return True
    except ClientError as e:
        if e.response["Error"]["Code"] == "ResourceNotFoundException":
            return False
        raise


def create_function(zip_bytes: bytes, r):
    lam.create_function(
        FunctionName=FN_NAME,
        Runtime="python3.12",
        Role=ROLE_ARN,
        Handler="lambda_function.lambda_handler",
        Code={"ZipFile": zip_bytes},
        Description="SEC EDGAR Form 4 insider-trades pipeline (every 30 min).",
        Timeout=300,
        MemorySize=512,
        Architectures=["x86_64"],
        Environment={"Variables": ENV_VARS},
    )
    lam.get_waiter("function_active").wait(
        FunctionName=FN_NAME, WaiterConfig={"Delay": 2, "MaxAttempts": 30},
    )
    r.ok(f"  ✓ created Lambda {FN_NAME}")

    # Reserved concurrency = 1 (single-flight; rolling-window S3 file)
    try:
        lam.put_function_concurrency(FunctionName=FN_NAME, ReservedConcurrentExecutions=1)
        r.ok(f"  ✓ reserved concurrency = 1")
    except ClientError as e:
        r.warn(f"  reserved concurrency: {e}")

    # Function URL (no auth — caller is the EB rule + manual triggers)
    try:
        url_resp = lam.create_function_url_config(
            FunctionName=FN_NAME,
            AuthType="NONE",
            Cors={
                "AllowCredentials": False,
                "AllowHeaders": ["content-type"],
                "AllowMethods": ["*"],
                "AllowOrigins": ["*"],
                "MaxAge": 86400,
            },
        )
        r.ok(f"  ✓ Function URL: {url_resp['FunctionUrl']}")
    except ClientError as e:
        if "ResourceConflictException" in str(e):
            r.log(f"  Function URL already configured")
        else:
            r.warn(f"  Function URL: {e}")

    # Public invoke permission (for the URL)
    try:
        lam.add_permission(
            FunctionName=FN_NAME,
            StatementId="FunctionUrlAllowPublicAccess",
            Action="lambda:InvokeFunctionUrl",
            Principal="*",
            FunctionUrlAuthType="NONE",
        )
    except ClientError as e:
        if "ResourceConflictException" not in str(e):
            r.warn(f"  add public permission: {e}")


def update_function(zip_bytes: bytes, r):
    lam.update_function_code(FunctionName=FN_NAME, ZipFile=zip_bytes)
    lam.get_waiter("function_updated").wait(
        FunctionName=FN_NAME, WaiterConfig={"Delay": 2, "MaxAttempts": 30},
    )
    lam.update_function_configuration(
        FunctionName=FN_NAME,
        Environment={"Variables": ENV_VARS},
        Timeout=300,
        MemorySize=512,
    )
    r.ok(f"  ✓ updated Lambda {FN_NAME}")


def ensure_eb_rule(r):
    fn_arn = f"arn:aws:lambda:{REGION}:{ACCOUNT}:function:{FN_NAME}"

    try:
        existing = eb.describe_rule(Name=EB_RULE_NAME)
        r.log(f"  EB rule exists: state={existing['State']}, schedule={existing.get('ScheduleExpression')}")
        if existing["State"] == "DISABLED":
            eb.enable_rule(Name=EB_RULE_NAME)
            r.ok(f"  ✓ enabled disabled rule")
    except ClientError as e:
        if e.response["Error"]["Code"] == "ResourceNotFoundException":
            eb.put_rule(
                Name=EB_RULE_NAME,
                ScheduleExpression="rate(30 minutes)",
                State="ENABLED",
                Description=f"Run {FN_NAME} every 30 min",
            )
            r.ok(f"  ✓ created EB rule {EB_RULE_NAME}")
        else:
            raise

    eb.put_targets(Rule=EB_RULE_NAME, Targets=[{"Id": "1", "Arn": fn_arn}])
    r.ok(f"  ✓ EB target → {FN_NAME}")

    # Lambda must allow EB to invoke
    rule_arn = f"arn:aws:events:{REGION}:{ACCOUNT}:rule/{EB_RULE_NAME}"
    sid = f"AllowEB-{EB_RULE_NAME}-{int(time.time())}"[:64]
    try:
        lam.add_permission(
            FunctionName=FN_NAME,
            StatementId=sid,
            Action="lambda:InvokeFunction",
            Principal="events.amazonaws.com",
            SourceArn=rule_arn,
        )
        r.ok(f"  ✓ added invoke permission ({sid})")
    except ClientError as e:
        if "ResourceConflictException" in str(e):
            r.log(f"  invoke permission already exists")
        else:
            r.warn(f"  add_permission: {e}")


def smoke_test_invocation(r):
    """Run once synchronously to verify the pipeline works end-to-end."""
    r.log(f"  Triggering smoke-test invocation (this populates initial data)…")
    try:
        resp = lam.invoke(
            FunctionName=FN_NAME,
            InvocationType="RequestResponse",
            Payload=b'{"source":"deploy.smoke_test"}',
        )
        status = resp["StatusCode"]
        body = json.loads(resp["Payload"].read().decode("utf-8"))
        r.log(f"  StatusCode: {status}")

        if "FunctionError" in resp:
            r.fail(f"  ✗ FunctionError: {resp['FunctionError']}")
            r.log(f"  Body: {json.dumps(body)[:500]}")
            return False

        # Parse the Lambda's own JSON body
        if "body" in body:
            try:
                inner = json.loads(body["body"])
                stats = inner.get("stats", {})
                r.ok(f"  ✓ smoke test passed")
                r.log(f"     buys:        {stats.get('total_buys', '?')}")
                r.log(f"     value:       ${stats.get('total_value_usd', 0):,.0f}")
                r.log(f"     companies:   {stats.get('unique_companies', '?')}")
                r.log(f"     clusters:    {stats.get('cluster_count', '?')}")
                r.log(f"     duration:    {stats.get('fetch_duration_s', '?')}s")
                r.log(f"     errors:      {stats.get('fetch_errors', 0)}")
                return True
            except Exception:
                r.log(f"  Body (raw): {body.get('body', '')[:300]}")
        return True
    except ClientError as e:
        r.fail(f"  ✗ invoke failed: {e}")
        return False


def main():
    with report("create_insider_trades_lambda") as r:
        r.heading("Create justhodl-insider-trades Lambda + EB rule")

        zip_bytes = build_zip()
        r.log(f"  zip: {len(zip_bytes)} bytes")

        r.section("1. Lambda function")
        if function_exists():
            r.log(f"  Lambda exists — updating code + config")
            update_function(zip_bytes, r)
        else:
            r.log(f"  Lambda missing — creating")
            create_function(zip_bytes, r)

        r.section("2. EB rule + permissions")
        ensure_eb_rule(r)

        r.section("3. Smoke test")
        smoke_test_invocation(r)

        r.section("4. Next steps")
        r.log("  - Frontend page /insiders.html consumes data/insider-trades.json")
        r.log("  - Health monitor will pick up the new file via expectations.py")
        r.log("  - Future code changes auto-deploy through deploy-lambdas.yml")


if __name__ == "__main__":
    main()
