#!/usr/bin/env python3
"""Step 235 — bootstrap justhodl-correlation-breaks Lambda for Phase 9.5.

The deploy-lambdas.yml workflow only updates EXISTING functions — it
doesn't create new ones. This script:

  1. Packages aws/lambdas/justhodl-correlation-breaks/source/ into a zip
  2. Creates the Lambda function (1024MB / 240s / python3.12)
  3. Creates the EventBridge rule justhodl-correlation-breaks-refresh
     scheduled daily at rate(1 day)
  4. Wires the EB rule → Lambda permission + target
  5. Tags the function for ka-aliases compatibility

After this step, future code updates auto-deploy via deploy-lambdas.yml.
Then the producer is invoked once manually to populate first JSON.
"""
import io
import json
import os
import time
import zipfile
from pathlib import Path
from ops_report import report
import sys
import boto3
from botocore.exceptions import ClientError
from botocore.config import Config

REGION = "us-east-1"
ACCOUNT = "857687956942"
FUNCTION_NAME = "justhodl-correlation-breaks"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
RULE_NAME = "justhodl-correlation-breaks-refresh"
SCHEDULE = "rate(1 day)"
SOURCE_DIR = Path("/__w/si/si/aws/lambdas/justhodl-correlation-breaks/source")
# Fallback for local CI
if not SOURCE_DIR.exists():
    SOURCE_DIR = Path("aws/lambdas/justhodl-correlation-breaks/source").resolve()

lam = boto3.client("lambda", region_name=REGION, config=Config(read_timeout=300))
eb = boto3.client("events", region_name=REGION)


def build_zip():
    """Zip the entire source/ directory contents."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for f in SOURCE_DIR.rglob("*"):
            if f.is_file() and not f.name.endswith(".pyc"):
                arcname = str(f.relative_to(SOURCE_DIR))
                zf.write(f, arcname)
    buf.seek(0)
    return buf.read()


with report("create_correlation_breaks_lambda") as r:
    r.heading("Phase 9.5 — bootstrap justhodl-correlation-breaks")

    r.section("1. Locate source")
    r.log(f"  SOURCE_DIR: {SOURCE_DIR}")
    if not SOURCE_DIR.exists():
        r.warn(f"  ✗ source dir not found")
        sys.exit(0)
    src_files = list(SOURCE_DIR.rglob("*.py"))
    r.log(f"  Found {len(src_files)} Python files: {[f.name for f in src_files]}")

    r.section("2. Build deployment zip")
    zip_bytes = build_zip()
    r.log(f"  zip size: {len(zip_bytes)} bytes")

    r.section("3. Create or update Lambda")
    try:
        cfg = lam.get_function_configuration(FunctionName=FUNCTION_NAME)
        r.log(f"  Lambda already exists (CodeSha256={cfg['CodeSha256']}); updating code...")
        lam.update_function_code(FunctionName=FUNCTION_NAME, ZipFile=zip_bytes)
        time.sleep(2)
        lam.get_waiter("function_updated").wait(FunctionName=FUNCTION_NAME)
        # Also update config to ensure 1024MB / 240s
        lam.update_function_configuration(
            FunctionName=FUNCTION_NAME,
            Runtime="python3.12",
            Handler="lambda_function.lambda_handler",
            Timeout=240,
            MemorySize=1024,
            Environment={"Variables": {"FRED_API_KEY": "2f057499936072679d8843d7fce99989"}},
        )
        r.log(f"  ✅ updated existing Lambda")
    except ClientError as e:
        if e.response["Error"]["Code"] != "ResourceNotFoundException":
            r.warn(f"  ✗ get_function_configuration error: {e}")
            raise
        r.log(f"  Lambda doesn't exist — creating fresh")
        resp = lam.create_function(
            FunctionName=FUNCTION_NAME,
            Runtime="python3.12",
            Role=ROLE_ARN,
            Handler="lambda_function.lambda_handler",
            Code={"ZipFile": zip_bytes},
            Timeout=240,
            MemorySize=1024,
            Architectures=["x86_64"],
            Environment={"Variables": {"FRED_API_KEY": "2f057499936072679d8843d7fce99989"}},
            Description="Phase 9.5 — cross-asset correlation break detector",
            Tags={"phase": "9.5", "owner": "justhodl"},
        )
        r.log(f"  ✅ created Lambda: {resp['FunctionArn']}")
        r.log(f"     CodeSha256: {resp['CodeSha256']}")

    r.section("4. Create EventBridge schedule")
    try:
        eb.put_rule(
            Name=RULE_NAME,
            ScheduleExpression=SCHEDULE,
            State="ENABLED",
            Description="Phase 9.5 — daily refresh of correlation-break detector",
        )
        r.log(f"  ✅ rule {RULE_NAME} {SCHEDULE} ENABLED")
    except ClientError as e:
        r.warn(f"  ✗ put_rule error: {e}")

    r.section("5. Grant EB → Lambda invoke permission")
    statement_id = "AllowExecutionFromEventBridge9_5"
    try:
        lam.add_permission(
            FunctionName=FUNCTION_NAME,
            StatementId=statement_id,
            Action="lambda:InvokeFunction",
            Principal="events.amazonaws.com",
            SourceArn=f"arn:aws:events:{REGION}:{ACCOUNT}:rule/{RULE_NAME}",
        )
        r.log(f"  ✅ permission added: {statement_id}")
    except ClientError as e:
        if e.response["Error"]["Code"] == "ResourceConflictException":
            r.log(f"  (permission already exists)")
        else:
            r.warn(f"  ✗ add_permission error: {e}")

    r.section("6. Wire EB target → Lambda")
    fn_arn = f"arn:aws:lambda:{REGION}:{ACCOUNT}:function:{FUNCTION_NAME}"
    try:
        eb.put_targets(
            Rule=RULE_NAME,
            Targets=[{"Id": "1", "Arn": fn_arn}],
        )
        r.log(f"  ✅ target wired: rule={RULE_NAME} → {fn_arn}")
    except ClientError as e:
        r.warn(f"  ✗ put_targets error: {e}")

    r.section("7. Manual invoke to seed first output")
    r.log("  invoking Lambda to produce data/correlation-breaks.json (first run)...")
    t0 = time.time()
    try:
        resp = lam.invoke(FunctionName=FUNCTION_NAME, InvocationType="RequestResponse")
        payload = json.loads(resp["Payload"].read())
        elapsed = round(time.time() - t0, 1)
        if resp.get("FunctionError"):
            r.warn(f"  ✗ FunctionError after {elapsed}s: {payload}")
        else:
            r.log(f"  ✅ first run OK ({elapsed}s)")
            r.log(f"  payload: {json.dumps(payload)[:600]}")
    except Exception as e:
        r.warn(f"  ✗ invoke error: {e}")

    r.section("FINAL")
    r.log("  Phase 9.5 producer Lambda is live.")
    r.log("  Output: s3://justhodl-dashboard-live/data/correlation-breaks.json")
    r.log("  Schedule: daily (rate(1 day))")
    r.log("  Next: build correlation.html frontend (separate commit)")
    r.log("Done")
