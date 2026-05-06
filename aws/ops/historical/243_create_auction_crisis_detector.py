#!/usr/bin/env python3
"""Step 243 — bootstrap justhodl-auction-crisis-detector Lambda (Phase 10).

Pattern follows step 235 (correlation-breaks bootstrap):
  1. Package source/ to zip
  2. Create Lambda (1024MB, 240s, python3.12)
  3. Create EventBridge rule for hourly schedule
  4. Wire EB → Lambda invoke permission + target
  5. Seed-invoke once to populate first data/auction-crisis.json

Background: 9 historical Treasury auction PDFs (covering 2008 GFC, 2020 COVID,
2021 crypto top, 2024 normal market) were analyzed to extract 6 quantified
crisis-pattern signatures. Those signatures are codified in score_indicators().

After this step:
  - Hourly invocation auto-updates data/auction-crisis.json
  - bonds.html (and a new dedicated auction-crisis.html) consume this data
  - When composite score crosses 50/75 thresholds, regime flags ELEVATED/ACUTE
"""
import io
import json
import os
import sys
import time
import zipfile
from pathlib import Path
from ops_report import report
import boto3
from botocore.exceptions import ClientError
from botocore.config import Config

REGION = "us-east-1"
ACCOUNT = "857687956942"
FUNCTION_NAME = "justhodl-auction-crisis-detector"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
RULE_NAME = "justhodl-auction-crisis-refresh"
SCHEDULE = "rate(1 hour)"

SOURCE_DIR = Path("/__w/si/si/aws/lambdas/justhodl-auction-crisis-detector/source")
if not SOURCE_DIR.exists():
    SOURCE_DIR = Path("aws/lambdas/justhodl-auction-crisis-detector/source").resolve()

lam = boto3.client("lambda", region_name=REGION, config=Config(read_timeout=300))
eb = boto3.client("events", region_name=REGION)


def build_zip():
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for f in SOURCE_DIR.rglob("*"):
            if f.is_file() and not f.name.endswith(".pyc"):
                zf.write(f, str(f.relative_to(SOURCE_DIR)))
    buf.seek(0)
    return buf.read()


with report("create_auction_crisis_detector") as r:
    r.heading("Phase 10 — bootstrap auction-crisis-detector")

    r.section("1. Locate source")
    r.log(f"  SOURCE_DIR: {SOURCE_DIR}")
    if not SOURCE_DIR.exists():
        r.warn("  ✗ source dir not found")
        sys.exit(0)
    src_files = list(SOURCE_DIR.rglob("*.py"))
    r.log(f"  Python files: {[f.name for f in src_files]}")

    r.section("2. Build deployment zip")
    zip_bytes = build_zip()
    r.log(f"  zip size: {len(zip_bytes):,} bytes")

    r.section("3. Create or update Lambda")
    try:
        cfg = lam.get_function_configuration(FunctionName=FUNCTION_NAME)
        r.log(f"  Lambda exists (CodeSha256={cfg['CodeSha256']}); updating")
        lam.update_function_code(FunctionName=FUNCTION_NAME, ZipFile=zip_bytes)
        time.sleep(2)
        lam.get_waiter("function_updated").wait(FunctionName=FUNCTION_NAME)
        lam.update_function_configuration(
            FunctionName=FUNCTION_NAME,
            Runtime="python3.12",
            Handler="lambda_function.lambda_handler",
            Timeout=240,
            MemorySize=1024,
            Environment={"Variables": {"FRED_API_KEY": "2f057499936072679d8843d7fce99989"}},
        )
        r.log("  ✅ updated existing Lambda")
    except ClientError as e:
        if e.response["Error"]["Code"] != "ResourceNotFoundException":
            r.warn(f"  ✗ {e}")
            raise
        r.log("  Lambda doesn't exist — creating fresh")
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
            Description="Phase 10 — Treasury auction crisis detector (calibrated from 9 historical PDFs)",
            Tags={"phase": "10", "owner": "justhodl"},
        )
        r.log(f"  ✅ created Lambda: {resp['FunctionArn']}")

    r.section("4. EventBridge schedule")
    try:
        eb.put_rule(
            Name=RULE_NAME, ScheduleExpression=SCHEDULE, State="ENABLED",
            Description="Hourly refresh of auction crisis detector",
        )
        r.log(f"  ✅ rule {RULE_NAME} {SCHEDULE} ENABLED")
    except ClientError as e:
        r.warn(f"  ✗ put_rule: {e}")

    r.section("5. EB → Lambda permission + target")
    try:
        lam.add_permission(
            FunctionName=FUNCTION_NAME,
            StatementId="AllowEventBridgePhase10",
            Action="lambda:InvokeFunction",
            Principal="events.amazonaws.com",
            SourceArn=f"arn:aws:events:{REGION}:{ACCOUNT}:rule/{RULE_NAME}",
        )
        r.log("  ✅ permission added")
    except ClientError as e:
        if e.response["Error"]["Code"] == "ResourceConflictException":
            r.log("  (permission already exists)")
        else:
            r.warn(f"  ✗ {e}")
    try:
        eb.put_targets(
            Rule=RULE_NAME,
            Targets=[{"Id": "1",
                      "Arn": f"arn:aws:lambda:{REGION}:{ACCOUNT}:function:{FUNCTION_NAME}"}],
        )
        r.log("  ✅ EB target wired")
    except ClientError as e:
        r.warn(f"  ✗ put_targets: {e}")

    r.section("6. Wait for Active state, then seed-invoke")
    for i in range(20):
        cfg = lam.get_function_configuration(FunctionName=FUNCTION_NAME)
        if cfg["State"] == "Active":
            break
        time.sleep(3)
    r.log(f"  state: {cfg['State']}")
    if cfg["State"] != "Active":
        r.warn("  ✗ not Active; skipping seed invoke")
        sys.exit(0)
    t0 = time.time()
    try:
        resp = lam.invoke(FunctionName=FUNCTION_NAME, InvocationType="RequestResponse")
        payload = json.loads(resp["Payload"].read())
        dur = round(time.time() - t0, 1)
        if resp.get("FunctionError"):
            r.warn(f"  ✗ FunctionError ({dur}s): {payload}")
        else:
            r.log(f"  ✅ first run OK ({dur}s)")
            r.log(f"  payload: {json.dumps(payload)[:500]}")
    except Exception as e:
        r.warn(f"  ✗ invoke error: {e}")

    r.section("FINAL")
    r.log("  Phase 10 auction-crisis-detector live.")
    r.log("  Output: s3://justhodl-dashboard-live/data/auction-crisis.json")
    r.log("  Schedule: rate(1 hour)")
    r.log("Done")
