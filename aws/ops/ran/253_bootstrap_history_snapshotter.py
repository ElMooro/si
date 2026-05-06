#!/usr/bin/env python3
"""Step 253 — Bootstrap justhodl-history-snapshotter end-to-end.

Creates (idempotent — safe to re-run):
  1. DynamoDB table justhodl-history (PAY_PER_REQUEST, TTL on `ttl`)
  2. Lambda function justhodl-history-snapshotter (zip-package the source dir)
  3. EventBridge rule justhodl-history-snapshotter-5m (rate 5 minutes)
  4. EventBridge → Lambda permission + target
  5. Initial synchronous invoke to populate first snapshots
  6. Verify: DDB table active, rule enabled, first writes landed

Reports to aws/ops/reports/253_history_bootstrap.json.
"""
import base64
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
BUCKET = "justhodl-dashboard-live"
LAMBDA_NAME = "justhodl-history-snapshotter"
DDB_TABLE = "justhodl-history"
ROLE_ARN = f"arn:aws:iam::{ACCOUNT_ID}:role/lambda-execution-role"
RULE_NAME = "justhodl-history-snapshotter-5m"
RULE_SCHEDULE = "rate(5 minutes)"
SOURCE_DIR = f"aws/lambdas/{LAMBDA_NAME}/source"
REPORT_PATH = "aws/ops/reports/253_history_bootstrap.json"

lam = boto3.client("lambda", region_name=REGION)
ddb = boto3.client("dynamodb", region_name=REGION)
events = boto3.client("events", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)


def ensure_ddb_table():
    """Create justhodl-history if missing, enable TTL on `ttl` attribute."""
    try:
        d = ddb.describe_table(TableName=DDB_TABLE)["Table"]
        status = d["TableStatus"]
        if status not in ("ACTIVE", "UPDATING"):
            ddb.get_waiter("table_exists").wait(TableName=DDB_TABLE)
        return {"created": False, "status": status}
    except ClientError as e:
        if e.response["Error"]["Code"] != "ResourceNotFoundException":
            raise
    print(f"[253] creating DDB table {DDB_TABLE}…")
    ddb.create_table(
        TableName=DDB_TABLE,
        KeySchema=[
            {"AttributeName": "pk", "KeyType": "HASH"},
            {"AttributeName": "sk", "KeyType": "RANGE"},
        ],
        AttributeDefinitions=[
            {"AttributeName": "pk", "AttributeType": "S"},
            {"AttributeName": "sk", "AttributeType": "S"},
        ],
        BillingMode="PAY_PER_REQUEST",
        Tags=[{"Key": "project", "Value": "justhodl"}, {"Key": "purpose", "Value": "time-series-snapshots"}],
    )
    ddb.get_waiter("table_exists").wait(TableName=DDB_TABLE)
    # Enable TTL
    try:
        ddb.update_time_to_live(
            TableName=DDB_TABLE,
            TimeToLiveSpecification={"Enabled": True, "AttributeName": "ttl"},
        )
        print(f"[253] enabled TTL on {DDB_TABLE}.ttl")
    except Exception as e:
        print(f"[253] warn: TTL enable failed: {e}")
    return {"created": True, "status": "ACTIVE"}


def build_zip():
    """Zip the source dir into bytes for create_function/update_function_code."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for root, _dirs, files in os.walk(SOURCE_DIR):
            for fn in files:
                fpath = os.path.join(root, fn)
                arcname = os.path.relpath(fpath, SOURCE_DIR)
                zf.write(fpath, arcname)
    return buf.getvalue()


def ensure_lambda():
    """Create the Lambda function if it doesn't exist; otherwise update code."""
    zip_bytes = build_zip()
    try:
        cur = lam.get_function(FunctionName=LAMBDA_NAME)
        # Already exists — update code
        print(f"[253] Lambda exists, updating code ({len(zip_bytes):,}b)")
        lam.update_function_code(
            FunctionName=LAMBDA_NAME, ZipFile=zip_bytes, Publish=False,
        )
        # Wait for update to finish
        lam.get_waiter("function_updated").wait(FunctionName=LAMBDA_NAME)
        # Reset env / config
        lam.update_function_configuration(
            FunctionName=LAMBDA_NAME,
            Runtime="python3.12",
            Handler="lambda_function.lambda_handler",
            MemorySize=256,
            Timeout=120,
            Environment={"Variables": {
                "S3_BUCKET": BUCKET,
                "DDB_TABLE": DDB_TABLE,
                "TTL_DAYS": "365",
            }},
        )
        lam.get_waiter("function_updated").wait(FunctionName=LAMBDA_NAME)
        return {"created": False, "arn": cur["Configuration"]["FunctionArn"]}
    except ClientError as e:
        if e.response["Error"]["Code"] != "ResourceNotFoundException":
            raise
    print(f"[253] creating Lambda {LAMBDA_NAME}…")
    resp = lam.create_function(
        FunctionName=LAMBDA_NAME,
        Runtime="python3.12",
        Handler="lambda_function.lambda_handler",
        Role=ROLE_ARN,
        Code={"ZipFile": zip_bytes},
        Description=(
            "Time-series snapshotter: every 5 min reads live data/*.json "
            "feeds, dedups by SHA256, writes to DynamoDB justhodl-history "
            "with 365d TTL"
        ),
        MemorySize=256,
        Timeout=120,
        Environment={"Variables": {
            "S3_BUCKET": BUCKET,
            "DDB_TABLE": DDB_TABLE,
            "TTL_DAYS": "365",
        }},
        Tags={"project": "justhodl", "purpose": "time-series"},
    )
    lam.get_waiter("function_active_v2").wait(FunctionName=LAMBDA_NAME)
    return {"created": True, "arn": resp["FunctionArn"]}


def ensure_eventbridge_rule(lambda_arn):
    """Create EB rule + target + lambda permission. All idempotent."""
    # 1. Create / update rule
    try:
        events.put_rule(
            Name=RULE_NAME,
            ScheduleExpression=RULE_SCHEDULE,
            State="ENABLED",
            Description="Trigger justhodl-history-snapshotter every 5 minutes",
        )
        print(f"[253] put_rule {RULE_NAME} ({RULE_SCHEDULE})")
    except Exception as e:
        print(f"[253] put_rule err: {e}")
        raise

    # 2. Add target (Lambda)
    try:
        events.put_targets(
            Rule=RULE_NAME,
            Targets=[{"Id": "1", "Arn": lambda_arn}],
        )
        print(f"[253] put_targets → {lambda_arn}")
    except Exception as e:
        print(f"[253] put_targets err: {e}")
        raise

    # 3. Permission for EB to invoke Lambda (idempotent — ignore "already exists")
    try:
        lam.add_permission(
            FunctionName=LAMBDA_NAME,
            StatementId="EventBridgeInvoke5Min",
            Action="lambda:InvokeFunction",
            Principal="events.amazonaws.com",
            SourceArn=f"arn:aws:events:{REGION}:{ACCOUNT_ID}:rule/{RULE_NAME}",
        )
        print(f"[253] added Lambda invoke permission for {RULE_NAME}")
    except ClientError as e:
        if e.response["Error"]["Code"] == "ResourceConflictException":
            print(f"[253] permission already exists (ok)")
        else:
            raise


def initial_invoke():
    """Synchronous invoke to write the first batch of snapshots."""
    print(f"[253] invoking {LAMBDA_NAME} synchronously…")
    resp = lam.invoke(
        FunctionName=LAMBDA_NAME, InvocationType="RequestResponse", Payload=b"{}",
    )
    payload = json.loads(resp["Payload"].read())
    func_err = resp.get("FunctionError")
    return {
        "function_error": func_err,
        "payload": payload,
        "status_code": resp.get("StatusCode"),
    }


def verify():
    """Read back the heartbeat + count DDB rows."""
    out = {}
    try:
        hb = json.loads(s3.get_object(
            Bucket=BUCKET, Key="data/history-snapshotter-status.json"
        )["Body"].read())
        out["heartbeat"] = hb
    except Exception as e:
        out["heartbeat_err"] = str(e)
    # Quick scan for total rows (limit 200; this is just a sanity check)
    try:
        resp = ddb.scan(
            TableName=DDB_TABLE,
            Select="COUNT",
            Limit=200,
        )
        out["ddb_row_count_first_200_pages"] = resp.get("Count", 0)
        out["ddb_scanned_count"] = resp.get("ScannedCount", 0)
    except Exception as e:
        out["ddb_scan_err"] = str(e)
    return out


def main():
    started = time.time()
    out = {"started": datetime.now(timezone.utc).isoformat()}
    try:
        out["ddb"] = ensure_ddb_table()
        lam_info = ensure_lambda()
        out["lambda"] = lam_info
        ensure_eventbridge_rule(lam_info["arn"])
        out["rule"] = {"name": RULE_NAME, "schedule": RULE_SCHEDULE, "state": "ENABLED"}
        out["initial_invoke"] = initial_invoke()
        time.sleep(3)
        out["verify"] = verify()
        out["duration_s"] = round(time.time() - started, 1)
    except Exception as e:
        import traceback
        out["fatal_error"] = str(e)
        out["traceback"] = traceback.format_exc()

    os.makedirs(os.path.dirname(REPORT_PATH), exist_ok=True)
    with open(REPORT_PATH, "w") as f:
        json.dump(out, f, indent=2, default=str)
    print(json.dumps(out, indent=2, default=str)[:4000])
    return 0 if "fatal_error" not in out else 1


if __name__ == "__main__":
    raise SystemExit(main())
