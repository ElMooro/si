#!/usr/bin/env python3
"""Step 286 — Provision DynamoDB tables for the Public API auth tier system.

Creates two tables:

  justhodl-api-keys:
    PK: key_hash (S)
    Attrs: tier, owner_email, label, created_at, last_used_at,
           revoked_at, usage_total
    Billing: PAY_PER_REQUEST (no provisioned throughput)
    Backup: continuous (PITR enabled) — keys are critical state

  justhodl-api-rate:
    PK: pk (S) — composite "{key_hash}#{window_type}{epoch}"
    Attrs: count (N), ttl (N), key_hash (S)
    Billing: PAY_PER_REQUEST
    TTL: enabled on attribute 'ttl' (auto-expires hourly/daily windows)

If tables already exist, this script is a no-op (idempotent).
"""
import json
import os
import time
from datetime import datetime, timezone

import boto3
from botocore.exceptions import ClientError

REGION = "us-east-1"
KEYS_TABLE = "justhodl-api-keys"
RATE_TABLE = "justhodl-api-rate"
REPORT_PATH = "aws/ops/reports/286_api_tables_provisioning.json"

ddb = boto3.client("dynamodb", region_name=REGION)


def table_exists(name):
    try:
        ddb.describe_table(TableName=name)
        return True
    except ClientError as e:
        if e.response["Error"]["Code"] == "ResourceNotFoundException":
            return False
        raise


def wait_active(name, max_wait_s=120):
    deadline = time.time() + max_wait_s
    while time.time() < deadline:
        d = ddb.describe_table(TableName=name)
        status = d["Table"]["TableStatus"]
        if status == "ACTIVE":
            return True
        time.sleep(2)
    return False


def create_keys_table():
    if table_exists(KEYS_TABLE):
        return {"status": "already_exists"}
    ddb.create_table(
        TableName=KEYS_TABLE,
        BillingMode="PAY_PER_REQUEST",
        AttributeDefinitions=[
            {"AttributeName": "key_hash", "AttributeType": "S"},
        ],
        KeySchema=[
            {"AttributeName": "key_hash", "KeyType": "HASH"},
        ],
        Tags=[
            {"Key": "Project", "Value": "JustHodl.AI"},
            {"Key": "Component", "Value": "PublicAPI-Auth"},
        ],
    )
    if not wait_active(KEYS_TABLE):
        return {"status": "created_but_timed_out_waiting"}
    # Enable PITR for backups
    try:
        ddb.update_continuous_backups(
            TableName=KEYS_TABLE,
            PointInTimeRecoverySpecification={"PointInTimeRecoveryEnabled": True},
        )
        return {"status": "created", "pitr": "enabled"}
    except Exception as e:
        return {"status": "created", "pitr_err": str(e)[:200]}


def create_rate_table():
    if table_exists(RATE_TABLE):
        return {"status": "already_exists"}
    ddb.create_table(
        TableName=RATE_TABLE,
        BillingMode="PAY_PER_REQUEST",
        AttributeDefinitions=[
            {"AttributeName": "pk", "AttributeType": "S"},
        ],
        KeySchema=[
            {"AttributeName": "pk", "KeyType": "HASH"},
        ],
        Tags=[
            {"Key": "Project", "Value": "JustHodl.AI"},
            {"Key": "Component", "Value": "PublicAPI-RateLimit"},
        ],
    )
    if not wait_active(RATE_TABLE):
        return {"status": "created_but_timed_out_waiting"}
    # Enable TTL on the 'ttl' attribute — DynamoDB auto-expires expired rows
    try:
        ddb.update_time_to_live(
            TableName=RATE_TABLE,
            TimeToLiveSpecification={
                "Enabled": True,
                "AttributeName": "ttl",
            },
        )
        return {"status": "created", "ttl": "enabled"}
    except Exception as e:
        return {"status": "created", "ttl_err": str(e)[:200]}


def main():
    started = time.time()
    out = {"started": datetime.now(timezone.utc).isoformat()}
    try:
        out["keys_table"] = create_keys_table()
        out["rate_table"] = create_rate_table()
        out["duration_s"] = round(time.time() - started, 1)
    except Exception as e:
        import traceback
        out["fatal_error"] = str(e)
        out["traceback"] = traceback.format_exc()

    os.makedirs(os.path.dirname(REPORT_PATH), exist_ok=True)
    with open(REPORT_PATH, "w") as f:
        json.dump(out, f, indent=2, default=str)
    print(json.dumps(out, indent=2, default=str))
    return 0 if "fatal_error" not in out else 1


if __name__ == "__main__":
    raise SystemExit(main())
