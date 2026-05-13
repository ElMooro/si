#!/usr/bin/env python3
"""Step 488 — create justhodl-trades DDB table for trade journal (#16).

Schema:
  pk (HASH):   "CALL" (all signals)
  sk (RANGE):  "{call_date_iso}#{symbol}#{strategy}"
               e.g. 2026-05-12T22:00:00Z#VRT#TIER_S_CONFLUENCE
               sortable by time, distinct keys per signal type

GSI 1: by-symbol — query all calls for a specific stock
  symbol (HASH), call_timestamp (RANGE)

GSI 2: by-strategy — query all calls of one strategy type
  strategy (HASH), call_timestamp (RANGE)
"""
import json, os
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/488_create_trades_ddb.json"
ddb = boto3.client("dynamodb", region_name="us-east-1")
TABLE = "justhodl-trades"


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}
    try:
        ddb.create_table(
            TableName=TABLE,
            KeySchema=[
                {"AttributeName": "pk", "KeyType": "HASH"},
                {"AttributeName": "sk", "KeyType": "RANGE"},
            ],
            AttributeDefinitions=[
                {"AttributeName": "pk", "AttributeType": "S"},
                {"AttributeName": "sk", "AttributeType": "S"},
                {"AttributeName": "symbol", "AttributeType": "S"},
                {"AttributeName": "call_timestamp", "AttributeType": "S"},
                {"AttributeName": "strategy", "AttributeType": "S"},
            ],
            GlobalSecondaryIndexes=[
                {
                    "IndexName": "by-symbol",
                    "KeySchema": [
                        {"AttributeName": "symbol", "KeyType": "HASH"},
                        {"AttributeName": "call_timestamp", "KeyType": "RANGE"},
                    ],
                    "Projection": {"ProjectionType": "ALL"},
                },
                {
                    "IndexName": "by-strategy",
                    "KeySchema": [
                        {"AttributeName": "strategy", "KeyType": "HASH"},
                        {"AttributeName": "call_timestamp", "KeyType": "RANGE"},
                    ],
                    "Projection": {"ProjectionType": "ALL"},
                },
            ],
            BillingMode="PAY_PER_REQUEST",
            Tags=[
                {"Key": "Project", "Value": "JustHodl"},
                {"Key": "Roadmap", "Value": "16"},
            ],
        )
        ddb.get_waiter("table_exists").wait(TableName=TABLE)
        out["status"] = "created"
    except ddb.exceptions.ResourceInUseException:
        out["status"] = "already_exists"
    except Exception as e:
        out["err"] = str(e)[:500]
    out["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f: json.dump(out, f, indent=2)


if __name__ == "__main__":
    main()
