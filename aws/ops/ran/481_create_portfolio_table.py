#!/usr/bin/env python3
"""Step 481 — Create justhodl-portfolio DynamoDB table for #9 Portfolio Tracker.

Schema:
  Table: justhodl-portfolio  (single-user, on-demand billing)
    pk (HASH, S)   "POSITION" | "WATCHLIST" | "STOPLOSS" | "META"
    sk (RANGE, S)  symbol (ticker), or "config" for META

  Item types:
    pk=POSITION   long/short equity positions
                    attrs: symbol, qty, cost_basis_per_share, cost_basis_total,
                           position_type (LONG|SHORT), stop_loss, target_weight_pct,
                           sector, added_at, notes
    pk=WATCHLIST  manual + auto-synced TIER S/A watchlist
                    attrs: symbol, source (MANUAL|AUTO_TIER_S|AUTO_TIER_A), added_at
    pk=STOPLOSS   standalone stop-loss alerts (without position) — optional
                    attrs: symbol, stop_price, qty_hypothetical
    pk=META       table metadata
"""
import json, os, time as _time
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/481_create_portfolio_table.json"
TABLE = "justhodl-portfolio"


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}
    ddb = boto3.client("dynamodb", region_name="us-east-1")

    try:
        desc = ddb.describe_table(TableName=TABLE)
        out["status"] = "already_exists"
        out["existing"] = {
            "item_count": desc["Table"].get("ItemCount"),
            "size_bytes": desc["Table"].get("TableSizeBytes"),
            "created": desc["Table"].get("CreationDateTime"),
            "key_schema": desc["Table"].get("KeySchema"),
            "billing": desc["Table"].get("BillingModeSummary", {}).get("BillingMode"),
        }
    except ddb.exceptions.ResourceNotFoundException:
        ddb.create_table(
            TableName=TABLE,
            KeySchema=[
                {"AttributeName": "pk", "KeyType": "HASH"},
                {"AttributeName": "sk", "KeyType": "RANGE"},
            ],
            AttributeDefinitions=[
                {"AttributeName": "pk", "AttributeType": "S"},
                {"AttributeName": "sk", "AttributeType": "S"},
            ],
            BillingMode="PAY_PER_REQUEST",
            Tags=[
                {"Key": "project", "Value": "justhodl"},
                {"Key": "purpose", "Value": "portfolio-tracker-risk-engine"},
            ],
        )
        ddb.get_waiter("table_exists").wait(TableName=TABLE)
        out["status"] = "created"
        # Initial META item
        ddb_res = boto3.resource("dynamodb", region_name="us-east-1")
        table = ddb_res.Table(TABLE)
        table.put_item(Item={
            "pk": "META", "sk": "config",
            "created_at": datetime.now(timezone.utc).isoformat(),
            "schema_version": "1.0",
            "description": "Portfolio + watchlist + stop-loss tracker for JustHodl.AI",
            "owner": "khalid",
        })
        out["seeded_meta"] = True

    out["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f: json.dump(out, f, indent=2, default=str)


if __name__ == "__main__":
    main()
