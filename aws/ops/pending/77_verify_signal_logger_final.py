#!/usr/bin/env python3
"""
Step 77 — Final loop closure: verify signal-logger now logs ml_risk
and carry_risk signals with real values (not 0).

Before tonight: ml_risk=0, carry_risk=0 for every run since the CF
migration. After step 75: source dict has real values. Trigger fresh
signal-logger run, then check the most recent ml_risk and carry_risk
items in DynamoDB.
"""
import json
from datetime import datetime, timezone, timedelta
from ops_report import report
import boto3

REGION = "us-east-1"
ddb = boto3.resource("dynamodb", region_name=REGION)
lam = boto3.client("lambda", region_name=REGION)


with report("verify_signal_logger_final") as r:
    r.heading("Verify signal-logger writes real ml_risk + carry_risk values")

    # Trigger fresh signal-logger run
    r.section("Trigger fresh signal-logger")
    try:
        resp = lam.invoke(FunctionName="justhodl-signal-logger", InvocationType="RequestResponse")
        # Wait for completion since this is sync
        body = resp.get("Payload").read().decode()
        r.log(f"  Status: {resp['StatusCode']}")
        r.log(f"  Body:   {body[:300]}")
    except Exception as e:
        r.fail(f"  {e}")

    # Now scan recent signals for ml_risk and carry_risk
    r.section("Last 5 min: ml_risk + carry_risk signal_value field")
    cutoff = int((datetime.now(timezone.utc) - timedelta(minutes=5)).timestamp())
    table = ddb.Table("justhodl-signals")
    from boto3.dynamodb.conditions import Attr

    for sig_type in ["ml_risk", "carry_risk"]:
        resp = table.scan(
            FilterExpression=Attr("logged_epoch").gte(cutoff) & Attr("signal_type").eq(sig_type),
            ProjectionExpression="signal_type, signal_value, predicted_direction, confidence, "
                                  "metadata, baseline_price, rationale, regime_at_log, "
                                  "khalid_score_at_log, schema_version, logged_at",
        )
        items = resp.get("Items", [])
        r.log(f"\n  {sig_type}: {len(items)} signals in last 5 min")
        for item in items[:3]:
            r.log(f"    signal_value: {item.get('signal_value')}")
            r.log(f"    predicted_dir: {item.get('predicted_direction')}")
            r.log(f"    confidence: {item.get('confidence')}")
            meta = item.get("metadata", {})
            if isinstance(meta, dict):
                r.log(f"    metadata.score: {meta.get('score')}")
            r.log(f"    baseline_price: {item.get('baseline_price')}")
            r.log(f"    schema_version: {item.get('schema_version')}")
            r.log(f"    regime_at_log: {item.get('regime_at_log')}")

    r.section("Compare to dead-zero past")
    # Sample a few older ml_risk/carry_risk to confirm they were 0
    old_cutoff_high = int((datetime.now(timezone.utc) - timedelta(hours=6)).timestamp())
    old_cutoff_low = int((datetime.now(timezone.utc) - timedelta(hours=24)).timestamp())
    for sig_type in ["ml_risk", "carry_risk"]:
        resp = table.scan(
            FilterExpression=Attr("logged_epoch").lt(old_cutoff_high) &
                             Attr("logged_epoch").gte(old_cutoff_low) &
                             Attr("signal_type").eq(sig_type),
            ProjectionExpression="signal_value, metadata",
            Limit=5,
        )
        items = resp.get("Items", [])
        r.log(f"\n  {sig_type} (6-24h ago, sample of {len(items)}):")
        zeros = 0
        for item in items:
            sv = item.get("signal_value", "?")
            meta = item.get("metadata", {}) or {}
            score = meta.get("score") if isinstance(meta, dict) else None
            r.log(f"    signal_value={sv}, metadata.score={score}")
            if str(sv) in ("0", "0.0") or score in (0, 0.0, "0"):
                zeros += 1
        if zeros > 0:
            r.log(f"  → {zeros}/{len(items)} were zero (the bug)")

    r.log("Done")

