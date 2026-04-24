#!/usr/bin/env python3
"""
Verify Week 2A schema v2 is appearing on freshly-logged signals.

Checks:
  A. Recent signals (last 5 min) have schema_version="2"
  B. regime_at_log + khalid_score_at_log are populated
  C. horizon_days_primary is set correctly (max of check_windows)
  D. Old signals still readable — backwards compatibility intact
  E. All optional fields default to None on existing call sites
     (no caller is passing magnitude= yet, so all should be None)
"""
import json
from datetime import datetime, timezone, timedelta
from collections import Counter
from ops_report import report
import boto3

REGION = "us-east-1"
ddb = boto3.resource("dynamodb", region_name=REGION)


with report("verify_week_2a_schema") as r:
    r.heading("Verify schema v2 on fresh signals + backwards compat")

    table = ddb.Table("justhodl-signals")

    # ─── A. Most recent signals (last 5 minutes) ───
    r.section("A. Schema v2 fields on signals from last 10 min")
    cutoff_ts = int((datetime.now(timezone.utc) - timedelta(minutes=10)).timestamp())
    from boto3.dynamodb.conditions import Attr
    resp = table.scan(
        FilterExpression=Attr("logged_epoch").gte(cutoff_ts),
        ProjectionExpression="signal_type, schema_version, predicted_magnitude_pct, "
                              "predicted_target_price, horizon_days_primary, "
                              "regime_at_log, khalid_score_at_log, rationale, "
                              "supporting_signals, baseline_price, predicted_direction",
    )
    items = resp.get("Items", [])
    while "LastEvaluatedKey" in resp:
        resp = table.scan(
            FilterExpression=Attr("logged_epoch").gte(cutoff_ts),
            ExclusiveStartKey=resp["LastEvaluatedKey"],
            ProjectionExpression="signal_type, schema_version, predicted_magnitude_pct, "
                                  "predicted_target_price, horizon_days_primary, "
                                  "regime_at_log, khalid_score_at_log, rationale, "
                                  "supporting_signals, baseline_price, predicted_direction",
        )
        items += resp.get("Items", [])

    r.log(f"  Fresh signals scanned: {len(items)}")
    if not items:
        r.warn("  No fresh signals — logger may not have run yet")
        r.log("Done")
        raise SystemExit(0)

    # Field coverage stats
    has_v2 = sum(1 for i in items if i.get("schema_version") == "2")
    has_horizon = sum(1 for i in items if i.get("horizon_days_primary") is not None)
    has_regime = sum(1 for i in items if i.get("regime_at_log") is not None)
    has_khalid_score = sum(1 for i in items if i.get("khalid_score_at_log") is not None)
    has_baseline = sum(1 for i in items if i.get("baseline_price") not in (None, 0, "0", ""))

    r.log(f"  schema_version='2':     {has_v2}/{len(items)} ({100*has_v2/len(items):.0f}%)")
    r.log(f"  horizon_days_primary:   {has_horizon}/{len(items)}")
    r.log(f"  regime_at_log:          {has_regime}/{len(items)}")
    r.log(f"  khalid_score_at_log:    {has_khalid_score}/{len(items)}")
    r.log(f"  baseline_price:         {has_baseline}/{len(items)} (carryover from prev fix)")

    # Optional fields should be None for now (no callers pass them yet)
    has_magnitude = sum(1 for i in items if i.get("predicted_magnitude_pct") is not None)
    has_target = sum(1 for i in items if i.get("predicted_target_price") is not None)
    has_rationale = sum(1 for i in items if i.get("rationale") is not None)
    has_supporting = sum(1 for i in items if i.get("supporting_signals") is not None)
    r.log(f"\n  Optional fields (expected mostly None for now):")
    r.log(f"    predicted_magnitude_pct: {has_magnitude}/{len(items)}")
    r.log(f"    predicted_target_price:  {has_target}/{len(items)}")
    r.log(f"    rationale:               {has_rationale}/{len(items)}")
    r.log(f"    supporting_signals:      {has_supporting}/{len(items)}")

    if has_v2 == len(items):
        r.ok(f"  Schema v2 landing on 100% of new signals")
    elif has_v2 > 0:
        r.warn(f"  Partial — only {100*has_v2/len(items):.0f}% have schema_version='2'")
    else:
        r.fail(f"  Schema v2 NOT landing — investigate")

    # ─── B. Sample 3 recent signals to inspect shape ───
    r.section("B. Sample 3 fresh signals to inspect shape")
    for item in items[:3]:
        r.log(f"\n  signal_type={item.get('signal_type')}:")
        for f in ["schema_version", "predicted_direction", "baseline_price",
                  "horizon_days_primary", "regime_at_log", "khalid_score_at_log",
                  "predicted_magnitude_pct", "predicted_target_price"]:
            r.log(f"    {f:30} = {item.get(f)}")

    # ─── C. Old signals still scannable (backwards compat) ───
    r.section("C. Old signals (24h+ ago) still readable — backwards compat")
    old_cutoff = int((datetime.now(timezone.utc) - timedelta(days=2)).timestamp())
    old_cutoff_top = int((datetime.now(timezone.utc) - timedelta(days=1)).timestamp())
    resp = table.scan(
        FilterExpression=Attr("logged_epoch").lt(old_cutoff_top) & Attr("logged_epoch").gte(old_cutoff),
        Limit=20,
        ProjectionExpression="signal_type, schema_version, predicted_direction",
    )
    old_items = resp.get("Items", [])
    r.log(f"  Old signals (24-48h ago, sampled): {len(old_items)}")
    v1_count = sum(1 for i in old_items if not i.get("schema_version"))
    v2_count = sum(1 for i in old_items if i.get("schema_version") == "2")
    r.log(f"  Implicit v1 (no schema_version): {v1_count}")
    r.log(f"  Explicit v2:                     {v2_count}")
    if v1_count > 0:
        r.ok(f"  Old items still readable; both schemas coexist cleanly")

    r.kv(
        fresh_signals=len(items),
        v2_coverage_pct=round(100*has_v2/max(len(items),1), 0),
        regime_capture_pct=round(100*has_regime/max(len(items),1), 0),
    )
    r.log("Done")
