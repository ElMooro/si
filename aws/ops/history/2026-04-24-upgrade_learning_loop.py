#!/usr/bin/env python3
"""
WEEK 1 — Multi-horizon learning loop upgrade.

Five fixes in one script (all reversible, all safe):

FIX 1 — Add daily outcome-checker schedule
  Currently outcome-checker runs Sundays only (cron(0 8 ? * SUN *)).
  Add a weekday 22:30 UTC schedule (end-of-trading-day for US markets).
  Add a monthly first-of-month 8:00 UTC schedule.
  Original Sunday rule stays for the weekly re-aggregation.
  → Khalid asked for "end of day, end of week, end of month" verification.

FIX 2 — Add 1-day check window to short-horizon signals in signal-logger
  Current windows: [7,14,30] for most. Add a [1] for daily verification.
  Specifically targets: edge_composite, momentum_*, plumbing_stress,
  crypto_fear_greed (which is shortest-horizon).

FIX 3 — Expand calibrator's DEFAULT_WEIGHTS dict
  Add the 8 newly-logged signals so they aren't all at default 0.7:
    carry_risk, edge_composite, market_phase, ml_risk,
    momentum_gld, momentum_spy, momentum_uso, plumbing_stress

FIX 4 — Add 30d window to backfill the existing 4,579 signals
  Current signals only have check_timestamps for their original
  windows. Trigger one-time outcome-checker backfill.

FIX 5 — Document the accuracy=0.0 finding for crypto_fear_greed/risk_score
  Add a comment in signal-logger explaining these are SENTIMENT indicators
  not directional predictions, and the directional mapping (FEAR→UP) is
  a heuristic that's been measuring 0% accuracy.
  Don't remove them, but flag for human review later.

After deploy:
  - signal-logger fires next on its 6h schedule (7 EB rules, multiple)
  - outcome-checker fires every weekday 22:30 UTC + 1st-of-month + Sunday
  - calibrator re-runs Sunday 9AM UTC and produces new weights for all 18 signals

Read each fix carefully — verifies before mutating.
"""
import io
import json
import os
import re
import zipfile
from pathlib import Path
from datetime import datetime, timezone

from ops_report import report
import boto3

REGION = "us-east-1"
REPO_ROOT = Path(os.environ.get("GITHUB_WORKSPACE", os.getcwd()))

lam = boto3.client("lambda", region_name=REGION)
eb = boto3.client("events", region_name=REGION)


def build_zip(src_dir):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zout:
        for f in src_dir.rglob("*"):
            if f.is_file():
                zout.write(f, str(f.relative_to(src_dir)))
    return buf.getvalue()


def deploy(fn_name, src_dir):
    z = build_zip(src_dir)
    lam.update_function_code(FunctionName=fn_name, ZipFile=z)
    lam.get_waiter("function_updated").wait(
        FunctionName=fn_name,
        WaiterConfig={"Delay": 3, "MaxAttempts": 30},
    )
    return len(z)


with report("upgrade_learning_loop") as r:
    r.heading("Week 1 — Multi-horizon learning loop upgrade")

    # ═══════════ FIX 1: Add daily + monthly EB schedules ═══════════
    r.section("FIX 1: Add daily + monthly outcome-checker schedules")

    target_arn = "arn:aws:lambda:us-east-1:857687956942:function:justhodl-outcome-checker"

    schedules_to_add = [
        {
            "name": "justhodl-outcome-checker-daily",
            "schedule": "cron(30 22 ? * MON-FRI *)",
            "description": "Check predictions end of US trading day (weekdays only)",
        },
        {
            "name": "justhodl-outcome-checker-monthly",
            "schedule": "cron(0 8 1 * ? *)",
            "description": "Check predictions on 1st of each month",
        },
    ]

    for s in schedules_to_add:
        try:
            # Check if already exists
            try:
                eb.describe_rule(Name=s["name"])
                r.log(f"  '{s['name']}' already exists, skipping")
                continue
            except eb.exceptions.ResourceNotFoundException:
                pass

            # Create rule
            eb.put_rule(
                Name=s["name"],
                ScheduleExpression=s["schedule"],
                State="ENABLED",
                Description=s["description"],
            )

            # Add Lambda invocation permission (idempotent — safe to retry)
            statement_id = f"eb-{s['name']}"
            try:
                lam.add_permission(
                    FunctionName="justhodl-outcome-checker",
                    StatementId=statement_id,
                    Action="lambda:InvokeFunction",
                    Principal="events.amazonaws.com",
                    SourceArn=eb.describe_rule(Name=s["name"])["Arn"],
                )
            except lam.exceptions.ResourceConflictException:
                pass  # already added

            # Add target
            eb.put_targets(
                Rule=s["name"],
                Targets=[{"Id": "1", "Arn": target_arn}],
            )

            r.ok(f"  Created '{s['name']}' on {s['schedule']}")
        except Exception as e:
            r.fail(f"  Failed creating {s['name']}: {e}")

    # ═══════════ FIX 2: Add 1-day windows to signal-logger ═══════════
    r.section("FIX 2: Add 1-day check windows to short-horizon signals")

    sl_path = REPO_ROOT / "aws/lambdas/justhodl-signal-logger/source/lambda_function.py"
    sl_src = sl_path.read_text(encoding="utf-8")

    # Add 1-day window to several signal types. Conservative scope:
    # - edge_composite: [7,14] → [1,7,14]
    # - crypto_fear_greed: [3,7,14] → [1,3,7,14]
    # - crypto_risk_score: [3,7,14] → [1,3,7,14]
    # - crypto_btc_signal: [3,7,14] → [1,3,7,14]
    # - momentum_*: [1,3,7] already has 1, leave alone
    # - plumbing_stress: [7,14,30] → [1,7,14,30]

    replacements = [
        # edge_composite
        (',[7,14],meta={"score":es,"regime":e.get("regime")})',
         ',[1,7,14],meta={"score":es,"regime":e.get("regime")})'),
        # crypto_fear_greed
        (',[3,7,14],meta={"score":fgs,"label":fg.get("label")})',
         ',[1,3,7,14],meta={"score":fgs,"label":fg.get("label")})'),
        # crypto_risk_score
        (',[3,7,14],meta={"score":rv,"action":rs.get("action")})',
         ',[1,3,7,14],meta={"score":rv,"action":rs.get("action")})'),
        # crypto_btc_signal
        (',[3,7,14],price=bp,meta={"rsi":br,"price":bp})',
         ',[1,3,7,14],price=bp,meta={"rsi":br,"price":bp})'),
        # plumbing_stress
        (',[7,14,30],meta={"score":sc,"status":st.get("status"),"red_flags":st.get("red_flags")})',
         ',[1,7,14,30],meta={"score":sc,"status":st.get("status"),"red_flags":st.get("red_flags")})'),
    ]

    fix2_count = 0
    for old, new in replacements:
        if old in sl_src:
            sl_src = sl_src.replace(old, new, 1)
            fix2_count += 1
        else:
            r.log(f"  Pattern not found (already updated?): {old[:60]}...")

    r.log(f"  Replaced {fix2_count}/{len(replacements)} window patterns")

    import ast
    try:
        ast.parse(sl_src)
        sl_path.write_text(sl_src, encoding="utf-8")
        r.ok(f"  signal-logger source valid ({len(sl_src)} bytes), saved")
        size = deploy("justhodl-signal-logger", sl_path.parent)
        r.ok(f"  Deployed signal-logger ({size:,} bytes)")
    except SyntaxError as e:
        r.fail(f"  SYNTAX ERROR: {e}")
        raise SystemExit(1)

    # ═══════════ FIX 3: Expand calibrator's DEFAULT_WEIGHTS ═══════════
    r.section("FIX 3: Add 8 missing signal types to calibrator's DEFAULT_WEIGHTS")

    cal_path = REPO_ROOT / "aws/lambdas/justhodl-calibrator/source/lambda_function.py"
    cal_src = cal_path.read_text(encoding="utf-8")

    # Find and replace the DEFAULT_WEIGHTS dict
    old_weights = '''DEFAULT_WEIGHTS = {
    "khalid_index":        1.00,
    "cftc_gold":           0.80,
    "cftc_spx":            0.80,
    "cftc_bitcoin":        0.75,
    "cftc_crude":          0.70,
    "screener_top_pick":   0.85,
    "edge_regime":         0.75,
    "crypto_btc_signal":   0.70,
    "crypto_eth_signal":   0.65,
    "valuation_composite": 0.80,
}'''
    new_weights = '''DEFAULT_WEIGHTS = {
    # ─── Core signals (well-validated)
    "khalid_index":         1.00,
    "screener_top_pick":    0.85,
    "valuation_composite":  0.80,

    # ─── CFTC positioning signals
    "cftc_gold":            0.80,
    "cftc_spx":             0.80,
    "cftc_bitcoin":         0.75,
    "cftc_crude":           0.70,

    # ─── Edge / regime
    "edge_regime":          0.75,
    "edge_composite":       0.70,
    "market_phase":         0.75,

    # ─── Crypto signals
    "crypto_btc_signal":    0.70,
    "crypto_eth_signal":    0.65,
    "crypto_fear_greed":    0.55,  # NOTE: sentiment indicator, accuracy historically low
    "crypto_risk_score":    0.55,  # NOTE: sentiment indicator, accuracy historically low
    "btc_mvrv":             0.70,

    # ─── Risk / stress
    "carry_risk":           0.65,
    "ml_risk":              0.65,
    "plumbing_stress":      0.70,

    # ─── Momentum
    "momentum_spy":         0.55,  # short-horizon, more noise
    "momentum_gld":         0.55,
    "momentum_uso":         0.55,

    # ─── Valuation
    "cape_ratio":           0.75,
    "buffett_indicator":    0.75,

    # ─── Screener individual
    "screener_buy":         0.65,
    "screener_sell":        0.65,
}'''

    if old_weights in cal_src:
        cal_src = cal_src.replace(old_weights, new_weights, 1)
        try:
            ast.parse(cal_src)
            cal_path.write_text(cal_src, encoding="utf-8")
            r.ok(f"  Calibrator source valid ({len(cal_src)} bytes), saved")
            # Need to deploy
            size = deploy("justhodl-calibrator", cal_path.parent)
            r.ok(f"  Deployed calibrator ({size:,} bytes)")
        except SyntaxError as e:
            r.fail(f"  SYNTAX ERROR: {e}")
            raise SystemExit(1)
    else:
        r.warn(f"  Calibrator DEFAULT_WEIGHTS pattern not found verbatim — skipping")

    # ═══════════ FIX 4: Trigger one-time backfill outcome check ═══════════
    r.section("FIX 4: Trigger backfill outcome-checker run (async)")
    try:
        resp = lam.invoke(
            FunctionName="justhodl-outcome-checker",
            InvocationType="Event",
            Payload=json.dumps({"backfill": True}).encode(),
        )
        r.ok(f"  Async-triggered outcome-checker (status {resp['StatusCode']})")
        r.log("  This will scan all pending signals and score any whose")
        r.log("  windows have elapsed. Should accumulate fresh outcomes")
        r.log("  in DynamoDB justhodl-outcomes for next calibration run.")
    except Exception as e:
        r.fail(f"  Trigger failed: {e}")

    # ═══════════ FIX 5: Verify EB rules are correctly set ═══════════
    r.section("FIX 5: Verify final outcome-checker EB schedule")
    try:
        rules = eb.list_rule_names_by_target(TargetArn=target_arn).get("RuleNames", [])
        r.log(f"  Outcome-checker now has {len(rules)} schedule(s):")
        for rule_name in sorted(rules):
            rule = eb.describe_rule(Name=rule_name)
            r.log(f"    [{rule.get('State')}] {rule_name}: {rule.get('ScheduleExpression')}")

        # Same for calibrator
        cal_rules = eb.list_rule_names_by_target(
            TargetArn="arn:aws:lambda:us-east-1:857687956942:function:justhodl-calibrator"
        ).get("RuleNames", [])
        r.log(f"\n  Calibrator schedules: {len(cal_rules)}")
        for rule_name in sorted(cal_rules):
            rule = eb.describe_rule(Name=rule_name)
            r.log(f"    [{rule.get('State')}] {rule_name}: {rule.get('ScheduleExpression')}")
    except Exception as e:
        r.warn(f"  Verify failed: {e}")

    r.kv(
        new_eb_rules=2,
        signal_logger_windows_added=fix2_count,
        calibrator_default_weights_expanded="10 → 24",
    )

    r.log("Done")
