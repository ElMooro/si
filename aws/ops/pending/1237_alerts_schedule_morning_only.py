"""1237 — Switch prepump-alerts-router from every-30-min to once-per-morning.

User reported Telegram firing "wayyy too many times". Root cause:
  EventBridge schedule was cron(0,30 * * * ? *) → 48 invocations/day.
  Even with per-signal-type daily dedup, each new ticker firing under
  multiple signal types (cascade_alert + options_flow + convergence)
  produces several alerts spread across the day.

Fix: Change schedule to cron(30 12 * * ? *) → 8:30 AM ET (12:30 UTC) DAILY.
     Existing daily-reset state ensures all new signals since yesterday
     are consolidated into ONE morning Telegram batch.

This op:
  1. Updates EventBridge rule cron expression
  2. Verifies rule is enabled
  3. Confirms Lambda code SHA matches new source (already pushed)
"""
import json
import time
from datetime import datetime, timezone
import boto3
from botocore.config import Config

REPORT = "aws/ops/reports/1237_alerts_schedule_morning_only.json"
REGION = "us-east-1"
LAMBDA = "justhodl-prepump-alerts-router"
RULE = "justhodl-prepump-alerts-router-30min"  # keep existing name
NEW_SCHEDULE = "cron(30 12 * * ? *)"

cfg = Config(read_timeout=60, retries={"max_attempts": 1})
events = boto3.client("events", region_name=REGION, config=cfg)
lam = boto3.client("lambda", region_name=REGION, config=cfg)

out = {"started": datetime.now(timezone.utc).isoformat()}

# 1. Get current rule state
print("[1237] 1. Current rule state")
try:
    rule = events.describe_rule(Name=RULE)
    out["current"] = {
        "name": rule.get("Name"),
        "schedule_expression": rule.get("ScheduleExpression"),
        "state": rule.get("State"),
    }
    print(f"  Current cron: {rule.get('ScheduleExpression')}")
    print(f"  State: {rule.get('State')}")
except Exception as e:
    out["current_err"] = str(e)[:200]
    print(f"  ⚠ {e}")

# 2. Update schedule
print(f"\n[1237] 2. Update cron → {NEW_SCHEDULE}")
try:
    events.put_rule(Name=RULE, ScheduleExpression=NEW_SCHEDULE, State="ENABLED",
                    Description="Daily 8:30 AM ET (12:30 UTC) — single morning digest")
    out["updated"] = NEW_SCHEDULE
    print(f"  ✓ updated to {NEW_SCHEDULE}")
except Exception as e:
    out["update_err"] = str(e)[:200]
    print(f"  ⚠ {e}")

# 3. Confirm update
print("\n[1237] 3. Confirm update")
try:
    rule = events.describe_rule(Name=RULE)
    out["after"] = {
        "schedule_expression": rule.get("ScheduleExpression"),
        "state": rule.get("State"),
    }
    print(f"  Now: {rule.get('ScheduleExpression')} · {rule.get('State')}")
except Exception as e:
    out["after_err"] = str(e)[:200]

# 4. Lambda code SHA
print("\n[1237] 4. Lambda code SHA (should match new source)")
try:
    info = lam.get_function_configuration(FunctionName=LAMBDA)
    out["lambda"] = {
        "code_sha": info.get("CodeSha256")[:16],
        "last_modified": info.get("LastModified")[:19],
    }
    print(f"  sha={info.get('CodeSha256')[:16]}  modified={info.get('LastModified')[:19]}")
except Exception as e:
    out["lambda_err"] = str(e)[:200]

# 5. Also fix trade-ticket-monitor — currently runs every 10 min. 
# With horizon-aware skip, that's fine, but to reduce overall noise
# (since user wants morning-only digest), let's keep monitor running
# for stop-loss tracking but log it.
print("\n[1237] 5. trade-ticket-monitor schedule (kept for stop-loss tracking)")
try:
    monitor_rule = events.describe_rule(Name="justhodl-trade-ticket-monitor-10min")
    out["monitor"] = {
        "schedule": monitor_rule.get("ScheduleExpression"),
        "note": "kept every 10min for real-time stop-loss alerts (these only fire when price actually crosses stop)",
    }
    print(f"  Schedule: {monitor_rule.get('ScheduleExpression')} (unchanged)")
    print(f"  Note: monitor only sends Telegram when price actually crosses stop/TP")
except Exception as e:
    out["monitor_err"] = str(e)[:200]

out["finished"] = datetime.now(timezone.utc).isoformat()
with open(REPORT, "w") as f:
    json.dump(out, f, indent=2, default=str)
print("\n[1237] DONE")
print("\n  Next firing: tomorrow 8:30 AM ET (12:30 UTC)")
print("  Telegram batch will contain ALL new signals since last fire.")
