"""ops/811 - decommission justhodl-catch-up-radar (consolidate, never build twice).

The parallel build pipeline shipped justhodl-beta-laggard - a dedicated
beta-laggard engine built on real FMP return data, which is strictly better
than catch-up-radar's beta-laggard section (it solves the noisy scraped-return
problem that inflated catch-up-radar's v1 numbers). Per ops/810 the parallel
pipeline is also building an ETF/CEF catch-up engine next. Both halves of
catch-up-radar are therefore redundant.

Per the never-build-twice doctrine we consolidate rather than ship a duplicate:
delete the catch-up-radar Lambda, its daily EventBridge rule, and its S3 output.
The HTML page, nav wiring and aws/lambdas/justhodl-catch-up-radar/ source are
already removed from the repo in the accompanying commit.
"""
import json
from datetime import datetime, timezone

import boto3
from botocore.config import Config

cfg = Config(read_timeout=120, connect_timeout=20, retries={"max_attempts": 3})
lam = boto3.client("lambda", region_name="us-east-1", config=cfg)
events = boto3.client("events", region_name="us-east-1", config=cfg)
s3 = boto3.client("s3", region_name="us-east-1", config=cfg)

FN = "justhodl-catch-up-radar"
RULE = "catch-up-radar-daily"
BUCKET = "justhodl-dashboard-live"
OUT_KEY = "screener/catch-up-radar.json"

report = {
    "ops": 811,
    "ts": datetime.now(timezone.utc).isoformat(),
    "subject": "Decommission justhodl-catch-up-radar (consolidate on beta-laggard)",
    "steps": {},
}

# 1. EventBridge rule: remove targets, then delete the rule.
try:
    tgts = events.list_targets_by_rule(Rule=RULE).get("Targets", [])
    if tgts:
        events.remove_targets(Rule=RULE, Ids=[t["Id"] for t in tgts])
    events.delete_rule(Name=RULE)
    report["steps"]["eventbridge_rule"] = f"deleted ({len(tgts)} target(s) removed)"
except events.exceptions.ResourceNotFoundException:
    report["steps"]["eventbridge_rule"] = "already absent"
except Exception as e:  # noqa: BLE001
    report["steps"]["eventbridge_rule"] = f"error: {e}"

# 2. Lambda permission granted to EventBridge (best-effort).
try:
    lam.remove_permission(FunctionName=FN, StatementId=f"{RULE}-invoke")
    report["steps"]["lambda_permission"] = "removed"
except Exception as e:  # noqa: BLE001
    report["steps"]["lambda_permission"] = f"skipped: {e}"

# 3. Delete the Lambda function itself.
try:
    lam.delete_function(FunctionName=FN)
    report["steps"]["lambda"] = "deleted"
except lam.exceptions.ResourceNotFoundException:
    report["steps"]["lambda"] = "already absent"
except Exception as e:  # noqa: BLE001
    report["steps"]["lambda"] = f"error: {e}"

# 4. Delete the now-orphaned S3 output so no page can fetch stale data.
try:
    s3.delete_object(Bucket=BUCKET, Key=OUT_KEY)
    report["steps"]["s3_output"] = f"deleted s3://{BUCKET}/{OUT_KEY}"
except Exception as e:  # noqa: BLE001
    report["steps"]["s3_output"] = f"skipped: {e}"

# 5. Verify the Lambda is gone.
try:
    lam.get_function(FunctionName=FN)
    report["verify_lambda_gone"] = False
except lam.exceptions.ResourceNotFoundException:
    report["verify_lambda_gone"] = True
except Exception as e:  # noqa: BLE001
    report["verify_lambda_gone"] = f"unknown: {e}"

report["ok"] = report["verify_lambda_gone"] is True

print(json.dumps(report, indent=2))
with open("aws/ops/reports/811_catch_up_radar_decommission.json", "w") as f:
    json.dump(report, f, indent=2)
