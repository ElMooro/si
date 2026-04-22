#!/usr/bin/env python3
"""
Delete Lambda sprawl — 36 functions identified by audit 2026-04-22.

Approved by Khalid on 2026-04-22 after review of
aws/ops/reports/lambda-audit-2026-04-22.md.

For each function in KILL_LIST:
  1. Verify it still has zero invocations in 90 days (safety re-check)
  2. Verify it has no Function URL (safety re-check)
  3. Verify it has no event source mappings (SQS/Kinesis/DynamoDB)
  4. Verify it has no EventBridge rule targets pointing at it
  5. Delete the Lambda
  6. Delete its /aws/lambda/<name> CloudWatch log group

Any safety check that fails → skip that function with a loud warning and
continue with the rest. Idempotent: already-deleted functions are skipped.
"""

import sys
from datetime import datetime, timezone, timedelta

import boto3
from botocore.exceptions import ClientError

REGION = "us-east-1"
LOOKBACK_DAYS = 90

# ─── The 36 kill list from audit 2026-04-22 ─────────────────────────
KILL_LIST = [
    # 0-byte stubs (never implemented)
    "ai-prediction-agent",
    "chatgpt-agent",
    "fed-liquidity-agent",
    "global-liquidity-agent",

    # Old BLS nodejs18.x
    "bls-employment-data-api",
    "bls-function-bls-minimal",

    # Deprecated ECB pipeline (replaced by justhodl-ecb-proxy)
    "ecb",
    "ecb-auto-updater",
    "ecb-data-daily-updater",
    "ecb-data-service",

    # Old NY Fed pipeline
    "nyfed-cmdi-fetcher",
    "nyfed-main-aggregator",

    # One-off legacy APIs
    "crisisapi",
    "ofrapi",
    "polygon-api",
    "financial-dashboard-api-function",
    "getSupabaseConfig",
    "indexToOpenSearch",
    "fed-data-v2",
    "testNewDataSources",

    # Old OpenBB fleet (superseded architecture)
    "enhanced-openbb-handler",
    "openbb-combined-daily-reports",
    "openbb-correlation-analysis",
    "openbb-daily-risk-report",
    "openbb-graphql-handler",
    "openbb-ml-predictions",
    "openbb-system2-proxy",
    "openbb-trading-signals",
    "openbb-vix-alert",
    "unified-openbb-handler",

    # Old data collector + autonomous stub (never productionized)
    "justhodl-data-collector",
    "autonomous-ai-system",

    # Explicit approval (justhodl-prefixed)
    "justhodl-calibrator",           # scheduled learning loop never wired
    "justhodl-daily-report",         # superseded by -v3
    "justhodl-email-reports-v2",     # superseded by morning-intelligence
    "justhodl-liquidity-agent",      # TGA/Fed work, never integrated
]

assert len(KILL_LIST) == 36, f"Kill list has {len(KILL_LIST)} items, expected 36"
assert len(set(KILL_LIST)) == 36, "Kill list has duplicates"

lam  = boto3.client("lambda", region_name=REGION)
cw   = boto3.client("cloudwatch", region_name=REGION)
ev   = boto3.client("events", region_name=REGION)
logs = boto3.client("logs", region_name=REGION)


def log(msg):
    print(f"[{datetime.now(timezone.utc).strftime('%H:%M:%S')}] {msg}", flush=True)


def function_exists(name: str) -> bool:
    try:
        lam.get_function(FunctionName=name)
        return True
    except lam.exceptions.ResourceNotFoundException:
        return False


def invocations_90d(name: str) -> int:
    end = datetime.now(timezone.utc)
    start = end - timedelta(days=LOOKBACK_DAYS)
    resp = cw.get_metric_statistics(
        Namespace="AWS/Lambda",
        MetricName="Invocations",
        Dimensions=[{"Name": "FunctionName", "Value": name}],
        StartTime=start,
        EndTime=end,
        Period=86400,
        Statistics=["Sum"],
    )
    return int(sum(dp["Sum"] for dp in resp.get("Datapoints", [])))


def has_function_url(name: str) -> bool:
    try:
        lam.get_function_url_config(FunctionName=name)
        return True
    except lam.exceptions.ResourceNotFoundException:
        return False


def event_source_mappings(name: str):
    resp = lam.list_event_source_mappings(FunctionName=name)
    return resp.get("EventSourceMappings", [])


def eb_rule_names_for(name: str):
    fn = lam.get_function(FunctionName=name)
    arn = fn["Configuration"]["FunctionArn"]
    try:
        return ev.list_rule_names_by_target(TargetArn=arn).get("RuleNames", [])
    except ClientError:
        return []


def delete_log_group(name: str) -> str:
    grp = f"/aws/lambda/{name}"
    try:
        logs.delete_log_group(logGroupName=grp)
        return "deleted"
    except logs.exceptions.ResourceNotFoundException:
        return "not found (skip)"
    except ClientError as e:
        return f"error: {e.response['Error']['Code']}"


def process(name: str) -> str:
    """Returns: 'deleted', 'already-gone', or 'skipped: <reason>'."""
    if not function_exists(name):
        return "already-gone"

    # Safety re-check — these should all pass based on the audit, but be paranoid
    invs = invocations_90d(name)
    if invs > 0:
        return f"skipped: has {invs} invocations in last 90d"

    if has_function_url(name):
        return "skipped: has Function URL (unexpected)"

    mappings = event_source_mappings(name)
    if mappings:
        return f"skipped: has {len(mappings)} event source mapping(s)"

    rules = eb_rule_names_for(name)
    if rules:
        return f"skipped: targeted by EB rule(s): {rules}"

    # All checks passed — delete
    lam.delete_function(FunctionName=name)
    log_status = delete_log_group(name)
    return f"deleted (log group: {log_status})"


def main():
    log(f"=== Delete Lambda Sprawl — {len(KILL_LIST)} targets ===")

    results = {"deleted": [], "already-gone": [], "skipped": []}

    for i, name in enumerate(KILL_LIST, 1):
        log(f"[{i}/{len(KILL_LIST)}] {name}")
        try:
            outcome = process(name)
        except ClientError as e:
            outcome = f"error: {e.response['Error']['Code']} — {e.response['Error']['Message']}"
        log(f"    → {outcome}")

        if outcome.startswith("deleted"):
            results["deleted"].append((name, outcome))
        elif outcome == "already-gone":
            results["already-gone"].append(name)
        else:
            results["skipped"].append((name, outcome))

    log("")
    log("══════════════════ SUMMARY ══════════════════")
    log(f"  Deleted:      {len(results['deleted'])}")
    log(f"  Already gone: {len(results['already-gone'])}")
    log(f"  Skipped:      {len(results['skipped'])}")
    log("═════════════════════════════════════════════")

    if results["skipped"]:
        log("")
        log("Skipped (did not delete):")
        for name, reason in results["skipped"]:
            log(f"  - {name}: {reason}")

    if results["already-gone"]:
        log("")
        log(f"Already deleted previously: {', '.join(results['already-gone'])}")

    # Fail the workflow run only if something errored
    errored = [s for _, s in results["skipped"] if s.startswith("error:")]
    if errored:
        sys.exit(f"{len(errored)} function(s) errored during deletion — see log above")


if __name__ == "__main__":
    main()
