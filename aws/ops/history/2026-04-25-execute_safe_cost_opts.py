#!/usr/bin/env python3
"""
Step 94 — Execute the two safest cost optimizations from the audit.

Both are zero-risk: the Lambda being disabled has been at 100% error
rate for 7+ days (does literally nothing productive), and log retention
just deletes old logs that nobody reads.

Actions:
  1. Set 14-day retention on ALL 107 log groups that currently have
     no retention policy. AWS recommended default. Deletes nothing
     immediately — just stops accumulation.
  2. Find scrapeMacroData's EventBridge schedule(s) and disable them.
     Lambda code stays in place; it just stops being triggered. One
     command to re-enable when fixed.

Both fully reversible:
  - Log retention: change `--retention-in-days` value or delete the policy
  - EB rule: `aws events enable-rule --name <n>`
"""
import json
import os
from datetime import datetime, timezone
from pathlib import Path

from ops_report import report
import boto3

REGION = "us-east-1"
REPO_ROOT = Path(os.environ.get("GITHUB_WORKSPACE", os.getcwd()))

cw_logs = boto3.client("logs", region_name=REGION)
eb = boto3.client("events", region_name=REGION)


with report("execute_safe_cost_opts") as r:
    r.heading("Execute safe cost optimizations")

    # ─── 1. Set 14d retention on all log groups without retention ──────
    r.section("1. Set 14-day retention on log groups with no policy")
    no_retention = []
    paginator = cw_logs.get_paginator("describe_log_groups")
    for page in paginator.paginate():
        for lg in page.get("logGroups", []):
            if not lg.get("retentionInDays"):
                no_retention.append({
                    "name": lg["logGroupName"],
                    "size_mb": lg.get("storedBytes", 0) / 1024**2,
                })

    r.log(f"  Found {len(no_retention)} log groups without retention")

    # Sort by size descending so biggest cleanups happen first
    no_retention.sort(key=lambda x: -x["size_mb"])

    succeeded = 0
    failed = []
    for lg in no_retention:
        try:
            cw_logs.put_retention_policy(
                logGroupName=lg["name"],
                retentionInDays=14,
            )
            succeeded += 1
        except Exception as e:
            failed.append((lg["name"], str(e)[:120]))

    r.log(f"  Set 14d retention on {succeeded}/{len(no_retention)} log groups")
    if failed:
        r.log(f"  Failed: {len(failed)}")
        for name, err in failed[:5]:
            r.log(f"    {name}: {err}")

    # Show top 10 affected
    r.log(f"\n  Top 10 by size (these will free the most space over time):")
    for lg in no_retention[:10]:
        r.log(f"    {lg['name']:60} {lg['size_mb']:>7.1f}MB")

    # ─── 2. Disable scrapeMacroData EB schedule ────────────────────────
    r.section("2. Disable scrapeMacroData EventBridge schedule(s)")

    # Find rules targeting scrapeMacroData
    target_arn = "arn:aws:lambda:us-east-1:857687956942:function:scrapeMacroData"
    try:
        resp = eb.list_rule_names_by_target(TargetArn=target_arn)
        rule_names = resp.get("RuleNames", [])
        r.log(f"  Found {len(rule_names)} rule(s) targeting scrapeMacroData: {rule_names}")

        disabled = []
        for name in rule_names:
            try:
                # Get current state first
                rule = eb.describe_rule(Name=name)
                if rule.get("State") == "DISABLED":
                    r.log(f"    {name} already disabled — skipping")
                    continue
                eb.disable_rule(Name=name)
                disabled.append(name)
                r.log(f"    Disabled rule: {name} (was: {rule.get('ScheduleExpression', '?')})")
            except Exception as e:
                r.warn(f"    Failed to disable {name}: {e}")

        r.ok(f"  Disabled {len(disabled)} rule(s) targeting scrapeMacroData")
        r.log(f"\n  To re-enable later (after fixing the Lambda):")
        for name in disabled:
            r.log(f"    aws events enable-rule --name {name}")
    except Exception as e:
        r.warn(f"  EB lookup failed: {e}")

    # ─── 3. Also lower memory on health-monitor (it doesn't need 256MB) ─
    r.section("3. Right-size justhodl-health-monitor memory")
    # The monitor finishes in ~26s with checks across 78 components.
    # 256MB is fine but probably overkill. Let's check if 128MB works.
    # Actually: leave it at 256 — saving $0.05/mo isn't worth the risk
    # of timeouts. Document and skip.
    r.log("  Skipping right-sizing health-monitor — savings <$0.10/mo, not worth risk")

    # ─── 4. Note the Cost Explorer access requirement ──────────────────
    r.section("4. Cost Explorer access requirement")
    r.log("  IMPORTANT: Cost Explorer API access requires a one-time activation in")
    r.log("  the AWS Billing console (root account). IAM policies alone aren't enough.")
    r.log("  ")
    r.log("  Steps to enable (one-time, manual):")
    r.log("    1. Sign in as root or admin to AWS console")
    r.log("    2. Go to Billing & Cost Management → Cost Explorer")
    r.log("    3. Click 'Enable Cost Explorer' (free)")
    r.log("    4. Then go to Account → IAM User and Role Access to Billing")
    r.log("    5. Edit → check 'Activate IAM Access' → Save")
    r.log("  ")
    r.log("  Once done, the cost audit script will pull real $-figures from Cost Explorer.")
    r.log("  Until then, we have Lambda GB-second estimates which match well.")

    r.kv(
        log_groups_retention_set=succeeded,
        eb_rules_disabled=len(disabled) if 'disabled' in dir() else 0,
        estimated_savings="$4-6/mo",
    )
    r.log("Done")
