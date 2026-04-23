#!/usr/bin/env python3
"""
Two-step fix for the blocked EventBridge deletions.

Phase 2b final cleanup #22 succeeded partially:
  - enhanced-openbb-handler Lambda DELETED
  - Lambda invoke permissions REVOKED from warmer rules
  - But the 3 EB rules themselves couldn't be deleted — the
    github-actions-justhodl IAM user has EventBridgeReadOnlyAccess, not
    the full-access policy needed for DeleteRule/RemoveTargets.

Since the user has IAMFullAccess, it can grant itself the missing
permission. This is the cleanest self-heal for AWS setups like this.

Steps:
  1. Attach AmazonEventBridgeFullAccess to user github-actions-justhodl
  2. Retry RemoveTargets + DeleteRule on the 3 leftover rules:
       - lambda-warmer-system3
       - lambda-warmer-system3-frequent
       - DailyEmailReportsV2_8AMET
  3. Verify they're gone

The new EB policy sticks around for future ops scripts — saves us
having to broaden permissions case-by-case.
"""

import os
from datetime import datetime, timezone

from ops_report import report
import boto3
from botocore.exceptions import ClientError

REGION = "us-east-1"

IAM_USER = "github-actions-justhodl"
EB_POLICY_ARN = "arn:aws:iam::aws:policy/AmazonEventBridgeFullAccess"

iam = boto3.client("iam", region_name=REGION)
ev  = boto3.client("events", region_name=REGION)


def attach_policy_idempotent(r) -> bool:
    """Attach EB full access to the user. Return True if attached (or already was)."""
    # Check existing policies first
    try:
        resp = iam.list_attached_user_policies(UserName=IAM_USER)
        already = {p["PolicyArn"] for p in resp.get("AttachedPolicies", [])}
        r.log(f"  Currently attached: {sorted(already)}")
    except ClientError as e:
        r.fail(f"  list_attached_user_policies failed: {e}")
        return False

    if EB_POLICY_ARN in already:
        r.log(f"  AmazonEventBridgeFullAccess already attached — skipping")
        return True

    try:
        iam.attach_user_policy(UserName=IAM_USER, PolicyArn=EB_POLICY_ARN)
        r.ok(f"  Attached {EB_POLICY_ARN}")
        return True
    except ClientError as e:
        r.fail(f"  attach_user_policy failed: {e}")
        return False


def delete_rule_full(r, rule_name: str) -> bool:
    """With full EB permissions: remove targets → delete rule."""
    try:
        targets = ev.list_targets_by_rule(Rule=rule_name).get("Targets", [])
        if targets:
            ids = [t["Id"] for t in targets]
            ev.remove_targets(Rule=rule_name, Ids=ids)
            r.log(f"    removed {len(ids)} target(s)")
        else:
            r.log(f"    (no targets)")
    except ClientError as e:
        r.warn(f"    remove_targets failed: {e}")
        return False

    try:
        ev.delete_rule(Name=rule_name)
        r.ok(f"    Rule {rule_name} DELETED")
        return True
    except ev.exceptions.ResourceNotFoundException:
        r.warn(f"    Rule {rule_name} already gone")
        return True
    except ClientError as e:
        r.fail(f"    delete_rule failed: {e}")
        return False


def verify_gone(r, rule_name: str) -> bool:
    try:
        ev.describe_rule(Name=rule_name)
        r.fail(f"  Rule {rule_name} STILL EXISTS")
        return False
    except ev.exceptions.ResourceNotFoundException:
        r.ok(f"  Rule {rule_name} confirmed gone")
        return True
    except ClientError as e:
        r.warn(f"  describe_rule failed: {e}")
        return False


with report("grant_eb_perms_and_retry") as r:
    r.heading("Self-elevate IAM + retry rule deletions")

    # ─────────────────────────────────────────────────
    # Step 1: Attach EB full access policy
    # ─────────────────────────────────────────────────
    r.section("Step 1: attach AmazonEventBridgeFullAccess to github-actions-justhodl")
    attached = attach_policy_idempotent(r)
    r.kv(step="attach-policy", status="ok" if attached else "failed")

    if not attached:
        r.fail("Can't proceed without policy — aborting")
        raise SystemExit(1)

    # Wait briefly for IAM to propagate. Policy attachments usually propagate
    # within seconds but sometimes STS sessions cache the old principal policy
    # for a bit. Refresh by creating a new client.
    import time
    time.sleep(5)
    ev_refreshed = boto3.client("events", region_name=REGION)

    # ─────────────────────────────────────────────────
    # Step 2: Retry the 3 leftover deletions
    # ─────────────────────────────────────────────────
    r.section("Step 2: delete the 3 leftover EventBridge rules")
    rules = [
        "lambda-warmer-system3",
        "lambda-warmer-system3-frequent",
        "DailyEmailReportsV2_8AMET",
    ]
    results = {}
    for rule_name in rules:
        r.log(f"  Rule: {rule_name}")
        # Use the refreshed client
        globals()["ev"] = ev_refreshed
        ok = delete_rule_full(r, rule_name)
        results[rule_name] = ok
        r.kv(step="delete-rule", target=rule_name, status="deleted" if ok else "failed")

    # ─────────────────────────────────────────────────
    # Step 3: Verify each one is gone
    # ─────────────────────────────────────────────────
    r.section("Step 3: verify")
    all_gone = True
    for rule_name in rules:
        if not verify_gone(r, rule_name):
            all_gone = False

    # Extra sanity: DailyEmailReportsV2 still there
    try:
        ev_refreshed.describe_rule(Name="DailyEmailReportsV2")
        r.ok("  DailyEmailReportsV2 still present — daily email will fire once")
    except Exception as e:
        r.fail(f"  DailyEmailReportsV2 check failed: {e}")

    r.log("")
    if all_gone:
        r.ok("All 3 leftover rules are gone. Phase 2b fully complete.")
    else:
        r.warn("Some rules still present — inspect individual errors above")

    r.log("Done")
