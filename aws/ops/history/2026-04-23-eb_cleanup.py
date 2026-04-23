#!/usr/bin/env python3
"""
EventBridge rule cleanup — delete duplicates and retired rules.

Principle: be conservative. For each rule we intend to delete:
  1. Verify its current target list matches what we expect
  2. If it has unexpected targets, SKIP and warn
  3. Remove targets first (required before delete_rule)
  4. Delete the rule
  5. Log the full action for rollback reference

Rules to delete (11 total):

Duplicates firing the same Lambda:
  justhodl-daily-8am         → justhodl-daily-report-v3  (redundant: v9-morning covers this)
  justhodl-daily-v3          → justhodl-daily-report-v3  (redundant: v9-auto-refresh every 5min)
  justhodl-crypto-15min      → justhodl-crypto-intel      (duplicate of crypto-intel-schedule)
  justhodl-ml-schedule       → justhodl-ml-predictions    (duplicate of ml-predictions-schedule)

DISABLED rules on global-liquidity-agent-v2 (clearly retired):
  liquidity-critical-monitor
  liquidity-daily-8am
  liquidity-daily-report
  liquidity-daily-report-v2
  liquidity-hourly-v2
  liquidity-news-v2

Note: fmp-stock-picks-daily showed twice in the listing but that's
likely a listing artifact (list_rules returns max ~100 items and some
get listed twice if there's pagination overlap). Skipping this one
and verifying separately — if it actually exists twice, we'll clean
it up in a follow-up after checking.

Read-only preview mode: set DRY_RUN=True below to see what would
happen without mutating anything.
"""
import os
from ops_report import report
import boto3

REGION = "us-east-1"
DRY_RUN = os.environ.get("DRY_RUN", "false").lower() == "true"

eb = boto3.client("events", region_name=REGION)
lam = boto3.client("lambda", region_name=REGION)

# ──────────────────────────────────────────────
# Rules to delete + which Lambda we expect each to target
# ──────────────────────────────────────────────
RULES_TO_DELETE = [
    # (rule_name, expected_lambda_name_fragment, reason)
    ("justhodl-daily-8am",         "justhodl-daily-report-v3", "duplicate of v9-morning"),
    ("justhodl-daily-v3",          "justhodl-daily-report-v3", "duplicate; v9-auto-refresh covers"),
    ("justhodl-crypto-15min",      "justhodl-crypto-intel",    "duplicate of crypto-intel-schedule"),
    ("justhodl-ml-schedule",       "justhodl-ml-predictions",  "duplicate of ml-predictions-schedule"),
    ("liquidity-critical-monitor", "global-liquidity-agent-v2","DISABLED; retired"),
    ("liquidity-daily-8am",        "global-liquidity-agent-v2","DISABLED; retired"),
    ("liquidity-daily-report",     "global-liquidity-agent-v2","DISABLED; retired"),
    ("liquidity-daily-report-v2",  "global-liquidity-agent-v2","DISABLED; retired"),
    ("liquidity-hourly-v2",        "global-liquidity-agent-v2","DISABLED; retired"),
    ("liquidity-news-v2",          "global-liquidity-agent-v2","DISABLED; retired"),
]


def verify_and_describe_rule(rule_name, expected_lambda_fragment):
    """Return (ok, rule_state, targets_list, reason_if_skipped)."""
    try:
        rule = eb.describe_rule(Name=rule_name)
    except eb.exceptions.ResourceNotFoundException:
        return False, None, None, "rule not found (already deleted?)"
    except Exception as e:
        return False, None, None, f"describe error: {e}"

    state = rule.get("State", "?")
    sched = rule.get("ScheduleExpression", "(none)")

    try:
        targets = eb.list_targets_by_rule(Rule=rule_name).get("Targets", [])
    except Exception as e:
        return False, state, None, f"list_targets error: {e}"

    target_arns = [t.get("Arn", "") for t in targets]

    # Every target must reference a Lambda; the Lambda name must
    # contain our expected fragment. Any surprise target = skip.
    for arn in target_arns:
        if ":lambda:" not in arn:
            return False, state, targets, f"unexpected non-Lambda target: {arn}"
        lambda_name = arn.split(":")[-1]
        if expected_lambda_fragment not in lambda_name:
            return False, state, targets, f"target Lambda {lambda_name} does not match expected {expected_lambda_fragment}"

    return True, state, targets, None


def safe_delete(rule_name, targets):
    """Remove targets then delete rule. Returns (ok, err_msg)."""
    # Remove targets first — EB requires this
    target_ids = [t.get("Id") for t in targets if t.get("Id")]
    if target_ids:
        try:
            eb.remove_targets(Rule=rule_name, Ids=target_ids)
        except Exception as e:
            return False, f"remove_targets: {e}"
    # Delete rule
    try:
        eb.delete_rule(Name=rule_name)
    except Exception as e:
        return False, f"delete_rule: {e}"
    return True, None


with report("eb_cleanup") as r:
    r.heading(f"EventBridge rule cleanup — {'DRY RUN' if DRY_RUN else 'LIVE'}")
    if DRY_RUN:
        r.log("  DRY_RUN=true — no mutations will be made, just previewing")

    r.section("Plan")
    r.log(f"  {len(RULES_TO_DELETE)} rules queued for deletion")
    r.log("")

    results = {
        "deleted": [],
        "skipped": [],
        "errors": [],
    }

    for rule_name, expected_lambda, reason in RULES_TO_DELETE:
        r.log(f"\n  → {rule_name}  (expect → {expected_lambda})  [{reason}]")
        ok, state, targets, skip_reason = verify_and_describe_rule(rule_name, expected_lambda)

        if not ok:
            r.warn(f"    SKIP: {skip_reason}")
            results["skipped"].append({"rule": rule_name, "reason": skip_reason})
            continue

        target_descr = ", ".join(t["Arn"].split(":")[-1] for t in targets)
        r.log(f"    State: {state}, Targets: [{target_descr}]")

        if DRY_RUN:
            r.log(f"    [DRY RUN] Would delete")
            results["skipped"].append({"rule": rule_name, "reason": "dry-run"})
            continue

        delete_ok, err = safe_delete(rule_name, targets)
        if delete_ok:
            r.ok(f"    ✓ Deleted")
            results["deleted"].append({
                "rule": rule_name,
                "state_was": state,
                "targets_were": [t["Arn"] for t in targets],
                "reason": reason,
            })
        else:
            r.fail(f"    ✗ Delete failed: {err}")
            results["errors"].append({"rule": rule_name, "error": err})

    r.section("Summary")
    r.log(f"  Deleted: {len(results['deleted'])}")
    r.log(f"  Skipped: {len(results['skipped'])}")
    r.log(f"  Errors:  {len(results['errors'])}")

    r.kv(deleted=len(results["deleted"]),
         skipped=len(results["skipped"]),
         errors=len(results["errors"]))

    # For rollback reference
    if results["deleted"]:
        r.log("\n  Rollback reference (rules deleted this run):")
        for d in results["deleted"]:
            r.log(f"    {d['rule']:40} state_was={d['state_was']}  targets={d['targets_were']}")

    r.log("Done")
