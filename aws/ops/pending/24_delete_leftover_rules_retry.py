#!/usr/bin/env python3
"""
Retry the 3 EB rule deletions. AmazonEventBridgeFullAccess was
attached in the previous script but IAM hadn't propagated by
the time retry fired within the same workflow session.

Running in a fresh workflow (this commit) should pick up the new
permissions cleanly. If propagation STILL isn't done after ~2 min
since the attach, the script sleeps + retries.
"""

import time
from datetime import datetime, timezone

from ops_report import report
import boto3
from botocore.exceptions import ClientError

REGION = "us-east-1"
ev = boto3.client("events", region_name=REGION)


def try_delete(r, rule_name: str) -> bool:
    try:
        targets = ev.list_targets_by_rule(Rule=rule_name).get("Targets", [])
        if targets:
            ev.remove_targets(Rule=rule_name, Ids=[t["Id"] for t in targets])
            r.log(f"    removed {len(targets)} target(s)")
    except ClientError as e:
        r.warn(f"    remove_targets: {e}")
        return False

    try:
        ev.delete_rule(Name=rule_name)
        r.ok(f"    Rule deleted")
        return True
    except ev.exceptions.ResourceNotFoundException:
        r.warn(f"    Rule already gone")
        return True
    except ClientError as e:
        r.fail(f"    delete_rule: {e}")
        return False


with report("delete_leftover_rules_retry") as r:
    r.heading("Retry EB rule deletions (IAM should be propagated now)")

    rules = [
        "lambda-warmer-system3",
        "lambda-warmer-system3-frequent",
        "DailyEmailReportsV2_8AMET",
    ]

    # Optionally sleep up to 2x (60s each) if the first attempt still fails
    for attempt in range(3):
        r.section(f"Attempt {attempt + 1}")
        all_done = True
        for rule_name in rules:
            # Skip if already gone
            try:
                ev.describe_rule(Name=rule_name)
            except ev.exceptions.ResourceNotFoundException:
                r.log(f"  {rule_name}: already deleted")
                r.kv(rule=rule_name, status="already-gone", attempt=attempt + 1)
                continue
            except ClientError as e:
                r.warn(f"  describe_rule({rule_name}): {e}")
                continue

            r.log(f"  {rule_name}:")
            ok = try_delete(r, rule_name)
            r.kv(rule=rule_name, status="deleted" if ok else "failed", attempt=attempt + 1)
            if not ok:
                all_done = False

        if all_done:
            r.ok("All rules handled successfully")
            break
        elif attempt < 2:
            r.warn(f"Some rules still blocked. Sleeping 45s then retrying…")
            time.sleep(45)
        else:
            r.warn("Still blocked after 3 attempts. Propagation may be unusually slow or policy mis-attached.")

    # Final verification
    r.section("Final verification")
    for rule_name in rules + ["DailyEmailReportsV2"]:
        try:
            ev.describe_rule(Name=rule_name)
            marker = "✓ present" if rule_name == "DailyEmailReportsV2" else "✗ STILL EXISTS"
            r.log(f"  {rule_name}: {marker}")
        except ev.exceptions.ResourceNotFoundException:
            marker = "✓ gone" if rule_name != "DailyEmailReportsV2" else "✗ ACCIDENTALLY DELETED"
            r.log(f"  {rule_name}: {marker}")

    r.log("Done")
