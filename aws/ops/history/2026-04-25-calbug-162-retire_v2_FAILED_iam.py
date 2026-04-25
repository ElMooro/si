#!/usr/bin/env python3
"""
Step 162 — Re-run legacy retirement, fix ttl reserved-keyword issue.

Step 161 confirmed 4,307 of 4,410 correct=None outcomes are pre-fix
legacy (signal-logger didn\\'t capture baseline_price before commit
2dce7a6 on 2026-04-24 23:25 UTC). The other 103 are also legacy by
proxy — they\\'re from signals logged BEFORE the fix that got scored
AFTER.

The tagging in step 161 failed because 'ttl' is a DynamoDB reserved
keyword. UpdateExpression rejected 'SET ttl = :t'. Need to use
ExpressionAttributeNames to escape it as #ttl.

This step:
  - Tags ALL 4,410 correct=None outcomes (legacy + step-160-induced)
    with is_legacy=true, legacy_reason, and 30-day TTL via #ttl
  - Verifies tagging worked by re-counting

After this:
  - Calibrator can filter legacy=true and compute clean accuracy on
    new outcomes only
  - In ~30 days the legacy records auto-purge from DDB
  - Loop 1 readiness will start showing 🟢 LIVE within 7 days as new
    signals get scored properly
"""
import json
import os
from datetime import datetime, timezone, timedelta
from decimal import Decimal

from ops_report import report
import boto3

REGION = "us-east-1"
ddb = boto3.resource("dynamodb", region_name=REGION)


with report("retire_legacy_v2_ttl_fix") as r:
    r.heading("Re-run legacy retirement with ttl-keyword fix")

    outcomes = ddb.Table("justhodl-outcomes")

    # ─── 1. Re-scan correct=None records ────────────────────────────────
    r.section("1. Re-scan correct=None outcomes")
    null_outcomes = []
    scan_kwargs = {}
    while True:
        resp = outcomes.scan(**scan_kwargs)
        for o in resp.get("Items", []):
            if o.get("correct") is None:
                null_outcomes.append({
                    "outcome_id": o["outcome_id"],
                    "signal_type": o.get("signal_type"),
                    "checked_at": o.get("checked_at"),
                })
        if "LastEvaluatedKey" not in resp:
            break
        scan_kwargs["ExclusiveStartKey"] = resp["LastEvaluatedKey"]
    r.log(f"  Found {len(null_outcomes)} correct=None outcomes")

    # ─── 2. Tag with #ttl reserved-keyword fix ──────────────────────────
    r.section("2. Tag with is_legacy=true, legacy_reason, #ttl=now+30d")
    thirty_days_ts = int((datetime.now(timezone.utc) + timedelta(days=30)).timestamp())
    r.log(f"  Target TTL: {thirty_days_ts} ({datetime.fromtimestamp(thirty_days_ts, timezone.utc).isoformat()})")

    tagged = 0
    failed = 0
    sample_errors = []

    for o in null_outcomes:
        try:
            outcomes.update_item(
                Key={"outcome_id": o["outcome_id"]},
                UpdateExpression="SET is_legacy = :l, legacy_reason = :r, #t = :t",
                ExpressionAttributeNames={"#t": "ttl"},
                ExpressionAttributeValues={
                    ":l": True,
                    ":r": "pre_baseline_fix_2026_04_24",
                    ":t": thirty_days_ts,
                },
            )
            tagged += 1
            if tagged % 1000 == 0:
                r.log(f"    Tagged {tagged}/{len(null_outcomes)}...")
        except Exception as e:
            failed += 1
            if len(sample_errors) < 3:
                sample_errors.append((o["outcome_id"][:30], str(e)[:120]))

    r.log(f"\n  Tagged: {tagged}")
    r.log(f"  Failed: {failed}")
    if sample_errors:
        r.log(f"  Sample errors:")
        for sid, err in sample_errors:
            r.log(f"    {sid}: {err}")

    if tagged == len(null_outcomes):
        r.ok(f"  ✅ All legacy records tagged successfully")
    elif tagged > 0:
        r.warn(f"  ⚠ Partial success — {failed} failed")
    else:
        r.fail(f"  ❌ All updates failed")
        raise SystemExit(1)

    # ─── 3. Verify by re-scanning + counting tagged ─────────────────────
    r.section("3. Verify — count outcomes with is_legacy=true")
    legacy_count = 0
    pristine_count = 0  # correct=None but NOT tagged (something missed)
    new_count = 0       # correct in (True, False) — non-legacy

    scan_kwargs = {}
    while True:
        resp = outcomes.scan(**scan_kwargs)
        for o in resp.get("Items", []):
            correct = o.get("correct")
            if correct is None:
                if o.get("is_legacy") is True:
                    legacy_count += 1
                else:
                    pristine_count += 1
            else:
                new_count += 1
        if "LastEvaluatedKey" not in resp:
            break
        scan_kwargs["ExclusiveStartKey"] = resp["LastEvaluatedKey"]

    r.log(f"  Tagged legacy:           {legacy_count}")
    r.log(f"  Untagged correct=None:   {pristine_count}")
    r.log(f"  Real outcomes (T/F):     {new_count}")

    if pristine_count == 0:
        r.ok(f"  ✅ All correct=None outcomes are now tagged as legacy")

    # ─── 4. Estimate when first new outcome will score ──────────────────
    r.section("4. When will first NEW outcome get scored?")
    r.log(f"  Signal-logger fix:    2026-04-24 23:25 UTC")
    r.log(f"  Earliest day_3 check: 2026-04-27 23:25 UTC (Sun morning)")
    r.log(f"  Earliest day_7 check: 2026-05-01 23:25 UTC (next Friday)")
    r.log(f"  ")
    r.log(f"  outcome-checker schedule: cron(0 8 ? * SUN *) — Sunday 8 UTC")
    r.log(f"  Next Sunday: 2026-04-26 (tomorrow)")
    r.log(f"  → Tomorrow's run will score the FIRST real day_3 outcomes")
    r.log(f"  → 2026-05-03 run will score first day_7 outcomes")
    r.log(f"  → calibrator runs cron(0 9 ? * SUN *) — Sunday 9 UTC")
    r.log(f"  → 2026-05-04 will be FIRST run with meaningful accuracy data")

    r.kv(
        n_null_outcomes=len(null_outcomes),
        tagged=tagged,
        failed=failed,
        legacy_count_after=legacy_count,
        untagged_remaining=pristine_count,
        real_outcomes=new_count,
    )
    r.log("Done")
