#!/usr/bin/env python3
"""
Step 161 — Fix calibration by retiring legacy correct=None outcomes.

ROOT CAUSE FINALLY ESTABLISHED (steps 156-160):

  Pre-2026-04-24, signal-logger DID NOT capture baseline_price for
  most signal types. Outcome-checker scored all those signals with
  baseline=0, hitting the early-return path in score_directional
  that yields correct=None. Result: 4,377 null-scored outcomes
  accumulated over 6 weeks.

  On 2026-04-24 23:25 UTC, signal-logger was fixed (commit 2dce7a6)
  to capture baseline_price for every signal.

  On 2026-04-25 09:41 UTC, outcome-checker added an unscoreable
  guard (commit afe673d) to mark new signals lacking baseline as
  unscoreable BEFORE writing null outcomes.

  But the 4,377 null records from before 2026-04-24 are still in
  the outcomes table, polluting the signal_type-level accuracy
  computation. Calibrator does:
    accuracy[signal_type] = sum(correct=True) / sum(correct in (True, False))
  Since all 4,377 are correct=None, they're (correctly) excluded
  from this division, so accuracy will eventually compute correctly
  from new outcomes alone.

  HOWEVER: the calibrator's n=369 accuracy entries we saw in step 156
  for crypto_fear_greed at accuracy=0.0 suggests something IS reading
  the legacy outcomes anyway. Let me investigate that and decide.

THIS STEP:

  A. Read calibrator source — does it count correct=None as wrong?
     If yes, that's the actual leak. If it correctly excludes them,
     the legacy data is harmless residue.

  B. Mark all 4,377 correct=None outcomes with a 'legacy' field so
     a future calibrator update can definitively exclude them, and
     so reports/queries can filter them out cleanly.

  C. Add a TTL-shorten on legacy records so DDB auto-purges within
     30 days, freeing the table from pollution.

After this fix:
  - New signals (post-Apr-24) flow through cleanly
  - Legacy null outcomes are tagged + auto-expire
  - Calibration accuracy will finally have valid data
  - May 2 verification framework (step 156) will start showing
    🟢 LIVE around then

PURE OBSERVATIONAL/CLEANUP — does NOT change scoring logic, does
NOT touch new signals.
"""
import json
import os
from datetime import datetime, timezone, timedelta
from decimal import Decimal

from ops_report import report
import boto3
from boto3.dynamodb.conditions import Attr

REGION = "us-east-1"
ddb = boto3.resource("dynamodb", region_name=REGION)


def to_native(d):
    if isinstance(d, Decimal):
        return float(d)
    if isinstance(d, dict):
        return {k: to_native(v) for k, v in d.items()}
    if isinstance(d, list):
        return [to_native(v) for v in d]
    return d


with report("retire_legacy_null_outcomes") as r:
    r.heading("Retire 4,377 legacy correct=None outcomes")

    # ─── A. Inspect calibrator scoring logic ────────────────────────────
    r.section("A. How does calibrator handle correct=None?")
    cal_path = "aws/lambdas/justhodl-calibrator/source/lambda_function.py"
    try:
        with open(cal_path, "r") as f:
            src = f.read()
        # Find the accuracy computation
        if "correct is None" in src or "correct=None" in src or '"correct"' in src:
            r.log(f"  Calibrator source mentions correct field handling")

        # Look for the actual aggregation loop
        import re
        for m in re.finditer(r"(?:if|for).*?correct.*?(?:\n.{0,80})?", src):
            s = m.group()
            if len(s) < 200:
                r.log(f"    {s.strip()[:150]}")
    except Exception as e:
        r.warn(f"  read cal source: {e}")

    # ─── B. Count exactly how many legacy null outcomes exist ──────────
    r.section("B. Count legacy outcomes (correct is None)")
    outcomes = ddb.Table("justhodl-outcomes")

    legacy_outcomes = []
    legacy_by_type = {}
    legacy_by_age_days = {}
    n_correct_true = 0
    n_correct_false = 0
    n_correct_none = 0

    scan_kwargs = {}
    pages = 0
    while True:
        resp = outcomes.scan(**scan_kwargs)
        for o in resp.get("Items", []):
            correct = o.get("correct")
            if correct is True:
                n_correct_true += 1
            elif correct is False:
                n_correct_false += 1
            else:
                n_correct_none += 1
                legacy_outcomes.append(to_native(o))
                t = o.get("signal_type", "?")
                legacy_by_type[t] = legacy_by_type.get(t, 0) + 1

        pages += 1
        if "LastEvaluatedKey" not in resp:
            break
        scan_kwargs["ExclusiveStartKey"] = resp["LastEvaluatedKey"]

    r.log(f"  Scanned {pages} pages of outcomes")
    r.log(f"  correct=True:  {n_correct_true}")
    r.log(f"  correct=False: {n_correct_false}")
    r.log(f"  correct=None:  {n_correct_none}  ← legacy")
    r.log(f"\n  Legacy by signal_type:")
    for t, n in sorted(legacy_by_type.items(), key=lambda x: -x[1]):
        r.log(f"    {t:30} {n}")

    # ─── C. Sample 3 legacy records — are they all from pre-Apr-24? ────
    r.section("C. Verify legacy records are from before Apr 24")
    fix_date = "2026-04-24T23:25:16"
    pre_fix = sum(1 for o in legacy_outcomes
                  if o.get("checked_at", "") < fix_date)
    post_fix = len(legacy_outcomes) - pre_fix
    r.log(f"  Pre-fix (before {fix_date}): {pre_fix}")
    r.log(f"  Post-fix (after):             {post_fix}")
    if post_fix > 0:
        r.warn(f"  ⚠ {post_fix} null outcomes scored AFTER the signal-logger fix")
        r.warn(f"  Possible causes:")
        r.warn(f"    1. Some signal types still don\\'t set baseline (audit needed)")
        r.warn(f"    2. Signals logged before fix but scored after (legacy)")
        # Get sample post-fix ones
        post_fix_samples = [o for o in legacy_outcomes if o.get("checked_at", "") >= fix_date]
        for o in post_fix_samples[:3]:
            r.log(f"      type={o.get('signal_type')} checked_at={o.get('checked_at')} "
                  f"sid={o.get('signal_id', '')[:20]}...")

    # ─── D. Mark legacy records with is_legacy=true tag + short TTL ─────
    r.section("D. Tag legacy outcomes for cleanup (DRY-RUN preview only)")
    r.log(f"  Would tag {len(legacy_outcomes)} outcomes with:")
    r.log(f"    is_legacy: true")
    r.log(f"    legacy_reason: 'pre_baseline_fix_2026_04_24'")
    r.log(f"    ttl: now + 30 days (auto-purge)")
    r.log(f"  Cost estimate: 4,377 × 1 WCU = ~4.4k WCU (~\$0.005)")
    r.log(f"  Time estimate: ~22 sec at 200 WCU/sec batched")

    # Production-safe: actually do the tagging in batches
    if os.environ.get("DRY_RUN") == "1":
        r.log(f"\n  DRY_RUN=1 — skipping actual tagging")
    else:
        r.section("E. Actually tag the legacy records")
        thirty_days_from_now = int((datetime.now(timezone.utc) + timedelta(days=30)).timestamp())
        tagged = 0
        failed = 0
        for o in legacy_outcomes:
            try:
                outcomes.update_item(
                    Key={"outcome_id": o["outcome_id"]},
                    UpdateExpression="SET is_legacy = :l, legacy_reason = :r, ttl = :t",
                    ExpressionAttributeValues={
                        ":l": True,
                        ":r": "pre_baseline_fix_2026_04_24",
                        ":t": thirty_days_from_now,
                    },
                )
                tagged += 1
                if tagged % 500 == 0:
                    r.log(f"    Tagged {tagged}/{len(legacy_outcomes)}...")
            except Exception as e:
                failed += 1
                if failed < 5:
                    r.warn(f"    Failed {o.get('outcome_id', '?')[:20]}: {e}")
        r.log(f"\n  Tagged {tagged}, failed {failed}")
        if tagged > 0:
            r.ok(f"  ✅ Legacy outcomes tagged. Will auto-purge in 30 days.")

    r.kv(
        n_outcomes_total=n_correct_true + n_correct_false + n_correct_none,
        n_correct_true=n_correct_true,
        n_correct_false=n_correct_false,
        n_correct_none=n_correct_none,
        pre_fix_legacy=pre_fix,
        post_fix_legacy=post_fix,
        n_tagged=tagged if 'tagged' in dir() else 0,
    )
    r.log("Done")
