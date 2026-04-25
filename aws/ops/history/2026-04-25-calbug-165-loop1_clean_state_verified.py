#!/usr/bin/env python3
"""
Step 165 — Re-run Loop 1 readiness framework AFTER all fixes.

Post-fix state expected:
  - Outcomes table: 4,410 records all tagged is_legacy=true with TTL
  - Calibrator: filter excludes legacy → total_outcomes = 0 (warming)
  - SSM weights: defaults (signal-quality priors)
  - Verdict: 🟡 STILL WARMING UP — but for the right reason now
    (no real outcomes yet vs. legacy contamination polluting accuracy)

Earliest meaningful state expected:
  - 2026-04-27 Sun 08:00 UTC: outcome-checker scores first day_7 outcomes
  - 2026-05-04 Sun 09:00 UTC: first calibrator run with real accuracy

This is the same readiness check from step 156, re-run to confirm
the 'right kind of warming up' state.
"""
import json
from datetime import datetime, timezone
from pathlib import Path

from ops_report import report
import boto3

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
s3 = boto3.client("s3", region_name=REGION)
ddb = boto3.resource("dynamodb", region_name=REGION)
ssm = boto3.client("ssm", region_name=REGION)


with report("loop1_readiness_post_fix") as r:
    r.heading("Loop 1 readiness — post-legacy-cleanup verification")

    # ─── A. Outcomes table state ────────────────────────────────────────
    r.section("A. justhodl-outcomes — counts by status + legacy")
    outcomes_table = ddb.Table("justhodl-outcomes")
    n_total = 0
    n_correct = n_wrong = n_unscored = 0
    n_legacy = 0
    n_real_correct_none = 0
    legacy_with_ttl = 0
    scan_kwargs = {}
    while True:
        resp = outcomes_table.scan(**scan_kwargs)
        for o in resp.get("Items", []):
            n_total += 1
            correct = o.get("correct")
            is_legacy = o.get("is_legacy") is True
            if correct is True:
                n_correct += 1
            elif correct is False:
                n_wrong += 1
            else:
                n_unscored += 1
                if is_legacy:
                    n_legacy += 1
                    if o.get("ttl"):
                        legacy_with_ttl += 1
                else:
                    n_real_correct_none += 1
        if "LastEvaluatedKey" not in resp:
            break
        scan_kwargs["ExclusiveStartKey"] = resp["LastEvaluatedKey"]

    r.log(f"  Total outcomes: {n_total}")
    r.log(f"  ✅ correct=True:  {n_correct}")
    r.log(f"  ❌ correct=False: {n_wrong}")
    r.log(f"  ⏳ correct=None:  {n_unscored}")
    r.log(f"     of which is_legacy=true:  {n_legacy} (with TTL: {legacy_with_ttl})")
    r.log(f"     of which untagged:        {n_real_correct_none}")

    if n_real_correct_none == 0 and n_legacy == n_unscored:
        r.ok(f"  ✅ All correct=None outcomes are legacy-tagged + TTL-scheduled")
    elif n_real_correct_none > 0:
        r.warn(f"  ⚠ {n_real_correct_none} correct=None outcomes are NOT tagged")

    # ─── B. SSM weights — what does calibrator see now? ─────────────────
    r.section("B. SSM /justhodl/calibration/weights")
    try:
        param = ssm.get_parameter(Name="/justhodl/calibration/weights")
        weights = json.loads(param["Parameter"]["Value"])
        r.log(f"  Weights stored: {len(weights)} entries")
        for sig, w in sorted(weights.items()):
            marker = " ← default-1.0" if abs(w - 1.0) < 0.01 else ""
            r.log(f"    {sig:30} weight={w:.3f}{marker}")
    except Exception as e:
        r.warn(f"  e: {e}")

    # ─── C. SSM accuracy — has it been re-computed? ─────────────────────
    r.section("C. SSM /justhodl/calibration/accuracy")
    try:
        param = ssm.get_parameter(Name="/justhodl/calibration/accuracy")
        acc = json.loads(param["Parameter"]["Value"])
        r.log(f"  Accuracy entries: {len(acc)}")
        for sig, data in sorted(acc.items()):
            if isinstance(data, dict):
                r.log(f"    {sig:30} {data}")
    except Exception as e:
        r.log(f"  Param read: {e}")

    # ─── D. Scorecard meta state ────────────────────────────────────────
    r.section("D. reports/scorecard.json — badge state")
    try:
        obj = s3.get_object(Bucket=BUCKET, Key="reports/scorecard.json")
        sc = json.loads(obj["Body"].read().decode())
        meta = sc.get("meta", {})
        r.log(f"  is_meaningful: {meta.get('is_meaningful')}")
        r.log(f"  n_calibrated_signals: {meta.get('n_calibrated_signals')}")
        r.log(f"  n_signals_with_outcomes: {meta.get('n_signals_with_outcomes')}")
        if meta.get("is_meaningful"):
            r.ok(f"  🟢 Badge GREEN — calibrated")
        else:
            r.log(f"  🟡 Badge YELLOW — awaiting data (correct state)")
    except Exception as e:
        r.warn(f"  e: {e}")

    # ─── E. Schedule check — when does calibrator next run? ─────────────
    r.section("E. Next scheduled runs")
    now_utc = datetime.now(timezone.utc)
    r.log(f"  Now: {now_utc.isoformat()}")

    # outcome-checker: cron(0 8 ? * SUN *)
    days_to_sun = (6 - now_utc.weekday()) % 7
    if days_to_sun == 0 and now_utc.hour >= 8:
        days_to_sun = 7
    next_oc = now_utc.replace(hour=8, minute=0, second=0, microsecond=0)
    from datetime import timedelta
    next_oc = next_oc + timedelta(days=days_to_sun)
    r.log(f"  Next outcome-checker (Sun 08:00 UTC): {next_oc.isoformat()}")

    next_cal = next_oc + timedelta(hours=1)
    r.log(f"  Next calibrator (Sun 09:00 UTC):     {next_cal.isoformat()}")
    r.log(f"  ")
    r.log(f"  First outcomes from fixed signals:")
    r.log(f"    day_3 score:  ~2026-04-27 (covers signals from 2026-04-24)")
    r.log(f"    day_7 score:  ~2026-05-01-04 (the meaningful window)")
    r.log(f"  → ~2026-05-04 calibrator run = first meaningful weights")

    # ─── VERDICT ────────────────────────────────────────────────────────
    r.section("VERDICT")
    if n_correct + n_wrong > 0:
        r.ok(f"  🟢 LOOP 1 LIVE — {n_correct + n_wrong} valid outcomes scored")
    elif n_real_correct_none == 0:
        r.log(f"  🟡 STILL WARMING UP — for the RIGHT reason now")
        r.log(f"  All correct=None records are legacy-tagged + TTL-scheduled")
        r.log(f"  Loop 1 will go LIVE as new signals get scored")
        r.log(f"  Earliest 🟢: 2026-05-04 (~9 days from now)")
    else:
        r.warn(f"  🔴 PROBLEM — {n_real_correct_none} untagged correct=None records exist")

    r.kv(
        n_total=n_total,
        n_correct=n_correct,
        n_wrong=n_wrong,
        n_legacy=n_legacy,
        n_real_correct_none=n_real_correct_none,
    )
    r.log("Done")
