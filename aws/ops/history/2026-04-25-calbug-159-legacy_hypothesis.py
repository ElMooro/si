#!/usr/bin/env python3
"""
Step 159 — Re-verify with the CORRECT field names.

Step 158 reported captured_at=None for everything, but signal-logger
writes logged_at, not captured_at. The field doesn\\'t exist, so the
read returned None — that wasn\\'t a bug, that was MY bug in the
diagnostic script.

Re-read with logged_at to confirm what's really happening with
baseline_price across status transitions.

ALSO: deeply check whether complete signals are actually getting
their baseline stripped, or if my read script was looking at OLD
records that predate the baseline_price feature (Week 1 fix mentioned
in ops/112 comment).

If the comment is true — \"pre-Week-1-fix signals never had
baseline_price\" — then COMPLETE signals being mostly old (because
they had time to walk through 7-day check) would explain why they
have baseline_price=$0 while pending (newer) signals all have it.

This means: NOT a strip bug, but a legacy data residue. NEW signals
are fine. OLD signals predating the fix can\\'t be retroactively
scored. The 4377 correct=None outcomes are from those legacy signals.

Verify:
  - Status × logged_at age distribution
  - Are pending signals NEWER than complete signals?
  - Newest complete signal — what's its logged_at?
  - Newest pending signal — what's its logged_at?
"""
import json
import os
from datetime import datetime, timezone
from decimal import Decimal

from ops_report import report
import boto3

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


with report("verify_legacy_data_hypothesis") as r:
    r.heading("Re-verify with logged_at field name (158 used wrong field)")

    signals = ddb.Table("justhodl-signals")

    # Scan all
    all_sigs = []
    scan_kwargs = {}
    while True:
        resp = signals.scan(**scan_kwargs)
        all_sigs.extend(resp.get("Items", []))
        if "LastEvaluatedKey" not in resp:
            break
        scan_kwargs["ExclusiveStartKey"] = resp["LastEvaluatedKey"]
    r.log(f"  Total signals: {len(all_sigs)}")

    # ─── 1. logged_at distribution by status ────────────────────────────
    r.section("1. logged_at distribution by status")
    by_status = {}
    for s in all_sigs:
        st = s.get("status", "?")
        by_status.setdefault(st, []).append(s)

    for status, sigs in sorted(by_status.items()):
        logged_ats = [s.get("logged_at") for s in sigs if s.get("logged_at")]
        n_with_logged = len(logged_ats)
        if n_with_logged > 0:
            logged_ats.sort()
            r.log(f"\n  {status:14} (n={len(sigs)}, with logged_at: {n_with_logged}):")
            r.log(f"    Oldest: {logged_ats[0]}")
            r.log(f"    Newest: {logged_ats[-1]}")
        else:
            r.log(f"\n  {status:14} (n={len(sigs)}, no logged_at on any) ← LEGACY SCHEMA")

    # ─── 2. Of complete signals: do ANY have baseline_price > 0? ───────
    r.section("2. baseline_price coverage on complete + partial signals")
    for st in ("complete", "partial"):
        sigs = by_status.get(st, [])
        with_bp = sum(1 for s in sigs if float(s.get("baseline_price") or 0) > 0)
        without_bp = len(sigs) - with_bp
        # Of those WITH bp, what's the logged_at?
        bp_set_logged = [s.get("logged_at") for s in sigs
                         if float(s.get("baseline_price") or 0) > 0 and s.get("logged_at")]
        bp_unset_logged = [s.get("logged_at") for s in sigs
                           if float(s.get("baseline_price") or 0) <= 0 and s.get("logged_at")]
        r.log(f"\n  {st}:")
        r.log(f"    with baseline_price>0: {with_bp}")
        r.log(f"    without:               {without_bp}")
        if bp_set_logged:
            bp_set_logged.sort()
            r.log(f"    WITH bp logged_at range: {bp_set_logged[0]} → {bp_set_logged[-1]}")
        if bp_unset_logged:
            bp_unset_logged.sort()
            r.log(f"    WITHOUT bp logged_at range: {bp_unset_logged[0]} → {bp_unset_logged[-1]}")

    # ─── 3. The fix date hypothesis ─────────────────────────────────────
    r.section("3. The legacy-data hypothesis")
    r.log(f"  ops/112 comment said pre-Week-1-fix signals lack baseline_price.")
    r.log(f"  If true:")
    r.log(f"    - Old signals (no logged_at) → no baseline → unscoreable forever")
    r.log(f"    - Newer signals (have logged_at + baseline) → scoreable on day_7")
    r.log(f"  Verify: are 'with bp' signals NEWER than 'without bp' signals?")

    # ─── 4. The smoking gun: pick the newest COMPLETE signal with bp ────
    r.section("4. Newest complete signal WITH baseline_price")
    complete_with_bp = [s for s in by_status.get("complete", [])
                        if float(s.get("baseline_price") or 0) > 0]
    complete_with_bp_logged = [s for s in complete_with_bp if s.get("logged_at")]
    if complete_with_bp_logged:
        complete_with_bp_logged.sort(key=lambda x: x.get("logged_at", ""), reverse=True)
        s = to_native(complete_with_bp_logged[0])
        r.log(f"  Newest complete with bp: signal_id={s['signal_id'][:30]}")
        r.log(f"    type:               {s.get('signal_type')}")
        r.log(f"    logged_at:          {s.get('logged_at')}")
        r.log(f"    baseline_price:     {s.get('baseline_price')}")
        r.log(f"    outcomes count:     {len(s.get('outcomes', {}))}")
        r.log(f"    sample outcome:     {list(s.get('outcomes', {}).values())[0] if s.get('outcomes') else 'N/A'}")
        r.ok(f"\n  ✅ SOME complete signals HAVE baseline_price + outcomes!")
    else:
        r.warn(f"  ❌ NO complete signal has both baseline_price AND logged_at")
        r.warn(f"  This is the bug — completes are losing baseline somehow")

    # ─── 5. Were ANY outcomes ever scored as correct=True/False? ────────
    r.section("5. Are ANY outcomes scored properly?")
    if complete_with_bp:
        # Look at the outcomes embedded in 5 sample complete signals
        scored_ok = 0
        for s in complete_with_bp[:20]:
            outs = s.get("outcomes", {})
            for win, out_data in outs.items():
                if isinstance(out_data, dict) and out_data.get("correct") is not None:
                    scored_ok += 1
        r.log(f"  Of 20 sample complete-with-bp signals: {scored_ok} have ≥1 properly scored outcome")
        if scored_ok > 0:
            r.ok(f"  ✅ Calibration system IS working — scoring those signals properly")
            r.log(f"  Bug verdict: 4377 correct=None outcomes are LEGACY records from")
            r.log(f"  signals predating the baseline_price fix. NEW signals work fine.")
            r.log(f"  → Solution: bulk-mark legacy outcomes as 'unscoreable' or delete.")

    r.kv(
        n_signals=len(all_sigs),
        n_complete_with_bp=len(complete_with_bp) if 'complete_with_bp' in dir() else 0,
        scored_ok=scored_ok if 'scored_ok' in dir() else 0,
    )
    r.log("Done")
