#!/usr/bin/env python3
"""
Step 157 — Diagnose the correct=None bug.

Step 156 surfaced 4377 outcomes in justhodl-outcomes table, ALL with
correct=None. Either the scoring loop is broken OR signals are missing
baseline_price.

This step is purely diagnostic. It:
  A. Reads 5 RECENT outcomes (sorted by created_at if present)
     and inspects every field
  B. Reads 5 corresponding SIGNALS to see if baseline_price is set
  C. Counts pending vs unscoreable vs scored signals in justhodl-signals
  D. Inspects 1 signal of each type to see field presence
  E. Verdict: is the scoring loop correctly handling new signals,
     or are NEW signals still being logged without baseline_price?

NO code changes. Pure investigation.
"""
import json
import os
from datetime import datetime, timezone

from ops_report import report
import boto3
from boto3.dynamodb.conditions import Attr

REGION = "us-east-1"
ddb = boto3.resource("dynamodb", region_name=REGION)


def decimal_to_native(d):
    """Recursively convert Decimal → float for JSON pretty-print."""
    from decimal import Decimal
    if isinstance(d, Decimal):
        return float(d)
    if isinstance(d, dict):
        return {k: decimal_to_native(v) for k, v in d.items()}
    if isinstance(d, list):
        return [decimal_to_native(v) for v in d]
    return d


with report("diagnose_correct_none_bug") as r:
    r.heading("Diagnose why 4377 outcomes have correct=None")

    outcomes = ddb.Table("justhodl-outcomes")
    signals = ddb.Table("justhodl-signals")

    # ─── A. Sample 5 recent outcomes ────────────────────────────────────
    r.section("A. Sample 5 outcomes — what fields do they have?")
    try:
        all_outcomes = []
        scan_kwargs = {"Limit": 100}
        resp = outcomes.scan(**scan_kwargs)
        all_outcomes.extend(resp.get("Items", []))
        # Pull a few more pages
        while "LastEvaluatedKey" in resp and len(all_outcomes) < 500:
            resp = outcomes.scan(
                ExclusiveStartKey=resp["LastEvaluatedKey"], Limit=100,
            )
            all_outcomes.extend(resp.get("Items", []))

        r.log(f"  Sampled {len(all_outcomes)} outcomes")
        # Sort by signal_id (assumes timestamp embedded) and take 5 most recent
        all_outcomes.sort(key=lambda o: o.get("signal_id", ""), reverse=True)
        for i, o in enumerate(all_outcomes[:5]):
            o = decimal_to_native(o)
            r.log(f"\n  Outcome {i+1}:")
            for k, v in sorted(o.items()):
                vstr = str(v)[:80]
                r.log(f"    {k:20} = {vstr}")
    except Exception as e:
        r.fail(f"  outcomes scan: {e}")

    # ─── B. Look at corresponding signals ───────────────────────────────
    r.section("B. Look at signals_table for the SIGNAL_IDs we just sampled")
    try:
        signal_ids = [o.get("signal_id") for o in all_outcomes[:5]]
        for sid in signal_ids:
            if not sid:
                continue
            try:
                resp = signals.get_item(Key={"signal_id": sid})
                signal = decimal_to_native(resp.get("Item"))
                if not signal:
                    r.warn(f"  No signal found for {sid[:8]}...")
                    continue
                r.log(f"\n  Signal {sid[:30]}:")
                # Just print key fields
                for k in ("signal_type", "predicted_direction", "baseline_price",
                          "ticker", "measure_against", "status",
                          "check_timestamps", "outcomes", "captured_at"):
                    if k in signal:
                        v = signal[k]
                        if k == "outcomes" and isinstance(v, dict):
                            r.log(f"    {k:25} {len(v)} entries")
                        elif k == "check_timestamps" and isinstance(v, dict):
                            r.log(f"    {k:25} {sorted(v.keys())}")
                        else:
                            vstr = str(v)[:80]
                            r.log(f"    {k:25} = {vstr}")
            except Exception as e:
                r.warn(f"    Lookup {sid[:8]}: {e}")
    except Exception as e:
        r.fail(f"  signals lookup: {e}")

    # ─── C. Count signals by status ─────────────────────────────────────
    r.section("C. Count signals by status")
    try:
        # Scan all signals, group by status
        all_signals = []
        scan_kwargs = {}
        while True:
            resp = signals.scan(**scan_kwargs)
            all_signals.extend(resp.get("Items", []))
            if "LastEvaluatedKey" not in resp:
                break
            scan_kwargs["ExclusiveStartKey"] = resp["LastEvaluatedKey"]

        r.log(f"  Total signals: {len(all_signals)}")

        # Group by status
        by_status = {}
        for s in all_signals:
            st = s.get("status", "?")
            by_status[st] = by_status.get(st, 0) + 1
        for st, n in sorted(by_status.items(), key=lambda x: -x[1]):
            r.log(f"    status={st:15} {n}")

        # Of pending: how many have baseline_price?
        pending = [s for s in all_signals if s.get("status") == "pending"]
        with_baseline = sum(1 for s in pending
                            if float(s.get("baseline_price") or 0) > 0)
        without_baseline = len(pending) - with_baseline
        r.log(f"\n  Pending signals: {len(pending)}")
        r.log(f"    with baseline_price>0: {with_baseline}")
        r.log(f"    without (or zero):     {without_baseline}")
        if without_baseline > 0:
            r.warn(f"  ⚠ {without_baseline} pending signals missing baseline_price")
            r.warn(f"    These will all score as correct=None until fixed.")
    except Exception as e:
        r.fail(f"  signals scan: {e}")

    # ─── D. Inspect ONE recent signal of each type ──────────────────────
    r.section("D. Latest signal of each type — does it have baseline_price?")
    try:
        # Group by signal_type, find latest
        latest_by_type = {}
        for s in all_signals:
            t = s.get("signal_type", "?")
            cap = s.get("captured_at", "")
            if t not in latest_by_type or cap > latest_by_type[t].get("captured_at", ""):
                latest_by_type[t] = s

        types_with_baseline = 0
        types_without = 0
        for t, s in sorted(latest_by_type.items()):
            bp = s.get("baseline_price") or 0
            cap = s.get("captured_at", "?")[:19]
            ticker = s.get("measure_against") or s.get("ticker", "?")
            mark = "✅" if float(bp) > 0 else "❌"
            r.log(f"    {mark} {t:30} ticker={ticker:8} baseline=${bp} captured={cap}")
            if float(bp) > 0:
                types_with_baseline += 1
            else:
                types_without += 1

        if types_without > 0:
            r.warn(f"\n  ⚠ {types_without} signal types still missing baseline_price")
            r.warn(f"  These will continue producing correct=None outcomes until fixed.")
        else:
            r.ok(f"\n  ✅ All signal types have baseline_price for latest entry")
    except Exception as e:
        r.fail(f"  latest signals: {e}")

    # ─── E. Compare 'created_at' on most-recent outcome vs status fixes ─
    r.section("E. When were these correct=None outcomes created?")
    # Sort outcomes by checked_at, get oldest and newest
    try:
        outs_with_ts = [o for o in all_outcomes if o.get("checked_at")]
        if outs_with_ts:
            outs_sorted = sorted(outs_with_ts, key=lambda o: o.get("checked_at", ""))
            r.log(f"  Outcomes with checked_at timestamps: {len(outs_sorted)}")
            r.log(f"    Oldest: {outs_sorted[0].get('checked_at')}")
            r.log(f"    Newest: {outs_sorted[-1].get('checked_at')}")
        # By signal_type, count correct=None
        none_by_type = {}
        for o in all_outcomes:
            if o.get("correct") is None:
                t = o.get("signal_type", "?")
                none_by_type[t] = none_by_type.get(t, 0) + 1
        r.log(f"\n  correct=None by signal_type:")
        for t, n in sorted(none_by_type.items(), key=lambda x: -x[1]):
            r.log(f"    {t:30} {n}")
    except Exception as e:
        r.warn(f"  e: {e}")

    r.kv(
        n_outcomes_sampled=len(all_outcomes),
        n_signals_total=len(all_signals) if 'all_signals' in dir() else 0,
        types_with_baseline=types_with_baseline if 'types_with_baseline' in dir() else 0,
        types_without_baseline=types_without if 'types_without' in dir() else 0,
    )
    r.log("Done")
