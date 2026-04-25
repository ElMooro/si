#!/usr/bin/env python3
"""
Step 158 — Pinpoint when baseline_price gets stripped.

Step 157 found:
  - Pending signals (2035): ALL have baseline_price > 0 ✅
  - Latest signal of each type: 14 of 15 have baseline_price = $0 ❌

These two facts together imply the baseline_price IS lost between
"newly logged" and "complete/partial". This step verifies that
hypothesis by:

  A. Reading 1 signal per status × per signal_type — does baseline
     exist at each status level?
  B. If pending=$X but complete=$0 for same type → confirms outcome-
     checker is stripping baseline somehow
  C. If newest signals (by captured_at) are the partial/complete ones
     → maybe signal-logger writes baseline_price IN the latest run
     but those aren\\'t the newest captured_at

This is purely diagnostic. Result will guide the fix.
"""
import json
import os
from datetime import datetime, timezone
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


with report("pinpoint_baseline_strip") as r:
    r.heading("Where is baseline_price getting stripped?")

    signals = ddb.Table("justhodl-signals")
    outcomes = ddb.Table("justhodl-outcomes")

    # ─── Scan ALL signals, group by (signal_type, status) ──────────────
    r.section("1. Scan all signals, group by (type, status), check baseline")
    all_sigs = []
    scan_kwargs = {}
    while True:
        resp = signals.scan(**scan_kwargs)
        all_sigs.extend(resp.get("Items", []))
        if "LastEvaluatedKey" not in resp:
            break
        scan_kwargs["ExclusiveStartKey"] = resp["LastEvaluatedKey"]
    r.log(f"  Total signals scanned: {len(all_sigs)}")

    # Group by (signal_type, status)
    grouped = {}
    for s in all_sigs:
        t = s.get("signal_type", "?")
        st = s.get("status", "?")
        key = (t, st)
        grouped.setdefault(key, []).append(s)

    r.log(f"\n  baseline_price coverage by (type, status):\n")
    r.log(f"    {'signal_type':30} {'status':12} {'n':>5}  {'with_bp':>8} {'%':>5}")
    type_status_summary = []
    for (t, st), sig_list in sorted(grouped.items()):
        n = len(sig_list)
        with_bp = sum(1 for s in sig_list if float(s.get("baseline_price") or 0) > 0)
        pct = with_bp / n * 100 if n else 0
        marker = " ⚠" if pct < 50 else ""
        r.log(f"    {t:30} {st:12} {n:>5}  {with_bp:>8} {pct:>4.0f}%{marker}")
        type_status_summary.append((t, st, n, with_bp, pct))

    # ─── Compute summary: types where pending HAS baseline but
    # ─── complete/partial DON\\'T → the strip is happening
    r.section("2. Verdict — where does baseline_price get lost?")
    by_type = {}
    for t, st, n, wbp, pct in type_status_summary:
        by_type.setdefault(t, {})[st] = (n, wbp, pct)

    strip_evidence = []
    for t, by_st in sorted(by_type.items()):
        pending_pct = by_st.get("pending", (0, 0, 0))[2]
        complete_pct = by_st.get("complete", (0, 0, 0))[2]
        partial_pct = by_st.get("partial", (0, 0, 0))[2]
        r.log(f"  {t:30} pending={pending_pct:>4.0f}% partial={partial_pct:>4.0f}% complete={complete_pct:>4.0f}%")
        if pending_pct >= 90 and complete_pct < 10:
            strip_evidence.append((t, pending_pct, complete_pct))

    if strip_evidence:
        r.warn(f"\n  ⚠ {len(strip_evidence)} types where pending has baseline but complete doesn\\'t:")
        for t, pp, cp in strip_evidence:
            r.warn(f"    {t}: pending {pp:.0f}%, complete {cp:.0f}%")
    else:
        r.log(f"\n  No clear strip pattern — baseline distribution is consistent")

    # ─── Sample 3 signals: 1 pending, 1 partial, 1 complete (same type) ─
    r.section("3. Walk through 3 signals of same type at 3 statuses")
    target_type = None
    # Pick a type that has all 3 statuses
    for t, by_st in by_type.items():
        if all(st in by_st for st in ("pending", "partial", "complete")):
            target_type = t
            break

    if target_type:
        r.log(f"  Type: {target_type}\n")
        for status in ("pending", "partial", "complete"):
            sig_list = grouped.get((target_type, status), [])
            if not sig_list:
                continue
            # Sort by captured_at desc, take newest
            sig_list_sorted = sorted(
                sig_list, key=lambda x: x.get("captured_at", ""), reverse=True,
            )
            s = to_native(sig_list_sorted[0])
            r.log(f"  ── status={status} (newest of {len(sig_list)}) ──")
            for k in ("signal_id", "captured_at", "baseline_price",
                      "predicted_direction", "ticker", "measure_against",
                      "outcomes", "status"):
                v = s.get(k)
                if k == "outcomes" and isinstance(v, dict):
                    r.log(f"    {k:20} {len(v)} entries: {sorted(v.keys())}")
                else:
                    vstr = str(v)[:80]
                    r.log(f"    {k:20} = {vstr}")

    # ─── Look at a sample outcome record + its source signal ────────────
    r.section("4. One outcome with correct=None — what signal_id?")
    try:
        resp = outcomes.scan(
            FilterExpression=Attr("correct").not_exists() | Attr("correct").eq(None),
            Limit=5,
        )
        for out in resp.get("Items", [])[:3]:
            out = to_native(out)
            sid = out.get("signal_id")
            if not sid:
                continue
            r.log(f"\n  outcome: {out.get('outcome_id', '?')[:40]}")
            for k in ("correct", "predicted_dir", "asset_price",
                      "outcome", "checked_at", "signal_value"):
                if k in out:
                    r.log(f"    {k:18} = {str(out[k])[:80]}")
            sig_resp = signals.get_item(Key={"signal_id": sid})
            sig = to_native(sig_resp.get("Item"))
            if sig:
                r.log(f"  source signal {sid[:30]}:")
                r.log(f"    baseline_price = {sig.get('baseline_price')}")
                r.log(f"    captured_at    = {sig.get('captured_at')}")
                r.log(f"    status         = {sig.get('status')}")
    except Exception as e:
        r.warn(f"  e: {e}")

    r.kv(
        n_signals=len(all_sigs),
        n_types=len(by_type),
        strip_evidence_types=len(strip_evidence),
    )
    r.log("Done")
