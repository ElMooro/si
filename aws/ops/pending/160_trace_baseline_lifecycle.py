#!/usr/bin/env python3
"""
Step 160 — Surgical lifecycle trace of baseline_price.

Last session\\'s investigation pointed to outcome-checker stripping
baseline_price during the pending → partial transition, but the
update_item UpdateExpression should not modify that field.

This step does a controlled experiment:
  1. Pick 1 pending signal with baseline_price > 0 and at least one
     check_window already elapsed
  2. Read its full record, snapshot baseline_price
  3. Invoke outcome-checker (sync) — should process this signal
  4. Re-read the same signal_id, check baseline_price again

If baseline_price is now $0 → outcome-checker IS stripping it (somehow)
If baseline_price is still $X → strip is happening elsewhere

If the former, dump the FULL diff between before/after to find what
attributes changed. The Set #s, SET outcomes update is supposed to
preserve everything else.
"""
import json
import os
from datetime import datetime, timezone
from decimal import Decimal

from ops_report import report
import boto3

REGION = "us-east-1"
ddb = boto3.resource("dynamodb", region_name=REGION)
lam = boto3.client("lambda", region_name=REGION)


def to_native(d):
    if isinstance(d, Decimal):
        return float(d)
    if isinstance(d, dict):
        return {k: to_native(v) for k, v in d.items()}
    if isinstance(d, list):
        return [to_native(v) for v in d]
    return d


def diff_dicts(before, after):
    """Return dict of {key: (before_val, after_val)} for keys that differ."""
    diffs = {}
    all_keys = set(before.keys()) | set(after.keys())
    for k in all_keys:
        b = before.get(k, "<MISSING>")
        a = after.get(k, "<MISSING>")
        if b != a:
            diffs[k] = (b, a)
    return diffs


with report("trace_baseline_lifecycle") as r:
    r.heading("Surgical trace — does outcome-checker strip baseline_price?")

    signals = ddb.Table("justhodl-signals")

    # ─── 1. Find a candidate pending signal ─────────────────────────────
    r.section("1. Find pending signal with bp + elapsed window")

    # Scan pending signals
    all_pending = []
    scan_kwargs = {
        "FilterExpression": "#s = :s",
        "ExpressionAttributeNames": {"#s": "status"},
        "ExpressionAttributeValues": {":s": "pending"},
    }
    while True:
        resp = signals.scan(**scan_kwargs)
        all_pending.extend(resp.get("Items", []))
        if "LastEvaluatedKey" not in resp:
            break
        scan_kwargs["ExclusiveStartKey"] = resp["LastEvaluatedKey"]

    r.log(f"  Total pending: {len(all_pending)}")

    # Find one with baseline > 0 and at least one elapsed check window
    now_iso = datetime.now(timezone.utc).isoformat()
    candidates = []
    for s in all_pending:
        bp = float(s.get("baseline_price") or 0)
        check_ts = s.get("check_timestamps") or {}
        if bp <= 0:
            continue
        # At least one window elapsed?
        elapsed = [w for w, t in check_ts.items() if t < now_iso]
        if elapsed:
            candidates.append((s, elapsed))

    r.log(f"  Pending with bp + elapsed window: {len(candidates)}")
    if not candidates:
        r.warn(f"  No candidate found — can\\'t test")
        raise SystemExit(0)

    # Pick the FIRST one (deterministic)
    target_signal, elapsed_windows = candidates[0]
    target_signal = to_native(target_signal)
    sid = target_signal["signal_id"]
    r.log(f"\n  Chose signal: {sid[:30]}...")
    r.log(f"  type:           {target_signal.get('signal_type')}")
    r.log(f"  baseline_price: {target_signal.get('baseline_price')}")
    r.log(f"  status:         {target_signal.get('status')}")
    r.log(f"  measure_against: {target_signal.get('measure_against')}")
    r.log(f"  elapsed windows: {elapsed_windows}")
    r.log(f"  logged_at:      {target_signal.get('logged_at')}")

    # ─── 2. Snapshot ALL fields before ─────────────────────────────────
    r.section("2. Full BEFORE snapshot of signal record")
    before = target_signal
    for k in sorted(before.keys()):
        v = before[k]
        if isinstance(v, dict):
            r.log(f"  {k:30} dict({len(v)} keys)")
        elif isinstance(v, list):
            r.log(f"  {k:30} list({len(v)})")
        else:
            r.log(f"  {k:30} = {str(v)[:80]}")

    # ─── 3. Invoke outcome-checker ──────────────────────────────────────
    r.section("3. Invoke outcome-checker synchronously")
    import time
    t0 = time.time()
    try:
        resp = lam.invoke(
            FunctionName="justhodl-outcome-checker",
            InvocationType="RequestResponse",
        )
        elapsed = time.time() - t0
        payload = resp.get("Payload").read().decode()
        if resp.get("FunctionError"):
            r.fail(f"  FunctionError: {payload[:500]}")
        else:
            r.ok(f"  Invoked in {elapsed:.1f}s, response: {payload[:200]}")
    except Exception as e:
        r.fail(f"  invoke: {e}")
        raise SystemExit(1)

    # Brief delay for write to settle
    time.sleep(2)

    # ─── 4. Re-read the same signal ─────────────────────────────────────
    r.section("4. Re-read signal AFTER outcome-checker ran")
    resp = signals.get_item(Key={"signal_id": sid})
    after = to_native(resp.get("Item"))
    if not after:
        r.fail(f"  Signal disappeared from table!")
        raise SystemExit(1)

    r.log(f"  baseline_price (BEFORE): {before.get('baseline_price')}")
    r.log(f"  baseline_price (AFTER):  {after.get('baseline_price')}")
    r.log(f"  status (BEFORE): {before.get('status')}")
    r.log(f"  status (AFTER):  {after.get('status')}")

    bp_before = float(before.get("baseline_price") or 0)
    bp_after = float(after.get("baseline_price") or 0)

    if bp_after > 0:
        r.ok(f"  ✅ baseline_price PRESERVED (still ${bp_after})")
    else:
        r.fail(f"  ❌ baseline_price WAS STRIPPED ({bp_before} → {bp_after})")

    # ─── 5. Show ALL field diffs ────────────────────────────────────────
    r.section("5. Full diff of ALL fields BEFORE vs AFTER")
    diffs = diff_dicts(before, after)
    if not diffs:
        r.warn(f"  No fields changed — outcome-checker didn\\'t process this signal")
        r.log(f"  Possible causes:")
        r.log(f"    - elapsed windows already had outcomes (skipped)")
        r.log(f"    - get_price() failed for ticker (skipped silently)")
    else:
        r.log(f"  {len(diffs)} fields changed:")
        for k, (b, a) in sorted(diffs.items()):
            b_str = str(b)[:60]
            a_str = str(a)[:60]
            r.log(f"\n    {k}:")
            r.log(f"      BEFORE: {b_str}")
            r.log(f"      AFTER:  {a_str}")

    r.kv(
        signal_id=sid[:20],
        bp_before=bp_before,
        bp_after=bp_after,
        baseline_preserved=bp_after > 0,
        n_diffs=len(diffs),
    )
    r.log("Done")
