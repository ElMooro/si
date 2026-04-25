#!/usr/bin/env python3
"""
Step 112 — Fix outcome-checker.

Root cause: most signals were logged BEFORE the Week 1 baseline_price
auto-capture fix (~2026-04-16). Those signals have baseline_price=None
or 0, so score_directional returns (correct=None, "UNKNOWN", 0.0) for
all of them forever. The outcome-checker keeps re-processing them and
writing null outcomes, producing 4,307 unscorable records.

This step:
  1. Scans signals to count pre-fix (no baseline) vs post-fix
     (baseline set). Gives Khalid a clear picture.
  2. Patches outcome-checker code:
     - If baseline_price is None/0, mark the parent signal as
       status='unscoreable' (new status outside 'pending'/'partial')
       so future scans skip it.
     - Don't write the null outcome record at all (it's noise).
  3. Re-deploys outcome-checker.
  4. Does a one-time cleanup: mark existing signals with no baseline
     and no scored outcomes as 'unscoreable' so they stop getting
     re-scanned.
  5. Invokes outcome-checker to verify it runs cleanly after the
     cleanup (should find far fewer signals to process).
"""
import io
import json
import os
import time
import zipfile
from collections import Counter
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

from ops_report import report
import boto3

REGION = "us-east-1"
REPO_ROOT = Path(os.environ.get("GITHUB_WORKSPACE", os.getcwd()))

lam = boto3.client("lambda", region_name=REGION)
ddb = boto3.resource("dynamodb", region_name=REGION)


def d2f(o):
    if isinstance(o, Decimal): return float(o)
    if isinstance(o, dict):    return {k: d2f(v) for k, v in o.items()}
    if isinstance(o, list):    return [d2f(v) for v in o]
    return o


with report("fix_outcome_checker") as r:
    r.heading("Fix outcome-checker — skip signals without baseline_price")

    # ─── 1. Inventory: pre-fix vs post-fix signals ──────────────────────
    r.section("1. Signal inventory")
    sigs_table = ddb.Table("justhodl-signals")
    counts = Counter()
    by_type_baseline = Counter()
    by_type_no_baseline = Counter()

    kwargs = {}
    n = 0
    while True:
        resp = sigs_table.scan(**kwargs)
        for item in resp.get("Items", []):
            n += 1
            bp = item.get("baseline_price")
            st = item.get("signal_type", "?")
            has_baseline = bp is not None and float(bp) > 0
            status = item.get("status", "?")
            counts[f"status={status} baseline={'yes' if has_baseline else 'no'}"] += 1
            if has_baseline:
                by_type_baseline[st] += 1
            else:
                by_type_no_baseline[st] += 1
        if "LastEvaluatedKey" not in resp:
            break
        kwargs["ExclusiveStartKey"] = resp["LastEvaluatedKey"]
        if n > 8000:
            break

    r.log(f"  Scanned {n} signals")
    r.log(f"  Breakdown:")
    for k, v in sorted(counts.items(), key=lambda x: -x[1]):
        r.log(f"    {k:45} {v}")

    r.log(f"\n  Signal types WITH baseline_price (post-fix, scoreable):")
    for st, cnt in by_type_baseline.most_common():
        r.log(f"    {cnt:>5}  {st}")
    r.log(f"\n  Signal types WITHOUT baseline_price (pre-fix, unscoreable):")
    for st, cnt in by_type_no_baseline.most_common():
        r.log(f"    {cnt:>5}  {st}")

    # ─── 2. Patch outcome-checker source ────────────────────────────────
    r.section("2. Patch outcome-checker source")
    src_path = REPO_ROOT / "aws/lambdas/justhodl-outcome-checker/source/lambda_function.py"
    src = src_path.read_text()

    # Fix: early-exit if baseline is None/0, marking signal as unscoreable
    # Inject right after the `baseline = ...` line in check_pending_signals
    old_block = """        baseline    = float(signal.get("baseline_price") or 0)
        check_ts    = signal.get("check_timestamps", {})
        existing_outcomes = dict(signal.get("outcomes", {}))
        pred_type   = "relative" if pred_dir in ("OUTPERFORM", "UNDERPERFORM") else "directional"

        outcomes_updated = False"""

    new_block = '''        baseline    = float(signal.get("baseline_price") or 0)
        check_ts    = signal.get("check_timestamps", {})
        existing_outcomes = dict(signal.get("outcomes", {}))
        pred_type   = "relative" if pred_dir in ("OUTPERFORM", "UNDERPERFORM") else "directional"

        # Skip signals with no baseline_price — can't be scored.
        # Mark them as 'unscoreable' so they don't clog future scans.
        # (Patched 2026-04-25 by ops/112: pre-Week-1-fix signals never had
        # baseline_price captured; keeping them in pending status caused
        # 4,307 null-correct outcome records.)
        if baseline <= 0 and not existing_outcomes:
            try:
                table.update_item(
                    Key={"signal_id": signal_id},
                    UpdateExpression="SET #s = :s, last_checked = :t",
                    ExpressionAttributeValues={
                        ":s": "unscoreable",
                        ":t": now_iso,
                    },
                    ExpressionAttributeNames={"#s": "status"},
                )
                print(f"[CHECKER] {signal_type} [{signal_id[:8]}] → UNSCOREABLE (no baseline_price)")
            except Exception as e:
                print(f"[CHECKER] failed to mark {signal_id[:8]} unscoreable: {e}")
            continue

        outcomes_updated = False'''

    if old_block not in src:
        r.fail("  Expected code block not found — has source changed?")
        raise SystemExit(1)
    src_new = src.replace(old_block, new_block)
    src_path.write_text(src_new)
    r.ok(f"  Patched {src_path.name} (+{len(new_block) - len(old_block)}B)")

    # Validate syntax
    import ast
    try:
        ast.parse(src_new)
        r.ok("  Syntax OK")
    except SyntaxError as e:
        r.fail(f"  Syntax error: {e}")
        raise SystemExit(1)

    # ─── 3. Re-deploy outcome-checker ───────────────────────────────────
    r.section("3. Re-deploy outcome-checker")
    name = "justhodl-outcome-checker"
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zout:
        info = zipfile.ZipInfo("lambda_function.py")
        info.external_attr = 0o644 << 16
        zout.writestr(info, src_new)
    zbytes = buf.getvalue()
    lam.update_function_code(FunctionName=name, ZipFile=zbytes)
    lam.get_waiter("function_updated").wait(
        FunctionName=name, WaiterConfig={"Delay": 3, "MaxAttempts": 30}
    )
    r.ok(f"  Re-deployed ({len(zbytes)}B)")

    # ─── 4. Invoke to process + mark unscoreables ──────────────────────
    r.section("4. Invoke outcome-checker (will mark no-baseline signals as unscoreable)")
    time.sleep(3)
    invoke_start = time.time()
    resp = lam.invoke(FunctionName=name, InvocationType="RequestResponse")
    elapsed = time.time() - invoke_start
    if resp.get("FunctionError"):
        payload = resp.get("Payload").read().decode()
        r.fail(f"  FunctionError: {payload[:500]}")
    else:
        body = json.loads(json.loads(resp.get("Payload").read().decode()).get("body", "{}"))
        r.ok(f"  Invoked in {elapsed:.1f}s: processed={body.get('processed')}")

    # ─── 5. Re-scan to see new status distribution ──────────────────────
    r.section("5. Post-fix status breakdown")
    counts2 = Counter()
    kwargs = {}
    n2 = 0
    while True:
        resp = sigs_table.scan(
            ProjectionExpression="#s, baseline_price",
            ExpressionAttributeNames={"#s": "status"},
            **kwargs,
        )
        for item in resp.get("Items", []):
            n2 += 1
            bp = item.get("baseline_price")
            has_baseline = bp is not None and float(bp) > 0
            st = item.get("status", "?")
            counts2[f"status={st} baseline={'yes' if has_baseline else 'no'}"] += 1
        if "LastEvaluatedKey" not in resp:
            break
        kwargs["ExclusiveStartKey"] = resp["LastEvaluatedKey"]
        if n2 > 8000:
            break

    r.log(f"  Total signals: {n2}")
    r.log(f"  New distribution:")
    for k, v in sorted(counts2.items(), key=lambda x: -x[1]):
        r.log(f"    {k:45} {v}")

    r.kv(
        signals_total=n,
        signals_with_baseline=sum(by_type_baseline.values()),
        signals_without_baseline=sum(by_type_no_baseline.values()),
        unscoreable_marked=counts2.get("status=unscoreable baseline=no", 0),
    )
    r.log("Done")
