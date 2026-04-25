#!/usr/bin/env python3
"""
Step 113 — Fix reports-builder scorecard math.

Bug: compute_scorecard divides correct/total where total includes
all outcomes (including those with correct=null because they haven't
been scored yet, or because they came from pre-fix unscoreable
signals). This makes every signal look like 0% hit rate.

Fix: per-signal-type, separate scored from unscored. Compute hit_rate
only over scored. Add a `scored` field to the output so reports.html
can show "X / Y scored" honestly.

Also adjust by_horizon and window_hit_rate the same way.

After this:
  - signal types with 0 scored outcomes (everything except the
    9-each post-fix signals) will show hit_rate=null with a
    "no scored outcomes" badge in the UI
  - signal types with some scored outcomes will show a real
    hit rate
  - The 4-card stats row in reports.html ('Scored: 0') will
    update once any outcomes are actually correct=True/False

Plus a small frontend tweak — instead of showing 0% in red for
0/0 cases, show — (em-dash) and a tiny "n=0 scored" note.
"""
import io
import json
import os
import re
import time
import zipfile
from pathlib import Path

from ops_report import report
import boto3

REGION = "us-east-1"
REPO_ROOT = Path(os.environ.get("GITHUB_WORKSPACE", os.getcwd()))

lam = boto3.client("lambda", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)


with report("fix_reports_builder_math") as r:
    r.heading("Fix reports-builder hit_rate calc — only score truly-scored outcomes")

    # ─── 1. Patch Lambda ────────────────────────────────────────────────
    r.section("1. Patch reports-builder source")
    src_path = REPO_ROOT / "aws/lambdas/justhodl-reports-builder/source/lambda_function.py"
    src = src_path.read_text()

    new_func = '''def compute_scorecard(signals, outcomes):
    """Group outcomes by signal_type and compute metrics.

    Critical: hit_rate is computed over SCORED outcomes only (those
    where correct is True or False). Unscored outcomes (correct=None)
    are not yet eligible for scoring and shouldn't drag the rate down.
    """
    # Build signal_id -> signal map
    sig_by_id = {s.get("signal_id"): s for s in signals if s.get("signal_id")}

    # Group outcomes by signal_type
    by_type = defaultdict(list)
    for o in outcomes:
        sid = o.get("signal_id")
        sig = sig_by_id.get(sid)
        if not sig:
            continue
        st = signal_type_of(sig)
        by_type[st].append({**o, "signal": sig})

    now = datetime.now(timezone.utc)
    scorecard = []
    for st, items in by_type.items():
        total = len(items)
        # Filter to scored outcomes (correct is explicitly True or False).
        # correct=None means not yet scored or unscoreable.
        scored_items = [i for i in items if i.get("correct") in (True, False)]
        scored = len(scored_items)
        correct = sum(1 for i in scored_items if i.get("correct") is True)
        hit_rate = (correct / scored) if scored else None

        # Magnitude error — only over scored items, requires both
        # predicted_magnitude and actual_change.
        mag_errors = []
        for i in scored_items:
            pred = i.get("signal", {}).get("predicted_magnitude_pct")
            actual = i.get("actual_change_pct") or i.get("actual_pct")
            if pred is not None and actual is not None:
                try:
                    mag_errors.append(abs(float(pred) - float(actual)))
                except Exception:
                    pass
        avg_mag_err = (sum(mag_errors) / len(mag_errors)) if mag_errors else None

        # By horizon — also filter to scored only
        by_horizon = defaultdict(lambda: {"total": 0, "correct": 0, "scored": 0})
        for i in items:
            h = i.get("horizon_days") or i.get("signal", {}).get("horizon_days_primary")
            try: h = int(h) if h is not None else None
            except Exception: h = None
            if h is None:
                continue
            by_horizon[h]["total"] += 1
            if i.get("correct") in (True, False):
                by_horizon[h]["scored"] += 1
                if i.get("correct") is True:
                    by_horizon[h]["correct"] += 1
        for h in by_horizon:
            sc = by_horizon[h]["scored"]
            by_horizon[h]["hit_rate"] = (by_horizon[h]["correct"] / sc) if sc else None

        # Trend over time windows (using scored_at) — scored items only
        def window_hit_rate(days):
            cutoff = now - timedelta(days=days)
            in_window = [i for i in scored_items
                         if (parse_iso(i.get("scored_at") or i.get("checked_at") or i.get("logged_at"))
                             or datetime(1970, 1, 1, tzinfo=timezone.utc)) >= cutoff]
            if not in_window: return None
            c = sum(1 for i in in_window if i.get("correct") is True)
            return c / len(in_window) if in_window else None

        scorecard.append({
            "signal_type": st,
            "total": total,           # all outcomes
            "scored": scored,         # only correct in {True, False}
            "correct": correct,       # only correct=True
            "hit_rate": round(hit_rate, 4) if hit_rate is not None else None,
            "avg_magnitude_error_pct": round(avg_mag_err, 3) if avg_mag_err is not None else None,
            "by_horizon": dict(by_horizon),
            "trend_30d": window_hit_rate(30),
            "trend_60d": window_hit_rate(60),
            "trend_90d": window_hit_rate(90),
        })

    # Sort by total desc (most data first)
    scorecard.sort(key=lambda x: -x["total"])
    return scorecard'''

    pattern = re.compile(
        r"def compute_scorecard\(signals, outcomes\):.*?(?=\ndef |\Z)",
        re.DOTALL,
    )
    if not pattern.search(src):
        r.fail("  Couldn't locate compute_scorecard")
        raise SystemExit(1)
    src_new = pattern.sub(new_func + "\n\n", src)
    src_path.write_text(src_new)
    r.ok(f"  Patched {src_path.name}")

    # Validate
    import ast
    try:
        ast.parse(src_new)
        r.ok("  Syntax OK")
    except SyntaxError as e:
        r.fail(f"  Syntax: {e}")
        raise SystemExit(1)

    # ─── 2. Re-deploy ──────────────────────────────────────────────────
    r.section("2. Re-deploy reports-builder")
    name = "justhodl-reports-builder"
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

    # ─── 3. Invoke + verify scorecard now has scored field ─────────────
    r.section("3. Invoke + verify scorecard.json")
    time.sleep(3)
    resp = lam.invoke(FunctionName=name, InvocationType="RequestResponse")
    if resp.get("FunctionError"):
        payload = resp.get("Payload").read().decode()
        r.fail(f"  FunctionError: {payload[:500]}")
    else:
        body = json.loads(json.loads(resp.get("Payload").read().decode()).get("body", "{}"))
        r.ok(f"  Invoked: {body}")

    # Read the new scorecard
    obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="reports/scorecard.json")
    data = json.loads(obj["Body"].read().decode("utf-8"))
    sc = data.get("signal_scorecard", [])
    r.log(f"  Scorecard rows: {len(sc)}")
    r.log(f"  Sample (top 5 by total):")
    for row in sc[:5]:
        r.log(f"    {row['signal_type']:25} total={row['total']:>4}  scored={row.get('scored', 0):>4}  "
              f"hit_rate={row.get('hit_rate')!r}")

    r.kv(
        scorecard_rows=len(sc),
        rows_with_scored_data=sum(1 for r_ in sc if r_.get("scored", 0) > 0),
    )
    r.log("Done")
