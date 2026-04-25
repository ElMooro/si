#!/usr/bin/env python3
"""
Step 156 — Loop 1 readiness verification framework.

Loop 1\\'s calibration becomes meaningful when at least one signal type
has ≥30 scored outcomes (not just ≥30 logged signals — they need to
reach their day_7 check window AND get scored as correct/wrong).

Today (2026-04-25) the system has:
  - 188 entries per signal type (signal-logger ran 4×/day for 30+ days)
  - 0 outcomes scored (outcome-checker runs Sun 8AM but only scores
    signals that have aged ≥7 days. Logger started 2026-03-12;
    earliest signals are now ~44 days old, so SOME should already be
    eligible)
  - So actually outcomes might already be scoring — let me check

This framework is the single source of truth for \"is Loop 1 working?\"
Run it any day. It returns:
  🟡 STILL WARMING — n_scored_outcomes < 30 for all signals
  🟢 LIVE — at least one signal has ≥30 scored, calibrator is producing
            real weights, intelligence-report.json shows is_meaningful=true

Specifically checks:

  A. justhodl-outcomes DDB table — count outcomes by correct status
     - correct=True  → predicted right
     - correct=False → predicted wrong
     - correct=None  → not yet scored (still in their wait window)

  B. SSM /justhodl/calibration/weights — are weights diverging from 1.0?
     If all weights = 1.0, calibrator hasn\\'t had enough data yet.

  C. intelligence-report.json calibration field
     - is_meaningful: false today, true once data accumulates

  D. reports/scorecard.json
     - meta.is_meaningful → controls reports.html badge color

  E. learning/improvement_log.json — has prompt-iterator skipped or
     iterated? skip_no_data is the expected state today.

  F. portfolio/pnl-history.json from Loop 2 — accumulating snapshots?

This step is purely diagnostic. It re-runs idempotently.
"""
import json
import os
from datetime import datetime, timezone
from pathlib import Path

from ops_report import report
import boto3

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"

s3 = boto3.client("s3", region_name=REGION)
ddb = boto3.resource("dynamodb", region_name=REGION)
ssm = boto3.client("ssm", region_name=REGION)


with report("loop1_readiness_check") as r:
    r.heading("Loop 1 readiness — is calibration meaningful yet?")

    # ─── A. Outcomes table — what\\'s scored? ─────────────────────────────
    r.section("A. justhodl-outcomes — count by status")
    outcomes_table = ddb.Table("justhodl-outcomes")

    try:
        # Scan with filter (table is small enough for full scan)
        all_items = []
        scan_kwargs = {}
        while True:
            resp = outcomes_table.scan(**scan_kwargs)
            all_items.extend(resp.get("Items", []))
            if "LastEvaluatedKey" not in resp:
                break
            scan_kwargs["ExclusiveStartKey"] = resp["LastEvaluatedKey"]

        n_total = len(all_items)
        n_correct = sum(1 for o in all_items if o.get("correct") is True)
        n_wrong   = sum(1 for o in all_items if o.get("correct") is False)
        n_unscored = sum(1 for o in all_items if o.get("correct") is None)

        r.log(f"  Total outcomes: {n_total}")
        r.log(f"  ✅ correct=True:  {n_correct}")
        r.log(f"  ❌ correct=False: {n_wrong}")
        r.log(f"  ⏳ correct=None:  {n_unscored} (waiting for day_7 check)")

        # By signal_type
        by_signal = {}
        for o in all_items:
            sig = o.get("signal_type", "unknown")
            by_signal.setdefault(sig, {"total": 0, "scored": 0,
                                       "correct": 0, "wrong": 0})
            by_signal[sig]["total"] += 1
            if o.get("correct") is True:
                by_signal[sig]["scored"] += 1
                by_signal[sig]["correct"] += 1
            elif o.get("correct") is False:
                by_signal[sig]["scored"] += 1
                by_signal[sig]["wrong"] += 1

        r.log(f"\n  By signal_type ({len(by_signal)} types):")
        # Sort by scored count desc
        sorted_signals = sorted(by_signal.items(),
                                key=lambda kv: -kv[1]["scored"])
        signals_above_30 = []
        for sig, stats in sorted_signals[:20]:
            scored = stats["scored"]
            total = stats["total"]
            if scored > 0:
                acc = stats["correct"] / scored * 100
                marker = " ← ≥30 SCORED" if scored >= 30 else ""
                r.log(f"    {sig:30} scored={scored:>3}/{total:>3} acc={acc:.0f}%{marker}")
                if scored >= 30:
                    signals_above_30.append((sig, scored, acc))
            else:
                r.log(f"    {sig:30} scored=0/{total} (none yet)")

        if signals_above_30:
            r.ok(f"\n  ✅ {len(signals_above_30)} signal types above ≥30 scored threshold")
        else:
            r.log(f"\n  Still warming up — no signal type has ≥30 scored outcomes yet")
    except Exception as e:
        r.warn(f"  outcomes scan: {e}")
        signals_above_30 = []

    # ─── B. SSM calibration weights ────────────────────────────────────
    r.section("B. SSM /justhodl/calibration/weights")
    try:
        param = ssm.get_parameter(Name="/justhodl/calibration/weights")
        weights = json.loads(param["Parameter"]["Value"])
        r.log(f"  Weights stored: {len(weights)} entries")
        diverged = []
        uniform = []
        for sig, w in sorted(weights.items()):
            if abs(w - 1.0) > 0.01:
                diverged.append((sig, w))
            else:
                uniform.append(sig)
        r.log(f"  Weights at 1.0 (default): {len(uniform)}")
        r.log(f"  Weights diverged from 1.0: {len(diverged)}")
        if diverged:
            r.ok(f"\n  ✅ Calibrator is producing real weights:")
            for sig, w in diverged:
                r.log(f"    {sig:30} weight={w:.3f}")
        else:
            r.log(f"\n  All weights uniform — calibrator hasn't had enough scored data")
    except ssm.exceptions.ParameterNotFound:
        r.log(f"  SSM weights parameter not yet set — calibrator hasn't run yet")
        diverged = []
    except Exception as e:
        r.warn(f"  SSM read: {e}")
        diverged = []

    # ─── C. SSM accuracy parameter ─────────────────────────────────────
    r.section("C. SSM /justhodl/calibration/accuracy")
    try:
        param = ssm.get_parameter(Name="/justhodl/calibration/accuracy")
        acc = json.loads(param["Parameter"]["Value"])
        r.log(f"  Accuracy data: {len(acc)} entries")
        for sig, data in sorted(acc.items())[:10]:
            if isinstance(data, dict):
                r.log(f"    {sig:30} {data}")
    except ssm.exceptions.ParameterNotFound:
        r.log(f"  Accuracy parameter not yet set")
    except Exception as e:
        r.warn(f"  SSM read: {e}")

    # ─── D. intelligence-report.json calibration field ─────────────────
    r.section("D. intelligence-report.json — is_meaningful flag")
    try:
        obj = s3.get_object(Bucket=BUCKET, Key="intelligence-report.json")
        data = json.loads(obj["Body"].read().decode())
        cal = data.get("calibration", {})
        is_m = cal.get("is_meaningful")
        n_sig = cal.get("n_signals")
        scored_meta = cal.get("scored_outcomes_per_signal", {})
        r.log(f"  is_meaningful: {is_m}")
        r.log(f"  n_signals: {n_sig}")
        if scored_meta:
            r.log(f"  scored_outcomes_per_signal: {scored_meta}")
        scores = data.get("scores", {})
        cc = scores.get("calibrated_composite")
        rc = scores.get("raw_composite")
        r.log(f"  calibrated_composite: {cc}")
        r.log(f"  raw_composite: {rc}")
        if cc is not None and rc is not None and abs(cc - rc) > 0.5:
            r.ok(f"  ✅ calibrated DIFFERS from raw — calibration affecting output")
        elif is_m:
            r.log(f"  is_meaningful=True but cc≈rc — weights very close to 1.0")
        else:
            r.log(f"  Still standby (uniform weights)")
    except Exception as e:
        r.warn(f"  intel-report: {e}")

    # ─── E. reports/scorecard.json badge state ─────────────────────────
    r.section("E. reports/scorecard.json — what does the badge show?")
    try:
        obj = s3.get_object(Bucket=BUCKET, Key="reports/scorecard.json")
        sc = json.loads(obj["Body"].read().decode())
        meta = sc.get("meta", {})
        r.log(f"  is_meaningful: {meta.get('is_meaningful')}")
        r.log(f"  n_calibrated_signals: {meta.get('n_calibrated_signals')}")
        r.log(f"  n_signals_with_outcomes: {meta.get('n_signals_with_outcomes')}")
        if meta.get("is_meaningful"):
            r.ok(f"  🟢 Badge would render GREEN — calibrated")
        else:
            r.log(f"  🟡 Badge would render YELLOW — awaiting data")
    except Exception as e:
        r.warn(f"  scorecard: {e}")

    # ─── F. learning/improvement_log.json ──────────────────────────────
    r.section("F. learning/improvement_log.json (Loop 3 prompt iterator)")
    try:
        obj = s3.get_object(Bucket=BUCKET, Key="learning/improvement_log.json")
        log = json.loads(obj["Body"].read().decode())
        if isinstance(log, list):
            r.log(f"  Total log entries: {len(log)}")
            actions = {}
            for e in log:
                a = e.get("action", "unknown")
                actions[a] = actions.get(a, 0) + 1
            for a, n in sorted(actions.items(), key=lambda x: -x[1]):
                r.log(f"    {a:30} {n}")
            if "applied_proposal" in actions:
                r.ok(f"  ✅ Iterator has APPLIED prompt changes — Loop 3 LIVE")
            elif "skip_healthy" in actions:
                r.ok(f"  Iterator running, accuracy is healthy")
            elif "skip_no_data" in actions:
                r.log(f"  Iterator running, awaiting scored data")
        else:
            r.log(f"  Unexpected structure: {type(log)}")
    except Exception as e:
        r.log(f"  No iteration log yet (Loop 3 hasn't run any meaningful iteration)")

    # ─── G. portfolio/pnl-history.json (Loop 2) ────────────────────────
    r.section("G. portfolio/pnl-history.json — Loop 2 PnL accumulation")
    try:
        obj = s3.get_object(Bucket=BUCKET, Key="portfolio/pnl-history.json")
        pnl = json.loads(obj["Body"].read().decode())
        snaps = pnl.get("snapshots", [])
        r.log(f"  Snapshots: {len(snaps)}")
        if snaps:
            first = snaps[0]
            last = snaps[-1]
            r.log(f"  First: {first.get('as_of')} bh={first.get('buy_and_hold_return_pct')}% "
                  f"khalid={first.get('khalid_return_pct')}%")
            r.log(f"  Last:  {last.get('as_of')} bh={last.get('buy_and_hold_return_pct')}% "
                  f"khalid={last.get('khalid_return_pct')}%")
            if len(snaps) >= 7:
                r.ok(f"  ✅ ≥7 days of PnL data — could start meaningful comparison")
    except Exception as e:
        r.log(f"  PnL history: {e}")

    # ─── H. investor-debate/_index.json (Loop 4) ───────────────────────
    r.section("H. investor-debate/_index.json — Loop 4 watchlist debate")
    try:
        obj = s3.get_object(Bucket=BUCKET, Key="investor-debate/_index.json")
        debate = json.loads(obj["Body"].read().decode())
        n_tickers = debate.get("n_tickers", 0)
        r.log(f"  n_tickers debated: {n_tickers}")
        gen_at = debate.get("generated_at")
        if gen_at:
            try:
                age_h = (datetime.now(timezone.utc) -
                         datetime.fromisoformat(gen_at.replace("Z", "+00:00"))
                         ).total_seconds() / 3600
                r.log(f"  age: {age_h:.1f}h")
            except Exception:
                pass
        summary = debate.get("summary", {})
        if summary:
            informative = sum(1 for tk, info in summary.items()
                              if info.get("debate_informative"))
            avg_novelty = (sum(info.get("avg_novelty", 0) for info in summary.values())
                           / len(summary)) if summary else 0
            r.log(f"  Debates marked 'informative': {informative}/{len(summary)}")
            r.log(f"  Avg novelty score: {avg_novelty:.2f}")
    except Exception as e:
        r.log(f"  No debate output yet — first run is nightly 03:00 UTC")

    # ─── VERDICT ────────────────────────────────────────────────────────
    r.section("VERDICT — Loop 1 status")
    if signals_above_30:
        r.ok(f"  🟢 LIVE — calibration is producing meaningful output")
        r.ok(f"  {len(signals_above_30)} signals at ≥30 scored")
        if diverged:
            r.ok(f"  {len(diverged)} weights diverged from 1.0")
    else:
        r.log(f"  🟡 STILL WARMING UP")
        r.log(f"  Need ≥30 scored outcomes per signal type")
        r.log(f"  Calibrator runs Sundays 9:00 UTC; outcome-checker Sun 8:00 UTC")
        r.log(f"  Earliest signal logged 2026-03-12 (44 days ago) — eligible")
        r.log(f"  for day_7 scoring. Next calibrator run will progress the count.")

    r.kv(
        signals_at_30_plus=len(signals_above_30),
        weights_diverged=len(diverged),
        outcomes_correct=n_correct if 'n_correct' in dir() else 0,
        outcomes_wrong=n_wrong if 'n_wrong' in dir() else 0,
        outcomes_unscored=n_unscored if 'n_unscored' in dir() else 0,
    )
    r.log("Done")
