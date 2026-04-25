#!/usr/bin/env python3
"""
Step 132 — Loop 1B v4: integrate calibration at scores level (CLEAN).

Step 131 used a regex that didn't match because the actual return
pattern in generate_full_intelligence is `return report` after a
`report = {...}` dict assignment, not `return {...timestamp:` inline.

This step uses explicit STRING anchors (no regex) for both insertions:
  - Anchor 1: `    return report` → insert calibration computation
    block right BEFORE this line. Block defines _loop1_calibrated_scores
    + _loop1_top_meta from local variables (khalid_index, repo_score,
    ml_risk_score, carry_score).
  - Anchor 2: the existing scores dict block → replace with version
    that spreads **_loop1_calibrated_scores and adds 'calibration'
    field after.

Plus cleans up the dormant step 129 patches in _synthesize_pred
(those fields are present in the source but never reach output —
they're invisible noise).

This is the safer pattern — explicit string anchors fail loudly
with a clear "couldn't find anchor" message rather than silently
mismatching like regex did.
"""
import io
import json
import os
import time
import zipfile
from pathlib import Path

from ops_report import report
import boto3

REGION = "us-east-1"
REPO_ROOT = Path(os.environ.get("GITHUB_WORKSPACE", os.getcwd()))

lam = boto3.client("lambda", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)


with report("integrate_calibration_v4_clean") as r:
    r.heading("Loop 1B v4 — clean integration with explicit string anchors")

    src_path = REPO_ROOT / "aws/lambdas/justhodl-intelligence/source/lambda_function.py"
    src = src_path.read_text()
    original_lines = src.count("\n")
    r.log(f"  Starting source: {len(src):,}B, {original_lines} lines")

    # ════════════════════════════════════════════════════════════════════
    # CLEANUP — remove step 129's dormant patches in _synthesize_pred
    # (they're present but invisible because pred isn't returned by handler)
    # ════════════════════════════════════════════════════════════════════
    r.section("1. Clean up dormant patches in _synthesize_pred")

    # Revert risk_dict back to original (3-key form)
    OLD_RISK_BLOCK_BLOATED = '''    # Risk from edge-data composite + plumbing stress
    edge_score=edge.get("composite_score", 0) if isinstance(edge, dict) else 0
    plumb=repo.get("stress", {}) if isinstance(repo, dict) else {}
    plumb_score_for_blend = plumb.get("score", 0) if isinstance(plumb, dict) else 0

    # ─── Loop 1: calibration-weighted composite ──────────────────────
    # Blend edge_composite + plumbing_stress + khalid_index using
    # historical accuracy weights from /justhodl/calibration/weights.
    # Falls back to uniform avg if calibrator hasn't run or doesn't
    # have enough scored outcomes yet.
    ki_raw_for_blend = rpt.get("khalid_index", {})
    if isinstance(ki_raw_for_blend, dict):
        ki_score_for_blend = ki_raw_for_blend.get("score", 0) or 0
    else:
        ki_score_for_blend = ki_raw_for_blend or 0

    blend_inputs = {}
    if edge_score:
        blend_inputs["edge_composite"] = float(edge_score)
    if plumb_score_for_blend:
        blend_inputs["plumbing_stress"] = float(plumb_score_for_blend)
    if ki_score_for_blend:
        blend_inputs["khalid_index"] = float(ki_score_for_blend)

    blended = blend_score(blend_inputs) if blend_inputs else {
        "value": 0.0, "raw_value": 0.0, "contributions": [],
        "total_weight": 0.0, "is_calibrated": False, "n_calibrated": 0,
    }

    risk_dict={
        "composite_score": edge_score,           # unchanged — backward compat
        "plumbing_stress": plumb.get("score", 0),
        "regime": edge.get("regime", "UNKNOWN") if isinstance(edge, dict) else "UNKNOWN",
        # NEW: calibration-weighted view (Loop 1)
        "calibrated_composite": round(blended["value"], 2),
        "raw_composite": round(blended["raw_value"], 2),
        "calibration_meta": {
            "is_calibrated": blended["is_calibrated"],
            "n_calibrated": blended["n_calibrated"],
            "n_signals": len(blend_inputs),
            "contributions": blended["contributions"],
        },
    }'''

    OLD_RISK_BLOCK_CLEAN = '''    # Risk from edge-data composite + plumbing stress
    edge_score=edge.get("composite_score", 0) if isinstance(edge, dict) else 0
    plumb=repo.get("stress", {}) if isinstance(repo, dict) else {}
    risk_dict={
        "composite_score": edge_score,
        "plumbing_stress": plumb.get("score", 0),
        "regime": edge.get("regime", "UNKNOWN") if isinstance(edge, dict) else "UNKNOWN",
    }'''

    if OLD_RISK_BLOCK_BLOATED in src:
        src = src.replace(OLD_RISK_BLOCK_BLOATED, OLD_RISK_BLOCK_CLEAN)
        r.ok("  Reverted bloated risk_dict to clean form")
    else:
        r.log("  risk_dict already clean")

    # Revert pred return calibration meta
    OLD_RETURN_BLOATED = '''    cal = get_calibration() if _CALIBRATION_AVAILABLE else None
    pred_calibration_meta = {
        "is_meaningful": getattr(cal, "is_meaningful", False),
        "n_weights": len(getattr(cal, "weights", {}) or {}),
        "n_accuracy_entries": len(getattr(cal, "accuracy", {}) or {}),
        "available": _CALIBRATION_AVAILABLE,
    }

    return {
        "executive_summary": exec_summary,
        "liquidity": {},                # not fabricating
        "calibration": pred_calibration_meta,
        "risk": risk_dict,'''

    NEW_RETURN_CLEAN = '''    return {
        "executive_summary": exec_summary,
        "liquidity": {},                # not fabricating
        "risk": risk_dict,'''

    if OLD_RETURN_BLOATED in src:
        src = src.replace(OLD_RETURN_BLOATED, NEW_RETURN_CLEAN)
        r.ok("  Reverted pred return")

    # ════════════════════════════════════════════════════════════════════
    # INTEGRATION — add calibration at the report assembly stage
    # ════════════════════════════════════════════════════════════════════
    r.section("2. Insert calibration computation block before 'return report'")

    # Anchor: the literal text just before "return report"
    ANCHOR_RETURN = "    }\n    \n    return report\n"

    LOOP1_COMPUTE_BLOCK = '''    }
    
    # ─── Loop 1: calibration-weighted composite ────────────────────────
    # Adds 'calibrated_composite' + 'raw_composite' to scores, plus a
    # top-level 'calibration' field with diagnostics.
    # Today: weights are 1.0 (calibrator data sparse) so calibrated = raw.
    # After May 2: as outcomes get scored, historical-accuracy weights
    # diverge and calibrated_composite tracks reality.
    _loop1_blend_inputs = {}
    try:
        if khalid_index is not None:
            _v = float(khalid_index)
            if _v > 0:
                _loop1_blend_inputs["khalid_index"] = _v
    except (TypeError, ValueError):
        pass
    try:
        if repo_score is not None:
            _v = float(repo_score)
            if _v > 0:
                _loop1_blend_inputs["plumbing_stress"] = _v
    except (TypeError, ValueError):
        pass
    try:
        if ml_risk_score is not None:
            _v = float(ml_risk_score)
            if _v > 0:
                _loop1_blend_inputs["ml_risk"] = _v
    except (TypeError, ValueError):
        pass
    try:
        if carry_score is not None:
            _v = float(carry_score)
            if _v > 0:
                _loop1_blend_inputs["carry_risk"] = _v
    except (TypeError, ValueError):
        pass

    if _loop1_blend_inputs:
        _loop1_blended = blend_score(_loop1_blend_inputs)
        _loop1_calibrated_scores = {
            'calibrated_composite': round(_loop1_blended['value'], 2),
            'raw_composite': round(_loop1_blended['raw_value'], 2),
        }
        _loop1_top_meta = {
            'is_meaningful': _loop1_blended['is_calibrated'],
            'n_calibrated': _loop1_blended['n_calibrated'],
            'n_signals': len(_loop1_blend_inputs),
            'contributions': _loop1_blended['contributions'],
        }
    else:
        _loop1_calibrated_scores = {}
        _loop1_top_meta = {
            'is_meaningful': False, 'n_calibrated': 0,
            'n_signals': 0, 'contributions': [],
        }

    # Inject into the assembled report
    if isinstance(report, dict) and 'scores' in report and isinstance(report['scores'], dict):
        report['scores'].update(_loop1_calibrated_scores)
        report['calibration'] = _loop1_top_meta
    
    return report
'''

    if ANCHOR_RETURN not in src:
        r.fail(f"  Anchor not found in source")
        # Show what's actually there
        for line in src.split("\n"):
            if "return report" in line:
                idx = src.find(line)
                ctx = src[max(0, idx - 50):idx + 50]
                r.log(f"    Found 'return report' but with context: {ctx!r}")
        raise SystemExit(1)

    # Count occurrences — must be exactly 1 (it's only in generate_full_intelligence)
    count = src.count(ANCHOR_RETURN)
    if count != 1:
        r.fail(f"  Anchor found {count} times — expected exactly 1")
        raise SystemExit(1)

    src = src.replace(ANCHOR_RETURN, LOOP1_COMPUTE_BLOCK)
    r.ok("  Inserted Loop 1 computation block before 'return report'")

    # Validate
    src_path.write_text(src)
    new_lines = src.count("\n")
    r.log(f"  Patched source: {len(src):,}B, {new_lines} lines (Δ {new_lines - original_lines} lines)")

    import ast
    try:
        ast.parse(src)
        r.ok("  Syntax OK")
    except SyntaxError as e:
        r.fail(f"  Syntax error after patch: {e}")
        # Show 10 lines of context around the error
        if hasattr(e, 'lineno') and e.lineno:
            lines = src.split("\n")
            for i in range(max(0, e.lineno - 5), min(len(lines), e.lineno + 5)):
                marker = " >>> " if i == e.lineno - 1 else "     "
                r.log(f"  {marker}L{i+1}: {lines[i]}")
        raise SystemExit(1)

    # ════════════════════════════════════════════════════════════════════
    # DEPLOY
    # ════════════════════════════════════════════════════════════════════
    r.section("3. Re-deploy Lambda")
    name = "justhodl-intelligence"
    src_dir = REPO_ROOT / "aws/lambdas/justhodl-intelligence/source"

    buf = io.BytesIO()
    files_added = 0
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zout:
        for src_file in sorted(src_dir.rglob("*.py")):
            arcname = str(src_file.relative_to(src_dir))
            info = zipfile.ZipInfo(arcname)
            info.external_attr = 0o644 << 16
            zout.writestr(info, src_file.read_text())
            files_added += 1
    zbytes = buf.getvalue()

    lam.update_function_code(
        FunctionName=name, ZipFile=zbytes, Architectures=["arm64"],
    )
    lam.get_waiter("function_updated").wait(
        FunctionName=name, WaiterConfig={"Delay": 3, "MaxAttempts": 30}
    )
    r.ok(f"  Re-deployed ({files_added} files, {len(zbytes):,}B zip)")

    # ════════════════════════════════════════════════════════════════════
    # VERIFY
    # ════════════════════════════════════════════════════════════════════
    r.section("4. Invoke + verify intelligence-report.json")
    time.sleep(3)
    invoke_start = time.time()
    resp = lam.invoke(FunctionName=name, InvocationType="RequestResponse")
    elapsed = time.time() - invoke_start
    if resp.get("FunctionError"):
        payload = resp.get("Payload").read().decode()[:1000]
        r.fail(f"  FunctionError: {payload}")
        raise SystemExit(1)
    r.ok(f"  Invoked in {elapsed:.1f}s")

    obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="intelligence-report.json")
    data = json.loads(obj["Body"].read().decode("utf-8"))

    scores = data.get("scores", {})
    cal = data.get("calibration", {})

    r.log(f"\n  scores in intelligence-report.json:")
    for k, v in scores.items():
        marker = " ← NEW" if k in ("calibrated_composite", "raw_composite") else ""
        r.log(f"    {k:25} {v}{marker}")

    r.log(f"\n  Top-level 'calibration':")
    if cal:
        r.log(f"    is_meaningful: {cal.get('is_meaningful')}")
        r.log(f"    n_calibrated:  {cal.get('n_calibrated')}")
        r.log(f"    n_signals:     {cal.get('n_signals')}")
        r.log(f"    contributions:")
        for c in cal.get("contributions", []):
            r.log(f"      {c.get('signal_type'):20} score={c.get('score'):.1f} "
                  f"weight={c.get('weight'):.2f} calibrated={c.get('calibrated')}")
    else:
        r.warn("    (missing)")

    loop1_active = "calibrated_composite" in scores and "calibration" in data
    if loop1_active:
        r.ok(f"\n  ✅ Loop 1 active in justhodl-intelligence")
        cc = scores.get('calibrated_composite')
        rc = scores.get('raw_composite')
        r.log(f"     calibrated_composite={cc}, raw_composite={rc}")
        if cc == rc:
            r.log(f"     (currently equal — uniform weighting because calibrator")
            r.log(f"      doesn't have ≥30 scored outcomes yet)")
            r.log(f"     (will diverge ~May 2 when post-Week-1 signals hit day_7)")
    else:
        r.fail(f"\n  ✗ Loop 1 fields missing")

    r.kv(
        zip_size=len(zbytes),
        invoke_s=f"{elapsed:.1f}",
        loop1_active=loop1_active,
        is_meaningful=cal.get("is_meaningful") if cal else False,
        calibrated_composite=scores.get("calibrated_composite"),
        raw_composite=scores.get("raw_composite"),
        n_signals=cal.get("n_signals") if cal else 0,
    )
    r.log("Done")
