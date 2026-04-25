#!/usr/bin/env python3
"""
Step 131 — Loop 1B v3: integrate calibration at the RIGHT injection point.

Step 129 patched _synthesize_pred() to add calibrated_composite to
its risk_dict, but the lambda_handler doesn't return pred — it builds
a separate `report` dict in generate_full_intelligence() with its
own `scores` dict, and that report.json is the actual output.

This step:
  1. Patches generate_full_intelligence() to compute calibration AT
     the score-aggregation level. Local variables already exist:
       - khalid_index (line 324)
       - repo_score / plumbing_stress (line 401)
       - ml_risk_score (line 447)
       - carry_score (line 466)
       - vix (line 416)
  2. Injects two NEW fields into the report's `scores` dict:
       - calibrated_composite — weighted blend
       - raw_composite        — uniform avg (for comparison)
  3. Injects a NEW top-level `calibration` field with diagnostic
     info (is_meaningful, n_calibrated, contributions list)
  4. Reverts the changes step 129 made to _synthesize_pred (clean up)
  5. Re-deploys, sync invokes, reads intelligence-report.json,
     verifies new fields are present
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


with report("integrate_calibration_at_scores_level") as r:
    r.heading("Loop 1B v3 — integrate calibration at the scores aggregation level")

    src_path = REPO_ROOT / "aws/lambdas/justhodl-intelligence/source/lambda_function.py"
    src = src_path.read_text()

    # ─── 1. Revert step 129's _synthesize_pred patches ──────────────────
    r.section("1. Revert _synthesize_pred patches from step 129 (wrong injection point)")

    OLD_SYNTHESIZE_NEW = '''    # Risk from edge-data composite + plumbing stress
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

    OLD_SYNTHESIZE_ORIG = '''    # Risk from edge-data composite + plumbing stress
    edge_score=edge.get("composite_score", 0) if isinstance(edge, dict) else 0
    plumb=repo.get("stress", {}) if isinstance(repo, dict) else {}
    risk_dict={
        "composite_score": edge_score,
        "plumbing_stress": plumb.get("score", 0),
        "regime": edge.get("regime", "UNKNOWN") if isinstance(edge, dict) else "UNKNOWN",
    }'''

    if OLD_SYNTHESIZE_NEW in src:
        src = src.replace(OLD_SYNTHESIZE_NEW, OLD_SYNTHESIZE_ORIG)
        r.ok("  Reverted risk_dict in _synthesize_pred")
    else:
        r.log("  risk_dict already in original form, skipping revert")

    # Also revert the pred return calibration meta
    OLD_RETURN_NEW = '''    cal = get_calibration() if _CALIBRATION_AVAILABLE else None
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

    OLD_RETURN_ORIG = '''    return {
        "executive_summary": exec_summary,
        "liquidity": {},                # not fabricating
        "risk": risk_dict,'''

    if OLD_RETURN_NEW in src:
        src = src.replace(OLD_RETURN_NEW, OLD_RETURN_ORIG)
        r.ok("  Reverted pred return calibration meta")

    # ─── 2. Add calibration logic to generate_full_intelligence ─────────
    r.section("2. Add calibration logic at the report-build stage")

    # Find the scores dict construction in generate_full_intelligence
    # and insert calibration computation just before it.
    OLD_SCORES_BLOCK = """        'scores': {
            'khalid_index': khalid_index,
            'crisis_distance': crisis_distance,
            'plumbing_stress': repo_score,
            'ml_risk_score': ml_risk_score,
            'carry_risk_score': carry_score,
            'vix': vix,
            'move': move_idx
        },"""

    NEW_SCORES_BLOCK = """        'scores': {
            'khalid_index': khalid_index,
            'crisis_distance': crisis_distance,
            'plumbing_stress': repo_score,
            'ml_risk_score': ml_risk_score,
            'carry_risk_score': carry_score,
            'vix': vix,
            'move': move_idx,
            # ─── Loop 1: calibration-weighted composite ───────────────
            # Weighted by historical accuracy from
            # /justhodl/calibration/weights. Falls back to uniform
            # avg until calibrator has ≥30 scored outcomes per signal.
            **_loop1_calibrated_scores
        },"""

    if OLD_SCORES_BLOCK not in src:
        r.fail("  Couldn't find scores dict block")
        raise SystemExit(1)
    src = src.replace(OLD_SCORES_BLOCK, NEW_SCORES_BLOCK)
    r.ok("  Patched scores dict to include calibrated_composite + raw_composite")

    # Now add the _loop1_calibrated_scores computation block right before
    # the return statement of generate_full_intelligence. We need to
    # find the function's actual return — look for `return {` followed
    # by `'timestamp':`
    # Easier: insert it right before the "return {" near `'scores': {`
    # Find pattern: blank line then `    return {`
    # Locate it precisely.
    return_pattern = re.compile(r"(\n\s+return\s*\{\s*\n\s*'timestamp':)", re.MULTILINE)
    m_ret = return_pattern.search(src)
    if not m_ret:
        r.fail("  Couldn't find return statement of generate_full_intelligence")
        raise SystemExit(1)

    # Insert the calibration block just before this return
    insertion = """    # ─── Loop 1: compute calibration-weighted composite ─────────
    # The scores dict gets two new keys via **_loop1_calibrated_scores:
    #   calibrated_composite — historical-accuracy-weighted blend
    #   raw_composite        — uniform avg (for comparison)
    # Plus a top-level `calibration` field with diagnostic info.
    _loop1_blend_inputs = {}
    try:
        _ki_for_blend = float(khalid_index) if khalid_index is not None else None
        if _ki_for_blend is not None:
            _loop1_blend_inputs["khalid_index"] = _ki_for_blend
    except (TypeError, ValueError):
        pass
    try:
        _ps_for_blend = float(repo_score) if repo_score is not None else None
        if _ps_for_blend is not None:
            _loop1_blend_inputs["plumbing_stress"] = _ps_for_blend
    except (TypeError, ValueError):
        pass
    try:
        _ml_for_blend = float(ml_risk_score) if ml_risk_score is not None else None
        if _ml_for_blend is not None and _ml_for_blend > 0:
            _loop1_blend_inputs["ml_risk"] = _ml_for_blend
    except (TypeError, ValueError):
        pass
    try:
        _cr_for_blend = float(carry_score) if carry_score is not None else None
        if _cr_for_blend is not None and _cr_for_blend > 0:
            _loop1_blend_inputs["carry_risk"] = _cr_for_blend
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
            'is_meaningful': False,
            'n_calibrated': 0,
            'n_signals': 0,
            'contributions': [],
        }

"""
    # Insert before the return
    src = src[:m_ret.start()] + "\n" + insertion + m_ret.group(1) + src[m_ret.end():]
    r.ok("  Added _loop1_calibrated_scores computation before return")

    # Add 'calibration' as a top-level field in the returned dict.
    # Insert after the 'scores' block we just patched.
    OLD_AFTER_SCORES = """            'move': move_idx,
            # ─── Loop 1: calibration-weighted composite ───────────────
            # Weighted by historical accuracy from
            # /justhodl/calibration/weights. Falls back to uniform
            # avg until calibrator has ≥30 scored outcomes per signal.
            **_loop1_calibrated_scores
        },
        'signals': {"""
    NEW_AFTER_SCORES = """            'move': move_idx,
            # ─── Loop 1: calibration-weighted composite ───────────────
            # Weighted by historical accuracy from
            # /justhodl/calibration/weights. Falls back to uniform
            # avg until calibrator has ≥30 scored outcomes per signal.
            **_loop1_calibrated_scores
        },
        'calibration': _loop1_top_meta,
        'signals': {"""
    if OLD_AFTER_SCORES in src:
        src = src.replace(OLD_AFTER_SCORES, NEW_AFTER_SCORES)
        r.ok("  Added top-level 'calibration' field in report dict")
    else:
        r.warn("  Couldn't find post-scores block — top-level calibration not added")

    # Validate
    src_path.write_text(src)
    import ast
    try:
        ast.parse(src)
        r.ok("  Syntax OK")
    except SyntaxError as e:
        r.fail(f"  Syntax: {e}")
        raise SystemExit(1)

    # ─── 3. Re-deploy ───────────────────────────────────────────────────
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
        FunctionName=name,
        ZipFile=zbytes,
        Architectures=["arm64"],
    )
    lam.get_waiter("function_updated").wait(
        FunctionName=name, WaiterConfig={"Delay": 3, "MaxAttempts": 30}
    )
    r.ok(f"  Re-deployed ({files_added} files, {len(zbytes):,}B)")

    # ─── 4. Invoke + verify intelligence-report.json ────────────────────
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

    # Read the just-published intelligence-report.json
    obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="intelligence-report.json")
    data = json.loads(obj["Body"].read().decode("utf-8"))

    scores = data.get("scores", {})
    r.log(f"\n  scores in intelligence-report.json:")
    for k, v in scores.items():
        marker = " ← NEW" if k in ("calibrated_composite", "raw_composite") else ""
        r.log(f"    {k:25} {v}{marker}")

    cal = data.get("calibration", {})
    r.log(f"\n  Top-level 'calibration' field:")
    if cal:
        r.log(f"    is_meaningful: {cal.get('is_meaningful')}")
        r.log(f"    n_calibrated:  {cal.get('n_calibrated')}")
        r.log(f"    n_signals:     {cal.get('n_signals')}")
        r.log(f"    contributions:")
        for c in cal.get("contributions", []):
            r.log(f"      {c.get('signal_type'):20} score={c.get('score'):.1f} "
                  f"weight={c.get('weight'):.2f} calibrated={c.get('calibrated')}")
    else:
        r.warn("    (empty or missing)")

    loop1_active = "calibrated_composite" in scores and "calibration" in data
    if loop1_active:
        r.ok(f"\n  ✅ Loop 1 active in justhodl-intelligence")
        r.log(f"     scores.calibrated_composite = {scores.get('calibrated_composite')}")
        r.log(f"     scores.raw_composite = {scores.get('raw_composite')}")
        if scores.get('calibrated_composite') == scores.get('raw_composite'):
            r.log(f"     (currently equal — calibrator data not yet meaningful)")
            r.log(f"     (will diverge once outcomes are scored ~May 2)")
    else:
        r.fail(f"\n  ✗ Loop 1 fields missing in output")

    r.kv(
        zip_size=len(zbytes),
        invoke_s=f"{elapsed:.1f}",
        loop1_active=loop1_active,
        is_meaningful=cal.get("is_meaningful") if cal else False,
        calibrated=scores.get("calibrated_composite"),
        raw=scores.get("raw_composite"),
    )
    r.log("Done")
