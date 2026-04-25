#!/usr/bin/env python3
"""
Step 128 — Loop 1B: integrate calibration into justhodl-intelligence.

The intelligence Lambda (874 LOC) is the biggest aggregator in the
system. It's invoked by morning-intelligence, intelligence.html,
and other downstream consumers. Patching it cleanly is the highest-
leverage integration of Loop 1.

Strategy:
  1. Copy aws/shared/calibration.py into the Lambda's source folder
  2. Find the _synthesize_pred() risk_dict construction (line 208)
  3. Compute a NEW field 'calibrated_composite' alongside the existing
     'composite_score'. This is the calibration-weighted blend of
     all available signals.
  4. Add a top-level 'calibration' field to the synthesized 'pred'
     output so downstream consumers can see what was calibrated and
     what wasn't.
  5. KEY DECISION: Don't replace existing fields. Add new ones.
     Existing consumers continue to work; new consumers can opt-in.
     Specifically:
       risk_dict["composite_score"]              # unchanged
       risk_dict["calibrated_composite"]         # NEW — weighted
       pred["calibration"] = {meta info}         # NEW — diagnostic

This is the safe rollout pattern. After a few days of observation,
if calibrated_composite tracks well, we can flip downstream
consumers to use it. If something looks off, the unchanged fields
mean nothing breaks.

After integration:
  6. Re-deploy
  7. Sync invoke and verify the response includes calibration data
  8. Print a sample of the new fields for inspection
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


with report("integrate_calibration_into_intelligence") as r:
    r.heading("Loop 1B — integrate calibration into justhodl-intelligence")

    # ─── 1. Copy the helper module ──────────────────────────────────────
    r.section("1. Copy calibration helper to Lambda source")
    helper_src = (REPO_ROOT / "aws/shared/calibration.py").read_text()
    target = REPO_ROOT / "aws/lambdas/justhodl-intelligence/source/calibration.py"
    target.write_text(helper_src)
    r.ok(f"  Copied calibration.py → {target.relative_to(REPO_ROOT)} ({len(helper_src):,}B)")

    # ─── 2. Patch lambda_function.py ────────────────────────────────────
    r.section("2. Patch lambda_function.py to use calibration")
    lf_path = REPO_ROOT / "aws/lambdas/justhodl-intelligence/source/lambda_function.py"
    src = lf_path.read_text()

    # Add import — after the existing imports
    if "from calibration import" not in src:
        old_imports = """import json,boto3,os,ssl,traceback
from datetime import datetime,timezone,timedelta
from urllib import request as urllib_request"""
        new_imports = """import json,boto3,os,ssl,traceback
from datetime import datetime,timezone,timedelta
from urllib import request as urllib_request

# Calibration helper — Loop 1: weight signals by historical accuracy.
# Falls back to uniform weighting when calibrator data is sparse.
try:
    from calibration import blend_score, get_calibration
    _CALIBRATION_AVAILABLE = True
except Exception as _e:
    print(f"WARN: calibration module unavailable: {_e}")
    _CALIBRATION_AVAILABLE = False
    def blend_score(scores, default_weight=1.0):
        # Fallback impl: simple uniform average + meta
        if not scores: return {"value": 0.0, "raw_value": 0.0, "contributions": [],
                                "total_weight": 0.0, "is_calibrated": False, "n_calibrated": 0}
        n = len(scores)
        avg = sum(float(v) for v in scores.values()) / n
        return {"value": avg, "raw_value": avg, "contributions": [],
                "total_weight": n, "is_calibrated": False, "n_calibrated": 0}
    def get_calibration():
        class _C:
            is_meaningful = False
            weights = {}
            accuracy = {}
            def weight(self, _): return 1.0
            def is_signal_calibrated(self, _): return False
        return _C()"""
        src = src.replace(old_imports, new_imports)
        r.ok("  Added calibration import (with fallback)")

    # Now patch _synthesize_pred to compute calibrated_composite
    # Find the risk_dict block. Replace with version that adds calibration.
    OLD_RISK_BLOCK = '''    # Risk from edge-data composite + plumbing stress
    edge_score=edge.get("composite_score", 0) if isinstance(edge, dict) else 0
    plumb=repo.get("stress", {}) if isinstance(repo, dict) else {}
    risk_dict={
        "composite_score": edge_score,
        "plumbing_stress": plumb.get("score", 0),
        "regime": edge.get("regime", "UNKNOWN") if isinstance(edge, dict) else "UNKNOWN",
    }'''

    NEW_RISK_BLOCK = '''    # Risk from edge-data composite + plumbing stress
    edge_score=edge.get("composite_score", 0) if isinstance(edge, dict) else 0
    plumb=repo.get("stress", {}) if isinstance(repo, dict) else {}
    plumb_score_for_blend = plumb.get("score", 0) if isinstance(plumb, dict) else 0

    # ─── Loop 1: calibration-weighted composite ──────────────────────
    # Blend edge_composite + plumbing_stress + (carry_risk derived) +
    # (khalid_index from rpt) using historical accuracy weights from
    # /justhodl/calibration/weights. Falls back to uniform avg if
    # calibrator hasn't run or doesn't have enough scored outcomes yet.
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
        # NEW: calibration-weighted view
        "calibrated_composite": round(blended["value"], 2),
        "raw_composite": round(blended["raw_value"], 2),
        "calibration_meta": {
            "is_calibrated": blended["is_calibrated"],
            "n_calibrated": blended["n_calibrated"],
            "n_signals": len(blend_inputs),
            "contributions": blended["contributions"],
        },
    }'''

    if OLD_RISK_BLOCK in src:
        src = src.replace(OLD_RISK_BLOCK, NEW_RISK_BLOCK)
        r.ok("  Patched risk_dict to include calibrated_composite + meta")
    else:
        r.fail("  Couldn't find risk_dict block — has source changed since step 126?")
        raise SystemExit(1)

    # Also add a top-level pred['calibration'] field for diagnostics
    OLD_RETURN = '''    return {
        "executive_summary": exec_summary,
        "liquidity": {},                # not fabricating
        "risk": risk_dict,'''

    NEW_RETURN = '''    # Include the live calibration snapshot for transparency.
    cal = get_calibration() if _CALIBRATION_AVAILABLE else None
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

    if OLD_RETURN in src:
        src = src.replace(OLD_RETURN, NEW_RETURN)
        r.ok("  Added top-level pred.calibration meta field")
    else:
        r.warn("  Couldn't find return block — non-fatal, continuing")

    # Write back
    lf_path.write_text(src)
    r.log(f"  Patched lambda_function.py: {len(src):,}B, {src.count(chr(10))} LOC")

    # Validate syntax
    import ast
    try:
        ast.parse(src)
        r.ok("  Syntax OK")
    except SyntaxError as e:
        r.fail(f"  Syntax: {e}")
        raise SystemExit(1)

    # ─── 3. Re-deploy Lambda ────────────────────────────────────────────
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
    r.log(f"  Bundled {files_added} files, {len(zbytes):,}B zip")

    # Preserve arm64 architecture (set in step 123)
    lam.update_function_code(
        FunctionName=name,
        ZipFile=zbytes,
        Architectures=["arm64"],
    )
    lam.get_waiter("function_updated").wait(
        FunctionName=name, WaiterConfig={"Delay": 3, "MaxAttempts": 30}
    )
    r.ok(f"  Re-deployed {name}")

    # ─── 4. Sync invoke + inspect output ────────────────────────────────
    r.section("4. Sync invoke + inspect calibration in output")
    time.sleep(3)
    invoke_start = time.time()
    resp = lam.invoke(FunctionName=name, InvocationType="RequestResponse")
    elapsed = time.time() - invoke_start
    if resp.get("FunctionError"):
        payload = resp.get("Payload").read().decode()[:1000]
        r.fail(f"  FunctionError: {payload}")
        raise SystemExit(1)
    payload_str = resp.get("Payload").read().decode()
    r.ok(f"  Invoked in {elapsed:.1f}s, payload {len(payload_str):,}B")

    try:
        payload = json.loads(payload_str)
        body = payload.get("body")
        if isinstance(body, str):
            body = json.loads(body)
    except Exception as e:
        r.warn(f"  Couldn't parse response: {e}")
        body = None

    if body and isinstance(body, dict):
        # Look for the calibration-related fields we added
        risk = (body.get("pred") or {}).get("risk", {}) if "pred" in body else body.get("risk", {})
        cal = (body.get("pred") or {}).get("calibration", {}) if "pred" in body else body.get("calibration", {})

        r.log(f"\n  In risk_dict:")
        r.log(f"    composite_score (legacy):     {risk.get('composite_score')}")
        r.log(f"    calibrated_composite (NEW):   {risk.get('calibrated_composite')}")
        r.log(f"    raw_composite (NEW):          {risk.get('raw_composite')}")
        cal_meta = risk.get("calibration_meta", {})
        if cal_meta:
            r.log(f"    calibration_meta:")
            r.log(f"      is_calibrated: {cal_meta.get('is_calibrated')}")
            r.log(f"      n_calibrated:  {cal_meta.get('n_calibrated')}")
            r.log(f"      n_signals:     {cal_meta.get('n_signals')}")
            for c in cal_meta.get("contributions", []):
                r.log(f"        {c.get('signal_type'):20} score={c.get('score'):.1f} "
                      f"weight={c.get('weight'):.2f} calibrated={c.get('calibrated')}")

        r.log(f"\n  In top-level pred.calibration:")
        if cal:
            r.log(f"    is_meaningful:        {cal.get('is_meaningful')}")
            r.log(f"    n_weights in SSM:     {cal.get('n_weights')}")
            r.log(f"    n_accuracy entries:   {cal.get('n_accuracy_entries')}")
            r.log(f"    helper available:     {cal.get('available')}")

        # Sanity check
        if risk.get("calibrated_composite") is not None:
            r.ok("\n  ✅ calibrated_composite is in the response — Loop 1 active")
        else:
            r.warn("\n  ⚠ calibrated_composite missing — check structure")

    r.kv(
        helper_size=len(helper_src),
        zip_size=len(zbytes),
        invoke_duration_s=f"{elapsed:.1f}",
        calibration_available_in_output=bool(risk.get("calibrated_composite") is not None) if 'risk' in dir() else False,
    )
    r.log("Done")
