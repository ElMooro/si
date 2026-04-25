#!/usr/bin/env python3
"""
Step 129 — Loop 1B retry: integrate calibration into justhodl-intelligence.

Step 128 failed because aws/shared/calibration.py from step 127 was
never committed back to git (the run-ops workflow's auto-commit
didn't cover that directory). The workflow has been patched in this
commit to also include aws/shared/, but to avoid timing dependencies,
this script embeds the helper source as a string constant.

This step:
  1. Writes aws/shared/calibration.py (canonical — workflow will
     auto-commit it now that scope is widened)
  2. Writes the SAME content into
     aws/lambdas/justhodl-intelligence/source/calibration.py
  3. Patches lambda_function.py to import + use blend_score
     (same patches as the broken step 128)
  4. Re-deploys, sync invokes, inspects output
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


# ─── EMBEDDED: calibration helper module source ──────────────────────────
# This is the canonical content. Step 127 wrote it to aws/shared/ but
# that directory wasn't in the workflow auto-commit scope. Embedding
# here removes the dependency.
CALIBRATION_HELPER = '''"""
Calibration helper — load weights + accuracy from SSM and provide
weight(signal_type) + blend_score(scores_dict) for every prediction-
producing Lambda.

Loaded once per Lambda cold start. Falls back to weight=1.0 if
calibration data is missing or not yet meaningful (n < 30 per signal).

Usage:
    from calibration import weight, blend_score, get_calibration

    # Single weight
    w = weight("khalid_index")  # 1.0 default if no calibration

    # Blend multiple signals
    result = blend_score({
        "khalid_index": 65.0,
        "edge_composite": 72.0,
        "crypto_fear_greed": 40.0,
    })
    # → {"value": 62.3, "contributions": [...], "total_weight": 2.1,
    #    "is_calibrated": True}
"""
import json
import os
import time
from typing import Dict, Optional

try:
    import boto3
    _ssm = boto3.client("ssm", region_name=os.environ.get("AWS_REGION", "us-east-1"))
except Exception:
    _ssm = None

# Module-level cache. Refreshed every CACHE_TTL seconds, or on cold start.
_cache = {
    "loaded_at": 0,
    "weights": {},
    "accuracy": {},
    "is_meaningful": False,
}
_CACHE_TTL_SECONDS = 600  # 10 min — calibrator runs weekly, so this is generous

# A signal needs this many scored outcomes before its calibrated
# weight is considered meaningful. Below this, fall back to 1.0.
_MIN_N_FOR_MEANINGFUL = 30


class Calibration:
    """Snapshot of calibration data at the time of fetch."""
    def __init__(self, weights: Dict[str, float],
                 accuracy: Dict[str, dict],
                 is_meaningful: bool):
        self.weights = weights
        self.accuracy = accuracy
        self.is_meaningful = is_meaningful

    def weight(self, signal_type: str) -> float:
        """Return the weight for a signal_type. Always returns a float.

        Logic:
          - If calibration not meaningful → 1.0 (uniform)
          - If signal has < 30 scored outcomes → 1.0 (insufficient data)
          - Otherwise → the calibrated weight (typically 0.5 to 1.5)
        """
        if not self.is_meaningful:
            return 1.0
        acc_entry = self.accuracy.get(signal_type, {})
        if isinstance(acc_entry, dict):
            n_scored = acc_entry.get("n_correct", 0) + acc_entry.get("n_wrong", 0)
        else:
            n_scored = 0
        if n_scored < _MIN_N_FOR_MEANINGFUL:
            return 1.0
        w = self.weights.get(signal_type)
        if w is None or not isinstance(w, (int, float)):
            return 1.0
        # Clamp to [0.1, 2.0] — defensive against calibrator errors
        return max(0.1, min(2.0, float(w)))

    def accuracy_pct(self, signal_type: str) -> Optional[float]:
        """Return measured accuracy 0-1, or None if not enough data."""
        acc_entry = self.accuracy.get(signal_type, {})
        if not isinstance(acc_entry, dict):
            return None
        n_scored = acc_entry.get("n_correct", 0) + acc_entry.get("n_wrong", 0)
        if n_scored < _MIN_N_FOR_MEANINGFUL:
            return None
        return acc_entry.get("accuracy")

    def is_signal_calibrated(self, signal_type: str) -> bool:
        """True if this specific signal has enough data to be calibrated."""
        if not self.is_meaningful:
            return False
        acc_entry = self.accuracy.get(signal_type, {})
        if not isinstance(acc_entry, dict):
            return False
        n_scored = acc_entry.get("n_correct", 0) + acc_entry.get("n_wrong", 0)
        return n_scored >= _MIN_N_FOR_MEANINGFUL


def _fetch() -> Calibration:
    """Read both SSM parameters; return Calibration snapshot.
    Falls back to empty (uniform weights) on any error."""
    if _ssm is None:
        return Calibration({}, {}, is_meaningful=False)

    weights = {}
    accuracy = {}
    is_meaningful = False
    try:
        w_resp = _ssm.get_parameter(Name="/justhodl/calibration/weights")
        weights = json.loads(w_resp["Parameter"]["Value"])
        if not isinstance(weights, dict):
            weights = {}
    except Exception:
        weights = {}

    try:
        a_resp = _ssm.get_parameter(Name="/justhodl/calibration/accuracy")
        accuracy = json.loads(a_resp["Parameter"]["Value"])
        if not isinstance(accuracy, dict):
            accuracy = {}
    except Exception:
        accuracy = {}

    # Calibration is meaningful if at least one signal has >= MIN_N
    # scored outcomes (correct in {True, False} — not None).
    for entry in accuracy.values():
        if isinstance(entry, dict):
            n_scored = entry.get("n_correct", 0) + entry.get("n_wrong", 0)
            if n_scored >= _MIN_N_FOR_MEANINGFUL:
                is_meaningful = True
                break

    return Calibration(weights, accuracy, is_meaningful)


def get_calibration(force_refresh: bool = False) -> Calibration:
    """Get current Calibration snapshot. Cached for _CACHE_TTL_SECONDS."""
    now = time.time()
    if force_refresh or now - _cache["loaded_at"] > _CACHE_TTL_SECONDS:
        snapshot = _fetch()
        _cache["weights"] = snapshot.weights
        _cache["accuracy"] = snapshot.accuracy
        _cache["is_meaningful"] = snapshot.is_meaningful
        _cache["loaded_at"] = now
        return snapshot
    return Calibration(
        weights=_cache["weights"],
        accuracy=_cache["accuracy"],
        is_meaningful=_cache["is_meaningful"],
    )


def weight(signal_type: str) -> float:
    """Convenience: get the weight for one signal_type."""
    return get_calibration().weight(signal_type)


def blend_score(scores: Dict[str, float],
                default_weight: float = 1.0) -> dict:
    """Produce a calibration-weighted average of multiple signal scores.

    scores: {signal_type: numeric_score}
    Returns:
      {
        "value":         the weighted average (float),
        "raw_value":     the unweighted average (for comparison),
        "contributions": [{signal_type, score, weight, contribution}, ...],
        "total_weight":  sum of weights (mostly informational),
        "is_calibrated": True if at least one signal had real calibration,
        "n_calibrated":  count of signals with real calibration in this blend
      }
    """
    cal = get_calibration()
    contributions = []
    weighted_sum = 0.0
    total_weight = 0.0
    raw_sum = 0.0
    n_calibrated = 0
    n = 0

    for sig_type, score in scores.items():
        try:
            score_f = float(score)
        except (TypeError, ValueError):
            continue
        n += 1
        raw_sum += score_f
        w = cal.weight(sig_type)
        if cal.is_signal_calibrated(sig_type):
            n_calibrated += 1
        weighted_sum += score_f * w
        total_weight += w
        contributions.append({
            "signal_type": sig_type,
            "score": score_f,
            "weight": w,
            "contribution": score_f * w,
            "calibrated": cal.is_signal_calibrated(sig_type),
        })

    return {
        "value": (weighted_sum / total_weight) if total_weight > 0 else 0.0,
        "raw_value": (raw_sum / n) if n > 0 else 0.0,
        "contributions": contributions,
        "total_weight": total_weight,
        "is_calibrated": n_calibrated > 0,
        "n_calibrated": n_calibrated,
    }
'''


with report("integrate_calibration_v2") as r:
    r.heading("Loop 1B retry — embedded helper, integrate into justhodl-intelligence")

    # ─── 1. Write helper to both locations ──────────────────────────────
    r.section("1. Write helper to both canonical + lambda-local")

    # Canonical: aws/shared/calibration.py (now in workflow auto-commit scope)
    canonical_dir = REPO_ROOT / "aws/shared"
    canonical_dir.mkdir(parents=True, exist_ok=True)
    (canonical_dir / "calibration.py").write_text(CALIBRATION_HELPER)
    r.ok(f"  Wrote canonical: aws/shared/calibration.py ({len(CALIBRATION_HELPER):,}B)")

    # Lambda-local: aws/lambdas/justhodl-intelligence/source/calibration.py
    target = REPO_ROOT / "aws/lambdas/justhodl-intelligence/source/calibration.py"
    target.write_text(CALIBRATION_HELPER)
    r.ok(f"  Wrote lambda-local: {target.relative_to(REPO_ROOT)}")

    # ─── 2. Patch lambda_function.py ────────────────────────────────────
    r.section("2. Patch lambda_function.py")
    lf_path = REPO_ROOT / "aws/lambdas/justhodl-intelligence/source/lambda_function.py"
    src = lf_path.read_text()

    # Detect prior partial patches and back them out before re-applying
    # (in case step 128 wrote partial state before erroring)
    if "from calibration import" in src or "_CALIBRATION_AVAILABLE" in src:
        r.warn("  Source already has calibration markers — checking what state it's in")
        # If the patches already applied cleanly, skip
        if "calibrated_composite" in src and "blend_inputs" in src:
            r.log("  Looks like prior patches landed — skipping patch step")
            patches_already_applied = True
        else:
            r.fail("  Inconsistent state — manual cleanup required")
            raise SystemExit(1)
    else:
        patches_already_applied = False

    if not patches_already_applied:
        # 2a. Add import
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
        if old_imports not in src:
            r.fail("  Couldn't find expected imports block")
            raise SystemExit(1)
        src = src.replace(old_imports, new_imports)
        r.ok("  Added calibration import")

        # 2b. Patch risk_dict
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

        if OLD_RISK_BLOCK not in src:
            r.fail("  Couldn't find risk_dict block — has source changed?")
            raise SystemExit(1)
        src = src.replace(OLD_RISK_BLOCK, NEW_RISK_BLOCK)
        r.ok("  Patched risk_dict")

        # 2c. Add top-level calibration meta
        OLD_RETURN = '''    return {
        "executive_summary": exec_summary,
        "liquidity": {},                # not fabricating
        "risk": risk_dict,'''

        NEW_RETURN = '''    cal = get_calibration() if _CALIBRATION_AVAILABLE else None
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
            r.ok("  Added top-level calibration meta")

        lf_path.write_text(src)
        r.log(f"  Patched lambda_function.py: {len(src):,}B")

    # Validate
    import ast
    ast.parse(src if not patches_already_applied else lf_path.read_text())
    r.ok("  Syntax OK")

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
    r.log(f"  Bundled {files_added} files, {len(zbytes):,}B zip")

    lam.update_function_code(
        FunctionName=name,
        ZipFile=zbytes,
        Architectures=["arm64"],
    )
    lam.get_waiter("function_updated").wait(
        FunctionName=name, WaiterConfig={"Delay": 3, "MaxAttempts": 30}
    )
    r.ok(f"  Re-deployed {name}")

    # ─── 4. Sync invoke + inspect ───────────────────────────────────────
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
    except Exception:
        body = None

    risk = {}
    cal = {}
    if body and isinstance(body, dict):
        # Find pred or risk top-level
        if "pred" in body and isinstance(body["pred"], dict):
            risk = body["pred"].get("risk", {})
            cal = body["pred"].get("calibration", {})
        elif "risk" in body:
            risk = body.get("risk", {})
            cal = body.get("calibration", {})

        r.log(f"\n  In risk_dict:")
        r.log(f"    composite_score (legacy):     {risk.get('composite_score')}")
        r.log(f"    calibrated_composite (NEW):   {risk.get('calibrated_composite')}")
        r.log(f"    raw_composite (NEW):          {risk.get('raw_composite')}")
        cm = risk.get("calibration_meta", {})
        if cm:
            r.log(f"    meta: is_calibrated={cm.get('is_calibrated')}, "
                  f"n_calibrated={cm.get('n_calibrated')}, "
                  f"n_signals={cm.get('n_signals')}")
            for c in cm.get("contributions", []):
                r.log(f"      {c.get('signal_type'):20} "
                      f"score={c.get('score'):.1f}  w={c.get('weight'):.2f}  "
                      f"calibrated={c.get('calibrated')}")

        r.log(f"\n  In top-level pred.calibration:")
        if cal:
            for k, v in cal.items():
                r.log(f"    {k:25} {v}")

        if risk.get("calibrated_composite") is not None:
            r.ok("\n  ✅ Loop 1 active in justhodl-intelligence")
        else:
            r.warn("\n  ⚠ calibrated_composite missing in output")

    r.kv(
        helper_size=len(CALIBRATION_HELPER),
        zip_size=len(zbytes),
        invoke_duration_s=f"{elapsed:.1f}",
        loop1_active=bool(risk.get("calibrated_composite") is not None),
        is_meaningful=cal.get("is_meaningful") if cal else False,
    )
    r.log("Done")
