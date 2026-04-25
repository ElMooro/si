#!/usr/bin/env python3
"""
Step 133 — Loop 1 on justhodl-morning-intelligence.

Current state (NEEDS FIX):
  Lines 13-14: WEIGHTS_PARAM/ACCURACY_PARAM defined
  Lines 61-73: load_weights() / load_accuracy() read SSM directly
  Line 141: kw=weights.get("khalid_index",1.0) — only weights khalid
  Line 148: khalid_adj=ki*kw  — ALREADY APPLIED, but blindly. The
            calibrator currently writes khalid_index=0.5 (default for
            n_unknown=369), so today the brief sees khalid_adj that's
            HALF of khalid_raw. That's MIS-applied calibration noise,
            not signal.

Plan:
  1. Replace ad-hoc load_weights/load_accuracy with the shared
     calibration helper. The helper has the is_meaningful gate
     (≥30 scored outcomes per signal) — falls back to weight=1.0
     when calibrator output is noisy.
  2. Compute a multi-signal blended_composite covering khalid_index,
     plumbing_stress, ml_risk, carry_risk (same set as step 132).
  3. Add new metric fields to the brief's metric dict:
       blended_composite, raw_composite, calibration_active
     so the brief copy can reference them and the calibration meta
     reaches Telegram.
  4. KEEP khalid_adj field for backward compat — but fix its math:
     use the helper's weight() (which returns 1.0 today) instead of
     the raw calibrator weight 0.5.
  5. Re-deploy + sync invoke + verify learning/morning_run_log.json
     reflects the new blended_composite + calibration_active fields.
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


# Helper source (same as canonical aws/shared/calibration.py)
CALIBRATION_HELPER = '''"""
Calibration helper — load weights + accuracy from SSM and provide
weight(signal_type) + blend_score(scores_dict) for prediction-producing
Lambdas. Falls back to weight=1.0 if calibration data is missing or
not yet meaningful (n < 30 per signal).
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

_cache = {"loaded_at": 0, "weights": {}, "accuracy": {}, "is_meaningful": False}
_CACHE_TTL_SECONDS = 600
_MIN_N_FOR_MEANINGFUL = 30


class Calibration:
    def __init__(self, weights, accuracy, is_meaningful):
        self.weights = weights
        self.accuracy = accuracy
        self.is_meaningful = is_meaningful

    def weight(self, signal_type):
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
        return max(0.1, min(2.0, float(w)))

    def is_signal_calibrated(self, signal_type):
        if not self.is_meaningful:
            return False
        acc_entry = self.accuracy.get(signal_type, {})
        if not isinstance(acc_entry, dict):
            return False
        n_scored = acc_entry.get("n_correct", 0) + acc_entry.get("n_wrong", 0)
        return n_scored >= _MIN_N_FOR_MEANINGFUL


def _fetch():
    if _ssm is None:
        return Calibration({}, {}, False)
    weights, accuracy = {}, {}
    try:
        weights = json.loads(_ssm.get_parameter(Name="/justhodl/calibration/weights")["Parameter"]["Value"])
        if not isinstance(weights, dict): weights = {}
    except Exception:
        weights = {}
    try:
        accuracy = json.loads(_ssm.get_parameter(Name="/justhodl/calibration/accuracy")["Parameter"]["Value"])
        if not isinstance(accuracy, dict): accuracy = {}
    except Exception:
        accuracy = {}
    is_meaningful = False
    for entry in accuracy.values():
        if isinstance(entry, dict):
            n_scored = entry.get("n_correct", 0) + entry.get("n_wrong", 0)
            if n_scored >= _MIN_N_FOR_MEANINGFUL:
                is_meaningful = True
                break
    return Calibration(weights, accuracy, is_meaningful)


def get_calibration(force_refresh=False):
    now = time.time()
    if force_refresh or now - _cache["loaded_at"] > _CACHE_TTL_SECONDS:
        snap = _fetch()
        _cache["weights"] = snap.weights
        _cache["accuracy"] = snap.accuracy
        _cache["is_meaningful"] = snap.is_meaningful
        _cache["loaded_at"] = now
        return snap
    return Calibration(_cache["weights"], _cache["accuracy"], _cache["is_meaningful"])


def weight(signal_type):
    return get_calibration().weight(signal_type)


def blend_score(scores, default_weight=1.0):
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
            "signal_type": sig_type, "score": score_f, "weight": w,
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


with report("loop1_morning_intelligence") as r:
    r.heading("Loop 1 on justhodl-morning-intelligence")

    # ─── 1. Update canonical helper + drop into Lambda ──────────────────
    r.section("1. Drop calibration helper into morning-intelligence")
    canonical = REPO_ROOT / "aws/shared/calibration.py"
    canonical.parent.mkdir(parents=True, exist_ok=True)
    canonical.write_text(CALIBRATION_HELPER)
    r.ok(f"  Refreshed canonical: aws/shared/calibration.py ({len(CALIBRATION_HELPER):,}B)")

    target = REPO_ROOT / "aws/lambdas/justhodl-morning-intelligence/source/calibration.py"
    target.write_text(CALIBRATION_HELPER)
    r.ok(f"  Wrote lambda-local: {target.relative_to(REPO_ROOT)}")

    # ─── 2. Patch lambda_function.py ────────────────────────────────────
    r.section("2. Patch lambda_function.py")
    lf_path = REPO_ROOT / "aws/lambdas/justhodl-morning-intelligence/source/lambda_function.py"
    src = lf_path.read_text()

    # 2a. Add import (graceful fallback if helper missing)
    OLD_IMPORTS = """import os
import json,boto3,urllib.request,time,math
from datetime import datetime,timezone,timedelta
from decimal import Decimal
from collections import defaultdict
from boto3.dynamodb.conditions import Attr"""
    NEW_IMPORTS = """import os
import json,boto3,urllib.request,time,math
from datetime import datetime,timezone,timedelta
from decimal import Decimal
from collections import defaultdict
from boto3.dynamodb.conditions import Attr

# Calibration helper — Loop 1: weight signals by historical accuracy
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
        avg = sum(float(v) for v in scores.values() if v is not None) / n if n else 0.0
        return {"value": avg, "raw_value": avg, "contributions": [],
                "total_weight": float(n), "is_calibrated": False, "n_calibrated": 0}
    def get_calibration():
        class _C:
            is_meaningful = False
            weights = {}
            accuracy = {}
            def weight(self, _): return 1.0
            def is_signal_calibrated(self, _): return False
        return _C()"""
    if "from calibration import" not in src:
        if OLD_IMPORTS not in src:
            r.fail(f"  Imports anchor not found")
            raise SystemExit(1)
        src = src.replace(OLD_IMPORTS, NEW_IMPORTS)
        r.ok("  Added calibration import with fallback")
    else:
        r.log("  Calibration import already present")

    # 2b. Patch extract_metrics to add blended_composite + calibration_active
    # The existing function returns a big dict at line 145+. We'll add
    # NEW fields without removing existing ones (backward compat).
    OLD_KW_LINE = '''    ki=d.get("khalid_index") or scores.get("khalid_index",0)
    kw=weights.get("khalid_index",1.0)'''
    NEW_KW_LINE = '''    ki=d.get("khalid_index") or scores.get("khalid_index",0)
    # Loop 1: use shared calibration helper instead of raw weights dict.
    # Helper applies the is_meaningful gate (≥30 scored outcomes per
    # signal); falls back to 1.0 when calibrator data is sparse, so
    # we don't apply noisy 0.5 default weights from a calibrator that
    # has 0 scored outcomes today.
    _cal = get_calibration() if _CALIBRATION_AVAILABLE else None
    kw = _cal.weight("khalid_index") if _cal is not None else 1.0'''
    if OLD_KW_LINE in src:
        src = src.replace(OLD_KW_LINE, NEW_KW_LINE)
        r.ok("  Replaced raw kw weight with calibration helper")
    elif "_cal = get_calibration()" in src:
        r.log("  kw assignment already patched, skipping")
    else:
        r.fail("  Couldn't find ki=/kw= block")
        raise SystemExit(1)

    # 2c. Add blended_composite + calibration meta to the returned dict.
    # The dict ends with closing brace at the return. Find a stable
    # anchor — the last KNOWN trailing field.
    OLD_RETURN_TAIL = '''        "fg":fg.get("current","N/A"),
        "fg_label":fg.get("label","N/A"),
        "crypto_risk":rs.get("score","N/A"),'''
    NEW_RETURN_TAIL = '''        "fg":fg.get("current","N/A"),
        "fg_label":fg.get("label","N/A"),
        "crypto_risk":rs.get("score","N/A"),
        # ─── Loop 1: calibration-weighted multi-signal composite ───
        # Blends khalid_index + plumbing_stress + ml_risk + carry_risk
        # weighted by historical accuracy. is_calibrated is True only
        # after the calibrator has scored ≥30 outcomes for at least
        # one signal (~ early May 2026 onward).
        **(lambda inputs=({k:v for k,v in [
            ("khalid_index", float(ki["score"]) if isinstance(ki, dict) and ki.get("score") is not None
                              else (float(ki) if isinstance(ki, (int, float)) and ki else None)),
            ("plumbing_stress", float(stress.get("score")) if stress.get("score") not in (None, "N/A") else None),
            ("ml_risk", float(scores.get("ml_risk_score")) if scores.get("ml_risk_score") not in (None, "N/A") else None),
            ("carry_risk", float(scores.get("carry_risk_score")) if scores.get("carry_risk_score") not in (None, "N/A") else None),
        ] if v is not None}): {
            "blended_composite": round(blend_score(inputs)["value"], 2) if inputs else None,
            "raw_composite": round(blend_score(inputs)["raw_value"], 2) if inputs else None,
            "calibration_active": blend_score(inputs)["is_calibrated"] if inputs else False,
            "calibration_n_signals": len(inputs),
        })(),'''
    if OLD_RETURN_TAIL in src:
        src = src.replace(OLD_RETURN_TAIL, NEW_RETURN_TAIL)
        r.ok("  Added blended_composite + calibration meta to metric dict")
    elif "blended_composite" in src:
        r.log("  blended_composite already in source, skipping")
    else:
        r.fail("  Couldn't find return-tail anchor")
        raise SystemExit(1)

    # Validate
    lf_path.write_text(src)
    import ast
    try:
        ast.parse(src)
        r.ok("  Syntax OK")
    except SyntaxError as e:
        r.fail(f"  Syntax: {e}")
        if hasattr(e, 'lineno') and e.lineno:
            lines = src.split("\n")
            for i in range(max(0, e.lineno - 3), min(len(lines), e.lineno + 3)):
                marker = " >>> " if i == e.lineno - 1 else "     "
                r.log(f"  {marker}L{i+1}: {lines[i][:200]}")
        raise SystemExit(1)

    # ─── 3. Re-deploy ───────────────────────────────────────────────────
    r.section("3. Re-deploy Lambda")
    name = "justhodl-morning-intelligence"
    src_dir = REPO_ROOT / "aws/lambdas/justhodl-morning-intelligence/source"
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
    r.ok(f"  Re-deployed ({files_added} files, {len(zbytes):,}B)")

    # ─── 4. Sync invoke + inspect ───────────────────────────────────────
    r.section("4. Sync invoke + verify in S3 / log output")
    time.sleep(3)
    invoke_start = time.time()
    resp = lam.invoke(FunctionName=name, InvocationType="RequestResponse")
    elapsed = time.time() - invoke_start
    if resp.get("FunctionError"):
        payload = resp.get("Payload").read().decode()[:1500]
        r.fail(f"  FunctionError: {payload}")
        raise SystemExit(1)
    payload_str = resp.get("Payload").read().decode()
    r.ok(f"  Invoked in {elapsed:.1f}s ({len(payload_str)}B response)")

    # The handler writes learning/morning_run_log.json at the end
    try:
        obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="learning/morning_run_log.json")
        from datetime import datetime, timezone
        age_min = (datetime.now(timezone.utc) - obj["LastModified"]).total_seconds() / 60
        body = json.loads(obj["Body"].read().decode("utf-8"))
        r.log(f"\n  learning/morning_run_log.json ({obj['ContentLength']}B, age {age_min:.1f}min):")
        r.log(f"  Top keys: {sorted(body.keys()) if isinstance(body, dict) else 'not dict'}")
        if isinstance(body, dict):
            khalid = body.get("khalid", {})
            r.log(f"  khalid: {json.dumps(khalid, default=str)[:300]}")
            for key in ["weights", "outcomes", "wrong"]:
                if key in body:
                    val = body[key]
                    r.log(f"  {key}: {json.dumps(val, default=str)[:200]}")
    except Exception as e:
        r.warn(f"  morning_run_log read: {e}")

    # Also try to extract metrics from handler response if returned
    try:
        payload = json.loads(payload_str)
        if isinstance(payload, dict):
            body = payload.get("body")
            if isinstance(body, str):
                body = json.loads(body)
            if isinstance(body, dict):
                metrics = body.get("metrics") or body.get("m")
                if metrics:
                    blended = metrics.get("blended_composite")
                    raw = metrics.get("raw_composite")
                    active = metrics.get("calibration_active")
                    n_sig = metrics.get("calibration_n_signals")
                    r.log(f"\n  In handler response:")
                    r.log(f"    blended_composite:      {blended}")
                    r.log(f"    raw_composite:          {raw}")
                    r.log(f"    calibration_active:     {active}")
                    r.log(f"    calibration_n_signals:  {n_sig}")
    except Exception:
        pass

    r.kv(
        zip_size=len(zbytes),
        invoke_s=f"{elapsed:.1f}",
    )
    r.log("Done")
