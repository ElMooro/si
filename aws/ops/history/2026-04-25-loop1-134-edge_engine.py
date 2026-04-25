#!/usr/bin/env python3
"""
Step 134 — Loop 1 on justhodl-edge-engine (light-touch).

Edge-engine produces composite_score = avg of 5 sub-engine scores
(options_flow, fund_sentiment, earnings, liquidity, correlation).

IMPORTANT: those 5 sub-engines are NOT tracked signal types in our
calibration data. /justhodl/calibration/weights has entries for
khalid_index, edge_regime, crypto_*, cftc_*, etc — none of the
sub-engine score names. Calibration here would weight everything
at 1.0 (uniform = same as today's behavior).

So full Loop 1 integration on edge-engine has zero immediate
functional impact. But there's still value in a LIGHT-TOUCH version:
  1. Install the helper (so future use is one import away)
  2. Compute a parallel `calibrated_composite` field (1.0 weights
     today; ready to diverge if/when we ever start tracking sub-
     engine outcomes)
  3. Expose top-level `calibration` field with helper status
     so consumers can see the helper is wired

This is the same shape as justhodl-intelligence but with all
weights=1.0 from day one. The infrastructure cost is ~5 LOC; the
optionality is preserved for the future.

Contrast with morning-intelligence which had a REAL calibration
issue (wrong weights being applied). Edge-engine is a placeholder
integration — no urgency, just consistency.
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


CALIBRATION_HELPER = '''"""Calibration helper for edge-engine — see aws/shared/calibration.py"""
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
        self.weights = weights; self.accuracy = accuracy; self.is_meaningful = is_meaningful
    def weight(self, signal_type):
        if not self.is_meaningful: return 1.0
        e = self.accuracy.get(signal_type, {})
        n = (e.get("n_correct", 0) + e.get("n_wrong", 0)) if isinstance(e, dict) else 0
        if n < _MIN_N_FOR_MEANINGFUL: return 1.0
        w = self.weights.get(signal_type)
        if w is None or not isinstance(w, (int, float)): return 1.0
        return max(0.1, min(2.0, float(w)))
    def is_signal_calibrated(self, signal_type):
        if not self.is_meaningful: return False
        e = self.accuracy.get(signal_type, {})
        if not isinstance(e, dict): return False
        return (e.get("n_correct", 0) + e.get("n_wrong", 0)) >= _MIN_N_FOR_MEANINGFUL


def _fetch():
    if _ssm is None: return Calibration({}, {}, False)
    weights, accuracy = {}, {}
    try:
        weights = json.loads(_ssm.get_parameter(Name="/justhodl/calibration/weights")["Parameter"]["Value"])
        if not isinstance(weights, dict): weights = {}
    except Exception: weights = {}
    try:
        accuracy = json.loads(_ssm.get_parameter(Name="/justhodl/calibration/accuracy")["Parameter"]["Value"])
        if not isinstance(accuracy, dict): accuracy = {}
    except Exception: accuracy = {}
    is_meaningful = False
    for entry in accuracy.values():
        if isinstance(entry, dict):
            n = entry.get("n_correct", 0) + entry.get("n_wrong", 0)
            if n >= _MIN_N_FOR_MEANINGFUL: is_meaningful = True; break
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
    weighted_sum = 0.0; total_weight = 0.0; raw_sum = 0.0; n_calibrated = 0; n = 0
    for sig_type, score in scores.items():
        try: score_f = float(score)
        except (TypeError, ValueError): continue
        n += 1; raw_sum += score_f
        w = cal.weight(sig_type)
        if cal.is_signal_calibrated(sig_type): n_calibrated += 1
        weighted_sum += score_f * w; total_weight += w
        contributions.append({"signal_type": sig_type, "score": score_f, "weight": w,
                              "contribution": score_f * w,
                              "calibrated": cal.is_signal_calibrated(sig_type)})
    return {
        "value": (weighted_sum / total_weight) if total_weight > 0 else 0.0,
        "raw_value": (raw_sum / n) if n > 0 else 0.0,
        "contributions": contributions,
        "total_weight": total_weight,
        "is_calibrated": n_calibrated > 0,
        "n_calibrated": n_calibrated,
    }
'''


with report("loop1_edge_engine") as r:
    r.heading("Loop 1 on justhodl-edge-engine (light-touch — no current data, future-proofs)")

    # ─── 1. Drop helper into Lambda source ──────────────────────────────
    r.section("1. Drop calibration helper into edge-engine source")
    target = REPO_ROOT / "aws/lambdas/justhodl-edge-engine/source/calibration.py"
    target.write_text(CALIBRATION_HELPER)
    r.ok(f"  Wrote: {target.relative_to(REPO_ROOT)} ({len(CALIBRATION_HELPER):,}B)")

    # ─── 2. Patch lambda_function.py ────────────────────────────────────
    r.section("2. Patch lambda_function.py")
    lf_path = REPO_ROOT / "aws/lambdas/justhodl-edge-engine/source/lambda_function.py"
    src = lf_path.read_text()

    # 2a. Add import — anchor on the existing import line at top
    OLD_IMPORTS = "import json, boto3, urllib.request, time, concurrent.futures\r\nfrom datetime import datetime, timezone\r"
    # The file has \r\n line endings — handle both
    if OLD_IMPORTS in src:
        line_ending = "\r\n"
        old_anchor = OLD_IMPORTS
    else:
        OLD_IMPORTS_UNIX = "import json, boto3, urllib.request, time, concurrent.futures\nfrom datetime import datetime, timezone\n"
        if OLD_IMPORTS_UNIX in src:
            line_ending = "\n"
            old_anchor = OLD_IMPORTS_UNIX
        else:
            r.fail(f"  Imports anchor not found")
            raise SystemExit(1)

    new_anchor = old_anchor + (
        line_ending +
        "# Calibration helper — Loop 1 (light-touch: edge-engine sub-engines" + line_ending +
        "# aren't currently tracked signal types, so weights = 1.0 today;" + line_ending +
        "# infrastructure ready for future calibration)" + line_ending +
        "try:" + line_ending +
        "    from calibration import blend_score, get_calibration" + line_ending +
        "    _CALIBRATION_AVAILABLE = True" + line_ending +
        "except Exception as _e:" + line_ending +
        "    print('WARN: calibration module unavailable: ' + str(_e))" + line_ending +
        "    _CALIBRATION_AVAILABLE = False" + line_ending +
        "    def blend_score(scores, default_weight=1.0):" + line_ending +
        "        if not scores: return {'value': 0.0, 'raw_value': 0.0, 'contributions': []," + line_ending +
        "                                'total_weight': 0.0, 'is_calibrated': False, 'n_calibrated': 0}" + line_ending +
        "        n = len(scores)" + line_ending +
        "        avg = sum(float(v) for v in scores.values() if v is not None) / n if n else 0.0" + line_ending +
        "        return {'value': avg, 'raw_value': avg, 'contributions': []," + line_ending +
        "                'total_weight': float(n), 'is_calibrated': False, 'n_calibrated': 0}" + line_ending +
        "    def get_calibration():" + line_ending +
        "        class _C:" + line_ending +
        "            is_meaningful = False; weights = {}; accuracy = {}" + line_ending +
        "            def weight(self, _): return 1.0" + line_ending +
        "            def is_signal_calibrated(self, _): return False" + line_ending +
        "        return _C()" + line_ending
    )

    if "from calibration import" in src:
        r.log("  Calibration import already present, skipping import patch")
    else:
        src = src.replace(old_anchor, new_anchor)
        r.ok(f"  Added calibration import (line ending: {repr(line_ending)})")

    # 2b. Patch the composite_score block to add calibrated_composite + calibration meta
    OLD_COMPOSITE_BLOCK = """        scores = [e.get('score', 50) for e in [e1, e2, e3, e4, e5]]
        composite = round(sum(scores) / len(scores))
        regime = 'RISK_ON' if composite >= 65 else ('RISK_OFF' if composite <= 35 else 'NEUTRAL')"""

    NEW_COMPOSITE_BLOCK = """        scores = [e.get('score', 50) for e in [e1, e2, e3, e4, e5]]
        composite = round(sum(scores) / len(scores))
        regime = 'RISK_ON' if composite >= 65 else ('RISK_OFF' if composite <= 35 else 'NEUTRAL')

        # ─── Loop 1: calibration-weighted composite ─────────────────────
        # Sub-engine score names aren't currently tracked signal types,
        # so weights are 1.0 today. Will start mattering only if/when
        # we begin scoring outcomes for these specific signals.
        _loop1_blend = blend_score({
            'engine_options_flow': scores[0],
            'engine_fund_sentiment': scores[1],
            'engine_earnings': scores[2],
            'engine_liquidity': scores[3],
            'engine_correlation': scores[4],
        })
        _loop1_meta = {
            'is_meaningful': _loop1_blend['is_calibrated'],
            'n_calibrated': _loop1_blend['n_calibrated'],
            'n_signals': len(scores),
            'contributions': _loop1_blend['contributions'],
        }"""

    # The source has \r\n; need to handle both
    block_to_match = OLD_COMPOSITE_BLOCK.replace("\n", line_ending)
    block_to_insert = NEW_COMPOSITE_BLOCK.replace("\n", line_ending)

    if block_to_match in src:
        src = src.replace(block_to_match, block_to_insert)
        r.ok("  Patched composite computation to include _loop1_blend + _loop1_meta")
    elif "_loop1_blend" in src:
        r.log("  _loop1_blend already in source, skipping")
    else:
        r.fail(f"  Couldn't find composite block")
        raise SystemExit(1)

    # 2c. Add calibrated_composite + calibration meta to output dict
    OLD_OUTPUT_OPENING = """        output = {
            'generated_at': datetime.now(timezone.utc).isoformat(),
            'composite_score': composite,
            'regime': regime,"""
    NEW_OUTPUT_OPENING = """        output = {
            'generated_at': datetime.now(timezone.utc).isoformat(),
            'composite_score': composite,
            'calibrated_composite': round(_loop1_blend['value'], 2),
            'raw_composite': round(_loop1_blend['raw_value'], 2),
            'calibration': _loop1_meta,
            'regime': regime,"""
    block_to_match2 = OLD_OUTPUT_OPENING.replace("\n", line_ending)
    block_to_insert2 = NEW_OUTPUT_OPENING.replace("\n", line_ending)

    if block_to_match2 in src:
        src = src.replace(block_to_match2, block_to_insert2)
        r.ok("  Added calibrated_composite + calibration to output dict")
    elif "'calibrated_composite'" in src:
        r.log("  calibrated_composite already in source, skipping")
    else:
        r.fail(f"  Couldn't find output dict opening")
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
            lines = src.split(line_ending)
            for i in range(max(0, e.lineno - 3), min(len(lines), e.lineno + 3)):
                marker = " >>> " if i == e.lineno - 1 else "     "
                r.log(f"  {marker}L{i+1}: {lines[i][:200]}")
        raise SystemExit(1)

    # ─── 3. Re-deploy ───────────────────────────────────────────────────
    r.section("3. Re-deploy Lambda")
    name = "justhodl-edge-engine"
    src_dir = REPO_ROOT / "aws/lambdas/justhodl-edge-engine/source"
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

    # ─── 4. Sync invoke + verify ────────────────────────────────────────
    r.section("4. Sync invoke + verify edge-data.json")
    time.sleep(3)
    invoke_start = time.time()
    resp = lam.invoke(FunctionName=name, InvocationType="RequestResponse")
    elapsed = time.time() - invoke_start
    if resp.get("FunctionError"):
        payload = resp.get("Payload").read().decode()[:1500]
        r.fail(f"  FunctionError: {payload}")
        raise SystemExit(1)
    r.ok(f"  Invoked in {elapsed:.1f}s")

    # Read edge-data.json
    obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="edge-data.json")
    data = json.loads(obj["Body"].read().decode("utf-8"))

    r.log(f"\n  edge-data.json fields:")
    for k in ["composite_score", "calibrated_composite", "raw_composite", "regime"]:
        marker = " ← NEW" if k in ("calibrated_composite", "raw_composite") else ""
        r.log(f"    {k:25} {data.get(k)}{marker}")

    cal = data.get("calibration", {})
    if cal:
        r.log(f"\n  Top-level 'calibration' field: ← NEW")
        r.log(f"    is_meaningful: {cal.get('is_meaningful')}")
        r.log(f"    n_calibrated:  {cal.get('n_calibrated')}")
        r.log(f"    n_signals:     {cal.get('n_signals')}")

    loop1_active = "calibrated_composite" in data and "calibration" in data
    if loop1_active:
        r.ok(f"\n  ✅ Loop 1 helper installed in justhodl-edge-engine")
        r.log(f"     (calibrated_composite == raw_composite today;")
        r.log(f"      sub-engine signal types not currently scored)")
    else:
        r.fail(f"\n  ✗ Loop 1 fields missing")

    r.kv(
        zip_size=len(zbytes),
        invoke_s=f"{elapsed:.1f}",
        loop1_installed=loop1_active,
        is_meaningful=cal.get("is_meaningful") if cal else False,
        composite=data.get("composite_score"),
        calibrated_composite=data.get("calibrated_composite"),
    )
    r.log("Done")
