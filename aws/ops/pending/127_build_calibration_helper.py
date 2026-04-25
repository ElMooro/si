#!/usr/bin/env python3
"""
Step 127 — Loop 1 part A: build the shared calibration helper module.

Strategy decision: instead of a Lambda Layer (which we documented as
'requires more verification' for arm64 in step 120), inline the helper
into each consumer Lambda's source. It's only ~80 LOC, and every
consumer Lambda already has its own source/ folder. Inlining keeps
the per-Lambda source self-contained, no shared deployment dependency,
no risk of breaking 80+ Lambdas if the layer ever has a problem.

This step:
  1. Writes aws/shared/calibration.py — the canonical helper module
  2. Each subsequent step that patches a consumer Lambda will copy
     this file into the Lambda's source/ as calibration.py and import
     locally.

Helper API:
  from calibration import get_calibration, weight, blend_score

  # Get the full snapshot (cached in module-level)
  cal = get_calibration()
  cal.weights      # {signal_type: float}
  cal.accuracy     # {signal_type: {accuracy, n, ...}}
  cal.is_meaningful  # True if calibrator has scored ≥ 30 outcomes

  # Get a single signal's weight (cheap, cached)
  w = weight("khalid_index")  # default 1.0 if not present

  # Blend N signals with their calibrated weights
  blended = blend_score({
      "khalid_index": 65,
      "edge_composite": 72,
      "crypto_fear_greed": 40,
  })
  # Returns: {value: 62.3, contributions: [...], total_weight: 2.1}

CRITICAL DESIGN DECISIONS:
  - Helper falls back to weight=1.0 when calibration is missing or
    not yet meaningful. This means consumer code can be written to
    ALWAYS call weight(), even before the calibrator has data. Day 1
    behavior = uniform weighting (= today's behavior). Day 90 behavior
    = full calibration. No code paths to maintain for 'before vs
    after calibration'.
  - is_meaningful flag uses min_n=30 — calibrator's accuracy is too
    noisy below that. Consumer code can check this if it wants to
    show 'calibrated' badge in UI vs not.
  - Module-level cache: the calibration is fetched once per Lambda
    cold start. SSM ParameterStore is fast but not free at scale.
    Refreshed on SnapStart restore via lazy initialization.
  - No external deps — uses boto3 (already in every Lambda).
"""
import json
import os
from pathlib import Path

from ops_report import report

REPO_ROOT = Path(os.environ.get("GITHUB_WORKSPACE", os.getcwd()))


HELPER_SRC = '''"""
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
        n = acc_entry.get("n", 0) if isinstance(acc_entry, dict) else 0
        n_scored = (acc_entry.get("n_correct", 0) + acc_entry.get("n_wrong", 0)) if isinstance(acc_entry, dict) else 0
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


with report("build_calibration_helper") as r:
    r.heading("Loop 1A — build shared calibration helper module")

    # Save canonical version
    out_dir = REPO_ROOT / "aws/shared"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "calibration.py"
    out_path.write_text(HELPER_SRC)
    r.ok(f"  Wrote canonical: {out_path.relative_to(REPO_ROOT)} ({len(HELPER_SRC):,}B, {HELPER_SRC.count(chr(10))} LOC)")

    # Validate syntax
    import ast
    ast.parse(HELPER_SRC)
    r.ok("  Syntax OK")

    # Self-test the helper logic with synthetic data
    r.section("Self-test with synthetic calibration")

    # Override the SSM client to return controlled data
    test_globals = {}
    exec(HELPER_SRC, test_globals)
    Calibration = test_globals["Calibration"]
    blend_score_fn = test_globals["blend_score"]

    # Manually populate cache with test data
    test_globals["_cache"] = {
        "loaded_at": 9999999999,  # never expire
        "weights": {
            "khalid_index": 1.5,    # historically accurate signal
            "edge_composite": 0.5,   # historically poor signal
            "crypto_fear_greed": 1.0,
        },
        "accuracy": {
            "khalid_index": {"accuracy": 0.72, "n": 100, "n_correct": 50, "n_wrong": 20},
            "edge_composite": {"accuracy": 0.30, "n": 80, "n_correct": 15, "n_wrong": 35},
            "crypto_fear_greed": {"accuracy": 0.55, "n": 50, "n_correct": 28, "n_wrong": 23},
        },
        "is_meaningful": True,
    }

    # Test blend_score
    result = test_globals["blend_score"]({
        "khalid_index": 70,
        "edge_composite": 70,
        "crypto_fear_greed": 70,
    })
    r.log(f"  Test 1: 3 signals all = 70")
    r.log(f"    raw_value (uniform): {result['raw_value']:.2f}  (would be 70 with no calibration)")
    r.log(f"    weighted value: {result['value']:.2f}  (high-trust signal dominates)")
    r.log(f"    n_calibrated: {result['n_calibrated']}/3")
    for c in result["contributions"]:
        r.log(f"      {c['signal_type']:20} score={c['score']:.0f}  w={c['weight']:.2f}  contrib={c['contribution']:.1f}  calibrated={c['calibrated']}")

    # Verify weighted dominates expected: weights [1.5, 0.5, 1.0] sum=3.0
    # value = (70*1.5 + 70*0.5 + 70*1.0) / 3.0 = 70 (all same score)
    assert abs(result["value"] - 70.0) < 0.01, f"expected 70.0 got {result['value']}"

    # Test 2: bullish signal disagreement
    result2 = test_globals["blend_score"]({
        "khalid_index": 80,        # high-trust says bullish
        "edge_composite": 20,       # low-trust says bearish
    })
    r.log(f"  Test 2: high-trust=80, low-trust=20")
    r.log(f"    raw_value: {result2['raw_value']:.2f} (uniform avg = 50)")
    r.log(f"    weighted value: {result2['value']:.2f}  (should lean bullish)")
    # value = (80*1.5 + 20*0.5) / 2.0 = (120 + 10) / 2.0 = 65
    assert result2["value"] > result2["raw_value"], "weighted should > raw when bullish signal is high-trust"
    r.ok(f"    weighted ({result2['value']:.1f}) > raw ({result2['raw_value']:.1f}) ✓ leans toward calibrated signal")

    # Test 3: empty calibration → returns 1.0 weights, raw == weighted
    test_globals["_cache"] = {
        "loaded_at": 9999999999,
        "weights": {},
        "accuracy": {},
        "is_meaningful": False,
    }
    result3 = test_globals["blend_score"]({"sig_a": 60, "sig_b": 80})
    r.log(f"  Test 3: empty calibration → uniform weights")
    r.log(f"    raw: {result3['raw_value']:.1f}, weighted: {result3['value']:.1f}, n_calibrated: {result3['n_calibrated']}")
    assert abs(result3["value"] - result3["raw_value"]) < 0.01, "with empty cal, weighted should equal raw"
    r.ok(f"    weighted == raw (both {result3['value']:.1f}) ✓ safe fallback")

    # Test 4: signal in weights but NOT in accuracy → still falls back to 1.0
    test_globals["_cache"] = {
        "loaded_at": 9999999999,
        "weights": {"unknown_sig": 1.5},  # has weight
        "accuracy": {},  # but no accuracy data
        "is_meaningful": False,
    }
    cal_test = test_globals["get_calibration"]()
    w_test = cal_test.weight("unknown_sig")
    r.log(f"  Test 4: signal with weight but no accuracy → weight = {w_test} (should be 1.0)")
    assert w_test == 1.0, f"expected 1.0, got {w_test}"
    r.ok(f"    falls back to 1.0 when accuracy data missing ✓")

    r.kv(
        helper_loc=HELPER_SRC.count("\n"),
        helper_size_b=len(HELPER_SRC),
        self_tests_passed=4,
    )
    r.log("Done")
