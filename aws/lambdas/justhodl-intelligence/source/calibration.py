"""
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
