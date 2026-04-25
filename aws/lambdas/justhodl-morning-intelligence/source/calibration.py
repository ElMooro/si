"""
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
