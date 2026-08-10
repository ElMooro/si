"""aws/shared/evidence_weights.py — learned component weights, honestly (wo4580).

Composite engines (accum-composite, stealth, distribution-composite) carry
hand-set component weights. This module lets those weights EARN empirical
adjustment from the platform's own graded ledger without ever fabricating
a track record:

  * reads /justhodl/calibration/accuracy (written by the calibrator from
    justhodl-signals outcomes) for each component's mapped signal_type
  * Wilson 95% lower-bound on hit rate; only components with n_scored >=
    MIN_N move off their prior at all
  * shrinkage: w = prior * (1 + LR * (wilson_lb - 0.5) / 0.5), clamped,
    renormalized — a component must EARN weight above its prior slowly,
    and a decaying one loses it slowly (no single-week whipsaw)
  * result always states its basis: "prior_only" until real n exists —
    the recon of 2026-08-10 found ZERO graded signal_types, so the fleet
    ships prior_only today and the emitted per-component signals build
    the empirical substrate from here forward

Never silently changes tier rules: weight learning adjusts magnitudes,
never lets a tier-4 component satisfy a tier-1 requirement.
"""
import json
import math

import boto3

_ssm = None
_CACHE = {"acc": None, "t": 0.0}
MIN_N = 30          # below this a hit rate is noise, prior holds
LR = 0.6            # max fractional move off prior at wilson_lb in {0,1}
CLAMP = (0.4, 1.8)  # weight multiplier bounds vs prior


def _accuracy():
    global _ssm
    if _CACHE["acc"] is not None:
        return _CACHE["acc"]
    try:
        if _ssm is None:
            _ssm = boto3.client("ssm", region_name="us-east-1")
        v = _ssm.get_parameter(Name="/justhodl/calibration/accuracy")
        acc = json.loads(v["Parameter"]["Value"])
        _CACHE["acc"] = acc if isinstance(acc, dict) else {}
    except Exception:
        _CACHE["acc"] = {}
    return _CACHE["acc"]


def wilson_lb(hits, n, z=1.96):
    """Wilson score interval lower bound — the platform's standard gate."""
    if n <= 0:
        return 0.0
    p = hits / n
    denom = 1 + z * z / n
    centre = p + z * z / (2 * n)
    margin = z * math.sqrt((p * (1 - p) + z * z / (4 * n)) / n)
    return max(0.0, (centre - margin) / denom)


def blend(priors, signal_type_map):
    """priors: {component: weight}. signal_type_map: {component:
    signal_type-in-the-ledger}. Returns (weights, meta) where meta carries
    the audit trail per component: basis, n_scored, wilson_lb, multiplier.
    Weights renormalized to sum(priors)."""
    acc = _accuracy()
    weights, meta = {}, {}
    for comp, prior in priors.items():
        st = signal_type_map.get(comp)
        entry = acc.get(st) if st else None
        n = 0
        hits = 0
        if isinstance(entry, dict):
            hits = int(entry.get("n_correct") or 0)
            n = hits + int(entry.get("n_wrong") or 0)
        if n >= MIN_N:
            lb = wilson_lb(hits, n)
            mult = 1.0 + LR * (lb - 0.5) / 0.5
            mult = max(CLAMP[0], min(CLAMP[1], mult))
            weights[comp] = prior * mult
            meta[comp] = {"basis": "empirical", "signal_type": st,
                          "n_scored": n, "hit_rate": round(hits / n, 3),
                          "wilson_lb": round(lb, 3),
                          "multiplier": round(mult, 3), "prior": prior}
        else:
            weights[comp] = prior
            meta[comp] = {"basis": "prior_only", "signal_type": st,
                          "n_scored": n, "prior": prior,
                          "note": "needs n_scored>=%d graded outcomes to move"
                                  % MIN_N}
    target = sum(priors.values()) or 1.0
    total = sum(weights.values()) or 1.0
    scale = target / total
    weights = {k: round(v * scale, 4) for k, v in weights.items()}
    overall = ("empirical_shrunk"
               if any(m["basis"] == "empirical" for m in meta.values())
               else "prior_only")
    return weights, {"components": meta, "overall_basis": overall,
                     "min_n": MIN_N, "learning_rate": LR,
                     "clamp": list(CLAMP)}
