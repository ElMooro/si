"""justhodl-cascade-recalibrator

Closes the self-improvement loop end-to-end.

Reads:
  data/theme-cascade.json (current cascade output with base rankings)
  data/cascade-calibration.json (learned feature weights from outcomes)

Computes:
  For each candidate, a calibration_adjustment = weighted product of
  feature values × learned weights. Features that historically predicted
  pumps (positive lift) boost the score; predictive losers reduce it.

Writes:
  data/theme-cascade-calibrated.json (re-ranked alert_tier + laggards)
  data/cascade-recalibration-audit.json (rank changes, correlation,
                                          confidence, methodology)

Downstream consumers (trade-tickets, prepump-router, dashboards) can
opt into either:
  - Original cascade ranking (data/theme-cascade.json)
  - Calibrated ranking (data/theme-cascade-calibrated.json)

CALIBRATION CONFIDENCE:
  n_predictions < 20  → CONFIDENCE=LOW, blend 90% original + 10% calibrated
  n_predictions 20-100 → CONFIDENCE=MEDIUM, blend 60% original + 40% calibrated
  n_predictions ≥ 100  → CONFIDENCE=HIGH, blend 30% original + 70% calibrated

Schedule: daily 9:05 ET (after self-improvement at 8:30 ET refreshes weights,
and before page-ai-commentary at 10:00 ET).
"""
import json
import time
import math
from datetime import datetime, timezone
from typing import Optional, List, Dict

import boto3

S3_BUCKET = "justhodl-dashboard-live"
s3 = boto3.client("s3", region_name="us-east-1")


def _read_json(key: str) -> Optional[dict]:
    try:
        return json.loads(s3.get_object(Bucket=S3_BUCKET, Key=key)["Body"].read())
    except Exception:
        return None


def normalize_feature(feat_name: str, value, all_values: List[float]) -> float:
    """Normalize a feature value to [0, 1] within the cohort.
    
    Returns 0.5 if no normalization possible (neutral).
    """
    if value is None or not isinstance(value, (int, float)):
        return 0.5
    if not all_values or len(all_values) < 3:
        return 0.5
    valid = [v for v in all_values if isinstance(v, (int, float))]
    if not valid:
        return 0.5
    lo, hi = min(valid), max(valid)
    if hi <= lo:
        return 0.5
    return max(0.0, min(1.0, (value - lo) / (hi - lo)))


def compute_calibration_adjustment(candidate: dict, weights: dict,
                                    cohort_features: dict) -> dict:
    """For one candidate, compute multiplicative adjustment based on weights.
    
    Returns dict with adjustment factor + feature contributions.
    """
    if not weights:
        return {"adjustment": 1.0, "contributions": [], "n_features_applied": 0}

    contributions = []
    log_adjustment = 0.0

    # For each weighted feature, look it up in the candidate
    for feat_name, weight in weights.items():
        val = candidate.get(feat_name)
        if val is None:
            # Try alternate key locations
            if feat_name == "theme_acceleration":
                val = candidate.get("max_rs_acceleration")
            elif feat_name == "ticket_atr_pct":
                val = candidate.get("atr_pct")
            elif feat_name == "ticket_rr_tp3":
                val = candidate.get("rr_tp3")

        if val is None or not isinstance(val, (int, float)):
            continue

        all_vals = cohort_features.get(feat_name) or []
        norm = normalize_feature(feat_name, val, all_vals)

        # weight is 0.3 to 2.5. norm is 0 to 1.
        # Contribution: (norm × (weight - 1)) means top-quartile gets full weight effect
        # Average contribution = 0 (neutral); top features get +log(weight); bottom get -log(weight)
        contribution = (norm - 0.5) * 2 * math.log(weight) if weight > 0 else 0
        log_adjustment += contribution

        contributions.append({
            "feature": feat_name,
            "value": val,
            "normalized": round(norm, 3),
            "weight": weight,
            "log_contribution": round(contribution, 4),
        })

    # Apply: adjustment = exp(log_adjustment / n_features) for stability
    n_applied = len(contributions)
    if n_applied == 0:
        return {"adjustment": 1.0, "contributions": [], "n_features_applied": 0}

    # Geometric mean of weights weighted by feature values
    adjustment = math.exp(log_adjustment / max(1, n_applied))
    adjustment = max(0.3, min(2.5, adjustment))  # Clamp

    return {
        "adjustment": round(adjustment, 3),
        "contributions": sorted(contributions, key=lambda x: -abs(x["log_contribution"]))[:5],
        "n_features_applied": n_applied,
    }


def determine_blend_weights(n_predictions: int) -> dict:
    """Calibration confidence based on data points accumulated."""
    if n_predictions >= 100:
        return {"original": 0.30, "calibrated": 0.70, "confidence": "HIGH",
                "rationale": "100+ scored predictions — calibration weights are reliable"}
    elif n_predictions >= 20:
        return {"original": 0.60, "calibrated": 0.40, "confidence": "MEDIUM",
                "rationale": "20-99 scored predictions — moderate confidence"}
    elif n_predictions >= 5:
        return {"original": 0.90, "calibrated": 0.10, "confidence": "LOW",
                "rationale": "<20 scored predictions — calibration is preliminary"}
    else:
        return {"original": 1.00, "calibrated": 0.00, "confidence": "NONE",
                "rationale": "<5 scored predictions — using base scoring only"}


def gather_cohort_features(candidates: List[dict], feature_names: List[str]) -> dict:
    """Aggregate feature values across all candidates for normalization."""
    cohort = {feat: [] for feat in feature_names}
    for c in candidates:
        for feat in feature_names:
            val = c.get(feat)
            if val is None:
                if feat == "theme_acceleration":
                    val = c.get("max_rs_acceleration")
                elif feat == "ticket_atr_pct":
                    val = c.get("atr_pct")
                elif feat == "ticket_rr_tp3":
                    val = c.get("rr_tp3")
            if val is not None and isinstance(val, (int, float)):
                cohort[feat].append(val)
    return cohort


def recalibrate_candidates(candidates: List[dict], weights: dict,
                            blend: dict) -> List[dict]:
    """For each candidate, compute calibrated_score and re-rank."""
    if not candidates:
        return []

    cohort_features = gather_cohort_features(candidates, list(weights.keys()))

    enriched = []
    for c in candidates:
        adj_info = compute_calibration_adjustment(c, weights, cohort_features)
        original_score = c.get("combined_score") or 0
        calibrated_score = original_score * adj_info["adjustment"]
        # Blend
        final_score = (blend["original"] * original_score +
                        blend["calibrated"] * calibrated_score)

        c_enriched = dict(c)
        c_enriched["original_combined_score"] = round(original_score, 2)
        c_enriched["calibrated_combined_score"] = round(calibrated_score, 2)
        c_enriched["combined_score"] = round(final_score, 2)  # used for re-ranking
        c_enriched["calibration_adjustment"] = adj_info["adjustment"]
        c_enriched["calibration_contributions"] = adj_info["contributions"]
        c_enriched["calibration_blend"] = blend
        enriched.append(c_enriched)

    # Re-sort by NEW combined_score
    enriched.sort(key=lambda x: -(x.get("combined_score") or 0))
    return enriched


def compute_rank_changes(original: List[dict], calibrated: List[dict]) -> dict:
    """Compute Kendall-tau-like rank correlation + max rank change."""
    orig_ranks = {c.get("ticker"): i for i, c in enumerate(original)}
    cal_ranks = {c.get("ticker"): i for i, c in enumerate(calibrated)}

    rank_changes = []
    for ticker, orig_rank in orig_ranks.items():
        new_rank = cal_ranks.get(ticker)
        if new_rank is not None:
            rank_changes.append({
                "ticker": ticker,
                "original_rank": orig_rank + 1,
                "calibrated_rank": new_rank + 1,
                "delta": orig_rank - new_rank,  # positive = moved up
            })

    if not rank_changes:
        return {"n": 0}

    rank_changes.sort(key=lambda x: -abs(x.get("delta", 0)))
    deltas = [abs(rc["delta"]) for rc in rank_changes]

    # Top-10 retention: how many of original top-10 are still in calibrated top-10?
    orig_top10 = set(c.get("ticker") for c in original[:10])
    cal_top10 = set(c.get("ticker") for c in calibrated[:10])
    retention = len(orig_top10 & cal_top10) / max(1, len(orig_top10)) * 100

    return {
        "n_compared": len(rank_changes),
        "top_10_retention_pct": round(retention, 1),
        "avg_rank_delta": round(sum(deltas) / len(deltas), 2),
        "max_rank_delta": max(deltas) if deltas else 0,
        "biggest_movers": rank_changes[:5],
    }


def lambda_handler(event, context):
    t0 = time.time()
    print(f"[recalibrator] starting at {datetime.now(timezone.utc).isoformat()}")

    cascade = _read_json("data/theme-cascade.json") or {}
    calibration = _read_json("data/cascade-calibration.json") or {}

    weights = calibration.get("current_weights") or {}
    n_predictions = (calibration.get("feature_attribution") or {}).get("n_predictions_analyzed", 0)

    blend = determine_blend_weights(n_predictions)
    print(f"[recalibrator] n_predictions={n_predictions} confidence={blend['confidence']} "
          f"blend={blend['original']:.0%} orig / {blend['calibrated']:.0%} calibrated")
    print(f"[recalibrator] {len(weights)} learned weights available")

    # Recalibrate each tier
    output = {
        "schema_version": "1.0",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "calibration_n_predictions": n_predictions,
        "blend": blend,
        "weights_applied": weights,
    }

    rank_audit = {}
    for tier_key in ["alert_tier", "medium_tier", "laggards_hot_themes", "watch_tier"]:
        original_candidates = cascade.get(tier_key) or []
        if not original_candidates:
            output[tier_key] = []
            continue

        calibrated_candidates = recalibrate_candidates(original_candidates, weights, blend)
        output[tier_key] = calibrated_candidates

        # Audit rank changes (only meaningful for alert_tier + laggards)
        if tier_key in ["alert_tier", "laggards_hot_themes"]:
            rank_audit[tier_key] = compute_rank_changes(original_candidates, calibrated_candidates)
            print(f"[recalibrator] {tier_key}: retention {rank_audit[tier_key].get('top_10_retention_pct')}%, "
                  f"avg_delta {rank_audit[tier_key].get('avg_rank_delta')}")

    # Copy metadata from original cascade
    for k in ["hot_themes", "themes_top_30", "industries_top_30",
              "industry_distribution", "etf_intelligence", "metadata"]:
        if k in cascade:
            output[k] = cascade[k]

    # Audit doc
    audit = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "blend": blend,
        "calibration_n_predictions": n_predictions,
        "n_weights": len(weights),
        "top_weights": sorted(weights.items(), key=lambda x: -abs(x[1] - 1.0))[:10],
        "rank_changes": rank_audit,
        "methodology": (
            "For each candidate, calibration_adjustment = exp(Σ(normalized_feature × log(weight)) / n_features) "
            "clamped to [0.3, 2.5]. Final combined_score = blend.original × original_score + "
            "blend.calibrated × (original_score × adjustment). Re-ranked by final_score."
        ),
    }

    # Write calibrated cascade
    s3.put_object(
        Bucket=S3_BUCKET, Key="data/theme-cascade-calibrated.json",
        Body=json.dumps(output, default=str).encode(),
        ContentType="application/json", CacheControl="public, max-age=600",
    )

    # Write audit
    s3.put_object(
        Bucket=S3_BUCKET, Key="data/cascade-recalibration-audit.json",
        Body=json.dumps(audit, default=str).encode(),
        ContentType="application/json", CacheControl="public, max-age=600",
    )

    # Dated history
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    s3.put_object(
        Bucket=S3_BUCKET, Key=f"data/cascade-recalibration-history/{today}.json",
        Body=json.dumps(audit, default=str).encode(),
        ContentType="application/json", CacheControl="public, max-age=86400",
    )

    elapsed = round(time.time() - t0, 1)
    n_total = (len(output.get("alert_tier", [])) + len(output.get("medium_tier", [])) +
                len(output.get("laggards_hot_themes", [])) + len(output.get("watch_tier", [])))
    print(f"[recalibrator] DONE — {n_total} candidates recalibrated in {elapsed}s")

    return {
        "statusCode": 200,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps({
            "ok": True, "elapsed_s": elapsed,
            "n_total_candidates": n_total,
            "n_weights_applied": len(weights),
            "blend": blend,
            "rank_audit": rank_audit,
            "calibration_confidence": blend["confidence"],
        }),
    }
