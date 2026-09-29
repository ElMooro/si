"""Complete-input legacy auction heuristics; no validated forecasting authority.

The existing threshold primitive remains unchanged. This layer fixes its feature
denominator and retains unsupported/incomplete observations in weighted windows.
It neither acquires data nor treats an absent observation as a zero score.
"""
import math

CONTRACT = 'auction-score-inputs.v1'
FEATURES = ('btc_extreme', 'tail_stress', 'pd_absorption',
            'indirect_collapse', 'zero_rate_floor')
BUCKETS = {'bills_lt_90d', 'bills_gte_90d', 'coupons_lt_3y', 'coupons_gt_3y', 'tips'}


def numeric(value):
    if type(value) not in (int, float):
        return None
    try:
        return float(value) if math.isfinite(value) else None
    except (OverflowError, ValueError):
        return None


def score_auction(metrics, scores, fed_rate):
    """No partial-feature renormalization and no deletion of unscorable rows."""
    bucket = metrics.get('tenor_bucket')
    supported = bucket in BUCKETS and metrics.get('instrument_classification_status') == 'verified'
    required = list(FEATURES[:3]) if supported else []
    policy = numeric(fed_rate)
    if supported and bucket in ('coupons_lt_3y', 'coupons_gt_3y', 'tips'):
        required.append('indirect_collapse')
    if supported and bucket == 'bills_lt_90d' and (policy is None or policy > 1):
        required.append('zero_rate_floor')
    values = {}
    for feature in required:
        value = numeric(scores.get(feature))
        if feature == 'zero_rate_floor' and policy is None:
            value = None
        values[feature] = value if value is not None and 0 <= value <= 100 else None
    missing = [key for key, value in values.items() if value is None]
    status = 'unsupported_instrument' if not supported else 'missing_inputs' if missing else 'complete'
    complete = status == 'complete'
    maximum = max(values.values()) if complete else None
    mean = math.fsum(values.values()) / len(required) if complete else None
    baseline = ('bills_lt_90d' if str(bucket).startswith('bills_') else
                'tips' if bucket == 'tips' else 'coupons_gt_3y' if supported else None)
    quality = {
        'contract': CONTRACT, 'status': status, 'tenor_bucket': bucket,
        'required_features': required, 'missing_features': missing,
        'not_applicable_features': [key for key in FEATURES if supported and key not in required],
        'n_required': len(required), 'n_observed': len(required) - len(missing),
        'fed_funds_rate_pct': policy if bucket == 'bills_lt_90d' else None,
        'policy_rule': 'Short-bill floor applies only when the known policy rate exceeds 1%; unknown policy is missing context.',
        'baseline_cohort': baseline, 'baseline_validation': 'unqualified_fixed_legacy_reference',
        'formula': '0.7 * maximum + 0.3 * arithmetic_mean of every required feature; round once to one decimal',
        'maximum': maximum, 'mean': mean,
        'population_rule': 'All supplied auctions are retained. Any positive-weight row without a complete score withholds its weighted window; a measured zero amount has no weight.',
        'calls_eligible': False, 'sizing_eligible': False, 'forecast_eligible': False,
        'execution_eligible': False,
    }
    return {**metrics, 'indicator_scores': values, 'observed_indicator_scores': dict(scores),
            'score_quality': quality, 'max_indicator_score': round(maximum, 1) if complete else None,
            'avg_indicator_score': round(mean, 1) if complete else None,
            'composite_score': round(.7 * maximum + .3 * mean, 1) if complete else None}


def aggregate_indicators(rows):
    """Keep measured zero, missing and inapplicable distinct; count exact thresholds."""
    out = {}
    for key in FEATURES:
        observations = []
        unavailable = 0
        inapplicable = 0
        for row in rows:
            quality = row.get('score_quality') or {}
            required = quality.get('required_features', list((row.get('indicator_scores') or {}).keys()))
            if quality.get('status') == 'unsupported_instrument':
                unavailable += 1
                continue
            if key not in required:
                inapplicable += 1
                continue
            score = numeric((row.get('indicator_scores') or {}).get(key))
            score = score if score is not None and 0 <= score <= 100 else None
            observations.append({'date': row.get('auction_date'), 'cusip': row.get('cusip'),
                                 'term': row.get('security_term'), 'score': score})
            unavailable += score is None
        known = [item['score'] for item in observations if item['score'] is not None]
        status = 'incomplete' if unavailable else 'complete' if known else 'not_applicable' if rows else 'empty'
        out[key] = {'status': status, 'n_fired': sum(value >= 50 for value in known),
                    'n_at_or_above_70': sum(value >= 70 for value in known),
                    'n_observed': len(known), 'n_missing': unavailable,
                    'n_not_applicable': inapplicable, 'max_score': max(known) if known else None,
                    'auctions': [item for item in observations if item['score'] is not None and item['score'] >= 50],
                    'observations': observations, 'count_threshold': 50,
                    'counts_complete': status == 'complete',
                    'calls_eligible': False, 'sizing_eligible': False}
    return out
