"""Retain the legacy auction catalog without pretending it is matched evidence.

No acquired original, security identity, comparable quote/feature basis or dated
outcome series accompanies these constants. A complete current vector cannot
repair that absence. This compiler never ranks or qualifies these entries.
"""
from copy import deepcopy
from datetime import date
from auction_scoring import numeric

CONTRACT = 'auction-reference-catalog.v1'
LABELS = ['zero_rate_floor', 'btc_extreme', 'tail_stress',
          'pd_absorption', 'indirect_collapse', 'issuance_anomaly']
PERMISSIONS = {'calls_eligible': False, 'sizing_eligible': False,
               'forecast_eligible': False, 'execution_eligible': False,
               'historical_point_in_time_verified': False}
REASONS = ['original_source_unverified', 'security_identity_unverified',
           'quote_basis_unverified', 'policy_context_unverified',
           'feature_mapping_incomparable', 'issuance_context_missing',
           'outcome_series_unverified', 'independent_validation_unavailable']


def build(references, narratives, current_vector, today):
    """Preserve every occurrence and extra field; never impute reference values."""
    if not isinstance(references, list) or not isinstance(narratives, dict):
        raise ValueError('Complete reference list and narrative mapping required')
    if type(today) is not date:
        raise ValueError('Explicit calculation date required')
    if not isinstance(current_vector, list) or len(current_vector) != len(LABELS):
        raise ValueError('All six current feature positions must be retained')
    current = [numeric(value) for value in current_vector]
    current = [value if value is not None and 0 <= value <= 100 else None for value in current]
    entries = []
    for occurrence, reference in enumerate(references, 1):
        if not isinstance(reference, dict):
            raise ValueError('Malformed reference occurrence; do not publish a partial catalog')
        day = reference.get('date')
        try:
            parsed = date.fromisoformat(day)
            date_status = 'valid_declared_date' if parsed.isoformat() == day and parsed <= today else 'invalid_declared_date'
        except (TypeError, ValueError):
            date_status = 'invalid_declared_date'
        narrative = narratives.get(day, {}) if isinstance(day, str) else {}
        entries.append({
            'entry_id': 'legacy-reference-%03d' % occurrence,
            'occurrence': occurrence, 'date': day, 'date_status': date_status,
            'regime': reference.get('regime'), 'status': 'unverified_legacy_entry',
            'similarity': None, 'rank': None, 'anchor_vec': [None] * len(LABELS),
            'anchor_metrics': {key: reference.get(key) for key in
                               ('btc', 'low_rate', 'aah', 'pd_share_pct', 'indirect_share_pct')},
            'legacy_reference': deepcopy(reference), 'legacy_narrative': deepcopy(narrative),
            'context': None, 'what_happened_next': None, 'duration': None,
            'missing_verification': list(REASONS),
            'source_capture_verified': False, 'instrument_comparability_verified': False,
            'outcome_measurement_verified': False, **PERMISSIONS,
        })
    return {
        'contract': CONTRACT, 'status': 'unverified_legacy_catalog' if entries else 'empty',
        'calculation_as_of': today.isoformat(), 'reference_count': len(entries),
        'current_vector': current, 'vector_labels': list(LABELS),
        'current_vector_status': 'complete_unqualified' if all(value is not None for value in current) else 'incomplete',
        'current_feature_unit': 'score_0_100', 'top_matches': [], 'all_matches': entries,
        'ranking_eligible': False, 'similarity_metric': None,
        'missing_verification': list(REASONS),
        'interpretation': 'Historical similarity is unavailable. These are legacy declarations without verified original security observations, comparable features or dated outcome measurements. Catalog order is source order, never a stress ranking.',
        'legacy_narratives': deepcopy(narratives),
        'source_capture_verified': False, 'instrument_comparability_verified': False,
        'outcome_measurement_verified': False, **PERMISSIONS,
    }
