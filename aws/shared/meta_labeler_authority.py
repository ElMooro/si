"""Fail-closed withdrawal, not a historical model validator.

No supplied status, version, timestamp or row count can establish causality.
Reactivation requires separately reviewed provenance and causal replay code.
"""
CONTRACT = 'meta-labeler-withdrawal.v1'
NOTICE = ('Meta-labeler performance and TAKE/SKIP recommendations are unavailable. '
          'Historical label-end times, feature/label availability and a causal '
          'training boundary are not established. This does not validate or '
          'invalidate the underlying strategy.')
REASONS = (
    'label_end_and_label_availability_missing',
    'feature_availability_and_vintages_unverified',
    'row_split_can_cross_signal_date_and_label_windows',
    'signal_type_selection_uses_test_rows',
    'historical_pending_scores_not_bound_to_available_model',
)


def blocked_packet():
    return {
        'contract': CONTRACT, 'engine': 'meta-labeler', 'status': 'unavailable',
        'qualification_status': 'BLOCKED', 'decision_eligible': False,
        'validated_strategy': False, 'historical_availability_verified': False,
        'notice': NOTICE, 'reason_codes': list(REASONS),
        'n_training_rows': None, 'min_rows_to_activate': None, 'threshold': None,
        'model': {key: None for key in (
            'n_train', 'n_test', 'coefficients', 'test_base_hit',
            'test_take_precision', 'test_take_rate', 'uplift_pp',
            'avg_excess_all_pct', 'avg_excess_taken_pct', 'brier')},
        'per_type_test': [], 'n_pending_gated': None, 'n_take': None, 'gates': [],
        'methodology': NOTICE,
    }


def meta_context(packet):
    """Never forward stale metrics, free text, or a self-asserted qualification."""
    return blocked_packet()
