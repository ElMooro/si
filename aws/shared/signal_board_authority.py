"""Signal Board inventory and producer claims do not qualify a decision model."""
CURRENT = 'data/signal-board.json'
FLAGS = ('calls_eligible', 'sizing_eligible', 'execution_eligible', 'forecast_qualified')
REASON = 'No independently qualified Signal Board forecasting, allocation or alert policy is registered.'


def context(packet):
    source = packet if isinstance(packet, dict) else {}
    return {'source_key': CURRENT, 'status': 'ABSTAIN',
            'reported_contract': source.get('contract') if isinstance(source.get('contract'), str) else None,
            'reported_generated_at': source.get('generated_at') if isinstance(source.get('generated_at'), str) else None,
            'reported_engine_rows': len(source['engines']) if isinstance(source.get('engines'), list) else None,
            'independent_investment_votes': 0, 'inventory_replay_performed_by_consumer': False,
            'original_provider_replay_performed_by_consumer': False,
            'current_observation_freshness_verified_by_consumer': False,
            'reason': REASON, **dict.fromkeys(FLAGS, False)}


def decision_view(packet):
    evidence = context(packet)
    return {'contract': 'signal-board-decision-view.v1', 'generated_at': evidence['reported_generated_at'],
            'composite_signal': None, 'composite_score': None, 'composite': None, 'score': None,
            'composite_posture': 'WAIT', 'posture': None, 'regime': None, 'deep_read': None,
            'engines': [], 'signals': [], 'per_engine': {}, 'n_live': 0,
            'research_context': evidence, **dict.fromkeys(FLAGS, False)}


def qualified_signals(packet):
    # No reviewed out-of-sample model is registered. Neither producer flags nor
    # successful inventory reconstruction can authorize confirmation or vetoes.
    return []


def guard(key, packet):
    return decision_view(packet) if key == CURRENT else packet
