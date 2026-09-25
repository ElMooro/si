"""SEC outstanding settlement balances carry lineage, never squeeze votes."""
from copy import deepcopy
from datetime import datetime, timezone
import hashlib, json, re

CURRENT = 'data/squeeze-fuel.json'
FLAGS = ('calls_eligible', 'sizing_eligible', 'execution_eligible', 'forecast_qualified')
NOTE = ('SEC CNS files report aggregate outstanding fail balances by CUSIP and settlement date. '
        'Adjacent changes are balance differences, not new-fail or settlement flows. '
        'Missing labels and observations remain explicit. Short-sale origin, fail age, borrow availability, '
        'forced buying, directional returns and position size are not established by these balances.')


def context(packet):
    p = packet if isinstance(packet, dict) else {}
    ref, available = p.get('replay'), False
    try:
        stamp = datetime.fromisoformat(p['generated_at'].replace('Z', '+00:00'))
        available = (p.get('contract') == 'sec-ftd-original-research.v1'
            and all(p.get(k) is False for k in FLAGS)
            and all(p.get(k) is None for k in ('call', 'signal', 'score', 'fail_age_days', 'short_sale_origin',
                                              'forced_buy_in_probability', 'borrow_fee', 'short_interest', 'short_float_pct'))
            and p.get('by_ticker') == {} and p.get('board') == [] and p.get('top_picks') == []
            and p.get('decision', {}).get('abstain') is True and p.get('decision', {}).get('eligible_votes') == 0
            and p.get('decision', {}).get('verb') == 'WAIT'
            and isinstance(ref, dict) and set(ref) == {'manifest_key', 'output_sha256'}
            and bool(re.fullmatch(r'data/sec-ftd-research/runs/[a-f0-9]{64}\.json', str(ref.get('manifest_key', ''))))
            and bool(re.fullmatch('[a-f0-9]{64}', str(ref.get('output_sha256', ''))))
            and hashlib.sha256(json.dumps({k: v for k, v in p.items() if k != 'replay'}, sort_keys=True,
                separators=(',', ':'), ensure_ascii=False, allow_nan=False).encode()).hexdigest() == ref['output_sha256']
            and stamp.tzinfo is not None and stamp <= datetime.now(timezone.utc))
    except (KeyError, ValueError, TypeError, AttributeError, OverflowError):
        available = False
    return {'contract': 'sec-ftd-context.v1', 'native_reference_available': bool(available),
            'status': 'descriptive_native_reference' if available else 'unqualified_legacy_or_unavailable',
            'canonical': {'key': CURRENT, 'replay': deepcopy(ref)} if available else None,
            'generated_at': p.get('generated_at') if available else None,
            'settlement_date': p.get('settlement_date') if available else None,
            'output_digest_checked': bool(available), 'original_provider_replay_performed_by_consumer': False,
            'current_freshness_verified_by_consumer': False, 'call': None, 'score': None, 'state': None,
            'independent_investment_votes': 0, 'portfolio_action': 'WAIT', 'note': NOTE, **dict.fromkeys(FLAGS, False)}


def decision_view(packet):
    return {'research_context': context(packet), 'call': None, 'score': None, 'signal': None, 'state': None,
            'names': [], 'rows': [], 'stocks': [], 'board': [], 'top_picks': [], 'squeeze_candidates': [],
            'by_ticker': {}, 'items': [], 'data': [], 'tickers': {}, 'top_squeeze': [], 'top_covering': [],
            'top_distribution': [], 'top_crowded': [], 'short_interest': None, 'days_to_cover': None,
            'short_float_pct': None, 'forced_buy_in_probability': None, 'tickets': [],
            'independent_investment_votes': 0, **dict.fromkeys(FLAGS, False)}


def guard(key, packet):
    return decision_view(packet) if key in (CURRENT, 'squeeze-fuel.json') else packet
