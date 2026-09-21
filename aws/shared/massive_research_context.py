"""Descriptive references cannot restore the retired Massive ranking authority."""
from copy import deepcopy
from datetime import datetime, timezone
import re

CURRENT = 'data/massive-research.json'
LEGACY = 'data/massive-signals.json'
FLAGS = ('forecast_qualified', 'calls_eligible', 'sizing_eligible', 'execution_eligible')
NOTE = ('Recorded option, ETF, currency and dated futures publications supply source research. Shared parents and '
    'different vintages are not independent confirmations. No pre-pump rank, dealer position, '
    'squeeze boost, sector-flow z-score or investment vote is qualified by this composite.')


def reference(value):
    return (isinstance(value, dict) and set(value) == {'manifest_key', 'output_sha256'}
        and isinstance(value.get('manifest_key'), str)
        and re.fullmatch(r'data/massive-research/runs/[a-f0-9]{64}\.json', value['manifest_key'])
        and isinstance(value.get('output_sha256'), str) and re.fullmatch('[a-f0-9]{64}', value['output_sha256']))


def context(packet):
    packet = packet if isinstance(packet, dict) else {}
    permitted = all(packet.get(k) is False for k in FLAGS)
    native = permitted and packet.get('contract') in ('massive-composite-research.v1', 'massive-composite-research.v2') and reference(packet.get('replay'))
    canonical = packet.get('canonical') if isinstance(packet.get('canonical'), dict) else {}
    alias = (permitted and packet.get('contract') in ('massive-research-compatibility.v1', 'massive-research-compatibility.v2')
        and canonical.get('key') == CURRENT and reference(canonical.get('replay'))
        and packet.get('tickers') == {} and packet.get('top_prepump') == [] and packet.get('market') == {})
    stamp = packet.get('generated_at')
    try:
        parsed = datetime.fromisoformat(stamp.replace('Z', '+00:00'))
        clock_ok = parsed.tzinfo is not None and parsed <= datetime.now(timezone.utc)
    except (TypeError, AttributeError, ValueError): clock_ok = False
    available = bool((native or alias) and clock_ok)
    replay = packet.get('replay') if native else canonical.get('replay')
    return {'contract': 'massive-research-context.v1',
        'status': 'descriptive_native_reference' if available else 'unqualified_legacy_or_unavailable',
        'native_reference_available': available,
        'canonical': {'key': CURRENT, 'replay': deepcopy(replay)} if available else None,
        'generated_at': stamp if available else None,
        'reference_hashes_independently_checked_by_consumer': False,
        'tickers': {}, 'top_prepump': [], 'market': {}, 'sources': {},
        'call': None, 'score': None, 'portfolio_action': 'WAIT', 'independent_investment_votes': 0,
        **{k: False for k in FLAGS}, 'evidence_note': NOTE}


def guard(key, packet): return context(packet) if key in (CURRENT, LEGACY) else packet
