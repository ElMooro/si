"""Provider flow observations cannot establish investor identity or trading edge.

This boundary applies only to the named provider/derived flow family. It does
not qualify other inputs, infer a neutral vote, or delete the stored evidence.
Native measurements remain available through their reviewed research packet.
"""
import hashlib
import json
import re
from datetime import datetime, timezone

PERMISSIONS = {k: False for k in ('forecast_qualified', 'calls_eligible',
                                'sizing_eligible', 'execution_eligible')}
CURRENT = 'data/provider-fund-flow-research.json'
CONTRACT = 'provider-fund-flow-research.v1'
FILES = frozenset(('daily.json', 'measurements.json', 'composite.json',
    'event-study.json', 'rotation.json', 'per-ticker-context.json',
    'ai-analysis.json', 'constituent-pressure.json', 'stock-exposure-lookup.json'))


def covered(key):
    if not isinstance(key, str): return False
    normalized = key.removeprefix('data/')
    return (normalized in ('provider-fund-flow-research.json', 'capital-flow-radar.json',
                           'flow-lookthrough.json', 'stealth-flow.json')
        or normalized.startswith('etf-flows/') and
            (normalized[len('etf-flows/'):] in FILES or
             bool(re.fullmatch(r'etf-flows/history/\d{4}-\d{2}-\d{2}\.json', normalized))))


def guard(key, packet):
    """No inherited scores, constituent-flow guesses or rows enter a decision.

    An empty mapping is deliberate: legacy flat ticker-map consumers may treat
    any metadata key as an instrument. Evidence metadata has its own function.
    """
    return {} if covered(key) else packet


def context(packet, at=None):
    out = {'available': False, 'reason': 'provider_flow_is_descriptive_research',
           'independent_investment_votes': 0, **PERMISSIONS}
    try:
        if not isinstance(packet, dict) or packet.get('contract') != CONTRACT: return out
        if any(packet.get(k) is not False for k in PERMISSIONS): return out
        if packet.get('call') is not None or packet.get('portfolio_action') != 'WAIT': return out
        def clock(value):
            result = datetime.fromisoformat(value.replace('Z', '+00:00'))
            if result.tzinfo is None: raise ValueError('Aware timestamp required')
            return result
        at = at or datetime.now(timezone.utc)
        if not clock(packet['generated_at']) <= at < clock(packet['source_valid_until']): return out
        ref = packet['replay']; body = {k: v for k, v in packet.items() if k != 'replay'}
        raw = json.dumps(body, sort_keys=True, separators=(',', ':'), ensure_ascii=False, allow_nan=False).encode()
        if not re.fullmatch(r'data/provider-flow-research/runs/[a-f0-9]{64}\.json', ref.get('manifest_key', '')): return out
        if hashlib.sha256(raw).hexdigest() != ref.get('output_sha256'): return out
        if packet.get('quality', {}).get('independent_investment_votes') != 0: return out
        return {**out, 'available': True, 'generated_at': packet['generated_at'],
                'source_valid_until': packet['source_valid_until'], 'replay': ref}
    except (KeyError, ValueError, TypeError, AttributeError, ArithmeticError): return out


def inventory(packet):
    """Research universe only, with no inferred flow or investment score."""
    from provider_flow_catalog import ETF_UNIVERSE
    return {ticker: {'ticker': ticker, **{k: row.get(k) for k in
        ('category', 'subcategory', 'region', 'ref_sector')}, **PERMISSIONS}
        for ticker, row in sorted(ETF_UNIVERSE.items())}


def holdings_inventory(metrics, constituents):
    """Preserve returned holding rows without inventing secondary-market buying.

    This is a compatibility view pending original holdings-response replay.
    It carries no portfolio weight: adding weights from different funds would
    require the user's fund positions and matched holdings dates.
    """
    result = {}
    for fund in metrics:
        ticker = fund['ticker']; source = constituents.get(ticker) or {}
        if source.get('error'): continue
        for row in source.get('top_constituents', []):
            stock = row.get('stock')
            if not stock: continue
            rec = result.setdefault(stock, {'stock': stock, 'name': row.get('name'),
                'n_etfs_holding': 0, 'cumulative_weight_pct': None, 'holding_etfs': [],
                'total_aggregate_flow_daily_usd': None, 'total_aggregate_flow_5d_usd': None,
                'total_aggregate_flow_21d_usd': None, 'flow_pct_mcap_21d': None,
                'flow_zscore_cross_sectional': None, 'quadrant': None,
                'qualification': 'source_holdings_not_yet_replayed', **PERMISSIONS})
            rec['holding_etfs'].append({'etf': ticker, 'weight_pct': row.get('weight_pct'),
                'source_processed_date': source.get('processed_date'), 'etf_zscore': None,
                'etf_label': None, **{field: None for field in ('etf_flow_daily_usd',
                    'etf_flow_5d_usd', 'etf_flow_21d_usd', 'implied_pressure_daily_usd',
                    'implied_pressure_5d_usd', 'implied_pressure_21d_usd')}})
    for rec in result.values():
        rec['holding_etfs'].sort(key=lambda row: row['etf'])
        rec['n_etfs_holding'] = len({row['etf'] for row in rec['holding_etfs']})
    return result
