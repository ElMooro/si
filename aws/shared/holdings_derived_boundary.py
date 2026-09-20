"""Explicit exclusion of direct and clustered 13F from selected composites.

The basis identifies a calculation revision, not predictive or trading authority.
It does not certify other upstream families or authenticate arbitrary JSON.
"""
import hashlib
import json
import math

from holdings_authority import context
from capital_research_boundary import current_basis as capital_current_basis

BASIS = 'holdings-direct-and-cluster-excluded.v1'
DIRECT = 'data/13f-positions.json'
CLUSTER = 'data/smart-money-clusters.json'


def exclusions(packets):
    sources = []
    for key, packet in sorted(packets.items()):
        row = context(packet, key)
        row['qualification_scope'] = 'This named input only, including its 13F-derived cluster scores.'
        row['input_json_sha256'] = hashlib.sha256(json.dumps(
            packet, sort_keys=True, separators=(',', ':'), ensure_ascii=False,
            allow_nan=False).encode()).hexdigest()
        sources.append(row)
    return {'basis': BASIS, 'sources': sources,
            'excluded_from': ['score', 'agreement_count', 'weight_denominator', 'universe'],
            'scope': 'Direct 13F and smart-money-cluster paths only. Other indirect paths and forecasting performance remain unqualified.'}


def current_basis(packet):
    """Old stored composite scores cannot pass a calculation-version boundary."""
    boundary = packet.get('holdings_exclusions') if isinstance(packet, dict) else None
    return isinstance(boundary, dict) and boundary.get('basis') == BASIS


def compound_rows(packet):
    if not current_basis(packet):
        return []
    rows = packet.get('compound')
    if not isinstance(rows, list):
        return []
    # Validate the actual scored components too; an envelope flag cannot hide
    # a legacy contribution, count, or agreement multiplier.
    result = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        systems, scores = row.get('systems'), row.get('scores')
        if not isinstance(systems, list) or not isinstance(scores, dict):
            continue
        if not systems or not all(isinstance(s, str) for s in systems):
            continue
        if (len(systems) != len(set(systems)) or set(systems) != set(scores)
                or row.get('n_systems') != len(systems)
                or any(s in ('smart_money', '13f', 'institutional_13f') for s in systems)):
            continue
        if not all(type(v) in (int, float) and math.isfinite(v) for v in scores.values()):
            continue
        expected = round(sum(scores.values()) * (1 + 0.5 * (len(systems) - 1)), 1)
        if not math.isfinite(expected) or row.get('compound_score') != expected:
            continue
        result.append(row)
    return result


def flow_rows(packet):
    if not current_basis(packet) or not capital_current_basis(packet):
        return []
    rows = packet.get('multi_engine_confluence')
    if not isinstance(rows, list):
        return []
    result = []
    for row in rows:
        engines = row.get('engines') if isinstance(row, dict) else None
        if not isinstance(engines, list) or not all(isinstance(v, str) for v in engines):
            continue
        if not _flow_components(row):
            continue
        result.append(row)
    return result


# These names identify the current producer's components; they do not establish
# source independence or predictive validity. Unknown revisions fail closed.
FLOW_COMPONENTS = frozenset(('dark-pool', 'etf-lookthrough', 'short-interest',
    'finra-short', 'stealth', 'options-flow', 'squeeze', 'insider', 'buyback', 'insider-buyback'))
FLOW_POSTURES = frozenset(('SHORT_SQUEEZE_SETUP', 'ACCUMULATION', 'DISTRIBUTION',
    'STEALTH_ACCUMULATION', 'ACCUMULATION_LEAN', 'DISTRIBUTION_LEAN', 'MIXED'))


def _flow_components(row):
    engines = row.get('engines') if isinstance(row, dict) else None
    return (isinstance(engines, list) and bool(engines)
        and all(isinstance(v, str) and v in FLOW_COMPONENTS for v in engines)
        and len(engines) == len(set(engines))
        and type(row.get('n_engines')) is int and row['n_engines'] == len(engines))


def flow_annotations(packet):
    """Inspect the actual annotation components, not just the packet revision."""
    if not current_basis(packet) or not capital_current_basis(packet):
        return {}
    rows = packet.get('ticker_map')
    if not isinstance(rows, dict):
        return {}
    out = {}
    for ticker, row in rows.items():
        if not isinstance(ticker, str) or not ticker or not _flow_components(row):
            continue
        score = row.get('score')
        if type(score) not in (int, float) or not math.isfinite(score):
            continue
        if not isinstance(row.get('posture'), str) or row['posture'] not in FLOW_POSTURES:
            continue
        if any(type(row.get(v)) is not bool for v in ('heavy_short', 'stealth')):
            continue
        if not isinstance(row.get('tags'), list) or not all(isinstance(v, str) for v in row['tags']):
            continue
        out[ticker] = {k: row[k] for k in ('posture', 'score', 'n_engines', 'engines', 'heavy_short', 'stealth', 'tags')}
    return out
