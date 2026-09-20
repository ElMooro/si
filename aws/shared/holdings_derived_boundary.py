"""Explicit exclusion of direct and clustered 13F from selected composites.

The basis identifies a calculation revision, not predictive or trading authority.
It does not certify other upstream families or authenticate arbitrary JSON.
"""
import hashlib
import json
import math

from holdings_authority import context

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
    if not current_basis(packet):
        return []
    rows = packet.get('multi_engine_confluence')
    if not isinstance(rows, list):
        return []
    result = []
    for row in rows:
        engines = row.get('engines') if isinstance(row, dict) else None
        if not isinstance(engines, list) or not all(isinstance(v, str) for v in engines):
            continue
        if (not engines or len(engines) != len(set(engines)) or len(engines) != row.get('n_engines')
                or any(v in ('13f', 'smart-money') for v in engines)):
            continue
        result.append(row)
    return result
