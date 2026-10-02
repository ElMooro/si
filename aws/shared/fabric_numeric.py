"""Typed, occurrence-preserving calculations for descriptive Fabric research.

Weights and strengths below are legacy heuristics, never calibrated confidence.
No source freshness, independence, forward edge or portfolio authority is earned
by this arithmetic. Every received occurrence survives in the source context.
"""
from copy import deepcopy
from decimal import Decimal
import hashlib
import json
import math
import re

CONTRACT = 'fabric-descriptive-numeric.v1'
TICK = re.compile(r'^[A-Z][A-Z0-9.\-]{0,6}$')


def number(value, low=None, high=None, integer=False):
    if type(value) not in (int, float):
        return None
    try:
        finite = math.isfinite(value)
    except OverflowError:
        return None
    if not finite or low is not None and value < low or high is not None and value > high:
        return None
    if integer and value != int(value):
        return None
    return value


def field(row, *names):
    if not isinstance(row, dict):
        return None
    return next((row[k] for k in names if k in row), None)


def symbol(row):
    if not isinstance(row, dict):
        return None
    names = [row[k] for k in ('ticker', 'symbol') if k in row]
    if not names or any(not isinstance(v, str) for v in names):
        return None
    names = [v.strip().upper() for v in names]
    return names[0] if len(set(names)) == 1 and TICK.fullmatch(names[0]) else None


def collection(packet, names):
    if not isinstance(packet, dict):
        return None, [], 'packet_unavailable'
    key = next((k for k in names if k in packet), None)
    if key is None:
        return None, [], 'collection_absent'
    if not isinstance(packet[key], list):
        return key, [], 'collection_invalid'
    return key, packet[key], 'received'


def stance(engine, row):
    """Presence is authoritative; missing direction/units cannot be invented."""
    if not isinstance(row, dict):
        return None
    if engine == 'trend-reversal':
        score = number(field(row, 'reversal_score'), 0, 100)
        direction = {'TOP_FORMING': 'DOWN', 'BOTTOM_FORMING': 'UP'}.get(field(row, 'direction')) if isinstance(field(row, 'direction'), str) else None
        if score is not None and score >= 15 and direction:
            return 'reversal', '%s %s' % (row['direction'], score), direction, min(1, score / 60)
    elif engine == 'ai-rerating':
        score = number(field(row, 'composite'), 0, 100)
        if score is not None and score >= 55:
            return 'rerating', 'composite %s' % score, 'UP', score / 100
    elif engine == 'magic-formula':
        rank = number(field(row, 'rank', 'magic_rank'), 1, integer=True)
        if rank is not None and rank <= 30:
            return 'value-rank', 'MF #%s' % rank, 'UP', max(.3, 1 - rank / 40)
    elif engine == 'opportunities':
        score = number(field(row, 'go_score', 'score', 'composite'), 0, 100)
        if score is not None and score >= 60:
            return 'opportunity', 'score %s' % score, 'UP', score / 100
    elif engine == 'insider-clusters':
        count = number(field(row, 'insiders', 'n_insiders', 'cluster_size'), 0, integer=True)
        if count is not None and count >= 2:
            return 'insider-cluster', '%s insiders' % count, 'UP', min(1, count / 5)
    elif engine == 'congress-direct':
        kind = field(row, 'type', 'transaction')
        if isinstance(kind, str):
            direction = {'purchase': 'UP', 'buy': 'UP', 'sale': 'DOWN', 'sale (full)': 'DOWN', 'sale (partial)': 'DOWN'}.get(kind.strip().lower())
            if direction:
                return 'congress', kind, direction, .6
    elif engine == 'squeeze-fuel':
        # Days to cover and a score have different dimensions. The original
        # score's scale itself remains unqualified; this is descriptive only.
        score = number(field(row, 'squeeze_score'), 0, 100)
        if score is not None and score >= 6:
            return 'squeeze', 'squeeze_score %s' % score, 'UP', min(1, score / 15)
    return None


def _engine(value, engine):
    return isinstance(value, str) and value.strip().lower() in (engine, 'justhodl-' + engine)


def weight(engine, leaderboard, learned, regime):
    """Exact engine match, deterministic duplicates and no invalid fallback."""
    base = {'value': None, 'status': 'withheld', 'records': [], 'formula': None,
            'forecast_qualified': False, 'basis': 'unqualified_legacy_weight'}
    unavailable = {'learned': learned is None, 'leaderboard': leaderboard is None}
    learned = {} if learned is None else learned
    leaderboard = {} if leaderboard is None else leaderboard
    base['source_unavailable'] = unavailable
    if not isinstance(learned, dict) or not isinstance(leaderboard, dict):
        return {**base, 'reason': 'weight_packet_invalid'}
    scopes = [('by_engine_regime', 8), ('by_engine', 15)]
    for name, minimum in scopes:
        if name not in learned:
            continue
        table = learned[name]
        if not isinstance(table, dict):
            return {**base, 'reason': 'weight_collection_invalid', 'collection': name}
        selected = []
        for key, value in table.items():
            if name == 'by_engine_regime':
                bits = key.split('|') if isinstance(key, str) else []
                match = len(bits) == 2 and isinstance(regime, str) and regime != 'UNKNOWN' and bits[1] == regime and _engine(bits[0], engine)
            else:
                match = _engine(key, engine)
            if match:
                selected.append({'key': key, 'record': deepcopy(value)})
        if not selected:
            continue
        base.update(records=selected, collection=name)
        if len(selected) != 1:
            return {**base, 'reason': 'ambiguous_weight_identity'}
        row = selected[0]['record']
        win = number(field(row, 'win'), 0, 100)
        lift = number(field(row, 'lift'), -50, 50)
        n = number(field(row, 'n'), minimum, integer=True)
        if win is None or lift is None or n is None or abs(Decimal(str(win)) - 50 - Decimal(str(lift))) > Decimal('.11'):
            return {**base, 'reason': 'invalid_or_inconsistent_weight_record'}
        return {**base, 'value': round(max(.2, min(1.6, .6 + lift / 50)), 2),
                'status': 'descriptive_only', 'basis': name,
                'formula': 'clip(0.6 + lift_percentage_points / 50, 0.2, 1.6)'}
    key, rows, status = collection(leaderboard, ('board',))
    if key is not None and status != 'received':
        return {**base, 'reason': 'leaderboard_collection_invalid'}
    selected = [{'pointer': '/board/' + str(i), 'record': deepcopy(row)} for i, row in enumerate(rows)
                if isinstance(row, dict) and _engine(row.get('engine'), engine)]
    if selected:
        base.update(records=selected, collection='board')
        if len(selected) != 1:
            return {**base, 'reason': 'ambiguous_weight_identity'}
        row = selected[0]['record']; win = number(row.get('win_pct'), 0, 100)
        if win is None or number(row.get('n'), 5, integer=True) is None:
            return {**base, 'reason': 'invalid_weight_record'}
        return {**base, 'value': max(.2, min(1.5, .6 + (win - 50) / 50)),
                'status': 'descriptive_only', 'basis': 'board',
                'formula': 'clip(0.6 + (win_pct - 50) / 50, 0.2, 1.5)'}
    return {**base, 'value': .6, 'status': 'declared_heuristic_prior',
            'basis': 'ungraded_prior', 'formula': 'constant 0.6; not measured skill'}


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, allow_nan=False, separators=(',', ':'), ensure_ascii=False).encode('utf-8')).hexdigest()


def family_projection(envelopes):
    """Do not select the first or strongest of ambiguous family occurrences."""
    families = {}
    for e in envelopes:
        families.setdefault(e['source_family'], []).append(e)
    accepted = []; diagnostics = []
    for family, rows in sorted(families.items()):
        # Repeated rows are retained but cannot manufacture agreement. Even
        # identical values can describe separate observations with unknown grain.
        valid = len(rows) == 1 and number(rows[0].get('weight'), 0) is not None
        diagnostics.append({'family': family, 'occurrence_ids': sorted(r['evidence_id'] for r in rows),
                            'status': 'descriptive_only' if valid else 'withheld_ambiguous_or_unweighted',
                            'independence_eligible': False})
        if valid:
            accepted.append(rows[0])
    return accepted, diagnostics


def consumer_context(packet):
    """Fabric has no qualified ranking contract. A claimed flag cannot create it."""
    return {'basis': CONTRACT, 'source': 'data/feature-bus.json', 'packet': deepcopy(packet),
            'ranking_multiplier': 1.0, 'ranking_eligible': False,
            'calls_eligible': False, 'sizing_eligible': False, 'forecast_qualified': False,
            'learning_weight_eligible': False,
            'reason': 'Descriptive adapters and legacy weights have no qualified independent forward edge.'}
