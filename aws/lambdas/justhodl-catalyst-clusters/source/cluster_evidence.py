"""Descriptive cluster evidence, with no calibrated ranking or sizing authority.

Reported legacy momentum scores may be inspected but are not measured returns,
independent confirmation, or an estimate of future portfolio consequences.
"""
from collections import Counter
from math import isfinite

CONTRACT = 'catalyst-cluster-abstention.v1'
FLAGS = ('calls_eligible', 'ranking_eligible', 'sizing_eligible', 'execution_eligible')


def reported_score(value):
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    return value if 0 <= value <= 100 and isfinite(value) else None


def momentum_scores(packet):
    """Keep absent, malformed, conflicting and withheld scores unavailable."""
    if not isinstance(packet, dict) or packet.get('status') == 'error':
        return {}
    # A repaired price packet expressly has no composite momentum scores.
    if packet.get('measurement_contract') == 'leader-price-observations.v1':
        return {}
    rows = packet.get('all_scored')
    if rows is None:
        rows = packet.get('leaders')
    if not isinstance(rows, list):
        return {}
    result = {}
    for row in rows:
        if not isinstance(row, dict) or not isinstance(row.get('ticker'), str) or not row['ticker']:
            continue
        ticker, value = row['ticker'], reported_score(row.get('momentum_score'))
        if ticker in result and result[ticker] != value:
            result[ticker] = None
        elif ticker not in result:
            result[ticker] = value
    return result


def describe_cluster(cluster, scores):
    """Retain source order and duplicate occurrences; never manufacture a leader."""
    original = cluster.get('member_records')
    members = original if isinstance(original, list) else []
    observations = []
    tickers = []
    grades = Counter()
    for index, member in enumerate(members):
        row = member if isinstance(member, dict) else {}
        ticker = row.get('ticker')
        valid_ticker = isinstance(ticker, str) and bool(ticker)
        score = reported_score(scores.get(ticker)) if valid_ticker and isinstance(scores, dict) else None
        if valid_ticker:
            tickers.append(ticker)
        grade = row.get('catalyst_grade')
        if isinstance(grade, str):
            grades[grade] += 1
        observations.append({
            'source_member_index': index, 'ticker': ticker if valid_ticker else None,
            'reported_catalyst_grade': grade if isinstance(grade, str) else None,
            'reported_momentum_score': score,
            'momentum_status': 'reported_unqualified' if score is not None else 'unavailable',
            'independent_evidence': False,
        })
    values = [row['reported_momentum_score'] for row in observations]
    unique = len(set(tickers))
    complete = bool(values) and all(value is not None for value in values)
    spread = max(values) - min(values) if complete and unique == len(members) else None
    return {
        'quality': None, 'quality_grade': None, 'avg_grade_score': None,
        'n_a_grade': grades['A'], 'n_d_grade': grades['D'],
        'momentum_spread': spread, 'momentum_spread_unit': 'reported_unqualified_score_points',
        'leader': None, 'ranked_members': [], 'member_observations': observations,
        'evidence_quality': {
            'status': 'unqualified_research', 'member_occurrences': len(members),
            'distinct_reported_tickers': unique,
            'missing_or_invalid_ticker_occurrences': len(members) - len(tickers),
            'duplicate_ticker_occurrences': len(tickers) - unique,
            'reported_score_occurrences': sum(value is not None for value in values),
            'unavailable_score_occurrences': sum(value is None for value in values),
            'cluster_minimum_unique_members_met': unique >= 3,
            'forecast_calibration_verified': False, 'independent_evidence_verified': False,
            'portfolio_consequences_verified': False,
        }, **{flag: False for flag in FLAGS},
    }


def abstain(cluster):
    """A cluster is a research grouping; no portfolio action is established."""
    return {
        'action': 'WAIT', 'call': None, 'scope': cluster.get('scope'),
        'leader': None, 'leader_current': None, 'leader_new_size': None,
        'suggested_entry': None, 'laggard_new_sizes': {}, 'hedge_suggest': None,
        'macro_adverse': None,
        'rationale': 'Research grouping only. Source grades and momentum scores have no '
                     'verified independent predictive or portfolio-sizing evidence. '
                     'WAIT means abstain; it is not a recommendation to hold a position.',
        **{flag: False for flag in FLAGS},
    }
