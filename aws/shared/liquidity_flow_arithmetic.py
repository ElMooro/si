"""Unpublished candidate: original-bound three-leg arithmetic, never a signal.

Not wired to a Lambda or publication path. Requires actual retained-baseline
acceptance and independent arithmetic before native migration is considered.
"""
from copy import deepcopy
from datetime import date, timedelta
from decimal import Decimal, localcontext, ROUND_HALF_EVEN
import report_observations as observations
from research_brief_model import clock

SPECS = {
    'WALCL': ('walcl', 'Millions of U.S. Dollars', 'W', 'Wednesday level', Decimal('0.001'), 21),
    'WTREGEN': ('tga', 'Millions of U.S. Dollars', 'W', 'Weekly average ending Wednesday', Decimal('0.001'), 21),
    'RRPONTSYD': ('rrp', 'Billions of US Dollars', 'D', 'Daily reported operation amount', Decimal('1'), 10),
}


def scalar(value):
    return {'value': float(value) if value is not None else None,
            'exact_decimal': str(value) if value is not None else None, 'unit': 'usd_bn'}


def point(series, target):
    eligible = [row for row in series['history'] if row['observation_date'] <= target.isoformat()]
    selected = eligible[-1] if eligible else None
    if selected is None:
        return {'valuation_date': str(target), 'selected': None, 'carry_days': None,
                'status': 'missing_observation', 'value': scalar(None)}
    lag = (target - date.fromisoformat(selected['observation_date'])).days
    value = observations.decimal(selected['native_value'])
    reason = 'missing_source_value' if value is None else 'invalid_domain' if value < 0 else 'age_ceiling_exceeded' if lag > series['max_observation_age_days'] else 'descriptive'
    normalized = value * Decimal(series['to_usd_bn']) if reason == 'descriptive' else None
    return {'valuation_date': str(target), 'selected': deepcopy(selected), 'carry_days': lag,
            'status': reason, 'value': scalar(normalized)}


def sample(series, target):
    legs = {sid: point(row, target) for sid, row in series.items()}
    values = {sid: observations.decimal(row['value']['exact_decimal']) for sid, row in legs.items()}
    usable = len(values) == 3 and all(value is not None for value in values.values())
    net = values['WALCL'] - values['WTREGEN'] - values['RRPONTSYD'] if usable else None
    return {'valuation_date': str(target), 'legs': legs, 'net': scalar(net),
            'status': 'descriptive_mixed_basis' if usable else 'unavailable',
            'unavailable_legs': [sid for sid, row in legs.items() if row['status'] != 'descriptive']}


def difference(series, end, start):
    current, prior = sample(series, end), sample(series, start)
    legs = {}
    for sid in SPECS:
        a, b = current['legs'][sid], prior['legs'][sid]
        av, bv = observations.decimal(a['value']['exact_decimal']), observations.decimal(b['value']['exact_decimal'])
        change = av - bv if av is not None and bv is not None else None
        contribution = change if sid == 'WALCL' or change is None else -change
        legs[sid] = {'current': a, 'baseline': b, 'reported_level_change': scalar(change),
            'signed_formula_contribution': scalar(contribution),
            'same_observation_carried': bool(a['selected'] and b['selected'] and a['selected']['original_row'] == b['selected']['original_row'])}
    a, b = observations.decimal(current['net']['exact_decimal']), observations.decimal(prior['net']['exact_decimal'])
    change = a - b if a is not None and b is not None else None
    if change is not None:
        assert sum(Decimal(row['signed_formula_contribution']['exact_decimal']) for row in legs.values()) == change
    return {'current_valuation_date': str(end), 'baseline_valuation_date': str(start),
            'calendar_days': (end-start).days, 'legs': legs, 'change': scalar(change),
            'status': 'descriptive_current_vintage' if change is not None else 'unavailable',
            'historical_information_availability_verified': False, 'causal_flow_estimate': False}


def build(source, originals, generated_at):
    now = clock(generated_at); acquired = clock(source['generated_at'])
    if source.get('contract') != observations.CONTRACT or acquired > now:
        raise ValueError('Canonical source and nonfuture acquisition required')
    replay = source.get('replay') or {}
    if replay.get('output_sha256') != observations.digest({k: v for k, v in source.items() if k != 'replay'}):
        raise ValueError('Canonical output content binding differs')
    with localcontext() as arithmetic:
        arithmetic.prec = 34; arithmetic.rounding = ROUND_HALF_EVEN
        series = {}
        for sid, (label, unit, frequency, basis, factor, ceiling) in SPECS.items():
            original = originals.get(sid)
            if original is None: raise ValueError('Missing original series: ' + sid)
            row = source['measurements'][sid]
            with localcontext() as source_arithmetic:
                source_arithmetic.prec = 28; source_arithmetic.rounding = ROUND_HALF_EVEN
                rebuilt = observations.measurement(sid, original['definition'], original['observations'], original['evidence'], source['generated_at'], original['acquired_at'])
            if rebuilt != row: raise ValueError('Original reconstruction differs: ' + sid)
            definition = original['definition']['seriess'][0]
            if (definition['units'], definition['frequency_short'], definition['seasonal_adjustment_short']) != (unit, frequency, 'NSA'):
                raise ValueError('Native unit, frequency or adjustment changed: ' + sid)
            history = [{'observation_date': r['date'], 'native_value': r.get('value'), 'original_row': i,
                        'after_snapshot_date': date.fromisoformat(r['date']) > acquired.date()}
                       for i, r in enumerate(original['observations']['observations'])]
            history.sort(key=lambda r: r['observation_date'])
            series[sid] = {'series_id': sid, 'label': label, 'definition': deepcopy(definition),
                'native_unit': unit, 'measurement_basis': basis, 'frequency': frequency,
                'to_usd_bn': str(factor), 'max_observation_age_days': ceiling,
                'age_policy': 'Inherited canonical age ceiling; not a verified release calendar.',
                'source_last_updated': definition.get('last_updated'), 'acquired_at': original['acquired_at'],
                'coverage': deepcopy(row['coverage']),
                'acquisition_age_hours': (now-clock(original['acquired_at'])).total_seconds()/3600,
                'evidence': deepcopy(original['evidence']), 'history': history,
                'all_original_rows_retained': True, 'source_quality': deepcopy(row['quality'])}
        end = acquired.date()
        latest = sample(series, end)
        source_age_hours = (now - acquired).total_seconds() / 3600
        eligible = source_age_hours <= 26 and all(row['source_quality']['status'] == 'fresh' and 0 <= row['acquisition_age_hours'] <= 26 for row in series.values())
        horizons = {'1d': end-timedelta(days=1), '1w': end-timedelta(days=7),
                    '1m': observations.months_before(end, 1), '3m': observations.months_before(end, 3)}
        comparisons = {label: difference(series, end, start) for label, start in horizons.items()}
        history = [sample(series, end-timedelta(days=n)) for n in range(179, -1, -1)]
        return {'contract': 'liquidity-flow-research-candidate.v1', 'generated_at': generated_at,
            'source_generated_at': source['generated_at'], 'source_replay': deepcopy(replay), 'series': series,
            'formula': 'WALCL - WTREGEN - RRPONTSYD', 'unit': 'usd_bn',
            'measurement_basis': 'Wednesday level minus weekly average minus daily operation amount. A mixed-basis proxy, not investable cash.',
            'last_reconstructed_snapshot': latest,
            'current': latest if eligible and latest['status'] != 'unavailable' else None,
            'comparisons': comparisons, 'calendar_history_180d': history,
            'history_scope': '180 explicit calendar dates reconstructed from current-retrieved vintage; not what was known on each historical date.',
            'quality': {'status': 'within_source_age_ceiling' if eligible and latest['status'] != 'unavailable' else 'source_ineligible',
                'source_age_hours': source_age_hours, 'max_acquisition_age_hours': 26, 'release_calendar_verified': False,
                'original_publication_time_verified': False, 'publication_date': None},
            'dependency_groups': {'fed_h41': ['WALCL', 'WTREGEN'], 'fed_rrp_operations': ['RRPONTSYD']},
            'call': None, 'decision': {'verb': 'WAIT', 'meaning': 'abstain'},
            'calls_eligible': False, 'sizing_eligible': False, 'execution_eligible': False,
            'forecast_qualified': False, 'point_in_time_backtest_qualified': False,
            'portfolio_consequences': {'status': 'UNAVAILABLE', 'target_weights': None, 'forced_liquidation': False},
            'publication_eligible': False, 'candidate_only': True}
