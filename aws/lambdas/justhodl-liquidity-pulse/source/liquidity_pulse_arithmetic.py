"""Unpublished original-bound Pulse measurements; never a calibrated risk score."""
from copy import deepcopy
from decimal import localcontext, ROUND_HALF_EVEN
import re
import report_observations as observations
from research_brief_model import clock, row_status

SPECS = {
    'WALCL': ('balance', 'Millions of U.S. Dollars', 'W'),
    'WRESBAL': ('balance', 'Millions of U.S. Dollars', 'W'),
    'WTREGEN': ('balance', 'Millions of U.S. Dollars', 'W'),
    'RESPPALGUONNWW': ('balance', 'Millions of U.S. Dollars', 'W'),
    'RESPPNTEPNWW': ('balance', 'Millions of U.S. Dollars', 'W'),
    'OTHL1690': ('facility', 'Millions of U.S. Dollars', 'W'),
    'SWP1690': ('facility', 'Millions of U.S. Dollars', 'W'),
    'BAMLH0A3HYC': ('credit', 'Percent', 'D'),
    'BAMLHE00EHYIOAS': ('credit', 'Percent', 'D'),
    'BAMLEMHBHYCRPIOAS': ('credit', 'Percent', 'D'),
    'HQMCB10YR': ('yield', 'Percent', 'M'),
}
BASIS = {sid: ('Monthly' if sid == 'HQMCB10YR' else 'Daily' if sid == 'BAMLEMHBHYCRPIOAS'
    else 'Daily, Close' if sid.startswith('BAML') else 'Weekly, Ending Wednesday' if sid in ('WRESBAL', 'WTREGEN')
    else 'Weekly, As of Wednesday') for sid in SPECS}


def build(source, originals, generated_at):
    if source.get('contract') != observations.CONTRACT:
        raise ValueError('Canonical original-source measurements required')
    ref = source.get('replay') or {}
    if (not re.fullmatch(r'data/report-research/runs/[a-f0-9]{64}\.json', ref.get('manifest_key', ''))
        or ref.get('output_sha256') != observations.digest({k: v for k, v in source.items() if k != 'replay'})):
        raise ValueError('Canonical source binding differs')
    now = clock(generated_at); age = (now-clock(source['generated_at'])).total_seconds()
    if age < 0: raise ValueError('Future source packet')
    series = {}; errors = {}; fresh = 0
    with localcontext() as context:
        context.prec = 28; context.rounding = ROUND_HALF_EVEN
        for sid, (group, unit, frequency) in SPECS.items():
            row = source.get('measurements', {}).get(sid); original = originals.get(sid)
            if row is None and original is not None:
                raise ValueError('Unbound original: '+sid)
            if row is not None:
                if not original: raise ValueError('Missing original: '+sid)
                rebuilt = observations.measurement(sid, original['definition'], original['observations'],
                    original['evidence'], source['generated_at'], original['acquired_at'])
                if rebuilt != row: raise ValueError('Original reconstruction differs: '+sid)
            state = row_status(row, now, age) if row else 'unavailable'
            if row and (row['unit'] != unit or row['frequency'] != frequency
                        or row['seasonal_adjustment'] != 'Not Seasonally Adjusted'
                        or row['definition'].get('frequency') != BASIS[sid]): state = 'definition_mismatch'
            usable = state == 'fresh'; fresh += usable
            row = row or {}; history = []
            if original:
                for index, record in enumerate(original['observations']['observations']):
                    history.append({'original_row': index, 'observation_date': record['date'],
                        'native_value': record.get('value'), 'realtime_start': record.get('realtime_start'),
                        'realtime_end': record.get('realtime_end')})
            changes = deepcopy(row.get('changes') or {})
            if not usable: errors[sid] = state
            series[sid] = {'series_id': sid, 'group': group, 'label': row.get('name') or sid,
                'unit': row.get('unit'), 'frequency': row.get('frequency'),
                'source_definition': deepcopy(row.get('definition')), 'seasonal_adjustment': row.get('seasonal_adjustment'),
                'latest_value': row.get('current') if usable else None,
                'latest_value_decimal': row.get('current_decimal') if usable else None,
                'last_observed_value': row.get('current_decimal'), 'latest_date': row.get('date'),
                'source_row': row.get('current_row_index'), 'acquired_at': row.get('acquired_at'),
                'published_at': None, 'provider_updated_at': row.get('provider_updated_at'),
                'quality': {'status': state, 'evaluated_at': generated_at, 'release_calendar_verified': False},
                'calendar_comparisons': changes if usable else {},
                'historical_calendar_comparisons': changes, 'coverage': deepcopy(row.get('coverage')),
                'deltas': {old+'_pct': (changes.get(new) or {}).get('pct_change') if usable else None
                    for old, new in (('wow', 'week'), ('mom', 'month'), ('qoq', 'quarter'), ('yoy', 'year'))},
                'history': history, 'history_scope': 'Every row in the retained current-vintage response; not publication-time history.',
                'evidence': deepcopy(row.get('evidence')), 'source_replay': deepcopy(ref),
                'z_score': None, 'signal': None, 'polarity': None,
                'interpretation': 'Dated measurement only; no calibrated stress state, policy response or return forecast.',
                'calls_eligible': False, 'sizing_eligible': False}
    return {'contract': 'liquidity-pulse-candidate.v1', 'schema_version': '2.0', 'candidate_only': True,
        'generated_at': generated_at, 'source_generated_at': source['generated_at'], 'source_replay': deepcopy(ref),
        'series': series, 'n_series': len(SPECS), 'n_series_ok': fresh, 'fetch_errors': errors,
        'quality': {'status': 'fresh' if fresh == len(SPECS) else 'degraded' if fresh else 'unavailable',
            'fresh_series': fresh, 'expected_series': len(SPECS), 'release_calendar_verified': False,
            'basis': 'Original-response reconstruction, explicit definitions and per-source observation/acquisition ceilings.'},
        'composites': {'credit_stress_score': None, 'liquidity_score': None, 'credit_regime': 'UNAVAILABLE',
            'liquidity_regime': 'UNAVAILABLE', 'calibration_status': 'UNVALIDATED'},
        'summary': 'Dated balance-sheet, loan, swap, credit-spread and monthly-yield observations. '
            'Facility balances alone do not establish funding distress; level changes do not identify QE, QT or causal cash flows.',
        'transitions': [], 'call': None, 'decision': {'verb': 'WAIT', 'meaning': 'abstain'},
        'calls_eligible': False, 'sizing_eligible': False, 'execution_eligible': False,
        'forecast_qualified': False, 'publication_eligible': False, 'point_in_time_backtest_qualified': False,
        'independence_note': 'These FRED series overlap other desks. A repeated original is not another independent vote.'}
