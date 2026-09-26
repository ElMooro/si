"""Complete descriptive Liquidity-agent candidate. No publication or signal IO."""
from copy import deepcopy
from datetime import date, timedelta
from decimal import Decimal, localcontext, ROUND_HALF_EVEN
import math, re
import report_observations as observations
from research_brief_model import clock, row_status
from liquidity_agent_catalog import SPECS, SERIES, CONTEXT_KEYS, DEFINITION_NOTES, METHOD_SOURCES
import liquidity_flow_arithmetic as flow

CONTRACT = 'liquidity-agent-candidate.v1'
PRIVATE = 'audit-private/20260909-originals/liquidity-agent-research/'
AUTHORITY = {key: False for key in ('calls_eligible', 'sizing_eligible', 'execution_eligible',
    'forecast_qualified', 'point_in_time_backtest_qualified', 'publication_eligible')}
USD_FACTORS = {'Millions of U.S. Dollars': '0.001', 'Billions of US Dollars': '1', 'Billions of Dollars': '1'}


def scalar(value):
    displayed = float(value) if value is not None else None
    if displayed is not None and not math.isfinite(displayed):
        raise ValueError('Nonfinite display number')
    return {'value': displayed, 'exact_decimal': str(value) if value is not None else None}


def original_ref(ref):
    return (isinstance(ref, dict) and bool(re.fullmatch('[a-f0-9]{64}', str(ref.get('sha256'))))
        and ref.get('key') == PRIVATE+ref['sha256']+'.bin' and type(ref.get('bytes')) is int
        and 0 < ref['bytes'] <= 64*1024*1024)


def complete_history(original, frequency):
    if original is None:
        return []
    history = []; seen = set()
    for index, row in enumerate(original['observations']['observations']):
        day = date.fromisoformat(row['date']); number = observations.decimal(row.get('value'))
        if str(day) != row['date'] or day in seen:
            raise ValueError('Duplicate or noncanonical observation date')
        seen.add(day)
        if row.get('value') not in (None, '.', '') and number is None:
            raise ValueError('Invalid original observation value')
        if number is not None:
            scalar(number)
        if frequency in ('M', 'Q', 'SA', 'A') and day.day != 1:
            raise ValueError('Unexpected observation period label')
        history.append({'original_row': index, 'observation_date': row['date'], 'native_value': row.get('value'),
            'realtime_start': row.get('realtime_start'), 'realtime_end': row.get('realtime_end')})
    return history


def measure(source, originals, generated_at):
    if source.get('contract') != observations.CONTRACT or source.get('calls_eligible') is not False or source.get('sizing_eligible') is not False:
        raise ValueError('Descriptive canonical source contract required')
    ref = source.get('replay') or {}
    if (not re.fullmatch(r'data/report-research/runs/[a-f0-9]{64}\.json', ref.get('manifest_key', ''))
        or ref.get('output_sha256') != observations.digest({k:v for k,v in source.items() if k != 'replay'})):
        raise ValueError('Exact canonical source identity required')
    if set(originals)-set(SERIES):
        raise ValueError('Unreviewed extra original identity')
    now = clock(generated_at); age = (now-clock(source['generated_at'])).total_seconds()
    if age < 0:
        raise ValueError('Future source publication')
    result = {}
    for sid, spec in SPECS.items():
        m = source.get('measurements', {}).get(sid); original = originals.get(sid)
        if (m is None) != (original is None):
            raise ValueError('Original and canonical availability differ: '+sid)
        if original is not None:
            with localcontext() as canonical_precision:
                canonical_precision.prec = 28; canonical_precision.rounding = ROUND_HALF_EVEN
                rebuilt = observations.measurement(sid, original['definition'], original['observations'],
                    original['evidence'], source['generated_at'], original['acquired_at'])
            if rebuilt != m:
                raise ValueError('Original reconstruction differs: '+sid)
        m = m or {}; definition = m.get('definition') or {}
        actual_definition = (m.get('unit'), m.get('frequency'), definition.get('frequency'), m.get('seasonal_adjustment'))
        reviewed = spec['reviewed_definition'] is not None and actual_definition == spec['reviewed_definition']
        state = row_status(m, now, age) if m else 'unavailable'
        if m and not reviewed:
            state = 'definition_unreviewed' if spec['reviewed_definition'] is None else 'definition_mismatch'
        current = observations.decimal(m.get('current_decimal'))
        positive = spec['group'] in ('dollar', 'equities')
        if reviewed and current is not None and ((positive and current <= 0) or (m.get('unit') in USD_FACTORS and current < 0)):
            state = 'invalid_domain'
        usable = state == 'fresh'; history = complete_history(original, m.get('frequency'))
        changes = deepcopy(m.get('changes') or {})
        factor = USD_FACTORS.get(m.get('unit')) if reviewed else None
        result[sid] = {'series_id': sid, 'group': spec['group'], 'label': m.get('name') or sid,
            'legacy_metadata': {k:v for k,v in spec.items() if k.startswith('legacy_')},
            'source_definition': deepcopy(definition) if definition else None, 'definition_reviewed': reviewed,
            'definition_note': DEFINITION_NOTES.get(sid), 'source_url': 'https://fred.stlouisfed.org/series/'+sid,
            'unit': m.get('unit'), 'frequency': m.get('frequency'), 'seasonal_adjustment': m.get('seasonal_adjustment'),
            'current': scalar(current if usable else None), 'last_observed': scalar(current),
            'latest_date': m.get('date'), 'original_row': m.get('current_row_index'),
            'acquired_at': m.get('acquired_at'), 'provider_updated_at': m.get('provider_updated_at'), 'published_at': None,
            'quality': {'status': 'within_age_ceiling' if usable else state, 'evaluated_at': generated_at,
                'release_calendar_verified': False, 'max_observation_age_days': observations.AGE_LIMITS.get(m.get('frequency')),
                'observation_age_days': (now.date()-date.fromisoformat(m['date'])).days if m.get('date') else None,
                'acquisition_age_seconds': (now-clock(m['acquired_at'])).total_seconds() if m.get('acquired_at') else None},
            'calendar_comparisons': changes if usable else {}, 'historical_calendar_comparisons': changes,
            'usd_billions': {'unit': 'usd_bn', 'native_unit': m.get('unit'), 'multiplier': factor,
                **scalar(current*Decimal(factor) if usable and factor else None)},
            'history': history, 'coverage': deepcopy(m.get('coverage')),
            'history_scope': 'Every retained current-vintage response row, including missing and future records; not original publication history.',
            'evidence': deepcopy(m.get('evidence') or {}), 'source_replay': deepcopy(ref), **AUTHORITY}
    return result


def price_window(row, source_generated_at):
    """Calendar return and distance to numeric window high; no causal sequence."""
    dated = sorted((r for r in row['history'] if r['observation_date'] <= str(clock(source_generated_at).date())),
        key=lambda r:r['observation_date'])
    current = dated[-1] if dated else None
    end = date.fromisoformat(current['observation_date']) if current else None
    target = end-timedelta(days=60) if end else None
    baseline = next((r for r in reversed(dated) if r['observation_date'] <= str(target)), None) if target else None
    if baseline and (target-date.fromisoformat(baseline['observation_date'])).days > 4:
        baseline = None
    selected = [r for r in dated if target and str(target) <= r['observation_date'] <= str(end)]
    numeric = [r for r in selected if observations.decimal(r['native_value']) is not None]
    high = max(numeric, key=lambda r:observations.decimal(r['native_value'])) if numeric else None
    a = observations.decimal((current or {}).get('native_value'))
    b = observations.decimal((baseline or {}).get('native_value'))
    h = observations.decimal((high or {}).get('native_value'))
    # A future definition change stays inspectable but cannot acquire this formula.
    eligible = row['definition_reviewed'] and (a is None or a > 0)
    change = 100*(a/b-1) if eligible and a is not None and b is not None and b > 0 else None
    distance = 100*(a/h-1) if eligible and a is not None and h is not None and h > 0 else None
    historical = {'price_return_pct': scalar(change), 'distance_from_window_high_pct': scalar(distance)}
    return {'series_id': 'SP500', 'calendar_days': 60, 'target_date': str(target) if target else None,
        'current': deepcopy(current), 'baseline': deepcopy(baseline), 'window_high': deepcopy(high),
        'window_original_rows': [r['original_row'] for r in selected], 'numeric_window_rows': len(numeric),
        'historical_comparison': historical,
        'current_comparison': deepcopy(historical) if row['quality']['status'] == 'within_age_ceiling' else None,
        'return_formula': '100 * (current / baseline - 1); baseline on or before 60-calendar-day cutoff, maximum lag 4 days',
        'high_formula': '100 * (current / highest numeric returned close in the closed 60-calendar-day window - 1)',
        'total_return': False, 'calendar_completeness_verified': False, 'maximum_peak_to_trough_drawdown': False,
        'causal_sequence_verified': False, **AUTHORITY}


def build(source, originals, generated_at, contexts, predecessor):
    if set(contexts) != set(CONTEXT_KEYS) or not original_ref(predecessor):
        raise ValueError('Complete predecessor and context inventory required')
    for key, item in contexts.items():
        if (item.get('status') not in ('retained_unqualified_context', 'missing') or item.get('independent_votes') != 0
            or (not original_ref(item.get('original')) if item.get('status') != 'missing' else item.get('original') is not None)):
            raise ValueError('Protected whole context required: '+key)
        if item.get('captured_at') and clock(item['captured_at']) > clock(generated_at):
            raise ValueError('Future context capture')
    with localcontext() as arithmetic:
        arithmetic.prec = 34; arithmetic.rounding = ROUND_HALF_EVEN
        series = measure(source, originals, generated_at)
        triad = ('WALCL', 'WTREGEN', 'RRPONTSYD')
        reconstruction = flow.build(source, originals, generated_at) if all(series[s]['definition_reviewed'] for s in triad) else None
        net = {'reconstruction': reconstruction,
            'current': reconstruction['current'] if reconstruction and all(series[s]['quality']['status'] == 'within_age_ceiling' for s in triad) else None,
            'formula': 'WALCL - WTREGEN - RRPONTSYD', 'unit': 'usd_bn',
            'basis': 'Reviewed Liquidity-flow arithmetic with an additional current observation-age gate.'}
        fresh = sum(row['quality']['status'] == 'within_age_ceiling' for row in series.values())
        groups = {}
        for sid, row in series.items():
            groups.setdefault(row['group'], []).append(sid)
        return {'contract': CONTRACT, 'candidate_only': True, 'engine': 'justhodl-liquidity-agent',
            'generated_at': generated_at, 'source_generated_at': source['generated_at'], 'source_replay': deepcopy(source['replay']),
            'series': series, 'series_groups': groups,
            'quality': {'status': 'within_age_ceiling' if fresh == len(SERIES) else 'degraded' if fresh else 'unavailable',
                'current_series': fresh, 'requested_series': len(SERIES),
                'reconstructed_series': sum(bool(row['history']) for row in series.values()),
                'missing_series': [sid for sid,row in series.items() if not row['history']], 'release_calendar_verified': False},
            'derived': {'net_liquidity': net, 'net_liquidity_unavailable_reason': 'unreviewed_or_missing_original_leg' if reconstruction is None else None,
                'sp500_60_calendar_days': price_window(series['SP500'], source['generated_at'])},
            'dependency_graph': {'series_to_engine': list(SERIES), 'known_overlap': [
                {'members': ['WALCL','WSHOSHO','WSHOTSL','WSHOMCB','WRESBAL','WTREGEN','WORAL'],
                 'basis': 'Overlapping Federal Reserve balance-sheet components and averages; not additive independent liquidity.'},
                {'members': ['DTWEXBGS','DTWEXAFEGS','DTWEXEMEGS','DEXUSEU','DEXJPUS','DEXCHUS','DEXKOUS','DEXBZUS'],
                 'basis': 'USD exposures recur across bilateral rates and weighted indices.'},
                {'members': ['DGS10','DFII10','T10YIE'], 'basis': 'Nominal and real Treasury yields are related to breakeven inflation.'}],
                'complete_component_lineage_verified': False, 'independent_votes': 0},
            'contexts': deepcopy(contexts), 'predecessor': deepcopy(predecessor), 'method_sources': METHOD_SOURCES.copy(),
            'uncalibrated_legacy_metrics': {'composite_score': None, 'regime': None, 'credit_first_stage': None},
            'call': None, 'decision': {'verb': 'WAIT', 'meaning': 'abstain'},
            'portfolio_consequences': {'status': 'UNAVAILABLE', 'target_weights': None, 'reason': 'No out-of-sample portfolio qualification.'},
            **AUTHORITY}
