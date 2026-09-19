"""Original-bound liquidity observations and calendar research; no allocation model."""
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import re

from inflection_research_catalog import SERIES, ARCHIVES, AUXILIARIES
from report_observations import measurement, liquidity
from research_brief_model import clock, digest, encoded, row_status, SOURCE_CONTRACT
from fred_vintage_model import net_liquidity
from liquidity_calendar import calendar_features

CONTRACT = 'liquidity-inflection-research.v1'
REASON = 'Descriptive measurements do not establish a return forecast, trade direction or account-specific position size.'


def build(inputs, originals, generated_at):
    now = clock(generated_at)
    source = inputs['macro']; ref = source.get('replay') or {}
    if source.get('contract') != SOURCE_CONTRACT or not re.fullmatch(r'data/report-research/runs/[a-f0-9]{64}\.json', ref.get('manifest_key', '')):
        raise ValueError('canonical macro source and replay required')
    if digest({k: v for k, v in source.items() if k != 'replay'}) != ref.get('output_sha256'):
        raise ValueError('macro content binding differs')
    age = (now - clock(source['generated_at'])).total_seconds()
    if age < 0: raise ValueError('future macro source')
    rows = {}; fresh = 0
    for sid, (category, label) in SERIES.items():
        row = deepcopy(source.get('measurements', {}).get(sid) or {})
        if row:
            original = originals.get(sid)
            if not original: raise ValueError('original source missing: ' + sid)
            rebuilt = measurement(sid, original['definition'], original['observations'], original['evidence'],
                                  source['generated_at'], original['acquired_at'])
            if rebuilt != row: raise ValueError('original measurement differs: ' + sid)
        status = row_status(row, now, age) if row else 'unavailable'
        usable = status == 'fresh'; fresh += usable
        row.update(series_id=sid, category=category, requested_label=label, available=usable,
                   quality={'status': status, 'evaluated_at': generated_at},
                   last_observed_value=row.get('current_decimal'), calls_eligible=False, sizing_eligible=False)
        if not usable:
            row.update(current=None, current_decimal=None, historical_changes=row.get('changes', {}), changes={})
        if not row.get('name'): row['error'] = source.get('errors', {}).get(sid, 'source_not_collected')
        rows[sid] = row
    proxy = liquidity(rows)
    proxy['basis'] = 'Mixed observation dates and bases: WALCL Wednesday stock, WTREGEN weekly average, RRP daily operation amount; not investable cash.'
    archive = inputs.get('archives') or {}
    history = net_liquidity(archive, now)
    # Include missing scheduled Fridays so a missing endpoint cannot silently
    # reuse last week's slope. No repeated daily rows inflate the sample count.
    weekly = {}
    if history['status'] == 'ARCHIVE_RESEARCH':
        start = max(clock(d['coverage']['archive_start'] + 'T00:00:00Z') for d in archive.values()) + timedelta(days=1)
        end = min(min(clock(d['coverage']['archive_end'] + 'T12:00:00Z') for d in archive.values()) + timedelta(days=1), now)
        cursor = start + timedelta(days=(4-start.weekday()) % 7, hours=12)
        while cursor <= end:
            day = cursor.date().isoformat(); weekly[day] = history['series_decimal'].get(day)
            cursor += timedelta(weeks=1)
    features = calendar_features(weekly)
    features['components'] = {day: history['components'].get(day) for day in weekly}
    features['levels_usd_mn_decimal'] = weekly
    for end, trend in features['history'].items():
        start = trend['start']; before = features['components'].get(start); after = features['components'].get(end)
        attribution = {'status': 'missing_endpoint', 'start': start, 'end': end, 'components': {},
            'net_change_usd_mn_decimal': None,
            'scope': 'Signed arithmetic contributions to the 13-week endpoint level change; descriptive accounting, no causal investment attribution.'}
        if before and after:
            total = Decimal(0)
            for sid in ARCHIVES:
                prior = Decimal(before[sid]['value_usd_mn_decimal']); current = Decimal(after[sid]['value_usd_mn_decimal'])
                sign = 1 if sid == 'WALCL' else -1; contribution = sign*(current-prior); total += contribution
                attribution['components'][sid] = {'baseline_usd_mn_decimal': str(prior), 'current_usd_mn_decimal': str(current),
                    'formula_sign': sign, 'signed_change_usd_mn_decimal': str(contribution),
                    'baseline_observation_date': before[sid]['observation_date'], 'current_observation_date': after[sid]['observation_date']}
            if total != Decimal(weekly[end])-Decimal(weekly[start]): raise ValueError('signed liquidity component changes do not reconcile')
            attribution.update(status='descriptive', net_change_usd_mn_decimal=str(total))
        trend['endpoint_change_decomposition'] = attribution
    features['archive_references'] = {sid: {'replay': doc['replay'], 'collection_id': doc['collection_id'],
        'coverage': doc['coverage'], 'generated_at': doc['generated_at']} for sid, doc in archive.items()}
    features['archive_collection'] = inputs.get('archive_collection')
    features['archive_collection_key'] = ('data/vintage-research/collections/' + digest(inputs['archive_collection']) + '.json') if inputs.get('archive_collection') else None
    features['status'] = history['status']; features['excluded_days'] = history['excluded_days']
    features['availability_rule'] = history['availability_rule']
    # A same-date rate comparison is a descriptive spread, not a stress score.
    sofr, iorb = rows['SOFR'], rows['IORB']
    funding = {'status': 'unavailable', 'spread_bps': None, 'observation_date': None,
               'formula': '100 * (SOFR - IORB)', 'series': ['SOFR', 'IORB'],
               'calls_eligible': False, 'sizing_eligible': False}
    if all(r.get('available') and r.get('unit') == 'Percent' for r in (sofr, iorb)) and sofr['date'] == iorb['date']:
        funding.update(status='descriptive', spread_bps=float(100*(Decimal(sofr['current_decimal'])-Decimal(iorb['current_decimal']))),
                       observation_date=sofr['date'])
    context = {}
    for name in AUXILIARIES:
        packet = (inputs.get('auxiliary') or {}).get(name)
        context[name] = {'status': 'unqualified_context' if packet else 'unavailable',
                         'generated_at': packet.get('generated_at') if isinstance(packet, dict) else None,
                         'input_sha256': digest(packet) if packet is not None else None,
                         'calls_eligible': False, 'sizing_eligible': False,
                         'reason': 'Full retained packet; original-source and investment-model qualification remains separate.'}
        if name in inputs.get('auxiliary_errors', {}):
            context[name].update(inputs['auxiliary_errors'][name])
    withheld = {'status': 'UNQUALIFIED', 'reason': REASON, 'publication_eligible': False}
    output = {'contract': CONTRACT, 'engine': 'liquidity-inflection', 'version': '3.0.0',
        'generated_at': generated_at, 'source_generated_at': source['generated_at'], 'source_replay': ref,
        'series': rows, 'net_liquidity': proxy, 'calendar_research': features, 'funding_spread': funding,
        'retained_context': context, 'quality': {'status': 'fresh' if fresh == len(SERIES) else 'degraded' if fresh else 'unavailable',
            'fresh_series': fresh, 'expected_series': len(SERIES), 'archive_status': history['status']},
        'decision': {'verb': 'WAIT', 'meaning': 'abstain', 'reason': REASON},
        'call': None, 'calls_eligible': False, 'sizing_eligible': False, 'execution_eligible': False,
        'publication_eligible': False, 'signals_logged': 0, 'signal_logging': withheld,
        'portfolio_consequences': {'status': 'UNAVAILABLE', 'target_weights': [], 'reason': REASON},
        'historical_validation': {'status': history['status'], 'point_in_time': False,
            'historical_feature_replay_ready': False, 'publication_eligible': False,
            'reason': 'Provider archive reconstruction under an explicit date-delay policy; no historical system-possession or executable strategy claim.'},
        'composite': {'liquidity_score': None, 'composite_z': None, 'regime': 'UNQUALIFIED', 'components': [], 'read': REASON},
        'trajectory': {'heading': 'UNQUALIFIED', 'read': REASON, 'drivers': [], 'vote': None, 'n_signals': 0},
        'usd': {'as_of': None, 'net_liq_usd_bn': proxy['net']/1000 if proxy['net'] is not None else None,
            'impulse_z': None, 'state': 'RESEARCH_ONLY', 'last_flip': None, 'n_flips_10y': None, 'impulse_tail_180d': [],
            'history_basis': 'See calendar_research for explicitly dated 13-week slope and separate acceleration; no trading-state alias.'},
        'us_money': {'z': None, 'real_m2_yoy_pct': None, 'reason': 'Native M2 and CPI are retained separately; no unqualified composite vote.'},
        'methodology': 'WALCL - WTREGEN - RRPONTSYD, normalized once to USD millions using official definitions. '
            'WALCL is a Wednesday stock, WTREGEN a weekly average, and RRP a daily operation amount; this is a mixed-basis proxy. '
            'Friday-noon archive samples use the completed prior archive day. OLS trend spans exactly 13 calendar weeks and requires all 14 samples. '
            'Acceleration is the change in that slope over 13 weeks, per week squared. Three-year z is descriptive only.',
        'validation_status': 'DESCRIPTIVE_RESEARCH_ONLY'}
    for key in ('projection', 'composite_projection', 'reserve_runway', 'forward_expectation', 'forward_expectation_composite',
                'regime_returns_composite', 'analogs', 'backtest', 'cycle_clock', 'composite_clock', 'brain_predictors'):
        output[key] = deepcopy(withheld)
    for key in ('event_study_after_flips', 'lead_estimates', 'regime_returns', 'lead_curves', 'flip_log'):
        output[key] = {}
    # Keep legacy field names discoverable without retaining their unqualified
    # derived values as active metrics. Full original bytes remain in legacy_context.
    for key in ('onshore_funding', 'treasury_auctions', 'stablecoin_full', 'tensions', 'reserves', 'rrp', 'tga',
                'funding_stress', 'global_liquidity', 'dollar', 'dollar_shortage', 'settlement_fails', 'swap_lines',
                'flow_divergence', 'leverage_stress', 'dealer_survey', 'credit', 'systemic_stress',
                'financial_conditions', 'china_engine', 'eur', 'china', 'stablecoin', 'stablecoin_schema_hint',
                'composite_snapshots'):
        output[key] = None
    output['availability'] = {sid: row['available'] for sid, row in rows.items()}
    output['data_health'] = [{'feed': sid, 'status': row['quality']['status'], 'as_of': row.get('date')} for sid, row in rows.items()]
    return output
