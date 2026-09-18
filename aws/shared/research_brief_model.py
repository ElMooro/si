"""Deterministic, observation-only intelligence from the canonical FRED packet.

No KI/risk/portfolio authority is inferred from a level, missing field or legacy
calibration. The as-of clock is an explicit replay input, not wall clock state.
"""
from copy import deepcopy
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
import hashlib
import json

CONTRACT = 'research-intelligence.v1'
SOURCE_CONTRACT = 'report-observations.v1'
MAX_PACKET_AGE_SECONDS = 26 * 3600
AGE_LIMITS = {'D': 10, 'W': 21, 'BW': 35, 'M': 100, 'Q': 200, 'SA': 370, 'A': 550}
CORE = ('ICSA', 'UNRATE', 'CPIAUCSL', 'DGS10', 'DTWEXBGS', 'WALCL', 'WTREGEN', 'RRPONTSYD', 'SOFR', 'VIXCLS', 'A191RL1Q225SBEA', 'IORB')
REASON = 'Prospective model validation and account-specific portfolio constraints are not established by these macro observations.'


def encoded(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False, allow_nan=False).encode()


def digest(value):
    return hashlib.sha256(encoded(value)).hexdigest()


def clock(value):
    stamp = datetime.fromisoformat(value.replace('Z', '+00:00'))
    if stamp.tzinfo is None:
        raise ValueError('timezone-aware clock required')
    return stamp.astimezone(timezone.utc)


def number(value):
    if value is None or isinstance(value, bool):
        return None
    try:
        parsed = Decimal(str(value))
        return parsed if parsed.is_finite() else None
    except (InvalidOperation, ValueError):
        return None


def row_status(row, now, source_age):
    if row.get('contract') != SOURCE_CONTRACT:
        return 'unavailable'
    try:
        observed = date.fromisoformat(row['date'])
        age = (now.date() - observed).days
        acquired_age = (now - clock(row['acquired_at'])).total_seconds()
        limit = AGE_LIMITS[row['frequency']]
        if age < 0 or acquired_age < 0:
            return 'invalid_clock'
        if number(row.get('current_decimal')) is None:
            return 'unavailable'
        if age > limit:
            return 'stale_observation'
        if acquired_age > MAX_PACKET_AGE_SECONDS or source_age > MAX_PACKET_AGE_SECONDS:
            return 'stale_source'
        if row.get('quality', {}).get('status') != 'fresh':
            return 'source_ineligible'
        if set(row.get('evidence', {})) != {'definition', 'observations'}:
            return 'evidence_missing'
        return 'fresh'
    except (ValueError, KeyError, TypeError, OverflowError):
        return 'unavailable'


def project_row(sid, row, now, source_age):
    status = row_status(row, now, source_age)
    output = {'series_id': sid, 'metric': row.get('name') or sid, 'status': status,
              'observed_at': row.get('date'), 'acquired_at': row.get('acquired_at'),
              'unit': row.get('unit'), 'frequency': row.get('frequency'),
              'seasonal_adjustment': row.get('seasonal_adjustment'),
              'value': row.get('current_decimal') if status == 'fresh' else None,
              'last_observed_value': row.get('current_decimal'),
              'current_row_index': row.get('current_row_index'),
              'evidence': deepcopy(row.get('evidence', {})), 'changes': deepcopy(row.get('changes', {})),
              'action': None, 'sizing_eligible': False, 'calls_eligible': False}
    # A stale level can remain inspectable as history but cannot generate a
    # present-tense narrative or a fresh calendar-change headline.
    if status != 'fresh':
        output['summary'] = f'{sid}: {status.replace("_", " ")}. No current interpretation.'
        return output
    text = f'{sid}: {output["value"]} {output["unit"]}, observed {output["observed_at"]}.'
    month = output['changes'].get('month') or {}
    delta = number(month.get('change_decimal'))
    baseline = month.get('baseline_date')
    if delta is not None and baseline:
        unit = 'percentage points' if month.get('change_unit') == 'percentage_points' else month.get('change_unit')
        text += f' Change versus {baseline}: {"+" if delta > 0 else ""}{delta} {unit}.'
    else:
        text += ' One-month comparison unavailable for this observation.'
    output['summary'] = text
    return output


def build(source, generated_at):
    if not isinstance(source, dict) or source.get('contract') != SOURCE_CONTRACT:
        raise ValueError('canonical macro measurements required; legacy report is not an evidence substitute')
    now = clock(generated_at)
    age = (now-clock(source['generated_at'])).total_seconds()
    if age < 0:
        raise ValueError('source packet is future-dated')
    catalog = source.get('catalog')
    measurements = source.get('measurements')
    if not isinstance(catalog, dict) or not isinstance(measurements, dict):
        raise ValueError('complete catalog and measurements required')
    rows = []
    for sid in sorted(catalog):
        item = measurements.get(sid)
        if isinstance(item, dict):
            if item.get('series_id') != sid:
                raise ValueError('measurement identity differs')
            rows.append(project_row(sid, item, now, age))
        else:
            rows.append({'series_id': sid, 'metric': catalog[sid].get('display_name') or sid,
                         'value': None, 'status': 'unavailable', 'observed_at': None, 'unit': None,
                         'reason': source.get('errors', {}).get(sid, 'source_unavailable'),
                         'summary': f'{sid}: original source unavailable.', 'action': None,
                         'calls_eligible': False, 'sizing_eligible': False, 'evidence': {}, 'changes': {}})
    by_id = {row['series_id']: row for row in rows}
    fresh = sum(row['status'] == 'fresh' for row in rows)
    status = 'fresh' if fresh == len(rows) and rows else 'degraded' if fresh else 'unavailable'
    decision = {'verb': 'WAIT', 'meaning': 'abstain', 'reason': REASON,
                'direction': None, 'target_weight': None, 'confidence': None,
                'calls_eligible': False, 'sizing_eligible': False,
                'requires': ['prospectively validated forecast with original-price outcomes',
                             'current reconciled holdings, cash, constraints and risk budget',
                             'explicit instrument, horizon, cost and liquidity assumptions']}
    # Mixed-date liquidity proxy remains an observation. Recheck each leg here;
    # a newly generated brief cannot revive an old source by copying its flag.
    liquidity = deepcopy(source.get('net_liquidity') or {})
    eligible_legs = all((by_id.get(sid) or {}).get('status') == 'fresh'
                        for sid in ('WALCL', 'WTREGEN', 'RRPONTSYD'))
    if not eligible_legs:
        liquidity['net'] = liquidity['net_decimal'] = None
    liquidity.update(direction=None, change=None, calls_eligible=False, sizing_eligible=False,
                     status='fresh' if eligible_legs and liquidity.get('net_decimal') is not None else 'unavailable')
    vix = by_id.get('VIXCLS') or {}
    return {'contract': CONTRACT, 'version': '4.0.0', 'generated_at': generated_at,
            'timestamp': generated_at, 'source_generated_at': source['generated_at'],
            'source_replay': deepcopy(source.get('replay')), 'publication_time_verified': False,
            'quality': {'status': status, 'fresh_series': fresh, 'expected_series': len(rows),
                        'source_packet_age_seconds': age, 'basis': 'observation and source acquisition age, not wrapper age'},
            'headline': 'Dated macro research; investment call withheld',
            'headline_detail': f'{fresh} of {len(rows)} configured series pass observation and acquisition age checks. {REASON}',
            'phase': 'RESEARCH_ONLY', 'phase_color': '#94a3b8', 'action_required': 'WAIT — abstain',
            'call': None, 'decision': decision, 'forecast': None, 'calls_eligible': False, 'sizing_eligible': False,
            'scores': {**{key: None for key in ('khalid_index', 'ka_index', 'crisis_distance', 'plumbing_stress', 'ml_risk_score',
                                               'carry_risk_score', 'calibrated_composite', 'raw_composite', 'move')},
                       'vix': float(number(vix['value'])) if vix.get('status') == 'fresh' else None},
            'regime': {key: None for key in ('khalid', 'ka', 'ml', 'ml_description', 'sector', 'credit', 'liquidity', 'curve')},
            'metrics_table': rows, 'brief_items': [by_id[sid] for sid in CORE if sid in by_id],
            'net_liquidity': liquidity, 'signals': {'crisis_signals': [], 'warning_signals': [], 'bullish_signals': [], 'crisis_factors': []},
            'risks': [], 'stock_signals': {'buys': [], 'sells': [], 'at_risk': [], 'gainers': [], 'losers': []},
            'ml_intelligence': {'trade_recommendations': [], 'predictions': {}, 'status': 'not_validated'},
            'portfolio': {'allocation': {}, 'top_picks': [], 'avoid': [], 'rebalance': None,
                          'rationale': REASON, 'status': 'withheld', 'account_data_used': False,
                          'holdings_url': '/portfolio/', 'validation_url': '/signal-scorecard.html'},
            'plumbing_flags': [], 'dxy': {'value': None, 'strength': None, 'weekly': None, 'monthly': None,
                                        'reason': 'DTWEXBGS is the broad trade-weighted dollar index, not ICE DXY'},
            'yield_curve': {}, 'swap_spreads': {'2Y': None, '10Y': None, '30Y': None},
            'data_sources': {'canonical_packet': 'data/report-measurements.json', 'sources_active': 1,
                             'count_basis': 'one upstream packet; not a count of independent economic factors',
                             'legacy_report_used': False, 'unvalidated_forecast_inputs_used': False},
            'scope': 'FRED observations and calendar changes from the current retrieved vintage; no causal, probability, return or sizing claim'}


def narrative_context(packet, as_of):
    """Bounded consumer context. Renewing narrative cannot renew its observations."""
    result = {'contract': CONTRACT, 'status': 'unavailable', 'observations': [],
              'call': None, 'sizing_eligible': False, 'calls_eligible': False, 'reason': REASON}
    if not isinstance(packet, dict) or packet.get('contract') != CONTRACT:
        return result
    try:
        now = clock(as_of)
        source_age = (now-clock(packet['source_generated_at'])).total_seconds()
        if source_age < 0:
            return result
    except (ValueError, KeyError, TypeError):
        return result
    observations = []
    for row in (packet.get('brief_items') or [])[:12]:
        check = {'contract': SOURCE_CONTRACT, 'quality': {'status': row.get('status')},
                 'date': row.get('observed_at'), 'acquired_at': row.get('acquired_at'),
                 'frequency': row.get('frequency'), 'current_decimal': row.get('value'),
                 'evidence': row.get('evidence') or {}}
        status = row_status(check, now, source_age)
        observations.append({'series_id': row.get('series_id'), 'unit': row.get('unit'),
                             'observed_at': row.get('observed_at'), 'acquired_at': row.get('acquired_at'),
                             'status': status, 'value': row.get('value') if status == 'fresh' else None,
                             'calendar_month_change': deepcopy(row.get('changes', {}).get('month')) if status == 'fresh' else None,
                             'evidence': deepcopy(row.get('evidence', {}))})
    result.update(status='research_only' if any(r['status'] == 'fresh' for r in observations) else 'unavailable',
                  observations=observations, generated_at=packet.get('generated_at'),
                  source_generated_at=packet.get('source_generated_at'), replay=deepcopy(packet.get('replay')))
    return result
