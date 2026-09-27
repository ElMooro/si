"""Descriptive SEC concept measurements with explicit units and calendar pairs.

All received observations survive. No input order, fiscal label or publication
clock substitutes for an observation period. This is not filing-vintage replay.
"""
from datetime import date, timedelta
import calendar
import json
import math
import re

CONTRACT = 'backlog-measurements.v1'
EPS = ('EarningsPerShareDiluted', 'EarningsPerShareBasic')
TAGS = ('RevenueRemainingPerformanceObligation', 'ContractWithCustomerLiability',
        'ContractWithCustomerLiabilityCurrent', 'DeferredRevenueCurrent',
        'ContractWithCustomerLiabilityNoncurrent') + EPS


def decode(raw):
    def pairs(items):
        result = {}
        for key, value in items:
            if key in result: raise ValueError('Duplicate source JSON key')
            result[key] = value
        return result
    def invalid(value): raise ValueError('Nonfinite source JSON value')
    def decimal(value):
        result = float(value)
        if not math.isfinite(result) or result == 0 and any(c in '123456789' for c in value.lower().split('e')[0]): invalid(value)
        return result
    return json.loads(raw, object_pairs_hook=pairs, parse_constant=invalid, parse_float=decimal)


def day(value):
    if not isinstance(value, str) or not re.fullmatch(r'\d{4}-\d{2}-\d{2}', value): return None
    try: return date.fromisoformat(value)
    except ValueError: return None


def number(value):
    if isinstance(value, bool) or not isinstance(value, (int, float)): return None
    try: return value if math.isfinite(value) else None
    except (OverflowError, ValueError): return None


def shifted(value, months):
    total = value.year * 12 + value.month - 1 + months
    year, month = divmod(total, 12); month += 1
    if not 1 <= year <= 9999: return None
    return date(year, month, min(value.day, calendar.monthrange(year, month)[1]))


def kind(start, end):
    if start is None: return 'instant'
    if start.day != 1: return 'unreviewed_duration'
    for months, label in ((3, 'calendar_quarter'), (12, 'calendar_year')):
        next_start = shifted(start, months)
        if next_start and next_start - timedelta(days=1) == end: return label
    return 'unreviewed_duration'


def percentage(current, prior):
    if number(current) is None or number(prior) is None or prior <= 0: return None
    try: result = (current / prior - 1) * 100
    except (OverflowError, ZeroDivisionError): return None
    return round(result, 1) if math.isfinite(result) else None


def compile_concept(payload, cik, tag, as_of):
    today = day(as_of)
    if not today or tag not in TAGS or not isinstance(payload, dict): raise ValueError('Reviewed concept and clock required')
    expected = str(cik).lstrip('0')
    if not expected.isdigit() or not expected: raise ValueError('Issuer CIK required')
    unit = 'USD/shares' if tag in EPS else 'USD'
    missing = payload.get('_source_status') == 404
    if not missing and (str(payload.get('cik')).lstrip('0') != expected
                        or payload.get('tag') != tag or payload.get('taxonomy') != 'us-gaap'):
        raise ValueError('Returned issuer or concept differs')
    units = {} if missing else payload.get('units')
    if not isinstance(units, dict): raise ValueError('Concept units object required')
    observations = []
    for received_unit, values in units.items():
        if not isinstance(values, list): raise ValueError('Whole concept observation array required')
        for index, source in enumerate(values):
            row = {'source_unit': received_unit, 'source_index': index, 'source': source,
                   'eligible': False, 'reasons': []}
            observations.append(row)
            if not isinstance(source, dict): row['reasons'].append('observation_not_object'); continue
            start = day(source.get('start')); end = day(source.get('end')); filed = day(source.get('filed'))
            value = number(source.get('val'))
            if received_unit != unit: row['reasons'].append('unreviewed_unit')
            if value is None: row['reasons'].append('missing_or_nonnumeric_value')
            if tag not in EPS and value is not None and value < 0: row['reasons'].append('negative_balance')
            if end is None or filed is None or end > today or filed > today or filed < end:
                row['reasons'].append('invalid_or_future_observation_or_filing_date')
            if (tag in EPS and (start is None or end is None or start > end)
                    or tag not in EPS and source.get('start') is not None):
                row['reasons'].append('wrong_or_unknown_duration')
            if source.get('form') not in ('10-K', '10-Q', '10-K/A', '10-Q/A', '20-F', '20-F/A', '40-F', '40-F/A'):
                row['reasons'].append('unreviewed_form')
            if not re.fullmatch(r'\d{10}-\d{2}-\d{6}', str(source.get('accn', ''))):
                row['reasons'].append('missing_or_invalid_accession')
            row.update(eligible=not row['reasons'], unit=unit, value=value,
                       start=start.isoformat() if start else None, end=end.isoformat() if end else None,
                       filed=filed.isoformat() if filed else None, accession=source.get('accn'),
                       period_kind=kind(start, end) if end else None)
    periods = []
    groups = {}
    for row in observations:
        if row['eligible']: groups.setdefault((row['start'], row['end']), []).append(row)
    for (start, end), rows in sorted(groups.items(), key=lambda p: (p[0][1], p[0][0] or '')):
        latest_filed = max(r['filed'] for r in rows)
        latest = [r for r in rows if r['filed'] == latest_filed]
        values = {r['value'] for r in latest}
        periods.append({'start': start, 'end': end, 'filed': latest_filed, 'unit': unit,
                        'period_kind': latest[0]['period_kind'], 'value': latest[0]['value'] if len(values) == 1 else None,
                        'status': 'latest_reported_value' if len(values) == 1 else 'conflicting_latest_filings',
                        'source_indices': [r['source_index'] for r in latest],
                        'accessions': sorted({r['accession'] for r in latest})})
    result = {'contract': CONTRACT, 'cik': str(cik).zfill(10), 'tag': tag, 'unit': unit,
              'checked_as_of': as_of, 'source_status': 'not_found' if missing else 'received',
              'source_metadata': {k: v for k, v in payload.items() if k != 'units'},
              'observations': observations, 'periods': periods, 'eligible_observation_count': sum(r['eligible'] for r in observations),
              'latest': None, 'qoq': None, 'yoy': None, 'comparisons': {},
              'provider_originals_replayed': False, 'point_in_time_availability_verified': False,
              'forecast_qualified': False, 'calls_eligible': False, 'sizing_eligible': False}
    if not periods: result['status'] = 'no_eligible_observation'; return result
    latest_end = max(r['end'] for r in periods)
    current = [r for r in periods if r['end'] == latest_end]
    # EPS can contain quarterly, YTD and annual facts ending on the same day.
    # Do not choose one from array order, fiscal fp or a frame label.
    if len(current) != 1 or current[0]['value'] is None:
        result['status'] = 'ambiguous_latest_period_or_value'; return result
    latest = current[0]; result.update(latest=latest, status='descriptive_measurement')
    for label, months in (('qoq', 3), ('yoy', 12)):
        allowed = latest['period_kind'] in ('instant', 'calendar_quarter') if label == 'qoq' else latest['period_kind'] in ('instant', 'calendar_quarter', 'calendar_year')
        expected_end = shifted(day(latest['end']), -months)
        if expected_end is None:
            result['comparisons'][label] = {'status': 'calendar_boundary', 'value_pct': None, 'current': latest, 'prior': None}
            continue
        # Preserve month-end convention for short February fiscal endpoints.
        if day(latest['end']).day == calendar.monthrange(day(latest['end']).year, day(latest['end']).month)[1]:
            expected_end = date(expected_end.year, expected_end.month, calendar.monthrange(expected_end.year, expected_end.month)[1])
        shifted_start = shifted(day(latest['start']), -months) if latest['start'] else None
        expected_start = shifted_start.isoformat() if shifted_start else None
        prior = [r for r in periods if r['end'] == expected_end.isoformat() and r['start'] == expected_start and r['period_kind'] == latest['period_kind']]
        status = 'exact_calendar_pair_missing'
        value = None
        if not allowed: status = 'duration_not_qualified_for_comparison'
        elif len(prior) == 1:
            value = percentage(latest['value'], prior[0]['value'])
            status = 'exact_calendar_pair' if value is not None else 'invalid_or_nonpositive_denominator'
        result[label] = value
        result['comparisons'][label] = {'status': status, 'value_pct': value, 'current': latest,
                                       'prior': prior[0] if len(prior) == 1 else None}
    return result


def row_from_concepts(symbol, cik, meta, concepts):
    rec = {'ticker': symbol, 'cik': cik, **meta, 'measurement_contract': CONTRACT,
           'measurements': concepts, 'call': None, 'calls_eligible': False, 'forecast_qualified': False,
           'sizing_eligible': False, 'quality': {'status': 'partial', 'provider_originals_replayed': False,
           'point_in_time_availability_verified': False}, 'rev_yoy': None, 'ev_to_rpo': None,
           'rpo_minus_rev_growth': None, 'demand_accelerating': None, 'deferred_accelerating': None}
    for family, key in (('rpo', 'rpo'), ('deferred', 'deferred_rev'), ('eps', 'eps')):
        c = concepts[family]; latest = c.get('latest') or {}
        rec.update({key: latest.get('value'), family+'_qoq': c.get('qoq'), family+'_yoy': c.get('yoy'),
                    family+'_asof': latest.get('end'), family+'_filed': latest.get('filed'),
                    family+'_unit': c.get('unit'), family+'_tag': c.get('tag'),
                    family+'_period_start': latest.get('start'), family+'_accessions': latest.get('accessions', [])})
        indices = latest.get('source_indices', [])
        sources = [r['source'] for r in c.get('observations', []) if r.get('eligible') and r.get('source_index') in indices]
        rec[family+'_form'] = sources[0].get('form') if sources and len({s.get('form') for s in sources}) == 1 else None
    rec['comparison_limits'] = 'Exact calendar pairs only; noncalendar and 52/53-week periods are not inferred. Revenue/EV ratios and acceleration remain unavailable without aligned currency, periods and source originals.'
    return rec
