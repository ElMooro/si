"""Pure FRED measurement compiler for the research report.

Calendar periods, source units and missing observations survive the transform.
This module assigns no investment score or portfolio authority.
"""
import calendar
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
import hashlib
import json
import re
from urllib.parse import parse_qs, urlsplit

CONTRACT = 'report-observations.v1'
AGE_LIMITS = {'D': 10, 'W': 21, 'BW': 35, 'M': 100, 'Q': 200, 'SA': 370, 'A': 550}


def encoded(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False, allow_nan=False).encode()


def digest(value):
    return hashlib.sha256(encoded(value)).hexdigest()


def decimal(value):
    if value is None or isinstance(value, bool) or value in ('', '.'):
        return None
    try:
        result = Decimal(str(value))
        return result if result.is_finite() else None
    except InvalidOperation:
        return None


def months_before(day, months):
    year, month = divmod(day.year * 12 + day.month - 1 - months, 12)
    month += 1
    return date(year, month, min(day.day, calendar.monthrange(year, month)[1]))


def comparison(current, baseline, target=None, reason=None, unit=None):
    c, b = decimal(current.get('value')), decimal((baseline or {}).get('value'))
    difference = c - b if c is not None and b is not None else None
    relative = 100 * difference / b if difference is not None and b > 0 else None
    return {'target_date': target.isoformat() if target else None,
            'current_date': current['date'], 'baseline_date': (baseline or {}).get('date'),
            'current_decimal': str(c) if c is not None else None,
            'baseline_decimal': str(b) if b is not None else None,
            'change_decimal': str(difference) if difference is not None else None,
            'change': float(difference) if difference is not None else None,
            'change_unit': 'percentage_points' if isinstance(unit, str) and unit.startswith('Percent') else unit,
            'source_unit': unit,
            'pct_change': round(float(relative), 6) if relative is not None else None,
            'relative_change_reason': reason or ('nonpositive_or_missing_baseline' if relative is None else None),
            'formula': 'current - baseline; relative_percent = 100 * (current / baseline - 1) when baseline > 0',
            'baseline_row_index': (baseline or {}).get('row_index')}


def baseline_for(rows, frequency, months, days=0):
    current_day = date.fromisoformat(rows[0]['date'])
    target = current_day - timedelta(days=days) if days else months_before(current_day, months)
    allowed = (frequency in ('D', 'W', 'BW') and (not days or frequency != 'BW'))
    allowed |= frequency == 'M' and months >= 1
    allowed |= frequency == 'Q' and months >= 3 and months % 3 == 0
    allowed |= frequency == 'SA' and months >= 6 and months % 6 == 0
    allowed |= frequency == 'A' and months >= 12
    if not allowed:
        return None, target, 'horizon_shorter_than_published_frequency'
    if frequency in ('M', 'Q', 'SA', 'A'):
        # FRED period labels are exact. A missing month/quarter cannot be replaced
        # by a neighboring one, even if a compacted history omitted its null row.
        match = next((r for r in rows if r['date'] == target.isoformat()), None)
    else:
        match = next((r for r in rows if r['date'] <= target.isoformat()), None)
        tolerance = {'D': 4, 'W': 6, 'BW': 13}[frequency]
        if match and (target - date.fromisoformat(match['date'])).days > tolerance:
            match = None
    return match, target, None if match else 'calendar_baseline_missing'


def measurement(series_id, definition, document, evidence, generated_at, acquired_at):
    now = datetime.fromisoformat(generated_at.replace('Z', '+00:00'))
    acquired = datetime.fromisoformat(acquired_at.replace('Z', '+00:00'))
    if now.tzinfo is None or acquired.tzinfo is None or not 0 <= (now-acquired).total_seconds():
        raise ValueError('invalid observation acquisition clock')
    definitions = definition.get('seriess') or []
    if len(definitions) != 1 or definitions[0].get('id') != series_id:
        raise ValueError('definition series identity differs')
    meta = definitions[0]
    frequency, unit = meta.get('frequency_short'), meta.get('units')
    if frequency not in AGE_LIMITS or not unit or not meta.get('seasonal_adjustment'):
        raise ValueError('incomplete official series definition')
    if document.get('units') != 'lin' or document.get('output_type', 1) != 1:
        raise ValueError('untransformed level response required')
    if set(evidence) != {'definition', 'observations'}:
        raise ValueError('both original responses required')
    for part, receipt in evidence.items():
        url = urlsplit(receipt.get('source_url', ''))
        query = parse_qs(url.query)
        expected_path = '/fred/series' + ('/observations' if part == 'observations' else '')
        if (receipt.get('contract') != 'source-evidence.v1' or receipt.get('captured') is not True
                or not re.fullmatch('[a-f0-9]{64}', str(receipt.get('sha256', '')))
                or url.scheme != 'https' or url.netloc != 'api.stlouisfed.org' or url.path != expected_path
                or query.get('series_id') != [series_id] or not receipt.get('key')
                or (part == 'observations' and query.get('units') != ['lin'])):
            raise ValueError('original request identity differs')
    rows = []; dates = set()
    for index, row in enumerate(document.get('observations') or []):
        day = date.fromisoformat(row['date'])
        if day in dates:
            raise ValueError('duplicate observation')
        dates.add(day)
        value = decimal(row.get('value'))
        rows.append({'date': day.isoformat(), 'value': str(value) if value is not None else None,
                     'row_index': index, 'realtime_start': row.get('realtime_start'),
                     'realtime_end': row.get('realtime_end')})
    rows.sort(key=lambda r: r['date'], reverse=True)
    if not rows:
        raise ValueError('no provider observations')
    reported_count = document.get('count')
    if type(reported_count) is not int or reported_count < len(rows):
        raise ValueError('invalid provider row count')
    limit = document.get('limit')
    if type(limit) is not int or not 1 <= limit <= 4000 or document.get('offset') != 0 or len(rows) != min(reported_count, limit):
        raise ValueError('incomplete or unexpected provider query page')
    query = parse_qs(urlsplit(evidence['observations']['source_url']).query)
    if query.get('limit') != [str(limit)] or query.get('sort_order') != ['desc']:
        raise ValueError('provider page differs from retained query')
    requested_start = query.get('observation_start',[None])[0]
    requested_end = query.get('observation_end',[None])[0]
    if requested_start is not None:date.fromisoformat(requested_start)
    if requested_end is not None:date.fromisoformat(requested_end)
    if requested_start and requested_end and requested_start>requested_end:
        raise ValueError('reversed source query bounds')
    if any((requested_start and r['date']<requested_start) or (requested_end and r['date']>requested_end) for r in rows):
        raise ValueError('provider row outside retained query bounds')
    returned_count = len(rows)
    future_rows = [r for r in rows if r['date'] > now.date().isoformat()]
    rows = [r for r in rows if r['date'] <= now.date().isoformat()]
    if not rows:
        raise ValueError('no observation at or before evaluation date')
    latest = rows[0]; current = decimal(latest['value'])
    age = (now.date() - date.fromisoformat(latest['date'])).days
    status = 'unavailable' if current is None else 'stale' if age > AGE_LIMITS[frequency] else 'fresh'
    # A recent retrieval of old observations remains stale. A recent publication
    # wrapper also cannot renew a source we have not checked in the last day.
    if status == 'fresh' and (now-acquired).total_seconds() > 26*3600:
        status = 'stale_source'
    changes = {}
    for label, months, days in (('week', 0, 7), ('month', 1, 0), ('quarter', 3, 0), ('year', 12, 0)):
        baseline, target, reason = baseline_for(rows, frequency, months, days)
        changes[label] = comparison(latest, baseline, target, reason, unit)
    previous = rows[1] if len(rows) > 1 else None
    previous_change = comparison(latest, previous, unit=unit)
    values = [decimal(r['value']) for r in rows if r['value'] is not None]
    out = {'contract': CONTRACT, 'series_id': series_id, 'name': meta.get('title') or series_id,
           'definition': meta, 'unit': unit, 'frequency': frequency,
           'seasonal_adjustment': meta['seasonal_adjustment'], 'date': latest['date'],
           'current': float(current) if current is not None else None,
           'current_decimal': str(current) if current is not None else None,
           'prev': float(decimal(previous['value'])) if previous and previous['value'] is not None else None,
           'change': previous_change['change'], 'pct_change': previous_change['pct_change'],
           'previous_observation_change': previous_change, 'changes': changes,
           'change_basis': 'dated_calendar_cutoffs_with_explicit_frequency; relative_percent_not_percentage_points',
           'high': float(max(values)) if values else None, 'low': float(min(values)) if values else None,
           'avg': round(float(sum(values) / len(values)), 6) if values else None,
           'statistics_scope': {'start': rows[-1]['date'], 'end': latest['date'], 'numeric_rows': len(values)},
           'history': [{**r, 'value_decimal': r['value'], 'value': float(decimal(r['value'])) if r['value'] is not None else None}
                       for r in rows[:60]],
           'embedded_history_scope': 'newest 60 returned observations; complete fetched response retained in evidence',
           'evidence': evidence, 'current_row_index': latest['row_index'], 'acquired_at': acquired_at,
           'provider_updated_at': meta.get('last_updated'), 'published_at': None,
           'vintage': {'basis': 'current_provider_response_not_original_publication_history',
                       'realtime_start': document.get('realtime_start'), 'realtime_end': document.get('realtime_end')},
           'coverage': {'returned': returned_count, 'matching_query_count': reported_count,
                        'complete_history': reported_count == returned_count and requested_start is None and requested_end is None,
                        'complete_query': reported_count == returned_count,
                        'requested_start': requested_start, 'requested_end': requested_end,
                        'query_limit': limit, 'first_returned_date': rows[-1]['date'],
                        'history_scope': 'Bounded current-vintage query; not an all-time or publication-time history',
                        'eligible_observations': len(rows), 'future_observations_excluded': len(future_rows)},
           'future_dated_rows': future_rows,
           'future_date_policy': 'Retained as provider records; excluded from observed values, changes and statistics',
           'quality': {'status': status, 'observation_age_days': age, 'max_age_days': AGE_LIMITS[frequency],
                       'basis': 'observation_age_and_acquisition_ceiling; release_calendar_not_verified'},
           'sizing_eligible': False, 'calls_eligible': False}
    for label in ('week', 'month', 'quarter', 'year'):
        out[label+'_pct'] = changes[label]['pct_change']
    return out


def liquidity(measurements):
    """Mixed-date proxy; never call it synchronized reserves or a causal impulse."""
    definitions = {'WALCL': ('Millions of U.S. Dollars', 1),
                   'WTREGEN': ('Millions of U.S. Dollars', 1),
                   'RRPONTSYD': ('Billions of US Dollars', 1000)}
    components = {}; missing = []
    for sid, (unit, factor) in definitions.items():
        row = measurements.get(sid) or {}
        value = decimal(row.get('current_decimal'))
        # FRED spells U.S. and US differently for these exact series.
        valid = row.get('contract') == CONTRACT and row.get('unit') == unit and row.get('quality', {}).get('status') == 'fresh' and value is not None
        if not valid: missing.append(sid)
        components[sid] = {'value_decimal': str(value) if value is not None else None,
                           'unit': row.get('unit'), 'multiplier_to_usd_millions': factor,
                           'date': row.get('date'), 'evidence': row.get('evidence'),
                           'row_index': row.get('current_row_index'), 'eligible': valid}
    result = None
    if not missing:
        result = decimal(components['WALCL']['value_decimal']) - decimal(components['WTREGEN']['value_decimal']) - decimal(components['RRPONTSYD']['value_decimal'])*1000
    return {'net': float(result) if result is not None else None, 'net_decimal': str(result) if result is not None else None,
            'unit': 'USD_millions', 'formula': 'WALCL - WTREGEN - 1000 * RRPONTSYD',
            'components': components, 'missing_or_ineligible': missing,
            'basis': 'mixed dates; WALCL Wednesday stock, WTREGEN weekly average, RRP daily stock',
            'change': None, 'direction': None, 'sizing_eligible': False, 'calls_eligible': False}


def build(catalog, inputs, generated_at, errors=None):
    measurements = {}; failures = dict(errors or {})
    for sid in catalog:
        source = inputs.get(sid)
        if not source:
            failures.setdefault(sid, 'source_unavailable')
            continue
        try:
            measurements[sid] = measurement(sid, source['definition'], source['observations'],
                                            source['evidence'], generated_at, source['acquired_at'])
        except (ValueError, TypeError, KeyError, InvalidOperation, OverflowError) as exc:
            failures[sid] = type(exc).__name__
    fresh = sum(m['quality']['status'] == 'fresh' for m in measurements.values())
    return {'contract': CONTRACT, 'version': '1.0.0', 'generated_at': generated_at,
            'measurements': measurements, 'catalog': catalog, 'errors': failures,
            'quality': {'status': 'fresh' if fresh == len(catalog) and not failures else 'degraded' if fresh else 'unavailable',
                        'expected_series': len(catalog), 'compiled_series': len(measurements), 'fresh_series': fresh},
            'net_liquidity': liquidity(measurements), 'call': None, 'sizing_eligible': False,
            'calls_eligible': False, 'publication_time_verified': False,
            'scope': 'FRED research measurements only; equities, crypto, legacy report scores and portfolio weights excluded'}
