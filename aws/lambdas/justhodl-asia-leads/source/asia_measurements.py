"""Exact-month merchandise export values; neither orders nor semiconductor volumes."""
from datetime import date,datetime,timezone
from decimal import Decimal,InvalidOperation
import math,re
CONTRACT='asia-export-calendar.v1'
FLAGS=dict.fromkeys(('forecast_qualified','calls_eligible','sizing_eligible','execution_eligible'),False)
PROFILES={
 'XTEXVA01KRM667N':('korea_exports','US dollars, exchange rate converted','Korea merchandise export value'),
 'VALEXPTWM052N':('taiwan_exports','Millions of Dollars','Taiwan goods export value')}
class MeasurementError(ValueError):
    pass


def clock(value):
    if not isinstance(value, str) or 'T' not in value:
        raise MeasurementError('Aware publication clock required')
    d = datetime.fromisoformat(value.replace('Z', '+00:00'))
    if d.tzinfo is None:
        raise MeasurementError('Aware publication clock required')
    return d.astimezone(timezone.utc)


def month(value):
    if not isinstance(value, str) or not re.fullmatch(r'\d{4}-(?:0[1-9]|1[0-2])', value):
        raise MeasurementError('Exact monthly identity required')
    return date.fromisoformat(value + '-01')


def shift(value, months):
    d = month(value); n = d.year * 12 + d.month - 1 + months
    return f'{n // 12:04d}-{n % 12 + 1:02d}'


def number(value):
    if value in (None, '', '.'):
        return None
    if isinstance(value, bool) or not isinstance(value, (str, int, float)):
        raise MeasurementError('Published numeric value required')
    if not re.fullmatch(r'[+-]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)(?:[eE][+-]?[0-9]+)?', str(value).strip()):
        raise MeasurementError('Direct decimal source representation required')
    try:
        d = Decimal(str(value))
    except InvalidOperation:
        raise MeasurementError('Invalid source number') from None
    if not d.is_finite() or d < 0 or d > Decimal('1e15') or (d != 0 and float(d) == 0):
        raise MeasurementError('Finite nonnegative representable source number required')
    return d


def change(current, previous, current_month, previous_month):
    status = 'latest_missing' if current is None else 'prior_month_missing' if previous is None else 'zero_denominator' if previous == 0 else 'measured'
    value = float((current / previous - 1) * 100) if status == 'measured' else None
    if value is not None and not math.isfinite(value):
        status = 'outside_numeric_range'; value = None
    return {'status': status, 'percent': value,
            'current_month': current_month, 'previous_month': previous_month, 'unit': 'percent'}


def summary(values):
    if not values:
        raise MeasurementError('Monthly population is empty')
    latest = max(values); current = values[latest]
    out = {'latest_month': latest, 'level': float(current) if current is not None else None,
           'status': 'measured' if current is not None else 'latest_missing'}
    for name, distance in (('mom', -1), ('three_month', -3), ('yoy', -12)):
        prior = shift(latest, distance)
        out[name] = change(current, values.get(prior), latest, prior)
    return out


def fred(sid, metadata, packet, at):
    key, unit, name = PROFILES[sid]; cutoff = clock(at).date()
    out = {'series_id': sid, 'key': key, 'name': name, 'source_url': 'https://fred.stlouisfed.org/series/' + sid,
           'status': 'unavailable', 'definition_status': 'unverified', 'unit': unit, 'seasonal_adjustment': 'NSA',
           'frequency': 'monthly', 'observation_date_kind': 'month_start_label', 'observations': [],
           'returned_rows': 0, 'original_vintage_verified': False, 'observation_freshness_verified': False, **FLAGS}
    records = metadata.get('seriess') if isinstance(metadata, dict) else None
    if not isinstance(records, list) or len(records) != 1 or not isinstance(records[0], dict) or records[0].get('id') != sid:
        out['status'] = 'metadata_identity_mismatch'; return out
    meta = records[0]; out['metadata'] = meta
    if (meta.get('units'), meta.get('frequency_short'), meta.get('seasonal_adjustment_short')) != (unit, 'M', 'NSA'):
        out['status'] = 'metadata_definition_changed'; return out
    out['definition_status'] = 'reviewed_source_definition'
    rows = packet.get('observations') if isinstance(packet, dict) else None
    if (not isinstance(rows, list) or type(packet.get('count')) is not int or packet['count'] != len(rows)
            or type(packet.get('offset')) is not int or packet['offset'] != 0 or packet.get('units') != 'lin'
            or type(packet.get('output_type')) is not int or packet['output_type'] != 1):
        out['status'] = 'incomplete_or_transformed_response'; return out
    values = {}; ambiguous = False
    for position, row in enumerate(rows):
        item = {'position': position, 'original': row, 'month': None, 'value': None, 'status': 'invalid_observation'}
        try:
            if not isinstance(row, dict):
                raise MeasurementError('Observation object required')
            label = row.get('date'); d = date.fromisoformat(label)
            if label != d.isoformat() or d.day != 1 or d > cutoff:
                raise MeasurementError('Monthly observation identity differs')
            ym = label[:7]; value = number(row.get('value'))
            if ym in values:
                raise MeasurementError('Duplicate month')
            values[ym] = value
            item.update(month=ym, value=float(value) if value is not None else None, status='observed' if value is not None else 'missing')
        except (ValueError, TypeError):
            ambiguous = True
        out['observations'].append(item)
    out['returned_rows'] = len(rows)
    if ambiguous:
        out['status'] = 'ambiguous_or_invalid_observations'; return out
    if not values:
        out['status'] = 'empty_observations'; return out
    out.update(summary(values), observation_age_days=(cutoff - month(max(values))).days,
               annualized_three_month_percent=None, annualization_status='not_seasonally_adjusted')
    return out
