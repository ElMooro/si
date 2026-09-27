"""Separate China monetary definitions, exact calendars and descriptive prices.

No money-growth series is a TSF credit impulse. No price move is a capital flow.
The full current-vintage population is inspectable; original vintages are not
established by fetching it today.
"""
from datetime import date, datetime, timezone, timedelta
from decimal import Decimal, InvalidOperation
import calendar, math, re

CONTRACT = 'china-source-calendar.v1'
FLAGS = dict.fromkeys(('forecast_qualified', 'calls_eligible', 'sizing_eligible', 'execution_eligible'), False)
# concept, exact provider unit, frequency, adjustment, definition reviewed
PROFILES = {
    'MANMM101CNM189S': ('M1', 'Yuan Renminbi', 'M', 'SA', True),
    'MANMM101CNQ189S': ('M1', 'Yuan Renminbi', 'Q', 'SA', True),
    'MYAGM2CNM189N': ('M2', 'National Currency', 'M', 'NSA', True),
    'MABMM301CNM189S': ('M3', 'Yuan Renminbi', 'M', 'SA', True),
    'MABMM301CNQ189S': ('M3', 'Yuan Renminbi', 'Q', 'SA', True),
    'IR3TIB01CNM156N': ('interbank_rate', 'Percent', 'M', 'NSA', True),
    'IR3TIB01CNM156S': ('interbank_rate_alternative', None, None, None, False),
    'DEXCHUS': ('usd_cny', 'Chinese Yuan Renminbi to One U.S. Dollar', 'D', 'NSA', True),
    'PCOPPUSDM': ('copper_price', 'U.S. Dollars per Metric Ton', 'M', 'NSA', True),
    'IQ12260': ('gold_export_price_index', 'Index Dec 2024=100', 'M', 'NSA', True),
    'GOLDAMGBD228NLBM': ('retired_gold_price', None, None, None, False),
}


class MeasurementError(ValueError):
    pass


def clock(value):
    if not isinstance(value, str) or 'T' not in value:
        raise MeasurementError('Aware calculation clock required')
    d = datetime.fromisoformat(value.replace('Z', '+00:00'))
    if d.tzinfo is None:
        raise MeasurementError('Aware calculation clock required')
    return d.astimezone(timezone.utc)


def shift(label, months):
    d = date.fromisoformat(label); n = d.year * 12 + d.month - 1 + months
    y, m = n // 12, n % 12 + 1
    return date(y, m, min(d.day, calendar.monthrange(y, m)[1])).isoformat()


def number(value, negative=False):
    if value in (None, '', '.'):
        return None
    if isinstance(value, bool) or not isinstance(value, (str, int, float)):
        raise MeasurementError('Direct numeric source representation required')
    if not re.fullmatch(r'[+-]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)(?:[eE][+-]?[0-9]+)?', str(value).strip()):
        raise MeasurementError('Direct decimal representation required')
    try:
        d = Decimal(str(value))
    except InvalidOperation:
        raise MeasurementError('Invalid source number') from None
    if not d.is_finite() or abs(d) > Decimal('1e18') or (d < 0 and not negative) or (d != 0 and float(d) == 0):
        raise MeasurementError('Finite representable source value required')
    return d


def comparison(values, current, prior, rate=False):
    a, b = values.get(current), values.get(prior)
    status = 'latest_missing' if a is None else 'prior_period_missing' if b is None else 'zero_denominator' if b == 0 and not rate else 'measured'
    value = (a - b if rate else (a / b - 1) * 100) if status == 'measured' else None
    value = float(value) if value is not None else None
    if value is not None and not math.isfinite(value):
        status, value = 'outside_numeric_range', None
    return {'status': status, 'value': value, 'unit': 'percentage_points' if rate else 'percent',
            'current_date': current, 'comparison_date': prior}


def fred(sid, metadata, packet, at):
    concept, unit, frequency, adjustment, reviewed = PROFILES[sid]
    cutoff = clock(at).date()
    out = {'series_id': sid, 'concept': concept, 'source_url': 'https://fred.stlouisfed.org/series/' + sid,
           'unit': unit, 'frequency': frequency, 'seasonal_adjustment': adjustment,
           'status': 'unavailable', 'definition_status': 'unverified', 'observations': [], 'returned_rows': 0,
           'current_measurement_eligible': False, 'original_vintage_verified': False, **FLAGS}
    # Preserve even an unsuccessful/definition-mismatched response in full.
    out['metadata_response'] = metadata
    out['observation_response'] = packet
    raw_rows = packet.get('observations') if isinstance(packet, dict) else None
    if isinstance(raw_rows, list):
        out['returned_rows'] = len(raw_rows)
        out['observations'] = [{'position': i, 'original': row, 'date': None, 'value': None, 'status': 'unqualified_observation'}
                               for i, row in enumerate(raw_rows)]
    records = metadata.get('seriess') if isinstance(metadata, dict) else None
    if not isinstance(records, list) or len(records) != 1 or not isinstance(records[0], dict) or records[0].get('id') != sid:
        out['status'] = 'metadata_identity_mismatch'; return out
    meta = records[0]
    if not reviewed:
        out['status'] = 'definition_not_reviewed'; return out
    if (meta.get('units'), meta.get('frequency_short'), meta.get('seasonal_adjustment_short')) != (unit, frequency, adjustment):
        out['status'] = 'metadata_definition_changed'; return out
    out['definition_status'] = 'reviewed_source_definition'
    rows = packet.get('observations') if isinstance(packet, dict) else None
    if (not isinstance(rows, list) or type(packet.get('count')) is not int or packet['count'] != len(rows)
            or type(packet.get('offset')) is not int or packet['offset'] != 0 or packet.get('units') != 'lin'
            or type(packet.get('output_type')) is not int or packet['output_type'] != 1):
        out['status'] = 'incomplete_or_transformed_response'; return out
    values = {}; invalid = False
    for position, row in enumerate(rows):
        item = {'position': position, 'original': row, 'date': None, 'value': None, 'status': 'invalid_observation'}
        try:
            if not isinstance(row, dict):
                raise MeasurementError('Observation object required')
            label = row.get('date'); d = date.fromisoformat(label)
            if label != d.isoformat() or d > cutoff or label in values:
                raise MeasurementError('Unique historical date required')
            if frequency in ('M', 'Q') and (d.day != 1 or (frequency == 'Q' and d.month not in (1, 4, 7, 10))):
                raise MeasurementError('Exact period-start identity required')
            v = number(row.get('value'), concept == 'interbank_rate')
            values[label] = v
            item.update(date=label, value=float(v) if v is not None else None, status='observed' if v is not None else 'missing')
        except (ValueError, TypeError):
            invalid = True
        out['observations'][position] = item
    out['returned_rows'] = len(rows)
    if invalid or not values:
        out['status'] = 'ambiguous_or_invalid_observations' if invalid else 'empty_observations'; return out
    latest = max(values); end = date.fromisoformat(latest)
    if frequency in ('M', 'Q'):
        end = date.fromisoformat(shift(latest, 1 if frequency == 'M' else 3)) - timedelta(days=1)
    if end > cutoff:
        out['status'] = 'incomplete_observation_period'; return out
    age = (cutoff - end).days; limit = {'D': 14, 'M': 120, 'Q': 220}[frequency]
    out.update(status='measured' if values[latest] is not None else 'latest_missing', latest_date=latest,
               latest_period_end=end.isoformat(), level=float(values[latest]) if values[latest] is not None else None,
               observation_age_days=age, age_policy_days=limit, freshness='within_age_policy' if age <= limit else 'stale',
               freshness_basis='Age after the observation period ends; an operational maximum, not a verified release-calendar SLA.',
               current_measurement_eligible=age <= limit and values[latest] is not None)
    rate = concept == 'interbank_rate'
    for name, months in (('three_month', -3), ('yoy', -12)):
        anchor = shift(latest, months); prior = anchor
        if frequency == 'D' and anchor not in values:
            permitted = [k for k in values if k <= anchor and (date.fromisoformat(anchor) - date.fromisoformat(k)).days <= 7]
            prior = max(permitted) if permitted else anchor
        out[name] = comparison(values, latest, prior, rate)
        out[name]['calendar_anchor'] = anchor
        out[name]['comparison_policy'] = 'exact_calendar_period' if frequency != 'D' else 'anchor_or_prior_observation_within_7_calendar_days; missing_value_never_backfilled'
    if concept in ('M1', 'M2', 'M3'):
        prior = shift(latest, -12); before = comparison(values, prior, shift(latest, -24))
        now = out['yoy']; measured = now['status'] == before['status'] == 'measured'
        out['money_growth_acceleration'] = {'value_pp': now['value'] - before['value'] if measured else None,
                                            'current_yoy': now, 'prior_yoy': before, 'is_credit_impulse': False}
    out['annualization_status'] = 'not_performed'
    return out
