"""Exact reported short positions and explicit arithmetic comparisons.

Two complete equal scans bound observed pagination stability. They do not prove
an atomic provider snapshot, historical availability, or security continuity.
"""
from collections import Counter
from datetime import date
from decimal import Decimal, localcontext, ROUND_HALF_UP, ROUND_HALF_EVEN
import hashlib, json, re
import offexchange_measurements as exact
from datetime import timedelta
from fractions import Fraction

DATASET = 'consolidatedshortinterest'
FIELDS = ('accountingYearMonthNumber', 'symbolCode', 'issueName',
          'issuerServicesGroupExchangeCode', 'marketClassCode',
          'currentShortPositionQuantity', 'previousShortPositionQuantity',
          'stockSplitFlag', 'averageDailyVolumeQuantity', 'daysToCoverQuantity',
          'revisionFlag', 'changePercent', 'changePreviousNumber', 'settlementDate')
GRAIN = ('symbolCode', 'issueName', 'issuerServicesGroupExchangeCode', 'marketClassCode')
LIMIT = 5000


def encoded(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False, allow_nan=False).encode()


def decimal_text(value):
    if isinstance(value, bool) or not isinstance(value, (int, Decimal)):
        raise ValueError('Provider numeric JSON required')
    value = Decimal(value)
    if not value.is_finite():
        raise ValueError('Finite provider quantity required')
    if abs(value) >= Decimal('1e30') or value.as_tuple().exponent < -24:
        raise ValueError('Provider precision exceeds reviewed arithmetic bounds')
    return format(value, 'f')


def normalized(row):
    if not isinstance(row, dict) or set(row) != set(FIELDS):
        raise ValueError('Explicit reviewed source schema required')
    return {key: decimal_text(value) if isinstance(value, (int, Decimal)) and not isinstance(value, bool) else value
            for key, value in row.items()}


def partitions(body, today, count=4):
    if type(count) is not int or not 1 <= count <= 12:
        raise ValueError('Reviewed settlement count required')
    doc = exact.strict(body)
    if not isinstance(doc, dict) or str(doc.get('datasetName', '')).lower() != DATASET or str(doc.get('datasetGroup', '')).lower() != 'otcmarket' or doc.get('partitionFields') != ['settlementDate']:
        raise ValueError('Reviewed settlement partition schema required')
    values = []
    for row in doc.get('availablePartitions', []):
        if not isinstance(row, dict) or set(row) != {'partitions'} or not isinstance(row['partitions'], list) or len(row['partitions']) != 1:
            raise ValueError('One settlement dimension required')
        stamp = exact.day(row['partitions'][0])
        if date.fromisoformat(stamp) > today:
            raise ValueError('Future settlement partition')
        values.append(stamp)
    if len(set(values)) != len(values) or len(values) < count + 1:
        raise ValueError('Unique full settlement boundaries required')
    values.sort(reverse=True)
    return [{'settlement_date': values[n], 'previous_settlement_date': values[n + 1]} for n in range(count)]


def request(stamp, offset):
    exact.day(stamp)
    if type(offset) is not int or not 0 <= offset <= 100000:
        raise ValueError('Reviewed offset bound required')
    return {'limit': LIMIT, 'offset': offset,
            'compareFilters': [{'fieldName': 'settlementDate', 'compareType': 'EQUAL', 'fieldValue': stamp}],
            'sortFields': list(GRAIN)}


def rows(body, stamp):
    records = exact.strict(body)
    if not isinstance(records, list):
        raise ValueError('Source array required')
    result = []
    for row in records:
        value = normalized(row)
        if value['settlementDate'] != stamp or Decimal(value['accountingYearMonthNumber']) != int(stamp.replace('-', '')):
            raise ValueError('Settlement dates disagree')
        if any(not isinstance(value[k], str) or not value[k].strip() or len(value[k]) > 500 for k in GRAIN):
            raise ValueError('Complete reported issue grain required')
        for name in ('currentShortPositionQuantity', 'previousShortPositionQuantity', 'averageDailyVolumeQuantity', 'daysToCoverQuantity'):
            number = row[name]
            decimal_text(number)
            if number < 0:
                raise ValueError('Negative position or volume')
        for name in ('changePercent', 'changePreviousNumber'):
            decimal_text(row[name])
        if row['stockSplitFlag'] not in (None, '', 'S') or row['revisionFlag'] not in (None, '', 'R'):
            raise ValueError('Unknown split or revision flag')
        result.append(value)
    return result


def record_key(row):
    return tuple(row[k] for k in GRAIN)


def fingerprint(records):
    mapping = {}
    for row in records:
        identity = record_key(row)
        if identity in mapping:
            raise ValueError('Duplicate issue within settlement population')
        mapping[identity] = row
    return hashlib.sha256(encoded([mapping[key] for key in sorted(mapping)])).hexdigest()


def describe(records):
    fingerprint(records)
    counters = {name: Counter() for name in ('exchange_codes', 'market_class_codes', 'split_flags', 'revision_flags', 'dtc_relationship', 'change_relationship')}
    sums = {name: Decimal(0) for name in ('currentShortPositionQuantity', 'previousShortPositionQuantity', 'averageDailyVolumeQuantity')}
    examples = {}
    for row in records:
        for output, field in (('exchange_codes', 'issuerServicesGroupExchangeCode'), ('market_class_codes', 'marketClassCode'), ('split_flags', 'stockSplitFlag'), ('revision_flags', 'revisionFlag')):
            counters[output][str(row[field])] += 1
        current, previous, adv, dtc, change, pct = (Decimal(row[k]) for k in ('currentShortPositionQuantity', 'previousShortPositionQuantity', 'averageDailyVolumeQuantity', 'daysToCoverQuantity', 'changePreviousNumber', 'changePercent'))
        for name in sums:
            sums[name] += Decimal(row[name])
        with localcontext() as ctx:
            ctx.prec = 60
            rounded = (current / adv).quantize(Decimal('.01'), rounding=ROUND_HALF_UP) if adv else None
            dtc_status = ('zero_adv' if adv == 0 else 'exact_rounded_ratio' if dtc == rounded else 'display_floor_one' if current / adv <= 1 and dtc == 1 else 'reported_999_99_unconfirmed_cap' if dtc == Decimal('999.99') else 'other_provider_difference')
            change_status = ('reported_difference_mismatch' if change != current - previous else 'previous_zero_percent_undefined' if previous == 0 else 'exact_rounded_percent' if pct == ((current - previous) * 100 / previous).quantize(Decimal('.01'), rounding=ROUND_HALF_UP) else 'reported_percent_difference')
        counters['dtc_relationship'][dtc_status] += 1
        counters['change_relationship'][change_status] += 1
        if dtc_status not in ('exact_rounded_ratio', 'display_floor_one') or change_status not in ('exact_rounded_percent', 'previous_zero_percent_undefined'):
            key = dtc_status + ':' + change_status
            if len(examples.setdefault(key, [])) < 3:
                examples[key].append(row)
    return {'rows': len(records), 'population_sha256': fingerprint(records),
            'symbol_collisions': sum(n - 1 for n in Counter(r['symbolCode'] for r in records).values()),
            'quantity_sums_not_economic_aggregates': {key: decimal_text(value) for key, value in sums.items()},
            **{key: dict(sorted(value.items())) for key, value in counters.items()},
            'diagnostic_examples': examples, 'population_arithmetic_qualified': False,
            'snapshot_atomic': False, 'historical_availability_verified': False,
            'security_continuity_verified': False, 'forecast_qualified': False, 'sizing_qualified': False}


def rounded(value):
    if value is None:
        return None
    with localcontext() as ctx:
        ctx.prec = 128
        answer = value.quantize(Decimal('0.000000000001'), rounding=ROUND_HALF_EVEN)
        return format(abs(answer) if answer == 0 else answer, 'f')


def quotient(numerator, denominator, scale=1):
    if denominator == 0:
        return None
    with localcontext() as ctx:
        ctx.prec = 128
        ctx.rounding = ROUND_HALF_EVEN
        return rounded(numerator * scale / denominator)


def measurement(row, previous_settlement, source_index, source_row):
    """Keep provider conventions distinct from ratios of its reported quantities."""
    if type(source_index) is not int or source_index < 0 or type(source_row) is not int or source_row < 0:
        raise ValueError('Exact page and zero-based row required')
    stamp = exact.day(row['settlementDate'])
    prior = exact.day(previous_settlement)
    if prior >= stamp:
        raise ValueError('Prior advertised settlement boundary required')
    current, previous, adv, reported_dtc, reported_delta, reported_pct = (Decimal(row[k]) for k in (
        'currentShortPositionQuantity', 'previousShortPositionQuantity', 'averageDailyVolumeQuantity',
        'daysToCoverQuantity', 'changePreviousNumber', 'changePercent'))
    with localcontext() as ctx:
        ctx.prec = 128
        delta = current - previous
        rounded_percent = ((delta * 100 / previous).quantize(Decimal('.01'), rounding=ROUND_HALF_UP) if previous else None)
        rounded_dtc = ((current / adv).quantize(Decimal('.01'), rounding=ROUND_HALF_UP) if adv else None)
    dtc_status = ('unavailable_zero_reported_adv' if not adv else
                  'matches_reconstructed_rounded_ratio' if reported_dtc == rounded_dtc else
                  'provider_display_floor_one' if current <= adv and reported_dtc == 1 else
                  'provider_999_99_convention_unconfirmed' if reported_dtc == Decimal('999.99') else
                  'provider_differs_from_reconstructed_ratio')
    return {
        'settlement_date': stamp, 'previous_settlement_date': prior,
        'source_index': source_index, 'source_row': source_row, 'source_fields': row,
        'short_interest_shares': row['currentShortPositionQuantity'],
        'reported_previous_short_interest_shares': row['previousShortPositionQuantity'],
        'reported_change_shares': row['changePreviousNumber'], 'computed_change_shares': decimal_text(delta),
        'change_shares_reconciled': reported_delta == delta,
        'reported_change_pct': row['changePercent'], 'computed_change_pct': quotient(delta, previous, 100),
        'change_pct_reconciliation': 'undefined_previous_zero' if previous == 0 else
                                     'matches_rounded_formula' if reported_pct == rounded_percent else
                                     'provider_differs_from_rounded_formula',
        'reported_average_daily_volume_shares': row['averageDailyVolumeQuantity'],
        'average_volume_window': {'first_date': (date.fromisoformat(prior) + timedelta(days=1)).isoformat(), 'last_date': stamp,
                                 'provider_window_definition': 'trading_days_in_interval_with_provider_split_adjustments',
                                 'underlying_daily_volumes_reconstructed': False},
        'reported_days_to_cover': row['daysToCoverQuantity'],
        'reconstructed_position_to_reported_adv_days': quotient(current, adv),
        'position_to_adv_exact_fraction': str(Fraction(current) / Fraction(adv)) if adv else None,
        'days_to_cover_status': dtc_status,
        'stock_split_flag': row['stockSplitFlag'], 'revision_flag': row['revisionFlag'],
        'split_adjusted_comparison_verified': False, 'covering_inferred': False, 'direction_inferred': False,
        'original_publication_at': None, 'historical_availability_verified': False,
        'signal': None, 'score': None,
    }


DERIVED = ('computed_change_shares', 'computed_change_pct', 'reconstructed_position_to_reported_adv_days',
           'position_to_adv_exact_fraction', 'days_to_cover_status', 'change_pct_reconciliation', 'change_shares_reconciled')
POINT_FIELDS = ('source_index', 'source_row', *FIELDS, *DERIVED,
                'matched_prior_record_shares', 'matches_reported_previous_quantity')


def compact_point(row, previous_settlement, source_index, source_row):
    value = measurement(row, previous_settlement, source_index, source_row)
    return [source_index, source_row, *(row[k] for k in FIELDS), *(value[k] for k in DERIVED), None, None]


def source_fields(point):
    if not isinstance(point, list) or len(point) != len(POINT_FIELDS):
        raise ValueError('Complete compact short-position observation required')
    return dict(zip(FIELDS, point[2:2 + len(FIELDS)]))


def history(identity, points, dates):
    if not points or dates != sorted(set(dates)):
        raise ValueError('Ordered settlement history required')
    if tuple(identity) != record_key(source_fields(points[0])):
        raise ValueError('Reported issue identity differs')
    by_date = {}
    for point in points:
        row = source_fields(point)
        stamp = row['settlementDate']
        if stamp not in dates or stamp in by_date or tuple(identity) != record_key(row):
            raise ValueError('Unique issue/date history required')
        by_date[stamp] = point
    result = []
    for stamp in sorted(by_date):
        point = list(by_date[stamp])
        i = dates.index(stamp)
        prior = by_date.get(dates[i - 1]) if i else None
        prior_value = source_fields(prior)['currentShortPositionQuantity'] if prior else None
        reconciled = (Decimal(prior_value) == Decimal(source_fields(point)['previousShortPositionQuantity'])) if prior else None
        point[-2:] = [prior_value, reconciled]
        result.append(point)
    return {'identity': dict(zip(GRAIN, identity)), 'identity_basis': 'exact_reported_symbol_name_exchange_market_class',
            'security_identity_continuity_verified': False, 'dates': dates,
            'latest_settlement_present': dates[-1] in by_date,
            'missing_settlements': [stamp for stamp in dates if stamp not in by_date],
            'observations': result, 'call': None, 'score': None, 'signal': None,
            'calls_eligible': False, 'sizing_eligible': False, 'execution_eligible': False, 'forecast_qualified': False}
