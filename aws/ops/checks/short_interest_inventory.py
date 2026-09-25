"""Exact source inventory, not investment-signal qualification.

Two complete equal scans bound observed pagination stability. They do not prove
an atomic provider snapshot, historical availability, or security continuity.
"""
from collections import Counter
from datetime import date
from decimal import Decimal, localcontext, ROUND_HALF_UP
import hashlib, json, re
import offexchange_measurements as exact

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
