"""Exact, dated bill-supply arithmetic over a retained input ledger; no I/O.

The overlapping 28/365-day windows and legacy thresholds are descriptive,
not an annual growth rate, monetary injection or validated forecast.
"""
from bisect import bisect_left, bisect_right
from collections import Counter
from datetime import date, timedelta
from decimal import Decimal, InvalidOperation
from fractions import Fraction
import hashlib
import json
import math
import re

from treasury_instruments import instrument_fields

CONTRACT = 'auction-issuance-inputs.v1'
FIELDS = ('auction_date', 'cusip', 'security_type', 'security_term',
          'inflation_index_security', 'floating_rate', 'total_accepted')
PERMISSIONS = {key: False for key in ('calls_eligible', 'sizing_eligible',
                                    'forecast_eligible', 'execution_eligible',
                                    'historical_point_in_time_verified')}


def day(value):
    if type(value) is not str or not re.fullmatch(r'\d{4}-\d{2}-\d{2}', value):
        return None
    try:
        return date.fromisoformat(value)
    except ValueError:
        return None


def amount(value):
    if type(value) not in (str, int, float):
        return None
    text = str(value).strip()
    if len(text) > 128 or not re.fullmatch(r'[+]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][-+]?\d+)?', text):
        return None
    try:
        number = Decimal(text)
        if not number.is_finite() or number < 0 or not math.isfinite(float(number)) or abs(number.as_tuple().exponent) > 400:
            return None
        return Fraction(number)
    except (InvalidOperation, OverflowError, ValueError):
        return None


def exact(value):
    return {'numerator': str(value.numerator), 'denominator': str(value.denominator)} if value is not None else None


def display(value):
    try:
        number = float(value)
        return number if math.isfinite(number) else None
    except (TypeError, ValueError, OverflowError):
        return None


def coverage_contains(intervals, start, end):
    """Require complete acquired dates, not merely min/max observed auctions."""
    cursor = start
    spans = sorted((day(r.get('start')), day(r.get('end'))) for r in intervals
                   if isinstance(r, dict) and day(r.get('start')) and day(r.get('end')))
    for lo, hi in spans:
        if lo > hi:
            continue
        if lo > cursor:
            return False
        if hi >= cursor:
            if hi >= end:
                return True
            cursor = hi + timedelta(days=1)
    return False


class IssuanceLedger:
    """Prepare once: O(N log N), then O(log N) windows; serialize rows once."""
    def __init__(self, records, as_of, coverage=()):
        self.as_of = day(as_of)
        if self.as_of is None or not isinstance(records, (list, tuple)):
            raise ValueError('Dated full input list required')
        rows = []
        for ordinal, source in enumerate(records):
            if not isinstance(source, dict):
                raise ValueError('Every supplied auction must be an object')
            raw = {key: source.get(key) for key in FIELDS}
            identity = instrument_fields(raw, 'fiscaldata')
            observed = day(raw['auction_date'])
            rows.append({'source_ordinal': ordinal, 'source_fields': raw,
                         'date': observed.isoformat() if observed else None,
                         'instrument_kind': identity['instrument_kind'],
                         'amount_status': 'observed' if amount(raw['total_accepted']) is not None else 'missing_or_invalid'})
        rows.sort(key=lambda row: (row['date'] or '', row['source_ordinal']))
        self.rows = rows
        self.dates = [row['date'] or '' for row in rows]
        self.invalid_dates = sum(row['date'] is None or row['date'] > as_of for row in rows)
        self.coverage = list(coverage)
        keys = [(r['date'], r['source_fields']['cusip']) for r in rows]
        duplicates = Counter((d, c) for d, c in keys if type(c) is str and c.strip())
        self.prefix = {key: [0] for key in ('bill', 'unknown', 'missing_amount', 'invalid_identity', 'duplicate', 'amount')}
        bill_days = set()
        for row in rows:
            raw = row['source_fields']; bill = row['instrument_kind'] == 'BILL'
            value = amount(raw['total_accepted'])
            key = (row['date'], raw['cusip'])
            identity_ok = type(raw['cusip']) is str and bool(raw['cusip'].strip())
            counts = {'bill': int(bill), 'unknown': int(row['instrument_kind'] == 'UNKNOWN'),
                      'missing_amount': int(bill and value is None), 'invalid_identity': int(not identity_ok),
                      'duplicate': int(identity_ok and duplicates[key] > 1),
                      'amount': value if bill and value is not None else Fraction(0)}
            row['duplicate_identity'] = bool(counts['duplicate'])
            for key, value in counts.items():
                self.prefix[key].append(self.prefix[key][-1] + value)
            if bill and row['date']:
                bill_days.add(row['date'])
        self.bill_days = sorted(bill_days)
        payload = {'contract': CONTRACT, 'as_of': as_of, 'coverage': self.coverage, 'rows': rows}
        self.ledger_id = hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()).hexdigest()
        self.document = {**payload, 'ledger_id': self.ledger_id,
                         'identity': 'sha256 of canonical contract/as_of/coverage/rows; integrity only',
                         'row_count': len(rows), 'invalid_date_count': self.invalid_dates,
                         'basis': 'current source vintage; exact fields consumed by the comparison, not a point-in-time backtest', **PERMISSIONS}

    def window(self, end, lookback):
        start = end - timedelta(days=lookback)
        lo, hi = start.isoformat(), end.isoformat()
        a, b = bisect_left(self.dates, lo), bisect_right(self.dates, hi)
        counts = {key: values[b] - values[a] for key, values in self.prefix.items()}
        days = bisect_right(self.bill_days, hi) - bisect_left(self.bill_days, lo)
        problems = [key for key in ('unknown', 'missing_amount', 'invalid_identity', 'duplicate') if counts[key]]
        if self.invalid_dates or end > self.as_of:
            problems.append('invalid_or_future_date')
        covered = coverage_contains(self.coverage, start, end)
        if not covered:
            problems.append('unverified_population_coverage')
        total = counts.pop('amount')
        complete = not problems
        mean = total / days if complete and days else None
        return {'start': lo, 'end': hi, 'inclusive': True, 'lookback_days': lookback,
                'calendar_dates': lookback + 1, 'ledger_id': self.ledger_id,
                'source_slice': [a, b], 'source_slice_rule': 'zero-based [start,end) in the single retained ledger; includes explicitly excluded non-bills',
                'status': 'complete' if complete else 'unavailable', 'problems': problems,
                'population_coverage_verified': covered, 'rows': b - a, 'counts': counts,
                'auction_days': days, 'known_sum_usd': exact(total),
                'sum_usd': exact(total) if complete else None,
                'sum_usd_bn': display(total / 10**9) if complete else None,
                'average_per_auction_day_usd': exact(mean),
                'average_per_auction_day_usd_bn': display(mean / 10**9) if mean is not None else None}, mean

    def calculate(self, as_of, include_ledger=False):
        end = day(as_of)
        if end is None:
            raise ValueError('Exact calculation date required')
        recent, avg_recent = self.window(end, 28)
        baseline, avg_base = self.window(end, 365)
        reasons = sorted(set(recent['problems'] + baseline['problems']))
        status = 'unavailable'
        pct = score = None
        if not reasons:
            if baseline['counts']['bill'] == 0:
                status, score = 'not_applicable', 0
            elif not recent['auction_days']:
                reasons.append('no_recent_bill_auction_days')
            elif avg_base == 0:
                reasons.append('zero_baseline')
            else:
                pct = (avg_recent / avg_base - 1) * 100
                if display(pct) is None:
                    reasons.append('nonfinite_display')
                else:
                    status = 'complete'
                    score = 90 if pct > 50 else 60 if pct > 30 else 30 if pct > 15 else 0
        out = {'contract': CONTRACT, 'calculation_as_of': as_of, 'status': status,
               'reasons': reasons, 'score': score,
               'pct_above_baseline': round(display(pct), 1) if status == 'complete' else None,
               'pct_exact': exact(pct), 'ledger_id': self.ledger_id,
               'recent': recent, 'baseline': baseline,
               'unit': 'usd', 'denominator': 'distinct bill auction dates, including measured-zero days',
               'formula': '(recent mean accepted amount per bill auction day / overlapping baseline mean - 1) * 100',
               'thresholds': [{'strictly_above_pct': 50, 'score': 90}, {'strictly_above_pct': 30, 'score': 60},
                              {'strictly_above_pct': 15, 'score': 30}, {'otherwise_score': 0}],
               'overlay_coefficient': .15, 'maximum_overlay_points': 13.5,
               'validation': 'unqualified_legacy_supply_heuristic', **PERMISSIONS}
        if include_ledger:
            out['ledger'] = self.document
        return out


def apply_overlay(base, issuance):
    base_ok = type(base) in (int, float) and math.isfinite(base) and 0 <= base <= 100
    valid = issuance.get('contract') == CONTRACT and issuance.get('status') in ('complete', 'not_applicable')
    score = issuance.get('score')
    valid = valid and type(score) in (int, float) and score in (0, 30, 60, 90)
    adjustment = score * .15 if valid else None
    combined = round(min(100, base + adjustment), 1) if valid and base_ok else None
    return {'contract': CONTRACT, 'status': 'complete' if combined is not None else 'unavailable',
            'base_composite': base if base_ok else None, 'issuance_score': score if valid else None,
            'adjustment_points': adjustment, 'composite': combined, 'cap': 100,
            'formula': 'round(min(100, base_composite + 0.15 * issuance_score), 1)',
            'issuance_status': issuance.get('status'), 'ledger_id': issuance.get('ledger_id'), **PERMISSIONS}


def replay(document, as_of=None):
    """Rebuild all normalized rows and integrity identity before replaying data.

    This consumes JSON data only. It never executes source from an artifact.
    """
    if not isinstance(document, dict) or document.get('contract') != CONTRACT or not isinstance(document.get('rows'), list):
        raise ValueError('Complete issuance ledger required')
    rows = document['rows']
    if any(not isinstance(row, dict) or type(row.get('source_ordinal')) is not int for row in rows):
        raise ValueError('Every retained occurrence requires its original ordinal')
    ordered = sorted(rows, key=lambda row: row['source_ordinal'])
    if [row['source_ordinal'] for row in ordered] != list(range(len(rows))):
        raise ValueError('Missing or duplicate source occurrence')
    rebuilt = IssuanceLedger([row['source_fields'] for row in ordered], document['as_of'], document['coverage'])
    if rebuilt.document != document:
        raise ValueError('Ledger fields, identity or normalized rows do not reproduce')
    return rebuilt.calculate(as_of or document['as_of'])
