"""Dated CNS balance comparisons; no fail flow, age or short-sale inference."""
from decimal import Decimal, localcontext, ROUND_HALF_EVEN
import hashlib, json, re

POINT_FIELDS = ('source_index', 'source_line', 'settlement_date', 'reported_symbol', 'reported_description',
                'fail_balance_shares', 'reported_previous_day_price', 'previous_reported_date',
                'previous_record_present', 'previous_fail_balance_shares',
                'adjacent_balance_change_shares', 'adjacent_balance_change_pct', 'comparison_status')
FLAGS = ('calls_eligible', 'sizing_eligible', 'execution_eligible', 'forecast_qualified')


def encoded(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False, allow_nan=False).encode()


def record_id(cusip):
    if not isinstance(cusip, str) or not re.fullmatch(r'[A-Z0-9*@#]{9}', cusip):
        raise ValueError('Exact reported CUSIP required')
    return hashlib.sha256(encoded(['SEC_CNS_REPORTED_CUSIP', cusip])).hexdigest()


def compact_point(row, source_index):
    if type(source_index) is not int or source_index < 0 or type(row['source_line']) is not int or row['source_line'] < 2:
        raise ValueError('Exact archive and one-based source row required')
    return [source_index, row['source_line'], row['settlement_date'], row['symbol'], row['description'],
            row['fail_balance_shares'], row['reported_price'], None, False, None, None, None, 'not_evaluated']


def percentage(delta, previous):
    if not previous:
        return None
    with localcontext() as ctx:
        ctx.prec = max(128, len(str(abs(delta))) + len(str(previous)) + 32)
        value = (Decimal(delta) * 100 / Decimal(previous)).quantize(Decimal('0.000000000001'), rounding=ROUND_HALF_EVEN)
        return format(abs(value) if value == 0 else value, 'f')


def history(cusip, points, dates):
    record_id(cusip)
    if not points or dates != sorted(set(dates)):
        raise ValueError('Complete ordered reported-date population required')
    by_date = {}
    for point in points:
        if not isinstance(point, list) or len(point) != len(POINT_FIELDS):
            raise ValueError('Complete typed balance observation required')
        stamp = point[2]
        if stamp not in dates or stamp in by_date:
            raise ValueError('Unique reported CUSIP/settlement required')
        if not isinstance(point[5], str) or not re.fullmatch('[0-9]+', point[5]):
            raise ValueError('Exact nonnegative share quantity required')
        by_date[stamp] = point
    preceding = {stamp: dates[i - 1] if i else None for i, stamp in enumerate(dates)}
    result, labels = [], set()
    for stamp in sorted(by_date):
        point = list(by_date[stamp])
        label = (point[3], point[4])
        labels.add(label)
        previous_date = preceding[stamp]
        prior = by_date.get(previous_date)
        point[7:10] = [previous_date, prior is not None, prior[5] if prior is not None else None]
        point[10:12] = [None, None]
        if previous_date is None:
            status = 'no_preceding_date_in_selected_archives'
        elif prior is None:
            status = 'prior_cusip_record_not_reported'
        elif label != (prior[3], prior[4]):
            status = 'reported_label_changed_comparison_unqualified'
        else:
            current, previous = int(point[5]), int(prior[5])
            delta = current - previous
            point[10], point[11] = str(delta), percentage(delta, previous)
            status = 'reported_previous_balance_zero' if previous == 0 else 'adjacent_reported_balances_only'
        point[12] = status
        result.append(point)
    return {'reported_cusip': cusip, 'identity_basis': 'literal_reported_cusip_not_a_verified_security_master',
            'security_identity_continuity_verified': False,
            'reported_labels': [{'symbol': symbol, 'description': description} for symbol, description in sorted(labels)],
            'dates': dates, 'missing_reported_dates': [stamp for stamp in dates if stamp not in by_date],
            'latest_settlement_present': dates[-1] in by_date, 'observations': result,
            'missing_records_imputed_zero': False, 'balances_summed_across_dates': False,
            'call': None, 'signal': None, 'score': None, **dict.fromkeys(FLAGS, False)}
