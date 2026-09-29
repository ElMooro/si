"""Complete, inspectable participation arithmetic over supplied auction rows.

These fixed legacy grades are descriptive. Canonical instrument fields and
current-vintage histories do not prove original-source or predictive validity.
"""
from datetime import date
import math
import statistics

CONTRACT = 'auction-participation-inputs.v1'
WEIGHTS = {'btc': 1.0, 'indirect': .8, 'pd': -.8}
FLAGS = {key: False for key in ('source_capture_verified', 'historical_point_in_time_verified',
                              'forecast_eligible', 'calls_eligible', 'sizing_eligible', 'execution_eligible')}


def number(value):
    if type(value) not in (int, float):
        return None
    try:
        return float(value) if math.isfinite(value) else None
    except (ValueError, OverflowError):
        return None


def observation_day(value):
    if type(value) is not str or len(value) != 10:
        return None
    try:
        result = date.fromisoformat(value)
        return result if result.isoformat() == value else None
    except ValueError:
        return None


def cohort(row):
    kind = row.get('instrument_kind')
    if row.get('instrument_contract') != 'treasury-instrument.v1' or row.get('instrument_classification_status') != 'verified':
        return None
    if kind not in ('BILL', 'NOMINAL_COUPON', 'TIPS', 'FRN') or type(row.get('term')) is not str or not row['term'].strip() or type(row.get('reopening')) is not bool:
        return None
    return kind, row['term'], row['reopening']


def shares(row):
    inputs = {key: row.get(key) for key in ('pd', 'direct', 'indirect')}
    values = {key: number(value) for key, value in inputs.items()}
    problems = [key for key, value in values.items() if value is None or value < 0]
    try:
        denominator = math.fsum(values.values()) if not problems else None
    except (ValueError, OverflowError):
        denominator = None
    if denominator is None or not math.isfinite(denominator) or denominator <= 0:
        status = 'missing_or_invalid_bidder_amount' if problems else 'no_positive_bidder_total'
        denominator = None
    else:
        status = 'complete'
    trace = {'status': status, 'accepted_bidder_amounts_usd': inputs,
             'reported_competitive_accepted_usd': row.get('competitive_accepted'),
             'reported_total_accepted_usd': row.get('total_accepted'),
             'denominator_usd': denominator,
             'denominator_basis': 'sum of all three observed nonnegative accepted bidder categories; never substitute gross issuance',
             'missing_or_invalid': problems}
    out = {key+'_pct': round((value / denominator) * 100, 1) if denominator is not None else None
           for key, value in values.items()}
    return {**out, 'bidder_share_inputs': trace}


def standardize(value, history):
    current = number(value)
    values = [number(item) for item in history]
    missing = [index for index, item in enumerate(values) if item is None]
    reason = ('missing_current' if current is None else 'missing_history' if missing else
              'insufficient_history' if len(values) < 4 else None)
    mean = sigma = result = None
    if reason is None:
        try:
            mean = statistics.fmean(values)
            sigma = statistics.pstdev(values)
            if not math.isfinite(mean) or not math.isfinite(sigma):
                reason = 'nonfinite_aggregate'
            elif sigma < 1e-9:
                reason = 'zero_variance'
            else:
                result = (current - mean) / sigma
                if not math.isfinite(result):
                    result = None
                    reason = 'nonfinite_standardized_value'
        except (ValueError, OverflowError):
            reason = 'nonfinite_aggregate'
    return {'status': 'complete' if reason is None else 'unavailable', 'reason': reason,
            'current': value, 'history': list(history), 'n': len(values), 'missing_history_indices': missing,
            'mean': mean if number(mean) is not None else None,
            'population_standard_deviation': sigma if number(sigma) is not None else None,
            'z': round(result, 2) if result is not None else None,
            'formula': '(current - mean of every supplied cohort observation) / population standard deviation; round z to two decimals'}


def grade(current, prior):
    """Use every row of the declared cohort, one fixed three-feature denominator."""
    rows = [current, *prior]
    inputs = []
    problems = []
    identity = cohort(current)
    current_day = observation_day(current.get('auction_date'))
    if identity is None:
        problems.append('unknown_current_cohort')
    if not 4 <= len(prior) <= 12:
        problems.append('cohort_size_outside_4_to_12')
    seen = set()
    for index, row in enumerate(rows):
        observed = observation_day(row.get('auction_date'))
        cusip = row.get('cusip')
        valid_identity = type(cusip) is str and bool(cusip.strip()) and observed is not None
        key = (row.get('auction_date'), cusip) if valid_identity else None
        if not valid_identity or key in seen:
            problems.append('invalid_or_duplicate_auction_identity')
        if key is not None:
            seen.add(key)
        if cohort(row) != identity or (index and (current_day is None or observed is None or observed >= current_day)):
            problems.append('incomparable_or_unordered_cohort')
        values = shares(row)
        btc = number(row.get('btc'))
        if btc is not None and btc < 0:
            btc = None
        inputs.append({'auction_date': row.get('auction_date'), 'cusip': cusip,
                       'instrument_kind': row.get('instrument_kind'), 'instrument_contract': row.get('instrument_contract'),
                       'instrument_classification_status': row.get('instrument_classification_status'),
                       'term': row.get('term'), 'reopening': row.get('reopening'),
                       'btc': btc, 'btc_original': row.get('btc'),
                       'indirect': values['indirect_pct'], 'pd': values['pd_pct'],
                       'bidder_share_inputs': values['bidder_share_inputs']})
    traces = {key: standardize(inputs[0][key], [row[key] for row in inputs[1:]]) for key in WEIGHTS}
    missing = [key for key, trace in traces.items() if trace['status'] != 'complete']
    complete = not problems and not missing
    score = None
    if complete:
        try:
            score = round(math.fsum(WEIGHTS[key] * traces[key]['z'] for key in WEIGHTS) / 3, 2)
        except (ValueError, OverflowError):
            problems.append('nonfinite_grade_aggregate'); complete = False
    letter = ('A' if score >= 1 else 'B' if score >= .4 else 'C' if score >= -.4 else 'D' if score >= -1 else 'F') if score is not None else 'n/a'
    return {'contract': CONTRACT, 'status': 'complete' if complete else 'unavailable',
            'problems': sorted(set(problems)), 'missing_features': missing,
            'cohort': list(identity) if identity else None, 'current': inputs[0], 'prior': inputs[1:],
            'features': traces, 'weights': dict(WEIGHTS), 'denominator': 3,
            'formula': 'round((z_btc + 0.8*z_indirect - 0.8*z_pd) / 3, 2); every feature and cohort observation required',
            'score': score, 'grade': letter,
            'source_vintage': 'current supplied bank; historical first-publication availability unverified', **FLAGS}


def classify(auctions, buybacks):
    """No unknown grade or instrument can silently disappear from a day's class."""
    classes, problems = [], []
    coupons, bills = [], []
    for index, row in enumerate(auctions):
        identity = cohort(row)
        if identity is None:
            problems.append({'auction_index': index, 'reason': 'unknown_instrument_cohort'})
        elif identity[0] == 'BILL':
            bills.append(row)
        else:
            coupons.append(row)
    for index, row in enumerate(auctions):
        identity = cohort(row)
        if identity is None or identity[0] == 'BILL':
            continue  # Bills-only is an instrument class, not a participation grade.
        trace = row.get('grading_inputs') or {}
        if trace.get('contract') != CONTRACT or trace.get('status') != 'complete' or row.get('grade') != trace.get('grade') or row.get('grade') not in ('A', 'B', 'C', 'D', 'F'):
            problems.append({'auction_index': index, 'reason': 'unavailable_grade'})
    for index, row in enumerate(buybacks):
        accepted, maximum = number(row.get('accepted')), number(row.get('max_par'))
        if accepted is None or maximum is None or accepted < 0 or maximum <= 0 or accepted > maximum:
            problems.append({'buyback_index': index, 'reason': 'missing_or_invalid_fill_inputs'})
        elif maximum >= 5e9 and accepted / maximum >= .9:
            if 'buyback_strong' not in classes:
                classes.append('buyback_strong')
    if problems:
        classes.append('unclassified')
    elif coupons:
        letters = [row['grade'] for row in coupons]
        classes.append('coupon_strong' if all(letter in ('A', 'B') for letter in letters) else
                       'coupon_weak' if any(letter in ('D', 'F') for letter in letters) else 'coupon_mixed')
    elif bills:
        classes.append('bills_only')
    elif not classes:
        classes.append('buyback_other' if buybacks else 'none')
    # A verified operation-size tag is context, but an incomplete day may not
    # enter a selected historical return cohort through that tag alone.
    return {'contract': CONTRACT, 'status': 'complete' if not problems else 'unavailable',
            'classes': classes, 'cohort_classes': classes if not problems else ['unclassified'],
            'problems': problems, 'auction_count': len(auctions), 'coupon_count': len(coupons),
            'bill_count': len(bills), 'buyback_count': len(buybacks),
            'classification_rule': 'All supplied operations retained; complete comparable grades and buyback fill inputs required.', **FLAGS}
