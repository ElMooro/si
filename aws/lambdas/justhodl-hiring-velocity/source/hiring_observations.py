"""Replayable reported workforce observations. No organic-hiring or return model."""
from datetime import date, datetime, timezone
from decimal import Decimal
import base64
import hashlib
import json
import math
import re

CONTRACT = 'hiring-statement-observations.v1'
COUNT_FIELDS = ('employeeCount', 'fullTimeEmployees', 'employees')
ANNUAL_FORMS = {'10-K', '20-F', '40-F'}


def number(value):
    if type(value) not in (int, float):
        return None
    try:
        return value if math.isfinite(value) and abs(value) <= 2**53-1 else None
    except OverflowError:
        return None


def strict(raw):
    def pairs(items):
        out = {}
        for key, value in items:
            if key in out:
                raise ValueError('Duplicate JSON member')
            out[key] = value
        return out
    def real(value):
        out = float(value)
        if not math.isfinite(out) or (out == 0 and Decimal(value) != 0):
            raise ValueError('Unrepresentable JSON number')
        return out
    def constant(_):
        raise ValueError('Nonfinite JSON')
    return json.loads(raw.decode('utf-8'), object_pairs_hook=pairs,
                      parse_float=real, parse_constant=constant)


def day(value):
    try:
        if not isinstance(value, str) or len(value) != 10:
            return None
        result = date.fromisoformat(value)
        return result if result.isoformat() == value else None
    except ValueError:
        return None


def clock(value):
    try:
        if not isinstance(value, str):
            return None
        result = datetime.fromisoformat(value.replace('Z', '+00:00'))
        return result.astimezone(timezone.utc) if result.tzinfo else None
    except ValueError:
        return None


def cik(value):
    if not isinstance(value, str) or not re.fullmatch(r'\d{1,10}', value):
        return None
    return value.zfill(10) if int(value) else None


def envelope(raw, received_at, endpoint):
    if clock(received_at) is None:
        raise ValueError('Explicit receipt clock required')
    out = {'status': 'received', 'endpoint': endpoint, 'received_at': received_at,
           'original_base64': base64.b64encode(raw).decode(),
           'original_sha256': hashlib.sha256(raw).hexdigest(), 'original_bytes': len(raw)}
    try:
        strict(raw)
    except (ValueError, UnicodeError, RecursionError):
        out['status'] = 'invalid_original'
    return out


def original(acquisition):
    if acquisition.get('status') not in ('received', 'invalid_original'):
        return None
    raw = base64.b64decode(acquisition['original_base64'], validate=True)
    if (type(acquisition.get('original_bytes')) is not int or len(raw) != acquisition['original_bytes']
            or hashlib.sha256(raw).hexdigest() != acquisition['original_sha256']):
        raise ValueError('Whole original response differs')
    try:
        parsed = strict(raw)
    except (ValueError, UnicodeError, RecursionError):
        if acquisition['status'] != 'invalid_original':
            raise
        return None
    if acquisition['status'] == 'invalid_original':
        raise ValueError('Invalid-source classification differs')
    return parsed


def employee_rows(symbol, acquisition, checked_as_of):
    today = day(checked_as_of)
    if today is None:
        raise ValueError('Explicit check date required')
    parsed = original(acquisition)
    if not isinstance(parsed, list):
        return []
    rows = []
    for index, raw in enumerate(parsed):
        item = raw if isinstance(raw, dict) else {}
        report = day(item.get('periodOfReport'))
        filed = day(item.get('filingDate'))
        form = item.get('formType')
        family = form.removesuffix('/A') if isinstance(form, str) else None
        fields = [key for key in COUNT_FIELDS if key in item]
        count = number(item.get(fields[0])) if len(fields) == 1 else None
        if count is not None and (count < 0 or int(count) != count):
            count = None
        issues = []
        if not isinstance(raw, dict): issues.append('non_object_record')
        if item.get('symbol') != symbol: issues.append('issuer_symbol_mismatch_or_missing')
        if cik(item.get('cik')) is None: issues.append('issuer_cik_missing_or_invalid')
        if report is None: issues.append('report_period_missing_or_invalid')
        elif report > today: issues.append('future_report_period')
        if count is None: issues.append('count_missing_invalid_or_ambiguous_definition')
        if 'filingDate' in item and filed is None: issues.append('filing_date_invalid')
        if filed and (filed > today or (report and filed < report)): issues.append('filing_date_inconsistent')
        accepted = item.get('acceptanceTime', item.get('acceptedDate'))
        accepted_clock = clock(accepted)
        accepted_day = day(accepted[:10]) if isinstance(accepted, str) else None
        accepted_valid = False
        if isinstance(accepted, str):
            try:
                datetime.fromisoformat(accepted.replace('Z', '+00:00'))
                accepted_valid = len(accepted) >= 19
            except ValueError:
                pass
        if accepted is not None and not accepted_valid: issues.append('acceptance_time_invalid')
        if accepted_day and (accepted_day > today or (report and accepted_day < report)):
            issues.append('acceptance_time_inconsistent')
        if accepted_clock and clock(acquisition.get('received_at')) and accepted_clock > clock(acquisition['received_at']):
            issues.append('acceptance_after_receipt')
        rows.append({'source_index': index, 'source_pointer': '/'+str(index), 'raw': raw,
                     'symbol': symbol, 'reported_cik': cik(item.get('cik')),
                     'report_period_end': report.isoformat() if report else None,
                     'filing_date': item.get('filingDate'), 'acceptance_time': accepted,
                     'acceptance_timezone_status': 'explicit' if accepted_clock else 'unknown_not_assumed',
                     'reported_form': form, 'annual_form_family': family if family in ANNUAL_FORMS else None,
                     'count_field': fields[0] if len(fields) == 1 else None, 'employee_count': count,
                     'unit': 'reported_persons', 'received_at': acquisition.get('received_at'),
                     'measurement_status': 'reported_count' if not issues else 'unqualified_record',
                     'issues': issues, 'organic_hiring_verified': False, 'first_publication_at': None})
    return rows


def annual_changes(rows):
    # Never deduplicate conflicting/repeated report periods or infer time from array offsets.
    periods = {}
    for row in rows:
        periods.setdefault(row['report_period_end'], []).append(row)
    valid = [r for r in rows if r['measurement_status'] == 'reported_count'
             and r['annual_form_family'] and len(periods[r['report_period_end']]) == 1]
    results = []
    for current in rows:
        identity = lambda r: (r['symbol'], r['reported_cik'], r['count_field'], r['annual_form_family'])
        matches = [old for old in valid if current in valid and identity(old) == identity(current)
                   and 350 <= (day(current['report_period_end'])-day(old['report_period_end'])).days <= 380]
        old = matches[0] if len(matches) == 1 else None
        change = current['employee_count']-old['employee_count'] if old else None
        pct = (change/old['employee_count']*100) if old and old['employee_count'] > 0 else None
        results.append({'source_index': current['source_index'], 'prior_source_index': old['source_index'] if old else None,
                        'period_end': current['report_period_end'], 'prior_period_end': old['report_period_end'] if old else None,
                        'elapsed_days': (day(current['report_period_end'])-day(old['report_period_end'])).days if old else None,
                        'reported_count_change': change, 'annual_interval_change_pct': pct,
                        'status': 'aligned_reported_counts' if old else 'no_unique_comparable_annual_interval',
                        'organic_hiring_verified': False})
    return results


def revenue_ratios(symbol, rows, acquisition, checked_as_of):
    parsed = original(acquisition)
    if not isinstance(parsed, list):
        return []
    result = []
    for index, raw in enumerate(parsed):
        item = raw if isinstance(raw, dict) else {}
        period = day(item.get('date')); currency = item.get('reportedCurrency')
        rev = number(item.get('revenue')); issuer = cik(item.get('cik'))
        issues = []
        if item.get('symbol') != symbol or issuer is None: issues.append('issuer_unresolved')
        if period is None or period > day(checked_as_of): issues.append('period_invalid_or_future')
        if item.get('period') != 'FY': issues.append('not_explicit_annual_income')
        if not isinstance(currency, str) or not re.fullmatch(r'[A-Z]{3}', currency): issues.append('currency_unknown')
        if rev is None: issues.append('revenue_missing_or_invalid')
        filed = item.get('filingDate', item.get('fillingDate'))
        filed_day = day(filed)
        if filed is not None and (filed_day is None or filed_day > day(checked_as_of) or (period and filed_day < period)):
            issues.append('filing_date_inconsistent')
        duplicates = [r for r in parsed if isinstance(r, dict) and r.get('date') == item.get('date')]
        matches = [r for r in rows if r['report_period_end'] == item.get('date')]
        count = matches[0] if len(matches) == 1 else None
        if len(duplicates) != 1: issues.append('ambiguous_income_period')
        if (count is None or count['measurement_status'] != 'reported_count' or count['reported_cik'] != issuer
                or not count['annual_form_family'] or count['employee_count'] <= 0):
            issues.append('no_unique_aligned_positive_ending_headcount')
        value = rev/count['employee_count'] if not issues else None
        result.append({'income_source_index': index, 'income_source_pointer': '/'+str(index), 'raw': raw,
                       'employee_source_index': count['source_index'] if count else None,
                       'period_end': item.get('date'), 'reported_currency': currency,
                       'annual_revenue_per_ending_employee': value, 'unit': (currency+'/reported_period_end_person') if not issues else None,
                       'issues': issues, 'status': 'aligned_descriptive_ratio' if not issues else 'unqualified_ratio',
                       'average_annual_employees_verified': False, 'productivity_or_efficiency_verified': False})
    return result


def dossier(stock, acquisitions, checked_as_of):
    item = stock if isinstance(stock, dict) else {}; symbol = item.get('symbol')
    employee = []
    for acquisition_index, acquisition in enumerate(acquisitions):
        if acquisition.get('endpoint') in ('historical-employee-count', 'employee-count'):
            for row in employee_rows(symbol, acquisition, checked_as_of):
                row['acquisition_index'] = acquisition_index
                employee.append(row)
    # Combining fallback populations would make unrelated arrays look like one history.
    populations = {r['acquisition_index'] for r in employee}
    if len(populations) > 1:
        for row in employee:
            row['issues'].append('multiple_endpoint_populations'); row['measurement_status'] = 'unqualified_record'
    changes = annual_changes(employee)
    income = next((a for a in acquisitions if a.get('endpoint') == 'income-statement'), {'status': 'not_requested'})
    ratios = revenue_ratios(symbol, employee, income, checked_as_of)
    # Calculations remain attached to each exact source occurrence; no ranking/top-list inference.
    return {'symbol': symbol, 'universe_record': stock, 'acquisitions': acquisitions,
            'employee_observations': employee, 'annual_comparisons': changes, 'income_observations': ratios,
            'expansion_score': None, 'inflection': None, 'headcount_yoy_pct': None,
            'headcount_accel_pp': None, 'headcount_multiyr_cagr_pct': None,
            'revenue_per_employee': None, 'revenue_per_employee_trend_pct': None,
            'measurement_status': 'research_only', 'call': None, 'calls_eligible': False,
            'forecast_qualified': False, 'sizing_eligible': False, 'execution_eligible': False}
