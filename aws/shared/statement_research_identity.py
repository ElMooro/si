"""Dated SEC current-ticker corroboration, never historical identity inference."""
from collections import defaultdict
from datetime import date
from decimal import Decimal
import re
import statement_research_source as source
import statement_measurements as measurements

URL = 'https://www.sec.gov/files/company_tickers.json'
MATCH = 'current_ticker_cik_pair_corroborated'


def index(body):
    rows = source.strict(body)
    if not isinstance(rows, dict) or not 1000 <= len(rows) <= 50000:
        raise ValueError('Complete original SEC ticker index required')
    mapping = defaultdict(set)
    for key, row in rows.items():
        if (not isinstance(key, str) or not key.isdigit() or not isinstance(row, dict)
                or type(row.get('cik_str')) is not int or not 0 < row['cik_str'] < 10**10
                or not isinstance(row.get('ticker'), str) or not row['ticker']
                or not isinstance(row.get('title'), str) or not row['title']):
            raise ValueError('Invalid SEC current identity index row')
        mapping[row['ticker']].add(str(row['cik_str']).zfill(10))
    return {key: sorted(values) for key, values in sorted(mapping.items())}


def capture(ref, read):
    cap = source.strict(source.original(ref, read))
    if (cap.get('url') != URL or cap.get('http_status') != 200
            or cap.get('status') != 'response_retained'
            or source.clock(cap['requested_at']) > source.clock(cap['received_at'])
            or not isinstance(cap.get('headers'), dict)):
        raise ValueError('Successful dated original SEC ticker response required')
    raw = source.original(cap['original'], read)
    if cap['headers'].get('content-length') not in (None, str(len(raw))):
        raise ValueError('SEC ticker response length differs')
    return cap, raw, index(raw)


def value_type(value):
    if value is None: return 'null'
    if type(value) is bool: return 'boolean'
    if isinstance(value, str): return 'string'
    if type(value) in (int, Decimal): return 'number'
    if isinstance(value, list): return 'array'
    if isinstance(value, dict): return 'object'
    raise ValueError('Original JSON value required')


def public_value(value):
    # Original bytes preserve lexical representation; Decimal text avoids loss
    # when exposing malformed numeric metadata for diagnosis in the browser.
    if isinstance(value, Decimal): return str(value)
    if isinstance(value, list): return [public_value(v) for v in value]
    if isinstance(value, dict): return {k: public_value(v) for k, v in value.items()}
    return value


def evidence(row, requested_symbol, mapping):
    ciks = mapping.get(requested_symbol, [])
    reported = str(row.get('cik', ''))
    if not ciks: status = 'not_in_current_sec_ticker_index'
    elif len(ciks) != 1: status = 'ambiguous_current_sec_ticker_index'
    elif type(row.get('cik')) not in (str, int) or not re.fullmatch('[0-9]{1,10}', reported) or int(reported) == 0:
        status = 'invalid_provider_cik'
    elif reported.zfill(10) != ciks[0]: status = 'provider_cik_differs_from_current_sec_index'
    else: status = MATCH
    issues = []
    try:
        end = date.fromisoformat(str(row.get('date')))
        for field in ('filingDate', 'acceptedDate'):
            if date.fromisoformat(str(row.get(field))[:10]) < end:
                issues.append(field + '_precedes_period_end')
    except ValueError:
        issues.append('invalid_provider_date')
    return {'reported_identity': {k: public_value(row.get(k)) for k in measurements.IDENTITY},
        'reported_identity_types': {k: value_type(row.get(k)) for k in measurements.IDENTITY},
        'missing_fields': [k for k in measurements.IDENTITY if k not in row],
        'current_sec_ciks': ciks, 'current_identity_status': status, 'clock_issues': issues,
        'historical_security_continuity_verified': False}
