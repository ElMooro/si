"""Diagnostic original-provider statement identity, without accounting scores."""
from collections import Counter
from decimal import Decimal
import json, re
from datetime import date

ENDPOINTS = ('income-statement', 'balance-sheet-statement', 'cash-flow-statement')
FIELDS = ('date', 'symbol', 'reportedCurrency', 'cik', 'filingDate', 'acceptedDate', 'fiscalYear', 'period')


def request_spec(symbol, endpoint, period):
    if symbol not in ('AAPL', 'JPM') or endpoint not in ENDPOINTS or period not in ('annual', 'quarter'):
        raise ValueError('Reviewed diagnostic statement request required')
    limit = 5 if period == 'annual' else 9
    return {'symbol': symbol, 'endpoint': endpoint, 'period': period, 'limit': limit,
        'url': 'https://financialmodelingprep.com/stable/' + endpoint + '?symbol=' + symbol + '&period=' + period + '&limit=' + str(limit)}


def strict(raw):
    def pairs(items):
        out = {}
        for key, value in items:
            if key in out:
                raise ValueError('Duplicate provider JSON key')
            out[key] = value
        return out
    def bad(value):
        raise ValueError('Nonfinite provider JSON constant')
    return json.loads(raw, parse_float=Decimal, parse_constant=bad, object_pairs_hook=pairs)


def inspect(raw, spec):
    if spec != request_spec(spec.get('symbol'), spec.get('endpoint'), spec.get('period')):
        raise ValueError('Exact reviewed request required')
    rows = strict(raw)
    if not isinstance(rows, list) or not rows or len(rows) > spec['limit'] or not all(isinstance(row, dict) for row in rows):
        raise ValueError('Complete bounded nonempty statement response required')
    metadata, problems = [], []
    for i, row in enumerate(rows):
        meta = {key: row.get(key) for key in FIELDS}
        if any(value is not None and not isinstance(value, (str, int)) for value in meta.values()):
            raise ValueError('Scalar identity metadata required')
        meta = {key: None if value is None else str(value) for key, value in meta.items()}
        if meta['symbol'] != spec['symbol']:
            problems.append({'row': i, 'reason': 'symbol_mismatch'})
        for key in ('date', 'filingDate'):
            try:
                if date.fromisoformat(meta[key]).isoformat() != meta[key]:
                    raise ValueError()
            except (TypeError, ValueError):
                problems.append({'row': i, 'reason': 'missing_or_invalid_' + key})
        if not re.fullmatch('[A-Z]{3}', meta['reportedCurrency'] or ''):
            problems.append({'row': i, 'reason': 'currency_unspecified'})
        if not re.fullmatch('[0-9]{1,10}', meta['cik'] or ''):
            problems.append({'row': i, 'reason': 'issuer_identity_unspecified'})
        expected = ('FY',) if spec['period'] == 'annual' else ('Q1', 'Q2', 'Q3', 'Q4')
        if meta['period'] not in expected:
            problems.append({'row': i, 'reason': 'unexpected_reported_period'})
        metadata.append({'source_row': i, **meta})
    field_counts = Counter(key for row in rows for key in row)
    nulls = Counter(key for row in rows for key, value in row.items() if value is None)
    zeros = Counter(key for row in rows for key, value in row.items() if isinstance(value, (int, Decimal)) and not isinstance(value, bool) and value == 0)
    identities = Counter(tuple(row[key] for key in ('date', 'symbol', 'reportedCurrency', 'cik', 'fiscalYear', 'period')) for row in metadata)
    return {'rows': len(rows), 'metadata': metadata, 'field_counts': dict(sorted(field_counts.items())),
        'null_field_counts': dict(sorted(nulls.items())), 'zero_field_counts': dict(sorted(zeros.items())),
        'repeated_statement_identities': sum(n - 1 for n in identities.values() if n > 1),
        'identity_problems': problems, 'provider_normalized_statements': True,
        'original_sec_filings_replayed': False, 'statement_duration_verified': False,
        'historical_availability_verified': False, 'accounting_calculations_qualified': False,
        'forecast_qualified': False, 'sizing_qualified': False}


def alignment(inventories):
    if set(inventories) != set(ENDPOINTS):
        raise ValueError('All three complete response inventories required')
    fields = ('date', 'symbol', 'reportedCurrency', 'cik', 'fiscalYear', 'period')
    sets = {key: {tuple(row[field] for field in fields) for row in value['metadata']} for key, value in inventories.items()}
    common = set.intersection(*sets.values())
    return {'identity_fields': list(fields), 'exact_common_identities': len(common),
        'unmatched_by_endpoint': {key: [list(v) for v in sorted(values - common, key=repr)] for key, values in sets.items()},
        'duplicate_rows_are_not_collapsed_in_sources': True,
        'positionally_aligned': all([tuple(row[f] for f in fields) for row in inventories[key]['metadata']] ==
            [tuple(row[f] for f in fields) for row in inventories[ENDPOINTS[0]]['metadata']] for key in ENDPOINTS),
        'common_identity_does_not_qualify_filing_vintage_or_duration': True}
