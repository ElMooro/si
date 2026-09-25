"""Dated, descriptive capital-structure arithmetic over exact provider rows.

Cash paid is not a count of shares retired. Weighted-average EPS denominators
are not period-end outstanding shares or free float. No cross-period growth,
TTM, market-cap yield, dilution verdict, investment vote or size is inferred.
"""
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, localcontext, ROUND_HALF_EVEN
from fractions import Fraction
import re

CONTRACT = 'capital-structure-measurements.v1'
INCOME, CASH = 'income-statement', 'cash-flow-statement'
IDENTITY = ('symbol', 'cik', 'reportedCurrency', 'date', 'fiscalYear', 'period', 'filingDate', 'acceptedDate')
FLAGS = {'calls_eligible': False, 'sizing_eligible': False, 'execution_eligible': False,
         'forecast_qualified': False, 'independent_investment_votes': 0, 'call': None, 'score': None}

# Each tuple is (endpoint, field, signed coefficient). Denominators must be
# strictly positive. An observed zero numerator is retained as a real zero.
DEFINITIONS = (
    ('cash_repurchase_outflow', 'Cash paid for common-stock repurchases', 'currency',
     ((CASH, 'commonStockRepurchased', -1),), None,
     ((CASH, 'commonStockRepurchased', 'nonpositive'),)),
    ('cash_common_stock_issuance', 'Cash received from common-stock issuance', 'currency',
     ((CASH, 'commonStockIssuance', 1),), None,
     ((CASH, 'commonStockIssuance', 'nonnegative'),)),
    ('cash_common_dividends', 'Cash paid for common dividends', 'currency',
     ((CASH, 'commonDividendsPaid', -1),), None,
     ((CASH, 'commonDividendsPaid', 'nonpositive'),)),
    ('gross_common_cash_distribution', 'Negative of repurchase cash flow plus common-dividend cash flow', 'currency',
     ((CASH, 'commonStockRepurchased', -1), (CASH, 'commonDividendsPaid', -1)), None,
     ((CASH, 'commonStockRepurchased', 'nonpositive'), (CASH, 'commonDividendsPaid', 'nonpositive'))),
    ('net_common_cash_return', 'Negative of issuance, repurchase and common-dividend cash flows; negative means net cash raised', 'currency',
     ((CASH, 'commonStockIssuance', -1), (CASH, 'commonStockRepurchased', -1), (CASH, 'commonDividendsPaid', -1)), None,
     ((CASH, 'commonStockIssuance', 'nonnegative'), (CASH, 'commonStockRepurchased', 'nonpositive'),
      (CASH, 'commonDividendsPaid', 'nonpositive'))),
    ('common_issuance_cash_residual', 'Reported net common-stock issuance minus issuance and repurchase cash flows', 'currency',
     ((CASH, 'netCommonStockIssuance', 1), (CASH, 'commonStockIssuance', -1), (CASH, 'commonStockRepurchased', -1)), None, ()),
    ('cash_repurchase_to_operating_cash_flow_pct', 'Cash paid for repurchases / positive operating cash flow', '%',
     ((CASH, 'commonStockRepurchased', -1),), (CASH, 'operatingCashFlow'),
     ((CASH, 'commonStockRepurchased', 'nonpositive'),)),
    ('sbc_to_operating_cash_flow_pct', 'Reported stock-based compensation expense / positive operating cash flow', '%',
     ((CASH, 'stockBasedCompensation', 1),), (CASH, 'operatingCashFlow'), ()),
    ('sbc_to_revenue_pct', 'Reported stock-based compensation expense / positive revenue in the exact same reported filing period', '%',
     ((CASH, 'stockBasedCompensation', 1),), (INCOME, 'revenue'), ()),
    ('weighted_diluted_over_basic_pct', 'Incremental diluted EPS denominator / positive basic EPS denominator in one reported period', '%',
     ((INCOME, 'weightedAverageShsOutDil', 1), (INCOME, 'weightedAverageShsOut', -1)),
     (INCOME, 'weightedAverageShsOut'), ((INCOME, 'weightedAverageShsOutDil', 'positive'),)),
)


def number(value):
    if isinstance(value, bool) or not isinstance(value, (int, Decimal)):
        return None
    result = Decimal(value)
    if not result.is_finite() or abs(result.adjusted()) > 100 or len(result.as_tuple().digits) > 128:
        return None
    return result


def typed(value):
    if isinstance(value, Decimal):
        return {'type': 'decimal', 'value': str(value)}
    if isinstance(value, list):
        return {'type': 'list', 'value': [typed(v) for v in value]}
    if isinstance(value, dict):
        return {'type': 'object', 'value': {k: typed(v) for k, v in value.items()}}
    return {'type': type(value).__name__, 'value': value}


def iso_day(value):
    try:
        return isinstance(value, str) and date.fromisoformat(value).isoformat() == value
    except ValueError:
        return False


def clock(value):
    if not isinstance(value, str):
        raise ValueError('Explicit source acquisition clock required')
    stamp = datetime.fromisoformat(value.replace('Z', '+00:00'))
    if stamp.tzinfo is None:
        raise ValueError('Source acquisition timezone required')
    return stamp.astimezone(timezone.utc)


def coordinate(entry):
    if not isinstance(entry, dict) or not isinstance(entry.get('values'), dict):
        return 'original_source_row_required'
    if (not re.fullmatch('[a-f0-9]{64}', str(entry.get('source_id', '')))
            or type(entry.get('source_row')) is not int or entry['source_row'] < 0):
        return 'exact_original_row_coordinate_required'
    return None


def reported_input(entry, endpoint, field):
    entry = entry if isinstance(entry, dict) else {}
    row = entry.get('values') if isinstance(entry.get('values'), dict) else {}
    value = number(row.get(field))
    return {'endpoint': endpoint, 'field': field, 'source_id': entry.get('source_id'),
        'source_row': entry.get('source_row'), 'present': field in row,
        'reported': typed(row.get(field)), 'numeric_value': format(value, 'f') if value is not None else None,
        'status': 'reported_numeric_value' if value is not None else
            ('missing_field' if field not in row else 'null_field' if row[field] is None else 'nonnumeric_or_unbounded_field')}


def statement_identity(entry, symbol, ciks, acquired_at):
    error = coordinate(entry)
    if error:
        return None, error
    row = entry['values']
    if row.get('symbol') != symbol:
        return None, 'reported_symbol_differs_from_request'
    reported_cik = row.get('cik')
    if (type(reported_cik) not in (str, int) or not re.fullmatch('[0-9]{1,10}', str(reported_cik))
            or int(reported_cik) == 0):
        return None, 'invalid_reported_cik'
    if not isinstance(ciks, list) or len(ciks) != 1 or ciks[0] != str(reported_cik).zfill(10):
        return None, 'current_ticker_cik_not_corroborated'
    if not isinstance(row.get('reportedCurrency'), str) or not re.fullmatch('[A-Z]{3}', row['reportedCurrency']):
        return None, 'reported_currency_missing_or_invalid'
    if (type(row.get('fiscalYear')) not in (str, int) or not re.fullmatch('[0-9]{4}', str(row['fiscalYear']))
            or row.get('period') not in ('FY', 'Q1', 'Q2', 'Q3', 'Q4')):
        return None, 'reported_fiscal_period_missing_or_invalid'
    if not iso_day(row.get('date')) or not iso_day(row.get('filingDate')):
        return None, 'reported_statement_date_missing_or_invalid'
    accepted = row.get('acceptedDate')
    try:
        if not isinstance(accepted, str) or len(accepted) < 16 or accepted[10] not in ('T', ' '):
            raise ValueError()
        accepted_stamp = datetime.fromisoformat(accepted)
    except ValueError:
        return None, 'reported_acceptance_date_missing_or_invalid'
    end, filed = date.fromisoformat(row['date']), date.fromisoformat(row['filingDate'])
    if filed < end or accepted_stamp.date() < end:
        return None, 'reported_filing_precedes_period_end'
    # Provider acceptance timestamps have no verified timezone. A date more
    # than one day beyond UTC acquisition is unambiguously future worldwide.
    if max(end, filed, accepted_stamp.date()) > clock(acquired_at).date()+timedelta(days=1):
        return None, 'reported_statement_date_after_acquisition'
    identity = {key: str(row[key]) for key in IDENTITY}
    identity['cik'] = str(reported_cik).zfill(10)
    return identity, None


def exact_result(value):
    """Retain a rational witness as well as reproducible display rounding."""
    with localcontext() as ctx:
        ctx.prec = max(256, len(str(abs(value.numerator))) + len(str(value.denominator)) + 64)
        rounded = (Decimal(value.numerator) / Decimal(value.denominator)).quantize(
            Decimal('0.000000000001'), rounding=ROUND_HALF_EVEN)
    return {'value': format(abs(rounded) if rounded == 0 else rounded, 'f'),
        'exact': {'numerator': str(value.numerator), 'denominator': str(value.denominator)},
        'rounding': 'half_even_12_decimal_places'}


def compute(bundle, symbol, current_sec_ciks, acquired_at):
    if not isinstance(bundle, dict) or not set(bundle) <= {INCOME, CASH}:
        raise ValueError('Explicit income/cash-flow source-row bundle required')
    if not isinstance(symbol, str) or not re.fullmatch('[A-Z0-9][A-Z0-9.-]{0,15}', symbol):
        raise ValueError('Exact requested provider symbol required')
    clock(acquired_at)
    identities = {endpoint: statement_identity(entry, symbol, current_sec_ciks, acquired_at)
                  for endpoint, entry in bundle.items()}
    out = {'contract': CONTRACT, 'requested_symbol': symbol, 'current_sec_ciks': current_sec_ciks,
        'source_acquired_at': acquired_at, 'identity_by_endpoint': {
            key: {'identity': value[0], 'status': value[1] or 'current_ticker_cik_corroborated'}
            for key, value in identities.items()},
        'statement_duration_verified': False, 'acceptance_timezone_verified': False,
        'historical_security_continuity_verified': False, 'original_sec_filings_replayed': False,
        'split_basis_verified': False, 'historical_availability_verified': False,
        'reconciliations_are_independent_evidence': False, 'period_annualized': False,
        'cash_flow_is_share_count': False, 'eps_denominator_is_outstanding_or_float': False,
        'sbc_expense_is_issued_shares': False, 'provider_scale_independently_verified': False,
        **FLAGS, 'metrics': {}}
    for key, label, unit, terms, denominator, sign_rules in DEFINITIONS:
        fields = [(e, f) for e, f, _ in terms]
        if denominator and denominator not in fields:
            fields.append(denominator)
        required = {e for e, _ in fields}
        inputs = [reported_input(bundle.get(e), e, f) for e, f in fields]
        ids = [identities[e][0] for e in sorted(required) if e in identities]
        errors = [identities[e][1] for e in sorted(required) if e in identities and identities[e][1]]
        status = ('required_original_statement_row_missing' if not required <= set(bundle) else
                  errors[0] if errors else
                  'reported_filing_period_or_currency_mismatch' if any(v != ids[0] for v in ids[1:]) else None)
        currency = ids[0]['reportedCurrency'] if ids and ids[0] else None
        metric = {'definition': label, 'unit': currency if unit == 'currency' else unit,
            'value': None, 'exact': None, 'inputs': inputs, 'status': status,
            'identity': ids[0] if not status else None, 'period_annualized': False,
            'supports_investment_action': False}
        nums = {(v['endpoint'], v['field']): number(Decimal(v['numeric_value']))
                if v['numeric_value'] is not None else None for v in inputs}
        if not status:
            if any(v is None for v in nums.values()):
                metric['status'] = 'required_provider_field_unavailable'
            elif any((rule == 'nonpositive' and nums[(e, f)] > 0)
                     or (rule == 'nonnegative' and nums[(e, f)] < 0)
                     or (rule == 'positive' and nums[(e, f)] <= 0) for e, f, rule in sign_rules):
                metric['status'] = 'provider_cash_flow_or_share_sign_conflict'
            elif denominator and nums[denominator] <= 0:
                metric['status'] = 'nonpositive_denominator'
            elif key == 'weighted_diluted_over_basic_pct' and nums[(INCOME, 'weightedAverageShsOutDil')] < nums[(INCOME, 'weightedAverageShsOut')]:
                metric['status'] = 'diluted_denominator_below_basic'
            else:
                result = sum((Fraction(nums[(e, f)]) * coefficient for e, f, coefficient in terms), Fraction(0))
                if denominator:
                    result = result / Fraction(nums[denominator]) * 100
                metric.update(exact_result(result), status='descriptive_provider_row_calculation')
        out['metrics'][key] = metric
    return out


def float_snapshot(entry, symbol):
    """Within one reported float row only; no quote join or history inference."""
    inputs = [reported_input(entry, 'shares-float', field) for field in ('floatShares', 'outstandingShares', 'freeFloat')]
    error = coordinate(entry)
    row = entry['values'] if not error else {}
    if not error and row.get('symbol') != symbol:
        error = 'reported_symbol_differs_from_request'
    result = {'contract': 'capital-structure-float-snapshot.v1', 'reported_date': typed(row.get('date')),
        'reported_date_timezone_verified': False, 'current_security_class_verified': False,
        'inputs': inputs, 'float_of_outstanding_pct': None, 'reported_free_float_residual_pp': None,
        'status': error, 'free_float_change_qualified': False, 'split_basis_verified': False, **FLAGS}
    f, o, percentage = [number(row.get(field)) for field in ('floatShares', 'outstandingShares', 'freeFloat')]
    if not error:
        if f is None or o is None:
            result['status'] = 'reported_share_counts_unavailable'
        elif o <= 0 or f < 0 or f > o:
            result['status'] = 'reported_share_count_range_conflict'
        else:
            ratio = Fraction(f) / Fraction(o) * 100
            result['float_of_outstanding_pct'] = exact_result(ratio)
            result['status'] = 'descriptive_within_provider_snapshot'
            if percentage is not None and 0 <= percentage <= 100:
                result['reported_free_float_residual_pp'] = exact_result(Fraction(percentage) - ratio)
    return result
