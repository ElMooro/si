"""Exact, descriptive ratios for explicitly aligned provider statement rows.

No ranks, fraud probabilities, synthetic TTM, sector grades or investment
votes. Provider-normalized records are not original SEC filing verification.
"""
from datetime import date, datetime
from decimal import Decimal, localcontext, ROUND_HALF_EVEN
import re

ENDPOINTS = ('income-statement', 'balance-sheet-statement', 'cash-flow-statement')
IDENTITY = ('symbol', 'cik', 'reportedCurrency', 'date', 'fiscalYear', 'period', 'filingDate', 'acceptedDate')
I, B, C = ENDPOINTS
DEFINITIONS = (
    ('gross_margin_pct', 'Reported gross profit / revenue', '%', ((I,'grossProfit'),), (I,'revenue'), 100),
    ('operating_margin_pct', 'Operating income / revenue', '%', ((I,'operatingIncome'),), (I,'revenue'), 100),
    ('net_margin_pct', 'Net income / revenue', '%', ((I,'netIncome'),), (I,'revenue'), 100),
    ('operating_cash_margin_pct', 'Operating cash flow / revenue', '%', ((C,'operatingCashFlow'),), (I,'revenue'), 100),
    ('cash_conversion_multiple', 'Operating cash flow / positive net income', 'multiple', ((C,'operatingCashFlow'),), (I,'netIncome'), 1),
    ('current_ratio', 'Current assets / current liabilities', 'multiple', ((B,'totalCurrentAssets'),), (B,'totalCurrentLiabilities'), 1),
    ('debt_to_assets_pct', 'Total debt / total assets', '%', ((B,'totalDebt'),), (B,'totalAssets'), 100),
    ('goodwill_to_assets_pct', 'Goodwill / total assets', '%', ((B,'goodwill'),), (B,'totalAssets'), 100),
    ('net_receivables_to_revenue_pct', 'Net receivables / reported-period revenue', '%', ((B,'netReceivables'),), (I,'revenue'), 100),
    ('sga_to_revenue_pct', 'Selling, general and administrative expenses / revenue', '%', ((I,'sellingGeneralAndAdministrativeExpenses'),), (I,'revenue'), 100),
    ('reported_fcf_margin_pct', 'Provider-reported free cash flow / revenue', '%', ((C,'freeCashFlow'),), (I,'revenue'), 100),
    ('earnings_cash_gap_to_assets_pct', '(Net income - operating cash flow) / period-end total assets', '%', ((I,'netIncome'),(C,'operatingCashFlow')), (B,'totalAssets'), 100),
    ('net_debt_derived', 'Total debt - cash and cash equivalents', 'reported_currency', ((B,'totalDebt'),(B,'cashAndCashEquivalents')), None, 1),
    ('earnings_cash_gap', 'Net income - operating cash flow', 'reported_currency', ((I,'netIncome'),(C,'operatingCashFlow')), None, 1),
    ('balance_identity_residual', 'Total assets - total liabilities - total equity', 'reported_currency', ((B,'totalAssets'),(B,'totalLiabilities'),(B,'totalEquity')), None, 1),
    ('gross_profit_residual', 'Reported gross profit - revenue + cost of revenue', 'reported_currency', ((I,'grossProfit'),(I,'revenue'),(I,'costOfRevenue')), None, 1),
    ('operating_cash_alias_residual', 'Operating cash flow - net cash provided by operating activities', 'reported_currency', ((C,'operatingCashFlow'),(C,'netCashProvidedByOperatingActivities')), None, 1),
)


def number(value):
    if isinstance(value, bool) or not isinstance(value, (int, Decimal)):
        return None
    result = Decimal(value)
    if not result.is_finite() or abs(result.adjusted()) > 100 or len(result.as_tuple().digits) > 128:
        return None
    return result


def text(value):
    return format(value, 'f') if value is not None else None


def identity(bundle):
    if not bundle or not set(bundle) <= set(ENDPOINTS):
        return None, 'reviewed_statement_records_required'
    identities = []
    for endpoint in ENDPOINTS:
        if endpoint not in bundle:
            continue
        entry = bundle[endpoint]
        if not isinstance(entry, dict) or not isinstance(entry.get('values'), dict):
            return None, 'statement_record_required'
        if (not re.fullmatch('[a-f0-9]{64}', str(entry.get('source_id', '')))
                or type(entry.get('source_row')) is not int or entry['source_row'] < 0):
            return None, 'exact_source_row_required'
        row = entry['values']
        if any(not isinstance(row.get(field), (str, int)) or isinstance(row.get(field), bool) for field in IDENTITY):
            return None, 'statement_identity_missing'
        values = tuple(str(row[field]) for field in IDENTITY)
        if (not re.fullmatch('[A-Z0-9][A-Z0-9.-]{0,15}', values[0])
                or not re.fullmatch('[0-9]{1,10}', values[1]) or int(values[1]) == 0):
            return None, 'reported_issuer_identity_invalid'
        if not re.fullmatch('[A-Z]{3}', values[2]) or not re.fullmatch('[0-9]{4}', values[4]) or values[5] not in ('FY','Q1','Q2','Q3','Q4'):
            return None, 'period_or_currency_invalid'
        try:
            if date.fromisoformat(values[3]).isoformat() != values[3] or date.fromisoformat(values[6]).isoformat() != values[6]:
                return None, 'statement_date_invalid'
            if len(values[7]) < 16 or values[7][10] not in ('T', ' '):
                return None, 'provider_acceptance_time_missing'
            accepted = datetime.fromisoformat(values[7])
            if date.fromisoformat(values[6]) < date.fromisoformat(values[3]) or accepted.date() < date.fromisoformat(values[3]):
                return None, 'filing_precedes_period_end'
        except ValueError:
            return None, 'statement_date_invalid'
        identities.append(values)
    if len(set(identities)) != 1:
        return None, 'statement_identity_or_filing_vintage_mismatch'
    return dict(zip(IDENTITY, identities[0])), None


def compute(bundle):
    if not isinstance(bundle, dict):
        raise ValueError('Explicit statement-row bundle required')
    ident, error = identity(bundle)
    out = {'contract': 'statement-measurements.v1', 'identity': ident, 'alignment_status': error or 'exact_provider_row_identity',
        'complete_three_statement_bundle': set(bundle) == set(ENDPOINTS) and not error,
        'provider_reported_acceptance_timezone_verified': False, 'statement_duration_verified': False,
        'original_sec_filings_replayed': False, 'point_in_time_availability_verified': False,
        'provider_fields_may_be_derived': True, 'provider_scale_independently_verified': False,
        'reconciliations_are_independent_evidence': False,
        'accounting_audit': False, 'call': None, 'score': None, 'grade': None, 'm_score': None,
        'calls_eligible': False, 'sizing_eligible': False, 'execution_eligible': False, 'forecast_qualified': False,
        'independent_investment_votes': 0, 'metrics': {}}
    for key, definition, unit, terms, denominator, scale in DEFINITIONS:
        refs = (*terms, *((denominator,) if denominator else ()))
        inputs = []
        for endpoint, field in refs:
            entry = bundle.get(endpoint, {}) if isinstance(bundle, dict) else {}
            if not isinstance(entry, dict):
                entry = {}
            row = entry.get('values', {}) if isinstance(entry, dict) else {}
            if not isinstance(row, dict):
                row = {}
            value = number(row.get(field))
            inputs.append({'endpoint': endpoint, 'field': field, 'source_id': entry.get('source_id'),
                'source_row': entry.get('source_row'), 'reported_value': text(value),
                'status': 'numeric_provider_value' if value is not None else 'missing_or_nonnumeric_provider_field'})
        metric = {'definition': definition, 'unit': ident['reportedCurrency'] if ident and unit == 'reported_currency' else unit,
            'value': None, 'inputs': inputs, 'status': error, 'rounding': 'half_even_12_decimal_places',
            'period_annualized': False, 'supports_investment_action': False}
        if not error:
            nums = [number(Decimal(v['reported_value'])) if v['reported_value'] is not None else None for v in inputs]
            if not {endpoint for endpoint, field in refs} <= set(bundle):
                metric['status'] = 'required_statement_record_missing'
            elif any(value is None for value in nums):
                metric['status'] = 'required_provider_field_missing'
            elif denominator and nums[-1] <= 0:
                metric['status'] = 'nonpositive_denominator'
            else:
                with localcontext() as ctx:
                    ctx.prec = max(256, sum(abs(v.adjusted()) + len(v.as_tuple().digits) for v in nums) + 64)
                    top = nums[0] - sum(nums[1:len(terms)], Decimal(0))
                    if key == 'gross_profit_residual':
                        top = nums[0] - nums[1] + nums[2]
                    value = top / nums[-1] * scale if denominator else top
                    rounded = value.quantize(Decimal('0.000000000001'), rounding=ROUND_HALF_EVEN)
                    metric.update(value=text(abs(rounded) if rounded == 0 else rounded), status='descriptive_calculation')
        out['metrics'][key] = metric
    return out
