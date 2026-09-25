from pathlib import Path
from copy import deepcopy
from decimal import Decimal
from fractions import Fraction
import sys, unittest
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT/'aws/shared'))
import capital_structure_measurements as model

I, C = model.INCOME, model.CASH
AS_OF = '2026-09-25T12:00:00Z'


def record(endpoint, **values):
    return {'source_id': ('a' if endpoint == I else 'b') * 64, 'source_row': 3,
        'values': {'symbol': 'ABC', 'cik': '123', 'reportedCurrency': 'USD',
            'date': '2025-12-31', 'fiscalYear': '2025', 'period': 'FY',
            'filingDate': '2026-02-15', 'acceptedDate': '2026-02-15 12:00:00', **values}}


def fixture():
    return {I: record(I, revenue=300, weightedAverageShsOut=100, weightedAverageShsOutDil=105),
        C: record(C, commonStockIssuance=25, commonStockRepurchased=-100,
                  netCommonStockIssuance=-75, commonDividendsPaid=-20,
                  stockBasedCompensation=Decimal('10.125'), operatingCashFlow=200)}


def metrics(bundle):
    return model.compute(bundle, 'ABC', ['0000000123'], AS_OF)['metrics']


def exact(metric):
    return Fraction(int(metric['exact']['numerator']), int(metric['exact']['denominator']))


class Tests(unittest.TestCase):
    def test_cash_issuance_does_not_disappear_and_cash_return_is_not_ownership_change(self):
        bundle = fixture(); original = deepcopy(bundle)
        result = model.compute(bundle, 'ABC', ['0000000123'], AS_OF); m = result['metrics']
        self.assertEqual(exact(m['cash_common_stock_issuance']), 25)
        self.assertEqual(exact(m['cash_repurchase_outflow']), 100)
        self.assertEqual(exact(m['gross_common_cash_distribution']), 120)
        self.assertEqual(exact(m['net_common_cash_return']), 95)
        self.assertEqual(exact(m['common_issuance_cash_residual']), 0)
        self.assertEqual(exact(m['cash_repurchase_to_operating_cash_flow_pct']), 50)
        self.assertEqual(exact(m['sbc_to_revenue_pct']), Fraction(27, 8))
        self.assertEqual(exact(m['weighted_diluted_over_basic_pct']), 5)
        self.assertEqual(bundle, original)
        self.assertFalse(result['cash_flow_is_share_count'])
        self.assertFalse(result['eps_denominator_is_outstanding_or_float'])
        self.assertFalse(result['sbc_expense_is_issued_shares'])
        self.assertFalse(result['sizing_eligible']); self.assertIsNone(result['call'])
        self.assertEqual(result['independent_investment_votes'], 0)
        self.assertNotIn('buyback_net_yield_pct', m); self.assertNotIn('sh_yoy_pct', m)

    def test_missing_null_zero_wrong_type_and_legacy_alias_are_distinct(self):
        for value in ('MISSING', None, True, '25', 25.0):
            bundle = fixture(); row = bundle[C]['values']; row['commonStockIssued'] = 900
            if value == 'MISSING': del row['commonStockIssuance']
            else: row['commonStockIssuance'] = value
            m = metrics(bundle)
            self.assertIsNone(m['cash_common_stock_issuance']['value'])
            self.assertIsNone(m['net_common_cash_return']['value'])
            self.assertEqual(exact(m['cash_repurchase_outflow']), 100)
            item = m['cash_common_stock_issuance']['inputs'][0]
            self.assertEqual(item['present'], value != 'MISSING')
            self.assertEqual(item['field'], 'commonStockIssuance')
        bundle = fixture(); bundle[C]['values']['commonStockIssuance'] = 0
        self.assertEqual(exact(metrics(bundle)['cash_common_stock_issuance']), 0)

    def test_signs_denominators_and_issuer_conflicts_fail_closed_without_hiding_original(self):
        for field, value, blocked in (
            ('commonStockRepurchased', 100, 'cash_repurchase_outflow'),
            ('commonStockIssuance', -25, 'cash_common_stock_issuance'),
            ('commonDividendsPaid', 20, 'cash_common_dividends'),
            ('operatingCashFlow', -1, 'cash_repurchase_to_operating_cash_flow_pct'),
            ('operatingCashFlow', 0, 'cash_repurchase_to_operating_cash_flow_pct')):
            bundle = fixture(); bundle[C]['values'][field] = value; m = metrics(bundle)[blocked]
            self.assertIsNone(m['value']); self.assertIsNone(m['exact'])
            self.assertIn(str(value), [v['numeric_value'] for v in m['inputs']])
        bundle = fixture(); bundle[I]['values']['weightedAverageShsOutDil'] = 90
        self.assertEqual(metrics(bundle)['weighted_diluted_over_basic_pct']['status'], 'diluted_denominator_below_basic')
        bundle = fixture(); bundle[C]['values']['cik'] = '999'
        self.assertEqual(metrics(bundle)['cash_repurchase_outflow']['status'], 'current_ticker_cik_not_corroborated')
        self.assertEqual(exact(metrics(bundle)['weighted_diluted_over_basic_pct']), 5)

    def test_exact_period_and_filing_vintage_required_for_cross_statement_ratio(self):
        for field, value in (('reportedCurrency', 'EUR'), ('date', '2025-12-30'),
                             ('filingDate', '2026-02-16'), ('acceptedDate', '2026-02-15 12:00:01'),
                             ('period', 'Q4'), ('fiscalYear', '2024')):
            bundle = fixture(); bundle[I]['values'][field] = value; m = metrics(bundle)
            self.assertEqual(m['sbc_to_revenue_pct']['status'], 'reported_filing_period_or_currency_mismatch')
            self.assertIsNone(m['sbc_to_revenue_pct']['value'])
            self.assertEqual(exact(m['cash_repurchase_outflow']), 100)
        bundle = fixture(); del bundle[I]
        self.assertIsNone(metrics(bundle)['sbc_to_revenue_pct']['value'])
        self.assertEqual(exact(metrics(bundle)['cash_repurchase_outflow']), 100)

    def test_coordinates_and_source_clock_are_required_and_future_rows_are_withheld(self):
        for mutation in ({'source_id': 'x'}, {'source_row': True}, {'source_row': -1}):
            bundle = fixture(); bundle[C].update(mutation)
            self.assertIsNone(metrics(bundle)['cash_repurchase_outflow']['value'])
        bundle = fixture(); bundle[C]['values']['filingDate'] = '2027-01-01'
        self.assertEqual(metrics(bundle)['cash_repurchase_outflow']['status'], 'reported_statement_date_after_acquisition')
        bundle = fixture(); bundle[C]['values']['acceptedDate'] = '2025-01-01 12:00:00'
        self.assertEqual(metrics(bundle)['cash_repurchase_outflow']['status'], 'reported_filing_precedes_period_end')
        with self.assertRaises(ValueError): model.compute(fixture(), 'ABC', ['0000000123'], '2026-09-25')
        for ciks in ([], ['0000000123', '0000000999'], ['123']):
            self.assertIsNone(model.compute(fixture(), 'ABC', ciks, AS_OF)['metrics']['cash_repurchase_outflow']['value'])

    def test_every_value_has_exact_row_field_and_fractional_arithmetic_witness(self):
        # Non-binary decimals and repeating ratios expose float conversion and
        # rounding errors independently of the model's formula dispatcher.
        bundle = fixture(); bundle[C]['values'].update(commonStockRepurchased=Decimal('-0.1'), operatingCashFlow=3)
        m = metrics(bundle)['cash_repurchase_to_operating_cash_flow_pct']
        self.assertEqual(exact(m), Fraction(10, 3))
        self.assertEqual(m['value'], '3.333333333333')
        self.assertEqual([(v['source_id'], v['source_row'], v['field']) for v in m['inputs']],
                         [('b'*64, 3, 'commonStockRepurchased'), ('b'*64, 3, 'operatingCashFlow')])
        bundle[C]['values'].update(commonStockIssuance=1000, commonStockRepurchased=0, commonDividendsPaid=0)
        self.assertEqual(exact(metrics(bundle)['net_common_cash_return']), -1000)

    def test_float_snapshot_is_separate_from_eps_and_does_not_estimate_float_change(self):
        entry = {'source_id': 'f'*64, 'source_row': 0, 'values': {
            'symbol': 'ABC', 'date': '2026-09-24', 'outstandingShares': 300,
            'floatShares': 100, 'freeFloat': Decimal('33.3333')}}
        result = model.float_snapshot(entry, 'ABC')
        self.assertEqual(exact(result['float_of_outstanding_pct']), Fraction(100, 3))
        self.assertEqual(exact(result['reported_free_float_residual_pp']), Fraction(-1, 30000))
        self.assertFalse(result['free_float_change_qualified'])
        self.assertFalse(result['current_security_class_verified'])
        self.assertFalse(result['calls_eligible'])
        for f, o in ((400, 300), (-1, 300), (0, 0), (None, 300)):
            changed = deepcopy(entry); changed['values'].update(floatShares=f, outstandingShares=o)
            self.assertIsNone(model.float_snapshot(changed, 'ABC')['float_of_outstanding_pct'])
        self.assertIsNone(model.float_snapshot(entry, 'OTHER')['float_of_outstanding_pct'])


if __name__ == '__main__': unittest.main(verbosity=2)
