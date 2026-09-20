from copy import deepcopy
from decimal import Decimal
from fractions import Fraction
import hashlib
import unittest

import capital_bridge as m


def position(cusip, quantity, value, kind='SH', option=None, title='COM'):
    identity = {'cusip': cusip, 'class': title, 'quantity_type': kind, 'put_call': option}
    return hashlib.sha256(m.encoded(identity)).hexdigest(), {
        'identity': identity, 'reported_quantity': str(quantity), 'reported_value_usd': str(value),
        'issuer_names': ['SYNTHETIC '+cusip], 'native_rows': 1}


def fixture(before, current):
    def period(rows):
        return {'status': m.COMPLETE, 'positions': dict(rows), 'valuation_reviews': [],
                'reported_value_usd': str(sum((Decimal(v['reported_value_usd']) for _, v in rows), Decimal(0)))}
    return {'contract': 'holdings-native-fund.v1', 'fund': 'SYNTHETIC', 'cik': '0000000001',
            'official_name': 'SYNTHETIC MANAGER', 'prior_holdings_period': '2026-03-31', 'current_holdings_period': '2026-06-30',
            'periods': {'2026-03-31': period(before), '2026-06-30': period(current)}}


def value(item): return Fraction(int(item['numerator']), int(item['denominator']))


class Tests(unittest.TestCase):
    def test_unchanged_quantity_attributes_no_value_to_quantity_change(self):
        out = m.build_manager(fixture([position('111111111', 400000000, 28000000000)],
                                      [position('111111111', 400000000, 30088000000)]))
        row = out['rows'][0]; bridge = row['bridge']
        self.assertEqual(value(bridge['quantity_term_usd']), 0)
        self.assertEqual(value(bridge['unit_value_term_usd']), 2088000000)
        self.assertEqual(value(row['reported_quantity_change']), 0)
        self.assertEqual(value(out['scopes']['SH|NONE']['change_in_reported_table_value_usd']), 2088000000)
        self.assertFalse(out['execution_inferred']); self.assertFalse(out['calls_eligible'])

    def test_fractional_unit_values_and_decimal_rounding_reconcile_exactly(self):
        for q0, v0, q1, v1 in [('3', '1', '7', '2'), ('0.000001', '1.01', '100000000000000000000', '200.02'),
                              ('10', '100', '12', '144'), ('1', '0', '2', '0.01')]:
            out = m.build_manager(fixture([position('111111111', q0, v0)], [position('111111111', q1, v1)]))
            row = out['rows'][0]['bridge']; change = Fraction(v1)-Fraction(v0)
            self.assertEqual(value(row['quantity_term_usd'])+value(row['unit_value_term_usd']), change)
            displayed = Fraction(row['quantity_term_usd']['display_decimal'])+Fraction(row['unit_value_term_usd']['display_decimal'])
            self.assertEqual(displayed+value(row['display_rounding_residual_usd']), change)
        self.assertEqual(m.rounded(Fraction('0.125')), '0.12'); self.assertEqual(m.rounded(Fraction('0.135')), '0.14')
        self.assertEqual(m.rounded(Fraction('-0.125')), '-0.12')

    def test_split_shaped_disclosure_does_not_become_a_purchase_or_sale(self):
        out = m.build_manager(fixture([position('111111111', 10, 200)], [position('111111111', 20, 200)]))
        row = out['rows'][0]['bridge']
        self.assertEqual(value(row['quantity_term_usd']), 150)
        self.assertEqual(value(row['unit_value_term_usd']), -150)
        self.assertEqual(value(row['reported_value_change_usd']), 0)
        self.assertFalse(out['corporate_actions_adjusted']); self.assertIsNone(out['call'])

    def test_options_principal_and_zero_quantities_remain_visible_without_unit_value_bridge(self):
        before = [position('111111111', 1, 10, option='CALL'), position('222222222', 100, 50, kind='PRN'), position('333333333', 0, 0)]
        current = [position('111111111', 2, 20, option='CALL'), position('222222222', 120, 55, kind='PRN'), position('333333333', 1, 5)]
        out = m.build_manager(fixture(before, current))
        self.assertEqual(out['record_count'], 3)
        self.assertEqual(out['bridge_status_counts'], {'option_or_principal_scope_not_decomposed': 2, 'zero_quantity_unit_value_unavailable': 1})
        self.assertEqual(set(out['scopes']), {'SH|CALL', 'PRN|NONE', 'SH|NONE'})
        self.assertTrue(all(v['sum_display_quantity_terms_usd'] is None for v in out['scopes'].values()))

    def test_large_manager_scope_has_bounded_currency_totals_and_exact_security_terms(self):
        before = [position(str(i).zfill(9), 10001+i, '1') for i in range(2048)]
        current = [position(str(i).zfill(9), 10002+i, '3') for i in range(2048)]
        out = m.build_manager(fixture(before, current)); scope = out['scopes']['SH|NONE']
        self.assertEqual(out['record_count'], 2048)
        self.assertEqual(value(scope['change_in_reported_table_value_usd']), 4096)
        self.assertEqual(sum(value(scope[k]) for k in ('sum_display_quantity_terms_usd', 'sum_display_unit_value_terms_usd',
                          'display_rounding_adjustment_usd', 'matched_value_change_not_decomposed_usd')), 4096)
        self.assertLess(len(scope['sum_display_quantity_terms_usd']['denominator']), 10)
        self.assertTrue(all(value(r['bridge']['quantity_term_usd']) > 0 for r in out['rows']))

    def test_full_table_bridge_retains_unmatched_identities_and_exact_components(self):
        out = m.build_manager(fixture([position('111111111', 10, 100), position('222222222', 3, 60)],
                                      [position('111111111', 12, 144), position('333333333', 4, 20)]))
        scope = out['scopes']['SH|NONE']
        self.assertEqual(out['record_count'], 3)
        self.assertEqual(value(scope['change_in_reported_table_value_usd']), 4)
        self.assertEqual(value(scope['matched_reported_value_change_usd']), 44)
        self.assertEqual(value(scope['newly_disclosed_reported_value_usd']), 20)
        self.assertEqual(value(scope['absent_prior_reported_value_usd']), 60)
        self.assertTrue(all(row['reported_value_change_usd'] is None for row in out['rows'] if row['status'] != 'matched_public_identity'))

    def test_missing_prior_chain_is_not_an_empty_prior_portfolio(self):
        detail = fixture([], [position('111111111', 12, 144)])
        complete_empty = m.build_manager(detail)
        self.assertEqual(value(complete_empty['scopes']['SH|NONE']['newly_disclosed_reported_value_usd']), 144)
        detail['periods']['2026-03-31'] = {'status': 'not_acquired', 'positions': {}}
        missing = m.build_manager(detail); scope = missing['scopes']['SH|NONE']
        self.assertFalse(missing['comparison_available'])
        self.assertIsNone(scope['prior_reported_value_usd']); self.assertIsNone(scope['change_in_reported_table_value_usd'])
        self.assertEqual(missing['rows'][0]['status'], 'comparison_unavailable')
        self.assertEqual(missing['rows'][0]['bridge']['status'], 'comparison_unavailable')

    def test_valuation_review_identity_corruption_and_incomplete_table_fail_safely(self):
        detail = fixture([position('111111111', 10, 100)], [position('111111111', 12, 144)])
        reviewed = deepcopy(detail); reviewed['periods']['2026-06-30']['valuation_reviews'] = ['Unresolved source value issue']
        out = m.build_manager(reviewed)
        self.assertEqual(out['rows'][0]['bridge']['status'], 'reported_value_requires_review')
        self.assertEqual(value(out['scopes']['SH|NONE']['matched_value_change_not_decomposed_usd']), 44)
        broken = deepcopy(detail); next(iter(broken['periods']['2026-06-30']['positions'].values()))['identity']['class'] = 'ALTERED'
        with self.assertRaisesRegex(ValueError, 'identity'): m.build_manager(broken)
        broken = deepcopy(detail); broken['periods']['2026-06-30']['reported_value_usd'] = '145'
        with self.assertRaisesRegex(ValueError, 'table value'): m.build_manager(broken)
        broken = deepcopy(detail); broken['periods']['2026-06-30']['status'] = 'amendment_number_gap_or_duplicate_original'
        with self.assertRaisesRegex(ValueError, 'Incomplete'): m.build_manager(broken)
        with self.assertRaises(ValueError): m.exact('-1')
        with self.assertRaises(ValueError): m.exact('NaN')


if __name__ == '__main__': unittest.main()
