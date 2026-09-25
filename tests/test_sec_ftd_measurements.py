from pathlib import Path
from decimal import localcontext, ROUND_DOWN
from fractions import Fraction
import sys, unittest
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'aws/shared'))
import sec_ftd_measurements as m
DATES = ['2026-08-17', '2026-08-18', '2026-08-19']


def point(day, amount, symbol='ABC', description='Example class'):
    return m.compact_point({'source_line': 2, 'settlement_date': day, 'symbol': symbol,
                            'description': description, 'fail_balance_shares': str(amount), 'reported_price': '.'}, 0)


def values(history):
    return [dict(zip(m.POINT_FIELDS, p)) for p in history['observations']]


class Tests(unittest.TestCase):
    def test_adjacent_balance_difference_is_exact_and_not_a_new_fail_flow(self):
        record = m.history('001234567', [point(DATES[0], 300), point(DATES[1], 200)], DATES)
        before, after = values(record)
        self.assertIsNone(before['adjacent_balance_change_shares'])
        self.assertEqual(after['adjacent_balance_change_shares'], '-100')
        self.assertLessEqual(abs(Fraction(after['adjacent_balance_change_pct']) - Fraction(-100, 3)), Fraction(1, 2*10**12))
        self.assertFalse(record['balances_summed_across_dates'])
        self.assertFalse(record['security_identity_continuity_verified'])
        self.assertEqual(record['missing_reported_dates'], [DATES[2]])
        self.assertTrue(all(record[k] is False for k in m.FLAGS))

    def test_absent_previous_record_is_not_zero_and_breaks_the_comparison(self):
        damaged = point(DATES[2], 999)
        damaged[10:12] = ['999', '999']
        record = m.history('001234567', [point(DATES[0], 100), damaged], DATES)
        last = values(record)[-1]
        self.assertEqual(last['previous_reported_date'], DATES[1])
        self.assertFalse(last['previous_record_present'])
        self.assertIsNone(last['previous_fail_balance_shares'])
        self.assertIsNone(last['adjacent_balance_change_shares'])
        self.assertIsNone(last['adjacent_balance_change_pct'])
        self.assertEqual(last['comparison_status'], 'prior_cusip_record_not_reported')
        self.assertFalse(record['missing_records_imputed_zero'])

    def test_changed_reported_label_never_implies_verified_security_continuity(self):
        record = m.history('001234567', [point(DATES[0], 100), point(DATES[1], 50, symbol='XYZ')], DATES)
        value = values(record)[-1]
        self.assertEqual(value['previous_fail_balance_shares'], '100')
        self.assertIsNone(value['adjacent_balance_change_shares'])
        self.assertEqual(value['comparison_status'], 'reported_label_changed_comparison_unqualified')
        self.assertEqual(len(record['reported_labels']), 2)

    def test_reported_zero_previous_keeps_percentage_unavailable(self):
        last = values(m.history('001234567', [point(DATES[0], 0), point(DATES[1], 50)], DATES))[-1]
        self.assertEqual(last['adjacent_balance_change_shares'], '50')
        self.assertIsNone(last['adjacent_balance_change_pct'])
        self.assertEqual(last['comparison_status'], 'reported_previous_balance_zero')

    def test_duplicate_dates_and_invalid_quantities_cannot_silently_overwrite(self):
        for points in ([point(DATES[0], 1), point(DATES[0], 2)], [point(DATES[0], '-1')], [point(DATES[0], 'NaN')]):
            with self.assertRaises(ValueError):
                m.history('001234567', points, DATES)
        self.assertNotEqual(m.record_id('001234567'), m.record_id('901234567'))
        with self.assertRaises(ValueError):
            m.record_id('1234567')

    def test_large_integer_math_is_independent_of_callers_decimal_context(self):
        previous = 10**160
        with localcontext() as ctx:
            ctx.prec = 4
            ctx.rounding = ROUND_DOWN
            last = values(m.history('001234567', [point(DATES[0], previous), point(DATES[1], previous*3)], DATES))[-1]
        self.assertEqual(last['adjacent_balance_change_shares'], str(previous*2))
        self.assertEqual(last['adjacent_balance_change_pct'], '200.000000000000')


if __name__ == '__main__':
    unittest.main(verbosity=2)
