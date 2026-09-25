from pathlib import Path
from datetime import date, timedelta
from decimal import Decimal
from fractions import Fraction
import sys, unittest
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'aws/shared'))
import short_volume_measurements as m

DATES = [(date(2026, 7, 26) + timedelta(days=i)).isoformat() for i in range(61)]


def points():
    return [[i, 2, str(i + 1), '0.000001', str((i + 2) * 2), 'B,Q,N'] for i in range(61)]


class Tests(unittest.TestCase):
    def test_independent_fraction_mean_pooled_and_sample_variance(self):
        data = points()
        out = m.comparisons('AAPL', data, DATES)
        for size in m.WINDOWS:
            prior = data[60 - size:60]
            ratios = [Fraction(p[2]) * 100 / Fraction(p[4]) for p in prior]
            mean = sum(ratios) / size
            variance = sum((v - mean) ** 2 for v in ratios) / (size - 1)
            pooled = sum(Fraction(p[2]) for p in prior) * 100 / sum(Fraction(p[4]) for p in prior)
            window = out['windows'][str(size)]
            self.assertLessEqual(abs(Fraction(window['mean_daily_short_volume_pct']) - mean), Fraction(1, 2 * 10**12))
            self.assertLessEqual(abs(Fraction(window['pooled_short_volume_pct']) - pooled), Fraction(1, 2 * 10**12))
            sd = Decimal(window['sample_sd_percentage_points'])
            self.assertAlmostEqual(float(sd * sd), float(variance), places=9)
            self.assertNotEqual(window['pooled_short_volume_pct'], window['mean_daily_short_volume_pct'])
            self.assertTrue(window['excludes_latest'])
        self.assertEqual(out['points'], data)
        for key in ('short_interest_shares', 'days_to_cover', 'squeeze_score'):
            self.assertIsNone(out[key])

    def test_latest_extreme_cannot_leak_into_baseline(self):
        data = [[i, 2, '5', '1', '10', 'B'] for i in range(61)]
        data[-1][2] = '10'
        out = m.comparisons('A', data, DATES)['windows']['60']
        self.assertEqual(out['mean_daily_short_volume_pct'], '50.000000000000')
        self.assertEqual(out['latest_minus_mean_percentage_points'], '50.000000000000')
        self.assertIsNone(out['descriptive_z_score'])
        self.assertEqual(out['z_score_missing_reason'], 'zero_prior_variance')

    def test_constant_repeating_ratios_have_exactly_zero_variance(self):
        for numerator, denominator in (('1', '3'), ('2', '7'), ('13.000001', '71.333333')):
            data = [[i, 2, numerator, '0', denominator, 'B'] for i in range(61)]
            data[-1][2] = denominator
            out = m.comparisons('A', data, DATES)
            for window in out['windows'].values():
                self.assertEqual(window['sample_sd_percentage_points'], '0.000000000000')
                self.assertIsNone(window['descriptive_z_score'])
                self.assertEqual(window['z_score_missing_reason'], 'zero_prior_variance')

    def test_tiny_real_variance_is_not_floored_or_mistaken_for_a_probability(self):
        data = [[i, 2, '0.000000000001', '0', '999999999999999999999999999999.000000000001', 'B'] for i in range(61)]
        data[58][4] = '999999999999999999999999999999.000000000002'
        data[-1][2] = data[-1][4]
        out = m.comparisons('A', data, DATES)['windows']['60']
        self.assertGreater(Decimal(out['descriptive_z_score']), Decimal('1e70'))
        self.assertTrue(out['sample_sd_below_display_precision'])
        self.assertGreater(Decimal(out['sample_sd_scientific']), 0)
        self.assertIsNone(out['z_score_missing_reason'])
        self.assertIsNone(out['probability_interpretation'])

    def test_missing_rows_do_not_shorten_sixty_file_window(self):
        data = points()[1:]
        out = m.comparisons('A', data, DATES)
        self.assertTrue(out['windows']['20']['rows_complete'])
        prior = out['windows']['60']
        self.assertEqual(prior['missing_dates'], [DATES[0]])
        self.assertEqual(prior['observed_rows'], 59)
        for key in ('mean_daily_short_volume_pct', 'pooled_short_volume_pct', 'short_volume_shares', 'descriptive_z_score'):
            self.assertIsNone(prior[key])

    def test_zero_volume_rows_preserve_sums_and_null_daily_statistics(self):
        data = points()
        data[-2][2:5] = ['0.000000', '0', '0.000000']
        window = m.comparisons('A', data, DATES)['windows']['5']
        self.assertTrue(window['rows_complete'])
        self.assertFalse(window['daily_ratios_complete'])
        self.assertEqual(window['zero_volume_dates'], [DATES[-2]])
        self.assertIsNone(window['mean_daily_short_volume_pct'])
        self.assertIsNotNone(window['pooled_short_volume_pct'])
        self.assertEqual(window['positive_volume_rows'], 4)

    def test_latest_absence_and_zero_do_not_resurrect_old_value(self):
        out = m.comparisons('A', points()[:-1], DATES)
        self.assertFalse(out['latest_point_present'])
        self.assertIsNone(out['latest_short_volume_pct'])
        self.assertIsNotNone(out['windows']['60']['mean_daily_short_volume_pct'])
        self.assertIsNone(out['windows']['60']['latest_minus_mean_percentage_points'])
        data = points()
        data[-1][2:5] = ['0', '0', '0']
        self.assertEqual(m.comparisons('A', data, DATES)['latest_missing_reason'], 'zero_reported_volume')

    def test_duplicate_dates_points_invalid_scope_and_reconciliation_fail(self):
        cases = [points() + [points()[-1]], points()[::-1]]
        for column, value in ((0, True), (1, 1), (2, '99999'), (3, '99999'), (4, 'NaN'), (5, 'X')):
            data = points()
            data[0][column] = value
            cases.append(data)
        for data in cases:
            with self.assertRaises(ValueError):
                m.comparisons('A', data, DATES)
        with self.assertRaises(ValueError):
            m.comparisons('A', points(), DATES[:-1] + [DATES[0]])

    def test_facility_changes_and_literal_symbol_continuity_are_explicit(self):
        data = points()
        data[-1][-1] = 'B'
        out = m.comparisons('ABC.A', data, DATES)
        self.assertFalse(out['windows']['60']['reported_facility_scope_consistent'])
        self.assertFalse(out['security_identity_continuity_verified'])
        self.assertFalse(out['calls_eligible'])

    def test_fractional_precision_and_exempt_not_double_counted(self):
        data = [[i, 2, '0.100001', '0.000001', '0.200002', 'B'] for i in range(61)]
        out = m.comparisons('A', data, DATES)
        self.assertEqual(out['latest_short_volume_pct'], '50.000000000000')
        self.assertEqual(Decimal(out['windows']['60']['short_volume_shares']), Decimal('6.000060'))


if __name__ == '__main__':
    unittest.main(verbosity=2)
