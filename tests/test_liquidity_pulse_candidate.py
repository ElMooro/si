"""Offline original-row checks plus explicit synthetic boundary cases."""
from pathlib import Path
from copy import deepcopy
from datetime import timedelta
from decimal import localcontext, ROUND_UP
import sys, unittest
ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT/p) for p in ('aws/ops/checks', 'aws/shared', 'aws/shared/tests',
    'scripts', 'aws/lambdas/justhodl-crisis-composite/tests')]
import liquidity_pulse_candidate as model
import verify_liquidity_pulse_arithmetic as independent
from test_native_research import fixtures
from test_report_observations import inputs, NOW


def synthetic():
    originals = {}
    for sid, (_, unit, frequency) in model.SPECS.items():
        rows = [('2026-08-01', '5.5'), ('2026-07-01', '5.0'), ('2025-08-01', '4.0')] if frequency == 'M' else [
            ('2026-09-16', '0'), ('2026-09-09', '0'), ('2026-08-12', '2.5'), ('2026-06-10', '2.0'), ('2025-09-10', '1.0')]
        originals[sid] = inputs(sid, frequency, rows, unit)
        originals[sid]['definition']['seriess'][0]['frequency'] = ('Monthly' if frequency == 'M' else
            'Daily' if sid == 'BAMLEMHBHYCRPIOAS' else 'Daily, Close' if frequency == 'D' else
            'Weekly, Ending Wednesday' if sid in ('WRESBAL', 'WTREGEN') else 'Weekly, As of Wednesday')
    return recompile(originals), originals


def recompile(originals):
    source = model.observations.build({sid: {} for sid in originals}, originals, NOW)
    source['replay'] = {'manifest_key': 'data/report-research/runs/'+'a'*64+'.json',
        'output_sha256': model.observations.digest(source)}
    return source


class Tests(unittest.TestCase):
    def test_retained_real_inputs_conserve_rows_and_keep_seven_uncollected_series_unavailable(self):
        source, originals = fixtures(); out = model.build(source, originals, source['generated_at'])
        proof = independent.verify(out, source, originals)
        self.assertEqual(proof['original_rows'], 6388); self.assertEqual(proof['missing_series'], 7)
        self.assertEqual(proof['calendar_comparisons'], 16); self.assertEqual(proof['rational_checks'], 92)
        self.assertTrue(out['candidate_only']); self.assertFalse(out['publication_eligible'])

    def test_all_eleven_series_and_native_calendar_units_without_classification(self):
        source, originals = synthetic(); before = deepcopy((source, originals)); out = model.build(source, originals, NOW)
        proof = independent.verify(out, source, originals); self.assertEqual(proof['fresh_series'], 11)
        self.assertEqual(proof['calendar_comparisons'], 44)
        self.assertEqual(out['series']['HQMCB10YR']['group'], 'yield')
        self.assertEqual(out['series']['HQMCB10YR']['frequency'], 'M')
        self.assertIsNone(out['series']['HQMCB10YR']['deltas']['wow_pct'])
        self.assertEqual(out['series']['HQMCB10YR']['calendar_comparisons']['month']['change_decimal'], '0.5')
        self.assertEqual(out['series']['HQMCB10YR']['calendar_comparisons']['month']['change_unit'], 'percentage_points')
        self.assertEqual(out['series']['SWP1690']['latest_value'], 0)
        self.assertIsNone(out['series']['SWP1690']['deltas']['wow_pct'])  # Zero denominator, not a zero-percent claim.
        self.assertEqual(before, (source, originals))

    def test_missing_latest_never_backfills_and_future_rows_remain_retained_without_becoming_current(self):
        _, originals = synthetic(); originals['WALCL']['observations']['observations'][0]['value'] = '.'
        original = originals['SWP1690']; original['observations']['observations'].insert(0, {'date': '2099-01-01', 'value': '999'})
        original['observations']['count'] += 1
        source = recompile(originals); out = model.build(source, originals, NOW); independent.verify(out, source, originals)
        self.assertIsNone(out['series']['WALCL']['latest_value']); self.assertEqual(out['series']['WALCL']['latest_date'], '2026-09-16')
        self.assertEqual(out['series']['SWP1690']['history'][0]['observation_date'], '2099-01-01')
        self.assertEqual(out['series']['SWP1690']['latest_value'], 0); self.assertEqual(out['series']['SWP1690']['source_row'], 1)

    def test_expiry_preserves_whole_histories_but_withholds_current_values_and_aliases(self):
        source, originals = synthetic(); before = model.build(source, originals, NOW)
        stamp = (model.clock(NOW)+timedelta(hours=27)).isoformat(); out = model.build(source, originals, stamp)
        proof = independent.verify(out, source, originals); self.assertEqual(proof['fresh_series'], 0)
        for sid, row in out['series'].items():
            self.assertEqual(row['history'], before['series'][sid]['history']); self.assertIsNone(row['latest_value'])
            self.assertFalse(row['calendar_comparisons']); self.assertTrue(all(v is None for v in row['deltas'].values()))

    def test_modified_original_or_rehashed_measurement_cannot_be_accepted(self):
        for kind in ('original', 'measurement'):
            source, originals = synthetic()
            if kind == 'original': originals['WALCL']['observations']['observations'][0]['value'] = '999'
            else:
                source['measurements']['WALCL']['current'] = 999
                source['replay']['output_sha256'] = model.observations.digest({k:v for k,v in source.items() if k != 'replay'})
            with self.assertRaisesRegex(ValueError, 'Original reconstruction'): model.build(source, originals, NOW)

    def test_unit_definition_mismatch_has_no_current_authority(self):
        _, originals = synthetic(); originals['WALCL']['definition']['seriess'][0]['units'] = 'Billions of Dollars'
        source = recompile(originals); out = model.build(source, originals, NOW); independent.verify(out, source, originals)
        self.assertEqual(out['series']['WALCL']['quality']['status'], 'definition_mismatch')
        self.assertIsNone(out['series']['WALCL']['latest_value']); self.assertEqual(out['series']['WALCL']['unit'], 'Billions of Dollars')

    def test_all_missing_sources_abstain_without_binding_unrelated_originals(self):
        source = recompile({}); out = model.build(source, {}, NOW)
        proof = independent.verify(out, source, {})
        self.assertEqual(proof['missing_series'], 11); self.assertEqual(proof['original_rows'], 0)
        self.assertEqual(out['quality']['status'], 'unavailable')
        _, originals = synthetic()
        with self.assertRaisesRegex(ValueError, 'Unbound original'): model.build(source, originals, NOW)

    def test_frequency_drift_preserves_comparisons_but_never_current_authority(self):
        for frequency in ('BW', 'M', 'Q', 'SA', 'A'):
            _, originals = synthetic(); originals['WALCL']['definition']['seriess'][0]['frequency_short'] = frequency
            source = recompile(originals); out = model.build(source, originals, NOW)
            independent.verify(out, source, originals)
            self.assertEqual(out['series']['WALCL']['quality']['status'], 'definition_mismatch')
            self.assertIsNone(out['series']['WALCL']['latest_value'])
            self.assertEqual(len(out['series']['WALCL']['history']), 5)

    def test_seasonal_basis_drift_is_not_the_reviewed_measurement(self):
        _, originals = synthetic(); originals['WTREGEN']['definition']['seriess'][0]['seasonal_adjustment'] = 'Seasonally Adjusted'
        source = recompile(originals); out = model.build(source, originals, NOW)
        independent.verify(out, source, originals)
        self.assertEqual(out['series']['WTREGEN']['quality']['status'], 'definition_mismatch')
        self.assertIsNone(out['series']['WTREGEN']['latest_value'])

    def test_weekly_average_cannot_silently_replace_weekly_stock(self):
        _, originals = synthetic(); originals['WALCL']['definition']['seriess'][0]['frequency'] = 'Weekly, Ending Wednesday'
        source = recompile(originals); out = model.build(source, originals, NOW)
        independent.verify(out, source, originals)
        self.assertEqual(out['series']['WALCL']['quality']['status'], 'definition_mismatch')
        self.assertIsNone(out['series']['WALCL']['latest_value'])

    def test_new_wrapper_cannot_renew_expired_original_acquisition(self):
        _, originals = synthetic()
        originals['WALCL']['acquired_at'] = (model.clock(NOW)-timedelta(hours=27)).isoformat()
        source = recompile(originals); out = model.build(source, originals, NOW)
        proof = independent.verify(out, source, originals)
        self.assertEqual(proof['fresh_series'], 10); self.assertIsNone(out['series']['WALCL']['latest_value'])
        self.assertTrue(out['series']['WALCL']['history']); self.assertEqual(out['quality']['status'], 'degraded')

    def test_fixed_arithmetic_and_future_source_rejection(self):
        source, originals = synthetic(); expected = model.build(source, originals, NOW)
        with localcontext() as ctx:
            ctx.prec = 4; ctx.rounding = ROUND_UP
            self.assertEqual(model.build(source, originals, NOW), expected)
        with self.assertRaisesRegex(ValueError, 'Future source'): model.build(source, originals, '2000-01-01T00:00:00Z')

    def test_independent_verifier_rejects_number_date_row_and_authority_tampering(self):
        source, originals = synthetic(); base = model.build(source, originals, NOW)
        mutations = [lambda o: o.update(calls_eligible=True),
            lambda o: o['series']['WALCL'].update(calls_eligible=True),
            lambda o: o['series']['WALCL'].update(frequency='M'),
            lambda o: o['quality'].update(status='unavailable'),
            lambda o: o['series']['WALCL'].update(latest_value=9),
            lambda o: o['series']['WALCL']['history'][0].update(native_value='9'),
            lambda o: o['series']['HQMCB10YR']['calendar_comparisons']['month'].update(baseline_date='2026-06-01')]
        for change in mutations:
            out = deepcopy(base); change(out)
            with self.assertRaises(AssertionError): independent.verify(out, source, originals)


if __name__ == '__main__': unittest.main(verbosity=2)
