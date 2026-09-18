import copy
import unittest
from report_observations import build as source_build
from research_brief_model import build, digest, narrative_context

NOW = '2026-09-18T22:00:00+00:00'


def source_packet():
    from test_report_observations import inputs
    raw = {
        'ICSA': inputs('ICSA', 'W', [('2026-09-12', '196000'), ('2026-08-08', '212000')], 'Number'),
        'UNRATE': inputs('UNRATE', 'M', [('2026-08-01', '4.1'), ('2026-07-01', '4.3')], 'Percent'),
        'WALCL': inputs('WALCL', 'W', [('2026-09-16', '6746548')], 'Millions of U.S. Dollars'),
        'WTREGEN': inputs('WTREGEN', 'W', [('2026-09-16', '877028')], 'Millions of U.S. Dollars'),
        'RRPONTSYD': inputs('RRPONTSYD', 'D', [('2026-09-18', '0')], 'Billions of US Dollars'),
        'DTWEXBGS': inputs('DTWEXBGS', 'D', [('2026-09-11', '118.2126')], 'Index Jan 2006=100'),
        'VIXCLS': inputs('VIXCLS', 'D', [('2026-09-17', '15.44')], 'Index'),
    }
    packet = source_build({sid: {'display_name': sid, 'category': 'test'} for sid in raw}, raw, NOW)
    packet['replay'] = {'manifest_key': 'data/report-research/runs/'+'a'*64+'.json', 'output_sha256': digest(packet)}
    return packet


class ResearchBriefTests(unittest.TestCase):
    def test_claims_and_unemployment_have_exact_units_dates_and_baselines(self):
        source = source_packet(); original = copy.deepcopy(source)
        out = build(source, NOW); rows = {r['series_id']: r for r in out['metrics_table']}
        self.assertEqual(rows['ICSA']['value'], '196000')
        self.assertIn('196000 Number, observed 2026-09-12', rows['ICSA']['summary'])
        self.assertNotIn('196000K', str(out))
        self.assertIn('-0.2 percentage points', rows['UNRATE']['summary'])
        self.assertEqual(rows['UNRATE']['changes']['month']['baseline_date'], '2026-07-01')
        self.assertEqual(out['net_liquidity']['net_decimal'], '5869520')
        self.assertIsNone(out['net_liquidity']['direction'])
        self.assertEqual(source, original)
        self.assertEqual(digest(out), digest(build(source, NOW)))

    def test_strong_levels_and_forged_permissions_do_not_manufacture_calls(self):
        source = source_packet()
        source.update(khalid_index={'score': 99}, calls_eligible=True, sizing_eligible=True, call='LONG')
        out = build(source, NOW)
        self.assertEqual(out['decision']['verb'], 'WAIT')
        self.assertIsNone(out['decision']['confidence'])
        self.assertIsNone(out['scores']['khalid_index'])
        self.assertIsNone(out['dxy']['value'])
        self.assertEqual(out['portfolio']['allocation'], {})
        self.assertFalse(out['portfolio']['account_data_used'])
        self.assertFalse(out['sizing_eligible'])
        self.assertEqual(out['ml_intelligence']['trade_recommendations'], [])

    def test_fresh_wrapper_cannot_renew_old_source_or_observation(self):
        source = source_packet()
        out = build(source, '2026-09-20T22:00:00+00:00')
        self.assertEqual(out['quality']['fresh_series'], 0)
        self.assertTrue(all(r['value'] is None for r in out['metrics_table']))
        self.assertIsNone(out['net_liquidity']['net'])
        source['measurements']['ICSA']['date'] = '2025-09-12'
        rows = {r['series_id']: r for r in build(source, NOW)['metrics_table']}
        self.assertEqual(rows['ICSA']['status'], 'stale_observation')
        self.assertEqual(rows['ICSA']['last_observed_value'], '196000')
        self.assertNotIn('Change versus', rows['ICSA']['summary'])

    def test_missing_and_future_observations_and_catalog_gaps_stay_explicit(self):
        source = source_packet()
        source['measurements']['ICSA']['current_decimal'] = None
        source['measurements']['UNRATE']['date'] = '2027-01-01'
        source['catalog']['MISSING'] = {'display_name': 'Unavailable source'}
        source['errors']['MISSING'] = 'HTTP_400'
        out = build(source, NOW); rows = {r['series_id']: r for r in out['metrics_table']}
        self.assertIsNone(rows['ICSA']['value'])
        self.assertEqual(rows['UNRATE']['status'], 'invalid_clock')
        self.assertEqual(rows['MISSING']['reason'], 'HTTP_400')
        self.assertEqual(len(rows), 8)
        with self.assertRaises(ValueError): build(source, '2026-09-17T22:00:00Z')
        with self.assertRaises(ValueError): build({'version': 'V10', 'khalid_index': {'score': 50}}, NOW)

    def test_consumer_context_rechecks_age_and_never_accepts_permissions(self):
        packet = build(source_packet(), NOW)
        packet['sizing_eligible'] = packet['calls_eligible'] = True
        context = narrative_context(packet, NOW)
        self.assertEqual(context['status'], 'research_only')
        self.assertTrue(all(row['observed_at'] and row['unit'] for row in context['observations']))
        self.assertFalse(context['sizing_eligible'])
        self.assertIsNone(context['call'])
        later = narrative_context(packet, '2026-09-20T22:00:00Z')
        self.assertTrue(all(row['value'] is None and row['calendar_month_change'] is None for row in later['observations']))
        self.assertEqual(narrative_context({'phase': 'BULL', 'scores': {'khalid_index': 99}}, NOW)['observations'], [])


if __name__ == '__main__':
    unittest.main()
