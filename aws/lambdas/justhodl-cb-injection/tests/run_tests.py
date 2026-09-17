import importlib.util
import sys
import types
import unittest
from datetime import date
from pathlib import Path
from unittest.mock import patch

with patch.dict(sys.modules, {'boto3': types.SimpleNamespace(client=lambda *a, **k: None),
                             'managed_secret': types.SimpleNamespace(managed_secret=lambda *a, **k: 'test'),
                             '_fred_shim': types.SimpleNamespace()}):
    spec = importlib.util.spec_from_file_location('cb_test', Path(__file__).resolve().parents[1] / 'source/lambda_function.py')
    e = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(e)

NOW = date(2026, 9, 17)
SOURCES = {'total_assets': 'total', 'securities': 'securities', 'lending': 'lending'}


def stocks():
    return {'total_assets': [('2026-09-16', 120), ('2026-08-12', 100)],
            'securities': [('2026-09-16', 80), ('2026-08-12', 75)],
            'lending': [('2026-09-16', 10), ('2026-08-12', 5)]}


class CentralBankMeasurements(unittest.TestCase):
    def test_stock_components_reconcile_but_do_not_identify_transactions(self):
        out = e.decompose(stocks(), 'USD_bn', SOURCES, NOW)
        self.assertEqual(out['stock_change_1m'], 20)
        self.assertEqual(out['other_assets_and_adjustments_change_1m'], 10)
        self.assertEqual(out['other_assets_and_adjustments_level'], 30)
        self.assertEqual(out['reconciliation_residual'], 0)
        for key in ('net_injection_estimate', 'fx_valuation_change_1m', 'policy_purchase_transactions_1m'):
            self.assertIsNone(out[key])

    def test_rate_cut_alone_is_not_a_cash_injection(self):
        out = e.build_measurements({'FED_RATE': [('2026-09-16', 3), ('2026-08-12', 4)]}, {}, NOW)
        self.assertIsNone(out['global_injection_impulse']['score'])
        self.assertIsNone(out['central_banks'][0]['injection_stance'])
        self.assertIsNone(out['call'])
        self.assertFalse(out['execution_eligible'])

    def test_required_missing_component_cannot_be_zero(self):
        rows = stocks(); del rows['lending']
        out = e.decompose(rows, 'USD_bn', SOURCES, NOW)
        self.assertEqual(out['status'], 'incomplete')
        self.assertIsNone(out['stock_change_1m'])
        self.assertIsNone(out['components']['lending']['latest'])

    def test_components_require_common_dates(self):
        rows = stocks()
        rows['lending'] = [('2026-09-15', 10), ('2026-08-11', 5)]
        out = e.decompose(rows, 'USD_bn', SOURCES, NOW)
        self.assertIsNone(out['stock_change_1m'])

    def test_components_exceeding_total_are_rejected(self):
        rows = stocks(); rows['securities'][0] = ('2026-09-16', 125)
        self.assertEqual(e.decompose(rows, 'USD_bn', SOURCES, NOW)['status'], 'invalid_components_exceed_total')
        rows = stocks(); rows['securities'][1] = ('2026-08-12', 110)
        self.assertEqual(e.decompose(rows, 'USD_bn', SOURCES, NOW)['status'], 'invalid_components_exceed_total')

    def test_previous_requires_calendar_endpoint(self):
        rows = [('2026-09-16', 120), ('2026-09-09', 100), ('2025-08-12', 70)]
        self.assertIsNone(e.measure(rows, 'weekly', 'USD_bn', 'test', today=NOW)['changes']['1']['level_change'])

    def test_stale_future_and_nonfinite_are_withheld(self):
        for rows, status in [([('2026-01-01', 100)], 'stale'), ([('2026-10-01', 100)], 'invalid'),
                             ([('2026-09-16', float('nan'))], 'invalid')]:
            out = e.measure(rows, 'weekly', 'USD_bn', 'test', today=NOW)
            self.assertEqual(out['quality']['status'], status)
            self.assertIsNone(out['latest'])

    def test_missing_all_banks_is_unavailable(self):
        out = e.build_measurements({}, {}, NOW)
        self.assertFalse(out['ok'])
        self.assertEqual(out['quality']['status'], 'unavailable')
        self.assertEqual(out['central_banks'][3]['decomposition']['status'], 'unavailable')
        self.assertIsNone(out['central_banks'][3]['balance_sheet']['latest'])

    def test_japan_units_and_interbank_definition(self):
        out = e.build_measurements({'BOJ_BS': [('2026-08-01', 6000000)],
                                    'BOJ_RATE': [('2026-07-01', 1.5)]}, {}, NOW)
        boj = out['central_banks'][2]
        self.assertEqual(boj['balance_sheet']['latest'], 600000)
        self.assertEqual(boj['balance_sheet']['unit'], 'JPY_bn')
        self.assertIsNone(boj['policy_rate_pct'])
        self.assertEqual(boj['interbank_proxy_pct'], 1.5)

    def test_unvalidated_ecb_archive_is_not_used(self):
        with patch.object(e, 'read_existing', return_value={'unit': 'EUR_bn', 'points': [['2026-09-11', 5]]}):
            self.assertEqual(e.ecb_archive('total_assets'), [])


if __name__ == '__main__':
    unittest.main()
