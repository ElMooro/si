from pathlib import Path
import sys, unittest, importlib.util
from io import BytesIO
from unittest.mock import Mock
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'aws/ops/checks'))
from financial_statement_inventory import inventory, records


class Tests(unittest.TestCase):
    def test_full_population_keeps_zero_missing_values_duplicates_and_unsupported_lineage(self):
        rows = [{'symbol': 'ABC', 'm_score': 0, 'concern_score': 0, 'date': '2026-08-31',
            'reportedCurrency': 'EUR', 'source': {'claim': 'unverified'}, 'components_missing': ['TATA'],
            'factors': dict.fromkeys(('DSRI', 'GMI', 'AQI', 'SGI', 'DEPI', 'SGAI', 'LVGI', 'TATA'), 0)},
            {'symbol': 'ABC', 'm_score': None, 'date': '2026-02-30'}, {'symbol': '', 'm_score': float('nan')},
            {'symbol': 'XYZ', 'm_score': True}]
        result = inventory({'all_results': rows, 'generated_at': '2026-09-25T00:00:00Z'}, 'forensic')
        self.assertEqual(result['rows'], 4)
        self.assertEqual(result['repeated_reported_labels'], {'ABC': 2})
        self.assertEqual(result['missing_reported_labels'], 1)
        self.assertEqual(result['rows_with_m_score'], 1)
        self.assertEqual(result['rows_with_all_eight_reported_beneish_factors'], 1)
        self.assertEqual(result['rows_with_recognized_date_field'], 1)
        self.assertEqual(result['rows_with_currency_field'], 1)
        self.assertEqual(result['nonfinite_values_in_whole_packet'], 1)
        self.assertFalse(result['original_statement_responses_verified'])
        self.assertFalse(result['forecast_qualified'])

    def test_all_source_shapes_refuse_partial_or_ambiguous_populations(self):
        self.assertEqual(records({'tickers': {'ABC': {'flag': True}, 'XYZ': {}}}, 'share_flows')[1]['reported_map_key'], 'XYZ')
        self.assertEqual(inventory({'book': [], 'logged': 0}, 'short_book')['reported_logged_signals'], 0)
        for packet, kind in (({'rows': [], 'stocks': []}, 'universe'), ({'tickers': {'ABC': None}}, 'share_flows'),
                ({'all_results': [{} , None]}, 'forensic'), ({}, 'universe'), ({}, 'unknown')):
            with self.assertRaises(ValueError):
                records(packet, kind)

    def test_baseline_preserves_whole_bytes_and_excludes_unreviewed_reads(self):
        path = ROOT / 'aws/ops/staged/ops_6071_financial_statement_source_inventory.py'
        spec = importlib.util.spec_from_file_location('financial_baseline_test', path)
        mod = importlib.util.module_from_spec(spec); spec.loader.exec_module(mod)
        body = b'{"zero":0,"missing":null,"whole_predecessor":"retained"}\r\n'
        client = Mock(); client.get_object.return_value = {'Body': BytesIO(body)}
        ref = mod.protect(client, body)
        sent = client.put_object.call_args.kwargs
        self.assertEqual(sent['Body'], body)
        self.assertEqual(sent['IfNoneMatch'], '*')
        self.assertEqual(ref['sha256'], mod.sha(body))
        self.assertEqual(ref['bytes'], len(body))
        self.assertTrue(sent['Key'].startswith(mod.PRIVATE))
        for key in ('accounts/current.json', 'data/trade-tickets.json', 'data/proven-portfolio.json', '../forensic.json'):
            with self.assertRaises(ValueError): mod.read(client, key)
        self.assertEqual(client.get_object.call_count, 1)
        client.get_object.return_value = {'Body': BytesIO(body + b' ')}
        with self.assertRaises(AssertionError): mod.protect(client, body)


if __name__ == '__main__':
    unittest.main(verbosity=2)
