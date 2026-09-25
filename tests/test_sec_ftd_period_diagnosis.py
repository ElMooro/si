from pathlib import Path
import sys, unittest
ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT / p) for p in ('aws/ops/checks', 'aws/ops/staged', 'tests')]
import ops_6063_sec_archive_period_diagnosis as audit
from test_sec_ftd_source_baseline import zipped, TEXT


class Tests(unittest.TestCase):
    def test_wrong_month_is_exposed_without_relabelling_or_dropping_original_rows(self):
        result = audit.inspect(zipped(), audit.URL)
        self.assertEqual(result['rows'], 3)
        self.assertEqual(result['outside_advertised_period'], {'2026-08-17': 1, '2026-08-18': 2})
        self.assertTrue(result['record_count_matches'])
        self.assertTrue(result['quantity_checksum_matches'])
        self.assertEqual(result['first_data_row']['source_line'], 2)
        self.assertFalse(result['source_period_qualified'])
        self.assertFalse(result['source_rows_relabelled'])

    def test_exact_half_month_boundary_violation_is_visible(self):
        body = TEXT.replace(b'20260817', b'20260715').replace(b'20260818', b'20260716')
        result = audit.inspect(zipped(body), audit.URL)
        self.assertEqual(result['outside_advertised_period'], {'2026-07-15': 1})
        self.assertEqual(result['dates'], {'2026-07-15': 1, '2026-07-16': 2})
        self.assertEqual(result['outside_examples'][0]['fields'][0], '20260715')


if __name__ == '__main__':
    unittest.main()
