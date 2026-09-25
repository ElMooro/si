from pathlib import Path
import sys, unittest
ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT / p) for p in ('aws/ops/checks', 'aws/ops/staged', 'tests')]
import ops_6065_sec_complete_row_diagnosis as audit
from test_sec_ftd_source_baseline import TEXT, zipped


class Tests(unittest.TestCase):
    def test_missing_labels_and_invalid_identity_are_counted_without_dropping_quantities(self):
        body = TEXT.replace(b'001234567|ABC|123|Reported class', b'?????????||123|')
        result = audit.inspect(zipped(body))
        self.assertEqual(result['rows'], 3)
        self.assertEqual(result['diagnostic_counts'], {'cusip_format': 1, 'symbol_empty': 1, 'description_empty': 1})
        self.assertTrue(result['quantity_checksum_matches'])
        self.assertEqual(result['diagnostic_examples']['symbol_empty'][0]['source_line'], 2)
        self.assertFalse(result['rows_discarded'])
        self.assertFalse(result['source_fields_repaired'])

    def test_numeric_and_date_errors_remain_separate_from_identity_diagnostics(self):
        body = TEXT.replace(b'20260817', b'20261399').replace(b'|123|', b'|-1|').replace(b'|12.3456', b'|NaN')
        result = audit.inspect(zipped(body))
        self.assertEqual(result['diagnostic_counts'], {'settlement_format': 1, 'quantity_format': 1, 'price_format': 1})
        self.assertTrue(result['record_count_matches'])
        self.assertFalse(result['quantity_checksum_matches'])
        self.assertFalse(result['source_schema_qualified'])


if __name__ == '__main__':
    unittest.main()
