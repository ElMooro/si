from pathlib import Path
from unittest.mock import patch
import sys, unittest
ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT / p) for p in ('aws/ops/checks', 'aws/ops/staged', 'tests')]
import ops_6067_sec_all_archive_field_diagnosis as audit
from test_sec_ftd_source_baseline import TEXT, zipped


class Tests(unittest.TestCase):
    def test_unreviewed_names_are_inspected_as_bytes_without_filesystem_extraction(self):
        for name in ('reported.TXT', 'cns fails.txt', '../reported.txt'):
            with self.subTest(name=name), patch('zipfile.ZipFile.extractall', side_effect=AssertionError('No extraction')):
                result = audit.inspect(zipped(TEXT, name=name))
            self.assertEqual(result['member']['name'], name)
            self.assertEqual(result['rows'], 3)
            self.assertTrue(result['record_count_matches'])
            self.assertTrue(result['quantity_checksum_matches'])
            self.assertFalse(result['source_schema_qualified'])

    def test_every_identity_anomaly_is_counted_with_original_row_positions(self):
        body = TEXT.replace(b'001234567|ABC|123|Reported class', b'?????????||123|')
        result = audit.inspect(zipped(body))
        self.assertEqual(result['diagnostic_counts'], {'cusip_format': 1, 'symbol_empty': 1, 'description_empty': 1})
        self.assertEqual(result['diagnostic_examples']['description_empty'][0]['source_line'], 2)
        self.assertTrue(result['quantity_checksum_matches'])
        self.assertFalse(result['rows_discarded'])

    def test_multiple_members_remain_a_hard_bound(self):
        with self.assertRaises(AssertionError):
            audit.inspect(zipped(TEXT, extra=('another.txt', TEXT)))


if __name__ == '__main__':
    unittest.main()
