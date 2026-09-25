from pathlib import Path
import sys, unittest
ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT / p) for p in ('aws/ops/checks', 'aws/ops/staged', 'tests')]
import ops_6059_sec_archive_row_diagnosis as audit
from test_sec_ftd_source_baseline import zipped, TEXT
PLAIN = TEXT.split(b'Trailer record count')[0]


class Tests(unittest.TestCase):
    def test_inspection_preserves_exact_positions_without_skipping_an_unexpected_row(self):
        output = audit.inspect(zipped(PLAIN + b'Unexpected trailer\n'))
        self.assertEqual(output['line_count'], 5)
        self.assertEqual(output['field_count_distribution'], {1: 1, 6: 4})
        self.assertEqual(output['sampled_non_schema_lines'], [
            {'source_line': 5, 'field_count': 1, 'line_characters': 18, 'fields': ['Unexpected trailer']}])
        self.assertFalse(output['source_schema_qualified'])

    def test_large_anomaly_is_counted_without_publishing_unbounded_line_contents(self):
        output = audit.inspect(zipped(PLAIN + b'x'*501 + b'\n'))
        row = output['sampled_non_schema_lines'][0]
        self.assertEqual(row['line_characters'], 501)
        self.assertNotIn('fields', row)
        self.assertEqual(row['source_line'], 5)


if __name__ == '__main__':
    unittest.main()
