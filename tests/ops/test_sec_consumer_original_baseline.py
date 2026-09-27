from pathlib import Path
import ast
import sys
import unittest

ROOT = Path(__file__).resolve().parents[2]
sys.path[:0] = [str(ROOT / p) for p in ('aws/ops/staged', 'aws/ops', 'aws/ops/checks')]
import ops_6231_sec_consumer_original_baseline as op


class Tests(unittest.TestCase):
    def test_whole_consumer_fixtures_are_pinned_without_execution(self):
        for fn, digest in op.PINS.items():
            raw = (ROOT / 'tests/fixtures' / ('pre-sec-consumer-' + fn.removeprefix('justhodl-') + '.py.txt')).read_bytes()
            self.assertEqual(op.source_check(fn, raw)['sha256'], digest)
            self.assertFalse(op.source_check(fn, raw)['native_imported_or_executed'])
            with self.assertRaises(ValueError): op.source_check(fn, raw + b'\n')
        with self.assertRaises(ValueError): op.source_check('unreviewed', b'')
        self.assertNotIn('lambda_function', sys.modules)

    def test_operation_has_no_native_invocation_or_output_capture_path(self):
        tree = ast.parse(Path(op.__file__).read_bytes())
        calls = {n.func.attr for n in ast.walk(tree) if isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute)}
        for name in ('invoke', 'publish', 'publish_many', 'get_secret_value', 'get_parameter', 'update_function_configuration'):
            self.assertNotIn(name, calls)
        self.assertNotIn('capture', {n.func.id for n in ast.walk(tree) if isinstance(n, ast.Call) and isinstance(n.func, ast.Name)})
        self.assertIn('sys.exit(1)', Path(op.__file__).read_text(encoding='utf-8'))


if __name__ == '__main__': unittest.main()
