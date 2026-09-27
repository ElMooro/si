from pathlib import Path
from unittest.mock import patch
import importlib.util
import json
import sys
import unittest

ROOT = Path(__file__).resolve().parents[2]
sys.path[:0] = [str(ROOT / p) for p in ('aws/ops/staged',)]
import ops_6234_capex_cohort_acceptance as op
spec = importlib.util.spec_from_file_location('isolated_capex_fixture', ROOT / 'aws/lambdas/justhodl-capex-pulse/tests/run_tests.py')
fixture = importlib.util.module_from_spec(spec); spec.loader.exec_module(fixture)


class Tests(unittest.TestCase):
    def test_predecessor_remains_pending(self):
        self.assertEqual(op.publication(b'{"version":"1.1.0","rows":[]}')['status'], 'pending_original_schedule_publication')

    def test_whole_cohort_recomputation_and_tamper_rejection(self):
        case = fixture.Tests(); writes, _ = case.native(fixture.SOURCE.read_bytes())
        packet = writes[op.KEY]; aggregate, _ = op.compiler()
        with patch.object(op, 'compiler', return_value=(aggregate, ['PAIRED', 'CURRENT'])):
            result = op.publication(json.dumps(packet).encode())
            self.assertEqual(result['status'], 'published_cohort_arithmetic_reproduced')
            self.assertFalse(result['provider_originals_replayed'])
            packet['market']['yoy_pct'] = 100
            with self.assertRaises(ValueError): op.publication(json.dumps(packet).encode())


if __name__ == '__main__': unittest.main()
