from copy import deepcopy
from pathlib import Path
from unittest.mock import patch
import sys
import unittest

ROOT = Path(__file__).resolve().parents[2]
sys.path[:0] = [str(ROOT / p) for p in ('aws/ops/staged', 'tests')]
import ops_6230_sec_atom_research_acceptance as op
from sec_atom_test_support import Memory, Tests, model


class Acceptance(unittest.TestCase):
    def test_exact_sources_receipt_and_original_cadence_required(self):
        cfg = {'FunctionName': 'justhodl-sec-8k', 'Runtime': 'python3.12', 'Handler': 'lambda_function.lambda_handler',
               'Timeout': 300, 'MemorySize': 768, 'Architectures': ['x86_64'], 'Role': 'same', 'EphemeralStorage': {'Size': 512}}
        original = {'runtime': cfg, 'schedules': [{'expression': 'rate(30 minutes)'}]}
        before = {'receipt': {'status': 'matched', 'commit': 'exact'}, 'source_files_checked': 3,
                  'function_name': cfg['FunctionName'], 'runtime': cfg['Runtime'], 'handler': cfg['Handler'],
                  'timeout': 300, 'memory_mb': 768, 'architectures': ['x86_64'], 'role': 'same', 'ephemeral_storage_mb': 512,
                  'schedules': deepcopy(original['schedules'])}
        op.check_runtime(before, original, 'exact')
        for key, changed in (('receipt', {'status': 'matched', 'commit': 'other'}), ('source_files_checked', 1),
                             ('schedules', []), ('memory_mb', 1024), ('timeout', 600)):
            with self.subTest(key=key), self.assertRaises(ValueError): op.check_runtime({**before, key: changed}, original, 'exact')

    def test_predecessor_is_pending_and_new_packet_requires_complete_replay(self):
        memory = Memory()
        pending, paths = op.publication(memory, None, '8k')
        self.assertEqual(pending['status'], 'pending_original_schedule_publication')
        self.assertEqual(paths, []); self.assertEqual(memory.writes, [])
        Tests.KIND = '8k'; Tests.setUpClass(); fixture = Tests(); fixture.setUp(); fixture.publish()
        writes = list(fixture.memory.writes)
        result, paths = op.publication(fixture.memory, fixture.native_path, '8k')
        self.assertEqual(result['status'], 'complete_native_atom_originals_replayed')
        self.assertGreater(len(set(paths)), 7)
        self.assertEqual(fixture.memory.writes, writes)
        changed = fixture.packet(); changed['stats']['total_filings'] = 999
        fixture.memory.data[fixture.memory.head] = model.encode(changed)
        with self.assertRaises(ValueError): op.publication(fixture.memory, fixture.native_path, '8k')


if __name__ == '__main__': unittest.main()
