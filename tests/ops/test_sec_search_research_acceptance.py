from copy import deepcopy
from pathlib import Path
import sys
import unittest

ROOT = Path(__file__).resolve().parents[2]
sys.path[:0] = [str(ROOT / p) for p in ('aws/ops/staged', 'tests')]
import ops_6232_sec_search_research_acceptance as op
import sec_search_test_support as fixtures


class Acceptance(unittest.TestCase):
    def test_exact_packages_and_original_schedules_required(self):
        cfg = {'FunctionName': op.PRODUCER, 'Runtime': 'python3.12', 'Handler': 'lambda_function.lambda_handler',
               'Timeout': 600, 'MemorySize': 256, 'Architectures': ['x86_64'], 'Role': 'same', 'EphemeralStorage': {'Size': 512}}
        original = {'runtime': cfg, 'schedules': [{'expression': 'cron(7 21 * * ? *)'}]}
        before = {'receipt': {'status': 'matched', 'commit': 'exact'}, 'source_files_checked': 6,
                  'function_name': cfg['FunctionName'], 'runtime': cfg['Runtime'], 'handler': cfg['Handler'],
                  'timeout': 600, 'memory_mb': 256, 'architectures': ['x86_64'], 'role': 'same', 'ephemeral_storage_mb': 512,
                  'schedules': deepcopy(original['schedules'])}
        op.check_runtime(before, original, 'exact', 6)
        for key, changed in (('receipt', {'status': 'matched', 'commit': 'other'}), ('source_files_checked', 3),
                             ('schedules', []), ('timeout', 900), ('memory_mb', 512)):
            with self.subTest(key=key), self.assertRaises(ValueError): op.check_runtime({**before, key: changed}, original, 'exact', 6)

    def test_previous_head_pending_full_new_head_replays_without_writes(self):
        pending, paths = op.publication(fixtures.Memory(), None)
        self.assertEqual(pending['status'], 'pending_original_schedule_publication'); self.assertEqual(paths, [])
        fixture = fixtures.Tests(); fixture.setUp(); fixture.publish()
        before = list(fixture.memory.writes)
        result, paths = op.publication(fixture.memory, fixtures.NATIVE)
        self.assertEqual(result['status'], 'complete_native_search_originals_replayed')
        self.assertGreater(len(set(paths)), 20)
        self.assertEqual(fixture.memory.writes, before)
        packet = fixture.packet(); packet['n_events_total'] = 999
        fixture.memory.data[fixtures.model.HEAD] = fixtures.model.encode(packet)
        with self.assertRaises(ValueError): op.publication(fixture.memory, fixtures.NATIVE)


if __name__ == '__main__': unittest.main()
