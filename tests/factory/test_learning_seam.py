"""Cross-component acceptance (re-audit minimum checks 4/5/6): verifier -> trace writer -> Gear B curator on one mixed batch.
A valid pass reaches the curator with intact checker/judge/cases; a failure becomes an attempt record; a non-trainable owner
task yields a result but no training row; replaying the batch is idempotent; a conflicting owner result raises."""
import importlib.util
import json
import sys
import tempfile
import types
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'aws/shared'))
sys.path.insert(0, str(ROOT / 'aws/lambdas/justhodl-ai/source'))
sys.path.insert(0, str(ROOT / 'tests/factory'))
from test_factory import MemoryS3  # noqa: E402


def module(name, path):
    spec = importlib.util.spec_from_file_location(name, str(ROOT / path))
    loaded = importlib.util.module_from_spec(spec); spec.loader.exec_module(loaded)
    return loaded


class LearningSeamTests(unittest.TestCase):
    def test_mixed_batch_end_to_end(self):
        verify = module('seam_verify', 'scripts/factory_code_verify.py')
        tv = module('seam_trace_verify', 'scripts/factory_trace_verify.py')
        gb = module('seam_gear_b', 'aws/lambdas/justhodl-ai/source/gear_b.py')
        cloud = MemoryS3()
        sys.modules['boto3'] = types.SimpleNamespace(client=lambda *a, **k: cloud)   # cmd_write imports boto3 lazily
        tv.PRIVATE = 'b'
        with tempfile.TemporaryDirectory() as tmp:
            tmp = Path(tmp)
            traces = [{'task_id': 'mbpp-1', 'sample': 0, 'completion': '```python\ndef add(a, b):\n    return a + b\n```', 'completion_sha256': 'a' * 64, 'mode': 'burst'},
                      {'task_id': 'mbpp-1', 'sample': 1, 'completion': 'def add(a, b):\n    return a - b\n', 'completion_sha256': 'b' * 64, 'mode': 'burst'},
                      {'task_id': 'task-owner1', 'sample': 0, 'completion': 'def parse_week(s):\n    y, w = s.split("-W")\n    return (int(y), int(w))\n', 'completion_sha256': 'c' * 64, 'mode': 'burst'}]
            cands = [{'task_id': 'mbpp-1', 'prompt': 'add two numbers', 'tests': 'assert add(1, 2) == 3\nassert add(-1, 1) == 0', 'timeout_s': 8, 'family': 'mbpp', 'source_sha': 's1', 'source_url': 'https://x'}]
            cards = [{'id': 'task-owner1', 'text': 'parse ISO week strings', 'tests': 'assert parse_week("2026-W38") == (2026, 38)', 'trainable': False}]
            (tmp / 'traces.jsonl').write_text('\n'.join(json.dumps(r) for r in traces) + '\n')
            (tmp / 'cands.jsonl').write_text('\n'.join(json.dumps(r) for r in cands) + '\n')
            (tmp / 'cards.jsonl').write_text('\n'.join(json.dumps(r) for r in cards) + '\n')
            (tmp / 'holdout.json').write_text(json.dumps({'ids': ['HumanEval/0']}))
            self.assertEqual(tv.main(['join', '--traces', str(tmp / 'traces.jsonl'), '--candidates', str(tmp / 'cands.jsonl'), '--out', str(tmp / 'joined.jsonl'),
                                      '--holdout-ids', str(tmp / 'holdout.json'), '--task-cards', str(tmp / 'cards.jsonl')]), 0)
            self.assertEqual(verify.main([str(tmp / 'joined.jsonl'), str(tmp / 'verified.jsonl')]), 0)
            for _ in range(2):   # first run, then an identical replay
                self.assertEqual(tv.main(['write', '--in', str(tmp / 'verified.jsonl'), '--burst', 'jh-burst-test', '--run-id', 'r1']), 0)
            keys = sorted(k for b, k in cloud.rows)
            rows = [json.loads(cloud.rows[('b', k)]) for k in keys if k.startswith('factory/curriculum/code/verified/')]
            attempts = [json.loads(cloud.rows[('b', k)]) for k in keys if '/attempts/' in k]
            results = [k for k in keys if '-result-' in k]
            verdicts = [k for k in keys if '/verdicts/' in k]
            self.assertEqual(len(rows), 1, keys)                                # exactly one eligible training row (the mbpp pass)
            self.assertEqual(len(attempts), 1)                                  # the failing sample is an inspectable attempt
            self.assertEqual(attempts[0]['reason'][:6], 'case 0')
            self.assertEqual(len(results), 1)                                   # the owner task got its verified result
            self.assertEqual(len(verdicts), 2)                                  # two passes -> two verdict receipts
            row = rows[0]
            self.assertEqual((row['checker'], row['judge'], row['cases'], row['kind']), ('factory-code-verify:v3-supervisor-judge', 'supervisor', 2, 'self_trace'))
            self.assertEqual(row['solution_sha256'], json.loads(cloud.rows[('b', row['receipt'])])['solution_sha256'])
            summary = json.loads(cloud.rows[('b', 'factory/bursts/jh-burst-test/summary-r1.json')])
            self.assertEqual((summary['rows_written'], summary['rows_existing'], summary['owner_results'], summary['owner_replays'], summary['skipped_not_trainable']), (0, 1, 0, 1, 1))
            # curator accepts the row with its metadata intact
            accepted = gb._row_from_verified('factory/curriculum/code/verified/x.json', row)
            self.assertIsNotNone(accepted)
            self.assertEqual(accepted.get('kind'), 'self_trace')
            legacy = dict(row, checker='factory-trace-verify:network-less-container')
            self.assertIsNone(gb._row_from_verified('k', legacy))
            # a conflicting owner result for the same task/sample raises loudly
            conflict_key = [k for k in results][0]
            cloud.rows[('b', conflict_key)] = json.dumps({**json.loads(cloud.rows[('b', conflict_key)]), 'solution_sha256': 'f' * 64}).encode()
            with self.assertRaises(RuntimeError):
                tv.main(['write', '--in', str(tmp / 'verified.jsonl'), '--burst', 'jh-burst-test', '--run-id', 'r2'])
        sys.modules.pop('boto3', None)


if __name__ == '__main__':
    unittest.main(verbosity=2)
