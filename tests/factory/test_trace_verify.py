"""Keep-only-passes join/write for owned bursts: tests never travel with the traces; holdout ids are refused."""
import importlib.util
import json
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def module(name, path):
    spec = importlib.util.spec_from_file_location(name, str(ROOT / path))
    loaded = importlib.util.module_from_spec(spec); spec.loader.exec_module(loaded)
    return loaded


tv = module('trace_verify_under_test', 'scripts/factory_trace_verify.py')


class TraceVerifyTests(unittest.TestCase):
    def test_join_extracts_code_refuses_holdout_and_drops_unknown_tasks(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp = Path(tmp)
            (tmp / 'traces.jsonl').write_text('\n'.join(json.dumps(r) for r in [
                {'task_id': 'mbpp-1', 'sample': 0, 'completion': 'Here is code:\n```python\ndef f(x):\n    return x + 1\n```\nDone.', 'completion_sha256': 'a' * 64, 'mode': 'burst'},
                {'task_id': 'mbpp-1', 'sample': 1, 'completion': 'def f(x):\r\n    return x + 2\r\n', 'mode': 'burst'},
                {'task_id': 'HumanEval/7', 'sample': 0, 'completion': 'def h(): pass', 'mode': 'burst'},
                {'task_id': 'apps-99', 'sample': 0, 'completion': 'def g(): pass', 'mode': 'burst'},
                {'task_id': 'mbpp-2', 'sample': 0, 'completion': '', 'mode': 'burst'}]) + '\n')
            (tmp / 'cands.jsonl').write_text('\n'.join(json.dumps(r) for r in [
                {'task_id': 'mbpp-1', 'prompt': 'add one', 'tests': 'assert f(1) == 2', 'timeout_s': 8, 'family': 'mbpp', 'source_sha': 's1', 'source_url': 'https://x', 'citation': 'c'},
                {'task_id': 'mbpp-2', 'prompt': 'x', 'tests': 'assert True', 'family': 'mbpp', 'source_sha': 's2', 'source_url': 'https://x'}]) + '\n')
            (tmp / 'holdout.json').write_text(json.dumps({'ids': ['HumanEval/7']}))
            rc = tv.main(['join', '--traces', str(tmp / 'traces.jsonl'), '--candidates', str(tmp / 'cands.jsonl'), '--out', str(tmp / 'joined.jsonl'), '--holdout-ids', str(tmp / 'holdout.json')])
            self.assertEqual(rc, 0)
            rows = [json.loads(l) for l in (tmp / 'joined.jsonl').read_text().splitlines()]
            self.assertEqual([(r['task_id'], r['sample']) for r in rows], [('mbpp-1', 0), ('mbpp-1', 1)])
            self.assertEqual(rows[0]['solution'], 'def f(x):\n    return x + 1\n')      # fenced block extracted
            self.assertEqual(rows[1]['solution'], 'def f(x):\n    return x + 2\n')      # CRLF normalised
            self.assertEqual(rows[0]['tests'], 'assert f(1) == 2')                       # tests joined here, never in the trace
            self.assertEqual(rows[0]['license'], 'own')

    def test_verifier_grades_the_joined_rows(self):
        verify = module('code_verify_under_test', 'scripts/factory_code_verify.py')
        with tempfile.TemporaryDirectory() as tmp:
            tmp = Path(tmp)
            (tmp / 'joined.jsonl').write_text('\n'.join(json.dumps(r) for r in [
                {'task_id': 'mbpp-1', 'sample': 0, 'solution': 'def f(x):\n    return x + 1\n', 'tests': 'assert f(1) == 2', 'timeout_s': 8},
                {'task_id': 'mbpp-1', 'sample': 1, 'solution': 'def f(x):\n    return x + 2\n', 'tests': 'assert f(1) == 2', 'timeout_s': 8}]) + '\n')
            rc = verify.main([str(tmp / 'joined.jsonl'), str(tmp / 'verified.jsonl')])
            self.assertEqual(rc, 0)
            out = [json.loads(l) for l in (tmp / 'verified.jsonl').read_text().splitlines()]
            passed = [r for r in out if r.get('passed')]
            self.assertEqual([(r['task_id'], r['sample']) for r in passed], [('mbpp-1', 0)])
            self.assertEqual(out[-1]['_report']['failed'], 1)


if __name__ == '__main__':
    unittest.main(verbosity=2)
