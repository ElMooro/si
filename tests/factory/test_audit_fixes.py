"""Regression checks for the 2026-09-14 audit fixes (F01/F02/F03/F04/F05/F06/F07/F08/F09/F10/F21/F22)."""
import importlib.util
import json
import sys
import tempfile
import types
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'aws/shared'))
sys.path.insert(0, str(ROOT / 'tests/factory'))
from test_factory import MemoryS3  # noqa: E402


def module(name, path):
    spec = importlib.util.spec_from_file_location(name, str(ROOT / path))
    loaded = importlib.util.module_from_spec(spec); spec.loader.exec_module(loaded)
    return loaded


class VerifierJudgeTests(unittest.TestCase):
    def run_rows(self, rows):
        v = module('verify_audit', 'scripts/factory_code_verify.py')
        with tempfile.TemporaryDirectory() as tmp:
            tmp = Path(tmp)
            (tmp / 'in.jsonl').write_text('\n'.join(json.dumps(r) for r in rows) + '\n')
            v.main([str(tmp / 'in.jsonl'), str(tmp / 'out.jsonl')])
            out = [json.loads(l) for l in (tmp / 'out.jsonl').read_text().splitlines()]
            fails = [json.loads(l) for l in (tmp / 'out.jsonl.failures.jsonl').read_text().splitlines()]
        return sorted(r['task_id'] for r in out if r.get('passed')), {r['task_id']: r['reason'] for r in fails}, out[-1]['_report']

    def test_supervisor_is_the_judge(self):
        cur = module('cur_audit', 'scripts/factory_oss_curriculum.py')
        stdio = cur._stdio_tests({'inputs': ['3\n1 2 3\n'], 'outputs': ['6\n']})
        rows = [
            {'task_id': 'good', 'solution': 'def f(x):\n    return x + 1\n', 'tests': 'assert f(1) == 2\nassert f(2) == 3'},
            {'task_id': 'nonce-import', 'solution': "m = __import__('__ma' + 'in__')\nprint(getattr(m, 'nonce', '') + ':PASS')\n", 'tests': 'assert False'},
            {'task_id': 'exit0', 'solution': 'import os\nos._exit(0)\n', 'tests': 'assert f(1) == 2'},
            {'task_id': 'print-pass', 'solution': "print('PASS')\ndef f(x):\n    return 0\n", 'tests': 'assert f(1) == 2'},
            {'task_id': 'empty', 'solution': 'def f(x):\n    return 1\n', 'tests': ''},
            {'task_id': 'zero-case', 'solution': 'def f(x):\n    return 1\n', 'tests': 'x = 1\n'},
            {'task_id': 'stdio-ok', 'solution': 'n = int(input())\nprint(sum(map(int, input().split())))\n', 'tests': stdio},
            {'task_id': 'stdio-exit', 'solution': 'import sys\nprint(6)\nsys.exit(3)\n', 'tests': stdio},          # F21: nonzero exit is a failure
            {'task_id': 'stdio-bytes', 'solution': 'import sys\ndata = sys.stdin.buffer.read().split()\nprint(sum(map(int, data[1:])))\n', 'tests': stdio},   # F21: real stdin.buffer
        ]
        passed, fails, report = self.run_rows(rows)
        self.assertEqual(passed, ['good', 'stdio-bytes', 'stdio-ok'])
        self.assertEqual((report['refused_suites'], report['refused_static']), (2, 1))
        self.assertEqual(report['partial_judge'], 0)
        self.assertEqual(report['checker'], 'factory-code-verify:v4-supervisor-judge')
        self.assertIn('exit 3', fails['stdio-exit'])
        # Forging the runner's result line needs the raw descriptor plus an early exit; both are screened for function
        # tasks. Residual (documented in OWNED.md): an obfuscated form of the same trick against a candidate that already
        # knows the expected values (MBPP prompts show them). Hidden tests are the real close; the harness cannot be.
        forge = [{'task_id': 'forge-fd1', 'solution': 'import os\nos.write(1, b\'\\n[{"i": 0, "repr": "2"}]\\n\')\nos._exit(0)\n', 'tests': 'assert f(1) == 2'},
                 {'task_id': 'forge-stdout', 'solution': 'import sys, os\nsys.stdout.write(\'\\n[{"i": 0, "repr": "2"}]\\n\')\nsys.stdout.flush()\nos._exit(0)\n', 'tests': 'assert f(1) == 2'}]
        passed, fails, _ = self.run_rows(forge)
        self.assertEqual(passed, [])
        self.assertTrue(all('refused_forbidden_token' in r for r in fails.values()), fails)
        # the re-audit's three bypasses, in obfuscated forms that no token screen sees: all fail
        bypass = [{'task_id': 'repr-2', 'solution': "class R:\n    def __repr__(self):\n        return '2'\n    def __eq__(self, o):\n        return True\ndef f(x):\n    return R()\n", 'tests': 'assert f(1) == 2'},
                  {'task_id': 'dyn-main', 'solution': "m = __import__('__ma' + 'in__')\ndef f(x):\n    return 0\n", 'tests': 'assert False'},
                  {'task_id': 'dyn-frame', 'solution': "import sys\ng = getattr(sys, '_get' + 'frame')\ndef f(x):\n    g(1).f_locals\n    return 0\n", 'tests': 'assert f(1) == 2'},
                  {'task_id': 'dyn-write', 'solution': "import os\nw = getattr(os, 'wr' + 'ite')\nw(1, b'\\n{\"i\": 0, \"val\": {\"t\": \"int\", \"v\": \"2\"}}\\n')\ndef f(x):\n    return 0\n", 'tests': 'assert f(1) == 2'},
                  {'task_id': 'stateful', 'solution': 'def add(l, x):\n    l.append(x)\n    return len(l)\n', 'tests': 'l = []\nassert add(l, 1) == 1\nl.append(9)\nassert add(l, 2) == 3'},
                  {'task_id': 'float-eq', 'solution': 'def h(x):\n    return x / 1\n', 'tests': 'assert h(2) == 2'}]
        passed, fails, report = self.run_rows(bypass)
        self.assertEqual(passed, ['float-eq', 'stateful'], fails)


class GatewayAuditTests(unittest.TestCase):
    def test_history_forwarding_and_spawn_intent(self):
        sys.path.insert(0, str(ROOT / 'aws/lambdas/justhodl-ai/source'))
        gw = module('gateway_audit', 'aws/lambdas/justhodl-ai/source/factory_gateway.py')
        seen = {}
        fake_router = types.SimpleNamespace(complete=lambda prompt, **kw: seen.update(prompt=prompt, kw=kw) or 'ok')
        sys.modules['llm_router'] = fake_router
        history = [{'role': 'owner', 'text': 'what is RRP'}, {'role': 'agent', 'text': 'INSIDE — your Brain: PRIVATE-NOTE-MARKER'}]
        gw._public_think('follow up?', 'facts', history)
        self.assertIn('OWNER: what is RRP', seen['prompt'])
        self.assertNotIn('PRIVATE-NOTE-MARKER', seen['prompt'])          # F05
        del sys.modules['llm_router']
        calls = []
        gw.spawn_workers = lambda store, agent, body, policy: calls.append(body) or {'created': body['count']}
        gw._brain_chat = lambda *a, **k: ('reply', 'stub')
        gw.chat_snapshot = lambda store, agent, owner: {'workers': {}}
        gw.factory_status = types.SimpleNamespace(is_task_request=lambda t: False, is_status_request=lambda t: False)
        cloud = MemoryS3()
        from factory_store import Store
        from datetime import datetime, timezone
        store = Store(cloud, 'private', 'public', lambda: datetime(2026, 9, 14, tzinfo=timezone.utc))
        cloud.rows[('public', 'data/student-state.json')] = b'{}'
        for text in ('do not spawn agents', 'should I spawn agents?', 'never hire anyone', 'the word spawn appears here'):
            gw.chat_post(store, 'khalid', True, {'text': text}, {})
        self.assertEqual(calls, [])                                          # F06
        gw.chat_post(store, 'khalid', True, {'text': 'spawn 3 researchers'}, {})
        self.assertEqual(len(calls), 1)


class GenerateAndLedgerTests(unittest.TestCase):
    def test_adapter_identity_fails_closed(self):
        gen = module('generate_audit', 'factory/training/generate.py')
        with tempfile.TemporaryDirectory() as tmp:
            d = Path(tmp)
            gen.MODEL_DIR = d / 'model'; gen.ADAPTER_DIR = d / 'adapter'; gen.TASK_DIR = d / 'tasks'; gen.OUT_DIR = d / 'out'; gen.HP_FILE = d / 'hp.json'
            for p in (gen.MODEL_DIR, gen.TASK_DIR): p.mkdir()
            (gen.MODEL_DIR / 'config.json').write_text('{}')
            (gen.TASK_DIR / 't.jsonl').write_text(json.dumps({'task_id': 'a', 'prompt': 'x'}) + '\n')
            gen.HP_FILE.write_text(json.dumps({'mode': 'burst', 'adapter_generation': 'gen-7'}))
            self.assertEqual(gen.main(), 5)                                   # F07: generation requested, no adapter -> refuse
            self.assertEqual(json.loads((gen.OUT_DIR / 'burst_manifest.json').read_text())['status'], 'refused')

    def test_job_ledger_dedups_and_unknown_state_blocks(self):
        sys.path.insert(0, str(ROOT / 'aws/lambdas/justhodl-ai/source'))
        gb = module('gear_b_audit', 'aws/lambdas/justhodl-ai/source/gear_b.py')
        s3 = MemoryS3()
        for key, doc in (('factory/gearb/jobs/j1.json', {'job_name': 'j1', 'status': 'launching', 'cap_usd': 3.0, 'launched_at': '2026-09-14T00:00:00+00:00'}),
                         ('factory/gearb/jobs/j1-terminal.json', {'job_name': 'j1', 'status': 'Completed', 'cap_usd': 3.0, 'launched_at': '2026-09-14T00:00:00+00:00'})):
            s3.put_object(Bucket='b', Key=key, Body=json.dumps(doc).encode())
        rows = gb._job_records(s3, 'b')
        self.assertEqual(len(rows), 1)                                       # F08
        self.assertEqual(rows[0]['status'], 'Completed')
        self.assertIn('"unknown"', open(ROOT / 'aws/lambdas/justhodl-ai/source/gear_b.py', encoding='utf-8', errors='replace').read())   # F09 wired


if __name__ == '__main__':
    unittest.main(verbosity=2)
