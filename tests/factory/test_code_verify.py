"""The code checker is the coding teacher: it must reject wrong, hanging, broken and escaping candidates
and must not let a candidate touch the verifier's files (2026-09-13 audit: candidates ran as root with the
results directory writable)."""
import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
VERIFY = ROOT / 'scripts/factory_code_verify.py'
GOOD = 'def add(a, b):\n    return a + b\n'
TESTS = 'assert add(1, 2) == 3\nassert add(-1, 1) == 0\n'


def run(rows):
    with tempfile.TemporaryDirectory() as tmp:
        src, out = os.path.join(tmp, 'c.jsonl'), os.path.join(tmp, 'v.jsonl')
        with open(src, 'w') as f:
            for i, (solution, timeout) in enumerate(rows):
                f.write(json.dumps({'task_id': 't%d' % i, 'solution': solution, 'tests': TESTS, 'timeout_s': timeout}) + '\n')
        proc = subprocess.run([sys.executable, str(VERIFY), src, out], capture_output=True, text=True, timeout=120)
        assert proc.returncode == 0, proc.stderr
        report = json.loads(proc.stdout.strip().splitlines()[-1])
        passed = [json.loads(l) for l in open(out) if '"task_id"' in l]
        return report, passed


class CodeVerifyTests(unittest.TestCase):
    def test_only_correct_candidates_pass(self):
        report, passed = run([
            (GOOD, 8),
            ('def add(a, b):\n    return a - b\n', 8),                       # wrong answer
            ('while True:\n    pass\n' + GOOD, 2),                           # hang
            ('def (:\n', 8),                                                 # syntax error
            ('import os\nopen(os.path.join(os.getcwd(), "..", "..", "owned"), "w").write("x")\n' + GOOD, 8),  # escape
            ('open("mine.txt", "w").write("x")\n' + GOOD, 8),                # writing its own scratch is fine
        ])
        self.assertEqual(report['seen'], 6)
        self.assertEqual(report['timeouts'], 1)
        ids = {r['task_id'] for r in passed}
        self.assertIn('t0', ids)
        self.assertIn('t5', ids)
        self.assertNotIn('t1', ids)
        self.assertNotIn('t2', ids)
        self.assertNotIn('t3', ids)
        if os.geteuid() == 0:
            self.assertNotIn('t4', ids)                                      # as nobody the parent dir is read-only
            self.assertEqual({r['verify_isolation'] for r in passed}, {'unprivileged'})
        self.assertTrue(all(r['passed'] is True for r in passed))


if __name__ == '__main__':
    unittest.main(verbosity=2)
