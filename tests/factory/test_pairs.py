"""Preference pairs from the owned model's bursts (scripts/factory_pairs.py) and their attachment in Gear B (attach_pairs)."""
import runpy, sys, unittest
from pathlib import Path
ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'aws/shared')); sys.path.insert(0, str(ROOT / 'aws/lambdas/justhodl-ai/source')); sys.path.insert(0, str(ROOT / 'scripts'))
P = runpy.run_path(str(ROOT / 'scripts/factory_pairs.py'))


class PairTests(unittest.TestCase):
    def test_a_task_with_a_pass_and_a_fail_becomes_one_pair_from_the_burst_text(self):
        traces = {("t1", "0"): "```python\ndef f(a, b):\n    return a + b\n```", ("t1", "1"): "```python\ndef f(a, b):\n    return a - b\n```",
                  ("t2", "0"): "```python\ndef g():\n    return 1\n```"}
        passed = [{"task_id": "t1", "sample": 0, "solution_sha256": P["sha"](P["extract_code"](traces[("t1", "0")]).encode())},
                  {"task_id": "t2", "sample": 0, "solution_sha256": "x"}]
        failed = [{"task_id": "t1", "sample": 1, "reason": "assert failed", "cases": 3}, {"task_id": "t3", "sample": 0}]
        pairs = P["build_pairs"](traces, failed, passed)
        self.assertEqual(len(pairs), 1); self.assertEqual(pairs[0]["task_id"], "t1")
        self.assertIn("return a - b", pairs[0]["rejected"]); self.assertEqual(pairs[0]["rejected_reason"], "assert failed")

    def test_identical_failing_text_and_missing_trace_never_pair(self):
        code = "```python\ndef f():\n    return 2\n```"
        sol_sha = P["sha"](P["extract_code"](code).encode())
        self.assertEqual(P["build_pairs"]({("t", "1"): code}, [{"task_id": "t", "sample": 1}], [{"task_id": "t", "sample": 0, "solution_sha256": sol_sha}]), [])
        self.assertEqual(P["build_pairs"]({}, [{"task_id": "t", "sample": 9}], [{"task_id": "t", "sample": 0}]), [])
        stored = P["build_pairs"]({}, [{"task_id": "t", "sample": 9, "solution": "def f():\n    return 3\n"}], [{"task_id": "t", "sample": 0}])
        self.assertEqual(len(stored), 1)                                   # a stored failing solution needs no trace

    def test_gear_b_attaches_the_failing_attempt_to_the_same_task_only(self):
        import gear_b as gb
        rows = [{"task_id": "t1", "prompt": "p", "solution": "def f(a,b): return a+b"}, {"task_id": "t2", "prompt": "q", "solution": "x"}]
        out = gb.attach_pairs(rows, {"t1": [{"rejected": "def f(a,b): return a-b", "rejected_reason": "assert"}]})
        self.assertEqual(out[0]["rejected"].strip(), "def f(a,b): return a-b"); self.assertNotIn("rejected", out[1])   # normalised like the chosen side (_lf)
        same = gb.attach_pairs([{"task_id": "t1", "prompt": "p", "solution": "S"}], {"t1": [{"rejected": "S"}]})
        self.assertNotIn("rejected", same[0])                               # never pair a solution with itself


if __name__ == '__main__':
    unittest.main()
