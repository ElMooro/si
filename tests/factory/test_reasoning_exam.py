"""How smart is it -- the reasoning exam (scripts/factory_reasoning_exam.py): fixed held-out samples, exact grading, executed truth."""
import json, runpy, sys, types, unittest
from pathlib import Path
ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'aws/shared'))
M = runpy.run_path(str(ROOT / 'scripts/factory_reasoning_exam.py'))
GSM = "\n".join(json.dumps({"question": q, "answer": "work #### %s" % a}) for q, a in
                [("Tom has 3 apples and buys 4 more. How many?", "7"), ("A car goes 60 miles per hour for 2.5 hours. Distance?", "150"),
                 ("Anna earns $1,200 and spends 25%%. Left?", "900"), ("A dozen eggs cost 12 dollars. One egg?", "1"), ("Three friends split 9 pies. Each?", "3")])


class FakeS3:
    def __init__(self): self.rows = {}
    def put_object(self, Bucket, Key, Body, **kw): self.rows[Key] = Body
    def get_object(self, Bucket, Key):
        if Key not in self.rows: raise KeyError("NoSuchKey " + Key)
        return {"Body": types.SimpleNamespace(read=lambda: self.rows[Key])}


class FakeEndpoint:
    def __init__(self, s3, answer_fn): self.s3, self.answer_fn = s3, answer_fn
    def invoke_endpoint_async(self, **kw):
        key = kw["InputLocation"].split("/", 3)[-1]; payload = json.loads(self.s3.rows[key])
        out = "factory/inference/out/%s.json" % kw["InferenceId"]
        self.s3.rows[out] = json.dumps({"generated_text": self.answer_fn(payload["inputs"])}).encode()
        return {"OutputLocation": "s3://%s/%s" % (M["PRIVATE"], out), "FailureLocation": "s3://%s/fail/%s" % (M["PRIVATE"], kw["InferenceId"])}


class ReasoningExamTests(unittest.TestCase):
    def test_gsm8k_sample_is_fixed_and_gold_is_the_number_after_the_marker(self):
        a = M["load_gsm8k"](3, GSM); b = M["load_gsm8k"](3, GSM)
        self.assertEqual([x["id"] for x in a], [x["id"] for x in b]); self.assertEqual(len(a), 3)
        self.assertTrue(all(x["gold"].replace(".", "").isdigit() for x in a))

    def test_number_grading_reads_the_final_answer_line_first(self):
        self.assertTrue(M["grade_number"]("3 + 4 = 7 apples.\nFinal answer: 7", "7"))
        self.assertTrue(M["grade_number"]("Total is $1,200 - $300 = $900\nFinal answer: $900", "900"))
        self.assertTrue(M["grade_number"]("So 150.0 miles. Final answer: 150.0", "150"))
        self.assertFalse(M["grade_number"]("Final answer: 8 (but 7 apples earlier)", "7"))
        self.assertFalse(M["grade_number"]("I cannot solve this.", "7"))

    def test_code_items_carry_executed_truth_and_output_grading_normalises_only_whitespace_and_fences(self):
        items = M["code_items"](8)
        self.assertEqual(len(items), 8); self.assertTrue(all(it["gold"] != "" for it in items))
        again = M["code_items"](8); self.assertEqual([i["gold"] for i in items], [i["gold"] for i in again])     # seeded: the same exam every time
        self.assertTrue(M["grade_output"]("```\n42\n```", "42")); self.assertTrue(M["grade_output"]("  [1, 2]  \n", "[1, 2]"))
        self.assertFalse(M["grade_output"]("[1,2]", "[1, 2]")); self.assertFalse(M["grade_output"]("42\n43", "42"))

    def test_exam_round_trip_grades_each_family_exactly(self):
        s3 = FakeS3()
        items = M["load_gsm8k"](3, GSM) + M["code_items"](4)
        gold = {it["prompt"]: it["gold"] for it in items}
        def oracle(prompt):
            for p, g in sorted(gold.items(), key=lambda kv: -len(kv[0])):        # longest prompt first: a short question must not shadow a program
                if p in prompt:
                    if p.startswith("Tom"): return "Working: 3 + 4 = 7\nFinal answer: 7"          # right
                    if p.startswith("A car"): return "60 * 2.5 = 15\nFinal answer: 15"           # wrong
                    if "print" in p: return g if "acc" in p else "nope"                          # code: right only for the first template
                    return "Final answer: %s" % g
            return "?"
        cloud = M["Cloud"](s3, FakeEndpoint(s3, oracle))
        res = M["run_exam"](cloud, items, {"endpoint_name": "ep", "model_id": "qwen"}, wait_s=5, sleep_s=0, clock=lambda: 0.0, sleep=lambda s: None, run_id="reasoning-test")
        fam = res["families"]
        self.assertEqual(fam["gsm8k"]["n"], 3); self.assertEqual(fam["gsm8k"]["answered"], 3)
        self.assertEqual(fam["code_reading"]["n"], 4); self.assertEqual(fam["code_reading"]["answered"], 4)
        self.assertEqual(fam["code_reading"]["correct"], len([i for i in items if i["family"] == "code_reading" and "acc" in i["prompt"]]))
        self.assertTrue(all(r["correct"] is not None for r in res["rows"]))


if __name__ == "__main__":
    unittest.main()
