"""Market exam on anonymized drills -- graded like the wall, against naive baselines (scripts/factory_market_exam.py)."""
import json
import runpy
import sys
import types
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'aws/shared'))
M = runpy.run_path(str(ROOT / 'scripts/factory_market_exam.py'))
SEASON = {"weights": {"direction": 0.5, "regime": 0.3, "crisis": 0.2}, "flat_thresholds": {"SPY": 0.003, "TLT": 0.003, "GLD": 0.003},
          "crisis_drawdown_thresholds": {"SPY": 0.05, "TLT": 0.05, "GLD": 0.05}}


def drill(direction="UP", regime="TREND", crisis=False, slope=0.4):
    bars = [{"o": 100 + i * slope, "h": 100.5 + i * slope, "l": 99.5 + i * slope, "c": 100.2 + i * slope, "v_rel": 1.0} for i in range(20)]
    return {"schema_version": "factory-drill.v1", "asset_class": "equity_index", "bars": bars, "label_horizon_sessions": 5,
            "labels": {"direction": direction, "regime": regime, "crisis": crisis}, "flat_threshold": 0.003, "crisis_drawdown_threshold": 0.05}


class FakeS3:
    def __init__(self):
        self.rows = {}
        self.exceptions = types.SimpleNamespace(NoSuchKey=KeyError)

    def put_object(self, Bucket, Key, Body, **kw):
        self.rows[Key] = Body

    def get_object(self, Bucket, Key):
        if Key not in self.rows:
            raise KeyError("NoSuchKey " + Key)
        return {"Body": types.SimpleNamespace(read=lambda: self.rows[Key])}


class FakeEndpoint:
    """Answers each request the moment it is polled, from a function of the drill it was asked about."""
    def __init__(self, s3, answer_fn):
        self.s3, self.answer_fn, self.calls = s3, answer_fn, []

    def invoke_endpoint_async(self, **kw):
        self.calls.append(kw)
        key = kw["InputLocation"].split("/", 3)[-1]
        payload = json.loads(self.s3.rows[key])
        out = "factory/inference/outputs/%s.out" % kw["InferenceId"]
        text = self.answer_fn(payload["inputs"])
        self.s3.rows[out + ".json"] = json.dumps({"generated_text": text}).encode()
        return {"OutputLocation": "s3://%s/%s.json" % (M["PRIVATE"], out), "FailureLocation": "s3://%s/%s.fail.json" % (M["PRIVATE"], out)}


class MarketExamTests(unittest.TestCase):
    def test_drill_prefix_matches_the_freezer(self):
        H = runpy.run_path(str(ROOT / 'scripts/factory_holdout.py'))
        self.assertEqual(M["DRILL_PREFIX"], H["DRILL_PREFIX"])          # the exam lists where the freezer writes (2026-09-17: they differed)

    def test_prompt_is_anonymous_and_carries_the_thresholds(self):
        p = M["drill_prompt"](drill())
        self.assertIn("bar,open,high,low,close,vol_rel", p); self.assertIn("Flat threshold: 0.003", p); self.assertIn("Crisis drawdown threshold: 0.05", p)
        self.assertNotIn("SPY", p); self.assertNotIn("2020", p)
        self.assertEqual(p.count("\n1,"), 1); self.assertIn("\n20,", p)

    def test_answers_are_parsed_and_coerced_never_invented(self):
        good = M["parse_answer"]('Sure:\n```json\n{"direction": "up", "regime": "trending", "crisis_probability": 0.2, "why": "efficient rise"}\n```')
        self.assertEqual(good, {"direction": "UP", "regime": "TREND", "crisis_probability": 0.2, "why": "efficient rise"})
        pct = M["parse_answer"]('{"direction": "DOWN", "regime": "RANGE", "crisis_probability": 35}')
        self.assertEqual(pct["crisis_probability"], 0.35)
        boolean = M["parse_answer"]('{"direction": "FLAT", "regime": "RANGE", "crisis": true}')
        self.assertEqual(boolean["crisis_probability"], 0.8)
        self.assertIsNone(M["parse_answer"]('{"direction": "SIDEWAYS-ISH", "regime": "RANGE"}'))
        self.assertIsNone(M["parse_answer"]("I think it goes up."))

    def test_grading_matches_the_wall_and_baselines_are_scored_on_the_same_drills(self):
        s3 = FakeS3()
        drills = [("d1", drill("UP", "TREND", False)), ("d2", drill("DOWN", "RANGE", True, slope=-0.4)), ("d3", drill("FLAT", "RANGE", False, slope=0.0))]
        oracle = {"d1": '{"direction":"UP","regime":"TREND","crisis_probability":0.1}', "d2": '{"direction":"DOWN","regime":"TREND","crisis_probability":0.7}', "d3": "no json here"}
        def answer(prompt):
            for did, d in drills:
                if M["drill_prompt"](d) in prompt:
                    return oracle[did]
            return "?"
        cloud = M["Cloud"](s3, FakeEndpoint(s3, answer))
        res = M["run_exam"](cloud, drills, SEASON, {"endpoint_name": "ep", "model_id": "qwen", "revision": "r"}, wait_s=5, sleep_s=0, clock=lambda: 0.0, sleep=lambda s: None, run_id="holdout-test")
        self.assertEqual(res["n_drills"], 3); self.assertEqual(res["n_answered"], 2); self.assertEqual(res["n_unanswered"], 1)
        r1 = next(r for r in res["rows"] if r["drill"] == "d1"); r2 = next(r for r in res["rows"] if r["drill"] == "d2"); r3 = next(r for r in res["rows"] if r["drill"] == "d3")
        self.assertEqual(r1["score"], 1.0); self.assertEqual(r1["components"], {"direction": 1, "regime": 1, "crisis": 1})
        self.assertAlmostEqual(r2["score"], 0.5 + 0.0 + 0.2); self.assertFalse(r2["missed_crisis"])
        self.assertIsNone(r3["score"]); self.assertIn("no json", r3["raw_head"])
        self.assertAlmostEqual(res["model_scores"]["score"], (1.0 + 0.7) / 2)
        self.assertEqual(res["model_scores"]["n"], 2); self.assertEqual(res["baselines"]["momentum"]["n"], 3); self.assertEqual(res["baselines"]["prior"]["n"], 3)
        self.assertEqual(r1["baselines"]["momentum"]["components"]["direction"], 1)     # a rising window -> momentum says UP
        self.assertEqual(r2["baselines"]["momentum"]["components"]["direction"], 1)
        self.assertIn(res["crisis_base_rate"], (round(1 / 3, 4),))
        self.assertEqual(res["baselines"]["prior"]["missed_crises"], 1)            # a base-rate crisis probability under 0.5 misses every crisis too
        self.assertEqual(len(cloud.rt.calls), 3); self.assertEqual(cloud.rt.calls[0]["InvocationTimeoutSeconds"], 900)
        self.assertIn("holdout-test/requests/d1.json", cloud.rt.calls[0]["InputLocation"])

    def test_prediction_contract_carries_probability_vectors_that_sum_to_one(self):
        p = M["as_prediction"]({"direction": "UP", "regime": "RANGE", "crisis_probability": 0.3}, 0.7)
        self.assertAlmostEqual(sum(p["direction_probabilities"].values()), 1.0); self.assertAlmostEqual(sum(p["regime_probabilities"].values()), 1.0)
        self.assertEqual(p["direction_probabilities"]["UP"], 0.7)


if __name__ == "__main__":
    unittest.main()
