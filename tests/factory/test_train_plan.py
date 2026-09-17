"""The owned trainer plans its steps from the data and stops on a time budget (2026-09-16).

Four Gear B jobs (jh-gearb-gen4..gen7) ran a fixed 400 steps over 570 rows (~70 epochs, ~5 h) inside a 3 h cap and were
killed with nothing saved. These tests pin the arithmetic that prevents it: steps = packed sequences x epochs / batch,
`max_steps` is a ceiling only, and the budget rule stops before the next step would cross it. Pure python -- no torch.
"""
import importlib.util
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location("train_qlora", str(ROOT / "factory" / "training" / "train_qlora.py"))
tq = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(tq)


class StepPlanTests(unittest.TestCase):
    def test_570_rows_three_epochs_is_minutes_not_hours(self):
        plan = tq.step_plan(170000, 2048, 3, 2, 8, 400)
        self.assertEqual((plan["packed_sequences"], plan["steps_per_epoch"], plan["planned_steps"], plan["max_steps"]), (84, 6, 18, 18))

    def test_cap_is_a_ceiling_not_a_plan(self):
        plan = tq.step_plan(20_000_000, 2048, 3, 2, 8, 400)
        self.assertEqual(plan["max_steps"], 400); self.assertGreater(plan["planned_steps"], 400)
        self.assertEqual(tq.step_plan(20_000_000, 2048, 3, 2, 8, 0)["max_steps"], tq.step_plan(20_000_000, 2048, 3, 2, 8, 0)["planned_steps"])

    def test_tiny_dataset_still_takes_one_step(self):
        self.assertEqual(tq.step_plan(10, 2048, 1, 2, 8, 400)["max_steps"], 1)

    def test_budget_rule_stops_before_crossing_and_never_without_a_budget(self):
        # 47 s/step, 9600 s budget: step 204 ends at 9588 s, the next would end at 9635 -> stop; step 203 (9541 + 47 = 9588) fits
        self.assertTrue(tq.budget_exhausted(0.0, 204 * 47.0, 204, 9600))
        self.assertFalse(tq.budget_exhausted(0.0, 203 * 47.0, 203, 9600))
        self.assertFalse(tq.budget_exhausted(0.0, 100 * 47.0, 100, 9600))
        self.assertFalse(tq.budget_exhausted(0.0, 99999.0, 5, 0))
        self.assertFalse(tq.budget_exhausted(0.0, 0.0, 0, 9600))

    def test_pin_and_launcher_agree_on_the_contract(self):
        pin_src = (ROOT / "scripts" / "factory_training_pin.py").read_text()
        self.assertIn('"epochs": 3', pin_src)
        launcher = (ROOT / "aws" / "lambdas" / "justhodl-ai" / "source" / "gear_b.py").read_text()
        self.assertIn('hp.setdefault("time_budget_s"', launcher)
        self.assertIn('as_int(hp.get("time_budget_s"), 9600)', (ROOT / "factory" / "training" / "train_qlora.py").read_text())


if __name__ == "__main__":
    unittest.main()
