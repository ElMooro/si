import unittest
"""Prove the doctrine: next answer can be shown less wrong than the last."""
from __future__ import annotations

import self_improve as s


def test_tie_does_not_promote():
    out = s.tighten_promotion(
        {"eligible": True},
        {"score": 0.8232, "passed": 135, "n": 164},
        {"score": 0.8232, "passed": 135, "n": 164},
    )
    assert out["eligible"] is False
    assert "coding_tie" in out["reason"]


def test_real_lift_promotes_without_market_block():
    out = s.tighten_promotion(
        {"eligible": True},
        {"score": 0.85, "passed": 140, "n": 164},
        {"score": 0.8232, "passed": 135, "n": 164},
    )
    assert out["eligible"] is True


def test_refuse_old_family():
    why = s.refuse_repeat_sft(
        [],
        {"eligibility_digest": "abc", "kept": 570, "families": 2, "kinds": {"public_benchmark_train": 570}},
        {"require_new_family_after_flat_gens": 3, "min_dpo_pairs": 64},
    )
    assert why and "curiosity refuse-SFT" in why


def test_lesson_from_wrong_avoid():
    lesson = s.lesson_from_grade({"stance": "AVOID", "asset": "crypto", "call_id": "c1"}, 5, {"net_return": 0.08})
    assert lesson["hit"] is False
    assert "wrong" in lesson["lesson"]


def test_enforce_repeats_lesson():
    parsed = {"crypto": {"stance": "AVOID", "read": "still out"}}
    lessons = [{"weight": 1.0, "hit": False, "asset": "crypto", "stance": "AVOID"}]
    out = s.enforce_lessons(parsed, lessons)
    assert out.get("lesson_violation") is True


def test_process_score():
    parsed = {
        "overall": "x" * 40,
        "macro": "rates and dollar",
        "stocks": {"stance": "SELECTIVE", "read": "a"},
        "bonds": {"stance": "NEUTRAL", "read": "b"},
        "metals": {"stance": "HOLD", "read": "c"},
        "crypto": {"stance": "AVOID", "read": "d"},
        "what_would_change_my_mind": ["DXY drop"],
    }
    out = s.process_score_read(parsed, {"sources": {"fusion": {"status": "FRESH"}}})
    assert out["process_score"] == 1.0


def test_think_rank():
    out = s.think_rank(["bad", "good", "ok"], lambda c: {"bad": 0.1, "good": 0.9, "ok": 0.5}[c])
    assert out["best"] == "good"
    assert out["n"] == 3


if __name__ == "__main__":
    for name, fn in list(globals().items()):
        if name.startswith("test_") and callable(fn):
            fn()
            print("ok", name)
    print("DOCTRINE_PROOF_OK")

class PairCarryingDatasetTests(unittest.TestCase):
    def test_pair_carrying_dataset_is_not_repeat_sft(self):
        import self_improve as si
        m = {"eligibility_digest": "d", "kinds": {"public_benchmark_train": 2010}, "kept": 2010, "families": 3, "new_task_fraction": 0.0, "pref_pairs": 339}
        self.assertIsNone(si.refuse_repeat_sft([], m, {}))                                   # DPO on the model's own pass/fail pairs
        self.assertIn("refuse-SFT", si.refuse_repeat_sft([], dict(m, pref_pairs=0), {}))       # the same tasks without pairs still refuse
        self.assertIn("refuse-SFT", si.refuse_repeat_sft([], m, {"train_mode": "sft"}))        # forced SFT on the same tasks still refuses
