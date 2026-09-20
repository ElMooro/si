"""Runtime wrap must intercept Gear-B before another GPU hour."""
from __future__ import annotations

import os
import types
import sys


def test_wrap_refuses_old_family(monkeypatch=None):
    os.environ["JH_AI_DOCTRINE"] = "1"
    gb = types.ModuleType("gear_b")

    def tick(*a, launch=True, **k):
        return {"built": {"kinds": {"public_benchmark_train": 570}, "kept": 570, "families": 2, "eligibility_digest": "abc"}, "launched": "JOB" if launch else None, "refusal": None if launch else "launch disabled"}

    def promotion(c, b, minimum_cases=30):
        return {"eligible": True}

    def public_status(*a, **k):
        return {"dataset": {"kinds": {"public_benchmark_train": 570}, "kept": 570, "families": 2}}

    gb.tick = tick
    gb.promotion = promotion
    gb.public_status = public_status
    gb.load_control = lambda *a, **k: {"require_new_family_after_flat_gens": 3, "min_dpo_pairs": 64}
    gb.latest_unlaunched_manifest = lambda *a, **k: {"kinds": {"public_benchmark_train": 570}, "kept": 570, "families": 2, "eligibility_digest": "abc"}
    gb._job_records = lambda *a, **k: []
    sys.modules["gear_b"] = gb

    mr = types.ModuleType("market_read")
    mr.compose_read = lambda *a, **k: {"overall": "x", "macro": "y", "stocks": {"stance": "SELECTIVE", "read": "s"}, "bonds": {"stance": "NEUTRAL", "read": "b"}, "metals": {"stance": "HOLD", "read": "m"}, "crypto": {"stance": "AVOID", "read": "c"}, "what_would_change_my_mind": ["dxy"]}
    mr.write_lessons = lambda *a, **k: {"lessons": []}
    sys.modules["market_read"] = mr

    import importlib
    import gear_b_doctrine
    importlib.reload(gear_b_doctrine)
    assert gear_b_doctrine.install()
    out = gb.tick(None, None, private_bucket="p", public_bucket="u", policy={}, role_arn="", projected={}, pricing=None, describe_card=None, launch=True)
    assert out["launched"] is None
    assert "curiosity refuse-SFT" in (out.get("refusal") or "")
    promo = gb.promotion({"score": 0.8232, "passed": 135, "n": 164}, {"score": 0.8232, "passed": 135, "n": 164})
    assert promo["eligible"] is False


if __name__ == "__main__":
    test_wrap_refuses_old_family()
    print("WRAP_OK")
