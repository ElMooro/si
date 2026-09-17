"""Offline compatibility and honest auction-score labels."""
import importlib.util
import sys
import types
from pathlib import Path
from unittest.mock import patch
SOURCE=Path(__file__).resolve().parents[1]/"source"
with patch.dict(sys.modules, {"managed_secret": types.SimpleNamespace(managed_secret=lambda *a, **k: "TEST_ONLY")}):
    spec=importlib.util.spec_from_file_location("auction_v2_test",SOURCE/"auction_crisis_v2.py")
    engine=importlib.util.module_from_spec(spec)
    spec.loader.exec_module(engine)


def test_heuristics_are_not_probabilities_and_legacy_alias_survives():
    out=engine.compute_tail_risk([{"indicator_scores": {"pd_absorption":80}}],
        {"current":{"composite":30}, "series":[]}, {"coupons_gt_3y":{"composite":40}}, {},
        {"repo_stress":{"regime":"WATCH"}, "dollar_strength":{"change_30d_pct":1}})
    for row in out.values():
        assert row["probability"] is None and row["calibrated"] is False
        assert row["forecast_horizon_days"] is None and row["unit"] == "score_0_100"
        assert 0 <= row["heuristic_score"] <= 100
    assert out["p_soft_demand_30d"]["heuristic_score"] == 45
    assert out["p_failed_auction_30d"]["alias_of"] == "p_soft_demand_30d"


def test_missing_inputs_do_not_become_low_event_risk():
    out=engine.compute_tail_risk([],{}, {}, {}, {})
    assert all(row["heuristic_score"] is None and row["status"] == "unavailable" for row in out.values())


if __name__ == "__main__":
    tests=[f for n,f in list(globals().items()) if n.startswith("test_")]
    for test in tests: test()
    print(f"Auction label tests passed: {len(tests)}")
