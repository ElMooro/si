"""Offline compatibility and honest auction-score labels."""
import importlib.util
import sys
import types
from pathlib import Path
from unittest.mock import patch
SOURCE=Path(__file__).resolve().parents[1]/"source"
sys.path.insert(0,str(SOURCE))
from auction_quality import stamp_quality
from datetime import datetime, timezone
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


def test_dated_measurements_expire_old_scores_without_redefining_current_zero():
    now=datetime(2026,9,17,tzinfo=timezone.utc)
    def report(day, rows):
        return {'freshness':{'latest_auction_date':day},'generated_at':now.isoformat(),'recent_auctions':rows,
                'composite_score':0,'regime':'CALM','tail_risk':{'x':{'heuristic_score':18}},'triggers':[{'action':'LONG'}]}
    current=stamp_quality(report('2026-09-17',[{}]),now)
    assert current['quality']['status']=='fresh' and current['composite_score']==0
    for day,rows,state in [('2026-08-01',[{}],'stale'),('2026-09-18',[{}],'invalid'),(None,[],'unavailable')]:
        out=stamp_quality(report(day,rows),now)
        assert out['quality']['status']==state and out['composite_score'] is None
        assert out['call'] is None and out['tail_risk']['x']['heuristic_score'] is None and not out['triggers']


if __name__ == "__main__":
    tests=[f for n,f in list(globals().items()) if n.startswith("test_")]
    for test in tests: test()
    print(f"Auction label tests passed: {len(tests)}")
