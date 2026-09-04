from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parents[1] / "source"))

from scoring import contract_error, risk_policy, score_candidate  # noqa: E402


def base_row():
    return {
        "ticker": "TEST",
        "name": "Test Asset",
        "close": 10,
        "vs_ema200_pct": -8,
        "vs_ema250_pct": -12,
        "bb_width_pctile": 8,
        "vcp_ok": True,
        "volume_dryup": 0.62,
        "higher_lows": 3,
        "ad_divergence": True,
        "rsi": 38,
        "flow_score": 82,
        "industry_inflow_major": True,
        "catalyst_score": 75,
        "has_contract_catalyst": True,
        "valuation_score": 88,
        "fwd_pe": 8,
        "peg": 0.7,
        "dilution_yoy_pct": 1,
        "confidence": 0.9,
        "asymmetry": 4,
        "safety_score": 75,
        "adv_usd_20d": 10_000_000,
        "trade_plan": {"reward_to_risk": 99, "pivot": 10.5, "stop": 9, "target": 13, "target_2": 15},
    }


def test_strict_setup_can_arm():
    out = score_candidate(base_row(), asset_class="STOCK", risk_allows_entries=True)
    assert out["action"] == "READY_TO_SNIPE"
    assert out["technical"]["vs_200d_pct"] == -8
    assert not out["vetoes"]


def test_above_200_day_is_never_actionable():
    row = base_row()
    row["vs_ema200_pct"] = 2
    out = score_candidate(row, asset_class="STOCK", risk_allows_entries=True)
    assert out["action"] != "READY_TO_SNIPE"
    assert any("does not chase" in x for x in out["vetoes"])


def test_reward_risk_is_recomputed_from_entry_geometry():
    row = base_row()
    row["trade_plan"] = {"reward_to_risk": 99, "pivot": 20, "stop": 19, "target_2": 19.5}
    out = score_candidate(row, asset_class="STOCK", risk_allows_entries=True)
    assert out["risk_reward"]["ratio"] is None
    assert out["action"] != "READY_TO_SNIPE"


def test_missing_rsi_or_dilution_cannot_arm_stock():
    row = base_row()
    row["rsi"] = None
    row["dilution_yoy_pct"] = None
    out = score_candidate(row, asset_class="STOCK", risk_allows_entries=True)
    assert out["action"] == "BUILDING_BASE"
    assert "Dilution coverage is unavailable" in " ".join(out["cautions"])


def test_dilution_is_hard_veto():
    row = base_row()
    row["dilution_yoy_pct"] = 18
    out = score_candidate(row, asset_class="STOCK", risk_allows_entries=True)
    assert out["action"] == "REJECTED"
    assert any("Share count increased" in x for x in out["vetoes"])


def test_missing_ma_fails_closed():
    row = base_row()
    row.pop("vs_ema200_pct")
    out = score_candidate(row, asset_class="ETF", risk_allows_entries=True)
    assert out["action"] == "REJECTED"


def test_stale_critical_feed_forces_data_hold():
    health = [{"critical": True, "status": "STALE", "key": "data/risk-gate.json"}]
    out = risk_policy({"posture": "RISK_ON"}, {}, health)
    assert out["mode"] == "DATA_HOLD"
    assert out["allows_new_entries"] is False


def test_risk_off_blocks_entries():
    out = risk_policy({"posture": "RISK_OFF"}, {"defcon_level": 3}, [])
    assert out["mode"] == "DEFENSIVE"
    assert out["allows_new_entries"] is False


def test_unknown_or_incomplete_risk_contract_fails_closed():
    out = risk_policy({}, {}, [])
    assert out["mode"] == "DATA_HOLD"
    assert out["allows_new_entries"] is False

    out = risk_policy({"posture": "SURPRISE_STATE"}, {"defcon_level": 4}, [])
    assert out["mode"] == "DATA_HOLD"
    assert out["allows_new_entries"] is False


def test_explicit_zero_confidence_is_preserved_and_blocks_entry():
    row = base_row()
    row["confidence"] = 0
    out = score_candidate(row, asset_class="STOCK", risk_allows_entries=True)
    assert out["confidence"] == 0
    assert out["action"] == "BUILDING_BASE"
    assert "below the 0.60 entry floor" in " ".join(out["cautions"])


def test_required_feed_contract_rejects_fresh_but_empty_payload():
    spec = {
        "contract": {
            "required_all": ["posture", "composite"],
            "types": {"posture": "string", "composite": "number"},
            "allowed": {"posture": ["RISK_ON", "NEUTRAL", "RISK_OFF", "SEVERE"]},
        }
    }
    assert contract_error(spec, {}) == "missing required fields: posture, composite"
    assert "unsupported value" in contract_error(
        spec, {"posture": "SURPRISE", "composite": 50}
    )
    assert contract_error(spec, {"posture": "RISK_ON", "composite": 50}) is None
