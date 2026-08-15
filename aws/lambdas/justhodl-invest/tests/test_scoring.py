"""
Unit tests for scoring.py. All inputs below are synthetic fixtures for
testing the MATH ONLY — they are not live market data and must never be
read as such. Run: pytest aws/lambdas/justhodl-invest/tests/ -v
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "source"))

import scoring  # noqa: E402


# ── zscore ──────────────────────────────────────────────────────────────

def test_zscore_needs_min_history():
    assert scoring.zscore(5.0, [1, 2, 3]) is None  # n<8


def test_zscore_basic():
    hist = [1, 2, 3, 4, 5, 6, 7, 8]  # mean 4.5, sd ~2.449
    z = scoring.zscore(9.0, hist)
    assert z is not None
    assert 1.8 < z < 2.0


def test_zscore_none_value():
    assert scoring.zscore(None, [1, 2, 3, 4, 5, 6, 7, 8]) is None


# ── Tier 1: confirm_indicator ───────────────────────────────────────────

def _leg(leg_id, z, direction=1, available=True, voting=True):
    return scoring.LegResult(leg_id=leg_id, z=z, direction=direction,
                              available=available, voting=voting)


def test_tier1_insufficient_when_too_few_legs_available():
    legs = [_leg("a", 2.0), _leg("b", None, available=False)]
    r = scoring.confirm_indicator(legs)
    assert r["status"] == "INSUFFICIENT_DATA"


def test_tier1_confirmed_when_majority_agree():
    legs = [_leg("a", 1.5), _leg("b", 2.1), _leg("c", 0.3)]
    r = scoring.confirm_indicator(legs)
    assert r["status"] == "CONFIRMED"
    assert r["confirmed_legs"] == 2


def test_tier1_conflicting_when_legs_disagree_strongly():
    legs = [_leg("a", 2.0), _leg("b", -2.0)]
    r = scoring.confirm_indicator(legs)
    assert r["status"] == "CONFLICTING"


def test_tier1_turning_on_single_early_leg():
    legs = [_leg("a", 1.2), _leg("b", 0.2)]
    r = scoring.confirm_indicator(legs)
    assert r["status"] == "TURNING"


def test_tier1_direction_flips_confirmation():
    # a leg where "falling" is bullish (direction=-1): raw z is negative,
    # signed_z should be positive and count as confirming
    legs = [_leg("a", -1.5, direction=-1), _leg("b", 1.4, direction=1)]
    r = scoring.confirm_indicator(legs)
    assert r["status"] == "CONFIRMED"


def test_tier1_insufficient_when_data_is_live_but_history_too_short_for_zscore():
    """The real-world cold-start case (found live, 2026-08-15): a leg can
    have available=True (fleet_io.read_leg_value returned a real number
    today) while z is still None, because zscore() needs >=8 days of
    accrued history and a brand-new engine hasn't run that many times
    yet. confirm_indicator's `available` list requires BOTH -- a leg
    with live data but no z history does not count toward
    MIN_LEGS_AVAILABLE. This is correct: it means every indicator
    honestly reports INSUFFICIENT_DATA for roughly the first week after
    initial deploy, not a bug. (test_tier1_insufficient_when_too_few_legs_
    available above only exercises available=False; this isolates the
    available=True-but-z=None case specifically, since that's the one
    that actually happened live and took several diagnostic round-trips
    to pin down precisely because it looks identical from the outside.)"""
    legs = [_leg("a", None, available=True), _leg("b", None, available=True)]
    r = scoring.confirm_indicator(legs)
    assert r["status"] == "INSUFFICIENT_DATA"
    assert r["available_legs"] == 0
    # detail still carries the live-but-unscored legs for audit, distinct
    # from a leg that's genuinely unavailable
    assert all(d["available"] is True for d in r["detail"])


def test_tier1_boom_stage_value_volume_divergence_shape():
    """Regression-style test mirroring Khalid's own worked case: Korea
    export VALUE up strongly while port VOLUME is soft. Value leg should
    vote confirm; volume leg's soft print doesn't clear the conflict
    threshold either, so the bundle lands on TURNING/CONFIRMED."""
    legs = [
        _leg("korea_export_value_yoy", 2.4),   # +52.3%-style print, strong z
        _leg("korea_port_volume", -0.6),       # -4.3%-style print, mild soft z, not a conflict-strength z
    ]
    r = scoring.confirm_indicator(legs)
    assert r["status"] in ("CONFIRMED", "TURNING")
    assert r["conflicting_legs"] == 0  # -0.6 doesn't clear the conflict threshold


def test_tier1_diagnostic_leg_does_not_veto_a_confirmation():
    """The real-world version of the above: when the volume leg's z is
    LARGE and negative (a much bigger move than the value leg's history
    would suggest is normal), a voting leg would wrongly flip this to
    CONFLICTING. A non-voting (diagnostic) leg must not be able to do that
    -- it is carried in `detail` for the reader, never counted."""
    legs = [
        _leg("korea_export_value_yoy", 2.6),
        _leg("korea_export_value_yoy_flash", 2.5),
        _leg("korea_port_volume", -8.0, voting=False),  # huge negative z, but diagnostic-only
    ]
    r = scoring.confirm_indicator(legs)
    assert r["status"] == "CONFIRMED"
    assert r["conflicting_legs"] == 0
    assert r["available_legs"] == 2  # the diagnostic leg isn't counted as available either
    # but it's still visible for audit:
    volume_row = next(d for d in r["detail"] if d["leg_id"] == "korea_port_volume")
    assert volume_row["voting"] is False
    assert volume_row["signed_z"] == -8.0


# ── Tier 2: spx_opportunity_cost_gate ───────────────────────────────────

def test_tier2_insufficient_without_er():
    r = scoring.spx_opportunity_cost_gate(None, 2.66, 5.0)
    assert r["status"] == "INSUFFICIENT_DATA"


def test_tier2_insufficient_without_tracking_error():
    r = scoring.spx_opportunity_cost_gate(9.0, 2.66, None)
    assert r["status"] == "INSUFFICIENT_DATA"
    assert r["excess_return_pp"] == round(9.0 - 2.66, 2)


def test_tier2_fails_on_thin_edge_even_with_good_ir():
    # small excess return, small tracking error -> good IR but under min_excess_pp
    r = scoring.spx_opportunity_cost_gate(4.0, 2.66, 1.0)
    assert r["information_ratio"] > scoring.IR_MIN_DEFAULT
    assert r["excess_return_pp"] < scoring.MIN_EXCESS_RETURN_PP_DEFAULT
    assert r["pass"] is False


def test_tier2_fails_on_big_edge_but_too_volatile():
    r = scoring.spx_opportunity_cost_gate(12.0, 2.66, 30.0)  # huge tracking error
    assert r["excess_return_pp"] >= scoring.MIN_EXCESS_RETURN_PP_DEFAULT
    assert r["information_ratio"] < scoring.IR_MIN_DEFAULT
    assert r["pass"] is False


def test_tier2_passes_when_both_clear():
    r = scoring.spx_opportunity_cost_gate(9.0, 2.66, 8.0)
    assert r["pass"] is True
    assert r["verdict"] == "OUTPERFORM_EXPECTED"
    assert r["pp_kind"] == "estimated"


def test_tier2_matches_khalid_worked_spy_numbers():
    # SPY 1y 2.66% (real figure from forward-returns v2 ship-report).
    # A sector at 9% ER with modest 6pp tracking error clears both legs.
    r = scoring.spx_opportunity_cost_gate(9.0, 2.66, 6.0)
    assert r["excess_return_pp"] == 6.34
    assert r["pass"] is True


# ── Tier 3: stock_composite_score / vs_industry_etf_verdict ────────────

def test_tier3_insufficient_when_majority_of_weight_missing():
    components = {"backlog_growth": 80.0}  # only 0.30 of 1.0 weight present
    r = scoring.stock_composite_score(components)
    assert r["status"] == "INSUFFICIENT_DATA"


def test_tier3_reweights_when_some_components_present():
    components = {"backlog_growth": 90.0, "valuation_discount": 70.0,
                   "catalyst_strength": 60.0}  # 0.75 of 1.0 weight
    r = scoring.stock_composite_score(components)
    assert r["status"] == "OK"
    assert r["reweighted"] is True
    assert 60.0 < r["score"] < 90.0


def test_tier3_full_components_no_reweight():
    components = {"backlog_growth": 80, "valuation_discount": 70,
                   "catalyst_strength": 90, "net_share_retirement": 50,
                   "qoq_acceleration": 60}
    r = scoring.stock_composite_score(components)
    assert r["status"] == "OK"
    assert r["reweighted"] is False
    expected = (80 * .30 + 70 * .25 + 90 * .20 + 50 * .15 + 60 * .10)
    assert abs(r["score"] - round(expected, 1)) < 0.05


def test_cycle_awareness_peak_margin_trap():
    flag = scoring.cycle_awareness_flag(None, pe_5y_pctile=20, margin_pctile=90)
    assert flag == "PEAK_MARGIN_TRAP"


def test_cycle_awareness_early_cycle_watch():
    flag = scoring.cycle_awareness_flag(None, pe_5y_pctile=60, margin_pctile=15)
    assert flag == "EARLY_CYCLE_WATCH"


def test_vs_industry_etf_verdict_outperform():
    assert scoring.vs_industry_etf_verdict(85, 70) == "OUTPERFORM_EXPECTED"


def test_vs_industry_etf_verdict_in_line_means_buy_etf():
    assert scoring.vs_industry_etf_verdict(72, 70) == "IN_LINE_BUY_THE_ETF"


def test_vs_industry_etf_verdict_insufficient():
    assert scoring.vs_industry_etf_verdict(None, 70) == "INSUFFICIENT_DATA"
