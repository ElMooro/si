from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parents[1] / "source"))

from scoring import contract_error, risk_policy, score_candidate  # noqa: E402
from discovery import apply_lifecycle, build_opportunity_radar  # noqa: E402


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
    assert out["action"] == "ARMED"
    assert out["entry_trigger"]["state"] == "ARMED"
    assert out["technical"]["vs_200d_pct"] == -8
    assert not out["vetoes"]


def test_ready_requires_observed_daily_4h_trigger():
    row = base_row()
    row["daily_4h_triggered"] = True
    out = score_candidate(row, asset_class="STOCK", risk_allows_entries=True)
    assert out["action"] == "READY_TO_SNIPE"
    assert out["entry_trigger"]["state"] == "TRIGGERED"


def test_text_false_is_not_an_observed_entry_trigger():
    row = base_row()
    row["daily_4h_triggered"] = "false"
    out = score_candidate(row, asset_class="STOCK", risk_allows_entries=True)
    assert out["action"] == "ARMED"
    assert out["entry_trigger"]["state"] == "ARMED"


def test_non_finite_strings_cannot_create_positive_evidence():
    row = base_row()
    row["confidence"] = "NaN"
    row["higher_lows"] = None
    row["ad_divergence"] = False
    row["obv_divergence"] = False
    row["bb_width_pctile"] = "Infinity"
    out = score_candidate(row, asset_class="STOCK", risk_allows_entries=True)
    assert out["action"] != "READY_TO_SNIPE"
    assert out["confidence"] == 0
    assert out["technical"]["bb_width_percentile"] is None


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


def test_discovery_tracks_reclaim_without_fabricating_entry_signal():
    feeds = {
        "sector_emergence": {
            "sectors": [{
                "ticker": "XTEST", "name": "Test Sector", "stage": "EMERGING",
                "emergence_score": 88, "breadth_score": 82,
                "signals": ["RS inflecting up", "breadth broadening"],
                "falsifier": "loses the rising 150-day average",
            }]
        },
        "asset_compass": {
            "assets": [{
                "ticker": "XTEST", "label": "Test Sector", "price": 25,
                "excess_vs_cash_pp": 5, "trend": {"ok": True},
                "asym": {"score": 80}, "read": {"bull": ["cheap versus history"], "bear": []},
            }],
            "compass_ranking": [["XTEST", 84]],
            "horizons": {"XTEST": {"opportunity_score": 84, "rr_10y": 3}},
        },
        "capital_flow": {
            "complexes": [{
                "primary": "XTEST", "complex": "Test Sector", "top_conviction_stocks": [],
                "flow_zscore_90d": 1.5, "breadth": 0.82, "accelerating": True,
            }]
        },
    }
    rows = build_opportunity_radar(feeds, [], "SELECTIVE")
    row = next(item for item in rows if item["ticker"] == "XTEST")
    assert row["discovery_stage"] == "HIGH_CONVICTION"
    assert row["action"] == "TRACKING"
    assert row["source_count"] == 3


def test_only_execution_engine_can_emit_ready_to_snipe():
    feeds = {"spinoff_desk": {"top_setups": [{
        "symbol": "VALUE", "name": "Value Inc", "spinoff_score": 99,
        "fundamentals": {"fcf_positive": True}, "thesis": "forced-selling neglect", "risk": "execution",
    }]}}
    row = build_opportunity_radar(feeds, [], "SELECTIVE")[0]
    assert row["score"] > 70
    assert row["action"] == "TRACKING"
    assert row["discovery_stage"] != "ENTRY_READY"


def test_radar_ranks_opportunity_magnitude_before_lifecycle_stage():
    feeds = {
        "spinoff_desk": {"top_setups": [{
            "symbol": "EARLY", "name": "Early Asymmetry", "spinoff_score": 100,
            "fundamentals": {"fcf_positive": True}, "thesis": "forced selling", "risk": "execution",
        }]},
        "sector_emergence": {"sectors": [{
            "ticker": "CONV", "name": "Confirmed Theme", "stage": "EMERGING",
            "emergence_score": 82, "breadth_score": 82,
            "signals": ["breadth broadening"], "falsifier": "breadth fails",
        }]},
        "asset_compass": {
            "assets": [{
                "ticker": "CONV", "label": "Confirmed Theme", "excess_vs_cash_pp": 4,
                "trend": {"ok": True}, "asym": {"score": 82},
                "read": {"bull": ["cheap versus history"], "bear": []},
            }],
            "compass_ranking": [["CONV", 82]],
            "horizons": {"CONV": {"opportunity_score": 82, "rr_10y": 3}},
        },
        "capital_flow": {"complexes": [{
            "primary": "CONV", "complex": "Confirmed Theme",
            "flow_zscore_90d": 1.5, "breadth": 0.8, "accelerating": True,
        }]},
    }
    rows = build_opportunity_radar(feeds, [], "SELECTIVE")
    assert rows[0]["ticker"] == "EARLY"
    assert rows[0]["score"] > rows[1]["score"]
    assert rows[0]["discovery_stage"] == "UNDERAPPRECIATED"
    assert rows[1]["discovery_stage"] == "HIGH_CONVICTION"


def test_extended_sector_is_penalized_and_never_chased():
    feeds = {
        "sector_emergence": {
            "sectors": [{
                "ticker": "XHOT", "name": "Hot Sector", "stage": "EXTENDED",
                "emergence_score": 99, "breadth_score": 99,
                "signals": ["late trend"], "falsifier": "trend failure",
            }]
        },
        "asset_compass": {
            "assets": [{
                "ticker": "XHOT", "label": "Hot Sector", "excess_vs_cash_pp": 8,
                "trend": {"ok": True}, "asym": {"score": 90}, "read": {"bull": [], "bear": []},
            }],
            "compass_ranking": [["XHOT", 95]],
        },
    }
    row = build_opportunity_radar(feeds, [], "SELECTIVE")[0]
    assert row["extended"] is True
    assert row["action"] == "TRACKING"
    assert "do not chase" in " ".join(row["risks"]).lower()


def test_risk_off_preserves_discovery_but_blocks_entry_ready_label():
    execution = [{
        "ticker": "SAFE", "name": "Safe", "asset_class": "ETF",
        "action": "READY_TO_SNIPE", "score": 90, "price": 10,
        "technical": {}, "valuation": {}, "dilution": {}, "risk_reward": {"ratio": 4},
        "evidence": {"flows": [{}], "catalysts": [{}]},
        "cautions": [], "vetoes": [], "entry_trigger": {}, "timeframe": {},
    }]
    row = build_opportunity_radar({}, execution, "DEFENSIVE")[0]
    assert row["discovery_stage"] == "RISK_BLOCKED"
    assert row["action"] == "BUILDING_BASE"


def test_normalizes_unit_interval_buyback_and_flow_scores():
    feeds = {
        "buyback_ranking": {"all_ranked": [{
            "ticker": "NORM", "composite_score": 0.9,
            "ttm_buyback_yield_net_pct": 5, "fcf_coverage_ratio": 2,
        }]},
        "capital_flow": {"complexes": [{
            "primary": "NORM", "complex": "Normalization", "top_conviction_stocks": [],
            "flow_zscore_90d": 2, "breadth": 0.8, "accelerating": True,
        }]},
    }
    row = next(item for item in build_opportunity_radar(feeds, [], "SELECTIVE") if item["ticker"] == "NORM")
    assert row["components"]["capital_confirmation"] >= 80


def test_correlated_rotation_sources_count_as_one_family():
    feeds = {
        "sector_emergence": {"sectors": [{
            "ticker": "XROT", "stage": "EMERGING", "emergence_score": 90, "breadth_score": 90,
        }]},
        "industry_rotation": {"ladder": [{
            "etf": "XROT", "leadership_score": 90, "rel_mom_pctile": 90, "tag": "ABSORPTION",
        }]},
    }
    row = build_opportunity_radar(feeds, [], "SELECTIVE")[0]
    assert row["source_count"] == 1
    assert row["source_families"] == ["rotation"]


def test_fx_list_schema_and_bullish_cftc_positioning_are_consumed():
    feeds = {
        "fx_intelligence": {"currencies": [{
            "code": "JPY", "available": True, "momentum_z": 1.5, "trend": "strengthening",
        }]},
        "cftc_positioning": {"top_divergences": [{
            "symbol": "GC", "name": "Gold", "type": "SMART_LONG_DUMB_SHORT",
            "severity": 90, "interpretation": "contrarian bullish",
        }]},
    }
    rows = build_opportunity_radar(feeds, [], "SELECTIVE")
    assert next(item for item in rows if item["ticker"] == "EWJ")["sources"] == ["fx-intelligence"]
    assert next(item for item in rows if item["ticker"] == "GLD")["sources"] == ["cftc-positioning"]


def test_lifecycle_is_stable_and_records_promotion_and_dormancy():
    now1 = "2026-09-04T12:00:00+00:00"
    now2 = "2026-09-04T13:00:00+00:00"
    initial = [{
        "ticker": "LIFE", "asset_class": "ETF", "discovery_stage": "UNDERAPPRECIATED",
        "score": 60, "sources": ["asset-compass"],
    }]
    rows1, ledger1, changes1 = apply_lifecycle(initial, [], now1)
    promoted = [{**initial[0], "discovery_stage": "HIGH_CONVICTION", "score": 75}]
    rows2, ledger2, changes2 = apply_lifecycle(promoted, ledger1["candidates"], now2)
    assert changes1["new"][0]["opportunity_id"] == "ETF:LIFE"
    assert changes2["promoted"][0]["from"] == "UNDERAPPRECIATED"
    assert rows2[0]["lifecycle"]["first_seen"] == now1
    assert rows2[0]["lifecycle"]["observations"] == 2
    _, ledger3, changes3 = apply_lifecycle([], ledger2["candidates"], "2026-09-04T14:00:00+00:00")
    assert changes3["dormant"][0]["opportunity_id"] == "ETF:LIFE"
    assert ledger3["candidates"][0]["status"] == "DORMANT"


def test_fortress_ledger_and_new_universe_candidates_are_not_erased():
    feeds = {
        "fortress": {"ledger": [{
            "ticker": "LEDG", "name": "Ledger Corp", "gates_passed": 3,
            "valuation_score": 85, "flow_score": 75, "composite": 70,
            "asymmetry": 3, "close": 12, "vs_ema250_pct": -8,
        }]},
        "universe_discovery": {
            "ipo_calendar": {"items": [{"symbol": "NEWI", "company": "New Issue"}]},
            "new_registrants": {"items": []},
            "threshold_crossers": {"items": []},
        },
    }
    rows = build_opportunity_radar(feeds, [], "SELECTIVE")
    ledger = next(item for item in rows if item["ticker"] == "LEDG")
    new_issue = next(item for item in rows if item["ticker"] == "NEWI")
    assert ledger["action"] == "TRACKING"
    assert "fortress-execution" in ledger["sources"]
    assert new_issue["discovery_stage"] == "EARLY_SIGNAL"
    assert new_issue["entry_trigger"]["state"] == "WAIT"


def test_execution_rejection_does_not_erase_discovery_candidate():
    rejected = [{
        "ticker": "NOENTRY", "name": "Rejected Entry, Preserved Thesis",
        "asset_class": "STOCK", "action": "REJECTED", "score": 20,
        "technical": {}, "valuation": {}, "dilution": {},
        "risk_reward": {}, "evidence": {}, "vetoes": ["No bottom confirmation"],
        "cautions": [], "entry_trigger": {"state": "WAIT"}, "timeframe": {},
    }]
    rows = build_opportunity_radar({}, rejected, "SELECTIVE")
    assert [row["ticker"] for row in rows] == ["NOENTRY"]
    assert rows[0]["action"] == "REJECTED"
    assert rows[0]["discovery_stage"] == "EARLY_SIGNAL"


def test_sparse_positioning_signal_cannot_outrank_correlated_thesis():
    feeds = {
        "cftc_positioning": {"top_divergences": [{
            "symbol": "GC", "name": "Gold", "type": "SMART_LONG_DUMB_SHORT",
            "severity": 100,
        }]},
        "sector_emergence": {"sectors": [{
            "ticker": "CONV", "name": "Corroborated", "stage": "EMERGING",
            "emergence_score": 78, "breadth_score": 75, "signals": [], "falsifier": "fails",
        }]},
        "asset_compass": {
            "assets": [{
                "ticker": "CONV", "label": "Corroborated", "excess_vs_cash_pp": 2,
                "trend": {"ok": True}, "asym": {"score": 70},
                "read": {"bull": ["cheap"], "bear": []},
            }],
            "compass_ranking": [["CONV", 73]],
            "horizons": {"CONV": {"opportunity_score": 73, "rr_10y": 2.5}},
        },
        "capital_flow": {"complexes": [{
            "primary": "CONV", "complex": "Corroborated",
            "flow_zscore_90d": 1, "breadth": 0.7, "accelerating": True,
        }]},
    }
    rows = build_opportunity_radar(feeds, [], "SELECTIVE")
    assert rows[0]["ticker"] == "CONV"
    assert next(row for row in rows if row["ticker"] == "GLD")["component_coverage"] == 0.15


def test_contract_rejects_malformed_optional_collection_and_accepts_dict():
    spec = {"contract": {
        "types": {"rows": "list", "lookup": "dict"},
        "item_types": {"rows": "dict"},
    }}
    assert contract_error(spec, {"rows": [{"ok": True}], "lookup": {}}) is None
    assert contract_error(spec, {"rows": {"bad": True}, "lookup": {}}) == "rows must be list"
    assert contract_error(spec, {"rows": ["bad"], "lookup": {}}) == "rows items must be dict"


def test_scoring_ignores_malformed_nested_optional_maps():
    row = base_row()
    row["flows"] = "bad"
    result = score_candidate(
        row,
        asset_class="STOCK",
        risk_allows_entries=True,
        bottom_confirmation="bad",
        best_setup="bad",
        floor="bad",
        buyback="bad",
        options="bad",
    )
    assert result["ticker"] == "TEST"
    row["flows"] = {}
    result = score_candidate(
        row,
        asset_class="STOCK",
        risk_allows_entries=True,
        bottom_confirmation={"dark_pool": "bad"},
        best_setup={"red_flags": "bad"},
    )
    assert result["ticker"] == "TEST"


def test_unavailable_supporting_feed_holds_candidate_instead_of_hiding_it():
    prior = [{
        "opportunity_id": "COMMODITY:GLD", "ticker": "GLD", "asset_class": "COMMODITY",
        "stage": "UNDERAPPRECIATED", "score": 70, "sources": ["cftc-positioning"],
        "first_seen": "2026-09-01T12:00:00+00:00", "last_seen": "2026-09-03T12:00:00+00:00",
        "observations": 3, "max_score": 72,
    }]
    rows, ledger, changes = apply_lifecycle(
        [], prior, "2026-09-04T12:00:00+00:00", {"cftc_positioning"}
    )
    assert rows[0]["ticker"] == "GLD"
    assert rows[0]["discovery_stage"] == "EVIDENCE_HOLD"
    assert rows[0]["confidence"] == 0
    assert changes["held"][0]["opportunity_id"] == "COMMODITY:GLD"
    assert changes["dormant"] == []
    assert ledger["candidates"][0]["status"] == "EVIDENCE_HOLD"


def test_feed_owner_mapping_holds_absent_and_partially_rebuilt_candidates():
    prior = [
        {
            "opportunity_id": "STOCK:FORT", "ticker": "FORT", "asset_class": "STOCK",
            "stage": "UNDERAPPRECIATED", "score": 64, "sources": ["fortress-execution"],
        },
        {
            "opportunity_id": "ETF:FLOW", "ticker": "FLOW", "asset_class": "ETF",
            "stage": "HIGH_CONVICTION", "score": 72,
            "sources": ["asset-compass", "capital-flow-radar"],
        },
    ]
    rebuilt = [{
        "ticker": "FLOW", "asset_class": "ETF", "discovery_stage": "EARLY_SIGNAL",
        "action": "TRACKING", "score": 48, "confidence": 0.3, "component_coverage": 0.25,
        "source_count": 1, "sources": ["asset-compass"], "source_families": ["cross_asset_value"],
        "components": {}, "why_underappreciated": [], "catalysts": [], "risks": [],
        "extended": False, "technical": {}, "valuation": {}, "dilution": {}, "risk_reward": {},
        "entry_trigger": {"state": "WAIT"}, "evidence": {}, "vetoes": [], "cautions": [],
        "timeframe": {}, "plain_english": "partial", "deep_links": [],
    }]
    rows, _, changes = apply_lifecycle(
        rebuilt, prior, "2026-09-04T12:00:00+00:00", {"fortress", "capital_flow"}
    )
    assert {row["ticker"] for row in rows} == {"FORT", "FLOW"}
    assert all(row["discovery_stage"] == "EVIDENCE_HOLD" for row in rows)
    assert {item["ticker"] for item in changes["held"]} == {"FORT", "FLOW"}
    assert changes["dormant"] == []


def test_crypto_ma200_contract_quarantines_malformed_rows():
    import json

    registry = json.loads(
        (Path(__file__).parents[1] / "source/input_registry.json").read_text()
    )
    spec = next(row for row in registry["inputs"] if row["id"] == "crypto_ma200")
    malformed = {"fresh_breakdowns_below": ["bad"]}
    assert contract_error(spec, malformed) == "fresh_breakdowns_below items must be dict"


def test_release_validation_is_read_only_until_alias_promotion():
    root = Path(__file__).parents[4]
    handler = (root / "aws/lambdas/justhodl-khalid/source/lambda_function.py").read_text()
    release = (root / "scripts/deploy_khalid_candidate.sh").read_text()
    validation_branch = handler.index("if validation_only:")
    first_authoritative_write = handler.index("_write(OUT_KEY, output)")
    assert validation_branch < first_authoritative_write
    assert '"validation_only": True' in handler
    assert release.index('"mode":"validate_only"') < release.index("publish-version")
    assert release.index('function-name "${fn}:live"') > release.index("update-alias")
    assert "rollback_alias" in release
    assert "data/khalid-candidates.json" in release
    assert "data/history/khalid.json" in release
    assert '"s3://justhodl-dashboard-live/$key"' in release
    assert 'exit "$failure_status"' in release
    assert "restore_failed=1" in release


def test_katlin_source_survives_scoring_and_discovery_without_bypassing_entry():
    row = base_row()
    row["_source"] = "katlin"
    row["_katlin"] = {"tier": "PRIME", "conviction": 90}
    scored = score_candidate(row, asset_class="STOCK", risk_allows_entries=True)
    assert scored["source"] == "katlin"
    assert scored["katlin"]["tier"] == "PRIME"
    assert scored["action"] == "ARMED"
    radar = build_opportunity_radar({}, [scored], "SELECTIVE")
    assert radar[0]["sources"] == ["katlin"]
    assert radar[0]["entry_trigger"]["state"] == "ARMED"


def test_katlin_registry_quarantines_malformed_top_level_collections():
    import json

    registry = json.loads(
        (Path(__file__).parents[1] / "source/input_registry.json").read_text()
    )
    spec = next(row for row in registry["inputs"] if row["id"] == "katlin")
    assert contract_error(
        spec,
        {"picks": ["bad"], "watch": [], "war_room": {}},
    ) == "picks items must be dict"
    assert contract_error(
        spec,
        {"picks": [], "watch": "bad", "war_room": {}},
    ) == "watch must be list"


def test_discovery_rejects_non_finite_component_values():
    feeds = {
        "asset_compass": {
            "assets": [{
                "ticker": "INF",
                "rank": "Infinity",
                "asymmetry": "Infinity",
                "opportunity_score": "NaN",
                "reward_risk": "Infinity",
            }],
            "compass_ranking": [],
            "horizons": {},
        },
    }
    rows = build_opportunity_radar(feeds, [], "SELECTIVE")
    assert rows[0]["components"]["underappreciation"] is None
    assert rows[0]["components"]["asymmetry"] == 0
    assert rows[0]["raw_score"] < 10
    assert rows[0]["discovery_stage"] == "EARLY_SIGNAL"
