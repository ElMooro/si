import copy
import json
import math
from datetime import timedelta
from pathlib import Path

import pytest

import jhsignal as J
from jh_fixtures import REPO, ROOT, ts


def base_signal(**over):
    kw = dict(engine_id="insider_radar", engine_version="1.0", entity_type="equity", symbol="NVDA", category="smart_money",
              signal_type="insider_cluster_buy", score=0.78, confidence=0.84, data_asof=ts(2), half_life_days=21)
    kw.update(over)
    return J.make_signal(**kw)


# ---------------------------------------------------------------- schema ----
class TestSchema:
    def test_builder_produces_valid_signal(self):
        s = base_signal()
        assert J.validate(s) == []
        assert s["entity_id"] == "equity:NVDA" and s["direction"] == "bullish" and s["horizon"] == "SWING"
        assert s["horizon_min_days"] == 5 and s["horizon_max_days"] == 90
        assert s["freshness_ttl_seconds"] == 2 * 21 * 86400

    def test_native_validator_agrees_with_json_schema(self):
        jsonschema = pytest.importorskip("jsonschema")
        schema = json.loads((REPO / "schemas" / "jhsignal-1.0.json").read_text())
        s = base_signal()
        jsonschema.validate(s, schema)
        for tamper in ({"score": 1.5}, {"direction": "up"}, {"horizon": "WEEKLY"}, {"entity_id": "NVDA"}, {"confidence": -0.1}, {"extra_field": 1}):
            bad = dict(s, **tamper)
            assert J.validate(bad), tamper
            with pytest.raises(jsonschema.ValidationError):
                jsonschema.validate(bad, schema)

    @pytest.mark.parametrize("tamper,needle", [
        ({"score": 2.0}, "score"), ({"score": "0.5"}, "score"), ({"direction": "very_bullish"}, "direction"),
        ({"direction": "bearish"}, "sign"), ({"horizon": "MONTHLY"}, "horizon"), ({"entity_id": "nvda"}, "entity_id"),
        ({"entity_type": "etf"}, "entity_type"), ({"confidence": 1.2}, "confidence"), ({"half_life_days": 0}, "half_life"),
        ({"freshness_ttl_seconds": 10}, "freshness_ttl"), ({"horizon_min_days": 100}, "horizon_min_days"),
        ({"quality": {"source_reliability": 1.0}}, "quality"), ({"affects": [{"entity_id": "x"}]}, "affects"),
        ({"signal_id": "not-a-uuid"}, "signal_id"), ({"data_asof": "yesterday"}, "data_asof"),
    ])
    def test_malformed_rejected_never_coerced(self, tamper, needle):
        bad = dict(base_signal(), **tamper)
        probs = J.validate(bad)
        assert probs and any(needle in p for p in probs), probs
        with pytest.raises(J.JHSignalError):
            J.assert_valid(bad)

    def test_future_timestamp_rejected(self, now):
        s = base_signal(now=now)
        s["data_asof"] = J.iso(now + timedelta(minutes=10))
        assert any("future" in p for p in J.validate(s, now=now))
        s2 = base_signal(now=now)
        s2["observed_at"] = J.iso(now + timedelta(hours=1))
        assert any("future" in p for p in J.validate(s2, now=now))

    def test_builder_refuses_missing_score(self):
        with pytest.raises(J.JHSignalError):
            base_signal(score=None)
        with pytest.raises(J.JHSignalError):
            base_signal(score=float("nan"))
        with pytest.raises(J.JHSignalError):
            base_signal(confidence=None)

    def test_neutral_score_gets_neutral_direction(self):
        s = base_signal(score=0.05)
        assert s["direction"] == "neutral"
        assert J.direction_from_numeric(-0.5) == "bearish" and J.direction_from_numeric(0.95) == "strong_bullish" and J.direction_from_numeric(-0.2) == "slightly_bearish"

    def test_signal_key_and_idempotency(self):
        a = base_signal(); b = copy.deepcopy(a); b["signal_id"] = "0" * 8 + "-0000-0000-0000-000000000000"
        assert J.signal_key(a) == "insider_radar#insider_cluster_buy#SWING"
        assert J.idempotency_key(a) == J.idempotency_key(b)   # same fact, different uuid
        c = base_signal(score=0.3)
        assert J.idempotency_key(a) != J.idempotency_key(c)


# ------------------------------------------------------------- decay --------
class TestDecay:
    def test_half_life_curve(self):
        assert J.freshness_weight(0, 21) == 1.0
        assert abs(J.freshness_weight(21, 21) - 0.5) < 1e-9
        assert abs(J.freshness_weight(42, 21) - 0.25) < 1e-9
        assert abs(J.freshness_weight(63, 21) - 0.125) < 1e-9

    def test_freshness_states(self, now):
        s = base_signal(now=now, half_life_days=1, freshness_ttl_seconds=3600, data_asof=ts(0))
        assert J.freshness_state(s, now) == "FRESH"
        assert J.freshness_state(s, now + timedelta(hours=1.5)) == "STALE"
        assert J.freshness_state(s, now + timedelta(hours=3)) == "EXPIRED"

    def test_effective_strength_factors(self, now):
        s = base_signal(now=now, score=0.8, confidence=0.5, data_asof=ts(0))
        e = J.effective_strength(s, now=now, reliability_weight=0.9, regime_fit=0.8, independence_weight=0.5)
        assert abs(e["effective"] - 0.8 * 0.5 * 1.0 * 0.9 * 0.8 * 0.5) < 1e-6
        old = J.effective_strength(s, now=now + timedelta(days=21), reliability_weight=0.9, regime_fit=0.8, independence_weight=0.5)
        assert abs(old["freshness_weight"] - 0.5) < 1e-6 and abs(old["effective"] - e["effective"] * 0.5) < 1e-6


# ---------------------------------------------------------- entities --------
class TestEntities:
    def test_canonical_symbols(self):
        assert J.canonical_symbol("brk.b") == "BRK-B"
        assert J.canonical_symbol("X:BTCUSD") == "BTC" and J.canonical_symbol("X:ETHUSDT") == "ETH"
        assert J.canonical_symbol("^GSPC") == "GSPC"
        assert J.entity_id("crypto", "X:BTCUSD") == "crypto:BTC"
        with pytest.raises(J.JHSignalError):
            J.entity_id("stock", "NVDA")

    def test_universe_resolves_aliases(self, universe):
        assert universe["GOOG"]["entity_id"] == "equity:GOOGL"
        assert universe["BTCUSD"]["entity_id"] == "crypto:BTC"
        assert universe["SPX"]["entity_id"] == "etf:SPY"


# ------------------------------------------------------------ envelope ------
class TestEnvelope:
    def test_root_parent_depth_chain_and_cap(self):
        e0 = J.envelope({"a": 1}, origin_engine="bridge")
        assert e0["propagation_depth"] == 0 and e0["root_event_id"] == e0["event_id"] and e0["parent_event_id"] is None
        e1 = J.envelope({"b": 2}, origin_engine="fusion", parent=e0, max_depth=2)
        e2 = J.envelope({"c": 3}, origin_engine="x", parent=e1, max_depth=2)
        assert e2["propagation_depth"] == 2 and e2["root_event_id"] == e0["event_id"] and e2["parent_event_id"] == e1["event_id"]
        with pytest.raises(J.JHSignalError):
            J.envelope({"d": 4}, origin_engine="y", parent=e2, max_depth=2)

    def test_bus_refuses_loop_and_counts(self):
        from jh_state_store import Bus
        sent = []
        bus = Bus(enabled=True, origin_engine="t", max_depth=1, publisher=lambda n, d, source_engine=None: sent.append((n, d)) or True)
        e0 = bus.publish("jhsignal.batch_published", {"x": 1})
        assert e0 and bus.sent == 1
        parent = sent[-1][1]
        child = J.envelope({}, origin_engine="t", parent=parent, max_depth=1)
        assert bus.publish("jhsignal.fusion_changed", {"y": 2}, parent=child) is False and bus.suppressed == 1
        off = Bus(enabled=False, publisher=lambda *a, **k: True)
        assert off.publish("jhsignal.published", {}) is False and off.suppressed == 1


# ------------------------------------------------------------ adapters ------
class TestAdapters:
    def test_every_registered_engine_has_a_working_adapter(self, signals_from_artifacts, registry):
        sigs, reports = signals_from_artifacts
        assert set(reports) == set(registry.engines)
        for eid, r in reports.items():
            assert r.source_status == "OK", (eid, r.diagnostics)
            assert r.signals, eid
            assert not r.rejected, (eid, r.rejected)
        assert all(J.validate(s) == [] for s in sigs)
        families = {registry.get(s["engine_id"])["engine_family"] for s in sigs}
        assert families == set(J.ENGINE_FAMILIES)

    def test_adapters_never_fabricate(self, signals_from_artifacts):
        sigs, reports = signals_from_artifacts
        ids = {(s["engine_id"], s["entity_id"]) for s in sigs}
        assert ("institutional_13f_flows", "equity:ZERO") not in ids          # wn == 0 -> no signal
        assert ("etf_flows", "etf:SMH") not in ids                          # z-score AND 5d/20d proxy missing -> no signal
        assert ("dark_pool", "equity:MSFT") not in ids                      # NEUTRAL state -> no signal
        assert ("catalyst", "equity:EMPTY") not in ids                      # no catalysts -> no signal
        assert ("dealer_gex", "equity:BAD") not in ids                      # err row -> no signal
        assert ("fortress", "equity:NOPE") not in ids                       # SCREENED tier -> no signal
        assert ("short_interest", "equity:AMZN") not in ids                 # NEUTRAL -> no signal
        assert reports["etf_flows"].skipped >= 1 and reports["short_interest"].skipped >= 1

    def test_directions_and_vetoes_follow_the_documented_mapping(self, signals_from_artifacts):
        sigs, _ = signals_from_artifacts
        by = {(s["engine_id"], s["entity_id"], s["signal_type"]): s for s in sigs}
        ins = by[("insider_radar", "equity:NVDA", "insider_cluster_buy")]
        assert ins["direction"] in ("bullish", "strong_bullish") and abs(ins["score"] - (0.45 + 0.1 + 0.25)) < 1e-6 and ins["magnitude"] == 6_200_000
        assert by[("institutional_13f_flows", "equity:META", "whale_net_flow")]["score"] == -0.7
        assert by[("institutional_13f_flows", "equity:NVDA", "whale_net_flow")]["score"] == 1.0
        qqq_tail = by[("tail_risk", "etf:QQQ", "crash_probability")]
        assert qqq_tail["score"] < 0 and qqq_tail["metadata"]["veto"]["type"] == "SOFT"
        assert by[("tail_risk", "etf:SPY", "crash_probability")]["metadata"]["veto"] is None
        gex = by[("dealer_gex", "equity:TSLA", "gamma_regime")]
        assert gex["score"] < 0 and gex["metadata"]["veto"]["type"] == "SOFT" and gex["horizon"] == "TACTICAL"
        rg = by[("risk_gate", "market:US_EQUITY", "risk_posture")]
        assert rg["metadata"]["veto"] is None and rg["entity_type"] == "market" and abs(rg["confidence"] - 0.75) < 1e-9
        cc = by[("crisis_composite", "market:US_EQUITY", "systemic_stress")]
        assert cc["score"] > 0 and cc["metadata"]["veto"] is None
        assert by[("katlin", "crypto:BTC", "katlin_bottom_setup")]["entity_type"] == "crypto"
        assert ("katlin", "crypto:BTC", "katlin_catalyst") in by and by[("katlin", "crypto:BTC", "katlin_catalyst")]["evidence"][0]["value"].startswith("spot ETF")
        assert ("katlin", "equity:TSM", "katlin_catalyst") not in by
        er = by[("estimate_revisions", "equity:AAPL", "eps_revision")]
        assert er["score"] < 0 and er["metadata"]["binary_event_soon"] is True
        assert by[("fortress", "etf:IWM", "fortress_accumulation")]["entity_type"] == "etf"
        assert by[("momentum_leaders", "equity:META", "momentum_composite")]["direction"] in ("bearish", "slightly_bearish")
        shy = by[("etf_flows", "etf:SHY", "etf_flow")]                       # 60d z-score null -> 5d/20d rotation proxy, sign from ROTATION_OUT
        assert shy["score"] < 0 and shy["metadata"]["confidence_basis"].startswith("5d_vs_20d_proxy") and abs(shy["score"] + 31.5 / 75.0) < 1e-6
        gbc = by[("global_business_cycle", "market:US_EQUITY", "global_cycle_phase")]  # v3 publishes the calibration block, not a float
        assert abs(gbc["score"] - (0.5 - 0.8 * (0.354 - 0.25))) < 1e-6 and abs(gbc["confidence"] - 30 / 34) < 1e-6
        assert by[("fortress", "etf:IWM", "fortress_accumulation")]["metadata"]["asof_basis"] == "engine"
        assert by[("catalyst", "equity:AMZN", "named_catalyst")]["metadata"]["asof_basis"] == "engine"

    def test_hard_veto_paths(self, registry, universe, now):
        from jh_adapters import adapter_for
        crisis = registry.get("crisis_composite"); rg = registry.get("risk_gate"); lce = registry.get("liquidity_credit_engine")
        r = adapter_for(crisis, universe, now=now).parse_existing_output({"generated_at": ts(1), "master_crisis_score": 86.0, "defcon_level": 2, "components": [{"available": True}]}, {})
        assert r.signals[0]["metadata"]["veto"]["type"] == "HARD" and r.signals[0]["score"] <= -0.8
        r = adapter_for(rg, universe, now=now).parse_existing_output({"generated_at": ts(1), "posture": "SEVERE", "composite": -8, "sizing_multiplier": 0.2, "legs": {"a": {"value": 1}}}, {})
        assert r.signals[0]["metadata"]["veto"]["type"] == "HARD"
        r = adapter_for(rg, universe, now=now).parse_existing_output({"generated_at": ts(1), "posture": "RISK_OFF", "composite": -4, "sizing_multiplier": 0.45, "legs": {"a": {"value": 1}}}, {})
        assert r.signals[0]["metadata"]["veto"]["type"] == "SOFT"
        r = adapter_for(lce, universe, now=now).parse_existing_output({"generated_at": ts(1), "composite": {"score": 75, "n_firing": 3}, "regime": "STRESS", "series": {"a": {"available": True}}}, {})
        assert r.signals[0]["metadata"]["veto"]["type"] == "SOFT" and r.signals[0]["score"] < 0

    def test_stale_and_missing_sources_are_reported_not_faked(self, registry, universe, now):
        from jh_adapters import adapter_for
        spec = registry.get("insider_radar")
        old = adapter_for(spec, universe, now=now).parse_existing_output({"generated_at": ts(24 * 3), "clusters": [{"ticker": "NVDA", "n_insiders": 2, "n_buys": 2, "total_value": 1e6}]}, {})
        assert old.source_status == "STALE" and old.signals and J.freshness_state(old.signals[0], now) == "STALE"
        miss = adapter_for(spec, universe, now=now).parse_existing_output(None, {"error": "NoSuchKey"})
        assert miss.source_status == "MISSING" and not miss.signals
        bad = adapter_for(spec, universe, now=now).parse_existing_output({"generated_at": ts(1), "clusters": "not-a-list"}, {})
        assert bad.source_status == "INVALID" and not bad.signals
        nots = adapter_for(spec, universe, now=now).parse_existing_output({"clusters": []}, {"last_modified": None})
        assert nots.source_status == "INVALID" and "timestamp" in nots.diagnostics[0]
        fallback = adapter_for(spec, universe, now=now).parse_existing_output({"clusters": []}, {"last_modified": ts(1)})
        assert fallback.asof_basis == "s3_last_modified"

    def test_extreme_scores_are_clipped_and_flagged(self, registry, universe, now):
        from jh_adapters import adapter_for
        spec = registry.get("momentum_leaders")
        r = adapter_for(spec, universe, now=now).parse_existing_output({"generated_at": ts(1), "all_scored": [{"ticker": "NVDA", "momentum_score": 999}]}, {})
        # percentile 999 is out of [0,100] -> the validator rejects that reading rather than coercing it
        assert not r.signals and r.rejected and any("percentile" in p for p in r.rejected[0]["problems"])
        r = adapter_for(spec, universe, now=now).parse_existing_output({"generated_at": ts(1), "all_scored": [{"ticker": "NVDA", "momentum_score": 100.0}]}, {})
        assert r.signals[0]["score"] == 1.0 and r.signals[0]["direction"] == "strong_bullish"

    def test_adapter_registered_type_guard(self, registry, universe):
        from jh_adapters import adapter_for
        spec = dict(registry.get("insider_radar")); spec["signal_types"] = {"other": {"cluster": "risk", "category": "risk"}}
        with pytest.raises(J.JHSignalError):
            adapter_for(spec, universe)


# ----------------------------------------------------- state / snapshot -----
class TestState:
    def test_snapshot_and_diff(self, snapshot, signals_from_artifacts, registry, now, flags):
        from jh_state_store import build_snapshot, diff_snapshots
        assert snapshot["n_entities"] >= 10 and "market:US_EQUITY" in snapshot["entities"]
        c = snapshot["entities"]["equity:NVDA"][0]
        assert {"family", "freshness", "freshness_weight", "age_days"} <= set(c)
        sigs, _ = signals_from_artifacts
        d0 = diff_snapshots(None, snapshot)
        assert len(d0["new"]) == snapshot["n_signals"] and not d0["revised"] and not d0["expired"]
        # second run: one signal flips, one disappears
        sigs2 = [copy.deepcopy(s) for s in sigs if not (s["engine_id"] == "dark_pool")]
        for s in sigs2:
            if s["engine_id"] == "momentum_leaders" and s["entity_id"] == "equity:NVDA":
                s["score"] = -0.6; s["direction"] = "bearish"; s["direction_numeric"] = -0.6
        snap2 = build_snapshot(sigs2, registry_doc=registry.doc, run_id="r2", now=now, adapter_reports=[], flags=flags)
        d = diff_snapshots(snapshot, snap2)
        assert len(d["expired"]) == 1 and d["expired"][0]["engine_id"] == "dark_pool"
        assert any(r["cur"]["engine_id"] == "momentum_leaders" for r in d["revised"]) and not d["new"]

    def test_expired_signals_leave_the_snapshot(self, registry, now, flags):
        from jh_state_store import build_snapshot
        s = base_signal(now=now, half_life_days=1, freshness_ttl_seconds=3600, data_asof=ts(0))
        snap = build_snapshot([s], registry_doc=registry.doc, run_id="x", now=now + timedelta(hours=5), adapter_reports=[], flags=flags)
        assert snap["n_signals"] == 0 and snap["freshness_counts"]["EXPIRED"] == 1

    def test_ddb_item_shape(self, now):
        from jh_state_store import DynamoState
        s = base_signal(now=now)
        it = DynamoState.item_from_signal(s, family="FLOW", status="ACTIVE")
        assert it["entity_id"] == "equity:NVDA" and it["signal_key"] == J.signal_key(s) and it["status"] == "ACTIVE"
        assert it["ttl_epoch"] > int(J.expires_at(s).timestamp())
        assert json.loads(it["doc"])["signal_id"] == s["signal_id"]
        assert type(it["score"]).__name__ == "Decimal"


# --------------------------------------------------------- config guards ---
class TestConfig:
    def test_bundled_configs_match_canonical(self):
        for name in ("engine-registry.v1.json", "jh-fusion-flags.json", "jh-fusion-universe.json"):
            canon = (REPO / "config" / name).read_bytes()
            for fn in ("justhodl-jhsignal-bridge", "justhodl-jh-fusion"):
                assert (ROOT / "lambdas" / fn / "source" / name).read_bytes() == canon, "%s drifted in %s -- copy config/%s into source/" % (name, fn, name)

    def test_registry_validation_catches_bad_entries(self, registry):
        from jh_registry import validate_registry
        doc = copy.deepcopy(registry.doc)
        doc["engines"][0]["engine_family"] = "VIBES"; doc["engines"][1]["criticality"] = "MAYBE"
        doc["engines"][2]["signal_types"] = {"x": {"cluster": "nope", "category": "macro"}}
        doc["engines"].append(copy.deepcopy(doc["engines"][3]))
        probs = validate_registry(doc)
        assert any("engine_family" in p for p in probs) and any("criticality" in p for p in probs) and any("cluster" in p for p in probs) and any("duplicate" in p for p in probs)

    def test_coordinator_routes_present(self):
        src = (ROOT / "lambdas" / "justhodl-event-coordinator" / "source" / "lambda_function.py").read_text()
        for ev in ("jhsignal.batch_published", "jhsignal.hard_veto", "jhsignal.published", "jhsignal.critical_dependency_failed"):
            assert '"%s"' % ev in src
        assert '"justhodl-jh-fusion"' in src
