import copy
import gzip
import json
import sys
from datetime import timedelta
from pathlib import Path

import pytest

from jh_fixtures import FakeS3, NOW, ts  # noqa: E402  (shared fixtures + fake S3, wired by conftest)

import jhsignal as J  # noqa: E402
import jh_fusion_core as F  # noqa: E402


def compact(sig, family, now=NOW, inherited=False):
    c = J.compact(sig); c["family"] = family; c["freshness"] = J.freshness_state(sig, now)
    c["age_days"] = J.age_days(sig, now); c["freshness_weight"] = J.freshness_weight(c["age_days"], sig["half_life_days"])
    if inherited:
        c["inherited"] = True
    return c


def mk(engine_id, signal_type, family, score, conf=0.8, entity=("equity", "NVDA"), horizon="SWING", asof=None, veto=None, hl=21):
    s = J.make_signal(engine_id=engine_id, engine_version="1", entity_type=entity[0], symbol=entity[1], category="macro",
                      signal_type=signal_type, score=score, confidence=conf, data_asof=asof or ts(1), horizon=horizon, half_life_days=hl,
                      metadata={"veto": veto} if veto else {}, now=NOW)
    return compact(s, family)


CLUSTERS = {("ea", "t1"): "smart_money", ("eb", "t2"): "smart_money", ("ec", "t3"): "price_confirmation", ("ed", "t4"): "fundamental_growth",
            ("ee", "t5"): "valuation", ("risk_gate", "risk_posture"): "risk", ("regime_composite", "meta_regime"): "macro_support"}
EXPECTED = {"MACRO": 1.0, "FLOW": 1.0, "FUNDAMENTAL": 1.0, "CATALYST": 0.75, "MARKET": 1.0, "RISK": 1.0}
NEUTRAL_REGIME = {"score": 0.0, "label": "MIXED", "certainty": 0.5, "n_legs": 1, "legs": []}


def fuse(sigs, flags, regime=NEUTRAL_REGIME, reliability=None, corr=None, expected=EXPECTED):
    return F.fuse_horizon("equity:NVDA", "SWING", sigs, expected=expected, clusters=CLUSTERS, reliability=reliability or {}, regime=regime, flags=flags, corr=corr)


class TestFusionMath:
    def test_effective_strength_composition_is_published_per_signal(self, flags):
        r = fuse([mk("ea", "t1", "FLOW", 0.8, 0.5)], flags, reliability={"ea": 0.9})
        row = r["signals"][0]
        exp_w = 0.5 * row["freshness_weight"] * 0.9 * 1.0 * row["evidence_quality"]
        assert abs(row["weight"] - exp_w) < 1e-4 and abs(row["effective"] - 0.8 * exp_w) < 1e-4
        assert r["fusion_score"] == pytest.approx(0.8, abs=1e-3) and r["conviction"] == 80 and r["direction"] == "strong_bullish"

    def test_bullish_and_bearish_evidence_are_never_averaged_away(self, flags):
        r = fuse([mk("ea", "t1", "FLOW", 0.9), mk("ee", "t5", "FUNDAMENTAL", -0.9)], flags)
        assert r["bullish_evidence"] > 0 and r["bearish_evidence"] > 0
        assert abs(r["fusion_score"]) < 0.05 and r["contradiction_class"] in ("HIGH", "EXTREME")
        assert r["top_supporting_evidence"] and r["top_opposing_evidence"]
        assert r["family_scores"]["FLOW"]["score"] > 0 > r["family_scores"]["FUNDAMENTAL"]["score"]

    def test_same_cluster_confirmations_do_not_double_count(self, flags):
        one = fuse([mk("ea", "t1", "FLOW", 0.8)], flags)
        two_same = fuse([mk("ea", "t1", "FLOW", 0.8), mk("eb", "t2", "FLOW", 0.8)], flags)
        two_indep = fuse([mk("ea", "t1", "FLOW", 0.8), mk("ec", "t3", "MARKET", 0.8)], flags)
        assert two_same["independent_evidence_count"] == 1 and two_indep["independent_evidence_count"] == 2
        assert two_same["raw_signal_count"] == 2
        w = {r["engine_id"]: r["independence_weight"] for r in two_same["signals"]}
        assert max(w.values()) == 1.0 and min(w.values()) == pytest.approx(1 / 1.6, abs=1e-6)
        assert two_indep["confidence"] > two_same["confidence"] > one["confidence"] - 1e-9

    def test_correlation_matrix_discounts_redundant_engines(self, flags):
        sigs = [mk("ea", "t1", "FLOW", 0.8, 0.9), mk("ec", "t3", "MARKET", 0.7, 0.8)]
        base = fuse(sigs, flags)
        corr = fuse(sigs, flags, corr={"ec": {"ea": 0.8}, "ea": {"ec": 0.8}})
        wc = {r["engine_id"]: r["independence_weight"] for r in corr["signals"]}
        assert wc["ec"] == pytest.approx(0.2, abs=1e-6) and wc["ea"] == 1.0
        assert corr["evidence_mass"] < base["evidence_mass"]
        off = dict(flags, FUSION_CORRELATION_ADJUST_ENABLED=False)
        assert all(r["independence_weight"] == 1.0 for r in fuse(sigs, off, corr={"ec": {"ea": 0.9}})["signals"])

    def test_stale_signal_loses_influence_and_coverage_counts_half(self, flags):
        fresh = fuse([mk("ea", "t1", "FLOW", 0.8)], flags)
        stale = fuse([mk("ea", "t1", "FLOW", 0.8, asof=ts(24 * 60), hl=21)], flags)   # 60 days old, 21d half-life
        assert stale["signals"][0]["freshness"] in ("STALE", "EXPIRED") or stale["signals"][0]["freshness_weight"] < 0.2
        assert stale["evidence_mass"] < fresh["evidence_mass"] * 0.3
        assert stale["fusion_coverage"] == pytest.approx(0.5 * EXPECTED["FLOW"] / sum(EXPECTED.values()), abs=1e-3)
        assert fresh["fusion_coverage"] == pytest.approx(EXPECTED["FLOW"] / sum(EXPECTED.values()), abs=1e-3)

    def test_reliability_weighting_and_flag(self, flags):
        on = fuse([mk("ea", "t1", "FLOW", 0.8)], flags, reliability={"ea": 0.5})
        off = fuse([mk("ea", "t1", "FLOW", 0.8)], dict(flags, FUSION_RELIABILITY_WEIGHTING_ENABLED=False), reliability={"ea": 0.5})
        assert on["signals"][0]["reliability_weight"] == 0.5 and off["signals"][0]["reliability_weight"] == 1.0
        assert on["evidence_mass"] < off["evidence_mass"]

    def test_regime_fit_discounts_against_the_tape_only_for_sensitive_families(self, flags):
        hostile = {"score": -0.8, "label": "HOSTILE", "certainty": 0.9, "n_legs": 2, "legs": []}
        r = fuse([mk("ea", "t1", "FLOW", 0.8), mk("risk_gate", "risk_posture", "RISK", -0.5)], flags, regime=hostile)
        fits = {x["engine_id"]: x["regime_fit"] for x in r["signals"]}
        assert fits["ea"] == pytest.approx(1 - 0.4 * 0.8, abs=1e-6) and fits["risk_gate"] == 1.0
        bear = fuse([mk("ea", "t1", "FLOW", -0.8)], flags, regime=hostile)
        assert bear["signals"][0]["regime_fit"] == 1.0
        off = fuse([mk("ea", "t1", "FLOW", 0.8)], dict(flags, FUSION_REGIME_WEIGHTING_ENABLED=False), regime=hostile)
        assert off["signals"][0]["regime_fit"] == 1.0

    def test_confidence_is_separate_from_conviction(self, flags):
        lone = fuse([mk("ea", "t1", "FLOW", 0.95, 0.9)], flags)
        broad = fuse([mk("ea", "t1", "FLOW", 0.6, 0.8), mk("ec", "t3", "MARKET", 0.6, 0.8), mk("ed", "t4", "FUNDAMENTAL", 0.6, 0.8)], flags)
        assert lone["conviction"] > broad["conviction"] and lone["confidence"] < broad["confidence"]
        assert set(lone["confidence_components"]) >= {"independence", "coverage", "reliability", "freshness", "completeness", "regime_certainty", "contradiction_penalty"}
        assert fuse([], flags)["confidence"] == 0.0 and fuse([], flags)["conviction"] == 0

    def test_extreme_score_cannot_exceed_bounds(self, flags):
        r = fuse([mk("ea", "t1", "FLOW", 1.0, 1.0), mk("ec", "t3", "MARKET", 1.0, 1.0), mk("ed", "t4", "FUNDAMENTAL", 1.0, 1.0)], flags)
        assert r["fusion_score"] <= 1.0 and r["conviction"] <= 100 and 0.0 <= r["confidence"] <= 1.0


class TestVetoes:
    def test_hard_veto_blocks_capital_soft_reduces(self, flags):
        hard = fuse([mk("ea", "t1", "FLOW", 0.9), mk("risk_gate", "risk_posture", "RISK", -0.9, veto={"type": "HARD", "severity": 1.0, "reason": "SEVERE"}, entity=("market", "US_EQUITY"))], flags)
        assert hard["capital_decision"] == "BLOCKED" and hard["hard_vetoes"][0]["reason"] == "SEVERE" and hard["hard_vetoes"][0]["engine_id"] == "risk_gate"
        soft = fuse([mk("ea", "t1", "FLOW", 0.9), mk("risk_gate", "risk_posture", "RISK", -0.4, veto={"type": "SOFT", "severity": 0.6, "reason": "RISK_OFF"})], flags)
        assert soft["capital_decision"] == "REDUCED" and soft["size_modifier"] == pytest.approx(0.7, abs=1e-6)
        assert fuse([mk("ea", "t1", "FLOW", 0.9)], flags)["capital_decision"] == "OPEN"
        off = fuse([mk("risk_gate", "risk_posture", "RISK", -0.9, veto={"type": "HARD", "severity": 1.0, "reason": "x"})], dict(flags, FUSION_VETO_ENABLED=False))
        assert off["capital_decision"] == "OPEN"

    def test_hard_veto_clears_when_the_signal_clears(self, flags):
        v = {"type": "HARD", "severity": 1.0, "reason": "crisis 86"}
        assert fuse([mk("risk_gate", "risk_posture", "RISK", -0.9, veto=v)], flags)["capital_decision"] == "BLOCKED"
        assert fuse([mk("risk_gate", "risk_posture", "RISK", 0.2)], flags)["capital_decision"] == "OPEN"


class TestRunLevel:
    def test_critical_dependency_missing_blocks_every_entity(self, snapshot, registry, universe_doc, flags):
        snap = copy.deepcopy(snapshot)
        snap["entities"]["market:US_EQUITY"] = [s for s in snap["entities"]["market:US_EQUITY"] if s["engine_id"] != "risk_gate"]
        res = F.run_fusion(snap, registry_doc=registry.doc, universe_doc=universe_doc, flags=flags, now=NOW)
        assert res["critical_dependencies"]["capital_blocked"] is True
        assert any("risk_gate" in f["reason"] for f in res["critical_dependencies"]["failures"])
        for r in res["entities"].values():
            for h in r["horizons"].values():
                assert h["capital_decision"] == "BLOCKED" and any(v["reason"].startswith("CAPITAL_DECISION_BLOCKED") for v in h["hard_vetoes"])

    def test_noncritical_family_missing_only_lowers_coverage(self, snapshot, registry, universe_doc, flags):
        snap = copy.deepcopy(snapshot)
        for eid, lst in snap["entities"].items():
            snap["entities"][eid] = [s for s in lst if s["family"] != "FLOW"]
        res = F.run_fusion(snap, registry_doc=registry.doc, universe_doc=universe_doc, flags=flags, now=NOW)
        assert res["critical_dependencies"]["capital_blocked"] is False
        nv = res["entities"]["equity:NVDA"]["horizons"]
        assert all(h["capital_decision"] != "BLOCKED" for h in nv.values())
        assert any("FLOW" in h["missing_families"] for h in nv.values())

    def test_pilot_universe_end_to_end(self, snapshot, registry, universe_doc, flags):
        res = F.run_fusion(snapshot, registry_doc=registry.doc, universe_doc=universe_doc, flags=flags, now=NOW, run_id="r1")
        assert res["shadow_mode"] is True and res["regime"]["label"] != "UNKNOWN" and res["regime"]["n_legs"] >= 3
        assert set(res["entities"]) == {e["entity_id"] for e in universe_doc["entities"]}
        nv = res["entities"]["equity:NVDA"]
        assert nv["own_signal_count"] >= 3 and nv["inherited_signal_count"] >= 3 and nv["best_horizon"] in J.HORIZONS
        h = nv["horizons"][nv["best_horizon"]]
        for k in ("fusion_score", "conviction", "direction", "confidence", "fusion_coverage", "independent_evidence_count", "raw_signal_count",
                  "contradiction_score", "regime_fit", "family_scores", "top_supporting_evidence", "top_opposing_evidence", "soft_vetoes", "hard_vetoes", "what_changed", "attribution"):
            assert k in h, k
        assert h["fusion_score"] > 0 and "FLOW" in h["family_scores"] and "MACRO" in h["family_scores"]
        # market subject never inherits itself
        assert res["entities"]["market:US_EQUITY"]["inherited_signal_count"] == 0
        # crypto expects no FUNDAMENTAL family -> coverage not penalised for it
        btc = res["entities"]["crypto:BTC"]["horizons"]
        assert all("FUNDAMENTAL" not in h["missing_families"] for h in btc.values())
        assert json.dumps(res)  # serialisable ledger

    def test_what_changed_attribution_and_velocity(self, snapshot, registry, universe_doc, flags):
        r1 = F.run_fusion(snapshot, registry_doc=registry.doc, universe_doc=universe_doc, flags=flags, now=NOW, run_id="r1")
        snap2 = copy.deepcopy(snapshot)
        for s in snap2["entities"]["equity:NVDA"]:
            if s["engine_id"] == "momentum_leaders":
                s["score"] = -0.6; s["direction"] = "bearish"
        later = NOW + timedelta(days=1, minutes=1)
        r2 = F.run_fusion(snap2, registry_doc=registry.doc, universe_doc=universe_doc, flags=flags, prior=r1, now=later, run_id="r2")
        h1 = r1["entities"]["equity:NVDA"]["horizons"]["SWING"]; h2 = r2["entities"]["equity:NVDA"]["horizons"]["SWING"]
        wc = h2["what_changed"]
        assert wc["prev_fusion_score"] == h1["fusion_score"] and wc["delta_fusion_score"] < 0
        assert wc["contributions"][0]["contributor"] == "momentum_leaders#momentum_composite" and wc["contributions"][0]["delta"] < 0
        v = r2["entities"]["equity:NVDA"]["velocity"]
        assert v["n_points"] == 2 and v["delta_1d"] is not None and v["classification"] in ("CONVICTION_DECAY", "RAPID_CONVICTION_DECAY", "STABLE")

    def test_replay_is_deterministic_and_point_in_time(self, snapshot, registry, universe_doc, flags):
        a = F.run_fusion(snapshot, registry_doc=registry.doc, universe_doc=universe_doc, flags=flags, now=NOW, run_id="same")
        b = F.run_fusion(snapshot, registry_doc=registry.doc, universe_doc=universe_doc, flags=flags, now=NOW, run_id="same")
        strip = lambda d: json.dumps({k: v for k, v in d.items() if k not in ("generated_at",)}, sort_keys=True)
        assert strip(a) == strip(b)
        # replaying the same signals 20 days later: decay only, no look-ahead (freshness weights fall, nothing else changes)
        later_snap = copy.deepcopy(snapshot)
        for lst in later_snap["entities"].values():
            for s in lst:
                s["age_days"] = s["age_days"] + 20; s["freshness_weight"] = J.freshness_weight(s["age_days"], s["half_life_days"])
        c = F.run_fusion(later_snap, registry_doc=registry.doc, universe_doc=universe_doc, flags=flags, now=NOW + timedelta(days=20), run_id="later")
        assert c["entities"]["equity:NVDA"]["horizons"]["SWING"]["evidence_mass"] < a["entities"]["equity:NVDA"]["horizons"]["SWING"]["evidence_mass"]

    def test_disabled_entities_and_emergent_flag(self, snapshot, registry, universe_doc, flags):
        res = F.run_fusion(snapshot, registry_doc=registry.doc, universe_doc=universe_doc, flags=dict(flags, FUSION_DISABLED_ENTITIES=["equity:NVDA"]), now=NOW)
        assert "equity:NVDA" not in res["entities"]
        res2 = F.run_fusion(snapshot, registry_doc=registry.doc, universe_doc=universe_doc, flags=dict(flags, FUSION_EMERGENT_ENTITIES_ENABLED=True), now=NOW)
        assert len(res2["entities"]) >= len(res["entities"])


class TestHandlersEndToEnd:
    """bridge handler -> S3 read model -> fusion handler, all against a fake S3, DDB + bus + metrics off."""

    def _patch(self, monkeypatch, fake):
        import jh_state_store as ST
        monkeypatch.setattr(ST.S3Store, "s3", property(lambda self: fake))
        monkeypatch.setattr(ST.Metrics, "flush", lambda self: 0)
        import jh_registry as R
        monkeypatch.setattr(R, "_flag_cache", None)
        monkeypatch.setattr(R, "load_flags", lambda **kw: dict(R.DEFAULT_FLAGS, FUSION_STATE_DDB_ENABLED=False, FUSION_SIGNAL_BUS_ENABLED=False))

    def test_bridge_then_fusion(self, artifacts, monkeypatch):
        fake = FakeS3(artifacts, now=NOW)
        self._patch(monkeypatch, fake)
        import importlib
        bridge = importlib.import_module("lambda_function") if False else None
        sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "justhodl-jhsignal-bridge" / "source"))
        import lambda_function as bridge_mod  # bridge source dir is first on the path
        importlib.reload(bridge_mod)
        monkeypatch.setattr(bridge_mod, "load_flags", lambda **kw: dict(__import__("jh_registry").DEFAULT_FLAGS, FUSION_STATE_DDB_ENABLED=False, FUSION_SIGNAL_BUS_ENABLED=False))
        summary = bridge_mod.lambda_handler({"mode": "run"}, None)
        assert summary["n_signals"] >= 20 and summary["state_store"]["enabled"] is False and summary["bus"]["suppressed"] >= 1
        assert "data/jhsignal/state/latest.json" in fake.objects and summary["archive_key"] in fake.objects
        arch = gzip.decompress(fake.objects[summary["archive_key"]]["Body"]).decode().strip().split("\n")
        assert len(arch) == summary["n_signals"] and all(J.validate(json.loads(l)) == [] for l in arch)
        validate = bridge_mod.lambda_handler({"mode": "validate_only"}, None)
        assert validate["mode"] == "validate_only" and "state_store" not in validate

        fus_src = str(Path(__file__).resolve().parents[1] / "source")
        spec = importlib.util.spec_from_file_location("jh_fusion_lambda", Path(fus_src) / "lambda_function.py")
        fus = importlib.util.module_from_spec(spec); spec.loader.exec_module(fus)
        monkeypatch.setattr(fus, "load_flags", lambda **kw: dict(__import__("jh_registry").DEFAULT_FLAGS, FUSION_SIGNAL_BUS_ENABLED=False))
        out = fus.lambda_handler({"trigger_event": "jhsignal.batch_published", "trigger_detail": {"run_id": summary["run_id"], "event_id": "e1"}, "triggered_by": "justhodl-event-coordinator"}, None)
        assert out["ok"] and out["stats"]["n_entities"] == 14 and out["shadow_mode"] is True
        doc = json.loads(fake.objects["data/jh-fusion.json"]["Body"])
        assert doc["snapshot_run_id"] == summary["run_id"] and doc["entities"]["equity:NVDA"]["horizons"]
        ledger = json.loads(gzip.decompress(fake.objects[out["ledger_key"]]["Body"]))
        assert ledger["fusion_result_id"] == out["run_id"] and ledger["result"]["entities"]["equity:NVDA"]["horizons"]["SWING"]["signals"]
        # duplicate trigger for the same snapshot is skipped (idempotent)
        again = fus.lambda_handler({"trigger_event": "jhsignal.batch_published", "trigger_detail": {"run_id": summary["run_id"]}}, None)
        assert again.get("skipped") is True
        forced = fus.lambda_handler({"mode": "scheduled"}, None)
        assert forced["ok"] and not forced.get("skipped")
