"""Brief-domain adapters as fusion legs (Layer 3 briefs registered in the engine registry).

First leg: plumbing_brief (RISK, shadow). These tests pin the contract that the
reconciled structure promised: a LIVE brief becomes a graded fusion leg, a HELD or
malformed brief is a missing vote, and nothing is ever fabricated.
"""
import importlib
import json
import sys

import jhsignal as J
from jh_fixtures import REPO, ts


def _spec(registry):
    rows = [e for e in registry.doc["engines"] if e["engine_id"] == "plumbing_brief"]
    assert len(rows) == 1, "plumbing_brief must appear exactly once in the registry"
    return rows[0]


def _live_brief(score=44.1, label="NORMAL", n_ok=11, n_tot=12, status="LIVE"):
    return {
        "schema": "brief-1.0",
        "mode": "plumbing",
        "status": status,
        "generated_at": ts(1),
        "source": "ops_5424",
        "fields": {
            "composite_score": score,
            "composite_label": label,
            "n_indicators": n_tot,
            "n_with_data": n_ok,
            "as_of": ts(2),
            "layer_scores": {"funding": 40.0, "reserves": 48.0},
        },
    }


def test_registry_row_is_shadow_risk_and_never_critical(registry):
    spec = _spec(registry)
    assert spec["engine_family"] == "RISK" and spec["status"] == "shadow"
    assert spec["criticality"] != "CRITICAL", "a one-shot brief must not be able to block capital"
    assert spec["adapter"] == "plumbing_brief" and "plumbing_stress" in spec["signal_types"]
    assert "plumbing_brief" not in registry.critical_engines(), "shadow brief leg must not be a CRITICAL dependency"
    # the three bundled copies are byte-identical (deploy contract)
    cfg = (REPO / "config" / "engine-registry.v1.json").read_bytes()
    for fn in ("justhodl-jhsignal-bridge", "justhodl-jh-fusion"):
        assert (REPO / "aws" / "lambdas" / fn / "source" / "engine-registry.v1.json").read_bytes() == cfg, fn


def test_brief_adapter_resolves_in_either_import_order(registry, universe, now):
    for mod in ("jh_brief_adapters", "jh_adapters"):
        sys.modules.pop(mod, None)
    importlib.import_module("jh_brief_adapters")           # brief module first: used to be a circular import
    ja = importlib.import_module("jh_adapters")
    assert "plumbing_brief" in ja.all_adapters()
    assert "plumbing_brief" not in ja.ADAPTERS, "brief adapters live in jh_brief_adapters, not the core map"
    a = ja.adapter_for(_spec(registry), universe, now=now)
    assert a.signal_type == "plumbing_stress" and a.category == "risk"


def test_live_brief_becomes_one_market_risk_signal(registry, universe, now):
    from jh_adapters import adapter_for
    res = adapter_for(_spec(registry), universe, now=now).parse_existing_output(_live_brief(), {"last_modified": ts(0)})
    assert res.source_status == "OK", res.diagnostics
    assert len(res.signals) == 1 and res.skipped == 0
    s = res.signals[0]
    assert s["entity_id"] == "market:US_EQUITY" and s["signal_type"] == "plumbing_stress" and s["category"] == "risk"
    assert s["horizon"] == "INTERMEDIATE"
    assert abs(s["score"] - (-(44.1 - 50.0) / 50.0)) < 1e-9          # 44.1 stress -> mildly supportive
    assert abs(s["confidence"] - 11 / 12) < 1e-5   # base rounds to 6 dp
    assert s["metadata"]["confidence_basis"] == "n_with_data/n_indicators"
    assert J.validate(s) == []


def test_stress_sign_convention(registry, universe, now):
    from jh_adapters import adapter_for
    hi = adapter_for(_spec(registry), universe, now=now).parse_existing_output(_live_brief(score=80.0, label="STRESS"), {}).signals[0]
    lo = adapter_for(_spec(registry), universe, now=now).parse_existing_output(_live_brief(score=20.0, label="EASY"), {}).signals[0]
    assert hi["score"] < 0 < lo["score"]
    assert hi["score"] >= -1.0 and lo["score"] <= 1.0


def test_held_or_malformed_brief_is_a_missing_vote_not_a_zero(registry, universe, now):
    from jh_adapters import adapter_for
    spec = _spec(registry)
    for doc in (
        _live_brief(status="HELD"),                                   # contract held the brief
        {**_live_brief(), "mode": "market-tape"},                     # wrong brief
        {**_live_brief(), "schema": "brief-0.9"},                     # wrong contract
        {**_live_brief(), "fields": {"composite_label": "NORMAL"}},   # no composite score
    ):
        res = adapter_for(spec, universe, now=now).parse_existing_output(doc, {"last_modified": ts(0)})
        assert res.signals == [] and res.source_status == "INVALID", (doc.get("status"), doc.get("mode"), res.diagnostics)
    res = adapter_for(spec, universe, now=now).parse_existing_output({}, {})
    assert res.signals == [] and res.source_status == "MISSING"


def test_stale_brief_is_stale_not_fresh(registry, universe, now):
    from jh_adapters import adapter_for
    spec = _spec(registry)
    ttl_h = spec["freshness_ttl_seconds"] / 3600.0
    doc = _live_brief()
    doc["generated_at"] = ts(ttl_h + 2)
    res = adapter_for(spec, universe, now=now).parse_existing_output(doc, {"last_modified": ts(0)})
    assert res.source_status == "STALE"
