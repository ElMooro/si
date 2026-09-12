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
    cfg = (REPO / "config" / "engine-registry.v1.json").read_bytes()
    for fn in ("justhodl-jhsignal-bridge", "justhodl-jh-fusion"):
        assert (REPO / "aws" / "lambdas" / fn / "source" / "engine-registry.v1.json").read_bytes() == cfg, fn


def test_brief_adapter_resolves_in_either_import_order(registry, universe, now):
    for mod in ("jh_brief_adapters", "jh_adapters"):
        sys.modules.pop(mod, None)
    importlib.import_module("jh_brief_adapters")
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
    assert abs(s["score"] - (-(44.1 - 50.0) / 50.0)) < 1e-9
    assert abs(s["confidence"] - 11 / 12) < 1e-5
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
        _live_brief(status="HELD"),
        {**_live_brief(), "mode": "market-tape"},
        {**_live_brief(), "schema": "brief-0.9"},
        {**_live_brief(), "fields": {"composite_label": "NORMAL"}},
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


def _pos_spec(registry):
    rows = [e for e in registry.doc["engines"] if e["engine_id"] == "positioning_brief"]
    assert len(rows) == 1
    return rows[0]


def _pos_brief(acc=3200, dist=1832, parsed=18, total=18, status="LIVE", basis="uncapped"):
    f = {"as_of_quarter": "2026-06-30", "funds_total": total, "funds_parsed": parsed, "stale_funds": ["PERSHING", "GREENLIGHT", "SCION"],
         "accumulating": acc, "distributing": dist, "flat": 32, "n_with_inst_trans": 5064}
    if basis:
        f["breadth_basis"] = basis
    return {"schema": "brief-1.0", "mode": "positioning", "status": status, "generated_at": ts(3), "source": "ops_5433", "fields": f}


def test_positioning_row_is_shadow_flow_noncritical(registry):
    spec = _pos_spec(registry)
    assert spec["engine_family"] == "FLOW" and spec["status"] == "shadow" and spec["criticality"] == "NONCRITICAL"
    assert spec["adapter"] == "positioning_brief" and "positioning_flow" in spec["signal_types"]
    assert "positioning_brief" not in registry.critical_engines()
    cfg = (REPO / "config" / "engine-registry.v1.json").read_bytes()
    for fn in ("justhodl-jhsignal-bridge", "justhodl-jh-fusion"):
        assert (REPO / "aws" / "lambdas" / fn / "source" / "engine-registry.v1.json").read_bytes() == cfg, fn


def test_positioning_live_brief_is_one_market_flow_signal_with_real_breadth(registry, universe, now):
    from jh_adapters import adapter_for
    res = adapter_for(_pos_spec(registry), universe, now=now).parse_existing_output(_pos_brief(), {"last_modified": ts(0)})
    assert res.source_status == "OK" and len(res.signals) == 1 and res.skipped == 0, res.diagnostics
    s = res.signals[0]
    assert s["entity_id"] == "market:US_EQUITY" and s["signal_type"] == "positioning_flow" and s["category"] == "institutional_flow"
    assert abs(s["score"] - (3200 - 1832) / (3200 + 1832)) < 1e-4
    assert abs(s["confidence"] - 15 / 18) < 1e-5
    assert s["metadata"]["confidence_basis"] == "(funds_parsed - stale_funds)/funds_total"
    assert J.validate(s) == []


def test_positioning_capped_lists_never_score(registry, universe, now):
    from jh_adapters import adapter_for
    spec = _pos_spec(registry)
    for acc, dist in ((100, 100), (200, 200), (250, 250)):
        res = adapter_for(spec, universe, now=now).parse_existing_output(_pos_brief(acc=acc, dist=dist, basis=None), {"last_modified": ts(0)})
        assert res.signals == [] and res.skipped == 1 and any("cap" in k for k in res.skip_reasons), (acc, dist, res.skip_reasons)
    res = adapter_for(spec, universe, now=now).parse_existing_output(_pos_brief(acc=100, dist=100, basis="uncapped"), {"last_modified": ts(0)})
    assert len(res.signals) == 1 and res.signals[0]["score"] == 0.0
    res = adapter_for(spec, universe, now=now).parse_existing_output(_pos_brief(acc=0, dist=0), {"last_modified": ts(0)})
    assert res.signals == [] and res.skipped == 1


def test_positioning_held_or_wrong_mode_is_a_missing_vote(registry, universe, now):
    from jh_adapters import adapter_for
    spec = _pos_spec(registry)
    for doc in (_pos_brief(status="HELD"), {**_pos_brief(), "mode": "plumbing"}, {**_pos_brief(), "fields": {"funds_total": 18}}):
        res = adapter_for(spec, universe, now=now).parse_existing_output(doc, {"last_modified": ts(0)})
        assert res.signals == [], (doc.get("status"), doc.get("mode"))


# ── Third leg: market_tape_brief (MARKET, shadow) -- uncapped ETF heavy-flow breadth (ops 5436) ──────────────────────

def _tape_spec(registry):
    rows = [e for e in registry.doc["engines"] if e["engine_id"] == "market_tape_brief"]
    assert len(rows) == 1, "market_tape_brief must appear exactly once in the registry"
    return rows[0]


def _tape_brief(n_in=3, n_out=5, other=52, n_etfs=60, status="LIVE", basis="uncapped", mode="market_tape"):
    f = {"session": "2026-09-10", "n_tickers": 12572, "n_etfs": n_etfs, "other_flow_n": other, "breadth_pct": -0.25}
    if n_in is not None:
        f["heavy_inflow_n"] = n_in
    if n_out is not None:
        f["heavy_outflow_n"] = n_out
    if basis:
        f["breadth_basis"] = basis
    return {"schema": "brief-1.0", "mode": mode, "status": status, "generated_at": ts(1), "source": "ops_5436", "fields": f}


def test_market_tape_row_is_shadow_market_noncritical_and_registries_do_not_drift(registry):
    spec = _tape_spec(registry)
    assert spec["engine_family"] == "MARKET" and spec["status"] == "shadow" and spec["criticality"] == "NONCRITICAL"
    assert spec["adapter"] == "market_tape_brief" and "market_tape_flow" in spec["signal_types"]
    assert spec["signal_types"]["market_tape_flow"]["cluster"] == "price_confirmation"
    assert "market_tape_brief" not in registry.critical_engines()
    cfg = (REPO / "config" / "engine-registry.v1.json").read_bytes()
    frag = json.loads((REPO / "config" / "brief-engine-market-tape.json").read_text())
    assert frag == spec, "registry row must equal config/brief-engine-market-tape.json"
    for fn in ("justhodl-jhsignal-bridge", "justhodl-jh-fusion"):
        assert (REPO / "aws" / "lambdas" / fn / "source" / "engine-registry.v1.json").read_bytes() == cfg, fn


def test_market_tape_live_uncapped_is_one_market_signal_with_honest_confidence(registry, universe, now):
    from jh_adapters import adapter_for
    res = adapter_for(_tape_spec(registry), universe, now=now).parse_existing_output(_tape_brief(), {"last_modified": ts(0)})
    assert res.source_status == "OK" and len(res.signals) == 1 and res.skipped == 0, res.diagnostics
    s = res.signals[0]
    assert s["entity_id"] == "market:US_EQUITY" and s["signal_type"] == "market_tape_flow" and s["category"] == "price_confirmation"
    assert abs(s["score"] - (3 - 5) / (3 + 5)) < 1e-9                       # -0.25
    assert abs(s["confidence"] - (3 + 5) / 60) < 1e-5                       # 0.1333… (rounded by the validator) -- 8 classified ETFs are NOT a full-universe tape
    assert s["confidence"] < 0.2
    assert s["metadata"]["confidence_basis"] == "(heavy_in+heavy_out)/n_etfs"
    assert s["horizon"] == "INTERMEDIATE"
    assert J.validate(s) == []


def test_market_tape_missing_or_capped_basis_and_zero_heavy_are_skips_not_zero_votes(registry, universe, now):
    from jh_adapters import adapter_for
    spec = _tape_spec(registry)
    for doc in (_tape_brief(n_in=0, n_out=0, basis=None),      # no basis + 0/0: the fabricated-neutral case
                _tape_brief(n_in=0, n_out=0, basis="capped"),
                _tape_brief(n_in=3, n_out=5, basis=None),      # real counts but no declared basis: still not a vote
                _tape_brief(n_in=3, n_out=5, basis="capped"),
                _tape_brief(n_in=0, n_out=0, basis="uncapped")):  # uncapped but empty: skip, never 0.0
        res = adapter_for(spec, universe, now=now).parse_existing_output(doc, {"last_modified": ts(0)})
        assert res.signals == [] and res.skipped == 1, (doc["fields"], res.skip_reasons)
        assert not any(sig.get("score") == 0.0 for sig in res.signals)


def test_market_tape_held_wrong_mode_or_missing_counts_is_a_missing_vote(registry, universe, now):
    from jh_adapters import adapter_for
    spec = _tape_spec(registry)
    for doc in (_tape_brief(status="HELD"), _tape_brief(mode="positioning"), _tape_brief(n_in=None), _tape_brief(n_out=None),
                {**_tape_brief(), "fields": {"n_etfs": 60}}):
        res = adapter_for(spec, universe, now=now).parse_existing_output(doc, {"last_modified": ts(0)})
        assert res.signals == [], (doc.get("status"), doc.get("mode"), doc["fields"])
