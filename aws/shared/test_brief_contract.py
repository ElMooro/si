"""Gates for brief-1.0 / verdict-1.0. Run: python3 -m pytest aws/shared/test_brief_contract.py"""
from brief_contract import (
    FORBIDDEN_HOSTS, freshness, project_verdict, validate_brief, validate_verdict,
)


def test_expired_required_cannot_be_live():
    doc = {
        "schema": "brief-1.0",
        "mode": "plumbing",
        "status": "LIVE",
        "inputs": {"plumbing-stress": {"required": True, "freshness": "EXPIRED"}},
    }
    assert "expired:plumbing-stress" in validate_brief(doc)


def test_held_ok_when_input_expired():
    doc = {
        "schema": "brief-1.0",
        "mode": "plumbing",
        "status": "HELD",
        "inputs": {"plumbing-stress": {"required": True, "freshness": "EXPIRED"}},
    }
    assert validate_brief(doc) == []


def test_freshness_bands():
    from datetime import datetime, timedelta, timezone
    now = datetime(2026, 9, 12, tzinfo=timezone.utc)
    fresh = (now - timedelta(hours=2)).isoformat()
    stale = (now - timedelta(hours=50)).isoformat()
    dead = (now - timedelta(hours=80)).isoformat()
    assert freshness(fresh, 36, now) == "FRESH"
    assert freshness(stale, 36, now) == "STALE"
    assert freshness(dead, 36, now) == "EXPIRED"
    assert freshness(None, 36, now) == "EXPIRED"


def test_verdict_requires_projection_writer():
    bad = {"schema": "verdict-1.0", "writer": "ops_weights", "bias": "mixed", "score": 0, "votes": []}
    assert "writer" in validate_verdict(bad)
    good = project_verdict({"_error": "missing"})
    assert validate_verdict(good) == []
    assert good["bias"] == "mixed"
    assert good["conflicts"]


def test_project_uses_fusion_entity():
    fusion = {
        "generated_at": "2026-09-12T01:56:00Z",
        "run_id": "x",
        "shadow_mode": True,
        "regime": {"score": 0.25, "label": "MILDLY_SUPPORTIVE", "legs": [
            {"engine_id": "regime_composite", "score": 0.56, "freshness": "FRESH", "label": "LATE_CYCLE", "signal_type": "meta_regime"}
        ]},
        "entities": {"market:US_EQUITY": {
            "best_horizon": "SWING",
            "horizons": {"SWING": {
                "fusion_score": 0.561, "direction": "bullish", "conviction": 56,
                "confidence": 0.67, "fusion_coverage": 0.62,
                "missing_families": ["FLOW"], "contradiction_class": "LOW",
                "capital_decision": "OPEN",
            }}
        }},
    }
    v = project_verdict(fusion)
    assert v["bias"] == "risk-on"
    assert v["score"] == 0.561
    assert v["writer"] == "jh-fusion-projection"
    assert any(c.get("note") == "FLOW" for c in v["conflicts"])
    assert validate_verdict(v) == []


def test_forbidden_hosts_listed():
    assert "api.massive.com" in FORBIDDEN_HOSTS
    assert "fred.stlouisfed.org" in FORBIDDEN_HOSTS
