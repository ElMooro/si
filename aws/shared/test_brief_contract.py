"""Gates for brief-1.0 / verdict-1.0. Run: python3 -m pytest aws/shared/test_brief_contract.py"""
from datetime import datetime, timedelta, timezone

import pytest
import brief_contract
import brief_compiler

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


# Fixed clock: assertions must not depend on the execution date or wall time.
NOW = datetime(2026, 9, 30, 12, tzinfo=timezone.utc)


@pytest.mark.parametrize("ttl", sorted(set(brief_contract.TTL_HOURS.values())))
@pytest.mark.parametrize("ahead", [timedelta(microseconds=1), timedelta(hours=1), timedelta(days=365)])
def test_future_timestamp_is_expired(ttl, ahead):
    assert freshness((NOW + ahead).isoformat(), ttl, NOW) == "EXPIRED"


@pytest.mark.parametrize("ttl", sorted(set(brief_contract.TTL_HOURS.values())))
@pytest.mark.parametrize("boundary,offset,expected", [
    (0, 0, "FRESH"), (0, 1, "FRESH"),
    (1, -1, "FRESH"), (1, 0, "FRESH"), (1, 1, "STALE"),
    (2, -1, "STALE"), (2, 0, "STALE"), (2, 1, "EXPIRED"),
])
def test_exact_freshness_boundaries(ttl, boundary, offset, expected):
    stamp = NOW - timedelta(hours=ttl * boundary, microseconds=offset)
    assert freshness(stamp, ttl, NOW) == expected


@pytest.mark.parametrize("stamp", [None, "", "not-a-date", "2026-02-30T12:00:00Z", {}, True])
def test_missing_or_malformed_timestamp_is_expired(stamp):
    assert freshness(stamp, 36, NOW) == "EXPIRED"


@pytest.mark.parametrize("stamp", [
    "2026-09-30T12:00:00Z", "2026-09-30T14:00:00+02:00",
    "2026-09-30T07:00:00-05:00", "2026-09-30T12:00:00",
    NOW, NOW.replace(tzinfo=None), NOW.astimezone(timezone(timedelta(hours=2))),
])
def test_equivalent_timezones_and_naive_utc_preserved(stamp):
    assert freshness(stamp, 36, NOW) == "FRESH"


@pytest.mark.parametrize("stamp", [
    "2026-09-30T14:00:00.000001+02:00",
    "2026-09-30T07:00:00.000001-05:00", "2026-09-30T12:00:00.000001",
])
def test_future_timestamp_after_timezone_normalization(stamp):
    assert freshness(stamp, 36, NOW) == "EXPIRED"


@pytest.mark.parametrize("mode,key", [
    ("plumbing", "data/plumbing-stress.json"),
    ("official_stats", "data/fed-nowcast-join.json"),
    ("market_tape", "data/warm/us-equities-daily/latest-summary.json"),
    ("market_tape", "data/etf-flows.json"),
    ("positioning", "data/13f-positions.json"),
    ("event", "data/finviz-signals.json"),
])
@pytest.mark.parametrize("ahead", [timedelta(0), timedelta(hours=1), timedelta(days=365)])
def test_compiler_holds_future_required_inputs(monkeypatch, mode, key, ahead):
    class FixedClock(datetime):
        @classmethod
        def now(cls, tz=None):
            return NOW

    monkeypatch.setattr(brief_contract, "datetime", FixedClock)
    monkeypatch.setattr(brief_compiler, "_now", lambda: NOW)

    def load(s3, input_key):
        stamp = (NOW + ahead if input_key == key else NOW).isoformat()
        return {"generated_at": stamp, "as_of": stamp}, NOW.isoformat(), None

    writes = []
    monkeypatch.setattr(brief_compiler, "_load", load)
    monkeypatch.setattr(brief_compiler, "_put", lambda s3, output_key, doc: writes.append(doc))
    doc = brief_compiler.run(None, mode)
    assert writes == [doc]
    expected = "EXPIRED" if ahead else "FRESH"
    assert doc["inputs"][key]["freshness"] == expected
    assert doc["status"] == ("HELD" if ahead else "LIVE")
    assert validate_brief(doc) == []
    if ahead:
        assert "expired:" + key in validate_brief(dict(doc, status="LIVE"))
