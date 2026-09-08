"""justhodl-katlin -- war-room capital-authority tests (audit 2026-09-08 FR-01/FR-02).

Dependency-free: boto3 is stubbed with a fail-on-any-call client so no AWS or
network is touched. Runs in the deploy-lambdas preflight for this engine.
"""
from __future__ import annotations

import importlib.util
import sys
import types
from datetime import datetime, timedelta, timezone
from pathlib import Path

HERE = Path(__file__).resolve().parent
SRC = HERE.parent / "source" / "lambda_function.py"


class _NoAWS:
    def __getattr__(self, name):
        def _boom(*a, **k):
            raise AssertionError("AWS call attempted in unit test: %s" % name)
        return _boom


def _load():
    fake = types.ModuleType("boto3")
    fake.client = lambda *a, **k: _NoAWS()
    fake.resource = lambda *a, **k: _NoAWS()
    sys.modules["boto3"] = fake
    for name in ("botocore", "botocore.exceptions", "botocore.config"):
        m = types.ModuleType(name); sys.modules.setdefault(name, m)
    sys.modules["botocore.exceptions"].ClientError = Exception
    sys.modules["botocore.config"].Config = lambda *a, **k: None
    spec = importlib.util.spec_from_file_location("katlin_under_test", SRC)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _iso(hours_ago):
    return (datetime.now(timezone.utc) - timedelta(hours=hours_ago)).isoformat()


def _gate(posture="RISK_ON", sizing=1.0, hours_ago=2):
    return {"posture": posture, "sizing_multiplier": sizing, "generated_at": _iso(hours_ago)}


def _auth(cap=50, allows=True, mode="SELECTIVE", hours_ago=1, vetoes=None):
    return {"generated_at": _iso(hours_ago), "schema_version": "khalid-risk.v1", "status": "OK",
            "capital_decision": "INVEST SELECTIVELY", "exposure_cap_pct": cap,
            "policy": {"mode": mode, "allows_new_entries": allows, "exposure_cap_pct": cap, "reasons": ["test"]},
            "hard_vetoes": vetoes or []}


def test_authority_cap_binds_and_desk_opinion_is_kept_separately(mod):
    wr = mod.war_room({"risk_gate": _gate(), "khalid_risk": _auth(cap=50)})
    assert wr["local"]["posture"] == "FULL_RISK" and wr["local"]["exposure_cap_pct"] == 100, wr["local"]
    assert wr["exposure_cap_pct"] == 50, wr["exposure_cap_pct"]
    assert wr["posture"] == "SELECTIVE", wr["posture"]
    assert wr["entries_allowed"] is True
    assert wr["authority"]["status"] == "FRESH" and wr["authority"]["source"] == "justhodl-khalid-risk"


def test_authority_blocks_new_entries_regardless_of_desk_rank(mod):
    wr = mod.war_room({"risk_gate": _gate(), "khalid_risk": _auth(cap=50, allows=False, mode="SELECTIVE", vetoes=["credit composite STRESS"])})
    assert wr["entries_allowed"] is False
    assert wr["posture"] == "CASH_OR_TBILLS", wr["posture"]
    assert any(v.startswith("authority:") for v in wr["vetoes"]), wr["vetoes"]
    rows = [{"tier": "KATLIN_PRIME"}, {"tier": "READY"}, {"tier": "BASING"}]
    basket = mod.build_basket([dict(r, learned_excess_126s_pct=9, composite=80) for r in rows], wr)
    assert basket["core"] == [] and basket["cash_pct"] == 100.0


def test_ancient_gate_with_no_authority_is_a_data_hold_not_full_risk(mod):
    # the audit's reproduction: one RISK_ON gate dated 2000-01-01, everything else missing -> was FULL_RISK 100%
    wr = mod.war_room({"risk_gate": {"posture": "RISK_ON", "sizing_multiplier": 1.0, "generated_at": "2000-01-01T00:00:00+00:00"}})
    assert wr["posture"] == "DATA_HOLD", wr["posture"]
    assert wr["exposure_cap_pct"] == 0
    assert wr["entries_allowed"] is False
    assert wr["authority"]["status"] == "MISSING"
    assert wr["hold_reasons"], "hold reason must be explicit"


def test_stale_authority_is_a_data_hold_even_with_a_fresh_gate(mod):
    wr = mod.war_room({"risk_gate": _gate(sizing=1.0), "khalid_risk": _auth(cap=100, hours_ago=30)})
    assert wr["authority"]["status"] == "STALE"
    assert wr["posture"] == "DATA_HOLD" and wr["exposure_cap_pct"] == 0 and wr["entries_allowed"] is False
    assert "STALE" in wr["hold_reasons"][0]


def test_zero_sizing_multiplier_is_a_zero_cap(mod):
    wr = mod.war_room({"risk_gate": _gate(posture="SEVERE", sizing=0.0), "khalid_risk": _auth(cap=50)})
    assert wr["raw_gate"]["cap_pct"] == 0
    assert wr["exposure_cap_pct"] == 0, wr["exposure_cap_pct"]
    assert wr["entries_allowed"] is False


def test_no_legs_is_a_data_hold_not_25_percent(mod):
    wr = mod.war_room({"khalid_risk": _auth(cap=50)})
    assert wr["local"]["posture"] == "UNKNOWN" and wr["local"]["exposure_cap_pct"] == 0
    assert wr["posture"] == "DATA_HOLD" and wr["exposure_cap_pct"] == 0


def test_missing_authority_never_increases_the_cap(mod):
    fresh = mod.war_room({"risk_gate": _gate(sizing=0.75), "khalid_risk": _auth(cap=100)})
    absent = mod.war_room({"risk_gate": _gate(sizing=0.75)})
    assert absent["exposure_cap_pct"] <= fresh["exposure_cap_pct"]


if __name__ == "__main__":
    mod = _load()
    tests = [(k, v) for k, v in sorted(globals().items()) if k.startswith("test_") and callable(v)]
    for name, fn in tests:
        fn(mod)
        print("ok", name)
    print("katlin war-room tests passed: %d" % len(tests))
