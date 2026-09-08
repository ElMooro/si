"""justhodl-risk-sizer -- audit 2026-09-08 FR-03/04/05 tests (dependency-free; fake S3, no AWS).

The whole handler runs against in-memory artifacts; every S3 write is captured.
"""
from __future__ import annotations

import importlib.util
import json
import sys
import types
from datetime import datetime, timedelta, timezone
from pathlib import Path

HERE = Path(__file__).resolve().parent
SRC = HERE.parent / "source" / "lambda_function.py"


class _FakeS3:
    def __init__(self, docs):
        self.docs = dict(docs)
        self.writes = {}

    def get_object(self, Bucket, Key):
        if Key not in self.docs:
            raise Exception("NoSuchKey " + Key)
        body = json.dumps(self.docs[Key]).encode()
        return {"Body": types.SimpleNamespace(read=lambda: body)}

    def put_object(self, Bucket, Key, Body, **kw):
        self.writes[Key] = json.loads(Body)


def _load(docs):
    fake = types.ModuleType("boto3")
    s3 = _FakeS3(docs)
    fake.client = lambda *a, **k: s3
    sys.modules["boto3"] = fake
    spec = importlib.util.spec_from_file_location("risk_sizer_under_test", SRC)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod, s3


def _iso(hours_ago=1):
    return (datetime.now(timezone.utc) - timedelta(hours=hours_ago)).isoformat()


def _auth(cap=100, allows=True, mode="SELECTIVE_RISK_ON", hours_ago=1):
    return {"generated_at": _iso(hours_ago), "status": "OK", "exposure_cap_pct": cap,
            "policy": {"mode": mode, "allows_new_entries": allows, "exposure_cap_pct": cap}, "hard_vetoes": []}


def _base_docs(**over):
    d = {
        # conviction is derived by the engine from dims_passed + composite_score (both 4/4 dims, composite 95 vs 70
        # -> convictions 0.835 / 0.76; quality weights ~1.15 / ~0.85 -- the audit's reproduction shape)
        "opportunities/asymmetric-equity.json": {"top_setups": [
            {"symbol": "AAA", "sector": "Tech", "dims_passed": 4, "composite_score": 95},
            {"symbol": "BBB", "sector": "Energy", "dims_passed": 4, "composite_score": 70}]},
        "investor-debate/_index.json": {"n_tickers": 0, "tickers": {}},
        "regime/current.json": {"regime": "RISK_ON"},
        "portfolio/pnl-history.json": {"snapshots": [{"as_of": "2026-09-01", "khalid_strategy_value_usd": 100.0}, {"as_of": "2026-09-08", "khalid_strategy_value_usd": 101.0}]},
        "portfolio/snapshot.json": {"positions": [], "portfolio_summary": {"total_market_value": 0}},
        "data/report.json": {"stocks": {}},
        "data/khalid-risk.json": _auth(),
        "data/risk-gate.json": {"sizing_multiplier": 1.0, "generated_at": _iso(2)},
    }
    d.update(over)
    return d


def _run(docs):
    mod, s3 = _load(docs)
    mod.lambda_handler({}, None)
    return mod, s3.writes.get("risk/recommendations.json")


def test_zero_nav_is_a_total_loss_not_zero_drawdown():
    mod, _ = _load({})
    dd, peak = mod.compute_drawdown([{"as_of": "2026-01-01", "khalid_strategy_value_usd": 100}, {"as_of": "2026-01-02", "khalid_strategy_value_usd": 0}])
    assert dd == -1.0, dd
    assert mod.drawdown_size_multiplier(dd) == 0.0
    assert mod.compute_drawdown([]) == (None, None)
    assert mod.compute_drawdown([{"as_of": "x", "khalid_strategy_value_usd": 100}]) == (None, None)
    assert mod.drawdown_size_multiplier(None) == 0.0, "unknown drawdown is a hold"
    assert mod.binding_trigger(-0.16) == (-0.15, 0.0), "the binding trigger is the deepest breached rule"
    assert mod.binding_trigger(-0.06) == (-0.05, 0.75)


def test_single_name_cap_binds_after_quality_tilt_and_rounding():
    # the audit's reproduction: quality 95 vs 70 produced 9.21% against a published 8% maximum
    mod, out = _run(_base_docs())
    sizes = {r["symbol"]: r["recommended_size_pct"] for r in out["sized_recommendations"]}
    assert all(v <= 8.0 for v in sizes.values()), sizes
    assert max(sizes.values()) == 8.0, sizes
    assert out["final_constraint_check"]["single_name_ok"] and out["final_constraint_check"]["gross_ok"]
    assert out["status"] == "OK" and out["entries_allowed"] is True
    assert out["constraints_applied"]["max_single_position_pct"] == 8.0


def test_authority_cap_and_entry_prohibition_bind():
    mod, out = _run(_base_docs(**{"data/khalid-risk.json": _auth(cap=10)}))
    assert out["max_gross_exposure_pct"] == 10.0, out["max_gross_exposure_pct"]
    assert sum(r["recommended_size_pct"] for r in out["sized_recommendations"]) <= 10.0 + 0.01
    mod, out = _run(_base_docs(**{"data/khalid-risk.json": _auth(cap=50, allows=False, mode="DEFENSIVE")}))
    assert out["status"] == "ENTRIES_BLOCKED" and out["entries_allowed"] is False
    assert all(r["recommended_size_pct"] == 0.0 for r in out["sized_recommendations"])
    assert any("forbids new entries" in h for h in out["hold_reasons"])


def test_missing_or_stale_authority_is_a_hold_not_a_default():
    mod, out = _run(_base_docs(**{"data/khalid-risk.json": {}}))
    assert out["entries_allowed"] is False and out["authority"]["status"] == "MISSING"
    mod, out = _run(_base_docs(**{"data/khalid-risk.json": _auth(hours_ago=30)}))
    assert out["entries_allowed"] is False and out["authority"]["status"] == "STALE"
    # regime unknown is no longer NEUTRAL/75%
    mod, out = _run(_base_docs(**{"regime/current.json": {}}))
    assert out["entries_allowed"] is False and any("regime" in h for h in out["hold_reasons"])


def test_unknown_drawdown_holds_and_zero_gate_multiplier_is_zero():
    mod, out = _run(_base_docs(**{"portfolio/pnl-history.json": {"snapshots": []}}))
    assert out["drawdown_status"]["status"] == "UNKNOWN" and out["entries_allowed"] is False
    assert all(r["recommended_size_pct"] == 0.0 for r in out["sized_recommendations"])
    mod, out = _run(_base_docs(**{"data/risk-gate.json": {"sizing_multiplier": 0.0, "generated_at": _iso(1)}}))
    assert out["risk_gate"]["sizing_multiplier"] == 0.0
    assert all(r["recommended_size_pct"] == 0.0 for r in out["sized_recommendations"])


def test_existing_book_is_netted_and_counts_against_gross():
    docs = _base_docs(**{"portfolio/snapshot.json": {"positions": [{"symbol": "AAA", "market_value": 6000}, {"symbol": "ZZZ", "market_value": 90000}],
                                                     "portfolio_summary": {"total_market_value": 100000}}})
    mod, out = _run(docs)
    aaa = next(r for r in out["sized_recommendations"] if r["symbol"] == "AAA")
    assert aaa["currently_held_pct"] == 6.0
    assert aaa["recommended_size_pct"] <= 2.0 + 0.01, aaa   # 8% target minus 6% held
    assert out["book"]["gross_pct"] == 96.0 and out["book"]["available_gross_pct"] == 4.0
    assert sum(r["recommended_size_pct"] for r in out["sized_recommendations"]) <= 4.0 + 0.01


def test_empty_pipeline_replaces_the_previous_actionable_book():
    mod, s3 = _load(_base_docs(**{"opportunities/asymmetric-equity.json": {"top_setups": []}}))
    mod.lambda_handler({}, None)
    out = s3.writes.get("risk/recommendations.json")
    assert out and out["status"] == "NO_IDEAS" and out["sized_recommendations"] == []
    assert "data/risk-sizer.json" in s3.writes


if __name__ == "__main__":
    tests = [(k, v) for k, v in sorted(globals().items()) if k.startswith("test_") and callable(v)]
    for name, fn in tests:
        fn()
        print("ok", name)
    print("risk-sizer tests passed: %d" % len(tests))
