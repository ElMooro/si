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
# shared modules (managed_secret etc.) are bundled into the Lambda zip; make them importable here too
sys.path.insert(0, str(HERE.parents[2] / "shared"))


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
    ts = datetime.now(timezone.utc) - timedelta(hours=hours_ago)
    return {"engine": "justhodl-khalid-risk", "schema_version": "1.0.0", "generated_at": ts.isoformat(), "expires_at": (ts+timedelta(hours=24)).isoformat(),
            "status": "OK", "exposure_cap_pct": cap, "policy": {"mode": mode, "allows_new_entries": allows, "exposure_cap_pct": cap}, "hard_vetoes": [], "critical_failures": [],
            "source_health": [{"name":"risk_gate", "critical":True, "status":"FRESH", "as_of":ts.isoformat(), "max_age_h":24.0}]}


def _book(positions=None, nav=100000, cash=None, hours_ago=1, orders=None):
    positions = [dict(p, valuation_status=p.get("valuation_status", "PRICED"), mark_age_h=p.get("mark_age_h", 1.0), sector=p.get("sector", "Tech")) for p in (positions or [])]
    orders = orders or []
    net = sum(p.get("market_value") or 0 for p in positions)
    return {"capital_book": {"schema_version":"1.0", "status":"READY", "allows_new_entries":True, "book_id":"book-test", "account_id":"account-test", "currency":"USD",
            "as_of":_iso(hours_ago), "reconciled_at":_iso(hours_ago), "equity_nav":nav, "cash":nav-net if cash is None else cash, "liabilities":0.0,
            "gross_exposure":sum(abs(p.get("market_value") or 0) for p in positions), "net_exposure":net,
            "reserved_order_exposure":sum(o["remaining_exposure"] for o in orders), "open_orders":orders, "positions":positions, "unpriced_positions":[],
            "nav_history":[{"as_of":_iso(25),"equity_nav":nav,"book_id":"book-test","account_id":"account-test"}, {"as_of":_iso(hours_ago),"equity_nav":nav,"book_id":"book-test","account_id":"account-test"}]}}


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
        "portfolio/snapshot.json": _book(),
        "data/report.json": {"stocks": {}},
        "data/khalid-risk.json": _auth(),
        "data/risk-gate.json": {"engine":"justhodl-risk-gate", "posture":"RISK_ON", "sizing_multiplier": 1.0, "generated_at": _iso(2)},
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
    missing = _book(); missing["capital_book"]["nav_history"] = []
    mod, out = _run(_base_docs(**{"portfolio/snapshot.json": missing}))
    assert out["drawdown_status"]["status"] == "UNKNOWN" and out["entries_allowed"] is False
    assert all(r["recommended_size_pct"] == 0.0 for r in out["sized_recommendations"])
    mod, out = _run(_base_docs(**{"data/risk-gate.json": {"engine":"justhodl-risk-gate", "posture":"SEVERE", "sizing_multiplier": 0.0, "generated_at": _iso(1)}}))
    assert out["risk_gate"]["sizing_multiplier"] == 0.0
    assert all(r["recommended_size_pct"] == 0.0 for r in out["sized_recommendations"])


def test_existing_book_is_netted_and_counts_against_gross():
    docs = _base_docs(**{"portfolio/snapshot.json": _book([{"symbol":"AAA", "market_value":6000, "sector":"Tech"}, {"symbol":"ZZZ", "market_value":90000, "sector":"Other"}])})
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


def test_missing_unpriced_stale_and_short_books_do_not_create_capacity():
    missing = {}
    unpriced = _book([{"symbol":"HELD","market_value":None,"valuation_status":"UNPRICED"}]); unpriced["capital_book"]["unpriced_positions"]=["HELD"]
    stale = _book(hours_ago=1000)
    shorts = _book([{"symbol":"LONG","market_value":100000},{"symbol":"SHORT","market_value":-100000}])
    for book in (missing, unpriced, stale):
        _, out = _run(_base_docs(**{"portfolio/snapshot.json":book}))
        assert out["entries_allowed"] is False and out["book"]["status"] == "BLOCKED", out
        assert sum(r["recommended_size_pct"] for r in out["sized_recommendations"]) == 0
    _, out = _run(_base_docs(**{"portfolio/snapshot.json":shorts}))
    assert out["book"]["gross_pct"] == 200 and out["book"]["available_gross_pct"] == 0
    assert sum(r["recommended_size_pct"] for r in out["sized_recommendations"]) == 0


def test_nav_and_order_reservations_define_capacity_not_position_sum():
    book = _book([{"symbol":"ZZZ","market_value":20000,"sector":"Other"}], orders=[{"symbol":"AAA","remaining_exposure":6000,"sector":"Tech"}])
    _, out = _run(_base_docs(**{"portfolio/snapshot.json":book}))
    assert out["book"]["gross_pct"] == 26 and out["book"]["available_gross_pct"] == 74, out["book"]
    aaa = next(r for r in out["sized_recommendations"] if r["symbol"]=="AAA")
    assert aaa["currently_held_pct"] == 6 and aaa["recommended_size_pct"] <= 2


def test_future_malformed_and_expired_authority_fails_closed():
    bad=[]
    future=_auth(hours_ago=-24*365); bad.append(future)
    for cap in ("NaN", float("nan"), float("inf"), True, -1, 101):
        a=_auth();a["exposure_cap_pct"]=cap;a["policy"]["exposure_cap_pct"]=cap;bad.append(a)
    a=_auth();a["schema_version"]="wrong";bad.append(a)
    a=_auth();a["status"]="INVALID";bad.append(a)
    a=_auth();a["expires_at"]=_iso(1);bad.append(a)
    for a in bad:
        _, out=_run(_base_docs(**{"data/khalid-risk.json":a}))
        assert out["entries_allowed"] is False and all(r["recommended_size_pct"]==0 for r in out["sized_recommendations"]), out


def test_rounding_never_publishes_over_budget_rows():
    ideas=[{"symbol":"S%d"%i,"sector":"Sector%d"%i,"dims_passed":4,"composite_score":95} for i in range(18)]
    _, out=_run(_base_docs(**{"data/khalid-risk.json":_auth(cap=1),"opportunities/asymmetric-equity.json":{"top_setups":ideas}}))
    total=sum(r["recommended_size_pct"] for r in out["sized_recommendations"])
    assert 0 < total <= 1.0, total
    assert out["final_constraint_check"]["gross_ok"] and out["entries_allowed"]


def test_existing_cluster_exposure_and_same_symbol_lots_aggregate():
    book=_book([{"symbol":"ZZZ","market_value":24000,"sector":"Tech"},{"symbol":"AAA","market_value":1000,"sector":"Tech"}])
    _,out=_run(_base_docs(**{"portfolio/snapshot.json":book}))
    aaa=next(r for r in out["sized_recommendations"] if r["symbol"]=="AAA")
    assert aaa["recommended_size_pct"]==0, aaa
    lots=_book([{"symbol":"AAA","market_value":3000},{"symbol":"AAA","market_value":4000}])
    _,out=_run(_base_docs(**{"portfolio/snapshot.json":lots}))
    aaa=next(r for r in out["sized_recommendations"] if r["symbol"]=="AAA")
    assert aaa["currently_held_pct"]==7 and aaa["recommended_size_pct"]<=1


def test_another_book_nav_history_and_unreconciled_orders_hold():
    book=_book();book["capital_book"]["nav_history"][-1]["account_id"]="other-account"
    _,out=_run(_base_docs(**{"portfolio/snapshot.json":book}))
    assert out["entries_allowed"] is False
    book=_book();book["capital_book"]["reserved_order_exposure"]=5000
    _,out=_run(_base_docs(**{"portfolio/snapshot.json":book}))
    assert out["entries_allowed"] is False


if __name__ == "__main__":
    tests = [(k, v) for k, v in sorted(globals().items()) if k.startswith("test_") and callable(v)]
    for name, fn in tests:
        fn()
        print("ok", name)
    print("risk-sizer tests passed: %d" % len(tests))
