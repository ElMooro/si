"""justhodl-portfolio-admin / justhodl-portfolio-snapshot -- audit 2026-09-08 INST-07/08 tests (no AWS).

Fake DynamoDB table + fake price feed; the snapshot's position loop is exercised through a
lightweight re-implementation harness that imports the real module and calls its handler
with stubbed I/O.
"""
from __future__ import annotations

import importlib.util
import json
import sys
import types
from decimal import Decimal
from pathlib import Path

HERE = Path(__file__).resolve().parent
LAMBDAS = HERE.parents[1]
sys.path.insert(0, str(HERE.parents[2] / "shared"))


class _Table:
    def __init__(self, items):
        self.items = {(i["pk"], i["sk"]): dict(i) for i in items}
        self.last_condition = None

    def get_item(self, Key):
        it = self.items.get((Key["pk"], Key["sk"]))
        return {"Item": dict(it)} if it else {}

    def update_item(self, Key, UpdateExpression, ExpressionAttributeValues, ExpressionAttributeNames, ReturnValues, ConditionExpression=None):
        self.last_condition = ConditionExpression
        it = self.items.get((Key["pk"], Key["sk"]))
        if ConditionExpression and ("attribute_exists(pk)" in ConditionExpression) and not it:
            raise Exception("ConditionalCheckFailedException")
        if it is None:
            it = {"pk": Key["pk"], "sk": Key["sk"]}
        if ConditionExpression and "#cq = :cq" in ConditionExpression:
            if it.get("qty") != ExpressionAttributeValues[":cq"]:
                raise Exception("ConditionalCheckFailedException")
        for part in UpdateExpression[len("SET "):].split(", "):
            alias, ph = [x.strip() for x in part.split("=")]
            it[ExpressionAttributeNames[alias]] = ExpressionAttributeValues[ph]
        self.items[(Key["pk"], Key["sk"])] = it
        return {"Attributes": dict(it)}

    def query(self, **kw):
        return {"Items": list(self.items.values())}


def _load_admin(items):
    fake = types.ModuleType("boto3")
    tbl = _Table(items)
    res = types.SimpleNamespace(Table=lambda name: tbl)
    fake.resource = lambda *a, **k: res
    fake.client = lambda *a, **k: types.SimpleNamespace(get_parameter=lambda **k: {"Parameter": {"Value": "tok"}})
    sys.modules["boto3"] = fake
    spec = importlib.util.spec_from_file_location("padmin", LAMBDAS / "justhodl-portfolio-admin" / "source" / "lambda_function.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod, tbl


def test_quantity_only_edit_recomputes_the_basis_and_flips_the_side():
    mod, tbl = _load_admin([{"pk": "POSITION", "sk": "AAA", "symbol": "AAA", "qty": Decimal("10"), "cost_basis_per_share": Decimal("100"), "cost_basis_total": Decimal("1000"), "position_type": "LONG"}])
    r = mod.update_position({"symbol": "AAA", "qty": 20})
    assert r["ok"], r
    it = tbl.items[("POSITION", "AAA")]
    assert float(it["cost_basis_total"]) == 2000.0, it            # was left at 1000 -> fabricated $1,000 gain
    assert it["position_type"] == "LONG"
    r = mod.update_position({"symbol": "AAA", "qty": -20})
    it = tbl.items[("POSITION", "AAA")]
    assert it["position_type"] == "SHORT" and float(it["cost_basis_total"]) == -2000.0, it
    r = mod.update_position({"symbol": "AAA", "cost_basis_per_share": 50})
    it = tbl.items[("POSITION", "AAA")]
    assert float(it["cost_basis_total"]) == -1000.0, "cost-only edit recomputes the total too"
    assert "attribute_exists(pk)" in tbl.last_condition and "#cq = :cq" in tbl.last_condition


def test_absent_position_and_non_finite_inputs_are_refused():
    mod, tbl = _load_admin([])
    r = mod.update_position({"symbol": "NOPE", "qty": 5})
    assert not r["ok"] and "does not exist" in r["err"], r
    assert ("POSITION", "NOPE") not in tbl.items, "no accidental upsert"
    mod, tbl = _load_admin([{"pk": "POSITION", "sk": "AAA", "symbol": "AAA", "qty": Decimal("10"), "cost_basis_per_share": Decimal("100"), "cost_basis_total": Decimal("1000"), "position_type": "LONG"}])
    r = mod.update_position({"symbol": "AAA", "qty": "nan"})
    assert not r["ok"] and "finite" in r["err"], r


def _load_snapshot(prices):
    src = (LAMBDAS / "justhodl-portfolio-snapshot" / "source" / "lambda_function.py").read_text()
    # isolate the position-valuation block by executing the module with stubbed boto3 and price fetch
    fake = types.ModuleType("boto3")
    fake.client = lambda *a, **k: types.SimpleNamespace(get_object=lambda **k: (_ for _ in ()).throw(Exception("no s3")), put_object=lambda **k: None)
    fake.resource = lambda *a, **k: types.SimpleNamespace(Table=lambda n: _Table([]))
    sys.modules["boto3"] = fake
    spec = importlib.util.spec_from_file_location("psnap", LAMBDAS / "justhodl-portfolio-snapshot" / "source" / "lambda_function.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    mod.batch_fetch_prices = lambda syms, max_workers=10: {s: prices[s] for s in syms if s in prices}
    return mod


def _snapshot_run(prices, positions):
    mod = _load_snapshot(prices)
    mod.load_s3_json = lambda key, default: default
    mod.sync_auto_watchlist = lambda _: dict(added_S=[], added_A=[], removed_S=[], removed_A=[])
    mod.query_pk = lambda key: positions if key == "POSITION" else []
    written = []
    mod.s3.put_object = lambda **kw: written.append(json.loads(kw["Body"]))
    result = mod.lambda_handler({}, None)
    assert result["statusCode"] == 200 and len(written) == 1
    return written[0]


def test_actual_handler_handles_mixed_priced_unpriced_book():
    from datetime import datetime, timezone
    now_ms = datetime.now(timezone.utc).timestamp() * 1000
    positions = [{"symbol":"AAA", "qty":10, "cost_basis_per_share":100, "cost_basis_total":999, "stop_loss":105},
                 {"symbol":"BBB", "qty":10, "cost_basis_per_share":50, "stop_loss":45},
                 {"symbol":"CCC", "qty":-5, "cost_basis_per_share":20, "position_type":"LONG"}]
    payload = _snapshot_run({"AAA":{"price":110, "as_of_unix_ms":now_ms}}, positions)
    a, b, c = payload["positions"]
    assert a["cost_basis_total"] == 1000 and a["pnl_dollars"] == 100
    assert b["market_value"] is None and b["stop_hit"] is None
    assert c["position_type"] == "SHORT" and c["market_value"] is None
    assert payload["portfolio_summary"]["total_pnl_dollars"] == 100
    assert payload["portfolio_summary"]["stops_not_evaluable"] == ["BBB"]
    assert payload["capital_book"]["status"] == "BLOCKED"
    assert payload["capital_book"]["equity_nav"] is None


def test_actual_handler_rejects_missing_future_stale_and_nonfinite_marks():
    from datetime import datetime, timezone
    now = datetime.now(timezone.utc).timestamp() * 1000
    for price, stamp, status in [(100,None,"INVALID_MARK"), (100,now+10*86400000,"INVALID_MARK"),
                                  (100,now-10*86400000,"STALE_MARK"), (float("inf"),now,"UNPRICED")]:
        payload = _snapshot_run({"AAA":{"price":price,"as_of_unix_ms":stamp}},
                                 [{"symbol":"AAA","qty":10,"cost_basis_per_share":100,"stop_loss":90}])
        row = payload["positions"][0]
        assert row["valuation_status"] == status, row
        assert row["market_value"] is None and row["pnl_dollars"] is None and row["stop_hit"] is None
        assert payload["portfolio_summary"]["total_pnl_dollars"] == 0


def test_validate_only_snapshot_skips_sync_and_all_writes():
    mod=_load_snapshot({})
    def forbidden(*a,**kw): raise AssertionError("write attempted in validate_only")
    mod.load_s3_json=lambda key,default:default
    mod.sync_auto_watchlist=forbidden
    mod.query_pk=lambda key:[{"symbol":"AAA","qty":10,"cost_basis_per_share":100}] if key=="POSITION" else []
    mod.s3.put_object=forbidden
    result=mod.lambda_handler({"mode":"validate_only"},None)
    assert result["ok"] and result["validation_only"] and result["status"]=="BLOCKED" and result["artifact_size_bytes"]>0,result


def test_snapshot_trigger_uses_promoted_live_alias():
    mod,_ = _load_admin([])
    calls=[]
    mod._lam=types.SimpleNamespace(invoke=lambda **kwargs:calls.append(kwargs))
    mod._trigger_snapshot()
    assert len(calls)==1 and calls[0]["Qualifier"]=="live" and calls[0]["InvocationType"]=="Event"


if __name__ == "__main__":
    tests = [(k, v) for k, v in sorted(globals().items()) if k.startswith("test_") and callable(v)]
    for name, fn in tests:
        fn()
        print("ok", name)
    print("portfolio tests passed: %d" % len(tests))
