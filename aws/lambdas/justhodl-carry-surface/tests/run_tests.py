"""Offline publication/measurement contracts; no AWS clients or provider requests."""
import importlib.util
import io
import json
import sys
import types
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

SOURCE = Path(__file__).resolve().parents[1] / "source"
sys.path.insert(0, str(SOURCE))
from carry_contract import public_payload, stale_snapshot

with patch.dict(sys.modules, {
    "boto3": types.SimpleNamespace(client=lambda *a, **kw: None),
    "managed_secret": types.SimpleNamespace(managed_secret=lambda *a, **kw: "TEST_ONLY"),
    "_fred_shim": types.SimpleNamespace(),
}):
    spec = importlib.util.spec_from_file_location("carry_engine_test", SOURCE / "lambda_function.py")
    engine = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(engine)


def test_public_diagnostics_are_redacted_without_dropping_metrics():
    raw = {"by_class": {"fx": [{"carry_pct": 0, "error": "SYNTHETIC_SECRET", "headers": {"token": "SYNTHETIC_SECRET"}}]},
           "errors": [{"message": "SYNTHETIC_SECRET"}], "body": "SYNTHETIC_SECRET"}
    out = public_payload(raw)
    assert out["by_class"]["fx"][0]["carry_pct"] == 0
    assert "SYNTHETIC_SECRET" not in json.dumps(out)
    assert out["public_history_review"] == "20260910.v1"
    assert raw["body"] == "SYNTHETIC_SECRET", "review must not mutate the candidate"


def test_stale_fallback_preserves_dates_and_expires_ranked_calls():
    old = {"generated_at": "2026-01-01T00:00:00Z", "by_class": {"fx": []},
           "quality": {"observation_date": "2025-12-31"}, "cross_asset_top": [{"symbol": "X"}]}
    out = stale_snapshot(old, "2026-01-02T00:00:00Z")
    assert out["generated_at"] == old["generated_at"] and out["quality"]["observation_date"] == "2025-12-31"
    assert out["quality"]["status"] == "stale" and out["cross_asset_top"] == [] and out["call"] is None
    assert stale_snapshot({}, "2026-01-02") is None


def test_commodity_missing_curves_never_enter_the_rankings():
    rows = engine.compute_commodity_carry()
    assert rows and all(r["carry_pct"] is None and r["structural_carry_pct"] is None for r in rows)
    assert all(r["quality"]["missing"] == ["futures_curve"] for r in rows)
    assert engine.cross_asset_rank(rows) == []


def test_funding_zero_is_valid_and_missing_or_stale_rates_stay_missing():
    today = datetime.now(timezone.utc).date().isoformat()
    for rows, expected in [([(today, 0)], "fresh"), ([], "unavailable"), ([("2000-01-01", 4)], "stale"), ([(today, float('nan'))], "invalid")]:
        with patch.object(engine, "fred_series", return_value=rows):
            result = engine.rate_observation("DFF")
            assert result["status"] == expected
            assert result["value"] == (0 if expected == "fresh" else None)
    assert all(r["carry_pct"] is None for r in engine.compute_equity_carry(None))
    assert all(r["carry_pct"] is None for r in engine.compute_fi_carry(None))


def test_buybacks_do_not_inflate_cash_income_and_missing_is_not_zero():
    with patch.object(engine, "EQUITY_UNIVERSE", ["TEST"]), \
         patch.object(engine, "fmp_quote_with_history", return_value={"closes": [100], "as_of": "2026-09-16"}), \
         patch.object(engine, "fmp_dividend_yield_ttm", return_value=2):
        for buyback in [3, None]:
            with patch.object(engine, "fmp_buyback_yield", return_value=buyback):
                row = engine.compute_equity_carry(1)[0]
            assert row["carry_pct"] == 1 and row["cash_income_yield_pct"] == 2
            assert row["shareholder_yield_pct"] == (5 if buyback == 3 else None)
            assert row["buyback_yield_pct"] == buyback
    with patch.object(engine, "http_get_json", return_value=[{"buybackYieldTTM": 0, "netBuybackYieldTTM": 3}]):
        assert engine.fmp_buyback_yield("TEST") == 0
    with patch.object(engine, "http_get_json", return_value=[]):
        assert engine.fmp_buyback_yield("TEST") is None
        assert engine.fmp_dividend_yield_ttm("NVDA") is None


def test_old_methodology_snapshots_are_not_compared_with_cash_income_carry():
    old = {"version": "1.4.0", "all_assets": [{"symbol": "X", "carry_pct": 9}]}
    client = types.SimpleNamespace(get_object=lambda **kw: {"Body": io.BytesIO(json.dumps(old).encode())})
    with patch.object(engine, "s3", client), patch.object(engine, "_list_history_snapshots", return_value=["old"]):
        assert engine.load_prior_snapshot(7) is None
        rows = [{"symbol": "X", "carry_pct": 1}]
        engine.attach_dislocation_zscore(rows)
        assert rows[0]["carry_own_z"] is None and rows[0]["carry_own_n"] == 0


def test_optional_history_failure_does_not_block_public_snapshot():
    writes = []
    def put(**kw):
        if kw["Key"].startswith(engine.HIST_PREFIX):
            raise RuntimeError("history storage unavailable")
        writes.append(kw)
    client = types.SimpleNamespace(put_object=put)
    patches = {
        "s3": client, "rate_observation": lambda *a: {"value": 0, "as_of": "2026-09-16", "status": "fresh"},
        "compute_equity_carry": lambda *a: [], "compute_fx_carry": lambda: [{"symbol": "X", "carry_pct": 1, "asset_class": "fx"}],
        "compute_fi_carry": lambda *a: [], "attach_carry_momentum": lambda *a: None,
        "attach_dislocation_zscore": lambda *a: None, "load_risk_regime": lambda: {},
        "attach_unwind_fragility": lambda *a: {}, "_carry_massive_fx": lambda: {},
        "maybe_send_telegram": lambda *a: (_ for _ in ()).throw(AssertionError("must not send alerts")),
    }
    with patch.multiple(engine, **patches):
        result = engine.lambda_handler({"suppress_alerts": True})
    assert result["statusCode"] == 200 and len(writes) == 1
    published = json.loads(writes[0]["Body"])
    assert published["public_history_review"] == "20260910.v1"
    assert published["quality"]["status"] == "incomplete" and published["financing_rate_pct"] == 0


if __name__ == "__main__":
    tests = [f for n, f in list(globals().items()) if n.startswith("test_") and callable(f)]
    for test in tests:
        test()
    print(f"Carry contracts passed: {len(tests)}")
