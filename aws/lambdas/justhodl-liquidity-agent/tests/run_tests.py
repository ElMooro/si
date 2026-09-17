"""Offline liquidity measurement and publication contracts."""
import importlib.util
import json
import sys
import types
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

SOURCE = Path(__file__).resolve().parents[1] / "source"
sys.path.insert(0, str(SOURCE))
from liquidity_contract import core_quality, unavailable_payload

with patch.dict(sys.modules, {"boto3": types.SimpleNamespace(client=lambda *a, **kw: None),
                             "managed_secret": types.SimpleNamespace(managed_secret=lambda *a, **kw: "TEST_ONLY")}):
    spec = importlib.util.spec_from_file_location("liquidity_test", SOURCE / "lambda_function.py")
    engine = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(engine)

NOW = datetime.now(timezone.utc)
TODAY = NOW.date().isoformat()
VALUES = {"WALCL": 6000, "WTREGEN": 800, "RRPONTSYD": 0}
DATES = {k: TODAY for k in VALUES}


def test_zero_rrp_is_observed_and_a_missing_leg_is_never_zero():
    q = core_quality(VALUES, DATES, NOW.isoformat(), NOW)
    assert q["status"] == "fresh" and q["missing"] == []
    missing = {**VALUES, "WTREGEN": None}
    q = core_quality(missing, DATES, NOW.isoformat(), NOW)
    assert q["status"] == "unavailable" and q["missing"] == ["WTREGEN"]
    out = unavailable_payload(missing, DATES, q, {"spy_signal": {"direction": "BULLISH"}, "legacy": 1})
    assert out["core"]["net_liquidity"]["value_bn"] is None
    assert out["core"]["tga"]["value_bn"] is None and out["core"]["rrp"]["value_bn"] == 0
    assert out["spy_signal"]["direction"] is None and out["signal_logger"]["score"] is None
    assert out["legacy"] == 1


def test_required_stale_future_and_nonfinite_values_cannot_be_fresh():
    for date, expected in [("2000-01-01", "stale"), ((NOW.date()+timedelta(days=1)).isoformat(), "invalid")]:
        assert core_quality(VALUES, {**DATES, "WALCL": date}, NOW.isoformat(), NOW)["status"] == expected
    assert core_quality({**VALUES, "WALCL": float("nan")}, DATES, NOW.isoformat(), NOW)["status"] == "invalid"


def test_official_units_are_series_specific():
    with patch.object(engine, "fetch_fred", return_value=[{"date": TODAY, "value": 23218.0}]):
        assert engine.get_latest("M2SL")[0] == 23218
        assert engine.get_latest("M1SL")[0] == 23218
        assert engine.get_latest("WALCL")[0] == 23.218
        assert engine.get_series_history("WTREGEN")[0]["value"] == 23.218
        assert engine.get_latest("RRPONTSYD")[0] == 23218


def test_stale_cache_is_retried_against_official_fred():
    cached = {"RRPONTSYD": [{"date": "2000-01-01", "value": 5}]}
    fresh = [{"date": TODAY, "value": 0}]
    with patch.object(engine, "_load_fred_cache", return_value=cached), patch.object(engine, "_fred_live", return_value=fresh) as live:
        assert engine.fetch_fred("RRPONTSYD") == fresh and live.call_count == 1


def test_missing_core_publishes_expired_contract_before_optional_work():
    writes = []
    client = types.SimpleNamespace(put_object=lambda **kw: writes.append(kw))
    with patch.object(engine, "s3", client), patch.object(engine, "_sfeed", return_value={}), \
         patch.object(engine, "get_latest", side_effect=lambda sid,*a: (None if sid == "WALCL" else 0,TODAY)), \
         patch.object(engine, "get_series_history", side_effect=AssertionError("optional work must not run")):
        result = engine.lambda_handler({}, None)
    out = json.loads(writes[0]["Body"])
    assert result["statusCode"] == 200 and out["quality"]["status"] == "unavailable"
    assert out["core"]["net_liquidity"]["score"] is None and out["call"] is None


def test_same_date_history_join_and_unvalidated_forecast_expiry():
    writes = []
    def history(sid, **kw):
        base = 6000 if sid == "WALCL" else 100 if sid == "WTREGEN" else 0
        return [{"date": (NOW.date()-timedelta(days=(19-i)*7)).isoformat(), "value": base+i if sid != "RRPONTSYD" else 0} for i in range(20)]
    client = types.SimpleNamespace(put_object=lambda **kw: writes.append(kw))
    with patch.object(engine, "s3", client), patch.object(engine, "get_latest", return_value=(0,TODAY)), \
         patch.object(engine, "get_series_history", side_effect=history), patch.object(engine, "FRED_SERIES", []), \
         patch.object(engine, "build_part4", return_value={}):
        engine.lambda_handler({}, None)
    out = json.loads(writes[0]["Body"])
    assert all(row["value"] == 5900 for row in out["chart_data"]["net_liquidity"]), "join must use latest <= observation, including zero"
    assert out["quality"]["status"] == "fresh"
    assert out["spy_signal"]["direction"] is None and out["spy_signal"]["confidence"] is None
    assert out["spy_signal"]["lead_days"] is None and out["signal_logger"]["direction"] is None


if __name__ == "__main__":
    tests = [f for n,f in list(globals().items()) if n.startswith("test_") and callable(f)]
    for test in tests:
        test()
    print(f"Liquidity integrity tests passed: {len(tests)}")
