"""Market tape identity, clock and missing-data contract fixtures; no network."""
import ast
import io
import json
import sys
import time
import types
import urllib.parse
from datetime import date, datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "shared"))
from macro_observations import calendar_yoy, finite, valid_yoy_row
SOURCE = Path(__file__).resolve().parents[1] / "source/lambda_function.py"
NOW = datetime(2026, 9, 18, 16, tzinfo=timezone.utc)


def load():
    env = dict(json=json, date=date, datetime=datetime, timezone=timezone, finite=finite,
               valid_yoy_row=valid_yoy_row, urllib=types.SimpleNamespace(parse=urllib.parse),
               FMP_KEY="fixture", FRED_KEY="fixture", BUCKET="fixture", KEY="data/market-tape.json")
    tree = ast.parse(SOURCE.read_text(encoding="utf8"))
    exec(compile(ast.Module(body=[n for n in tree.body if isinstance(n, ast.FunctionDef)], type_ignores=[]), str(SOURCE), "exec"), env)
    return env


def quote(symbol):
    return {"symbol": symbol, "price": 100, "timestamp": NOW.timestamp(), "changePercentage": 1.5}


def test_mismatched_symbol_undated_or_nonfinite_quote_is_not_displayable():
    env = load()
    for row in (quote("WRONG"), {**quote("^IXIC"), "price": float("nan")},
                {**quote("^IXIC"), "timestamp": None}, {**quote("^IXIC"), "timestamp": NOW.timestamp() + 3600}):
        env["source_json"] = lambda *a: ([row], {"first_received_at": NOW.isoformat()})
        try: env["fmp_quote"]("^IXIC", NOW)
        except ValueError: pass
        else: raise AssertionError("bad quote accepted")


def test_daily_latest_report_keeps_its_observation_date_across_a_missing_day():
    env = load()
    rows = [{"date": "2026-09-17", "value": "."}, {"date": "2026-09-16", "value": "120"}, {"date": "2026-09-15", "value": "100"}]
    env["source_json"] = lambda *a: ({"observations": rows}, {"first_received_at": NOW.isoformat()})
    result = env["fred_latest"]("DTWEXBGS", NOW)
    assert result["observation_date"] == "2026-09-16" and result["comparison_date"] == "2026-09-15"
    assert result["quality"]["latest_returned_period"] == "2026-09-17" and result["chg_pct"] == 20


def test_broad_dollar_weekly_release_does_not_expire_like_daily_rates():
    env = load()
    env["source_json"] = lambda *a: ({"observations": [{"date": "2026-09-11", "value": "120"}]}, {"first_received_at": NOW.isoformat()})
    sunday = datetime(2026, 9, 20, tzinfo=timezone.utc)
    row = env["fred_latest"]("DTWEXBGS", sunday)
    assert row["observation_date"] == "2026-09-11" and row["quality"]["max_observation_age_days"] == 11
    for sid, when in (("DGS10", sunday), ("DTWEXBGS", datetime(2026, 9, 23, tzinfo=timezone.utc))):
        try: env["fred_latest"](sid, when)
        except ValueError: pass
        else: raise AssertionError("stale observation accepted")


def run_tape(bus=None, bus_time=None):
    env = load()
    def source(url, provider):
        query = urllib.parse.parse_qs(urllib.parse.urlsplit(url).query)
        data = [quote(query["symbol"][0])] if provider == "fmp" else {"observations": [{"date": "2026-09-17", "value": "100"}]}
        return data, {"first_received_at": NOW.isoformat()}
    env["source_json"] = source
    class S3:
        def get_object(self, **kwargs):
            return {"Body": io.BytesIO(json.dumps({"generated_at": bus_time or NOW.isoformat(), "indicators": bus or {}}).encode())}
    env["_s3"] = S3()
    return env["build_tape"](NOW)


def test_index_identity_and_legacy_macro_values_cannot_be_mislabeled():
    packet = run_tape({"CNGDPYY": {"v": 21.05}, "USIRYY": {"v": 2.95}})
    by_label = {row["label"]: row for row in packet["items"]}
    assert by_label["COMP"]["provider_symbol"] == "^IXIC" and by_label["COMP"]["legacy_label"] == "NDX"
    assert by_label["USD BROAD"]["series_id"] == "DTWEXBGS" and by_label["USD BROAD"]["legacy_label"] == "DXY"
    assert not {"NDX", "DXY", "CN GDP", "US CPI"}.intersection(by_label)
    assert len(packet["gaps"]) == 3 and packet["schema_version"] == "2.0"


def test_verified_macro_metadata_survives_and_stale_bus_cannot_renew_it():
    row = calendar_yoy([{"date": "2025-08-01", "value": "100"}, {"date": "2026-08-01", "value": "105"}],
                      {"id": "CPIAUCSL", "frequency_short": "M", "units": "Index", "seasonal_adjustment": "SA"}, NOW)
    proof = {"contract": "source-evidence.v1", "captured": True, "sha256": "a" * 64, "key": "fixture"}
    row.update(v=5, evidence={"observations": proof, "definition": proof})
    packet = run_tape({"USIRYY": row})
    cpi = next(i for i in packet["items"] if i["label"] == "US CPI SA YoY")
    assert cpi["value"] == 5 and cpi["observation_date"] == "2026-08-01" and cpi["unit"] == "% YoY"
    assert cpi["comparison_date"] == "2025-08-01" and cpi["evidence"] == row["evidence"]
    assert len(cpi["source_packet_sha256"]) == 64
    stale = run_tape({"USIRYY": row}, "2026-09-01T00:00:00+00:00")
    assert all(i["label"] != "US CPI SA YoY" for i in stale["items"])


if __name__ == "__main__":
    tests = [v for k, v in sorted(globals().items()) if k.startswith("test_")]
    for test in tests: test()
    print("Market tape tests passed:", len(tests))
