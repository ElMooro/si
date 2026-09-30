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
from unittest.mock import Mock

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "shared"))
from macro_observations import calendar_yoy, finite, valid_yoy_row
from quote_meta import classify_badge, time_fetch
SOURCE = Path(__file__).resolve().parents[1] / "source/lambda_function.py"
NOW = datetime(2026, 9, 18, 16, tzinfo=timezone.utc)


def load():
    env = dict(json=json, date=date, datetime=datetime, timezone=timezone, finite=finite,
               classify_badge=classify_badge, time_fetch=time_fetch,
               valid_yoy_row=valid_yoy_row, urllib=types.SimpleNamespace(parse=urllib.parse),
               FMP_KEY="fixture", FRED_KEY="fixture", BUCKET="fixture", KEY="data/market-tape.json")
    tree = ast.parse(SOURCE.read_text(encoding="utf8"))
    exec(compile(ast.Module(body=[n for n in tree.body if isinstance(n, ast.FunctionDef)], type_ignores=[]), str(SOURCE), "exec"), env)
    return env


def quote(symbol):
    return {"symbol": symbol, "price": 100, "timestamp": NOW.timestamp(), "changePercentage": 1.5}


def test_real_timing_and_badge_helpers_execute_on_stubbed_sources():
    env = load()
    assert env["time_fetch"] is time_fetch and env["classify_badge"] is classify_badge
    timed, badge = Mock(wraps=env["time_fetch"]), Mock(wraps=env["classify_badge"])
    env.update(time_fetch=timed, classify_badge=badge)
    env["source_json"] = Mock(side_effect=[
        ([quote("^IXIC")], {"first_received_at": NOW.isoformat()}),
        ({"observations": [{"date": "2026-09-17", "value": "100"}]},
         {"first_received_at": NOW.isoformat()}),
    ])
    quoted = env["fmp_quote"]("^IXIC", NOW)
    observed = env["fred_latest"]("DTWEXBGS", NOW)
    assert timed.call_count == 2 and env["source_json"].call_count == 2
    badge.assert_called_once_with(NOW.isoformat(), "fmp", now=NOW)
    assert quoted["value"] == observed["value"] == 100
    assert quoted["latency_ms"] >= 0 and observed["latency_ms"] >= 0


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


def run_tape(bus=None, bus_time=None, now=NOW, quote_age=0):
    env = load()
    def source(url, provider):
        query = urllib.parse.parse_qs(urllib.parse.urlsplit(url).query)
        data = [{**quote(query["symbol"][0]), "timestamp": now.timestamp() - quote_age}] if provider == "fmp" else {"observations": [{"date": "2026-09-17", "value": "100"}]}
        return data, {"first_received_at": NOW.isoformat()}
    env["source_json"] = source
    class S3:
        def get_object(self, **kwargs):
            return {"Body": io.BytesIO(json.dumps({"generated_at": bus_time or NOW.isoformat(), "indicators": bus or {}}).encode())}
    env["_s3"] = S3()
    return env["build_tape"](now)


def test_quote_status_badge_clocks_and_crypto_weekend_boundaries():
    env = load()
    sunday = datetime(2026, 9, 20, 16, tzinfo=timezone.utc)
    for now in (NOW, sunday):
        for symbol in ("^GSPC", "^IXIC", "BTCUSD", "GCUSD"):
            for age in (-300, 0, 900, 901, 43200, 86401, 7 * 86400):
                stamp = now.timestamp() - age
                env["source_json"] = lambda *a: ([{**quote(symbol), "timestamp": stamp}], {"first_received_at": now.isoformat()})
                result = env["fmp_quote"](symbol, now)
                assert result["quality"]["status"] == ("fresh" if age <= 900 else "delayed")
                expected = ("DELAYED" if age <= 900 else "STALE") if symbol not in ("^GSPC", "^IXIC") else classify_badge(
                    datetime.fromtimestamp(stamp, timezone.utc).isoformat(), "fmp", now=now)
                assert result["badge"] == expected and result["quality"]["age_seconds"] == max(0, age)
                assert result["sizing_eligible"] is False
            for stamp in (None, float("nan"), now.timestamp() + 301, now.timestamp() - 7 * 86400 - 1):
                env["source_json"] = lambda *a: ([{**quote(symbol), "timestamp": stamp}], {"first_received_at": now.isoformat()})
                try: env["fmp_quote"](symbol, now)
                except ValueError: pass
                else: raise AssertionError("invalid quote clock accepted")
    weekend = run_tape(now=sunday, quote_age=43200)
    by_label = {r["label"]: r for r in weekend["items"]}
    assert by_label["BTC"]["badge"] == "STALE"
    assert by_label["SPX"]["badge"] == by_label["COMP"]["badge"] == "SESSION"


def test_only_verified_equity_indices_use_equity_session_badges():
    env = load()
    after_close = datetime(2026, 9, 18, 20, 30, tzinfo=timezone.utc)
    sunday = datetime(2026, 9, 20, 16, tzinfo=timezone.utc)
    for now in (after_close, sunday):
        for symbol in ("GCUSD", "BTCUSD", "UNKNOWN", "^GSPC", "^IXIC"):
            for age, conservative_badge in ((0, "DELAYED"), (900, "DELAYED"), (901, "STALE"), (3600, "STALE")):
                env["source_json"] = lambda *a: ([{**quote(symbol), "timestamp": now.timestamp() - age}], {"first_received_at": now.isoformat()})
                result = env["fmp_quote"](symbol, now)
                if symbol in ("GCUSD", "BTCUSD", "UNKNOWN"):
                    assert result["badge"] == conservative_badge, (symbol, now, age, result["badge"])
                elif age == 3600:
                    assert result["badge"] == "SESSION"
                assert result["quality"]["status"] == ("fresh" if age <= 900 else "delayed")
                assert result["sizing_eligible"] is False


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
