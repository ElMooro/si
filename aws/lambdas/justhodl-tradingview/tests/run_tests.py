"""JPLG source/unit regression fixtures. AST loading prevents cloud imports."""
import ast
from datetime import datetime, timezone, timedelta
import json
import math
from pathlib import Path
import re
import types
import urllib.request
import sys
import io
import gzip
import hashlib

SOURCE = Path(__file__).parents[1] / "source" / "lambda_function.py"
tree = ast.parse(SOURCE.read_text())
sys.path.insert(0, str(SOURCE.parents[3] / "shared"))
from macro_observations import CURATED_YOY, YOY_CONTRACT, calendar_yoy, valid_yoy_row
from evidence_store import capture, read_verified
FUNCTIONS = {"boj_yoy", "resolve_jplg_row", "_family_try", "resolve_curated_yoy_row", "fred_yoy"}
NOW = datetime(2026, 9, 9, tzinfo=timezone.utc)
ALIAS = "boj:MD11:DLCLAADBLTTO|MD11:DLCLBADBLTTO"
CONTRACT = "boj-loan-growth-yoy.v1"


def scope():
    env = {"datetime": datetime, "timezone": timezone, "json": json, "math": math,
           "re": re, "JPLG_CONTRACT": CONTRACT, "ALIASES": {"JPLG": ALIAS},
           "CURATED_YOY": CURATED_YOY, "YOY_CONTRACT": YOY_CONTRACT,
           "calendar_yoy": calendar_yoy, "valid_yoy_row": valid_yoy_row}
    selected = ast.Module(body=[node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name in FUNCTIONS], type_ignores=[])
    exec(compile(selected, str(SOURCE), "exec"), env)
    return env


def observation(value=3.0):
    return {"value": value, "prev": 2.9, "chg_pct": 0.1, "asof": "boj:202608 YoY",
            "observation_date": "2026-08-01", "unit": "% YoY", "contract_version": CONTRACT}


def test_imf_family_level_cache_cannot_be_reused_as_yoy():
    env = scope()
    old = {"value": 3_000_000, "source": "imf:MFS_DC (family)", "adapter": "family:LG", "status": "LIVE", "fetched_at": NOW.isoformat()}
    called = []
    env["boj_yoy"] = lambda target: called.append(target) or observation()
    row = {"symbol": "JPLG"}
    env["resolve_jplg_row"](row, old, NOW)
    assert row["value"] == 3.0 and row["unit"] == "% YoY" and row["source"] == "bank-of-japan"
    assert row["contract_version"] == CONTRACT and row["cached"] is False and len(called) == 1
    assert env["_family_try"]({"symbol": "JPLG"}) is None


def test_failed_or_budgeted_official_resolution_never_falls_back_to_level():
    env = scope()
    env["boj_yoy"] = lambda target: None
    for allowed in (True, False):
        row = {"symbol": "JPLG"}
        env["resolve_jplg_row"](row, {"value": 3_000_000, "status": "LIVE"}, NOW, allow_fetch=allowed)
        assert row["value"] is None and row["status"] == "PENDING_RESOLUTION"


def test_only_versioned_fresh_yoy_cache_survives():
    env = scope()
    calls = []
    env["boj_yoy"] = lambda target: calls.append(target)
    cached = {**observation(), "source": "bank-of-japan", "resolved_via": ALIAS,
              "status": "LIVE", "fetched_at": NOW.isoformat()}
    row = {"symbol": "JPLG"}
    env["resolve_jplg_row"](row, cached, NOW)
    assert row["cached"] is True and row["value"] == 3.0 and not calls
    for bad in ({**cached, "contract_version": "old"}, {**cached, "observation_date": "2000-01-01"},
                {**cached, "value": float("nan")}, {**cached, "fetched_at": (NOW + timedelta(days=1)).isoformat()}):
        row = {"symbol": "JPLG"}
        env["resolve_jplg_row"](row, bad, NOW)
        assert row["value"] is None and row["cached"] is False


def test_boj_yoy_joins_calendar_months_even_if_provider_omits_a_month():
    env = scope()
    dates = ["202507", "202508", "202509", "202511", "202512", "202601", "202602", "202603", "202604", "202605", "202606", "202607", "202608"]
    values = [100, 200, 201, 201, 201, 201, 201, 201, 201, 201, 201, 103, 208]
    payload = {"RESULTSET": [{"VALUES": {"SURVEY_DATES": dates, "VALUES": values}}]}
    class Response:
        def __enter__(self): return self
        def __exit__(self, *args): pass
        def read(self): return json.dumps(payload).encode()
    env["urllib"] = types.SimpleNamespace(request=types.SimpleNamespace(Request=urllib.request.Request, urlopen=lambda *a, **k: Response()))
    result = env["boj_yoy"](ALIAS.partition(":")[2])
    assert result["value"] == 4.0 and result["prev"] == 3.0
    assert result["observation_date"] == "2026-08-01" and result["unit"] == "% YoY"
    # Without the year-earlier month, do not silently use the 12th prior row.
    del dates[1]; del values[1]
    assert env["boj_yoy"](ALIAS.partition(":")[2]) is None


def test_public_boundary_removes_cached_note_text_and_keeps_reference_ids():
    handler = next(node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name == "lambda_handler")
    boundary = next(node for node in handler.body if isinstance(node, ast.For) and isinstance(node.target, ast.Name) and node.target.id == "public_row")
    rows = [{"symbol": "JPLG", "note_snippet": "private cached note", "note_ids": ["test-id"], "n_notes": 1}, {"symbol": "DXY"}]
    exec(compile(ast.Module(body=[boundary], type_ignores=[]), str(SOURCE), "exec"), {"out": {"symbols": rows}})
    assert all("note_snippet" not in row and row["note_text_private"] is True for row in rows)
    assert rows[0]["note_ids"] == ["test-id"] and rows[0]["n_notes"] == 1


def definition(sid="GDPC1", frequency="Q"):
    return {"id": sid, "frequency_short": frequency, "units": "Billions of Chained Dollars",
            "seasonal_adjustment": "Seasonally Adjusted Annual Rate", "title": "Real GDP"}


def test_quarterly_growth_uses_matching_quarter_not_twelfth_row():
    # Explicitly arrange 14 quarters, with different one-year and three-year levels.
    rows = [{"date": f"{year}-{month:02d}-01", "value": str(100 + year - 2023 + month)}
            for year in (2023, 2024, 2025, 2026) for month in (1, 4, 7, 10) if (year, month) <= (2026, 4)]
    result = calendar_yoy(rows, definition(), NOW)
    assert result["comparison_date"] == "2025-04-01"
    assert result["value"] == round((107 / 106 - 1) * 100, 6)
    assert result["calculation"]["annualized"] is False and result["frequency"] == "Q"


def test_month_gap_and_missing_latest_never_move_the_yoy_base():
    rows = [{"date": "2025-08-01", "value": "100"}, {"date": "2025-09-01", "value": "180"},
            {"date": "2026-07-01", "value": "104"}, {"date": "2026-08-01", "value": "105"}]
    result = calendar_yoy(rows, definition("CPIAUCSL", "M"), NOW)
    assert result["value"] == 5 and result["comparison_date"] == "2025-08-01" and result["prev"] is None
    for invalid in (rows[1:], rows + [{"date": "2026-09-01", "value": "."}],
                    rows + [{"date": "2026-08-02", "value": "110"}]):
        try: calendar_yoy(invalid, definition("CPIAUCSL", "M"), NOW)
        except ValueError: pass
        else: raise AssertionError("missing or conflicting calendar evidence accepted")


def proof():
    return {"contract": "source-evidence.v1", "captured": True, "sha256": "a" * 64, "key": "fixture"}


def yoy_row():
    row = calendar_yoy([{"date": "2025-08-01", "value": "100"}, {"date": "2026-08-01", "value": "105"}], definition("CPIAUCSL", "M"), NOW)
    row.update(evidence={"observations": proof(), "definition": proof()}, fetched_at=NOW.isoformat(), resolved_via="yoy:CPIAUCSL")
    return row


def test_curated_cache_cannot_use_world_bank_annual_or_old_unversioned_value():
    env = scope(); calls = []
    env["fred_yoy"] = lambda sid: calls.append(sid) or yoy_row()
    row = {"symbol": "USIRYY"}
    env["resolve_curated_yoy_row"](row, {"value": 2.95, "status": "LIVE", "source": "worldbank", "fetched_at": NOW.isoformat()}, NOW)
    assert row["value"] == 5 and calls == ["CPIAUCSL"]
    assert env["_family_try"]({"symbol": "USIRYY"}) is None
    cached = dict(row); calls.clear()
    env["resolve_curated_yoy_row"](row, cached, NOW)
    assert row["cached"] is True and not calls
    env["resolve_curated_yoy_row"](row, {"value": 2.95}, NOW, allow_fetch=False)
    assert row["value"] is None and row["quality"]["status"] == "unavailable"


def test_old_nominal_china_gdp_remains_stale_and_preserves_its_definition():
    result = calendar_yoy([{"date": "2022-07-01", "value": "100"}, {"date": "2023-07-01", "value": "106"}],
                          {**definition("CHNGDPNQDSMEI"), "title": "GDP Current Prices China", "units": "Yuan"}, NOW)
    result["evidence"] = {"observations": proof(), "definition": proof()}
    assert result["status"] == "STALE" and result["source_unit"] == "Yuan" and result["published_at"] is None
    assert not valid_yoy_row(result, "CNGDPYY", NOW)
    good = yoy_row()
    assert valid_yoy_row(good, "USIRYY", NOW)
    assert not valid_yoy_row(good, "USIRYY", NOW + timedelta(days=100))


class ConditionalError(Exception):
    response = {"Error": {"Code": "PreconditionFailed"}}


class EvidenceS3:
    def __init__(self): self.objects = {}; self.last = None
    def put_object(self, **kwargs):
        self.last = kwargs
        assert kwargs["IfNoneMatch"] == "*"
        if kwargs["Key"] in self.objects: raise ConditionalError()
        self.objects[kwargs["Key"]] = kwargs
    def get_object(self, **kwargs):
        item = self.objects[kwargs["Key"]]
        return {"Body": io.BytesIO(item["Body"]), "Metadata": item["Metadata"]}


def test_evidence_archive_is_conditional_replayable_and_credential_free():
    s3 = EvidenceS3(); raw = b'{"value":105}'
    url = "https://user:private@api.stlouisfed.org/fred/series?series_id=CPIAUCSL&api_key=secret&token=other#private"
    first = capture(s3, "test", "fred", url, raw, NOW)
    second = capture(s3, "test", "fred", url, raw, NOW + timedelta(days=1))
    assert first == second and len(first["sha256"]) == 64 and read_verified(s3, "test", first) == raw
    assert all(secret not in json.dumps(s3.last["Metadata"]) for secret in ("secret", "private", "other", "user:"))
    s3.objects[first["key"]]["Body"] = gzip.compress(b'{"value":106}')
    try: read_verified(s3, "test", first)
    except ValueError: pass
    else: raise AssertionError("tampered evidence accepted")


def test_evidence_archive_failure_cannot_return_a_success_receipt():
    class Denied:
        def put_object(self, **kwargs): raise PermissionError("fixture")
    try: capture(Denied(), "test", "fred", "https://fred.stlouisfed.org/series/CPIAUCSL", b"x", NOW)
    except PermissionError: pass
    else: raise AssertionError("failed archive marked captured")


def test_fred_fetch_archives_exact_bodies_and_validates_definition_identity():
    env = scope(); s3 = EvidenceS3()
    payloads = {"observations": {"observations": [{"date": "2025-08-01", "value": "100"}, {"date": "2026-08-01", "value": "105"}]},
                "definition": {"seriess": [definition("CPIAUCSL", "M")]}}
    def open_source(request, timeout):
        return io.BytesIO(json.dumps(payloads["observations" if "/observations?" in request.full_url else "definition"]).encode())
    env.update(_FRED_CALLS={"n": 0}, FRED_KEY="private-fixture", S3_BUCKET="test", s3=s3, capture=capture,
               time=types.SimpleNamespace(sleep=lambda *a: None),
               urllib=types.SimpleNamespace(request=types.SimpleNamespace(Request=urllib.request.Request, urlopen=open_source)))
    row = env["fred_yoy"]("CPIAUCSL")
    assert row["value"] == 5 and len(row["evidence"]) == 2
    assert json.loads(read_verified(s3, "test", row["evidence"]["observations"])) == payloads["observations"]
    payloads["definition"]["seriess"][0]["id"] = "WRONG"
    assert env["fred_yoy"]("CPIAUCSL") is None


def test_consumer_checks_reject_malformed_proof_and_value_drift():
    row = yoy_row()
    for key, bad in (("quality", []), ("evidence", []), ("evidence", {"observations": [], "definition": {}}),
                     ("v", 999), ("value", True), ("value", float("nan")), ("calculation", {})):
        assert not valid_yoy_row({**row, key: bad}, "USIRYY", NOW), key


if __name__ == "__main__":
    tests = [fn for name, fn in sorted(globals().items()) if name.startswith("test_") and callable(fn)]
    for test in tests: test()
    print(f"TradingView macro contracts passed: {len(tests)}")
    import subprocess
    subprocess.run([sys.executable, str(Path(__file__).with_name('test_fred_levels.py'))], check=True)
