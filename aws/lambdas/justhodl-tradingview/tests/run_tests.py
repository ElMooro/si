"""JPLG source/unit regression fixtures. AST loading prevents cloud imports."""
import ast
from datetime import datetime, timezone, timedelta
import json
import math
from pathlib import Path
import re
import types
import urllib.request

SOURCE = Path(__file__).parents[1] / "source" / "lambda_function.py"
tree = ast.parse(SOURCE.read_text())
FUNCTIONS = {"boj_yoy", "resolve_jplg_row", "_family_try"}
NOW = datetime(2026, 9, 9, tzinfo=timezone.utc)
ALIAS = "boj:MD11:DLCLAADBLTTO|MD11:DLCLBADBLTTO"
CONTRACT = "boj-loan-growth-yoy.v1"


def scope():
    env = {"datetime": datetime, "timezone": timezone, "json": json, "math": math,
           "re": re, "JPLG_CONTRACT": CONTRACT, "ALIASES": {"JPLG": ALIAS}}
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


if __name__ == "__main__":
    tests = [fn for name, fn in sorted(globals().items()) if name.startswith("test_") and callable(fn)]
    for test in tests: test()
    print(f"TradingView JPLG tests passed: {len(tests)}")
