"""Consumer tests: unitless fallbacks cannot recreate a rejected curated metric."""
import ast
import io
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "shared"))
from macro_observations import CURATED_YOY, YOY_CONTRACT, calendar_yoy, finite, valid_yoy_row
SOURCE = Path(__file__).resolve().parents[1] / "source/lambda_function.py"
NOW = datetime(2026, 9, 18, tzinfo=timezone.utc)


class Clock:
    @staticmethod
    def now(tz): return NOW


def run(vault):
    feeds = {"data/tradingview.json": vault, "data/families.json": {"families": {"IRYY": {"US": [2.95, "2024-01-01"]}}},
             "data/te-feed.json": {"prices": {"USIRYY": {"value": 88}, "CNGDPYY": {"value": 21.05}, "OTHER": {"value": 10}}},
             "data/symbol-feed.json": {"prices": {"USIRYY": {"value": 99}}}}
    output = []
    class S3:
        def put_object(self, **kw): output.append(json.loads(kw["Body"]))
    tree = ast.parse(SOURCE.read_text(encoding="utf8"))
    env = dict(json=json, time=time, datetime=Clock, timezone=timezone, CURATED_YOY=CURATED_YOY,
               YOY_CONTRACT=YOY_CONTRACT, finite=finite, valid_yoy_row=valid_yoy_row,
               BUCKET="fixture", MARKER="fixture", S3=S3())
    exec(compile(ast.Module(body=[n for n in tree.body if isinstance(n, ast.FunctionDef)], type_ignores=[]), str(SOURCE), "exec"), env)
    env["load"] = lambda key: feeds.get(key, {})
    env["lambda_handler"]({}, None)
    return output[0]


def test_legacy_percentage_cannot_escape_through_any_fallback():
    result = run({"symbols": [{"symbol": "USIRYY", "status": "LIVE", "value": 2.95}]})
    assert "USIRYY" not in result["indicators"] and "CNGDPYY" not in result["indicators"]
    assert result["gaps"]["USIRYY"]["last_observed_value"] == 2.95
    assert result["indicators"]["OTHER"]["v"] == 10


def test_full_definition_and_evidence_survive_bus_transport():
    row = calendar_yoy([{"date": "2025-08-01", "value": "100"}, {"date": "2026-08-01", "value": "105"}],
                      {"id": "CPIAUCSL", "frequency_short": "M", "units": "Index", "seasonal_adjustment": "SA"}, NOW)
    proof = {"contract": "source-evidence.v1", "captured": True, "sha256": "a" * 64, "key": "fixture"}
    row.update(symbol="USIRYY", source="fred_yoy:CPIAUCSL", evidence={"observations": proof, "definition": proof})
    result = run({"symbols": [row], "generated_at": NOW.isoformat()})
    actual = result["indicators"]["USIRYY"]
    assert actual["v"] == 5 and actual["evidence"] == row["evidence"] and actual["source_unit"] == "Index"
    assert actual["comparison_date"] == "2025-08-01" and valid_yoy_row(actual, "USIRYY", NOW)
    assert result["source_generated_at"] == NOW.isoformat()


if __name__ == "__main__":
    tests = [v for k, v in sorted(globals().items()) if k.startswith("test_")]
    for test in tests: test()
    print("Indicator bus tests passed:", len(tests))
