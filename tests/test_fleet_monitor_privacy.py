"""Execute the real fleet handler with local metadata/provider fixtures only."""
import ast
from contextlib import redirect_stdout
from copy import deepcopy
from datetime import datetime, timezone
import io
import json
from pathlib import Path
import sys
import time
import types
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "aws/shared"))
from private_artifact import is_private_source
from public_brain_projection import sanitize_public

MARKER = "PRIVATE_NOTE_OR_PROVIDER_SECRET_SENTINEL"
NOW = datetime(2026, 9, 9, 12, tzinfo=timezone.utc)
SOURCE = ROOT / "aws/lambdas/justhodl-fleet-monitor/source/lambda_function.py"


class Store:
    def __init__(self):
        self.reads, self.writes = [], {}

    def list_objects_v2(self, **kwargs):
        return {"Contents": [{"Key": key, "LastModified": NOW, "Size": 500} for key in
                ("data/brain.json", "data/ai-brief.json", "data/good.json", "data/bad.json", "data/unreadable.json")]}

    def get_object(self, *, Key, **kwargs):
        self.reads.append(Key)
        if is_private_source(Key):
            raise AssertionError("Private content read attempted")
        if Key == "data/unreadable.json":
            raise RuntimeError(MARKER)
        document = {"outputs": {}} if Key.endswith("cadence-manifest.json") else {"error": MARKER} if Key == "data/bad.json" else {"ok": True}
        return {"Body": io.BytesIO(json.dumps(document).encode())}

    def put_object(self, *, Key, Body, **kwargs):
        self.writes[Key] = json.loads(Body)


def handler_scope(store, compute_error=False):
    tree = ast.parse(SOURCE.read_text())
    nodes = [node for node in tree.body if isinstance(node, ast.FunctionDef)]
    def list_functions(**kwargs):
        if compute_error:
            raise RuntimeError(MARKER)
        return {"Functions": [{"FunctionName": "fixture"}]}
    namespace = dict(json=json, datetime=datetime, timezone=timezone, time=time,
        s3=store, boto3=types.SimpleNamespace(client=lambda *_: types.SimpleNamespace(list_functions=list_functions)),
        BUCKET="fixture", OUT_KEY="_health/fleet.json", ALERT_STATE_KEY="_health/fleet_last_alert.json",
        FRESH_H=30, STALE_RED_H=168, MIN_SIZE=60, STATIC_OUTPUTS=set(),
        ANTHROPIC_KEY="synthetic-key", KEYS={key: "synthetic-key" for key in ("FRED", "FMP", "POLYGON", "ALPHAVANTAGE", "CMC")},
        is_private_source=is_private_source, sanitize_public=sanitize_public)
    exec(compile(ast.Module(body=nodes, type_ignores=[]), str(SOURCE), "exec"), namespace)
    namespace["now"] = lambda: NOW
    namespace["http"] = lambda *_args, **_kwargs: (500, MARKER)
    messages = []
    namespace["send_telegram"] = lambda message: messages.append(message) or False
    return namespace, messages


class FleetPrivacyTests(unittest.TestCase):
    def test_real_handler_skips_private_bodies_and_never_publishes_or_alerts_error_text(self):
        store = Store()
        namespace, messages = handler_scope(store, compute_error=True)
        logs = io.StringIO()
        with redirect_stdout(logs):
            response = namespace["lambda_handler"]({}, None)
        self.assertFalse(set(store.reads) & {"data/brain.json", "data/ai-brief.json"})
        output = store.writes["_health/fleet.json"]
        self.assertEqual(output["summary"]["data_outputs_total"], 5)
        self.assertEqual(output["data_outputs"]["private_content_checks_skipped"], 2)
        self.assertEqual(output["data_outputs"]["n_degraded"], 2)
        self.assertEqual(output["compute"]["error"], "COMPUTE_INVENTORY_UNAVAILABLE")
        self.assertEqual(output["privacy_version"], "fleet-metadata-20260909-v1")
        self.assertNotIn(MARKER, json.dumps([store.writes, response, messages, logs.getvalue()]))
        self.assertEqual(len(output["dependencies"]), 6)

    def test_legacy_projection_preserves_measurements_and_strips_all_diagnostic_free_text(self):
        legacy = {"engine": "fleet-monitor", "generated_at": "2026-09-08T12:00:00Z", "elapsed_s": 0,
            "system_status": "red", "summary": {"lambda_count": 321, "dependencies_down": 0},
            "data_outputs": {"available": True, "n_degraded": 1, "error": MARKER,
                "degraded": [{"output": "example", "age_hours": 0, "size": 300, "cadence_hours": 3, "issue": MARKER}],
                "static": [{"output": "user-watchlist", "age_hours": 10}]},
            "compute": {"available": False, "error": MARKER, "note": MARKER},
            "dependencies": [{"name": "FRED", "status": "yellow", "detail": MARKER, "response": MARKER}],
            "note": MARKER, "raw_unknown_error": MARKER}
        original = deepcopy(legacy)
        output = sanitize_public("_health/fleet.json", legacy)
        self.assertEqual(output["summary"]["lambda_count"], 321)
        self.assertEqual(output["elapsed_s"], 0)
        self.assertEqual(output["data_outputs"]["degraded"][0]["cadence_hours"], 3)
        self.assertNotIn(MARKER, json.dumps(output))
        self.assertEqual(legacy, original)
        self.assertEqual(sanitize_public("_health/fleet.json", output), output)
        self.assertEqual(sanitize_public("data/_health/fleet.json", legacy), output)

    def test_probe_categories_preserve_actionable_status_without_echoing_provider_body(self):
        scope, _ = handler_scope(Store())
        for code, expected in ((401, "red"), (429, "yellow"), (503, "yellow"), (None, "yellow")):
            scope["http"] = lambda *_a, **_kw: (code, MARKER)
            for result in (scope["probe_provider"]("FRED", "fixture"), scope["probe_anthropic"]()):
                self.assertEqual(result["status"], expected)
                self.assertNotIn(MARKER, json.dumps(result))

    def test_failed_inventory_does_not_claim_healthy(self):
        store = Store()
        store.list_objects_v2 = lambda **_kw: (_ for _ in ()).throw(RuntimeError(MARKER))
        scope, _ = handler_scope(store)
        with redirect_stdout(io.StringIO()):
            scope["lambda_handler"]({}, None)
        output = store.writes["_health/fleet.json"]
        self.assertEqual(output["system_status"], "red")
        self.assertFalse(output["data_outputs"]["available"])
        self.assertNotIn(MARKER, json.dumps(output))


if __name__ == "__main__":
    unittest.main()
