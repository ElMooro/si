import importlib.util
import json
from pathlib import Path
import sys
import types
import unittest
from unittest.mock import patch


class CollectorTests(unittest.TestCase):
    def test_capture_failure_cannot_write_new_warehouse_or_return_green(self):
        path = Path(__file__).resolve().parents[1] / "source/lambda_function.py"
        spec = importlib.util.spec_from_file_location("fiscal_collector_test", path); mod = importlib.util.module_from_spec(spec)
        with patch.dict(sys.modules, {"boto3": types.SimpleNamespace(client=lambda *a, **kw: object())}): spec.loader.exec_module(mod)
        mod.DATASETS = {"debt_to_penny": {}}
        def fail(*args): raise ValueError("archive readback failed")
        def forbidden(*args): raise AssertionError("no derived write after failed original")
        mod.acquire = fail; mod.merge = forbidden
        mod.summary = lambda *args: {"generated_at": "2026-09-18"}
        result = mod.lambda_handler({}, None)
        self.assertEqual(result["statusCode"], 503)
        self.assertFalse(json.loads(result["body"])["ok"])
