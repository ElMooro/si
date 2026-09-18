import importlib.util
import json
from pathlib import Path
import sys
import types
import unittest
from unittest.mock import patch


class BackfillTests(unittest.TestCase):
    def test_old_complete_cursor_cannot_skip_new_dimensional_backfill(self):
        import treasury_fiscal_model as model
        from test_treasury_fiscal_model import page, NOW
        ds = "tga_operating_cash"
        rows = [{"record_date": "2026-01-02", "account_type": model.TGA_CLOSING, "open_today_bal": "1"}]
        warehouse = model.build(ds, None, [page(ds, rows)], NOW)
        path = Path(__file__).resolve().parents[1] / "source/lambda_function.py"
        spec = importlib.util.spec_from_file_location("backfill_test", path); mod = importlib.util.module_from_spec(spec)
        with patch.dict(sys.modules, {"boto3": types.SimpleNamespace(client=lambda *a, **kw: object())}): spec.loader.exec_module(mod)
        class Store:
            def put_object(self, **kw): self.saved = kw
        mod.s3 = Store(); calls = []
        def get(client, bucket, key):
            calls.append(key)
            return (None, None, None) if key == mod.PROG_KEY else (warehouse, "etag", b"old")
        def acquire(dataset, bucket, before):
            self.assertEqual(before, "2026-01-03")  # boundary is re-read, not skipped
            return page(ds, rows)
        mod.get = get; mod.acquire = acquire; mod.merge = lambda *a: {**warehouse, "replay": {}}
        mod.summary = lambda *a: None
        result = mod.lambda_handler({"dataset": ds}, None)
        self.assertEqual(result["statusCode"], 200)
        self.assertEqual(json.loads(result["body"])["status"], "complete")
        self.assertNotIn("data/audit/backfill-progress.json", calls)
        self.assertEqual(mod.s3.saved["IfNoneMatch"], "*")
