import gzip
import importlib.util
import io
import json
from pathlib import Path
import sys
import types
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT.parents[1] / "shared"))
from evidence_store import read_verified


class Missing(Exception):
    response = {"Error": {"Code": "NoSuchKey"}}


class Store:
    def __init__(self): self.objects = {}; self.denied = False; self.conflict = False
    def get_object(self, **kw):
        if self.denied and kw["Key"].startswith("data/warm/"):
            err = Missing(); err.response = {"Error": {"Code": "AccessDenied"}}; raise err
        if kw["Key"] not in self.objects: raise Missing()
        row = self.objects[kw["Key"]]
        return {"Body": io.BytesIO(row["Body"]), "Metadata": row.get("Metadata", {}), "ETag": '"fixture-etag"'}
    def put_object(self, **kw):
        if self.conflict and kw["Key"].startswith("data/warm/") and "IfMatch" in kw:
            err = Missing(); err.response = {"Error": {"Code": "PreconditionFailed"}}; raise err
        self.objects[kw["Key"]] = kw


def module(store):
    fake_boto = types.SimpleNamespace(client=lambda *a, **kw: store)
    with patch.dict(sys.modules, {"boto3": fake_boto}):
        spec = importlib.util.spec_from_file_location("usgov_fixture", ROOT / "source/lambda_function.py")
        mod = importlib.util.module_from_spec(spec); spec.loader.exec_module(mod)
        return mod


class BLSTests(unittest.TestCase):
    def setUp(self):
        self.store = Store(); self.mod = module(self.store)
        self.mod.BLS_SERIES = ["CUUR0000SA0"]
        self.mod._key = lambda _: "fixture-key"
        self.key = "data/warm/usgov/bls/CUUR0000SA0.json.gz"
        self.requests = []

    def invoke(self, rows, status="REQUEST_SUCCEEDED"):
        raw = json.dumps({"status": status, "Results": {"series": [{"seriesID": "CUUR0000SA0", "data": rows}]}}).encode()
        def fetch(req, **kw): self.requests.append(json.loads(req.data)); return io.BytesIO(raw)
        summary = {}
        with patch.object(self.mod.urllib.request, "urlopen", fetch): self.mod._bls(summary)
        return summary, raw

    def test_recent_window_and_old_history_are_both_preserved(self):
        previous = {"series": "CUUR0000SA0", "data": [{"year": "2000", "period": "M01", "value": "100"}, {"year": "2025", "period": "M12", "value": "310"}]}
        self.store.put_object(Key=self.key, Body=gzip.compress(json.dumps(previous).encode()))
        summary, raw = self.invoke([{"year": "2026", "period": "M08", "value": "330"}, {"year": "2025", "period": "M12", "value": "311"}])
        request = self.requests[0]
        self.assertEqual(int(request["endyear"])-int(request["startyear"]), 19)
        self.assertTrue(summary["bls"]["ok"])
        doc = json.loads(gzip.decompress(self.store.objects[self.key]["Body"]))
        values = {(r["year"], r["period"]): r["value"] for r in doc["data"]}
        self.assertEqual(values[("2000", "M01")], "100")
        self.assertEqual(values[("2025", "M12")], "311")
        self.assertEqual(read_verified(self.store, self.mod.BUCKET, doc["evidence"]), raw)
        self.assertEqual(json.loads(read_verified(self.store, self.mod.BUCKET, doc["prior_warehouse_evidence"])), previous)

    def test_unkeyed_window_is_ten_years(self):
        self.mod._key = lambda _: None
        self.invoke([{"year": "2026", "period": "M08", "value": "330"}])
        request = self.requests[0]
        self.assertEqual(int(request["endyear"])-int(request["startyear"]), 9)
        self.assertNotIn("registrationkey", request)

    def test_failed_request_and_empty_series_do_not_replace_history(self):
        old = b"existing archival bytes"
        self.store.put_object(Key=self.key, Body=old)
        for status in ("REQUEST_FAILED", "REQUEST_SUCCEEDED"):
            summary, _ = self.invoke([], status)
            self.assertFalse(summary["bls"]["ok"])
            self.assertEqual(self.store.objects[self.key]["Body"], old)

    def test_read_failure_cannot_reset_history(self):
        self.store.denied = True
        summary, _ = self.invoke([{"year": "2026", "period": "M08", "value": "330"}])
        self.assertFalse(summary["bls"]["ok"])
        self.assertNotIn(self.key, self.store.objects)

    def test_concurrent_revision_cannot_be_overwritten(self):
        old = gzip.compress(json.dumps({"series": "CUUR0000SA0", "data": [{"year": "2025", "period": "M12", "value": "310"}]}).encode())
        self.store.put_object(Key=self.key, Body=old)
        self.store.conflict = True
        summary, _ = self.invoke([{"year": "2026", "period": "M08", "value": "330"}])
        self.assertFalse(summary["bls"]["ok"])
        self.assertEqual(self.store.objects[self.key]["Body"], old)

    def test_outside_request_year_is_rejected(self):
        summary, _ = self.invoke([{"year": "2000", "period": "M08", "value": "330"}])
        self.assertFalse(summary["bls"]["ok"])
        self.assertNotIn(self.key, self.store.objects)

    def test_bounded_bls_refresh_skips_unrelated_collectors(self):
        self.mod._bls = lambda summary: summary.update(bls={"ok": True})
        self.mod._bea = self.mod._fed_ddp = lambda summary: (_ for _ in ()).throw(AssertionError("unrelated collector"))
        result = self.mod.lambda_handler({"feed": "bls"}, None)
        self.assertEqual(result["statusCode"], 200)


if __name__ == "__main__": unittest.main()
