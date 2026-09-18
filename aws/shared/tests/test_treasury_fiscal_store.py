import copy
import gzip
import hashlib
import importlib
import io
import json
import sys
import types
import unittest
from datetime import datetime, timezone
from unittest.mock import patch
from test_treasury_fiscal_model import NOW, page, debt
from treasury_fiscal_model import build, encoded


class Store:
    def __init__(self):
        self.docs = {}; self.writes = []; self.race = None
    def put_object(self, **kw):
        key = kw["Key"]
        if self.race and key == "data/warm/treasury/debt_to_penny.json.gz":
            race, self.race = self.race, None
            race()
        old = self.docs.get(key)
        if (kw.get("IfNoneMatch") == "*" and old) or ("IfMatch" in kw and (not old or kw["IfMatch"] != old["ETag"])):
            exc = RuntimeError("conditional conflict"); exc.response = {"Error": {"Code": "PreconditionFailed"}}; raise exc
        self.docs[key] = {**copy.deepcopy(kw), "ETag": '"' + hashlib.md5(kw["Body"]).hexdigest() + '"',
                          "LastModified": datetime.now(timezone.utc)}
        self.writes.append(key)
    def get_object(self, **kw):
        if kw["Key"] not in self.docs:
            exc = RuntimeError("absent"); exc.response = {"Error": {"Code": "NoSuchKey"}}; raise exc
        out = copy.deepcopy(self.docs[kw["Key"]]); out["Body"] = io.BytesIO(out["Body"])
        return out


def module():
    with patch.dict(sys.modules, {"boto3": types.SimpleNamespace(client=lambda *a, **k: object())}):
        return importlib.import_module("treasury_fiscal_store")


class FiscalStoreTests(unittest.TestCase):
    def test_previous_legacy_archive_and_full_replay_before_conditional_publish(self):
        mod = module(); store = Store(); key = "data/warm/treasury/debt_to_penny.json.gz"
        legacy = {"observations": [{"date": "2010-01-01", "value": 5}]}
        store.put_object(Bucket="b", Key=key, Body=gzip.compress(encoded(legacy)))
        p = page("debt_to_penny", [debt()])
        with patch.object(mod, "now", return_value=NOW), patch.object(mod, "read_snapshot", return_value=encoded(p["document"])):
            out = mod.merge(store, "b", "debt_to_penny", [p])
        self.assertEqual(out["legacy_rows_promoted"], 0)
        self.assertEqual(json.loads(mod.read_verified(store, "b", out["previous_evidence"])), legacy)
        self.assertTrue(out["replay"]["retained_inputs_replayed"])
        manifest = json.loads(store.docs[out["replay"]["manifest_key"]]["Body"])
        self.assertEqual(manifest["output_sha256"], out["replay"]["output_sha256"])
        self.assertIn("IfMatch", store.docs[key])

    def test_concurrent_backfill_survives_daily_writer_retry(self):
        mod = module(); store = Store(); key = "data/warm/treasury/debt_to_penny.json.gz"
        initial = build("debt_to_penny", None, [page("debt_to_penny", [debt()])], NOW)
        store.put_object(Bucket="b", Key=key, Body=gzip.compress(encoded(initial)))
        deep = build("debt_to_penny", initial, [page("debt_to_penny", [debt("2010-01-01")])], NOW)
        store.race = lambda: store.put_object(Bucket="b", Key=key, Body=gzip.compress(encoded(deep)))
        p = page("debt_to_penny", [debt("2026-09-18")])
        with patch.object(mod, "now", return_value=NOW), patch.object(mod, "read_snapshot", return_value=encoded(p["document"])):
            out = mod.merge(store, "b", "debt_to_penny", [p])
        self.assertEqual([o["date"] for o in out["observations"]], ["2010-01-01", "2026-09-17", "2026-09-18"])

    def test_failed_original_replay_never_overwrites_existing_history(self):
        mod = module(); store = Store(); key = "data/warm/treasury/debt_to_penny.json.gz"
        old = {"observations": [{"date": "2020-01-01", "value": 7}]}
        store.put_object(Bucket="b", Key=key, Body=gzip.compress(encoded(old)))
        original = store.docs[key]["Body"]
        p = page("debt_to_penny", [debt()])
        bad = copy.deepcopy(p["document"]); bad["data"][0]["tot_pub_debt_out_amt"] = "4"
        with patch.object(mod, "now", return_value=NOW), patch.object(mod, "read_snapshot", return_value=encoded(bad)):
            with self.assertRaisesRegex(ValueError, "replay differs"):
                mod.merge(store, "b", "debt_to_penny", [p])
        self.assertEqual(store.docs[key]["Body"], original)

    def test_access_error_is_not_treated_as_empty_history(self):
        mod = module(); store = Store()
        err = RuntimeError("denied"); err.response = {"Error": {"Code": "AccessDenied"}}
        with patch.object(store, "get_object", side_effect=err):
            with self.assertRaises(RuntimeError): mod.get(store, "b", "data/test.json")


if __name__ == "__main__": unittest.main()
