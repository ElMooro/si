"""Provider-flow decision boundary regression + the incremental reader (v2.0.0); no AWS calls."""
from pathlib import Path
import gzip
import io
import json
import sys
import time
import types
import unittest
from datetime import datetime, timezone

ROOT = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(ROOT / "tests"))
sys.path.insert(0, str(ROOT / "aws" / "shared"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "source"))
from provider_flow_test_support import FlowBoundaries  # noqa: E402

import lambda_function as engine  # noqa: E402


class FakeS3:
    """Enough of boto3 S3 for the engine: list_objects_v2 pages with ETag + Delimiter, get/put."""

    def __init__(self, objects):
        self.objects = dict(objects)      # key -> bytes
        self.gets = []
        self.puts = {}
        self.get_delay = 0.0

    def _etag(self, key):
        import hashlib
        return hashlib.md5(self.objects[key]).hexdigest()

    def get_paginator(self, name):
        assert name == "list_objects_v2"
        s3 = self

        class P:
            def paginate(self, Bucket, Prefix, Delimiter=None):
                keys = sorted(k for k in s3.objects if k.startswith(Prefix))
                if Delimiter:
                    keys = [k for k in keys if Delimiter not in k[len(Prefix):]]
                for i in range(0, len(keys), 3):
                    yield {"Contents": [{"Key": k, "ETag": '"%s"' % s3._etag(k), "Size": len(s3.objects[k]),
                                         "LastModified": datetime.now(timezone.utc)} for k in keys[i:i + 3]]}
        return P()

    def get_object(self, Bucket, Key):
        self.gets.append(Key)
        if Key not in self.objects:
            raise KeyError(Key)
        if self.get_delay:
            time.sleep(self.get_delay)
        return {"Body": io.BytesIO(self.objects[Key])}

    def put_object(self, Bucket, Key, Body, **kw):
        self.puts[Key] = Body
        self.objects[Key] = Body


def research_doc(ticker, price=100.0, generated_at="2026-10-25T09:00:00+00:00"):
    return json.dumps({"ticker": ticker, "company": {"name": ticker + " Inc", "sector": "Tech"}, "quote": {"price": price},
                       "verdict": {"rating": "BUY"}, "generated_at": generated_at}).encode()


def edgar_doc(ticker):
    return json.dumps({"ticker": ticker, "signal": {"label": "CLUSTER_BUY"}, "generated_at": "2026-10-25T10:00:00+00:00"}).encode()


def context(seconds):
    return types.SimpleNamespace(get_remaining_time_in_millis=lambda: int(seconds * 1000))


def base_objects():
    objs = {"equity-research/%s.json" % t: research_doc(t) for t in ("AAPL", "MSFT", "NVDA", "TSLA", "AMD")}
    objs["equity-research/manifest.json"] = b"{}"
    objs["equity-research/industry2/AAPL.json"] = json.dumps({"ticker": "AAPL", "peers": ["MSFT"], "prices": [1] * 50}).encode()
    objs["equity-research/industry2/CTHRX.json"] = json.dumps({"ticker": "CTHRX", "peers": [], "prices": [1] * 50}).encode()
    objs["equity-research/shared/x.json"] = b"{}"
    objs["edgar-insiders/WBD.json"] = edgar_doc("WBD")
    objs["edgar-insiders/AAPL.json"] = edgar_doc("AAPL")
    objs["equity-critique/BRK-B.json"] = json.dumps({"ticker": "BRK-B", "generated_at": "2026-09-01T00:00:00+00:00"}).encode()
    return objs


class IncrementalReader(unittest.TestCase):
    def setUp(self):
        self.s3 = FakeS3(base_objects())
        engine.s3 = self.s3

    def body(self, key):
        return json.loads(self.s3.puts[key])

    def state(self, table):
        return json.loads(gzip.decompress(self.s3.puts["analytics/_state/%s.json.gz" % table]))

    def test_tables_are_the_top_level_only_and_subfolders_are_never_read(self):
        res = json.loads(engine.lambda_handler({}, context(600))["body"])
        self.assertEqual(res["n_research_rows"], 5)
        self.assertEqual(res["n_edgar_rows"], 2)
        self.assertEqual(res["n_critique_rows"], 1)
        self.assertTrue(res["complete"])
        self.assertEqual(res["pending"], 0)
        self.assertFalse(any("industry2/" in k or "/shared/" in k or k.endswith("manifest.json") for k in self.s3.gets))
        rows = self.body("analytics/equity_research_flat.json")
        self.assertEqual([r["ticker"] for r in rows["rows"]], ["AAPL", "AMD", "MSFT", "NVDA", "TSLA"])
        self.assertTrue(rows["complete"])
        self.assertEqual(rows["pending"], 0)
        self.assertEqual(rows["schema_version"], engine.SCHEMA_VERSION)
        man = self.body("analytics/manifest.json")
        self.assertTrue(man["complete"])
        self.assertEqual(man["read_stats"]["equity_research"]["listed"], 5)
        self.assertEqual(man["read_stats"]["equity_research"]["read"], 5)
        self.assertEqual(man["read_stats"]["equity_research"]["reused"], 0)
        self.assertEqual([t["name"] for t in man["tables"]], ["equity_research", "edgar_insiders", "research_critique", "etf_flows"])
        st = self.state("equity_research")
        self.assertEqual(st["n_keys"], 5)
        self.assertEqual(set(st["rows"]), {"equity-research/%s.json" % t for t in ("AAPL", "MSFT", "NVDA", "TSLA", "AMD")})
        self.assertTrue(all(v["etag"] and v["row"]["ticker"] for v in st["rows"].values()))

    def test_second_run_reads_only_changed_and_new_objects_and_drops_deleted_keys(self):
        engine.lambda_handler({}, context(600))
        first_rows = self.body("analytics/equity_research_flat.json")["rows"]
        self.s3.gets.clear()
        self.s3.objects["equity-research/AAPL.json"] = research_doc("AAPL", price=123.0, generated_at="2026-10-26T09:00:00+00:00")
        self.s3.objects["equity-research/GOOG.json"] = research_doc("GOOG")
        del self.s3.objects["equity-research/TSLA.json"]
        res = json.loads(engine.lambda_handler({}, context(600))["body"])
        got = [k for k in self.s3.gets if k.startswith("equity-research/")]
        self.assertEqual(sorted(got), ["equity-research/AAPL.json", "equity-research/GOOG.json"])
        stats = res["read_stats"]["equity_research"]
        self.assertEqual((stats["listed"], stats["reused"], stats["read"], stats["dropped"], stats["pending"]), (5, 3, 2, 1, 0))
        rows = {r["ticker"]: r for r in self.body("analytics/equity_research_flat.json")["rows"]}
        self.assertEqual(set(rows), {"AAPL", "AMD", "MSFT", "NVDA", "GOOG"})
        self.assertEqual(rows["AAPL"]["price"], 123.0)
        self.assertEqual(rows["AAPL"]["generated_at"], "2026-10-26T09:00:00+00:00")
        self.assertEqual(rows["MSFT"], next(r for r in first_rows if r["ticker"] == "MSFT"))
        # edgar / critique: nothing changed, nothing read
        self.assertFalse(any(k.startswith(("edgar-insiders/", "equity-critique/")) for k in self.s3.gets))

    def test_deadline_keeps_previous_rows_and_reports_pending_without_claiming_completeness(self):
        engine.lambda_handler({}, context(600))
        for t in ("AAPL", "MSFT", "NVDA", "TSLA", "AMD"):
            self.s3.objects["equity-research/%s.json" % t] = research_doc(t, price=1.0, generated_at="2026-10-26T09:00:00+00:00")
        old_reserve = engine.RESERVE_S
        engine.RESERVE_S = 10 ** 6   # the reserve swallows the whole budget: no time to read, tables still publish
        try:
            res = json.loads(engine.lambda_handler({}, context(600))["body"])
        finally:
            engine.RESERVE_S = old_reserve
        self.assertFalse(res["complete"])
        self.assertGreaterEqual(res["pending"], 1)
        rows = self.body("analytics/equity_research_flat.json")
        self.assertFalse(rows["complete"])
        self.assertEqual(rows["pending"], res["read_stats"]["equity_research"]["pending"])
        self.assertEqual(len(rows["rows"]), 5, "previous rows stay (each carries its own generated_at)")
        man = self.body("analytics/manifest.json")
        self.assertFalse(man["complete"])
        self.assertEqual(man["pending"]["equity_research"], rows["pending"])
        # the state keeps the OLD etag for pending keys, so the next run reads them
        st = self.state("equity_research")
        stale = [k for k, v in st["rows"].items() if v["row"]["price"] == 100.0]
        self.assertEqual(len(stale), rows["pending"])
        self.assertEqual(rows["pending"], 5)
        res2 = json.loads(engine.lambda_handler({}, context(600))["body"])
        self.assertTrue(res2["complete"])
        self.assertTrue(all(r["price"] == 1.0 for r in self.body("analytics/equity_research_flat.json")["rows"]))

    def test_unflattenable_or_tickerless_bodies_are_skipped_once_and_not_reread_until_changed(self):
        self.s3.objects["equity-research/BAD.json"] = b"{not json"
        self.s3.objects["equity-research/NOTICKER.json"] = json.dumps({"company": {"name": "x"}}).encode()
        res = json.loads(engine.lambda_handler({}, context(600))["body"])
        self.assertEqual(res["n_research_rows"], 5)
        self.assertEqual(res["research_skipped"], 1)
        self.assertTrue(res["complete"])
        self.s3.gets.clear()
        engine.lambda_handler({}, context(600))
        self.assertFalse(any(k.startswith("equity-research/") for k in self.s3.gets), "nothing changed, nothing re-read")

    def test_config_has_the_budget_the_reader_assumes(self):
        cfg = json.loads((Path(__file__).resolve().parents[1] / "config.json").read_text())
        self.assertEqual(cfg["timeout"], 600)
        self.assertGreaterEqual(cfg["memory"], 1024)
        self.assertEqual(cfg["handler"], "lambda_function.lambda_handler")
        self.assertEqual(engine.VERSION, "2.0.0")
        self.assertLess(engine.RESERVE_S, 120)


if __name__ == "__main__":
    suite = unittest.TestSuite()
    suite.addTests(unittest.defaultTestLoader.loadTestsFromTestCase(FlowBoundaries))
    suite.addTests(unittest.defaultTestLoader.loadTestsFromTestCase(IncrementalReader))
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    if not result.wasSuccessful():
        raise SystemExit(1)
