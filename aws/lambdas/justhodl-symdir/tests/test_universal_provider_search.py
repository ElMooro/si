import gzip
import importlib.util
import json
import os
import sqlite3
import sys
import tempfile
import types
import unittest
from array import array
from collections import defaultdict
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
CATALOG = ROOT / "justhodl-provider-catalog/source/lambda_function.py"
SYMDIR = ROOT / "justhodl-symdir/source/lambda_function.py"


class FakeS3:
    def __init__(self):
        self.puts = {}

    def put_object(self, **kwargs):
        self.puts[kwargs["Key"]] = kwargs
        return {}

    def upload_file(self, filename, bucket, key, ExtraArgs=None):
        with open(filename, "rb") as src:
            self.puts[key] = {
                "Bucket": bucket,
                "Key": key,
                "Body": src.read(),
                **(ExtraArgs or {}),
            }


def load_lambda(name, path, fake_s3):
    boto3 = types.ModuleType("boto3")
    boto3.client = lambda *args, **kwargs: fake_s3
    config_mod = types.ModuleType("botocore.config")
    config_mod.Config = lambda **kwargs: kwargs
    botocore = types.ModuleType("botocore")
    botocore.config = config_mod
    saved = {key: sys.modules.get(key) for key in
             ("boto3", "botocore", "botocore.config")}
    sys.modules.update({
        "boto3": boto3,
        "botocore": botocore,
        "botocore.config": config_mod,
    })
    try:
        spec = importlib.util.spec_from_file_location(name, path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module
    finally:
        for key, old in saved.items():
            if old is None:
                sys.modules.pop(key, None)
            else:
                sys.modules[key] = old


class UniversalProviderSearchTests(unittest.TestCase):
    def test_catalog_writes_compact_search_shard(self):
        s3 = FakeS3()
        catalog = load_lambda("catalog_under_test", CATALOG, s3)
        db = sqlite3.connect(":memory:")
        db.execute(
            "CREATE VIRTUAL TABLE docs USING fts5("
            "id UNINDEXED,provider,provider_name UNINDEXED,title,key,"
            "kind UNINDEXED,nbytes UNINDEXED,age_h UNINDEXED,hot UNINDEXED)")
        meta = catalog._write_search_shard(
            "gdelt",
            "GDELT",
            "gdeltproject.org",
            [{
                "key": "data/warm/gdelt/events/2026/09/02/world-events.json.gz",
                "bytes": 1234,
                "age_h": 2.5,
                "hot": True,
            }],
            {"ids": ["GKG_THEME"]},
            db,
        )

        self.assertEqual(meta["provider"], "gdelt")
        self.assertEqual(meta["count"], 3)
        stored = s3.puts["data/search/providers/gdelt.json.gz"]
        payload = json.loads(gzip.decompress(stored["Body"]))
        asset = next(row for row in payload["rows"] if row[2] == "asset")
        self.assertRegex(asset[0], r"^gdelt:asset:[0-9a-f]{16}$")
        self.assertEqual(
            asset[3],
            "data/warm/gdelt/events/2026/09/02/world-events.json.gz",
        )
        self.assertEqual(asset[1], "world events")
        indexed = db.execute(
            "SELECT provider,title,key FROM docs WHERE docs MATCH "
            "'\"world\"* AND \"events\"*'").fetchone()
        self.assertEqual(
            indexed,
            ("gdelt", "world events",
             "data/warm/gdelt/events/2026/09/02/world-events.json.gz"),
        )
        db.close()

    def test_raw_asset_path_is_searchable_but_not_chartable(self):
        symdir = load_lambda("symdir_under_test", SYMDIR, FakeS3())
        asset = symdir.doc(
            "gdelt:asset:0123456789abcdef",
            "gdelt",
            "world events",
            "dataset",
            0.08,
            key="data/warm/gdelt/events/2026/09/02/world-events.json.gz",
            extra={
                "key": "data/warm/gdelt/events/2026/09/02/world-events.json.gz",
                "raw": True,
                "chartable": False,
                "provider_name": "GDELT",
            },
        )
        provider = symdir.doc(
            "provider:gdelt",
            "gdelt",
            "GDELT",
            "dataset",
            0.55,
            extra={
                "browse_provider": "gdelt",
                "chartable": False,
                "provider_name": "GDELT",
            },
        )
        docs = [provider, asset]
        post = defaultdict(lambda: array("I"))
        for i, item in enumerate(docs):
            for token in symdir.doc_tokens(item):
                post[token].append(i)
        symdir._IDX.update({
            "docs": docs,
            "pop": array("f", (item[symdir.D_POP] for item in docs)),
            "index": dict(post),
            "toklist": sorted(post),
            "ids": sorted((item[symdir.D_ID].upper(), i)
                          for i, item in enumerate(docs)),
            "bare": sorted((item[symdir.D_ID].rsplit(":", 1)[-1].upper(), i)
                           for i, item in enumerate(docs)
                           if ":" in item[symdir.D_ID]),
            "built_at": "test",
        })

        result = symdir.search("world events", 10)
        self.assertEqual(result["rows"][0]["id"], asset[symdir.D_ID])
        self.assertEqual(result["rows"][0]["provider_name"], "GDELT")
        self.assertTrue(result["rows"][0]["raw"])
        self.assertFalse(result["rows"][0]["chartable"])
        self.assertIn("gdelt", {facet["provider"]
                               for facet in result["facets"]})

    def test_warehouse_fts_returns_provider_and_raw_file_contracts(self):
        symdir = load_lambda("symdir_fts_under_test", SYMDIR, FakeS3())
        fd, path = tempfile.mkstemp(suffix=".sqlite")
        os.close(fd)
        try:
            con = sqlite3.connect(path)
            con.execute(
                "CREATE VIRTUAL TABLE docs USING fts5("
                "id UNINDEXED,provider,provider_name UNINDEXED,title,key,"
                "kind UNINDEXED,nbytes UNINDEXED,age_h UNINDEXED,hot UNINDEXED)")
            con.executemany(
                "INSERT INTO docs VALUES(?,?,?,?,?,?,?,?,?)",
                [
                    ("provider:gdelt", "gdelt", "GDELT", "GDELT", "",
                     "provider", None, None, 1),
                    ("gdelt:asset:0123456789abcdef", "gdelt", "GDELT",
                     "world events",
                     "data/warm/gdelt/events/world-events.json.gz",
                     "asset", 1234, 1.5, 1),
                ],
            )
            con.commit()
            con.close()
            symdir._warehouse_db = lambda force=False: path

            result = symdir.warehouse_search("world events", 10)
            self.assertEqual(len(result["rows"]), 1)
            row = result["rows"][0]
            self.assertEqual(row["provider_name"], "GDELT")
            self.assertEqual(row["catalog_kind"], "asset")
            self.assertTrue(row["raw"])
            self.assertFalse(row["chartable"])
            self.assertEqual(
                row["key"],
                "data/warm/gdelt/events/world-events.json.gz",
            )
        finally:
            os.remove(path)

    def test_native_series_wins_over_same_id_catalog_reference(self):
        symdir = load_lambda("symdir_merge_under_test", SYMDIR, FakeS3())
        native = symdir.doc(
            "fred:DGS10", "fred", "10-Year Treasury Rate", "series", 1.0)
        post = defaultdict(lambda: array("I"))
        for token in symdir.doc_tokens(native):
            post[token].append(0)
        symdir._IDX.update({
            "docs": [native],
            "pop": array("f", [1.0]),
            "index": dict(post),
            "toklist": sorted(post),
            "ids": [("FRED:DGS10", 0)],
            "bare": [("DGS10", 0)],
            "built_at": "test",
        })
        symdir.warehouse_search = lambda *args, **kwargs: {
            "rows": [{
                "id": "fred:DGS10", "provider": "fred",
                "provider_name": "FRED", "kind": "dataset",
                "chartable": False, "raw": True,
            }],
            "more": False,
            "facets": [{
                "provider": "fred", "provider_name": "FRED", "n": 1,
            }],
        }
        result = symdir.search("DGS10", 10)
        match = next(row for row in result["rows"]
                     if row["id"] == "fred:DGS10")
        self.assertEqual(match["kind"], "series")
        self.assertTrue(match["chartable"])
        fred_facet = next(
            facet for facet in result["facets"]
            if facet["provider"] == "fred")
        self.assertEqual(fred_facet["n"], 1)

    def test_provider_explorer_preserves_selected_raw_key(self):
        symdir = load_lambda("symdir_explorer_under_test", SYMDIR, FakeS3())
        selected = {
            "id": "gdelt:asset:0123456789abcdef",
            "provider": "gdelt",
            "provider_name": "GDELT",
            "kind": "dataset",
            "chartable": False,
            "raw": True,
            "key": "data/warm/gdelt/events/world-events.json.gz",
        }
        symdir.search = lambda *args, **kwargs: {
            "rows": [selected], "total": 1, "warehouse_more": False,
        }
        symdir.hub = lambda: {
            "providers": [{
                "slug": "gdelt", "name": "GDELT", "datasets": 1,
            }],
        }
        result = symdir.explorer({
            "provider": "gdelt",
            "q": selected["key"],
            "offset": "0",
            "limit": "20",
        })
        self.assertEqual(result["rows"][0]["key"], selected["key"])
        self.assertTrue(result["rows"][0]["raw"])


if __name__ == "__main__":
    unittest.main()
