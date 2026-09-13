"""Real internals build with fake S3 and forbidden vendor HTTP."""
import copy
from datetime import datetime, timedelta, timezone
import importlib.util
import io
import json
from pathlib import Path
import sys
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[4]
SOURCE = Path(__file__).resolve().parents[1] / "source"
sys.path.insert(0, str(SOURCE))
import internals_warehouse as iw
import compile_jh_internals as compiler


class FakeS3:
    def __init__(self):
        self.stamp = datetime.now(timezone.utc)-timedelta(hours=2)
        self.rows = {
            iw.KEY: {"schema_version":1,"fields":{"twos_tens":.39,"liq_proxy_bn":5852.,"nfci":-.564,"custom":42},"unrelated":{"keep":True}},
            "data/fred-cache.json": {sid:{"observations":[{"date":"2026-09-04","value":v}]} for sid,v in
                {"DGS10":4.95,"DGS2":4.56,"WALCL":6740619,"WTREGEN":883335,"RRPONTSYD":5.255,"NFCI":-.564}.items()},
            "data/finviz-universe.json": {"n_tickers":1000,"by_ticker":{str(i):{"price":2 if i<700 else 1,"prev_close":1 if i<700 else 2} for i in range(1000)},"generated_at":self.stamp.isoformat()}}
        self.puts = []
    def get_object(self, Bucket, Key):
        return {"Body":io.BytesIO(json.dumps(self.rows[Key]).encode()),"LastModified":self.stamp,"ETag":"existing"}
    def put_object(self, **kw):
        assert kw["IfMatch"] == "existing"
        self.puts.append(kw)
        self.rows[kw["Key"]] = json.loads(kw["Body"])
    def get_paginator(self, *args):
        raise AssertionError("Fresh cache must not traverse scoped banks")


class InternalsTests(unittest.TestCase):
    def test_fresh_warehouse_no_http_and_uncapped_merge(self):
        s3 = FakeS3()
        with patch("urllib.request.urlopen", side_effect=AssertionError("vendor HTTP forbidden")):
            out, _ = iw.build(s3)
            receipt = iw.run(s3)
        self.assertTrue(receipt["ok"])
        self.assertEqual(out["fields"], {"twos_tens":.39,"liq_proxy_bn":5852.,"nfci":-.564,"custom":42,
                          "ad_breadth":.4,"n_up":700,"n_down":300,"n_univ":1000,"n_missing":0})
        self.assertEqual(out["unrelated"], {"keep":True})
        self.assertEqual(out["warehouse"]["fresh_fred_legs"],6)
        self.assertEqual(out["warehouse"]["fred_http_requests"],0)
        self.assertTrue(all(x["fresh_36h"] for x in out["warehouse"]["fred"].values()))
    def test_coverage_denominator_includes_unchanged_and_missing(self):
        pairs = [(2,1)]*3 + [(1,2)]*2 + [(1,1)]*3 + [(None,1)]*2
        self.assertTrue(iw.count_pairs(pairs)["eligible"])
        self.assertEqual(iw.count_pairs(pairs)["n_univ"],8)
        self.assertFalse(iw.count_pairs(pairs+[(None,1)])["eligible"])
    def test_low_coverage_preserves_prior_publication(self):
        s3 = FakeS3(); s3.rows[iw.KEY]["fields"]["ad_breadth"] = .3238
        before = copy.deepcopy(s3.rows[iw.KEY])
        for row in list(s3.rows["data/finviz-universe.json"]["by_ticker"].values())[:201]:
            row["prev_close"] = None
        with self.assertRaisesRegex(ValueError,"preserving existing"):
            iw.run(s3)
        self.assertEqual(s3.rows[iw.KEY],before)
        self.assertEqual(s3.puts,[])
    def test_compute_matches_canonical_script(self):
        spec = importlib.util.spec_from_file_location("canonical",ROOT/"scripts/compile_jh_internals.py")
        canonical = importlib.util.module_from_spec(spec); spec.loader.exec_module(canonical)
        legs = dict(dgs10=4.95,dgs2=4.56,walcl=6740619,tga=883335,rrp=5255,nfci=-.564,n_up=7421,n_down=3659,n_univ=11618)
        self.assertEqual(compiler.compute(legs)["fields"],canonical.compute(legs)["fields"])
        self.assertEqual(compiler.FRED_LEGS,canonical.FRED_LEGS)


if __name__ == "__main__":
    unittest.main()
