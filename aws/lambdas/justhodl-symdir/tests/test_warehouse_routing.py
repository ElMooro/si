import io
import json
from datetime import datetime, timezone
from pathlib import Path
import sys
from unittest.mock import Mock, patch
import unittest
from test_universal_provider_search import load_lambda, SYMDIR, FakeS3

sys.path.insert(0,str(Path(__file__).resolve().parents[1]/"source"))
import warehouse_routing


class WarehouseTests(unittest.TestCase):
    def module(self):
        m = load_lambda("symdir_warehouse_test",SYMDIR,FakeS3())
        m._get_json = Mock(return_value=None)
        m._cache_get = Mock(return_value=(None,None))
        m._tv_pull = Mock(side_effect=AssertionError("vendor bank forbidden"))
        guard = patch.object(warehouse_routing,"banked_ohlc",return_value={"warehouse_empty":True})
        guard.start(); self.addCleanup(guard.stop)
        return m
    def test_stale_bank_served_for_equity_and_tv(self):
        m = self.module()
        m._tv_bank_doc = Mock(return_value={"bars":[[946684800,1,2,.5,1.5,100]],"last_date":"2000-01-01"})
        for call in (lambda:m.r_equity("AAPL","AAPL",None),lambda:m.r_tvsym("TVC:VIX","TVC:VIX",None)):
            self.assertEqual(call()["n"],1)
        m._tv_pull.assert_not_called()
    def test_expired_materialized_bars_avoid_vendor(self):
        m = self.module(); m._tv_bank_doc = Mock(return_value=None)
        bank = {"ohlc":[["2000-01-01",1,2,.5,1.5,100]],"n":1}
        m._cache_get = Mock(return_value=(bank,10000000))
        self.assertEqual(m.r_equity("AAPL","AAPL",None),bank)
        m._tv_pull.assert_not_called()
    def test_resolved_failure_never_falls_back_to_tv(self):
        m = self.module(); m._tv_bank_doc = Mock(return_value=None)
        docs = {"data/tv-symbol-resolver.json":{"exact":{"TVC:US10Y":{"id":"FRED:DGS10","engine":"fred"}}},"data/symbol-map.json":{"map":{}}}
        m._get_json = Mock(side_effect=lambda key:docs.get(key))
        m._fetch_series = Mock(side_effect=ValueError("known series unavailable"))
        with self.assertRaisesRegex(ValueError,"known series unavailable"):
            m.r_tvsym("TVC:US10Y","TVC:US10Y",None)
        m._tv_pull.assert_not_called()
        m._fetch_series.assert_called_once_with("fred:DGS10")
    def test_fresh_object_with_old_observation_avoids_fred_http(self):
        m = self.module(); m.FRED_KEY = "fixture"
        m.s3 = Mock()
        m.s3.head_object.return_value = {"LastModified":datetime.now(timezone.utc)}
        m.fred_get = Mock(side_effect=AssertionError("FRED HTTP forbidden"))
        doc = {"observations":[{"date":"2000-01-01","value":"1"}]}
        self.assertEqual(m._fred_tail_refresh("Interest_Rates","DGS10",doc,"D",force=True),(doc,0))
        m.fred_get.assert_not_called()

    def test_authenticated_missing_is_distinct_from_access_denied(self):
        class S3Error(Exception):
            def __init__(self,code): self.response={"Error":{"Code":code}}
        missing=Mock(); missing.get_object.side_effect=S3Error("NoSuchKey")
        self.assertTrue(warehouse_routing.banked_ohlc(missing,"bucket","AAPL")["warehouse_empty"])
        denied=Mock(); denied.get_object.side_effect=S3Error("AccessDenied")
        with self.assertRaises(S3Error):
            warehouse_routing.banked_ohlc(denied,"bucket","AAPL")
    def test_banked_ohlc_normalizes_iso_dates_and_checks_span(self):
        class Missing(Exception):
            response={"Error":{"Code":"NoSuchKey"}}
        s3=Mock()
        def read(**kw):
            if kw["Key"] != "data/warm/tv-bars/universe/NASDAQ__AAPL.json.gz": raise Missing()
            return {"Body":io.BytesIO(json.dumps({"bars":[["2000-01-01",1,2,.5,1.5,100]]}).encode()),"LastModified":datetime.now(timezone.utc)}
        s3.get_object.side_effect=read
        doc=warehouse_routing.banked_ohlc(s3,"bucket","AAPL")
        self.assertEqual(doc["bars"][0][0],946684800)
        self.assertIn("NASDAQ__AAPL",doc["warehouse_key"])
        self.assertTrue(warehouse_routing.banked_ohlc(s3,"bucket","AAPL","minute")["warehouse_empty"])


if __name__ == "__main__":
    unittest.main()
