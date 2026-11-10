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
    def test_katlin_crypto_bank_is_served_without_vendor(self):
        s3=Mock()
        def read(**kw):
            self.assertEqual(kw["Key"],"data/warm/katlin/crypto-bars/BTC.json.gz")
            return {"Body":io.BytesIO(json.dumps({"rows":[["2026-09-11",100,110,95,105,500]]}).encode()),"LastModified":datetime.now(timezone.utc)}
        s3.get_object.side_effect=read
        for symbol in ("X:BTCUSD","BTC-USD"):
            result=warehouse_routing.banked_ohlc(s3,"bucket",symbol)
            self.assertEqual(result["bars"][0][4],105)
            self.assertEqual(result["vendor_requests"],0)
    def test_forming_day_bar_is_appended_from_the_today_overlay(self):
        """2026-11-10 (Khalid: SPY delayed 24 h): a daily read of a US name carries today's forming bar from
        data/warm/polygon-full/today.json.gz when the bank's last session is older; never twice, never on intraday spans,
        never for crypto / FX, and the bank is served as banked when the overlay is missing."""
        import gzip as _gz
        class Missing(Exception):
            response={"Error":{"Code":"NoSuchKey"}}
        bank_key="data/warm/tv-bars/universe/US__SPY.json.gz"
        overlay={"session":"2026-11-10","t":1794321000,"as_of":"2026-11-10T14:37:00+00:00","delay_minutes":15,
                 "rows":{"SPY":[747.94,749.16,747.05,749.115,3594789,748.4,1794321420000],"AAPL":[351.35,353.87,350,350.67,2670219,351.1,1794321420000]}}
        def make(bank_last, with_overlay=True):
            s3=Mock()
            def read(**kw):
                if kw["Key"]==bank_key:
                    return {"Body":io.BytesIO(json.dumps({"bars":[[1794234600,750,753,749,752.69,60000000]] if bank_last=="2026-11-09" else [[1794321000,748,749,747,749.0,1000000]],"span":"day"}).encode()),"LastModified":datetime.now(timezone.utc)}
                if kw["Key"]==warehouse_routing.TODAY_KEY and with_overlay:
                    return {"Body":io.BytesIO(_gz.compress(json.dumps(overlay).encode())),"LastModified":datetime.now(timezone.utc)}
                raise Missing()
            s3.get_object.side_effect=read
            return s3
        warehouse_routing._TODAY.update({"at":0.0,"doc":None})
        doc=warehouse_routing.banked_ohlc(make("2026-11-09"),"bucket","SPY")
        self.assertEqual(len(doc["bars"]),2)
        self.assertEqual(doc["bars"][-1],[1794321000,747.94,749.16,747.05,749.115,3594789.0])
        self.assertEqual(doc["bar_sources"][-1],"polygon-snapshot:forming-day (15-minute delayed)")
        self.assertEqual(doc["forming_bar"]["session"],"2026-11-10")
        self.assertEqual(doc["forming_bar"]["updated_ms"],1794321420000)
        self.assertEqual(doc["vendor_requests"],0)
        # the overlay is read once a minute, not per request
        s3=make("2026-11-09"); warehouse_routing.banked_ohlc(s3,"bucket","SPY"); warehouse_routing.banked_ohlc(s3,"bucket","SPY")
        self.assertEqual(sum(1 for c in s3.get_object.call_args_list if c.kwargs["Key"]==warehouse_routing.TODAY_KEY),0)
        # the bank already holds the session (after the nightly append): nothing appended
        warehouse_routing._TODAY.update({"at":0.0,"doc":None})
        doc=warehouse_routing.banked_ohlc(make("2026-11-10"),"bucket","SPY")
        self.assertEqual(len(doc["bars"]),1); self.assertNotIn("forming_bar",doc)
        # a weekly / monthly read gets the daily source WITH the forming bar (the Worker folds it into the current week); an intraday read stays empty
        warehouse_routing._TODAY.update({"at":0.0,"doc":None})
        self.assertEqual(len(warehouse_routing.banked_ohlc(make("2026-11-09"),"bucket","SPY","week")["bars"]),2)
        self.assertEqual(len(warehouse_routing.banked_ohlc(make("2026-11-09"),"bucket","SPY","month")["bars"]),2)
        self.assertTrue(warehouse_routing.banked_ohlc(make("2026-11-09"),"bucket","SPY","minute")["warehouse_empty"])
        # no overlay object: served as banked
        warehouse_routing._TODAY.update({"at":0.0,"doc":None})
        doc=warehouse_routing.banked_ohlc(make("2026-11-09",with_overlay=False),"bucket","SPY")
        self.assertEqual(len(doc["bars"]),1); self.assertNotIn("forming_bar",doc)
        # pure helper: crypto / FX / index symbols never take a US overlay row
        self.assertIsNone(warehouse_routing.us_ticker("BTC-USD",["X:BTCUSD"]))
        self.assertIsNone(warehouse_routing.us_ticker("EURUSD=X",["C:EURUSD"]))
        self.assertIsNone(warehouse_routing.us_ticker("^VIX",["I:VIX","TVC:VIX"]))
        self.assertEqual(warehouse_routing.us_ticker("NASDAQ:AAPL",["NASDAQ:AAPL"]),"AAPL")
        self.assertEqual(warehouse_routing.us_ticker("SPY",warehouse_routing._banks_for("SPY")),"SPY")
        warehouse_routing._TODAY.update({"at":0.0,"doc":None})


if __name__ == "__main__":
    unittest.main()
