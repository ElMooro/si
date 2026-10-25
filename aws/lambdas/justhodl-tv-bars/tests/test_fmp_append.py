"""mode=fmp-append (v1.3): non-tape chart names advance from the FMP warehouse only when the FMP spelling is proven by
list membership and the first new bar is continuous with the bank; nothing reaches a provider; nothing is guessed."""
import gzip
import importlib.util
import json
import sys
import types
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import Mock, patch

SOURCE = Path(__file__).resolve().parents[1] / "source" / "lambda_function.py"
SHARED = Path(__file__).resolve().parents[3] / "shared"
sys.path.insert(0, str(SHARED))


class S3:
    def __init__(self):
        self.store = {}
        self.puts = []

    def get_object(self, Bucket, Key):
        if Key not in self.store:
            class Missing(Exception):
                response = {"Error": {"Code": "NoSuchKey"}}
            raise Missing(Key)
        return {"Body": __import__("io").BytesIO(self.store[Key])}

    def put_object(self, Bucket, Key, Body, **kw):
        self.store[Key] = Body
        self.puts.append(Key)


def load(s3):
    spec = importlib.util.spec_from_file_location("tvbars_fmp_append_test", SOURCE)
    module = importlib.util.module_from_spec(spec)
    with patch.dict(sys.modules, {"boto3": types.SimpleNamespace(client=lambda *a, **kw: s3)}):
        spec.loader.exec_module(module)
    module.s3 = s3
    return module


def ts_utc(y, m, d, hh=0, mm=0):
    return int(datetime(y, m, d, hh, mm, tzinfo=timezone.utc).timestamp())


def bank_doc(bars):
    return {"symbol": "x", "tv_symbol": "x", "bars": bars, "bar_sources": ["tradingview-ws (session-auth, server-side)"] * len(bars),
            "market_history_quality": {"contract": "market-bank-custody.v1"}, "n": len(bars), "first_date": "2026-09-01", "last_date": "2026-10-02"}


class FmpAppendTests(unittest.TestCase):
    def setUp(self):
        self.s3 = S3()
        self.m = load(self.s3)
        # the chart bank: DAX (XETR, bars at 08:00 UTC), ^TNX (US namespace, 09:30 ET = 13:30 UTC in October), a BTC name, an unmapped future
        self.keys = {"XETR:DAX": "data/warm/tv-bars/universe/XETR__DAX.json.gz", "US:^TNX": "data/warm/tv-bars/universe/US__^TNX.json.gz",
                     "X:BTCUSD": "data/warm/tv-bars/universe/X__BTCUSD.json.gz", "CME_MINI:ES1!": "data/warm/tv-bars/universe/CME_MINI__ES1!.json.gz"}
        docs = {"XETR:DAX": bank_doc([[ts_utc(2026, 10, 1, 8), 19000, 19100, 18900, 19050, 0], [ts_utc(2026, 10, 2, 8), 19050, 19200, 19000, 19150, 0]]),
                "US:^TNX": bank_doc([[ts_utc(2026, 10, 1, 13, 30), 4.1, 4.2, 4.0, 4.15, None], [ts_utc(2026, 10, 2, 13, 30), 4.15, 4.25, 4.1, 4.2, None]]),
                "X:BTCUSD": bank_doc([[ts_utc(2026, 10, 3, 0), 60000, 61000, 59000, 60500, 10.0], [ts_utc(2026, 10, 4, 0), 60500, 61500, 60000, 61000, 11.0]]),
                "CME_MINI:ES1!": bank_doc([[ts_utc(2026, 10, 2, 22), 5700, 5720, 5690, 5710, 100]])}
        for sym, key in self.keys.items():
            self.s3.store[key] = gzip.compress(json.dumps(docs[sym]).encode())
        self.s3.store["data/warm/tv-bars/universe/_index.json"] = json.dumps({"symbols": {sym: {"key": key, "last": docs[sym]["last_date"]} for sym, key in self.keys.items()}, "failures": {}}).encode()
        # the FMP warehouse (walker bank): lists + histories.  ^TNX quoted x10 by this fake -> must be refused, never written.
        self.fmp_lists = {"index-list": [{"symbol": "^GDAXI"}, {"symbol": "^TNX"}], "forex-list": [{"symbol": "EURUSD"}], "cryptocurrency-list": [{"symbol": "BTCUSD"}], "stock-list": [{"symbol": "0700.HK"}]}
        weekdays = [d for d in range(1, 24) if datetime(2026, 10, d).weekday() < 5]       # FMP's files hold sessions only
        self.fmp_hist = {
            "^GDAXI": [{"date": "2026-10-%02d" % d, "open": 19100 + d, "high": 19300 + d, "low": 19000, "close": 19200 + d, "volume": 0} for d in weekdays],
            "^TNX": [{"date": "2026-10-%02d" % d, "open": 41.0, "high": 42.5, "low": 40.5, "close": 42.0, "volume": 0} for d in weekdays],
            "BTCUSD": [{"date": "2026-10-%02d" % d, "open": 61000, "high": 62000, "low": 60000, "close": 61500 + d, "volume": 12.5} for d in range(1, 24)],
        }
        self.live_calls = []

    def fake_fmp_bank(self):
        tests = self

        class FB:
            @staticmethod
            def walker(dataset, entity=None, period=None, day=None, exchange=None):
                if entity is None:
                    return tests.fmp_lists.get(dataset)
                return tests.fmp_hist.get(entity)
        return FB()

    def run_mode(self, through="2026-10-23"):
        with patch.dict(sys.modules, {"fmp_bank": self.fake_fmp_bank()}), patch("urllib.request.urlopen", side_effect=AssertionError("fmp-append must never call a provider")):
            return self.m.lambda_handler({"mode": "fmp-append", "through": through}, None)

    def test_proven_names_advance_from_the_bank_with_their_own_stamps(self):
        out = self.run_mode()
        self.assertEqual(out["mode"], "fmp-append")
        self.assertIn("appended", out["verdicts"]["XETR:DAX"])
        dax = json.loads(gzip.decompress(self.s3.store[self.keys["XETR:DAX"]]))
        new = [b for b, src in zip(dax["bars"], dax["bar_sources"]) if src == "fmp-eod-full:bank"]
        self.assertEqual(len(new), 15)                                    # Oct 5 .. Oct 23 weekdays (Oct 3/4 weekend), none on or before Oct 2
        self.assertEqual(datetime.fromtimestamp(new[0][0], tz=timezone.utc).isoformat(), "2026-10-05T08:00:00+00:00")   # inherits the series' 08:00 UTC
        self.assertEqual(dax["last_date"], "2026-10-23")
        self.assertEqual(dax["bar_sources"][:2], ["tradingview-ws (session-auth, server-side)"] * 2)   # legacy rows keep their label
        self.assertEqual(new[0][4], 19205.0)
        # BTC: 24/7 series, every calendar day after Oct 4 through the 23rd
        btc = json.loads(gzip.decompress(self.s3.store[self.keys["X:BTCUSD"]]))
        self.assertEqual(btc["last_date"], "2026-10-23")
        self.assertEqual(sum(1 for src in btc["bar_sources"] if src == "fmp-eod-full:bank"), 19)
        idx = json.loads(self.s3.store["data/warm/tv-bars/universe/_index.json"])
        self.assertEqual(idx["symbols"]["XETR:DAX"]["last"], "2026-10-23")
        self.assertEqual(idx["fmp_append"]["appended_symbols"], 2)

    def test_scale_mismatch_is_refused_and_recorded_never_written(self):
        before = self.s3.store[self.keys["US:^TNX"]]
        out = self.run_mode()
        self.assertTrue(out["verdicts"]["US:^TNX"].startswith("series_or_scale_mismatch"), out["verdicts"]["US:^TNX"])
        self.assertEqual(self.s3.store[self.keys["US:^TNX"]], before)
        idx = json.loads(self.s3.store["data/warm/tv-bars/universe/_index.json"])
        self.assertIn("US:^TNX", idx["failures"])
        self.assertTrue(idx["failures"]["US:^TNX"]["err"].startswith("fmp-append: series_or_scale_mismatch"))
        self.assertEqual(out["failed"], 1)

    def test_unmapped_and_unproven_names_are_left_alone(self):
        self.fmp_lists["index-list"] = [{"symbol": "^TNX"}]              # ^GDAXI no longer proven by the banked list
        before = self.s3.store[self.keys["XETR:DAX"]]
        out = self.run_mode()
        self.assertTrue(out["verdicts"]["XETR:DAX"].startswith("fmp_symbol_not_in_index-list"))
        self.assertEqual(self.s3.store[self.keys["XETR:DAX"]], before)
        self.assertNotIn("CME_MINI:ES1!", out["verdicts"])                  # not in FMP_MAP: never touched
        self.assertEqual(out["unproven"], 1)

    def test_bank_without_newer_sessions_waits(self):
        self.fmp_hist["^GDAXI"] = [r for r in self.fmp_hist["^GDAXI"] if r["date"] <= "2026-10-02"]
        out = self.run_mode()
        self.assertTrue(out["verdicts"]["XETR:DAX"].startswith("fmp_bank_has_no_newer_session"))
        self.assertEqual(out["fmp_bank_not_ready"], 1)
        self.assertNotIn(self.keys["XETR:DAX"], self.s3.puts)


if __name__ == "__main__":
    unittest.main(verbosity=2)
