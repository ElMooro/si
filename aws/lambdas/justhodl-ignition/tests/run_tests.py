"""justhodl-ignition tests: (1) the off-exchange consumer suite (tests/test_offexchange_consumers.py - load_dark stays
the abstaining reader); (2) v1.2.0 bank-fusion offline tests: profile from the factor book first, the insider pillar
from the banked whole-market feed, the squeeze block per rank, the old path when the banks are absent.  No network, no
AWS, no key.

Run: python3 aws/lambdas/justhodl-ignition/tests/run_tests.py
"""
from pathlib import Path
import io
import json
import os
import sys
import types
import unittest
from datetime import datetime, timedelta, timezone

ROOT = Path(__file__).resolve().parents[4]
HERE = Path(__file__).resolve().parent
sys.path[:0] = [str(ROOT / 'tests'), str(ROOT / 'aws/shared'), str(HERE.parent / 'source')]
os.environ["FMP_KEY"] = "test-key-not-real"
os.environ["POLYGON_KEY"] = "x"


class FakeBook(types.ModuleType):
    rows, quotes, on = {}, {}, True

    def available(self):
        return self.on

    def as_of(self):
        return {"tape": "2026-10-24", "fmp-bank": "2026-10-25"} if self.on else None

    def quote(self, symbol):
        q = self.quotes.get(str(symbol).upper())
        return None if q is None else dict(q, symbol=str(symbol).upper(), as_of=self.as_of(), source="factor-book (bank close, not a live tick)")

    def factor(self, symbol):
        q = self.quotes.get(str(symbol).upper())
        return None if q is None else {"identity": {"symbol": str(symbol).upper(), "name": "Name of " + str(symbol).upper()}}

    def squeeze(self, symbol):
        r = self.rows.get(str(symbol).upper())
        return None if r is None else dict(r, symbol=str(symbol).upper(), as_of=self.as_of(), source="factor-book (test)")


class FakeBank(types.ModuleType):
    days = []

    def walker_days(self, dataset, days=30, end=None, where=None):
        assert dataset == "insider-trading-latest"
        return [(d, [r for r in rows if where is None or where(r)]) for d, rows in self.days]      # mirrors the real reader: filter on read


class S3Stub:
    def __init__(self, docs):
        self.docs, self.written = docs, {}

    def get_object(self, Bucket, Key):
        if Key in self.docs:
            return {"Body": io.BytesIO(json.dumps(self.docs[Key]).encode())}
        raise KeyError(Key)

    def put_object(self, **k):
        self.written[k["Key"]] = json.loads(k["Body"])


UNIVERSE = ["T%02d" % i for i in range(20)]


def boot(book_on=True, bank_days=None):
    import boto3
    for name in ("fmp_bankfirst", "polygon_bankfirst"):
        sys.modules[name] = types.ModuleType(name)
        sys.modules[name].install = lambda: None
    ms = types.ModuleType("managed_secret")
    ms.managed_secret = lambda *a, **k: "x"
    sys.modules["managed_secret"] = ms
    book = FakeBook("bank_enrich")
    book.on = book_on
    sys.modules["bank_enrich"] = book
    bank = FakeBank("fmp_bank")
    bank.days = bank_days or []
    sys.modules["fmp_bank"] = bank
    s3 = S3Stub({"data/master-ranker.json": {"tickers": UNIVERSE}})
    boto3.client = lambda *a, **k: s3
    boto3.resource = lambda *a, **k: types.SimpleNamespace(Table=lambda *a, **k: (_ for _ in ()).throw(RuntimeError("no ddb in tests")))
    sys.modules.pop("lambda_function", None)
    import lambda_function as lf
    lf._book, lf._bank, lf.S3 = book, bank, s3
    lf.DDB = types.SimpleNamespace(Table=lambda *a, **k: (_ for _ in ()).throw(RuntimeError("no ddb in tests")))
    lf.load_ftd = lambda: (None, [])
    # two synthetic pillars (OBV divergence from the tape, EDGAR cadence) so names clear the engine's 35% coverage floor
    lf.load_cik_map = lambda: {t: "%010d" % i for i, t in enumerate(UNIVERSE)}
    lf.edgar_profile = lambda t, cik: {"n30": int(t[1:]) % 5, "base30": 2.0, "cadence_ratio": (int(t[1:]) % 5) / 2.0, "shelf_active": False}
    lf.obv_divergence = lambda t: {"obv_slope_n": (int(t[1:]) % 7) * 0.1, "px_slope_n": 0.05, "divergence": (int(t[1:]) % 7) * 0.1 - 0.05, "px": 10.0 + int(t[1:]), "adv30": 1e6}
    return lf, book, bank, s3


def test_book_profile_banked_insider_feed_and_squeeze_context():
    today = datetime.now(timezone.utc).date()
    feed = []
    for i in range(6):                                   # six insiders bought T00 across three banked days; one bought T01
        feed.append(((today - timedelta(days=1 + i % 3)).isoformat(), [{"symbol": "T00", "transactionType": "P-Purchase", "reportingName": "Insider %d" % i}]))
    feed.append(((today - timedelta(days=2)).isoformat(), [{"symbol": "T01", "transactionType": "P-Purchase", "reportingName": "Solo"}, {"symbol": "T02", "transactionType": "S-Sale", "reportingName": "Seller"}]))
    lf, book, bank, s3 = boot(bank_days=feed)
    book.quotes = {t: {"marketCap": 1e9 + i * 1e8, "sector": "Technology", "name": None} for i, t in enumerate(UNIVERSE[:18])}   # T18/T19 not in the book
    book.rows = {"T00": {"short_pct_float": 18.0, "days_to_cover": 4.2, "float_shares": 50e6, "short_volume_ratio": 0.5, "mcap": 1e9, "avg_volume": 1e6, "close": 10.0, "ret_21d": 0.0, "ret_63d": 0.1, "from_52w_high_pct": -8.0, "composite_squeeze": 80.0}}
    calls = []

    def fake_fmp(path, params):
        calls.append(path)
        return [{"companyName": "Live Co", "marketCap": 5e8, "sector": "Energy"}] if path == "profile" else None
    lf.fmp = fake_fmp
    r = lf.lambda_handler({}, None)
    assert r["statusCode"] == 200, r
    out = s3.written[lf.OUT_KEY]
    assert out["version"] == "1.2.0" and out["universe_n"] == 20 and out["book_as_of"]["tape"] == "2026-10-24"
    assert out["insider_feed"] == "fmp-walker bank (7 days)" and out["probes"]["insider_fmp"] is False and out["probes"]["factor_book"] is True   # 7 (day, rows) entries in the fixture
    assert "insider-trading/search" not in calls, "the banked feed must replace every live insider call"
    assert calls.count("profile") == 2, calls                       # only the two names the book lacks reach the live profile
    by = {x["ticker"]: x for x in out["ranks"]}
    assert by["T00"]["profile_source"] == "factor-book" and by["T00"]["name"] == "Name of T00" and by["T00"]["mkt_cap"] == 1e9 and by["T00"]["sector"] == "Technology"
    assert by["T00"]["insider_buys45"] == 6 and by["T00"]["insider_buyers45"] == 6 and by["T00"]["insider_source"].startswith("fmp-walker bank")
    assert by["T01"]["insider_buys45"] == 1 and by["T02"]["insider_buys45"] == 0
    assert by["T00"]["squeeze"]["short_pct_float"] == 18.0 and by["T01"]["squeeze"] is None
    assert by["T18"]["profile_source"] == "fmp-live" and by["T18"]["name"] == "Live Co" and by["T18"]["sector"] == "Energy"
    assert out["pillar_availability"]["insider"] is True and by["T00"]["z_insider"] > 1.0 and "insider" in by["T00"]["pillars_hit"]
    assert out["factor_book"].startswith("profile + squeeze context from the bank close on 18 of 20")
    assert set(out["methodology"][:40]) and lf.WEIGHTS["insider"] == 0.15, "weights unchanged"


def test_banks_absent_keep_the_old_live_path_and_say_so():
    lf, book, bank, s3 = boot(book_on=False, bank_days=[])
    calls = []

    def fake_fmp(path, params):
        calls.append((path, params.get("symbol")))
        if path == "profile":
            return [{"companyName": "Live Co", "marketCap": 5e8, "sector": "Energy"}]
        if path == "insider-trading/search":
            n = (int(params["symbol"][1:]) % 4) if params["symbol"].startswith("T") else 1
            return [{"transactionDate": datetime.now(timezone.utc).date().isoformat(), "transactionType": "P-Purchase", "reportingName": "A%d" % i} for i in range(n)]
        return None
    lf.fmp = fake_fmp
    lf.lambda_handler({}, None)
    out = s3.written[lf.OUT_KEY]
    assert out["factor_book"] == "factor book unavailable" and out["book_as_of"] is None and out["insider_feed"] == "fmp-live"
    assert out["probes"]["insider_fmp"] is True and out["probes"]["insider_bank"] is False
    assert sum(1 for p, _ in calls if p == "insider-trading/search") == 21     # the NVDA probe + one per name, as before
    row = out["ranks"][0]
    assert row["profile_source"] == "fmp-live" and row["insider_source"] == "fmp-live" and "squeeze" not in row


if __name__ == '__main__':
    suite = unittest.defaultTestLoader.discover(str(ROOT / 'tests'), pattern='test_offexchange_consumers.py')
    ok = unittest.TextTestRunner(verbosity=1).run(suite).wasSuccessful()
    if not ok:
        sys.exit(1)
    test_book_profile_banked_insider_feed_and_squeeze_context()
    print("ok test_book_profile_banked_insider_feed_and_squeeze_context")
    test_banks_absent_keep_the_old_live_path_and_say_so()
    print("ok test_banks_absent_keep_the_old_live_path_and_say_so")
    print("2 tests passed (+ the off-exchange consumer suite)")
