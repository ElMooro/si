"""justhodl-analyst-consensus offline tests (v1.1 bank fusion): whole analyst-covered market from the factor book,
book-sourced rows, composite unchanged.  No network, no AWS, no key.

Run: python3 aws/lambdas/justhodl-analyst-consensus/tests/run_tests.py
"""
import json
import os
import sys
import types

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "source"))
os.environ["FMP_KEY"] = "test-key-not-real"
os.environ["AC_BOOK_CANDIDATES"] = "2"


class Stub:
    def __init__(self):
        self.store = {}

    def get_object(self, Bucket, Key):
        raise KeyError(Key)

    def put_object(self, Bucket, Key, Body, **kw):
        self.store[Key] = Body


DOCS = {
    "AAPL": {"identity": {"mcap": 3e12}, "profile": {"price": 100.0}, "returns": {"close": 100.0, "ret_1d": 0.5},
             "price_target": {"lastMonthAvgPriceTarget": 120.0, "lastMonthCount": 12}, "consensus": {"strongBuy": 10, "buy": 20, "hold": 5, "sell": 1, "strongSell": 0, "consensus": "Buy"},
             "surprises": [{"date": "2026-07-31", "actualEarningResult": 1.1, "estimatedEarning": 1.0}, {"date": "2026-04-30", "actualEarningResult": 0.9, "estimatedEarning": 1.0}, {"date": "2026-01-31", "actualEarningResult": 1.2, "estimatedEarning": 1.0}],
             "as_of": {"fmp-bank": "2026-10-25T23:25:00+00:00"}},
    "COVD": {"identity": {"mcap": 5e9}, "profile": {"price": 50.0}, "returns": {}, "price_target": {"lastMonthAvgPriceTarget": 45.0, "lastMonthCount": 4}, "consensus": {"strongBuy": 0, "buy": 1, "hold": 4, "sell": 2, "strongSell": 1, "consensus": "Hold"}, "surprises": [], "as_of": {"fmp-bank": "2026-10-25T23:25:00+00:00"}},
    "NOANALYST": {"identity": {"mcap": 8e8}, "profile": {"price": 9.0}, "returns": {}, "price_target": None, "consensus": None, "surprises": [], "as_of": {}},
}
MATRIX = [{"symbol": "AAPL", "pt_n_1m": 12, "mcap": 3e12}, {"symbol": "COVD", "pt_n_1m": 4, "mcap": 5e9}, {"symbol": "THIRD", "pt_n_1m": 3, "mcap": 4e8}, {"symbol": "TWO", "pt_n_1m": 2, "mcap": 9e9}, {"symbol": "SMALLCAP", "pt_n_1m": 6, "mcap": 1e8}]


class FakeBook(types.ModuleType):
    on = True

    def available(self):
        return self.on

    def as_of(self):
        return {"fmp-bank": "2026-10-25T23:25:00+00:00"}

    def screen(self, where, fields=None, limit=None, common_only=True):
        return [dict(r) for r in MATRIX if where(r)]

    def factor(self, symbol):
        return DOCS.get(symbol) if self.on else None


def boot():
    import boto3
    stub = Stub()
    boto3.client = lambda *a, **k: stub
    sys.modules["fmp_bankfirst"] = types.ModuleType("fmp_bankfirst")
    sys.modules["fmp_bankfirst"].install = lambda: None
    book = FakeBook("bank_enrich")
    sys.modules["bank_enrich"] = book
    sys.modules.pop("lambda_function", None)
    import lambda_function as lf
    lf.s3 = stub
    lf.UNIVERSE_TICKERS = ["AAPL", "LIVE1"]
    lf.maybe_telegram = lambda msg: None
    calls = []

    def fake_fmp_get(path, params=None, retries=2, timeout=20):
        calls.append((path, (params or {}).get("symbol")))
        if path == "quote":
            return [{"price": 10.0, "marketCap": 2e9, "changesPercentage": 1.0}]
        if path == "price-target-consensus":
            return [{"targetHigh": 15, "targetLow": 8, "targetMedian": 12, "targetMean": 12}]
        if path == "grades-consensus":
            return [{"strongBuy": 1, "buy": 2, "hold": 1, "sell": 0, "strongSell": 0, "consensus": "Buy"}]
        if path == "earnings":
            return [{"epsActual": 1.0, "epsEstimated": 0.9}]
        if path == "grades-latest-news":
            return []
        return None
    lf.fmp_get = fake_fmp_get
    return lf, stub, book, calls


def test_book_rows_cover_the_analyst_covered_market_and_live_stays_for_the_rest():
    lf, stub, book, calls = boot()
    syms, as_of = lf.book_universe()
    assert syms == ["AAPL", "COVD"], syms                     # >= 3 analysts and >= $300M, largest first, cap 2 (THIRD cut by the cap; TWO/SMALLCAP filtered)
    row = lf.fetch_one_book("AAPL")
    assert row["source"] == "factor-book" and row["price"] == 100.0 and row["target_median"] == 120.0 and row["n_targets_1m"] == 12
    assert row["rec_strong_buy"] == 10 and row["beats_8q"] == 2 and row["beat_pct_8q"] == 66.7 and row["last_surprise_pct"] == 10.0
    assert lf.fetch_one_book("NOANALYST") is None and lf.fetch_one_book("LIVE1") is None
    out = lf.lambda_handler({}, None)
    body = json.loads(out["body"])
    assert body["ok"] and body["n_universe"] == 3 and body["n_from_book"] == 2, body       # AAPL + LIVE1 curated, COVD from the book
    doc = json.loads(stub.store[lf.S3_KEY])
    assert doc["method"] == "consensus_engine_v1.1" and doc["universe_size"] == 3 and doc["universe_basis"].startswith("2 curated + 2 factor-book")
    allt = doc["all_tickers"]
    assert allt["AAPL"]["source"] == "factor-book" and allt["LIVE1"]["source"] == "live" and allt["LIVE1"]["target_median"] == 12
    assert allt["AAPL"]["components"]["upside_pct"] == 20.0 and allt["AAPL"]["components"]["total_analysts"] == 36
    assert not any(p != "grades-latest-news" and s in ("AAPL", "COVD") for p, s in calls)   # no live per-symbol calls for book names
    assert [t["ticker"] for t in doc["top_consensus_25"]] == ["LIVE1", "AAPL", "COVD"] and allt["AAPL"]["composite_score"] == 73.4   # weights unchanged: 0.3*100 + 0.25*77.1 + 0.2*50 + 0.15*50 + 0.1*66.7


def test_without_the_book_the_curated_universe_and_live_path_stand():
    lf, stub, book, calls = boot()
    book.on = False
    assert lf.book_universe() == ([], None)
    out = lf.lambda_handler({}, None)
    body = json.loads(out["body"])
    assert body["n_universe"] == 2 and body["n_from_book"] == 0
    doc = json.loads(stub.store[lf.S3_KEY])
    assert doc["universe_basis"].endswith("(factor book unavailable)") and doc["all_tickers"]["AAPL"]["source"] == "live"


if __name__ == "__main__":
    n = 0
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            fn()
            n += 1
            print("ok", name)
    print("%d tests passed" % n)
