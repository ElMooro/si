"""justhodl-insider-cluster-scanner offline tests (v2.1.0 bank fusion): the banked whole-market insider feed is the
first rung (purchases only, MIN_BUY_VALUE, the parser's transaction shape), the SEC EDGAR parse is the fallback, the
factor book enriches the top clusters first.  No network, no AWS, no key.

Run: python3 aws/lambdas/justhodl-insider-cluster-scanner/tests/run_tests.py
"""
import json
import os
import sys
import types
from datetime import datetime, timedelta, timezone

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "source"))
os.environ["TELEGRAM_TOKEN"] = ""


class FakeBank(types.ModuleType):
    days = []

    def walker_days(self, dataset, days=30, end=None):
        assert dataset == "insider-trading-latest" and days == 33, (dataset, days)
        return list(self.days)


class FakeBook(types.ModuleType):
    docs = {}
    on = True

    def available(self):
        return self.on

    def as_of(self):
        return {"fmp-bank": "2026-10-25", "tape": "2026-10-24"} if self.on else None

    def factor(self, symbol):
        return self.docs.get(str(symbol).upper()) if self.on else None

    def lookup(self, symbol, fields=None):
        d = self.docs.get(str(symbol).upper())
        if not d or not self.on:
            return None
        ctx = d.get("_ctx") or {}
        return {k: ctx.get(k) for k in (fields or ctx)}


class S3Stub:
    def __init__(self):
        self.written = {}

    def put_object(self, **k):
        self.written[k["Key"]] = json.loads(k["Body"])

    def get_object(self, **k):
        raise RuntimeError("no s3 in tests")


def boot():
    import boto3
    s3 = S3Stub()
    boto3.client = lambda name, *a, **k: s3
    sys.modules["fmp_bankfirst"] = types.ModuleType("fmp_bankfirst")
    sys.modules["fmp_bankfirst"].install = lambda: None
    ms = types.ModuleType("managed_secret")
    ms.managed_secret = lambda *a, **k: "x"
    sys.modules["managed_secret"] = ms
    bank = FakeBank("fmp_bank")
    sys.modules["fmp_bank"] = bank
    book = FakeBook("bank_enrich")
    sys.modules["bank_enrich"] = book
    sys.modules.pop("lambda_function", None)
    import lambda_function as lf
    lf._bank, lf._book, lf.S3 = bank, book, s3
    return lf, bank, book, s3


def row(sym, who, owner, d, shares=10000, price=25.0, tt="P-Purchase", ad="A", url="https://www.sec.gov/Archives/edgar/data/1/0001122330-26-000111-index.htm", cik="0000000001"):
    return {"symbol": sym, "filingDate": d + " 16:30:00", "transactionDate": d, "reportingCik": "0009", "companyCik": cik, "transactionType": tt,
            "securitiesOwned": 50000, "reportingName": who, "typeOfOwner": owner, "acquisitionOrDisposition": ad, "formType": "4",
            "securitiesTransacted": shares, "price": price, "securityName": "Common Stock", "url": url}


class Ctx:
    def get_remaining_time_in_millis(self):
        return 500000


def test_bank_feed_is_the_first_rung_and_maps_to_the_form4_schema():
    lf, bank, book, s3 = boot()
    today = datetime.now(timezone.utc).date()
    d1, d2, old = (today - timedelta(days=1)).isoformat(), (today - timedelta(days=4)).isoformat(), (today - timedelta(days=40)).isoformat()
    bank.days = [
        (d1, [row("DUAL", "Jane Ceo", "officer: Chief Executive Officer", d1, url="https://www.sec.gov/Archives/edgar/data/1/0001122330-26-000111-index.htm"),
              row("DUAL", "Jane Ceo", "officer: Chief Executive Officer", d1, url="https://www.sec.gov/Archives/edgar/data/1/0001122330-26-000111-index.htm"),   # same accession -> deduped
              row("DUAL", "Bob Cfo", "officer: Chief Financial Officer", d1, url="https://www.sec.gov/Archives/edgar/data/1/0001122330-26-000112-index.htm"),
              row("SELL", "Sam Seller", "director", d1, tt="S-Sale", ad="D"),
              row("TINY", "Tiny Buyer", "director", d1, shares=100, price=5.0)]),                                   # $500 < MIN_BUY_VALUE
        (d2, [row("LONE", "Dir One", "director", d2, url="https://www.sec.gov/Archives/edgar/data/2/0001122330-26-000200-index.htm"),
              row("LONE", "Dir One", "director", d2, url="https://www.sec.gov/Archives/edgar/data/2/0001122330-26-000201-index.htm")]),
        (old, [row("OLD", "Old Buyer", "officer: President and CEO", old)]),
    ]
    book.docs = {"DUAL": {"identity": {"symbol": "DUAL", "name": "Dual Corp", "sector": "Technology", "industry": "Software", "mcap": 4e9},
                          "profile": {"price": 26.0, "country": "US"}, "returns": {"close": 26.0, "high_52w": 40.0, "low_52w": 20.0},
                          "float": {"outstandingShares": 150000000}, "as_of": {"fmp-bank": "2026-10-25"},
                          "_ctx": {"altman_z": 2.5, "piotroski": 7, "short_pct_float": 3.0, "ret_21d": -0.1}}}
    sec_calls = []
    lf.sec_get = lambda url, retries=3, timeout=15: sec_calls.append(url) or (_ for _ in ()).throw(RuntimeError("no SEC in tests"))
    fmp_calls = []
    lf.fmp_get = lambda path, params=None, timeout=10: fmp_calls.append(path) or None
    rc = lf.lambda_handler({}, Ctx())
    body = json.loads(rc["body"])
    out = s3.written[lf.S3_KEY]
    assert out["version"] == "2.1.0" and out["source_used"].startswith("fmp-walker bank insider-trading-latest (3 days, 8 feed rows, 6 purchases)"), out["source_used"]      # raw purchases; the duplicate accession and the 40-day-old row fall in build_clusters
    assert out["stats"]["n_buy_transactions"] == 6 and out["stats"]["n_form4_filings_scanned"] == 0 and out["stats"]["n_bank_feed_days"] == 3
    assert not sec_calls, "the SEC ladder must not run when the bank answers"
    tickers = {c["ticker"]: c for c in out["clusters"]}
    assert set(tickers) == {"DUAL", "LONE"}, tickers.keys()                       # SELL dropped, TINY below the floor, OLD outside the window
    dual = tickers["DUAL"]
    assert dual["n_insiders"] == 2 and dual["n_transactions"] == 2 and dual["has_ceo"] and dual["has_cfo"] and dual["signal_type"] == "smart_money_dual"
    assert dual["insiders"][0]["role"] == "Chief Executive Officer" and dual["insiders"][0]["role_tier"] == 100
    assert dual["fundamentals"]["fundamentals_source"].startswith("factor-book") and dual["fundamentals"]["market_cap"] == 4e9 and dual["fundamentals"]["pct_from_52w_high"] == -35.0
    assert dual["company"] == "Dual Corp" and dual["bank_context"]["piotroski"] == 7 and dual["bank_context"]["source"] == "factor-book"
    assert "35% off 52w high" in dual["rationale"], dual["rationale"]
    assert tickers["LONE"]["fundamentals"] == {} and fmp_calls == ["/stable/profile"], fmp_calls      # live FMP only for the name the book lacks (profile fails -> {})
    assert out["fundamentals_sources"] == {"factor-book (bank close, not a live tick)": 1, "none": 1}, out["fundamentals_sources"]
    assert out["book_as_of"]["fmp-bank"] == "2026-10-25" and body["n_clusters"] == 2
    assert {b["ticker"] for b in out["all_ticker_buys"]} == {"DUAL", "LONE"}


def test_sec_ladder_stands_when_the_bank_is_empty():
    lf, bank, book, s3 = boot()
    bank.days = []
    book.on = False
    idx_calls = []
    lf.get_daily_form4_index = lambda d: idx_calls.append(d) or []
    lf.fmp_get = lambda path, params=None, timeout=10: None
    rc = lf.lambda_handler({}, Ctx())
    out = s3.written[lf.S3_KEY]
    assert out["source_used"] == "sec_edgar_form4_sample" and len(idx_calls) == lf.N_BUSINESS_DAYS_INDEX and out["book_as_of"] is None
    assert out["clusters"] == [] and out["stats"]["n_bank_feed_days"] == 0 and json.loads(rc["body"])["n_clusters"] == 0


if __name__ == "__main__":
    test_bank_feed_is_the_first_rung_and_maps_to_the_form4_schema()
    print("ok test_bank_feed_is_the_first_rung_and_maps_to_the_form4_schema")
    test_sec_ladder_stands_when_the_bank_is_empty()
    print("ok test_sec_ladder_stands_when_the_bank_is_empty")
    print("2 tests passed")
