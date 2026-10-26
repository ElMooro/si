"""justhodl-insider-buyback-confluence offline tests (v1.1.0 bank fusion): the banked whole-market insider feed widens
the insider side (feeder first, bank fills), the factor book answers price / market cap / context, live quotes are
capped, the old path stands when the bank is empty.  No network, no AWS, no key.

Run: python3 aws/lambdas/justhodl-insider-buyback-confluence/tests/run_tests.py
"""
import json
import os
import sys
import types
from datetime import datetime, timedelta, timezone

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "source"))
os.environ["FMP_KEY"] = "test-key-not-real"
os.environ["TELEGRAM_BOT_TOKEN"] = ""


class FakeBank(types.ModuleType):
    days = []

    def walker_days(self, dataset, days=30, end=None, where=None):
        assert dataset == "insider-trading-latest" and days == 60
        return [(d, [r for r in rows if where is None or where(r)]) for d, rows in self.days]      # mirrors the real reader: filter on read


class FakeBook(types.ModuleType):
    rows = {}
    on = True

    def available(self):
        return self.on

    def as_of(self):
        return {"fmp-bank": "2026-10-25"} if self.on else None

    def quote(self, symbol):
        r = self.rows.get(str(symbol).upper())
        if not r or not self.on:
            return None
        return {"symbol": symbol, "name": None, "exchange": r.get("exchange"), "price": r.get("price"), "marketCap": r.get("mcap"), "as_of": self.as_of(), "source": "factor-book (bank close, not a live tick)"}

    def lookup(self, symbol, fields=None):
        r = self.rows.get(str(symbol).upper())
        if not r or not self.on:
            return None
        return {k: r.get(k) for k in (fields or r)}

    def factor(self, symbol):
        r = self.rows.get(str(symbol).upper())
        return {"identity": {"symbol": symbol, "name": r.get("name")}} if (r and self.on) else None


class S3Stub:
    def __init__(self, docs):
        self.docs, self.written = docs, {}

    def put_object(self, **k):
        self.written[k["Key"]] = json.loads(k["Body"])

    def get_object(self, **k):
        if k["Key"] not in self.docs:
            raise RuntimeError("missing " + k["Key"])
        return {"Body": types.SimpleNamespace(read=lambda: json.dumps(self.docs[k["Key"]]).encode())}


class SSMStub:
    def get_parameter(self, **k):
        raise RuntimeError("no ssm")

    def put_parameter(self, **k):
        return None


def boot(docs):
    import boto3
    stubs = {"s3": S3Stub(docs), "ssm": SSMStub()}
    boto3.client = lambda name, *a, **k: stubs[name]
    sys.modules["fmp_bankfirst"] = types.ModuleType("fmp_bankfirst")
    sys.modules["fmp_bankfirst"].install = lambda: None
    bank = FakeBank("fmp_bank")
    sys.modules["fmp_bank"] = bank
    book = FakeBook("bank_enrich")
    sys.modules["bank_enrich"] = book
    sys.modules.pop("lambda_function", None)
    import lambda_function as lf
    lf._bank, lf._book = bank, book
    return lf, bank, book, stubs


def buy(sym, who, d, shares=20000, price=50.0, tt="P-Purchase"):
    return {"symbol": sym, "transactionType": tt, "transactionDate": d, "securitiesTransacted": shares, "price": price, "reportingName": who, "typeOfOwner": "director"}


FEEDS = {
    "data/insider-buys-enriched.json": {"top_setups": [{"ticker": "FEED", "n_buyers": 2, "total_value": 600000, "days_since_last_buy": 10}]},
    "data/buyback-engine.json": {"n_scored": 3, "tickers": {"FEED": {"class": "💪 NET_SHRINKER", "buyback_score": 70, "share_count_reduction_yoy": 3, "active_execution": True},
                                                            "BANK": {"class": "🚀 FRESH_LARGE_AUTH", "buyback_score": 80, "auth_pct_mcap": 6},
                                                            "SMALL": {"class": "💪 NET_SHRINKER", "buyback_score": 60}}},
    "data/buyback-scanner.json": {"top_opportunities": []},
}


def test_bank_feed_widens_the_insider_side_and_the_book_answers_quotes():
    lf, bank, book, stubs = boot(FEEDS)
    today = datetime.now(timezone.utc).date()
    d1, d2 = (today - timedelta(days=3)).isoformat(), (today - timedelta(days=20)).isoformat()
    bank.days = [(d1, [buy("BANK", "A One", d1), buy("BANK", "B Two", d1), buy("BANK", "C Three", d1), buy("FEED", "Z", d1, shares=1000), buy("NOBYB", "N", d1), buy("BANK", "S Seller", d1, tt="S-Sale")]),
                 (d2, [buy("SMALL", "Small Guy", d2)])]
    book.rows = {"BANK": {"price": 52.0, "mcap": 3e9, "name": "Bank Corp", "exchange": "NASDAQ", "altman_z": 4.0, "piotroski": 8},
                 "FEED": {"price": 20.0, "mcap": 9e8, "name": "Feed Inc", "exchange": "NYSE"},
                 "SMALL": {"price": 5.0, "mcap": 2e8, "name": "Small Co"}}
    live = []
    lf.http_get = lambda url, timeout=10, retries=2: live.append(url) or "[]"
    rc = lf.lambda_handler({}, None)
    assert rc["statusCode"] == 200, rc
    out = stubs["s3"].written[lf.S3_KEY]
    assert out["version"] == "1.1.0" and out["feeders"]["insider_bank_days"] == 2 and out["feeders"]["insider_bank_feed_rows"] == 6   # the S-Sale row never leaves the reader (filtered on read)
    assert out["feeders"]["insider_feeder_tickers"] == 1 and out["feeders"]["insider_bank_tickers"] == 4 and out["feeders"]["insider_tickers_count"] == 4
    assert out["feeders"]["insider_source_used"].startswith("fmp-walker bank insider-trading-latest (2 days) + feeder")
    rows = {c["ticker"]: c for c in out["all_confluences"]}
    assert set(rows) == {"BANK", "FEED"}, rows.keys()                 # SMALL fails the $500M gate; NOBYB has no buyback
    assert rows["BANK"]["insider_source"] == "bank" and rows["BANK"]["insider_stats"]["n_buyers"] == 3 and rows["BANK"]["insider_stats"]["total_value_usd"] == 3_000_000
    assert rows["BANK"]["insider_stats"]["days_since_last_buy"] == 3 and rows["BANK"]["price_source"].startswith("factor-book") and rows["BANK"]["name"] == "Bank Corp"
    assert rows["BANK"]["bank_context"]["altman_z"] == 4.0 and rows["BANK"]["bank_context"]["source"] == "factor-book"
    assert rows["FEED"]["insider_source"] == "feeder+bank" and rows["FEED"]["insider_stats"]["n_buyers"] == 2 and rows["FEED"]["insider_stats"]["total_value_usd"] == 600000   # feeder's stats win (max)
    assert out["price_sources"] == {"factor-book (bank close, not a live tick)": 3} and out["book_as_of"]["fmp-bank"] == "2026-10-25"
    assert not live, live


def test_old_path_stands_when_the_bank_is_empty_with_capped_live_quotes():
    lf, bank, book, stubs = boot(FEEDS)
    bank.days = []
    book.on = False
    live = []

    def fake_http_get(url, timeout=10, retries=2):
        live.append(url)
        return json.dumps([{"price": 21.0, "marketCap": 9e8, "name": "Feed Inc", "exchange": "NYSE"}])
    lf.http_get = fake_http_get
    rc = lf.lambda_handler({}, None)
    assert rc["statusCode"] == 200
    out = stubs["s3"].written[lf.S3_KEY]
    assert out["feeders"]["insider_source_used"].startswith("feeder only") and out["feeders"]["insider_tickers_count"] == 1 and out["book_as_of"] is None
    assert [c["ticker"] for c in out["all_confluences"]] == ["FEED"] and out["all_confluences"][0]["insider_source"] == "feeder"
    assert out["all_confluences"][0]["price_source"] == "fmp live /stable/quote" and out["all_confluences"][0]["bank_context"] is None and len(live) == 1


if __name__ == "__main__":
    test_bank_feed_widens_the_insider_side_and_the_book_answers_quotes()
    print("ok test_bank_feed_widens_the_insider_side_and_the_book_answers_quotes")
    test_old_path_stands_when_the_bank_is_empty_with_capped_live_quotes()
    print("ok test_old_path_stands_when_the_bank_is_empty_with_capped_live_quotes")
    print("2 tests passed")
