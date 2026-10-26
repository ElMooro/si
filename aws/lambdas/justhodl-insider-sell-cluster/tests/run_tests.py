"""justhodl-insider-sell-cluster offline tests (v1.1.0 bank fusion): the banked whole-market insider feed is the first
rung (strict sale filter, 14-day window, 10b5-1 excluded), the factor book answers price / market cap / context, the
live rungs stand when the bank is empty.  No network, no AWS, no key.

Run: python3 aws/lambdas/justhodl-insider-sell-cluster/tests/run_tests.py
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

    def walker_days(self, dataset, days=30, end=None):
        assert dataset == "insider-trading-latest" and days == 16
        return list(self.days)


class FakeBook(types.ModuleType):
    rows = {}
    on = True

    def available(self):
        return self.on

    def as_of(self):
        return {"fmp-bank": "2026-10-25", "tape": "2026-10-24"} if self.on else None

    def quote(self, symbol):
        r = self.rows.get(str(symbol).upper())
        if not r or not self.on:
            return None
        return {"symbol": symbol, "name": None, "price": r.get("price"), "marketCap": r.get("mcap"), "as_of": self.as_of(), "source": "factor-book (bank close, not a live tick)"}

    def lookup(self, symbol, fields=None):
        r = self.rows.get(str(symbol).upper())
        if not r or not self.on:
            return None
        return {k: r.get(k) for k in (fields or r)}

    def factor(self, symbol):
        r = self.rows.get(str(symbol).upper())
        return {"identity": {"symbol": symbol, "name": r.get("name")}} if (r and self.on) else None


class S3Stub:
    def __init__(self):
        self.written = {}

    def put_object(self, **k):
        self.written[k["Key"]] = json.loads(k["Body"])

    def get_object(self, **k):
        raise RuntimeError("no s3 in tests")


class SSMStub:
    def get_parameter(self, **k):
        raise RuntimeError("no ssm in tests")

    def put_parameter(self, **k):
        return None


def boot():
    import boto3
    stubs = {"s3": S3Stub(), "ssm": SSMStub()}
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


def sell(sym, who, d, shares=50000, price=100.0, owner="officer: Chief Executive Officer", tt="S-Sale", held=10000):
    return {"symbol": sym, "transactionType": tt, "transactionDate": d, "securitiesTransacted": shares, "price": price,
            "reportingName": who, "typeOfOwner": owner, "securitiesOwned": held}


def test_bank_feed_is_the_first_rung_with_the_strict_sale_filter_and_book_quotes():
    lf, bank, book, stubs = boot()
    today = datetime.now(timezone.utc).date()
    d1, d2, old = (today - timedelta(days=2)).isoformat(), (today - timedelta(days=5)).isoformat(), (today - timedelta(days=20)).isoformat()
    # BIG: 3 distinct non-plan sellers, $15M -> cluster; PLAN: 10b5-1 excluded; PUR: purchases must NOT pass as sales
    # (P-PURCHASE contains an S); OLD: outside the 14-day window; SMALL: $800k < $2M floor
    bank.days = [
        (d1, [sell("BIG", "A One", d1), sell("BIG", "B Two", d1), sell("PUR", "X", d1, tt="P-Purchase"), sell("PUR", "Y", d1, tt="P-Purchase"), sell("PUR", "Z", d1, tt="P-Purchase")]),
        (d2, [sell("BIG", "C Three", d2), sell("PLAN", "P1", d2, owner="officer: CFO 10b5-1 plan"), sell("PLAN", "P2", d2, owner="director (10b5-1)"), sell("PLAN", "P3", d2, owner="10B5-1 PLAN"),
              sell("SMALL", "S1", d2, shares=2000, price=100.0), sell("SMALL", "S2", d2, shares=2000, price=100.0), sell("SMALL", "S3", d2, shares=4000, price=100.0)]),
        (old, [sell("BIG", "D Four", old)]),
    ]
    book.rows = {"BIG": {"price": 101.5, "mcap": 12e9, "name": "Big Corp", "altman_z": 3.1, "piotroski": 6, "short_pct_float": 4.2, "ret_21d": -0.05},
                 "PUR": {"price": 10.0, "mcap": 5e9, "name": "Pur Inc"}}
    live = []
    lf.http_get = lambda url, timeout=15, retries=2: live.append(url) or "[]"
    rc = lf.lambda_handler({}, None)
    assert rc["statusCode"] == 200, rc
    out = stubs["s3"].written[lf.S3_KEY]
    assert out["version"] == "1.1.0" and out["source_used"].startswith("fmp-walker bank (3 days, 13 feed rows)"), out["source_used"]
    assert out["feed_basis"]["bank_days"] == 3 and out["n_raw_sells_scanned"] == 6, out["feed_basis"]      # 3 BIG + 3 SMALL; purchases, 10b5-1 and the 20-day-old row dropped
    assert [c["ticker"] for c in out["all_clusters"]] == ["BIG"], out["all_clusters"]
    big = out["all_clusters"][0]
    assert big["n_distinct_sellers"] == 3 and big["total_sale_value_usd"] == 15_000_000 and big["price_source"].startswith("factor-book")
    assert big["market_cap_usd"] == 12e9 and big["name"] == "Big Corp" and big["bank_context"]["altman_z"] == 3.1 and big["bank_context"]["source"] == "factor-book"
    assert out["price_sources"] == {"factor-book (bank close, not a live tick)": 1} and out["book_as_of"]["fmp-bank"] == "2026-10-25"
    assert not live, "no live FMP call when the bank and the book answer: %s" % live
    assert out["state"] in ("NORMAL", "QUIET", "ACTIVE", "RED_FLAG_RICH")


def test_live_ladder_stands_when_the_bank_is_empty_and_live_quotes_are_capped():
    lf, bank, book, stubs = boot()
    bank.days = []
    book.on = False
    today = datetime.now(timezone.utc).date()
    d1 = (today - timedelta(days=1)).isoformat()
    live = []

    def fake_http_get(url, timeout=15, retries=2):
        live.append(url)
        if "insider-trading/latest" in url and "page=0" in url:
            return json.dumps([sell("LIV", "A", d1), sell("LIV", "B", d1), sell("LIV", "C", d1)])
        if "insider-trading/latest" in url:
            return "[]"
        if "/stable/quote" in url:
            return json.dumps([{"price": 50.0, "marketCap": 2e9, "name": "Live Co"}])
        return "[]"
    lf.http_get = fake_http_get
    lf.LIVE_QUOTE_CAP = 1
    rc = lf.lambda_handler({}, None)
    assert rc["statusCode"] == 200
    out = stubs["s3"].written[lf.S3_KEY]
    assert out["source_used"] == "fmp live /stable/insider-trading/latest" and out["feed_basis"]["bank_days"] == 0 and out["book_as_of"] is None
    assert len([u for u in live if "/stable/quote" in u]) == 1 and out["all_clusters"][0]["price_source"] == "fmp live /stable/quote"
    assert out["all_clusters"][0]["bank_context"] is None and out["all_clusters"][0]["name"] == "Live Co"


if __name__ == "__main__":
    test_bank_feed_is_the_first_rung_with_the_strict_sale_filter_and_book_quotes()
    print("ok test_bank_feed_is_the_first_rung_with_the_strict_sale_filter_and_book_quotes")
    test_live_ladder_stands_when_the_bank_is_empty_and_live_quotes_are_capped()
    print("ok test_live_ladder_stands_when_the_bank_is_empty_and_live_quotes_are_capped")
    print("2 tests passed")
