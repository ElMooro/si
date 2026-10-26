"""justhodl-rating-change-cluster offline tests (v1.1.0 bank fusion): the rating feed is the union of the banked
grades-latest-news days (deduplicated, window-bounded), picks are priced from the factor book's bank close, and the live
page / quote stay as the fallback.  No network, no AWS, no key.

Run: python3 aws/lambdas/justhodl-rating-change-cluster/tests/run_tests.py
"""
import datetime as dt
import json
import os
import sys
import types

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "source"))
os.environ["FMP_KEY"] = "test-key-not-real"
os.environ["TELEGRAM_BOT_TOKEN"] = "x"

TODAY = dt.date.today()


def d(days_ago):
    return (TODAY - dt.timedelta(days=days_ago)).isoformat()


def row(sym, firm, days_ago, action="upgrade", new="Buy", prev="Hold"):
    return {"symbol": sym, "gradingCompany": firm, "publishedDate": d(days_ago) + "T13:00:00.000Z", "action": action, "newGrade": new, "previousGrade": prev}


class FakeS3:
    def __init__(self):
        self.written = {}

    def put_object(self, **k):
        self.written[k["Key"]] = json.loads(k["Body"])


class FakeSSM:
    def get_parameter(self, **k):
        raise RuntimeError("no ssm in tests")

    def put_parameter(self, **k):
        return None


class FakeBook(types.ModuleType):
    on = True
    quotes = {}
    rows = {}
    names = {}

    def available(self):
        return self.on

    def as_of(self):
        return {"fmp-bank": "2026-10-25"} if self.on else None

    def quote(self, symbol):
        return self.quotes.get(str(symbol).upper())

    def factor(self, symbol):
        n = self.names.get(str(symbol).upper())
        return {"identity": {"name": n}} if n else None

    def lookup(self, symbol, fields=None):
        r = self.rows.get(str(symbol).upper())
        return None if r is None else {k: r.get(k) for k in (fields or r)} | {"symbol": str(symbol).upper()}


class FakeBank(types.ModuleType):
    days = {}
    asked = []

    def walker_latest(self, dataset, lookback_days=10, end=None):
        assert dataset == "grades-latest-news" and lookback_days == 1 and end
        self.asked.append(end)
        page = self.days.get(end)
        return (end, page) if page else (None, None)


def boot():
    import boto3
    s3, ssm = FakeS3(), FakeSSM()
    boto3.client = lambda name, *a, **k: s3 if name == "s3" else ssm
    sys.modules["fmp_bankfirst"] = types.ModuleType("fmp_bankfirst")
    sys.modules["fmp_bankfirst"].install = lambda: None
    ms = types.ModuleType("managed_secret")
    ms.managed_secret = lambda *a, **k: "x"
    sys.modules["managed_secret"] = ms
    book, bank = FakeBook("bank_enrich"), FakeBank("fmp_bank")
    sys.modules["bank_enrich"], sys.modules["fmp_bank"] = book, bank
    sys.modules.pop("lambda_function", None)
    import lambda_function as lf
    lf._book, lf._bank, lf.s3, lf.ssm = book, bank, s3, ssm
    lf.telegram_send = lambda text: None
    return lf, s3, book, bank


def test_banked_feed_days_union_dedupe_window_and_book_prices():
    lf, s3, book, bank = boot()
    cluster = [row("AAA", "Goldman Sachs", 1), row("AAA", "Morgan Stanley", 4), row("AAA", "JPMorgan", 9)]
    bank.days = {
        d(0): cluster[:2] + [row("BBB", "Citi", 2, action="downgrade", new="Sell", prev="Buy"), row("CCC", "Goldman Sachs", 1)],
        d(1): cluster[:2],                                                     # yesterday's file repeats today's rows -> dedupe
        d(9): [cluster[2], row("AAA", "Goldman Sachs", 9)],                     # a second Goldman upgrade -> one per firm per cluster
        d(16): [row("AAA", "Barclays", 16)],                                     # late day read, but outside the 14-day window -> dropped
        d(3): [row("BBB", "Barclays", 3, action="downgrade", new="Underweight", prev="Overweight"), row("BBB", "UBS", 5, action="downgrade", new="Neutral", prev="Buy")],
    }
    book.quotes = {"AAA": {"price": 100.0, "marketCap": 20e9, "name": None}, "BBB": {"price": 50.0, "marketCap": 3e9, "name": "BBB Corp"}}
    book.names = {"AAA": "AAA Inc"}
    book.rows = {"AAA": {"pt_upside_pct": 18.0, "pt_n_1m": 20, "consensus_score": 0.8, "altman_z": 6.0, "ret_21d": 0.05, "short_pct_float": 1.0, "sector": "Technology", "composite_street": 95.0}}
    live = []
    lf.fmp_get = lambda path, params=None: live.append((path, params)) or None
    lf.lambda_handler({}, None)
    out = s3.written[lf.S3_KEY]
    assert out["version"] == "v1.1.0"
    summ = out["summary"]
    assert summ["feed_basis"].startswith("fmp-walker grades-latest-news (5 banked days)"), summ["feed_basis"]
    assert summ["feed_size"] == 8, summ["feed_size"]                                            # AAA 3 + AAA Goldman day-9 1 + BBB 3 + CCC 1; yesterday's repeats deduped, the 16-day-old row dropped
    assert set(bank.asked) >= {d(0), d(17)} and len(bank.asked) == 18                              # window 14 d + 3 late days + today
    buys = {p["ticker"]: p for p in out["buy_picks"]}
    sells = {p["ticker"]: p for p in out["sell_picks"]}
    assert set(buys) == {"AAA"} and set(sells) == {"BBB"}, (set(buys), set(sells))
    assert buys["AAA"]["n_major_upgrades"] == 3 and buys["AAA"]["price"] == 100.0 and buys["AAA"]["price_source"] == "factor-book close"
    assert buys["AAA"]["name"] == "AAA Inc" and buys["AAA"]["bank_context"]["pt_upside_pct"] == 18.0 and buys["AAA"]["mcap_billions"] == 20.0
    assert sells["BBB"]["n_major_downgrades"] == 3 and sells["BBB"]["name"] == "BBB Corp" and sells["BBB"]["bank_context"] is None
    assert summ["price_basis"] == {"factor-book close": 2} and summ["book_as_of"] == {"fmp-bank": "2026-10-25"}
    assert not live, live                                                                           # nothing live when the banks answer
    assert out["state"] in ("CLUSTER_BUY_ACTIVE", "CLUSTER_SELL_ACTIVE", "NORMAL")
    assert any("walker bank" in x for x in out["sources"]) and any("live fallback" in x for x in out["sources"])


def test_bank_off_falls_back_to_the_live_page_and_quote():
    lf, s3, book, bank = boot()
    book.on = False
    bank.days = {}
    answers = {"grades-latest-news": [row("AAA", "Goldman Sachs", 1), row("AAA", "Morgan Stanley", 2), row("AAA", "Citi", 3)],
               "quote": [{"symbol": "AAA", "price": 10.0, "marketCap": 1e9, "name": "AAA Inc"}]}
    lf.fmp_get = lambda path, params=None: answers.get(path)
    lf.lambda_handler({}, None)
    out = s3.written[lf.S3_KEY]
    assert out["summary"]["feed_basis"] == "fmp live grades-latest-news" and out["summary"]["book_as_of"] is None
    assert out["buy_picks"][0]["price_source"] == "fmp live" and out["buy_picks"][0]["bank_context"] is None and out["buy_picks"][0]["n_major_upgrades"] == 3


if __name__ == "__main__":
    test_banked_feed_days_union_dedupe_window_and_book_prices()
    print("ok test_banked_feed_days_union_dedupe_window_and_book_prices")
    test_bank_off_falls_back_to_the_live_page_and_quote()
    print("ok test_bank_off_falls_back_to_the_live_page_and_quote")
    print("2 tests passed")
