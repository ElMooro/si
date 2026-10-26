"""justhodl-coffee-can offline tests (v1.1.0 bank fusion): the quote comes from the factor book, the thesis series from the
FMP statement bank, every holding carries bank_context; with the banks absent the live FMP path runs unchanged.
No network, no AWS, no key.

Run: python3 aws/lambdas/justhodl-coffee-can/tests/run_tests.py
"""
import io
import json
import os
import sys
import types

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "source"))
os.environ["FMP_KEY"] = "test-key-not-real"
os.environ.pop("TELEGRAM_TOKEN", None)

HOLDINGS = {"holdings": [{"symbol": "AAPL", "entry_price": 100.0, "entry_date": "2020-01-15", "target_multiple": 10, "thesis": "ecosystem"},
                         {"symbol": "ZZZZ", "entry_price": 5.0, "entry_date": "2024-06-01", "target_multiple": 25, "thesis": "unknown name"}]}


class FakeS3:
    def __init__(self):
        self.store = {"data/coffee-can-holdings.json": json.dumps(HOLDINGS).encode()}
        self.puts = {}

    def get_object(self, Bucket, Key):
        if Key not in self.store:
            raise KeyError(Key)
        return {"Body": io.BytesIO(self.store[Key])}

    def put_object(self, Bucket, Key, Body, **kw):
        self.puts[Key] = json.loads(Body)
        self.store[Key] = Body


class FakeBook(types.ModuleType):
    on = True
    rows = {"AAPL": {"altman_z": 9.1, "piotroski": 7, "roic": 0.55, "revenue_growth": 0.06, "gross_margin": 0.46, "net_debt_ebitda": 0.3,
                     "short_pct_float": 0.7, "pt_upside_pct": 8.0, "dcf_upside_pct": -5.0, "composite_quality": 97.0, "from_52w_high_pct": -3.0, "ret_252d": 0.21, "mcap": 3.9e12}}
    quotes = {"AAPL": {"symbol": "AAPL", "name": None, "price": 262.5, "yearHigh": 270.0, "yearLow": 169.0, "marketCap": 3.9e12,
                       "as_of": {"fmp-bank": "2026-10-25T23:40:00+00:00", "tape": "2026-10-24"}, "source": "factor-book (bank close, not a live tick)"}}

    def available(self):
        return self.on

    def as_of(self):
        return {"fmp-bank": "2026-10-25T23:40:00+00:00", "tape": "2026-10-24"} if self.on else None

    def lookup(self, symbol, fields=None):
        r = self.rows.get(str(symbol).upper())
        return None if r is None else {k: r.get(k) for k in (fields or r)}

    def quote(self, symbol):
        return self.quotes.get(str(symbol).upper()) if self.on else None


class FakeBank(types.ModuleType):
    on = True
    calls = []

    def statements(self, path, symbol, period="annual", limit=None, years=None):
        self.calls.append((path, symbol, period, limit))
        if not self.on or symbol != "AAPL":
            return None
        if path == "income-statement":
            return [{"date": "2025-09-27", "revenue": 416e9}, {"date": "2024-09-28", "revenue": 391e9}, {"date": "2023-09-30", "revenue": 383e9}]
        if path == "key-metrics":
            return [{"date": "2025-09-27", "returnOnInvestedCapital": 0.55}, {"date": "2024-09-28", "returnOnInvestedCapital": 0.5}]
        if path == "ratios":
            return [{"date": "2025-09-27", "grossProfitMargin": 0.466, "debtToEquityRatio": 1.4, "currentRatio": 0.9},
                    {"date": "2024-09-28", "grossProfitMargin": 0.462}, {"date": "2023-09-30", "grossProfitMargin": 0.441}]
        return None


def boot(book_on=True, bank_on=True):
    import boto3
    s3 = FakeS3()
    boto3.client = lambda *a, **k: s3
    sys.modules["fmp_bankfirst"] = types.ModuleType("fmp_bankfirst")
    sys.modules["fmp_bankfirst"].install = lambda: None
    book = FakeBook("bank_enrich")
    book.on = book_on
    sys.modules["bank_enrich"] = book
    bank = FakeBank("fmp_bank")
    bank.on = bank_on
    bank.calls = []
    sys.modules["fmp_bank"] = bank
    sys.modules.pop("lambda_function", None)
    import lambda_function as lf
    lf._book, lf._bank, lf.s3 = book, bank, s3
    lf.live = []
    lf._get_json = lambda url, timeout=12, retries=3: lf.live.append(url) or None       # live FMP: counted, answers nothing (429 today)
    return lf, s3, book, bank


def test_book_quote_bank_series_and_context_with_no_live_call_for_a_banked_name():
    lf, s3, book, bank = boot()
    r = lf.lambda_handler({}, None)
    assert json.loads(r["body"])["ok"] is True
    out = s3.puts["data/coffee-can.json"]
    assert out["version"] == "1.1.0" and out["method"] == "coffee_can_v1" and out["schema_version"] == "1.0"       # additive
    by = {h["symbol"]: h for h in out["holdings"]}
    a = by["AAPL"]
    assert a["current_price"] == 262.5 and a["year_high"] == 270.0 and a["price_source"].startswith("factor-book (bank close")
    assert a["current_multiple"] == 2.62 and a["drawdown_from_peak_pct"] == 2.8                                    # peak = year high 270
    assert a["thesis_health"] == "THESIS_INTACT" and a["thesis_detail"]["revenue"] == "growing" and a["thesis_detail"]["roic"] == "healthy"
    assert a["thesis_detail"]["sources"] == {"income-statement": "fmp-bank", "key-metrics": "fmp-bank", "ratios": "fmp-bank"}
    assert a["bank_context"]["altman_z"] == 9.1 and a["bank_context"]["composite_quality"] == 97.0 and a["bank_context"]["source"] == "factor-book"
    assert not [u for u in lf.live if "AAPL" in u], "a banked name must not cost a live FMP call: %s" % lf.live
    # the unknown name fell through to the live path (which answers nothing today) and is labelled honestly
    z = by["ZZZZ"]
    assert z["current_price"] is None and z["price_source"] == "live quote unavailable" and z["bank_context"] is None
    assert z["thesis_detail"]["sources"]["income-statement"] == "unavailable" and z["thesis_health"] == "THESIS_INTACT"
    assert out["facts_basis"]["factor_book"] == "available" and out["facts_basis"]["price_sources"] == {"factor-book": 1, "live quote unavailable": 1}
    assert out["facts_basis"]["thesis_series_sources"] == {"fmp-bank": 3, "unavailable": 3} and out["book_as_of"]["tape"] == "2026-10-24"
    assert bank.calls[0] == ("income-statement", "AAPL", "annual", 5)


def test_without_the_banks_the_live_path_runs_and_is_labelled():
    lf, s3, book, bank = boot(book_on=False, bank_on=False)
    lf._get_json = lambda url, timeout=12, retries=3: ([{"price": 250.0, "yearHigh": 260.0, "yearLow": 170.0, "name": "Apple Inc."}] if "quote" in url and "AAPL" in url
                                                        else ([{"revenue": 4e11}, {"revenue": 3.9e11}, {"revenue": 3.8e11}] if "income-statement" in url and "AAPL" in url else None))
    lf.lambda_handler({}, None)
    out = s3.puts["data/coffee-can.json"]
    a = {h["symbol"]: h for h in out["holdings"]}["AAPL"]
    assert a["current_price"] == 250.0 and a["price_source"] == "fmp live quote" and a["bank_context"] is None and a["name"] == "Apple Inc."
    assert a["thesis_detail"]["sources"]["income-statement"] == "fmp live" and a["thesis_detail"]["revenue"] == "growing"
    assert out["facts_basis"]["factor_book"] == "factor book unavailable" and out["book_as_of"] is None
    # the add action takes its entry price from the live quote when the book is absent
    lf.lambda_handler({"action": "add", "symbol": "aapl", "thesis": "x"}, None)
    hold = json.loads(s3.store["data/coffee-can-holdings.json"])["holdings"]
    assert [h for h in hold if h["symbol"] == "AAPL"][0]["entry_price"] == 250.0


if __name__ == "__main__":
    test_book_quote_bank_series_and_context_with_no_live_call_for_a_banked_name()
    print("ok test_book_quote_bank_series_and_context_with_no_live_call_for_a_banked_name")
    test_without_the_banks_the_live_path_runs_and_is_labelled()
    print("ok test_without_the_banks_the_live_path_runs_and_is_labelled")
    print("2 tests passed")
