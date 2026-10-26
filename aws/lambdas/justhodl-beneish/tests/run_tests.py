"""justhodl-beneish offline tests (v1.1.0 bank fusion): the whole-market M-Score from the bank's statements beside the
top-50 desk.  No network, no AWS, no key.

Run: python3 aws/lambdas/justhodl-beneish/tests/run_tests.py
"""
import json
import os
import sys
import types

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "source"))
os.environ["FMP_KEY"] = "test-key-not-real"
os.environ["BENEISH_BOOK_LIMIT"] = "3"


def statements(sym, scale=1.0, manipulate=False):
    """Two fiscal years of plausible statements; `manipulate` inflates receivables/sales/accruals in year t."""
    base = {"revenue": 1000.0, "costOfRevenue": 600.0, "grossProfit": 400.0, "sellingGeneralAndAdministrativeExpenses": 150.0, "depreciationAndAmortization": 50.0,
            "netIncome": 100.0, "incomeBeforeTax": 130.0, "operatingIncome": 150.0, "calendarYear": "2025", "date": "2025-12-31"}
    prev = dict(base, revenue=900.0, costOfRevenue=540.0, grossProfit=360.0, sellingGeneralAndAdministrativeExpenses=135.0, netIncome=90.0, calendarYear="2024", date="2024-12-31")
    bs_t = {"netReceivables": 150.0, "totalCurrentAssets": 500.0, "propertyPlantEquipmentNet": 400.0, "totalAssets": 1200.0, "totalCurrentLiabilities": 300.0, "longTermDebt": 200.0,
            "totalDebt": 250.0, "cashAndCashEquivalents": 100.0, "date": "2025-12-31"}
    bs_1 = dict(bs_t, netReceivables=120.0, totalCurrentAssets=450.0, propertyPlantEquipmentNet=380.0, totalAssets=1100.0, totalCurrentLiabilities=280.0, longTermDebt=190.0, totalDebt=230.0, cashAndCashEquivalents=90.0, date="2024-12-31")
    cf_t = {"operatingCashFlow": 120.0, "netIncome": 100.0, "depreciationAndAmortization": 50.0, "date": "2025-12-31"}
    if manipulate:
        base = dict(base, revenue=1600.0, grossProfit=500.0, netIncome=300.0)
        bs_t = dict(bs_t, netReceivables=700.0, totalCurrentAssets=1100.0, totalAssets=1900.0)
        cf_t = dict(cf_t, operatingCashFlow=-50.0, netIncome=300.0)
    scale_row = lambda d: {k: (v * scale if isinstance(v, float) else v) for k, v in d.items()}
    return [scale_row(base), scale_row(prev)], [scale_row(bs_t), scale_row(bs_1)], [scale_row(cf_t)]


class FakeBank(types.ModuleType):
    HAVE = {"AAPL": False, "MKT1": False, "MKT2": True, "NOSTMT": None}

    def statements(self, path, symbol, period="annual", limit=None):
        flag = self.HAVE.get(symbol)
        if flag is None:
            return None
        inc, bs, cf = statements(symbol, manipulate=flag)
        return {"income-statement": inc, "balance-sheet-statement": bs, "cash-flow-statement": cf}[path]


class FakeBook(types.ModuleType):
    on = True
    ROWS = [{"symbol": "MKT1", "mcap": 9e9, "sector": "Industrials"}, {"symbol": "MKT2", "mcap": 2e9, "sector": "Technology"},
            {"symbol": "NOSTMT", "mcap": 1e9, "sector": "Energy"}, {"symbol": "TINYCO", "mcap": 1e8, "sector": "Energy"}, {"symbol": "AAPL", "mcap": 3e12, "sector": "Technology"}]

    def available(self):
        return self.on

    def as_of(self):
        return {"fmp-bank": "2026-10-25T23:25:00+00:00"}

    def screen(self, where, fields=None, limit=None, common_only=True):
        return [dict(r) for r in self.ROWS if where(r)]

    def lookup(self, symbol, fields=None):
        return {f: 1.0 for f in fields} if symbol == "MKT2" else None


def boot():
    import boto3
    stub = types.SimpleNamespace(store={})
    stub.put_object = lambda **kw: stub.store.__setitem__(kw["Key"], kw["Body"])
    boto3.client = lambda *a, **k: stub
    sys.modules["fmp_bankfirst"] = types.ModuleType("fmp_bankfirst")
    sys.modules["fmp_bankfirst"].install = lambda: None
    book, bank = FakeBook("bank_enrich"), FakeBank("fmp_bank")
    sys.modules["bank_enrich"], sys.modules["fmp_bank"] = book, bank
    sys.modules.pop("lambda_function", None)
    import lambda_function as lf
    lf.s3 = stub
    lf.STATIC_TOP50_SPX = [{"symbol": "AAPL", "sector": "Technology"}, {"symbol": "LIVE1", "sector": "Healthcare"}]
    lf.UNIVERSE_TOP_N = 2
    lf.telegram_notify = lambda msg: None
    lf.FMP_SLEEP_SEC = 0
    live_calls = []

    def live(symbol):
        live_calls.append(symbol)
        inc, bs, cf = statements(symbol)
        return lf.compute_beneish(inc, bs, cf)
    lf.analyze_ticker = live
    return lf, stub, book, live_calls


def test_market_is_scored_from_the_bank_and_the_top50_desk_keeps_its_meaning():
    lf, stub, book, live_calls = boot()
    out = lf.lambda_handler({}, None)
    assert out["statusCode"] == 200, out
    doc = json.loads(stub.store[lf.S3_KEY])
    assert doc["version"] == "1.1.0" and doc["universe_state_scope"].startswith("STATIC_TOP50_SPX (2 scored)")
    assert live_calls == ["LIVE1"], live_calls                           # AAPL came from the bank; only the book-less static name went live
    tick = {t["ticker"]: t for t in doc["all_tickers"]}
    assert set(tick) == {"AAPL", "LIVE1", "MKT1", "MKT2"}, tick.keys()   # NOSTMT has no bank statements; TINYCO below $500M
    m = doc["market"]
    assert m["n_candidates"] == 3 and m["n_scored_total"] == 4 and m["book_as_of"]["fmp-bank"].startswith("2026-10-25"), m
    assert tick["MKT2"]["verdict"] == "LIKELY_MANIPULATOR" and tick["MKT1"]["verdict"] != "LIKELY_MANIPULATOR", (tick["MKT2"], tick["MKT1"])
    assert m["top_red_flags"][0]["ticker"] == "MKT2" and m["top_red_flags"][0]["bank_context"]["income_quality"] == 1.0
    assert m["by_sector"]["Technology"]["red"] == 1 and m["pct_red_total"] == 25.0
    assert doc["red_flags"] == [] and doc["n_likely_manipulator"] == 0            # the top-50 desk: AAPL + LIVE1 clean in this fixture
    assert {t["ticker"] for t in doc["cleanest_10"]} == {"AAPL", "LIVE1"}


def test_without_the_book_the_top50_desk_alone_runs():
    lf, stub, book, live_calls = boot()
    book.on = False
    out = lf.lambda_handler({}, None)
    doc = json.loads(stub.store[lf.S3_KEY])
    assert doc["market"]["n_candidates"] == 0 and doc["market"]["book_as_of"] is None and doc["n_universe_analyzed"] == 2
    assert {t["ticker"] for t in doc["all_tickers"]} == {"AAPL", "LIVE1"}


if __name__ == "__main__":
    n = 0
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            fn()
            n += 1
            print("ok", name)
    print("%d tests passed" % n)
