"""justhodl-deep-value-screener offline tests (v1.1 bank fusion): whole-market net-cash candidates from the factor book
first, bank-close quotes with context, the cash + investments - debt test unchanged.  No network, no AWS, no key.

Run: python3 aws/lambdas/justhodl-deep-value-screener/tests/run_tests.py
"""
import json
import os
import sys
import types

HERE = os.path.dirname(os.path.abspath(__file__))
SRC = os.path.join(HERE, "..", "source")
sys.path.insert(0, SRC)
os.environ["FMP_KEY"] = "test-key-not-real"
os.environ["MAX_TICKERS"] = "3"
os.environ["TIMEOUT_BUDGET_S"] = "60"


class Stub:
    def __init__(self):
        self.store = {}

    def get_object(self, Bucket, Key):
        raise KeyError(Key)

    def put_object(self, Bucket, Key, Body, **kw):
        self.store[Key] = Body


class FakeBook(types.ModuleType):
    ROWS = {
        "CASHCO": {"symbol": "CASHCO", "mcap": 5e8, "price": 10.0, "cash_per_share": 6.0, "net_current_asset_value": 2e8, "ps": 0.9, "altman_z": 4.0, "piotroski": 7, "composite_value": 88.0, "composite_quality": 60.0, "short_pct_float": 2.0, "pt_upside_pct": 30.0, "dcf_upside_pct": 25.0},
        "NETNET": {"symbol": "NETNET", "mcap": 3e8, "price": 20.0, "cash_per_share": 3.0, "net_current_asset_value": 1e8, "ps": 0.4, "altman_z": 2.0, "piotroski": 5, "composite_value": 95.0, "composite_quality": 40.0, "short_pct_float": 8.0, "pt_upside_pct": None, "dcf_upside_pct": None},
        "RICH": {"symbol": "RICH", "mcap": 1e12, "price": 300.0, "cash_per_share": 5.0, "net_current_asset_value": -1e9, "ps": 8.0, "altman_z": 6.0, "piotroski": 8, "composite_value": 10.0, "composite_quality": 95.0, "short_pct_float": 1.0, "pt_upside_pct": 5.0, "dcf_upside_pct": -10.0},
        "SMALLC": {"symbol": "SMALLC", "mcap": 5e7, "price": 2.0, "cash_per_share": 1.5, "net_current_asset_value": 3e7, "ps": 0.3, "altman_z": 1.0, "piotroski": 3, "composite_value": 99.0, "composite_quality": 20.0, "short_pct_float": 30.0, "pt_upside_pct": None, "dcf_upside_pct": None},
        "CHEAPSALES": {"symbol": "CHEAPSALES", "mcap": 8e8, "price": 50.0, "cash_per_share": 2.0, "net_current_asset_value": 1e7, "ps": 0.5, "altman_z": 2.2, "piotroski": 6, "composite_value": 70.0, "composite_quality": 50.0, "short_pct_float": 4.0, "pt_upside_pct": 12.0, "dcf_upside_pct": 8.0},
        "SHELLISH": {"symbol": "SHELLISH", "mcap": 3e8, "price": 10.0, "cash_per_share": 10.0, "net_current_asset_value": 3e8, "ps": None, "altman_z": None, "piotroski": None, "composite_value": None, "composite_quality": None, "short_pct_float": None, "pt_upside_pct": None, "dcf_upside_pct": None},   # no revenue: a cash shell
        "FOREIGN": {"symbol": "FOREIGN", "mcap": 6e8, "price": 12.0, "cash_per_share": 8.0, "net_current_asset_value": 2e8, "ps": 0.7, "is_adr": True, "altman_z": 3.0, "piotroski": 5, "composite_value": 80.0, "composite_quality": 50.0, "short_pct_float": 1.0, "pt_upside_pct": 5.0, "dcf_upside_pct": 1.0},
    }
    on = True

    def available(self):
        return self.on

    def as_of(self):
        return {"fmp-bank": "2026-10-25T23:25:00+00:00", "polygon-tape": "2026-10-24"}

    def screen(self, where, fields=None, limit=None, common_only=True):
        return [dict(r) for r in self.ROWS.values() if where(r)]

    def lookup(self, symbol, fields=None):
        r = self.ROWS.get(symbol)
        return {f: r.get(f) for f in fields} if r and fields else (dict(r) if r else None)

    def quote(self, symbol):
        r = self.ROWS.get(symbol)
        return {"symbol": symbol, "price": r["price"], "marketCap": r["mcap"], "yearHigh": r["price"] * 1.6, "yearLow": r["price"] * 0.8, "as_of": self.as_of()} if r and self.on else None


def boot():
    import boto3
    stub = Stub()
    boto3.client = lambda *a, **k: stub
    sys.modules["fmp_bankfirst"] = types.ModuleType("fmp_bankfirst")
    sys.modules["fmp_bankfirst"].install = lambda: None
    ms = types.ModuleType("managed_secret")
    ms.managed_secret = lambda *a, **k: "test-key-not-real"
    sys.modules["managed_secret"] = ms
    book = FakeBook("bank_enrich")
    sys.modules["bank_enrich"] = book
    sys.modules.pop("lambda_function", None)
    import lambda_function as lf
    lf.S3 = stub
    lf.SP500_BACKUP = ["RICH"]
    calls = []
    lf.fetch_quote = lambda sym: (calls.append(("quote", sym)) or {"price": 300.0, "marketCap": 1e12, "yearHigh": 400.0}) if sym == "RICH" else None
    lf.fetch_balance = lambda sym: {"cashAndCashEquivalents": {"CASHCO": 2.5e8, "NETNET": 5e7, "RICH": 5e10, "CHEAPSALES": 5e7}.get(sym, 0), "shortTermInvestments": {"CASHCO": 5e7}.get(sym, 0), "longTermInvestments": 0, "totalDebt": {"CASHCO": 2e7, "NETNET": 1e7, "RICH": 1e11, "CHEAPSALES": 3e8}.get(sym, 0)}
    lf.fetch_income = lambda sym: {"revenue": {"CASHCO": 5.5e8, "NETNET": 7.5e8, "RICH": 1.2e11, "CHEAPSALES": 1.6e9}.get(sym, 0)}
    lf.fetch_cashflow = lambda sym: [{"operatingCashFlow": 1e7}] * 4
    lf.fetch_profile = lambda sym: {"sector": "Technology", "industry": "Software", "companyName": sym + " Inc"}
    return lf, stub, book, calls


def test_book_candidates_lead_the_universe_and_rows_carry_the_bank_close_and_context():
    lf, stub, book, calls = boot()
    syms, as_of = lf.book_candidates()
    assert syms == ["CASHCO", "NETNET", "CHEAPSALES"], syms      # ordered by cash/price 0.6, 0.15, 0.04; RICH fails the cheapness gates; SMALLC below $200M; SHELLISH has no revenue; FOREIGN is an ADR; cap 3
    uni = lf.get_universe()
    assert uni == ["CASHCO", "NETNET", "CHEAPSALES"], uni        # MAX_TICKERS reached by the book; the seeds would follow otherwise
    out = lf.lambda_handler({}, None)
    assert out["statusCode"] == 200, out
    doc = json.loads(stub.store[lf.S3_KEY])
    assert doc["method"] == "deep_value_screener_v1.1" and doc["stats"]["n_book_candidates"] == 3 and doc["stats"]["universe_basis"].startswith("factor book")
    rows = {r["symbol"]: r for r in doc["all_qualifying"]}
    assert "CASHCO" in rows, doc["stats"]
    c = rows["CASHCO"]
    assert c["price_source"].startswith("factor-book close as of 2026-10-25") and c["bank_context"]["altman_z"] == 4.0 and c["bank_context"]["composite_value"] == 88.0
    assert c["fundamentals"]["market_cap"] == 5e8 and c["fundamentals"]["year_high"] == 16.0 and c["fundamentals"]["net_cash_pct_of_mcap"] == 0.56
    assert c["flag"] == "DEEP_VALUE_TIER_A", c["flag"]                                   # 56% net cash, 1.1x revenue yield, OCF positive
    assert not calls                                                                     # no live quote spent: the book held every name


def test_without_the_book_the_seed_universe_and_live_quotes_stand():
    lf, stub, book, calls = boot()
    book.on = False
    assert lf.book_candidates() == ([], None)
    uni = lf.get_universe()
    assert uni == ["RICH"], uni
    out = lf.lambda_handler({}, None)
    assert out["statusCode"] == 200
    doc = json.loads(stub.store[lf.S3_KEY])
    assert doc["stats"]["n_book_candidates"] == 0 and doc["stats"]["universe_basis"].endswith("(factor book unavailable)")
    assert calls == [("quote", "RICH")]


if __name__ == "__main__":
    n = 0
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            fn()
            n += 1
            print("ok", name)
    print("%d tests passed" % n)
