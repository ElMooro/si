"""justhodl-magic-formula offline tests (v1.1.0 bank fusion): whole-market universe from the factor book, bank-close
prices, Greenblatt ranking unchanged.  No network, no AWS, no key.

Run: python3 aws/lambdas/justhodl-magic-formula/tests/run_tests.py
"""
import json
import os
import sys
import types

HERE = os.path.dirname(os.path.abspath(__file__))
SRC = os.path.join(HERE, "..", "source")
sys.path.insert(0, SRC)
os.environ["FMP_KEY"] = "test-key-not-real"
os.environ["MF_BOOK_CANDIDATES"] = "3"


class S3Stub:
    def __init__(self):
        self.store = {}

    def put_object(self, Bucket, Key, Body, **kw):
        self.store[Key] = Body


class FakeBook(types.ModuleType):
    """Stands in for aws/shared/bank_enrich: a tiny whole-market matrix."""
    ROWS = [
        {"symbol": "AAPL", "sector": "Technology", "industry": "Consumer Electronics", "mcap": 3e12, "earnings_yield": 0.03, "roic": 0.5},
        {"symbol": "SMALL", "sector": "Industrials", "industry": "Machinery", "mcap": 4e8, "earnings_yield": 0.15, "roic": 0.4},
        {"symbol": "MICRO", "sector": "Industrials", "industry": "Machinery", "mcap": 5e7, "earnings_yield": 0.5, "roic": 0.9},      # below the floor
        {"symbol": "BANK", "sector": "Financial Services", "industry": "Banks", "mcap": 5e10, "earnings_yield": 0.12, "roic": 0.2},   # excluded sector
        {"symbol": "NEG", "sector": "Energy", "industry": "Oil & Gas", "mcap": 1e9, "earnings_yield": -0.1, "roic": 0.1},            # negative EY
        {"symbol": "MIDQ", "sector": "Healthcare", "industry": "Devices", "mcap": 2e9, "earnings_yield": 0.08, "roic": 0.3},
        {"symbol": "LOWQ", "sector": "Healthcare", "industry": "Devices", "mcap": 2e9, "earnings_yield": 0.02, "roic": 0.05},
    ]
    on = True

    def available(self):
        return self.on

    def as_of(self):
        return {"fmp-bank": "2026-10-25T23:25:00+00:00", "polygon-tape": "2026-10-24"}

    def screen(self, where, fields=None, limit=None, common_only=True):
        return [dict(r) for r in self.ROWS if where(r)]

    def quote(self, symbol):
        if not self.on:
            return None
        for r in self.ROWS:
            if r["symbol"] == symbol:
                return {"symbol": symbol, "price": 100.0, "marketCap": r["mcap"], "yearHigh": 120.0, "yearLow": 80.0, "sector": r["sector"], "industry": r["industry"], "as_of": self.as_of(), "source": "factor-book"}
        return None


def boot():
    import boto3
    s3 = S3Stub()
    boto3.client = lambda *a, **k: s3
    sys.modules["fmp_bankfirst"] = types.ModuleType("fmp_bankfirst")
    sys.modules["fmp_bankfirst"].install = lambda: None
    book = FakeBook("bank_enrich")
    sys.modules["bank_enrich"] = book
    sys.modules.pop("lambda_function", None)
    import lambda_function as lf
    lf.s3 = s3
    calls = []

    def fake_fmp(path):
        calls.append(path)
        if path == "sp500-constituent":
            return [{"symbol": "AAPL", "sector": "Information Technology", "subSector": "Hardware"}, {"symbol": "JPM", "sector": "Financials", "subSector": "Banks"}]
        sym = path.split("symbol=")[1].split("&")[0]
        if path.startswith("income-statement"):
            return [{"operatingIncome": {"AAPL": 120e9, "SMALL": 60e6, "MIDQ": 150e6, "LOWQ": 40e6, "JPM": 5e10}.get(sym, 0), "date": "2025-09-30"}]
        if path.startswith("balance-sheet-statement"):
            return [{"totalDebt": 10e9, "cashAndShortTermInvestments": 5e9, "totalStockholdersEquity": 50e9}] if sym == "AAPL" else [{"totalDebt": 1e8, "cashAndShortTermInvestments": 5e7, "totalStockholdersEquity": 2e8}]
        if path.startswith("profile"):
            return [{"sector": None, "industry": None}]
        if path.startswith("quote"):
            return [{"price": 50.0, "marketCap": 3e12 if sym == "AAPL" else 1e9}]
        return None
    lf.fmp = fake_fmp
    lf.telegram_alert = lambda text: None
    return lf, s3, book, calls


def test_universe_is_sp500_plus_the_books_whole_market_candidates_and_prices_are_the_bank_close():
    lf, s3, book, calls = boot()
    uni, n_book, as_of = lf.get_universe()
    # candidates: AAPL, SMALL, MIDQ, LOWQ pass (>= $200M, allowed sector, EY > 0, ROIC > 0); pre-rank keeps the best 3
    assert n_book == 3 and as_of["fmp-bank"].startswith("2026-10-25"), (n_book, as_of)
    assert set(uni) == {"AAPL", "JPM", "SMALL", "MIDQ"}, uni            # LOWQ ranked last and cut; MICRO/BANK/NEG filtered
    assert uni["JPM"] == ("Financials", "Banks") and uni["SMALL"] == ("Industrials", "Machinery")
    out = lf.lambda_handler({}, None)
    assert out["statusCode"] == 200, out
    payload = json.loads(s3.store[lf.S3_KEY])
    assert payload["version"] == "1.1.0" and payload["n_book_candidates"] == 3 and payload["n_universe"] == 4
    assert "factor book" in payload["universe"] and "1,200" not in payload["universe"]
    tickers = {r["ticker"]: r for r in payload["all_eligible"]}
    assert "JPM" not in tickers                                            # Greenblatt's exclusion still applies to the S&P list
    assert set(tickers) == {"AAPL", "SMALL", "MIDQ"}, tickers.keys()
    assert tickers["SMALL"]["price"] == 100.0 and tickers["SMALL"]["market_cap_usd"] == 4e8 and tickers["SMALL"]["price_source"].startswith("factor-book close as of 2026-10-25")
    assert not any(c.startswith("quote") for c in calls)                   # no live quote spent when the book has the name
    # ranking: EY = EBIT/EV with the book's market cap; SMALL: 60e6 / (4e8 + 1e8 - 5e7) = 0.133; AAPL: 120e9 / (3e12 + 5e9) ~ 0.04
    assert tickers["SMALL"]["rank_earnings_yield"] == 1 and tickers["AAPL"]["rank_roic"] == 1, (tickers["SMALL"], tickers["AAPL"])
    assert all(r["magic_rank"] == 4 for r in payload["top_30"]), [(r["ticker"], r["magic_rank"]) for r in payload["top_30"]]   # a three-way tie in this fixture
    assert payload["methodology"]["price_basis"].startswith("factor-book bank close")


def test_without_the_book_the_engine_is_the_sp500_screen_with_live_quotes():
    lf, s3, book, calls = boot()
    book.on = False
    uni, n_book, as_of = lf.get_universe()
    assert n_book == 0 and as_of is None and set(uni) == {"AAPL", "JPM"}
    out = lf.lambda_handler({}, None)
    assert out["statusCode"] == 200
    payload = json.loads(s3.store[lf.S3_KEY])
    assert payload["universe"].endswith("(factor book unavailable)") and payload["n_book_candidates"] == 0
    aapl = next(r for r in payload["all_eligible"] if r["ticker"] == "AAPL")
    assert aapl["price_source"] == "live quote" and aapl["price"] == 50.0 and any(c.startswith("quote") for c in calls)


if __name__ == "__main__":
    n = 0
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            fn()
            n += 1
            print("ok", name)
    print("%d tests passed" % n)
