"""justhodl-eps-revision-velocity offline test (v1.1 bank fusion): the analyst-covered market from the factor book
leads the universe and the quote comes from the bank close.  No network, no AWS, no key.

Run: python3 aws/lambdas/justhodl-eps-revision-velocity/tests/run_tests.py
"""
import os
import sys
import types

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "source"))
os.environ["MAX_TICKERS"] = "2"


class FakeBook(types.ModuleType):
    ROWS = [{"symbol": "BIG", "pt_n_1m": 20, "mcap": 5e11}, {"symbol": "MID", "pt_n_1m": 5, "mcap": 4e9}, {"symbol": "THIRD", "pt_n_1m": 3, "mcap": 1e9}, {"symbol": "UNCOVERED", "pt_n_1m": 1, "mcap": 9e9}, {"symbol": "TINY", "pt_n_1m": 8, "mcap": 1e8}]
    on = True

    def available(self):
        return self.on

    def as_of(self):
        return {"fmp-bank": "2026-10-25T23:25:00+00:00"}

    def screen(self, where, fields=None, limit=None, common_only=True):
        return [dict(r) for r in self.ROWS if where(r)]

    def quote(self, symbol):
        r = next((x for x in self.ROWS if x["symbol"] == symbol), None)
        return {"symbol": symbol, "price": 50.0, "marketCap": r["mcap"], "name": symbol + " Co", "sector": "Tech", "as_of": self.as_of()} if r and self.on else None


def boot():
    import boto3
    boto3.client = lambda *a, **k: types.SimpleNamespace(get_object=lambda **kw: (_ for _ in ()).throw(KeyError("x")))
    sys.modules["fmp_bankfirst"] = types.ModuleType("fmp_bankfirst")
    sys.modules["fmp_bankfirst"].install = lambda: None
    ms = types.ModuleType("managed_secret")
    ms.managed_secret = lambda *a, **k: "test-key-not-real"
    sys.modules["managed_secret"] = ms
    book = FakeBook("bank_enrich")
    sys.modules["bank_enrich"] = book
    sys.modules.pop("lambda_function", None)
    import lambda_function as lf
    lf.SP500_BACKUP = ["SEED"]
    return lf, book


def test_book_candidates_lead_and_quotes_are_the_bank_close():
    lf, book = boot()
    syms, as_of = lf.book_candidates()
    assert syms == ["BIG", "MID"] and as_of["fmp-bank"].startswith("2026-10-25"), syms      # >= 3 analysts, >= $300M, largest first, cap 2
    uni = lf.get_universe()
    assert uni[:2] == ["BIG", "MID"] and getattr(lf.get_universe, "book") == (2, as_of)
    q = lf.book_quote("MID")
    assert q["price"] == 50.0 and q["marketCap"] == 4e9 and lf.book_quote("NOPE") is None
    book.on = False
    assert lf.book_candidates() == ([], None) and lf.book_quote("MID") is None
    uni2 = lf.get_universe()
    assert uni2 == ["SEED"] and getattr(lf.get_universe, "book") == (0, None)


if __name__ == "__main__":
    test_book_candidates_lead_and_quotes_are_the_bank_close()
    print("ok test_book_candidates_lead_and_quotes_are_the_bank_close")
    print("1 tests passed")
