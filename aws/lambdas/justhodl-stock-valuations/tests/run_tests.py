"""justhodl-stock-valuations offline test (v1.6.0 bank fusion): the HP small/mid-cap universe comes from the factor
book first (the FMP company-screener is a search endpoint no bank holds).  No network, no AWS, no key.

Run: python3 aws/lambdas/justhodl-stock-valuations/tests/run_tests.py
"""
import os
import sys
import types

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "source"))
sys.path.insert(0, os.path.join(HERE, "..", "..", "..", "shared"))
os.environ["FMP_KEY"] = "test-key-not-real"


class FakeBook(types.ModuleType):
    ROWS = [{"symbol": "SMID", "mcap": 1.2e9, "avg_volume": 400000, "sector": "Financial Services", "industry": "Banks", "exchange": "NYSE"},
            {"symbol": "BIGCO", "mcap": 5e10, "avg_volume": 1e7, "sector": "Technology", "industry": "Software", "exchange": "NASDAQ"},      # above $3B
            {"symbol": "THIN", "mcap": 9e8, "avg_volume": 20000, "sector": "Energy", "industry": "Oil", "exchange": "NYSE"},                   # illiquid
            {"symbol": "CEF1", "mcap": 5e8, "avg_volume": 300000, "sector": "Financial Services", "industry": "Closed-End Fund - Equity", "exchange": "NYSE"},
            {"symbol": "OTCO", "mcap": 5e8, "avg_volume": 300000, "sector": "Healthcare", "industry": "Biotech", "exchange": "PNK"},           # OTC
            {"symbol": "DOT.A", "mcap": 5e8, "avg_volume": 300000, "sector": "Healthcare", "industry": "Biotech", "exchange": "NYSE"}] + [
            {"symbol": "S%03d" % i, "mcap": 1e8 + i * 1e6, "avg_volume": 200000, "sector": "Industrials", "industry": "Machinery", "exchange": "NASDAQ"} for i in range(60)]
    on = True

    def available(self):
        return self.on

    def as_of(self):
        return {"fmp-bank": "2026-10-25T23:25:00+00:00"}

    def screen(self, where, fields=None, limit=None, common_only=True):
        return [dict(r) for r in self.ROWS if where(r)]


def boot():
    import boto3
    boto3.client = lambda *a, **k: types.SimpleNamespace()
    boto3.resource = lambda *a, **k: types.SimpleNamespace()
    sys.modules["fmp_bankfirst"] = types.ModuleType("fmp_bankfirst")
    sys.modules["fmp_bankfirst"].install = lambda: None
    ms = types.ModuleType("managed_secret")
    ms.managed_secret = lambda *a, **k: "test-key-not-real"
    sys.modules["managed_secret"] = ms
    book = FakeBook("bank_enrich")
    sys.modules["bank_enrich"] = book
    sys.modules.pop("lambda_function", None)
    import lambda_function as lf
    return lf, book


def test_hp_universe_comes_from_the_factor_book_first():
    lf, book = boot()
    rows, as_of = lf.hp_universe_book()
    syms = [r[0] for r in rows]
    assert syms[0] == "SMID" and len(syms) == 61, syms[:5]
    assert not {"BIGCO", "THIN", "CEF1", "OTCO", "DOT.A"} & set(syms)
    assert rows[0] == ("SMID", "Financials", 1.2e9, "Banks")                  # (symbol, sector alias, mcap, industry) - the shape the HP scorer reads
    rows2, src = lf.hp_universe()
    assert src == "factor_book_small_mid_caps" and len(rows2) == 61 and lf.DIAG[-1].startswith("hp universe: factor book 61 rows")


def test_without_the_book_the_old_ladder_runs():
    lf, book = boot()
    book.on = False
    assert lf.hp_universe_book() == ([], None)
    lf.jget = lambda url, timeout=25: []                     # screener empty -> fallback union (S3 stubs raise -> empty)
    lf.S3 = types.SimpleNamespace(get_object=lambda **kw: (_ for _ in ()).throw(KeyError("x")))
    rows, src = lf.hp_universe()
    assert rows == [] and src.startswith("fallback_union")


if __name__ == "__main__":
    n = 0
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            fn()
            n += 1
            print("ok", name)
    print("%d tests passed" % n)
