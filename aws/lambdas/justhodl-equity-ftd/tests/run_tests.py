"""justhodl-equity-ftd offline tests (v1.1.0 bank fusion): the decision core with and without the factor book's
squeeze context and volume fallback.  No network, no AWS, no key.

Run: python3 aws/lambdas/justhodl-equity-ftd/tests/run_tests.py
"""
import os
import sys
import types

HERE = os.path.dirname(os.path.abspath(__file__))
SRC = os.path.join(HERE, "..", "source")
sys.path.insert(0, SRC)


class FakeBook(types.ModuleType):
    ROWS = {"GME": {"short_pct_float": 22.5, "days_to_cover": 3.4, "float_shares": 3.0e8, "short_volume_ratio": 0.48, "mcap": 9e9, "avg_volume": 4.0e6, "close": 25.5, "ret_21d": 12.1},
            "XYZ": {"short_pct_float": None, "days_to_cover": None, "float_shares": 5e7, "short_volume_ratio": None, "mcap": 4e8, "avg_volume": 900000, "close": 8.0, "ret_21d": -3.0}}
    on = True

    def available(self):
        return self.on

    def as_of(self):
        return {"fmp-bank": "2026-10-25T23:25:00+00:00", "polygon-short-interest": "2026-10-15"}

    def lookup(self, symbol, fields=None):
        r = self.ROWS.get(symbol)
        return {f: r.get(f) for f in fields} if r and fields else (dict(r) if r else None)


def boot():
    import boto3
    boto3.client = lambda *a, **k: types.SimpleNamespace(put_object=lambda **kw: None)
    boto3.resource = lambda *a, **k: types.SimpleNamespace(Table=lambda n: None)
    sys.modules["fmp_bankfirst"] = types.ModuleType("fmp_bankfirst")
    sys.modules["fmp_bankfirst"].install = lambda: None
    se = types.ModuleType("signals_emit")
    se.log_signal = lambda *a, **k: True
    sys.modules["signals_emit"] = se
    book = FakeBook("bank_enrich")
    sys.modules["bank_enrich"] = book
    sys.modules.pop("lambda_function", None)
    import lambda_function as lf
    return lf, book


def files():
    per = {"GME": {"q": 900_000, "usd": 2.2e7, "days": {"2026-10-01": 400_000, "2026-10-02": 500_000}, "desc": "GAMESTOP CORP"},
           "XYZ": {"q": 150_000, "usd": 1.2e6, "days": {"2026-10-01": 150_000}, "desc": "XYZ INC"},
           "SPY": {"q": 5_000_000, "usd": 3e9, "days": {"2026-10-01": 5_000_000}, "desc": "SPDR S&P 500 ETF TRUST"}}
    prev = {"GME": {"q": 100_000, "usd": 2.5e6, "days": {"2026-09-15": 100_000}, "desc": "GAMESTOP CORP"},
            "XYZ": {"q": 140_000, "usd": 1.1e6, "days": {"2026-09-15": 140_000}, "desc": "XYZ INC"},
            "SPY": {"q": 5_000_000, "usd": 3e9, "days": {"2026-09-15": 5_000_000}, "desc": "SPDR S&P 500 ETF TRUST"}}
    return [{"tag": "202610a", "per": per, "meta": {"n_symbols": 3}}, {"tag": "202609b", "per": prev, "meta": {"n_symbols": 3}}]


def test_rows_carry_squeeze_context_and_fall_back_to_the_books_volume_when_fmp_is_empty():
    lf, book = boot()
    fs = files()
    rows, quals = lf.evaluate(fs, lambda s: (2_000_000, 24.0, "fmp") if s == "GME" else (None, None, "none"), ctx_fn=lf.book_context)
    by = {r["t"]: r for r in rows}
    assert "SPY" not in by and by["GME"]["vol_source"] == "fmp" and by["GME"]["avg20"] == 2_000_000
    assert by["GME"]["squeeze"] == {"si_pct_float": 22.5, "dtc_si": 3.4, "float_m": 300.0, "sv_ratio": 0.48, "mcap_m": 9000.0, "ret_21d": 12.1}, by["GME"]["squeeze"]
    assert by["XYZ"]["squeeze"]["si_pct_float"] is None and by["XYZ"]["squeeze"]["float_m"] == 50.0
    # the handler's vol_price: FMP first, then the book's bank average volume + close, labelled
    lf.fmp_vol_price = lambda sym, key: (None, None)
    assert lf.vol_price("GME", "k") == (4_000_000.0, 25.5, "factor-book")
    lf.fmp_vol_price = lambda sym, key: (3_000_000, 26.0)
    assert lf.vol_price("GME", "k") == (3_000_000, 26.0, "fmp")
    rows2, _ = lf.evaluate(fs, lambda s: lf.vol_price(s, "k"), ctx_fn=lf.book_context)
    assert {r["t"]: r["vol_source"] for r in rows2} == {"GME": "fmp", "XYZ": "fmp"}
    # legacy two-tuple vol_fn and no ctx_fn still work (the decision core's old contract)
    rows3, _ = lf.evaluate(fs, lambda s: (1_000_000, 10.0))
    assert all(r["vol_source"] == "fmp" and "squeeze" not in r for r in rows3)


def test_without_the_book_nothing_is_attached_and_the_fallback_is_honest():
    lf, book = boot()
    book.on = False
    assert lf.book_context("GME") is None
    lf.fmp_vol_price = lambda sym, key: (None, None)
    assert lf.vol_price("GME", "k") == (None, None, "none")
    rows, quals = lf.evaluate(files(), lambda s: lf.vol_price(s, "k"), ctx_fn=lf.book_context)
    assert all("squeeze" not in r and r["avg20"] is None and r["dtc_peak"] is None for r in rows) and quals == []


if __name__ == "__main__":
    n = 0
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            fn()
            n += 1
            print("ok", name)
    print("%d tests passed" % n)
