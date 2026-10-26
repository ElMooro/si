"""justhodl-dividend-growth offline tests (v1.1.0 bank fusion): whole-market dividend payers + facts from the factor book,
dividend history from the FMP walker bank, labels; with the banks absent the stock-screener / live FMP path runs
unchanged.  No network, no AWS, no key.

Run: python3 aws/lambdas/justhodl-dividend-growth/tests/run_tests.py
"""
import io
import json
import os
import sys
import types
from datetime import datetime, timezone

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "source"))

YEAR = datetime.now(timezone.utc).year


def divs(start, growth, years=12, cut_year=None):
    """Quarterly per-payment records, `growth` per year, ending last year; an optional 40% cut in `cut_year`."""
    out, amt = [], start
    for y in range(YEAR - years, YEAR):
        if cut_year and y == cut_year:
            amt *= 0.6
        for m in (2, 5, 8, 11):
            out.append({"date": "%d-%02d-15" % (y, m), "adjDividend": round(amt / 4, 4), "dividend": round(amt / 4, 4)})
        amt *= (1 + growth)
    return list(reversed(out))


SCREENER = {"stocks": [
    {"symbol": "JNJ", "name": "Johnson & Johnson", "sector": "Healthcare", "industry": "Drug Manufacturers", "price": 160.0, "marketCap": 3.9e11,
     "dividendYield": 3.1, "fcfYieldCalc": 5.0, "peRatio": 18.0, "piotroski": 7, "altmanZ": 4.0, "debtToEquity": 0.5},
    {"symbol": "SCRN", "name": "Screener Only Co", "sector": "Utilities", "industry": "Utilities", "price": 50.0, "marketCap": 2e10,
     "dividendYield": 4.0, "fcfYieldCalc": 6.0, "peRatio": 15.0, "piotroski": 6, "altmanZ": 3.0, "debtToEquity": 1.0},
]}


class FakeS3:
    def __init__(self):
        self.store = {"screener/data.json": json.dumps(SCREENER).encode()}
        self.puts = {}

    def get_object(self, Bucket, Key):
        if Key not in self.store:
            raise KeyError(Key)
        return {"Body": io.BytesIO(self.store[Key])}

    def put_object(self, Bucket, Key, Body, **kw):
        self.puts[Key] = json.loads(Body)


def brow(dy, mcap, fcf=0.05, pe=18.0, pio=7, az=4.0, de=0.5, sector="Healthcare", **extra):
    return dict({"dividend_yield": dy, "mcap": mcap, "price": 100.0, "fcf_yield": fcf, "pe": pe, "piotroski": pio, "altman_z": az,
                 "debt_to_equity": de, "sector": sector, "industry": "x", "payout_ratio": 0.5, "short_pct_float": 1.2, "pt_upside_pct": 6.0, "composite_quality": 88.0}, **extra)


class FakeBook(types.ModuleType):
    on = True
    rows = {"JNJ": brow(0.031, 3.9e11),                       # in both: facts from the book, name from the screener
            "BOOK": brow(0.028, 1.2e11, sector="Consumer Defensive"),   # book-only compounder
            "TRAP": brow(0.085, 5e9, fcf=0.02, pio=2, az=1.0, sector="Energy"),      # 8.5% yield + distress + cut -> trap
            "TOKEN": brow(0.002, 9e11),                       # 0.2% -> below MIN_YIELD
            "NOPAY": brow(None, 9e11),                        # no dividend
            "ETFX": brow(0.03, 9e11, is_etf=True)}            # excluded by common_only

    def available(self):
        return self.on

    def as_of(self):
        return {"fmp-bank": "2026-10-25T23:40:00+00:00", "tape": "2026-10-24"} if self.on else None

    def screen(self, where, fields=None, limit=None, common_only=True):
        out = []
        for s, r in self.rows.items():
            rr = dict(r, symbol=s)
            if common_only and rr.get("is_etf"):
                continue
            try:
                ok = where(rr)
            except Exception:
                ok = False
            if ok:
                out.append(rr)
        return out


class FakeBank(types.ModuleType):
    on = True
    hist = {"JNJ": divs(2.0, 0.06), "BOOK": divs(1.0, 0.08), "TRAP": divs(3.0, 0.02, cut_year=YEAR - 2)}

    def walker(self, dataset, entity=None, period=None, day=None, exchange=None):
        assert dataset == "dividends" and entity
        return self.hist.get(entity) if self.on else None


def boot(book_on=True, bank_on=True):
    import boto3
    s3 = FakeS3()
    boto3.client = lambda *a, **k: s3
    sys.modules["fmp_bankfirst"] = types.ModuleType("fmp_bankfirst")
    sys.modules["fmp_bankfirst"].install = lambda: None
    ms = types.ModuleType("managed_secret")
    ms.managed_secret = lambda *a, **k: "x"
    sys.modules["managed_secret"] = ms
    book = FakeBook("bank_enrich")
    book.on = book_on
    sys.modules["bank_enrich"] = book
    bank = FakeBank("fmp_bank")
    bank.on = bank_on
    sys.modules["fmp_bank"] = bank
    sys.modules.pop("lambda_function", None)
    import lambda_function as lf
    lf._book, lf._bank, lf.s3 = book, bank, s3
    lf.live = []
    lf.fetch_dividends = lambda symbol: (lf.live.append(symbol), (symbol, None))[1]       # live FMP answers nothing (429 today)
    return lf, s3, book, bank


def test_book_payers_bank_history_and_labels():
    lf, s3, book, bank = boot()
    r = lf.lambda_handler({}, None)
    assert json.loads(r["body"])["ok"] is True
    out = s3.puts["data/dividend-growth.json"]
    assert out["version"] == "1.1.0" and out["schema_version"] == "dividend-growth-1.0" and out["ok"] is True
    # universe: JNJ, BOOK, TRAP from the book (TOKEN below the band, NOPAY no dividend, ETFX excluded) + SCRN from the screener
    assert out["n_book_candidates"] == 3 and out["n_evaluated"] == 4 and out["universe_basis"].startswith("factor book: US common dividend payers 0.5-9.0% yield (3 in the book, 3 in the top 700)")
    assert out["facts_source_counts"] == {"factor-book": 3, "stock-screener": 1}
    assert out["history_source_counts"] == {"fmp-walker bank": 3, "unavailable": 1} and lf.live == ["SCRN"]     # only the screener-only name went live
    comp = {c["symbol"]: c for c in out["compounders"]}
    assert set(comp) == {"JNJ", "BOOK"}, comp.keys()
    j = comp["JNJ"]
    assert j["name"] == "Johnson & Johnson" and j["facts_source"] == "factor-book (bank close)" and j["history_source"] == "fmp-walker bank"
    assert j["dividend_yield_pct"] == 3.1 and j["fcf_yield_pct"] == 5.0 and j["piotroski"] == 7 and j["growth_streak_years"] >= 10 and j["tier"] == "ARISTOCRAT"
    assert j["bank_context"]["composite_quality"] == 88.0 and j["bank_context"]["source"] == "factor-book" and j["bank_context"]["as_of"]["tape"] == "2026-10-24"
    assert comp["BOOK"]["name"] == "" and comp["BOOK"]["div_cagr_5y_pct"] == 8.0
    traps = {t["symbol"]: t for t in out["yield_traps"]}
    assert set(traps) == {"TRAP"} and traps["TRAP"]["tier"] == "YIELD TRAP" and "has cut the dividend before" in traps["TRAP"]["why"]
    assert out["source"].startswith("factor book + stock-screener universe") and out["book_as_of"]["fmp-bank"].startswith("2026-10-25")
    assert out["summary"]["n_aristocrats"] == 2 and out["n_compounders"] == 2 and out["n_with_history"] == 3


def test_without_the_banks_the_screener_live_path_runs_unchanged():
    lf, s3, book, bank = boot(book_on=False, bank_on=False)
    lf.fetch_dividends = lambda symbol: (symbol, divs(2.0, 0.06) if symbol == "JNJ" else None)
    lf.lambda_handler({}, None)
    out = s3.puts["data/dividend-growth.json"]
    assert out["n_book_candidates"] == 0 and out["universe_basis"].endswith("factor book unavailable") and out["book_as_of"] is None
    assert out["n_evaluated"] == 2 and out["facts_source_counts"] == {"stock-screener": 2} and out["history_source_counts"] == {"fmp live": 1, "unavailable": 1}
    comp = {c["symbol"]: c for c in out["compounders"]}
    assert set(comp) == {"JNJ"} and comp["JNJ"]["facts_source"] == "stock-screener" and comp["JNJ"]["history_source"] == "fmp live" and comp["JNJ"]["bank_context"] is None
    assert out["source"] == "stock-screener universe + FMP /stable/dividends history (factor book unavailable)"
    # no screener and no book -> the old 500, unchanged
    s3.store.clear()
    r = lf.lambda_handler({}, None)
    assert r["statusCode"] == 500 and r["body"].startswith("screener read failed")


if __name__ == "__main__":
    test_book_payers_bank_history_and_labels()
    print("ok test_book_payers_bank_history_and_labels")
    test_without_the_banks_the_screener_live_path_runs_unchanged()
    print("ok test_without_the_banks_the_screener_live_path_runs_unchanged")
    print("2 tests passed")
