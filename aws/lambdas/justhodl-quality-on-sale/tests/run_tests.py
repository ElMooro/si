"""justhodl-quality-on-sale offline test (v1.1.0 bank fusion): the whole-market `book_screen` beside the 5-gate desk.
No network, no AWS, no key.

Run: python3 aws/lambdas/justhodl-quality-on-sale/tests/run_tests.py
"""
import os
import sys
import types

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "source"))
os.environ["FMP_KEY"] = "test-key-not-real"


class FakeBook(types.ModuleType):
    ROWS = [
        {"symbol": "ELITE", "sector": "Technology", "composite_quality": 96.0, "piotroski": 8, "roic": 0.31, "from_52w_high_pct": -27.5, "altman_z": 6.1, "mcap": 4e10, "gross_margin": 0.6, "pe": 18.0, "pt_upside_pct": 22.0, "composite_value": 55.0},
        {"symbol": "EXTENDED", "sector": "Technology", "composite_quality": 97.0, "piotroski": 8, "roic": 0.40, "from_52w_high_pct": -3.0, "altman_z": 7.0, "mcap": 9e11, "gross_margin": 0.7, "pe": 35.0, "pt_upside_pct": 5.0, "composite_value": 20.0},
        {"symbol": "JUNK", "sector": "Energy", "composite_quality": 30.0, "piotroski": 3, "roic": 0.02, "from_52w_high_pct": -60.0, "altman_z": 0.8, "mcap": 2e9, "gross_margin": 0.1, "pe": None, "pt_upside_pct": 40.0, "composite_value": 90.0},
        {"symbol": "SMALLQ", "sector": "Healthcare", "composite_quality": 95.0, "piotroski": 7, "roic": 0.25, "from_52w_high_pct": -35.0, "altman_z": 3.5, "mcap": 5e8, "gross_margin": 0.5, "pe": 12.0, "pt_upside_pct": 30.0, "composite_value": 70.0},
    ]
    on = True

    def available(self):
        return self.on

    def as_of(self):
        return {"fmp-bank": "2026-10-25T23:25:00+00:00", "polygon-tape": "2026-10-24"}

    def screen(self, where, fields=None, limit=None, common_only=True):
        return [dict(r) for r in self.ROWS if where(r)]


def boot():
    import boto3
    boto3.client = lambda *a, **k: types.SimpleNamespace()
    sys.modules["fmp_bankfirst"] = types.ModuleType("fmp_bankfirst")
    sys.modules["fmp_bankfirst"].install = lambda: None
    book = FakeBook("bank_enrich")
    sys.modules["bank_enrich"] = book
    sys.modules.pop("lambda_function", None)
    import lambda_function as lf
    return lf, book


def test_book_screen_asks_the_whole_market_the_quality_on_sale_question():
    lf, book = boot()
    bs = lf.book_screen()
    assert bs["available"] and bs["n"] == 1 and bs["candidates"][0]["ticker"] == "ELITE", bs       # EXTENDED not on sale, JUNK not quality, SMALLQ below $1B
    c = bs["candidates"][0]
    assert c == {"ticker": "ELITE", "sector": "Technology", "quality_pct": 96.0, "piotroski": 8, "roic_pct": 31.0, "off_52w_high_pct": -27.5, "altman_z": 6.1, "mcap_b": 40.0, "pe": 18.0, "pt_upside_pct": 22.0, "value_pct": 55.0}, c
    assert bs["as_of"]["fmp-bank"].startswith("2026-10-25") and "composite_quality >= 90" in bs["rule"] and bs["source"].startswith("factor book")
    book.on = False
    assert lf.book_screen() == {"available": False}


if __name__ == "__main__":
    test_book_screen_asks_the_whole_market_the_quality_on_sale_question()
    print("ok test_book_screen_asks_the_whole_market_the_quality_on_sale_question")
    print("1 tests passed")
