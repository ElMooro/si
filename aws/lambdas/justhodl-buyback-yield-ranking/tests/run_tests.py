"""justhodl-buyback-yield-ranking offline tests (v1.1.0 bank fusion): whole-market candidates from the factor book, quote
facts at the bank close, TTM cash flows from the FMP statement bank, labels on every row; with the banks absent the
master-ranker / live FMP path runs unchanged.  No network, no AWS, no key.

Run: python3 aws/lambdas/justhodl-buyback-yield-ranking/tests/run_tests.py
"""
import io
import json
import os
import sys
import types

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "source"))
os.environ["FMP_KEY"] = "test-key-not-real"
os.environ.pop("TELEGRAM_BOT_TOKEN", None)


class FakeS3:
    def __init__(self):
        self.store = {"data/master-ranker.json": json.dumps({"picks": [{"ticker": "AAPL"}, {"ticker": "LIVE"}]}).encode()}
        self.puts = {}

    def get_object(self, Bucket, Key):
        if Key not in self.store:
            raise KeyError(Key)
        return {"Body": io.BytesIO(self.store[Key])}

    def put_object(self, Bucket, Key, Body, **kw):
        self.puts[Key] = json.loads(Body)


class FakeSSM:
    def get_parameter(self, **k):
        raise RuntimeError("none")

    def put_parameter(self, **k):
        return None


def q(cf_rows):
    return [{"commonStockRepurchased": -r[0], "commonStockIssued": r[1], "operatingCashFlow": r[2], "capitalExpenditure": -r[3]} for r in cf_rows]


class FakeBook(types.ModuleType):
    on = True
    # matrix rows: AAPL (big buyer), SMALL (below $2B), DILUTER (issues more than it buys), ETFX (excluded by common_only), ADRX (adr)
    rows = {"AAPL": {"mcap": 3.9e12, "price": 262.5, "pe": 28.0, "shares_growth": -0.03, "buyback_growth": 0.1, "altman_z": 9.1, "piotroski": 7, "fcf_yield": 0.03, "pt_upside_pct": 8.0, "short_pct_float": 0.7, "composite_quality": 97.0, "composite_value": 20.0},
            "SMALL": {"mcap": 1.5e9, "price": 10.0, "pe": 12.0, "shares_growth": -0.05, "buyback_growth": None},
            "DILUTER": {"mcap": 5e9, "price": 50.0, "pe": 25.0, "shares_growth": 0.04, "buyback_growth": None},
            "BUYR": {"mcap": 8e9, "price": 80.0, "pe": 14.0, "shares_growth": -0.06, "buyback_growth": 0.2, "altman_z": 4.0, "piotroski": 8},
            "ETFX": {"mcap": 9e10, "price": 100.0, "pe": None, "shares_growth": -0.1, "buyback_growth": None, "is_etf": True},
            "ADRX": {"mcap": 9e10, "price": 100.0, "pe": 10.0, "shares_growth": -0.1, "buyback_growth": 0.3, "is_adr": True}}

    def available(self):
        return self.on

    def as_of(self):
        return {"fmp-bank": "2026-10-25T23:40:00+00:00", "tape": "2026-10-24"} if self.on else None

    def _rows(self, common_only=True):
        out = []
        for s, r in self.rows.items():
            if common_only and r.get("is_etf"):
                continue
            out.append(dict(r, symbol=s))
        return out

    def lookup(self, symbol, fields=None):
        r = self.rows.get(str(symbol).upper())
        return None if r is None else {k: r.get(k) for k in (fields or r)}

    def screen(self, where, fields=None, limit=None, common_only=True):
        hits = [r for r in self._rows(common_only) if where(r)]
        return hits[:limit] if limit else hits

    def prescreen(self, where, order_by=None, descending=True, limit=1500, fields=None, common_only=True):
        """Like the SDK: a list of SYMBOLS, ordered, capped."""
        hits = self.screen(where, fields, None, common_only)
        if order_by is not None:
            keyf = order_by if callable(order_by) else (lambda r, f=order_by: r.get(f))
            hits = sorted([r for r in hits if keyf(r) is not None], key=keyf, reverse=descending)
        return [r["symbol"] for r in hits[:limit]]


class FakeBank(types.ModuleType):
    on = True
    cf = {"AAPL": q([(25e9, 0, 30e9, 3e9), (24e9, 0, 29e9, 3e9), (23e9, 0, 28e9, 2e9), (22e9, 0, 27e9, 2e9)]),
          "BUYR": q([(200e6, 50e6, 500e6, 100e6), (150e6, 0, 450e6, 100e6), (0, 0, 400e6, 90e6), (100e6, 0, 420e6, 90e6)])}

    def statements(self, path, symbol, period="annual", limit=None, years=None):
        assert path == "cash-flow-statement" and period == "quarter" and limit == 4
        return self.cf.get(symbol) if self.on else None


def boot(book_on=True, bank_on=True):
    import boto3
    s3, ssm = FakeS3(), FakeSSM()
    boto3.client = lambda name, *a, **k: ssm if name == "ssm" else s3
    sys.modules["fmp_bankfirst"] = types.ModuleType("fmp_bankfirst")
    sys.modules["fmp_bankfirst"].install = lambda: None
    book = FakeBook("bank_enrich")
    book.on = book_on
    sys.modules["bank_enrich"] = book
    bank = FakeBank("fmp_bank")
    bank.on = bank_on
    sys.modules["fmp_bank"] = bank
    sys.modules.pop("lambda_function", None)
    import lambda_function as lf
    lf._book, lf._bank, lf.s3, lf.ssm = book, bank, s3, ssm
    lf.live = []

    def http_get(url, timeout=15, retries=2):
        lf.live.append(url)
        raise RuntimeError("HTTP 429 Bandwidth Limit Reach")          # the live key answers 429 today
    lf.http_get = http_get
    return lf, s3, book, bank


def test_book_universe_bank_cash_flows_and_labels():
    lf, s3, book, bank = boot()
    r = lf.lambda_handler({}, None)
    assert json.loads(r["body"])["ok"] is True
    out = s3.puts["data/buyback-yield-ranking.json"]
    assert out["version"] == "1.1.0" and out["engine"] == "buyback-yield-ranking"
    # candidates: AAPL + BUYR (>= $2B, shrinking count / repurchases, common, not ADR); SMALL / DILUTER / ETFX / ADRX excluded
    assert out["n_book_candidates"] == 2 and out["universe_basis"].startswith("factor book: US common stocks >= $2B")
    assert out["universe_size"] == 3                                      # AAPL, BUYR + the curated LIVE name
    ranked = {r["ticker"]: r for r in out["all_ranked"]}
    assert set(ranked) == {"AAPL", "BUYR"}, ranked.keys()                 # (a P/E above 30 would still drop a name - gate unchanged)
    a = ranked["AAPL"]
    assert a["price_source"] == "factor-book (bank close)" and a["cashflow_source"] == "fmp-bank cash-flow-statement quarterly"
    assert a["market_cap_usd"] == 3.9e12 and a["pe"] == 28.0 and a["ttm_buybacks_gross_usd"] == 94_000_000_000 and a["quarters_with_buybacks"] == 4
    assert a["ttm_buyback_yield_gross_pct"] == 2.41 and a["fcf_coverage_ratio"] == 1.11 and a["bank_context"]["piotroski"] == 7 and a["bank_context"]["source"] == "factor-book"
    b = ranked["BUYR"]
    assert b["quarters_with_buybacks"] == 3 and b["ttm_buybacks_net_usd"] == 400_000_000 and b["composite_score"] > a["composite_score"]   # 5.6% gross yield beats 2.4%
    assert out["price_source_counts"] == {"factor-book (bank close)": 2} and out["book_as_of"]["tape"] == "2026-10-24"
    # the book's names cost no live call; only the curated LIVE name (not in the book) went live and failed honestly
    assert all("LIVE" in u for u in lf.live) and lf.live, lf.live
    assert out["top_20_ranked"][0]["ticker"] == "BUYR" and out["trade_tickets"][0]["ticker"] == "BUYR" and out["n_tickets"] == 2


def test_without_the_banks_the_master_ranker_live_path_runs_unchanged():
    lf, s3, book, bank = boot(book_on=False, bank_on=False)

    def http_get(url, timeout=15, retries=2):
        if "quote" in url:
            return json.dumps([{"price": 250.0, "marketCap": 3.8e12, "pe": 29.0, "name": "Apple Inc."}] if "AAPL" in url else [{"price": 1.0, "marketCap": 5e9, "pe": 10.0, "name": "Live Co"}])
        return json.dumps(q([(10e9, 1e9, 20e9, 2e9), (10e9, 1e9, 20e9, 2e9), (10e9, 1e9, 20e9, 2e9), (10e9, 1e9, 20e9, 2e9)]))
    lf.http_get = http_get
    lf.lambda_handler({}, None)
    out = s3.puts["data/buyback-yield-ranking.json"]
    assert out["n_book_candidates"] == 0 and out["universe_basis"] == "master-ranker list (2); factor book unavailable" and out["book_as_of"] is None
    ranked = {r["ticker"]: r for r in out["all_ranked"]}
    assert set(ranked) == {"AAPL", "LIVE"} and ranked["AAPL"]["price_source"] == "fmp live quote" and ranked["AAPL"]["name"] == "Apple Inc."
    assert ranked["AAPL"]["cashflow_source"] == "fmp live cash-flow-statement" and ranked["AAPL"]["bank_context"] is None
    assert ranked["AAPL"]["ttm_buybacks_gross_usd"] == 40_000_000_000 and ranked["AAPL"]["ttm_buybacks_net_usd"] == 36_000_000_000


if __name__ == "__main__":
    test_book_universe_bank_cash_flows_and_labels()
    print("ok test_book_universe_bank_cash_flows_and_labels")
    test_without_the_banks_the_master_ranker_live_path_runs_unchanged()
    print("ok test_without_the_banks_the_master_ranker_live_path_runs_unchanged")
    print("2 tests passed")
