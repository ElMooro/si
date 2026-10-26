"""justhodl-divcut-warning offline tests (v1.1.0 bank fusion): whole-market at-risk universe from the factor book,
bank-close quotes with context, the 5-signal model unchanged.  No network, no AWS, no key.

Run: python3 aws/lambdas/justhodl-divcut-warning/tests/run_tests.py
"""
import json
import os
import sys
import types

HERE = os.path.dirname(os.path.abspath(__file__))
SRC = os.path.join(HERE, "..", "source")
sys.path.insert(0, SRC)
os.environ["FMP_KEY"] = "test-key-not-real"
os.environ["TELEGRAM_BOT_TOKEN"] = "x"
os.environ["DIVCUT_BOOK_CANDIDATES"] = "2"


class Stub:
    def __init__(self):
        self.store = {}

    def put_object(self, Bucket, Key, Body, **kw):
        self.store[Key] = Body

    def get_parameter(self, **kw):
        raise KeyError("no state")

    def put_parameter(self, **kw):
        return {}


class FakeBook(types.ModuleType):
    ROWS = {
        "RISKY": {"symbol": "RISKY", "mcap": 5e9, "dividend_yield": 0.09, "payout_ratio": 1.3, "net_debt_ebitda": 5.5, "altman_z": 1.1, "piotroski": 2, "short_pct_float": 12.0, "pt_upside_pct": -5.0},
        "SAFE": {"symbol": "SAFE", "mcap": 8e10, "dividend_yield": 0.035, "payout_ratio": 0.4, "net_debt_ebitda": 1.0, "altman_z": 5.0, "piotroski": 8, "short_pct_float": 1.0, "pt_upside_pct": 10.0},
        "HIPAY": {"symbol": "HIPAY", "mcap": 2e9, "dividend_yield": 0.05, "payout_ratio": 0.9, "net_debt_ebitda": 2.0, "altman_z": 2.5, "piotroski": 5, "short_pct_float": 3.0, "pt_upside_pct": 2.0},
        "TINYDIV": {"symbol": "TINYDIV", "mcap": 2e8, "dividend_yield": 0.1, "payout_ratio": 2.0, "net_debt_ebitda": 6.0, "altman_z": 0.5, "piotroski": 1, "short_pct_float": 20.0, "pt_upside_pct": -20.0},
        "MO": {"symbol": "MO", "mcap": 9e10, "dividend_yield": 0.07, "payout_ratio": 0.8, "net_debt_ebitda": 2.2, "altman_z": 1.5, "piotroski": 6, "short_pct_float": 2.0, "pt_upside_pct": 4.0},
    }
    on = True

    def available(self):
        return self.on

    def as_of(self):
        return {"fmp-bank": "2026-10-25T23:25:00+00:00"}

    def screen(self, where, fields=None, limit=None, common_only=True):
        return [dict(r) for r in self.ROWS.values() if where(r)]

    def lookup(self, symbol, fields=None):
        r = self.ROWS.get(symbol)
        return {f: r.get(f) for f in fields} if r and fields else (dict(r) if r else None)

    def quote(self, symbol):
        r = self.ROWS.get(symbol)
        return {"symbol": symbol, "name": symbol + " Corp", "price": 40.0, "marketCap": r["mcap"], "as_of": self.as_of()} if r and self.on else None


def boot():
    import boto3
    stub = Stub()
    boto3.client = lambda *a, **k: stub
    sys.modules["fmp_bankfirst"] = types.ModuleType("fmp_bankfirst")
    sys.modules["fmp_bankfirst"].install = lambda: None
    ms = types.ModuleType("managed_secret")
    ms.managed_secret = lambda *a, **k: ""
    sys.modules["managed_secret"] = ms
    book = FakeBook("bank_enrich")
    sys.modules["bank_enrich"] = book
    sys.modules.pop("lambda_function", None)
    import lambda_function as lf
    lf.s3 = stub
    lf.ssm = stub
    lf.UNIVERSE = ["MO", "T"]                      # a short curated list for the test
    lf.get_state = lambda: {}
    lf.set_state = lambda st: None
    lf.telegram_send = lambda text: None
    calls = []

    def fake_fmp_get(path, params=None):
        calls.append((path, params))
        sym = (params or {}).get("symbol")
        if path == "quote":
            return [{"price": 20.0, "marketCap": 1.2e11, "yield": 6.5, "name": "AT&T"}] if sym == "T" else None
        if path == "ratios-ttm":
            return [{"payoutRatioTTM": {"RISKY": 1.3, "MO": 0.8, "T": 1.2, "HIPAY": 0.9}.get(sym, 0.3), "netDebtToEBITDATTM": {"RISKY": 5.5, "MO": 2.2, "T": 3.0, "HIPAY": 2.0}.get(sym, 1.0)}]
        if path == "cash-flow-statement":
            return [{"freeCashFlow": 1e9 if sym != "RISKY" else 1e8, "dividendsPaid": -5e8}]
        if path == "analyst-estimates":
            return [{"estimatedEpsAvg": 1.0}, {"estimatedEpsAvg": 1.3 if sym == "RISKY" else 1.0}]
        return None
    lf.fmp_get = fake_fmp_get
    return lf, stub, book, calls


def test_universe_adds_the_books_at_risk_payers_and_quotes_come_from_the_bank_close_with_context():
    lf, stub, book, calls = boot()
    syms, as_of = lf.book_candidates()
    assert syms == ["RISKY", "MO"], syms          # RISKY 9% + payout 130%; MO 7% + payout 80%; HIPAY cut by the cap of 2; SAFE no marker; TINYDIV below $1B
    out = lf.lambda_handler({}, None)
    assert out["statusCode"] == 200, out
    doc = json.loads(stub.store[lf.S3_KEY])
    assert doc["version"] == "v1.1.0" and doc["summary"]["universe_size"] == 3 and "factor book" in doc["summary"]["universe_basis"]   # MO, T curated + RISKY
    w = {x["ticker"]: x for x in doc["warnings"]}
    assert "RISKY" in w and w["RISKY"]["n_signals"] == 5, w.get("RISKY")                 # payout 130, FCF 0.2x, EPS -23%, debt 5.5x, yield 9%
    assert w["RISKY"]["price_source"].startswith("factor-book close as of 2026-10-25") and w["RISKY"]["bank_context"]["altman_z"] == 1.1
    assert w["RISKY"]["yield_pct"] == 9.0 and w["RISKY"]["mcap_billions"] == 5.0
    assert "T" in w and w["T"]["price_source"] == "live quote" and w["T"]["bank_context"] is None     # not in the book -> the live quote path, unchanged
    assert not any(p == "quote" and (q or {}).get("symbol") in ("RISKY", "MO") for p, q in calls)        # no live quote for book names
    assert doc["warnings"][0]["ticker"] == "RISKY" and doc["state"] in ("NORMAL", "ELEVATED", "HIGH_RISK_RICH")


def test_without_the_book_the_curated_list_and_live_quotes_stand():
    lf, stub, book, calls = boot()
    book.on = False
    assert lf.book_candidates() == ([], None)
    out = lf.lambda_handler({}, None)
    assert out["statusCode"] == 200
    doc = json.loads(stub.store[lf.S3_KEY])
    assert doc["summary"]["universe_size"] == 2 and doc["summary"]["universe_basis"].endswith("factor book unavailable")
    assert {x["ticker"] for x in doc["warnings"]} == {"T"} and doc["warnings"][0]["price_source"] == "live quote"


if __name__ == "__main__":
    n = 0
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            fn()
            n += 1
            print("ok", name)
    print("%d tests passed" % n)
