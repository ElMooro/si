"""justhodl-pump-mechanics tests: (1) the SEC consumer boundary harness (tests/sec_consumer_test_support.py - the
isolated extract_concentration_signals / compute_squeeze_score contract, unchanged); (2) v1.1.0 bank-fusion offline
tests: profile from the factor book first, history from the Polygon EOD bank first, the real short-interest block per
candidate, the old path when the book is absent.  No network, no AWS, no key.

Run: python3 aws/lambdas/justhodl-pump-mechanics/tests/run_tests.py
"""
from pathlib import Path
import io
import json
import os
import sys
import types
from datetime import datetime, timedelta, timezone

ROOT = Path(__file__).resolve().parents[4]
HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent / "source"))
sys.path.insert(0, str(ROOT / "aws" / "shared"))
os.environ["FMP_KEY"] = "test-key-not-real"


class FakeBook(types.ModuleType):
    rows, quotes, on = {}, {}, True

    def available(self):
        return self.on

    def as_of(self):
        return {"tape": "2026-10-24", "fmp-bank": "2026-10-25"} if self.on else None

    def quote(self, symbol):
        q = self.quotes.get(str(symbol).upper())
        return None if q is None else dict(q, symbol=str(symbol).upper(), as_of=self.as_of(), source="factor-book (bank close, not a live tick)")

    def lookup(self, symbol, fields=None):
        r = self.rows.get(str(symbol).upper())
        return None if r is None else {k: r.get(k) for k in (fields or r)}

    def squeeze(self, symbol):
        r = self.rows.get(str(symbol).upper())
        return None if r is None else dict(r, symbol=str(symbol).upper(), as_of=self.as_of(), source="factor-book (test)")


class FakePoly(types.ModuleType):
    banked, asked = {}, []

    def answer(self, url, now=None):
        self.asked.append(url)
        sym = url.split("/ticker/")[1].split("/")[0]
        rows = self.banked.get(sym)
        return None if rows is None else json.dumps({"ticker": sym, "results": rows}).encode()


class S3Stub:
    def __init__(self, docs):
        self.docs, self.written = docs, {}

    def get_object(self, Bucket, Key):
        if Key in self.docs:
            return {"Body": io.BytesIO(json.dumps(self.docs[Key]).encode())}
        raise KeyError(Key)

    def put_object(self, **k):
        self.written[k["Key"]] = json.loads(k["Body"])


def bars(n=95, px=20.0):
    out, t0 = [], datetime(2026, 7, 1, tzinfo=timezone.utc)
    for i in range(n):
        p = px * (1 + 0.003 * (1 if i % 5 else -1)) ** i
        out.append({"t": int((t0 + timedelta(days=i)).timestamp() * 1000), "o": p, "h": p * 1.03, "l": p * 0.97, "c": p, "v": 2_000_000 + (i % 7) * 100000})
    return out


def boot(book_on=True):
    import boto3
    sys.modules["fmp_bankfirst"] = types.ModuleType("fmp_bankfirst")
    sys.modules["fmp_bankfirst"].install = lambda: None
    ms = types.ModuleType("managed_secret")
    ms.managed_secret = lambda *a, **k: "x"
    sys.modules["managed_secret"] = ms
    book = FakeBook("bank_enrich")
    book.on = book_on
    sys.modules["bank_enrich"] = book
    poly = FakePoly("polygon_bankfirst")
    poly.asked = []
    sys.modules["polygon_bankfirst"] = poly
    s3 = S3Stub({"data/convergence-radar.json": {"pump_candidates": [{"ticker": "AAA", "pump_likelihood": 0.7, "pump_category": "x"}, {"ticker": "BBB", "pump_likelihood": 0.5, "pump_category": "y"}]},
                 "data/options-flow-scanner.json": {"all_qualifying": [{"symbol": "AAA", "tier": "TIER_A_BULLISH_FLOW", "flags": ["CPR_SURGING", "CALL_VOL_3X"], "score": 80}]},
                 "data/sec-filings-intel.json": {"all_tickers": []}})
    boto3.client = lambda *a, **k: s3
    sys.modules.pop("lambda_function", None)
    import lambda_function as lf
    lf._book, lf._poly_bank, lf.s3 = book, poly, s3
    return lf, book, poly, s3


def test_book_profile_polygon_bank_history_and_real_short_interest():
    lf, book, poly, s3 = boot()
    book.quotes = {"AAA": {"price": 20.0, "marketCap": 800e6, "sharesOutstanding": 40e6, "name": None, "sector": "Technology"}}
    book.rows = {"AAA": {"outstanding_shares": 40e6, "float_shares": 30e6, "short_pct_float": 31.2, "days_to_cover": 7.4, "short_volume_ratio": 0.55, "mcap": 800e6, "avg_volume": 2e6, "close": 20.0, "ret_21d": 0.12, "ret_63d": 0.4, "from_52w_high_pct": -3.0, "composite_squeeze": 95.0}}
    poly.banked = {"AAA": bars(), "BBB": bars(px=5.0)}
    live_profiles = []

    def fake_live_profile(ticker):
        live_profiles.append(ticker)
        return {"marketCap": 50e6, "price": 5.0} if ticker == "BBB" else None
    lf.fetch_profile_live = fake_live_profile
    lf.fetch_price_history_fmp = lambda t, d=90: (_ for _ in ()).throw(AssertionError("banked names must not reach FMP history"))
    r = lf.lambda_handler({}, None)
    assert r["statusCode"] == 200, r
    out = s3.written[lf.OUTPUT_KEY]
    assert out["version"] == "1.1.0" and out["n_candidates"] == 2 and out["book_as_of"]["tape"] == "2026-10-24" and out["factor_book"].startswith("short interest + profile from the bank on 1 of 2")
    by = {c["ticker"]: c["squeeze_profile"] for c in out["candidates"]}
    a, b = by["AAA"], by["BBB"]
    assert a["profile_source"] == "factor-book" and a["shares_outstanding"] == 40e6 and a["float_tier"] == "S" and a["history_source"] == "polygon-eod-bank"
    assert a["short_interest"]["si_pct_float"] == 31.2 and a["short_interest"]["days_to_cover"] == 7.4 and a["short_interest"]["source"].startswith("factor-book")
    assert "31.2% of float, 7.4 days to cover" in a["squeeze_caveat"] and "unchanged" in a["squeeze_caveat"]
    assert live_profiles == ["BBB"] and b["profile_source"] == "fmp-live" and b["shares_outstanding"] == 10e6 and b["short_interest"] is None
    assert "not banked for this name (factor book on)" in b["squeeze_caveat"]
    assert isinstance(a["squeeze_proxy_score"], float) and a["squeeze_proxy_score"] >= 22, a   # the proxy still scores (S tier 22 pts + options flow)
    assert out["metadata"]["components"]["float_tier_weight"] == 30, "component weights unchanged"


def test_book_absent_keeps_the_old_path_and_says_so():
    lf, book, poly, s3 = boot(book_on=False)
    poly.banked = {}
    lf.fetch_profile_live = lambda t: {"marketCap": 100e6, "price": 10.0}
    lf.fetch_price_history_fmp = lambda t, d=90: [{"date": (datetime(2026, 7, 1) + timedelta(days=i)).date().isoformat(), "open": 10, "high": 10.3, "low": 9.7, "close": 10 + i * 0.01, "volume": 1_000_000} for i in range(95)]
    lf.lambda_handler({}, None)
    out = s3.written[lf.OUTPUT_KEY]
    assert out["factor_book"] == "factor book unavailable" and out["book_as_of"] is None
    sp = out["candidates"][0]["squeeze_profile"]
    assert sp["profile_source"] == "fmp-live" and sp["history_source"] == "fmp" and sp["short_interest"] is None and "factor book unavailable" in sp["squeeze_caveat"]


if __name__ == "__main__":
    sys.path.insert(0, str(ROOT / "tests"))
    from sec_consumer_test_support import run as run_sec_research
    run_sec_research()
    test_book_profile_polygon_bank_history_and_real_short_interest()
    print("ok test_book_profile_polygon_bank_history_and_real_short_interest")
    test_book_absent_keeps_the_old_path_and_says_so()
    print("ok test_book_absent_keeps_the_old_path_and_says_so")
    print("2 tests passed (+ the SEC consumer boundary harness)")
