"""justhodl-pre-pump-detector offline tests (v2.1.0 bank fusion): history from the Polygon EOD bank first, FMP second;
the factor book's squeeze block on every result; the old path when the book is absent.  No network, no AWS, no key.

Run: python3 aws/lambdas/justhodl-pre-pump-detector/tests/run_tests.py
"""
import io
import json
import os
import sys
import types
from datetime import datetime, timedelta, timezone

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "source"))
os.environ["FMP_KEY"] = "test-key-not-real"
os.environ["TIMEOUT_BUDGET_S"] = "60"


class FakeBook(types.ModuleType):
    rows, on = {}, True

    def available(self):
        return self.on

    def as_of(self):
        return {"tape": "2026-10-24", "fmp-bank": "2026-10-25"} if self.on else None

    def squeeze(self, symbol):
        r = self.rows.get(str(symbol).upper())
        return None if r is None else dict(r, symbol=str(symbol).upper(), as_of=self.as_of(), source="factor-book (test)")

    def squeezes(self, symbols):
        out = {}
        for s in symbols:
            b = self.squeeze(s)
            if b is not None:
                out[str(s).upper()] = b
        return out


class FakePoly(types.ModuleType):
    """polygon_bankfirst stand-in: answer(url) -> Polygon envelope bytes for banked tickers, None otherwise."""
    banked = {}
    asked = []

    def answer(self, url, now=None):
        self.asked.append(url)
        sym = url.split("/ticker/")[1].split("/")[0]
        rows = self.banked.get(sym)
        if rows is None:
            return None
        return json.dumps({"ticker": sym, "resultsCount": len(rows), "results": rows}).encode()


class S3Stub:
    def __init__(self, universe):
        self.universe, self.written = universe, {}

    def get_object(self, Bucket, Key):
        if Key == "data/universe.json":
            return {"Body": io.BytesIO(json.dumps({"stocks": [{"symbol": s} for s in self.universe]}).encode())}
        raise KeyError(Key)

    def put_object(self, **k):
        self.written[k["Key"]] = json.loads(k["Body"])


def bars(n=220, start_px=20.0, drift=0.002, vol=800000):
    """Synthetic sessions: a steady, liquid uptrend with calm recent days -> compute_signals returns a result."""
    out, px, t0 = [], start_px, datetime(2026, 1, 5, tzinfo=timezone.utc)
    for i in range(n):
        px = px * (1 + drift * (1 if i % 7 else -1))
        day = t0 + timedelta(days=i)
        out.append({"t": int(day.timestamp() * 1000), "c": round(px, 4), "v": vol + (i % 5) * 50000})
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
    s3 = S3Stub(["AAA", "BBB", "CCC"])
    boto3.client = lambda *a, **k: s3
    sys.modules.pop("lambda_function", None)
    import lambda_function as lf
    lf._book, lf._poly_bank, lf.S3 = book, poly, s3
    return lf, book, poly, s3


def test_polygon_bank_history_first_then_fmp_with_squeeze_context():
    lf, book, poly, s3 = boot()
    poly.banked = {"AAA": bars(), "BBB": bars(start_px=50.0)}          # CCC is not banked -> FMP path
    fmp_calls = []

    def fake_http(url, timeout=15):
        fmp_calls.append(url)
        if "symbol=CCC" in url:
            return [{"date": (datetime(2026, 1, 5) + timedelta(days=i)).date().isoformat(), "close": 10 + i * 0.02, "volume": 900000} for i in range(220)][::-1]
        raise RuntimeError("429 Bandwidth Limit Reach")
    lf._http_get_json = fake_http
    book.rows = {"AAA": {"short_pct_float": 22.5, "days_to_cover": 6.1, "float_shares": 40e6, "short_volume_ratio": 0.48, "mcap": 900e6, "avg_volume": 1.1e6, "close": 21.0, "ret_21d": 0.08, "ret_63d": 0.3, "from_52w_high_pct": -5.0, "composite_squeeze": 91.0},
                 "CCC": {"short_pct_float": 3.0, "days_to_cover": 1.0, "float_shares": 500e6, "short_volume_ratio": 0.4, "mcap": 5e9, "avg_volume": 3e6, "close": 14.0, "ret_21d": 0.01, "ret_63d": 0.05, "from_52w_high_pct": -20.0, "composite_squeeze": 20.0}}
    lf.lambda_handler({}, None)
    out = s3.written[lf.S3_KEY]
    assert out["version"] == "2.1.0" and out["stats"]["n_evaluated"] == 3, out["stats"]
    assert out["stats"]["history_sources"] == {"polygon-eod-bank": 2, "fmp": 1}, out["stats"]
    assert len([u for u in fmp_calls if "symbol=CCC" in u]) == 1 and not any("symbol=AAA" in u for u in fmp_calls), "banked names must not reach FMP"
    assert out["stats"]["n_with_squeeze"] == 2 and out["book_as_of"]["tape"] == "2026-10-24" and out["factor_book"].startswith("squeeze context on 2 of 3")
    by = {r["symbol"]: r for r in out["all_qualifying"]}
    assert by["AAA"]["squeeze"]["short_pct_float"] == 22.5 and by["AAA"]["history_source"] == "polygon-eod-bank" and by["BBB"]["squeeze"] is None
    top = {r["symbol"]: r for r in out["summary"]["top_25_overall"]}
    assert top["AAA"]["si_pct_float"] == 22.5 and top["AAA"]["days_to_cover"] == 6.1 and top["CCC"]["history_source"] == "fmp"
    assert set(by["AAA"]) >= {"score", "tier", "flags", "metrics"}, "the v2 schema stays"


def test_book_absent_keeps_the_old_path_and_says_so():
    lf, book, poly, s3 = boot(book_on=False)
    poly.banked = {}
    lf._http_get_json = lambda url, timeout=15: [{"date": (datetime(2026, 1, 5) + timedelta(days=i)).date().isoformat(), "close": 10 + i * 0.02, "volume": 900000} for i in range(220)][::-1]
    lf.lambda_handler({}, None)
    out = s3.written[lf.S3_KEY]
    assert out["factor_book"] == "factor book unavailable" and out["book_as_of"] is None and out["stats"]["n_with_squeeze"] == 0
    assert out["stats"]["history_sources"] == {"fmp": 3} and all("squeeze" not in r for r in out["all_qualifying"])


if __name__ == "__main__":
    test_polygon_bank_history_first_then_fmp_with_squeeze_context()
    print("ok test_polygon_bank_history_first_then_fmp_with_squeeze_context")
    test_book_absent_keeps_the_old_path_and_says_so()
    print("ok test_book_absent_keeps_the_old_path_and_says_so")
    print("2 tests passed")
