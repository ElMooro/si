"""justhodl-insider-radar offline tests (v1.2.0 bank fusion): the banked insider feed is the first rung of the source
ladder; the live rungs stand when the bank has nothing.  No network, no AWS, no key.

Run: python3 aws/lambdas/justhodl-insider-radar/tests/run_tests.py
"""
import json
import os
import sys
import types
from datetime import datetime, timedelta, timezone

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "source"))
os.environ["FMP_KEY"] = "test-key-not-real"
os.environ["POLYGON_KEY"] = "x"


class FakeBank(types.ModuleType):
    days = {}

    def walker(self, dataset, entity=None, period=None, day=None, exchange=None):
        assert dataset == "insider-trading-latest" and day
        return self.days.get(day)


class FakeBook(types.ModuleType):
    """bank_enrich stand-in: ret_63d / ps per symbol from the book's matrix (fractions / multiples, as the book writes them)."""
    rows = {}
    on = True

    def available(self):
        return self.on

    def as_of(self):
        return {"tape": "2026-10-24"} if self.on else None

    def lookup(self, symbol, fields=None):
        r = self.rows.get(str(symbol).upper())
        if r is None:
            return None
        return {k: r.get(k) for k in (fields or r)} | {"symbol": symbol}


def boot():
    import boto3
    boto3.client = lambda *a, **k: types.SimpleNamespace()
    sys.modules["fmp_bankfirst"] = types.ModuleType("fmp_bankfirst")
    sys.modules["fmp_bankfirst"].install = lambda: None
    ms = types.ModuleType("managed_secret")
    ms.managed_secret = lambda *a, **k: "x"
    sys.modules["managed_secret"] = ms
    bank = FakeBank("fmp_bank")
    sys.modules["fmp_bank"] = bank
    book = FakeBook("bank_enrich")
    sys.modules["bank_enrich"] = book
    sys.modules.pop("lambda_function", None)
    import lambda_function as lf
    lf._book = book
    return lf, bank, book


def test_bank_feed_is_the_first_rung_and_normalizes_like_the_live_feed():
    lf, bank, _ = boot()
    today = datetime.now(timezone.utc).date()
    row = lambda sym, d: {"symbol": sym, "transactionType": "P-Purchase", "transactionDate": d, "securitiesTransacted": 1000, "price": 10.0, "reportingName": "Jane Doe", "typeOfOwner": "director"}
    bank.days = {(today - timedelta(days=1)).isoformat(): [row("AAA", (today - timedelta(days=1)).isoformat())] * 3,
                 (today - timedelta(days=9)).isoformat(): [row("BBB", (today - timedelta(days=9)).isoformat())]}
    rows, got = lf.fetch_bank_feed(days=12)
    assert got == 2 and len(rows) == 4 and lf.DIAG[-1].startswith("bank/insider-trading-latest: 4 rows over 2 banked days")
    norm = lf.normalize(rows)
    assert norm[0]["ticker"] == "AAA" and norm[0]["type"] == "P-Purchase" and norm[0]["value"] == 10000.0 and norm[0]["insider"] == "Jane Doe"
    bank.days = {}
    lf.DIAG.clear()
    assert lf.fetch_bank_feed(days=3) == ([], 0) and lf.DIAG[-1] == "bank/insider-trading-latest: no banked days"


def test_decline_join_reads_the_factor_book_and_caps_live_polygon():
    lf, bank, book = boot()
    today = datetime.now(timezone.utc).date()
    d = (today - timedelta(days=2)).isoformat()
    row = lambda sym, who: {"symbol": sym, "transactionType": "P-Purchase", "transactionDate": d, "securitiesTransacted": 5000, "price": 20.0, "reportingName": who, "typeOfOwner": "officer: CEO"}
    # 60 tickers bought by insiders; the book knows 50 of them (ret_63d as a fraction), 10 are unknown to every source
    feed = []
    for i in range(60):
        sym = "T%02d" % i
        feed += [row(sym, "A %d" % i), row(sym, "B %d" % i)]
        if i < 50:
            book.rows[sym] = {"ret_63d": -0.40 if i < 20 else 0.05, "ps": 1.5}
    bank.days = {d: feed}
    live = []
    lf.jget = lambda url, timeout=35: live.append(url) or {"results": []}
    lf.LIVE_RET_CAP = 4
    written = {}
    lf.S3 = types.SimpleNamespace(get_object=lambda **k: (_ for _ in ()).throw(RuntimeError("no s3 in tests")), put_object=lambda **k: written.update({k["Key"]: json.loads(k["Body"])}))
    lf.DDB = types.SimpleNamespace(Table=lambda *a, **k: (_ for _ in ()).throw(RuntimeError("no ddb in tests")))
    lf.lambda_handler({}, None)
    out = written[lf.OUT_KEY]
    basis = out["ret_60d_basis"]
    assert basis["factor_book"] == 50 and basis["rings"] == 0, basis
    assert basis["polygon_live"] == 0 and basis["none"] == 10, basis          # live Polygon returned nothing -> counted as none
    assert len([u for u in live if "api.polygon.io" in u]) == 4, "live Polygon reads must stop at the cap (4), got %d" % len(live)
    assert out["book_as_of"] == {"tape": "2026-10-24"} and out["version"] == "1.2.1"
    assert len(out["decline_clusters"]) == 10 and all(c["ret_60d_pct"] == -40.0 for c in out["decline_clusters"])   # output caps the list at 10 of the 20
    assert all(c["ps_ttm"] == 1.5 for c in out["clusters"] if c["ticker"] in book.rows), "P/S must come from the book"
    assert any(x.startswith("ps_ttm:") and "from the factor book" in x for x in out["diagnostics"])
    # book absent -> the live rung is the only fallback, still capped
    book.on = False
    live.clear()
    lf.DIAG.clear()
    lf.lambda_handler({}, None)
    out = written[lf.OUT_KEY]
    assert out["ret_60d_basis"]["factor_book"] == 0 and out["book_as_of"] is None
    assert len([u for u in live if "api.polygon.io" in u]) == 4


if __name__ == "__main__":
    test_bank_feed_is_the_first_rung_and_normalizes_like_the_live_feed()
    print("ok test_bank_feed_is_the_first_rung_and_normalizes_like_the_live_feed")
    test_decline_join_reads_the_factor_book_and_caps_live_polygon()
    print("ok test_decline_join_reads_the_factor_book_and_caps_live_polygon")
    print("2 tests passed")
