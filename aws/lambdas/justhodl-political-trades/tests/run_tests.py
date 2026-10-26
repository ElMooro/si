"""justhodl-political-trades offline tests (v1.1.0 bank fusion): the banked senate-latest / house-latest page-sets are
the first rung of each chamber's ladder (newest rows, capped), the live candidates stand per chamber when the bank has
none, cluster / large-trade rows carry a labelled bank_context.  No network, no AWS, no key.

Run: python3 aws/lambdas/justhodl-political-trades/tests/run_tests.py
"""
import json
import os
import sys
import types
from datetime import datetime, timedelta, timezone

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "source"))
os.environ["TELEGRAM_TOKEN"] = ""


class FakeBank(types.ModuleType):
    feeds = {}

    def walker_latest(self, dataset, lookback_days=10, end=None):
        return self.feeds.get(dataset, (None, None))


class FakeBook(types.ModuleType):
    rows = {}
    on = True

    def available(self):
        return self.on

    def as_of(self):
        return {"fmp-bank": "2026-10-25"} if self.on else None

    def lookup(self, symbol, fields=None):
        r = self.rows.get(str(symbol).upper())
        if not r or not self.on:
            return None
        return {k: r.get(k) for k in (fields or r)}


class S3Stub:
    def __init__(self):
        self.written = {}

    def put_object(self, **k):
        self.written[k["Key"]] = json.loads(k["Body"])

    def get_object(self, **k):
        raise RuntimeError("no prior run")


def boot():
    import boto3
    s3 = S3Stub()
    boto3.client = lambda name, *a, **k: s3
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
    lf._bank, lf._book, lf.s3 = bank, book, s3
    return lf, bank, book, s3


def trade(sym, first, last, d, amount="$1,001 - $15,000", typ="Purchase", office="Senate"):
    return {"symbol": sym, "disclosureDate": d, "transactionDate": d, "firstName": first, "lastName": last, "office": office, "district": "",
            "owner": "Self", "assetDescription": sym + " common", "assetType": "Stock", "type": typ, "amount": amount, "link": "https://efts.sec.gov/x"}


def test_bank_pages_first_with_context_and_no_live_call():
    lf, bank, book, s3 = boot()
    today = datetime.now(timezone.utc).date()
    d0, d1, old = today.isoformat(), (today - timedelta(days=3)).isoformat(), (today - timedelta(days=300)).isoformat()
    senate = [trade("NVDA", "Tommy", "Tuberville", d0, amount="$500,001 - $1,000,000"), trade("NVDA", "Sheldon", "Whitehouse", d1), trade("NVDA", "Ted", "Cruz", d1, typ="Sale"),
              trade("OLD", "Some", "One", old)] + [trade("FILL%d" % i, "F", "Member%d" % i, d1) for i in range(5)]
    house = [trade("NVDA", "Nancy", "Pelosi", d1, amount="$1,000,001 - $5,000,000", office="House"), trade("AAPL", "Ro", "Khanna", d1, office="House")]
    bank.feeds = {"senate-latest": ("2026-10-25", senate), "house-latest": ("2026-10-25", house)}
    book.rows = {"NVDA": {"mcap": 4e12, "sector": "Technology", "ret_21d": 0.05}}
    live = []
    lf.http_get_json = lambda url, timeout=30: live.append(url) or []
    lf.MAX_FEED_ROWS = 8
    rc = lf.lambda_handler({}, None)
    out = s3.written[lf.S3_KEY_OUT]
    assert out["version"] == "1.1.0" and out["endpoints"] == {"senate": "fmp-walker bank senate-latest", "house": "fmp-walker bank house-latest"}, out["endpoints"]
    assert out["feed_basis"]["senate"] == {"source": "fmp-walker bank senate-latest", "day": "2026-10-25", "rows": 8}, out["feed_basis"]   # 9 rows -> newest 8 kept (OLD dropped)
    assert out["stats"]["n_senate"] == 8 and out["stats"]["n_house"] == 2 and not live
    assert out["clusters_top_10"][0]["ticker"] == "NVDA" and out["clusters_top_10"][0]["n_members"] == 4 and out["clusters_top_10"][0]["bank_context"]["sector"] == "Technology"
    assert out["clusters_top_10"][0]["bank_context"]["source"] == "factor-book" and out["book_as_of"]["fmp-bank"] == "2026-10-25"
    large = {t["member"]: t for t in out["large_trades_top_15"]}
    assert set(large) == {"Tommy Tuberville", "Nancy Pelosi"} and large["Nancy Pelosi"]["bank_context"]["mcap"] == 4e12
    assert out["stats"]["n_high_watch"] >= 4 and json.loads(rc["body"])["ok"] is True
    assert all("OLD" != t["ticker"] for t in out["trades_recent_50"])


def test_live_candidates_stand_per_chamber_when_the_bank_has_none():
    lf, bank, book, s3 = boot()
    today = datetime.now(timezone.utc).date().isoformat()
    bank.feeds = {"senate-latest": ("2026-10-25", [trade("MSFT", "A", "B", today)])}      # house not banked -> live house ladder only
    book.on = False
    live = []

    def fake_get(url, timeout=30):
        live.append(url)
        return [trade("MSFT", "C", "D", today, office="House")] if "house-latest" in url else []
    lf.http_get_json = fake_get
    lf.lambda_handler({}, None)
    out = s3.written[lf.S3_KEY_OUT]
    assert out["endpoints"]["senate"] == "fmp-walker bank senate-latest" and out["endpoints"]["house"].endswith("/stable/house-latest"), out["endpoints"]
    assert all("senate" not in u for u in live) and out["feed_basis"]["house"]["source"].startswith("fmp live") and out["book_as_of"] is None
    assert out["stats"]["n_total"] == 2 and out["clusters_top_10"] == []


if __name__ == "__main__":
    test_bank_pages_first_with_context_and_no_live_call()
    print("ok test_bank_pages_first_with_context_and_no_live_call")
    test_live_candidates_stand_per_chamber_when_the_bank_has_none()
    print("ok test_live_candidates_stand_per_chamber_when_the_bank_has_none")
    print("2 tests passed")
