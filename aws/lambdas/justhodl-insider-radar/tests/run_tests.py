"""justhodl-insider-radar offline tests (v1.2.0 bank fusion): the banked insider feed is the first rung of the source
ladder; the live rungs stand when the bank has nothing.  No network, no AWS, no key.

Run: python3 aws/lambdas/justhodl-insider-radar/tests/run_tests.py
"""
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
    sys.modules.pop("lambda_function", None)
    import lambda_function as lf
    return lf, bank


def test_bank_feed_is_the_first_rung_and_normalizes_like_the_live_feed():
    lf, bank = boot()
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


if __name__ == "__main__":
    test_bank_feed_is_the_first_rung_and_normalizes_like_the_live_feed()
    print("ok test_bank_feed_is_the_first_rung_and_normalizes_like_the_live_feed")
    print("1 tests passed")
