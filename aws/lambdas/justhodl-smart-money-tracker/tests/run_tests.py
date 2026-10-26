"""justhodl-smart-money-tracker offline tests (v2.1.0 bank fusion): discovery from the banked 13F latest feed, per-filer
performance from the banked per-CIK files, live calls only for unbanked filers and capped; the live ladder stands when
the bank is empty.  No network, no AWS, no key.

Run: python3 aws/lambdas/justhodl-smart-money-tracker/tests/run_tests.py
"""
import json
import os
import sys
import types

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "source"))


class FakeBank(types.ModuleType):
    latest = (None, None)
    perf = {}

    def walker_latest(self, dataset, lookback_days=10, end=None):
        assert dataset == "institutional-ownership-latest"
        return self.latest

    def walker(self, dataset, entity=None, period=None, day=None, exchange=None):
        assert dataset == "institutional-ownership-holder-performance-summary" and entity
        return self.perf.get(entity)


class S3Stub:
    def __init__(self):
        self.written = {}

    def put_object(self, **k):
        self.written[k["Key"]] = json.loads(k["Body"])


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
    sys.modules.pop("lambda_function", None)
    import lambda_function as lf
    lf._bank, lf.s3 = bank, s3
    return lf, bank, s3


def perf_rows(mv, prev, added=5, removed=2, date="2026-06-30"):
    return [{"date": "2025-12-31", "marketValue": prev, "previousMarketValue": prev * 0.9, "investorName": "X", "portfolioSize": 10},
            {"date": date, "marketValue": mv, "previousMarketValue": prev, "securitiesAdded": added, "securitiesRemoved": removed, "portfolioSize": 12,
             "performancePercentage": 3.5, "investorName": "X"}]


def test_bank_discovery_and_bank_performance_with_live_capped():
    lf, bank, s3 = boot()
    lf.WELL_KNOWN_FUNDS = [("0001067983", "BERKSHIRE HATHAWAY INC"), ("0001336528", "PERSHING SQUARE CAPITAL"), ("0009999999", "UNBANKED FUND")]
    bank.latest = ("2026-10-25", [{"cik": "0001067983", "name": "BERKSHIRE HATHAWAY INC"}, {"cik": "0001111111", "name": "DISCOVERED ONE"}, {"cik": "0001111111", "name": "dup"},
                                  {"cik": "0002222222", "name": "DISCOVERED TWO"}, {"cik": "0003333333", "name": "DISCOVERED THREE"}])
    bank.perf = {"0001067983": perf_rows(300e9, 280e9), "0001336528": perf_rows(12e9, 11e9, added=1, removed=4), "0001111111": perf_rows(5e9, 6e9)}
    live = []

    def fake_fmp(path, params="", retries=2):
        live.append((path, params))
        if "holder-performance-summary" in path and "0009999999" in params:
            return perf_rows(2e9, 1e9)
        return None
    lf.fmp = fake_fmp
    lf.LIVE_FILER_CAP = 2
    rc = lf.lambda_handler({}, None)
    out = s3.written[lf.S3_KEY]
    assert out["version"] == "2.1.0" and out["source_used"].startswith("fmp-walker bank institutional-ownership-latest (2026-10-25, 5 rows)"), out["source_used"]
    fb = out["feed_basis"]
    assert fb["n_targets"] == 6 and fb["bank_feed_rows"] == 5, fb                             # 3 curated + 3 distinct discovered
    by_cik = {f["cik"]: f for f in out["filers"]}
    assert set(by_cik) == {"0001067983", "0001336528", "0001111111", "0009999999"}, by_cik.keys()
    assert by_cik["0001067983"]["source"] == "fmp-walker bank" and by_cik["0009999999"]["source"] == "fmp live"
    assert by_cik["0001067983"]["qoq_change_pct"] == 7.14 and by_cik["0001067983"]["date"] == "2026-06-30" and by_cik["0001336528"]["net_activity"] == -3
    live_perf = [p for p in live if "holder-performance-summary" in p[0]]
    assert len(live_perf) == 2 and fb["live_filer_calls"] == 2, live_perf                     # 3 unbanked filers, cap 2 -> one waits for the bank
    assert not any("institutional-ownership/latest" in p[0] for p in live), "no live discovery when the bank answers"
    assert fb["filer_sources"] == {"fmp-walker bank": 3, "fmp live": 1}
    assert out["filers"][0]["cik"] == "0001067983" and out["as_of_quarter"] == "Q2 2026" and json.loads(rc["body"])["n_filers"] == 4


def test_live_ladder_stands_when_the_bank_is_empty():
    lf, bank, s3 = boot()
    lf.WELL_KNOWN_FUNDS = [("0001067983", "BERKSHIRE HATHAWAY INC")]
    bank.latest = (None, None)
    bank.perf = {}
    live = []

    def fake_fmp(path, params="", retries=2):
        live.append(path)
        if path == "institutional-ownership/latest" and "page=0" in params:
            return [{"cik": "0005555555", "name": "LIVE FUND"}]
        if path == "institutional-ownership/latest":
            return []
        return perf_rows(1e9, 0.5e9)
    lf.fmp = fake_fmp
    lf.lambda_handler({}, None)
    out = s3.written[lf.S3_KEY]
    assert out["source_used"] == "fmp live institutional-ownership/latest" and out["feed_basis"]["bank_day"] is None
    assert {f["cik"] for f in out["filers"]} == {"0001067983", "0005555555"} and all(f["source"] == "fmp live" for f in out["filers"])


if __name__ == "__main__":
    test_bank_discovery_and_bank_performance_with_live_capped()
    print("ok test_bank_discovery_and_bank_performance_with_live_capped")
    test_live_ladder_stands_when_the_bank_is_empty()
    print("ok test_live_ladder_stands_when_the_bank_is_empty")
    print("2 tests passed")
