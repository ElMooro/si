"""justhodl-smart-money-holdings offline tests (v1.1.0 bank fusion): per-fund holdings from the banked per-CIK quarter
files first (with the quarter fallback), the live paginated extract only for unbanked funds and capped; the inverted
index and noise filter unchanged.  No network, no AWS, no key.

Run: python3 aws/lambdas/justhodl-smart-money-holdings/tests/run_tests.py
"""
import json
import os
import sys
import types

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "source"))


class FakeBank(types.ModuleType):
    files = {}

    def walker(self, dataset, entity=None, period=None, day=None, exchange=None):
        assert dataset == "institutional-ownership-extract" and entity and period
        return self.files.get((entity, period))


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


def h(sym, value, shares=1000, cusip=None, pcs=None):
    return {"symbol": sym, "securityCusip": cusip or sym + "CUSIP", "shares": shares, "value": value, "nameOfIssuer": sym + " Inc", "titleOfClass": "COM", "putCallShare": pcs}


def test_bank_quarter_files_first_with_fallback_and_capped_live():
    lf, bank, s3 = boot()
    y, q = lf.get_latest_filed_quarter()
    y1, q1 = lf._yq_offset(y, q, 1)
    lf.CONCENTRATED_FUNDS = [("0001067983", "Berkshire Hathaway"), ("0001336528", "Pershing Square Capital"), ("0007777777", "Unbanked A"), ("0008888888", "Unbanked B")]
    bank.files = {("0001067983", (y, q)): [h("AAPL", 60e9), h("AAPL", 1, cusip="dupe-no"), h("KO", 25e9), h("OPT", 9e9, pcs="Call"), h("TINY", 100)],
                  ("0001336528", (y1, q1)): [h("CMG", 2e9), h("HHH", 1.5e9)]}        # Pershing filed late: latest quarter missing, previous banked
    bank.files[("0001067983", (y, q))][1]["securityCusip"] = "AAPLCUSIP"            # same cusip|symbol -> deduped
    live = []

    def fake_fmp(path, params="", retries=2):
        live.append(params)
        if "cik=0007777777" in params and "page=0" in params and f"year={y}&quarter={q}" in params:
            return [h("LIVE", 3e9)]
        return []
    lf.fmp = fake_fmp
    lf.LIVE_FUND_CAP = 1
    rc = lf.lambda_handler({}, None)
    out = s3.written[lf.S3_KEY]
    assert out["version"] == "1.1.0" and out["source_used"].startswith("fmp-walker bank institutional-ownership-extract first"), out["source_used"]
    funds = {f["cik"]: f for f in out["funds"]}
    assert set(funds) == {"0001067983", "0001336528", "0007777777"}, funds.keys()
    assert funds["0001067983"]["n_holdings"] == 3 and funds["0001067983"]["source"] == "fmp-walker bank"          # dupe + call option dropped, TINY kept here (noise filter is on the index)
    assert funds["0001336528"]["as_of_quarter"] == "Q%d %d" % (q1, y1) and funds["0001336528"]["source"] == "fmp-walker bank"
    assert funds["0007777777"]["source"] == "fmp live"
    assert out["feed_basis"]["source_counts"] == {"fmp-walker bank": 2, "fmp live": 1, "empty: unbanked (live cap reached)": 1}, out["feed_basis"]
    assert out["feed_basis"]["live_fund_calls"] == 1 and all("cik=0008888888" not in p for p in live), "the second unbanked fund must wait for the bank"
    assert "AAPL" in out["holdings"] and out["holdings"]["AAPL"]["holders"][0]["pct_of_fund"] > 50 and "TINY" not in out["holdings"]   # noise filter unchanged
    assert out["n_funds_scanned"] == 3 and json.loads(rc["body"])["n_funds"] == 3


def test_live_extract_stands_when_the_bank_has_nothing():
    lf, bank, s3 = boot()
    bank.files = {}
    lf.CONCENTRATED_FUNDS = [("0001067983", "Berkshire Hathaway")]
    y, q = lf.get_latest_filed_quarter()
    lf.fmp = lambda path, params="", retries=2: [h("AAPL", 60e9)] if "page=0" in params else []
    lf.lambda_handler({}, None)
    out = s3.written[lf.S3_KEY]
    assert out["source_used"].startswith("fmp live extract") and out["funds"][0]["source"] == "fmp live" and out["n_symbols"] == 1


if __name__ == "__main__":
    test_bank_quarter_files_first_with_fallback_and_capped_live()
    print("ok test_bank_quarter_files_first_with_fallback_and_capped_live")
    test_live_extract_stands_when_the_bank_has_nothing()
    print("ok test_live_extract_stands_when_the_bank_has_nothing")
    print("2 tests passed")
