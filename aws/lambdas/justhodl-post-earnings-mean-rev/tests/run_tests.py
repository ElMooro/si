"""justhodl-post-earnings-mean-rev offline tests (v1.1.0 bank fusion): report date / quote / closes come from the banks
first (walker earnings + daily prices, factor book close, Polygon EOD bank), the live FMP calls are the fallback, the
universe gains the banked earnings calendar's recent reporters, and every setup is labelled.  No network, no AWS, no key.

Run: python3 aws/lambdas/justhodl-post-earnings-mean-rev/tests/run_tests.py
"""
import json
import os
import sys
import types
from datetime import datetime, timedelta, timezone

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "source"))
os.environ["FMP_KEY"] = "test-key-not-real"
os.environ["TELEGRAM_BOT_TOKEN"] = ""

TODAY = datetime.now(timezone.utc).date()


def d(days_ago):
    return (TODAY - timedelta(days=days_ago)).isoformat()


def oversold_closes(n=80):
    """Newest first: 14 sessions of a steady -0.5%/day slide after the report, on a flat +-0.3% base -> RSI 0, extension
    <= -1.5 std, realised vol below the base's (vol crushed) -> a LONG counter-trend setup."""
    decline = [100.0 * 0.995 ** (14 - i) for i in range(14)]
    base = [100.3 if j % 2 == 0 else 99.7 for j in range(n - 14)]
    return decline + base


class FakeS3:
    def __init__(self):
        self.written = {}

    def get_object(self, Bucket, Key):
        if Key == "data/master-ranker.json":
            return {"Body": types.SimpleNamespace(read=lambda: json.dumps({"picks": [{"ticker": "AAA"}, {"ticker": "ZZZ"}]}).encode())}
        raise KeyError(Key)

    def put_object(self, **k):
        self.written[k["Key"]] = json.loads(k["Body"])


class FakeSSM:
    def get_parameter(self, **k):
        raise RuntimeError("no ssm in tests")

    def put_parameter(self, **k):
        return None


class FakeBook(types.ModuleType):
    on = True
    rows = {}
    docs = {}
    quotes = {}

    def available(self):
        return self.on

    def as_of(self):
        return {"fmp-bank": "2026-10-25"} if self.on else None

    def lookup(self, symbol, fields=None):
        r = self.rows.get(str(symbol).upper())
        return None if r is None else {k: r.get(k) for k in (fields or r)} | {"symbol": str(symbol).upper()}

    def factor(self, symbol):
        return self.docs.get(str(symbol).upper())

    def quote(self, symbol):
        return self.quotes.get(str(symbol).upper())


class FakeBank(types.ModuleType):
    earnings = {}
    eod = {}
    calendar = {}

    def walker(self, dataset, entity=None, period=None, day=None, exchange=None):
        if dataset == "earnings":
            return self.earnings.get(entity)
        if dataset == "historical-price-eod-full":
            return self.eod.get(entity)
        if dataset == "earnings-calendar":
            assert day and len(day) == 7
            return self.calendar.get(day)
        raise AssertionError(dataset)


class FakePoly(types.ModuleType):
    series = {}
    calls = []

    def answer(self, url, now=None):
        self.calls.append(url)
        sym = url.split("/ticker/")[1].split("/")[0]
        closes = self.series.get(sym)
        if not closes:
            return None
        asc = list(reversed(closes))
        t0 = int((datetime.now(timezone.utc) - timedelta(days=len(asc))).timestamp() * 1000)
        return json.dumps({"results": [{"c": c, "t": t0 + i * 86400000} for i, c in enumerate(asc)], "status": "OK"}).encode()


def boot():
    import boto3
    s3, ssm = FakeS3(), FakeSSM()
    boto3.client = lambda name, *a, **k: s3 if name == "s3" else ssm
    sys.modules["fmp_bankfirst"] = types.ModuleType("fmp_bankfirst")
    sys.modules["fmp_bankfirst"].install = lambda: None
    book, bank, poly = FakeBook("bank_enrich"), FakeBank("fmp_bank"), FakePoly("polygon_bankfirst")
    sys.modules["bank_enrich"], sys.modules["fmp_bank"], sys.modules["polygon_bankfirst"] = book, bank, poly
    sys.modules.pop("lambda_function", None)
    import lambda_function as lf
    lf._book, lf._bank, lf._poly, lf.s3, lf.ssm = book, bank, poly, s3, ssm
    lf.send_telegram = lambda text: None
    return lf, s3, book, bank, poly


def test_bank_first_inputs_calendar_candidates_and_labels():
    lf, s3, book, bank, poly = boot()
    closes = oversold_closes()
    # AAA (master-ranker): report 14 calendar days ago (10 trading days) in the walker earnings file, closes from the walker daily bank
    bank.earnings = {"AAA": [{"date": d(14), "epsActual": 1.2, "epsEstimated": 1.0}, {"date": d(105), "epsActual": 1.0, "epsEstimated": 1.0}, {"date": (TODAY + timedelta(days=77)).isoformat(), "epsActual": None, "epsEstimated": 1.1}]}
    bank.eod = {"AAA": [{"date": d(i), "close": c} for i, c in enumerate(closes)]}
    # BBB (calendar candidate): reported 14 days ago per the month window; closes only in the Polygon EOD bank
    bank.calendar = {TODAY.strftime("%Y-%m"): [{"symbol": "BBB", "date": d(14)}, {"symbol": "CCC", "date": d(14)}, {"symbol": "DDD", "date": d(2)}, {"symbol": "AAA", "date": d(14)}],
                     (TODAY.replace(day=1) - timedelta(days=1)).strftime("%Y-%m"): [{"symbol": "EEE", "date": d(40)}]}
    book.rows = {"AAA": {"mcap": 50e9, "avg_volume": 5e6, "pt_upside_pct": 12.0, "consensus_score": 0.6, "beat_rate_4q": 0.75, "last_surprise_pct": 20.0, "short_pct_float": 1.5, "altman_z": 5.0, "composite_momentum": 30.0, "sector": "Technology"},
                 "BBB": {"mcap": 5e9, "avg_volume": 1e6, "is_adr": False, "sector": "Healthcare"},
                 "CCC": {"mcap": 1e9, "avg_volume": 1e6},            # below the $2B floor
                 "DDD": {"mcap": 9e9, "avg_volume": 9e6}}            # reported 2 days ago -> outside 5-15 trading days
    book.quotes = {"AAA": {"price": closes[0], "source": "factor-book (bank close, not a live tick)"}, "BBB": {"price": closes[0], "source": "factor-book (bank close, not a live tick)"}}
    poly.series = {"BBB": closes}
    live = []
    lf.http_get = lambda url, timeout=12, retries=2: live.append(url) or "[]"
    lf.lambda_handler({}, None)
    out = s3.written[lf.S3_KEY]
    assert out["version"] == "1.1.0" and out["book_as_of"] == {"fmp-bank": "2026-10-25"}
    assert out["n_book_candidates"] == 1 and out["universe_size"] == 3 and "reported 5-15 trading days ago" in out["universe_basis"], (out["n_book_candidates"], out["universe_basis"])
    rows = {r["ticker"]: r for r in out["all_setups"]}
    assert set(rows) == {"AAA", "BBB"}, set(rows)
    aaa, bbb = rows["AAA"], rows["BBB"]
    assert aaa["direction"] == "LONG" and aaa["days_post_earnings"] == 10 and aaa["rsi14"] == 0.0 and aaa["price_ext_std_50d"] <= -1.5 and aaa["iv_percentile_60d"] <= 50
    assert aaa["earnings_date_source"] == "fmp-walker earnings" and aaa["price_source"] == "factor-book close" and aaa["history_source"] == "fmp-walker eod" and aaa["history_as_of"] == d(0)
    assert aaa["bank_context"]["altman_z"] == 5.0 and aaa["bank_context"]["source"] == "factor-book"
    assert bbb["earnings_date_source"] == "fmp-walker earnings-calendar" and bbb["history_source"] == "polygon-bank eod" and bbb["price_source"] == "factor-book close"
    assert bbb["bank_context"]["sector"] == "Healthcare" and bbb["earnings_date"] == d(14)
    assert not [u for u in live if "symbol=AAA" in u or "symbol=BBB" in u], live                     # nothing live for banked names
    assert [u for u in live if "symbol=ZZZ" in u]                                                     # the unbanked core name fell through to live
    ds = out["data_sufficiency"]
    assert ds["bank_reads"] >= 6 and ds["bank_basis"]["history"] == {"fmp-walker eod": 1, "polygon-bank eod": 1} and "bank" in ds["rule"]
    assert out["state"] in ("NORMAL", "ACTIVE", "MEAN_REV_RICH") and out["n_long_setups"] == 2
    assert any("factor book" in s for s in out["sources"]) and any("live fallback" in s for s in out["sources"])


def test_bank_off_runs_the_live_path_and_labels_it():
    lf, s3, book, bank, poly = boot()
    book.on = False
    closes = oversold_closes()
    answers = {"earnings-calendar": [{"symbol": "AAA", "date": d(14)}], "quote": [{"symbol": "AAA", "price": closes[0]}],
               "historical-price-eod/light": [{"date": d(i), "price": c} for i, c in enumerate(closes)]}

    def http_get(url, timeout=12, retries=2):
        lf._HTTP["ok"] += 1
        for k, v in answers.items():
            if "/stable/" + k in url and "symbol=AAA" in url:
                return json.dumps(v)
        return "[]"
    lf.http_get = http_get
    lf.lambda_handler({}, None)
    out = s3.written[lf.S3_KEY]
    assert out["book_as_of"] is None and out["universe_basis"].endswith("factor book unavailable") and out["n_book_candidates"] == 0
    assert len(out["all_setups"]) == 1
    row = out["all_setups"][0]
    assert row["ticker"] == "AAA" and row["earnings_date_source"] == "fmp live" and row["price_source"] == "fmp live" and row["history_source"] == "fmp live" and row["bank_context"] is None
    assert out["data_sufficiency"]["bank_reads"] == 0 and out["data_sufficiency"]["http_ok"] >= 3


if __name__ == "__main__":
    test_bank_first_inputs_calendar_candidates_and_labels()
    print("ok test_bank_first_inputs_calendar_candidates_and_labels")
    test_bank_off_runs_the_live_path_and_labels_it()
    print("ok test_bank_off_runs_the_live_path_and_labels_it")
    print("2 tests passed")
