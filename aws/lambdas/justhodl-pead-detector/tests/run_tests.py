"""justhodl-pead-detector offline tests (v1.1.0 bank fusion): earnings rows come from the walker file, then the factor
book, then live; the drift reads the book's returns; the universe gains the book's beat-streak candidates; both paths
(book on / off) write the same document shape.  No network, no AWS, no key.

Run: python3 aws/lambdas/justhodl-pead-detector/tests/run_tests.py
"""
import json
import os
import sys
import types
from datetime import datetime, timedelta

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "source"))
os.environ["FMP_KEY"] = "test-key-not-real"
os.environ["N_WORKERS"] = "2"

TODAY = datetime.now()


def d(days):
    return (TODAY + timedelta(days=days)).strftime("%Y-%m-%d")


def quarters(n_beats, start_days_ago=20, rev=True):
    """n reported quarters newest first, every one a beat growing in magnitude (newest biggest), plus one future report."""
    rows = [{"date": d(45), "symbol": "X", "epsActual": None, "epsEstimated": 1.0, "revenueActual": None, "revenueEstimated": 100.0}]
    for i in range(n_beats):
        est = 1.0
        act = 1.0 + 0.30 - 0.05 * i                    # 30%, 25%, 20% ... beats
        rows.append({"date": d(-(start_days_ago + 91 * i)), "symbol": "X", "epsActual": act, "epsEstimated": est,
                     "revenueActual": 110.0 if rev else None, "revenueEstimated": 100.0 if rev else None})
    return rows


class FakeS3:
    def __init__(self, universe):
        self.universe = universe
        self.written = {}

    def get_object(self, Bucket, Key):
        if Key == "data/universe.json":
            return {"Body": types.SimpleNamespace(read=lambda: json.dumps({"stocks": self.universe}).encode())}
        raise KeyError(Key)

    def put_object(self, **k):
        self.written[k["Key"]] = json.loads(k["Body"])


class FakeBook(types.ModuleType):
    on = True
    docs = {}
    rows = {}        # symbol -> matrix row (lookup / prescreen)

    def available(self):
        return self.on

    def as_of(self):
        return {"fmp-bank": "2026-10-25", "polygon-tape": "2026-10-24"} if self.on else None

    def lookup(self, symbol, fields=None):
        r = self.rows.get(str(symbol).upper())
        if r is None:
            return None
        return {k: r.get(k) for k in (fields or r)} | {"symbol": str(symbol).upper()}

    def factor(self, symbol):
        return self.docs.get(str(symbol).upper())

    def prescreen_rows(self, where, order_by=None, descending=True, limit=1500, fields=None, common_only=True):
        hits = [dict(r, symbol=s) for s, r in self.rows.items() if where(dict(r, symbol=s))]
        hits.sort(key=lambda r: r.get(order_by) or 0, reverse=descending)
        return hits[:limit]

    def prescreen(self, where, order_by=None, descending=True, limit=1500, fields=None, common_only=True):
        return [r["symbol"] for r in self.prescreen_rows(where, order_by, descending, limit, fields, common_only)]   # the real SDK returns SYMBOLS


class FakeBank(types.ModuleType):
    earnings = {}

    def walker(self, dataset, entity=None, period=None, day=None, exchange=None):
        assert dataset == "earnings" and entity
        return self.earnings.get(entity)


def boot(universe):
    import boto3
    s3 = FakeS3(universe)
    boto3.client = lambda *a, **k: s3
    sys.modules["fmp_bankfirst"] = types.ModuleType("fmp_bankfirst")
    sys.modules["fmp_bankfirst"].install = lambda: None
    ms = types.ModuleType("managed_secret")
    ms.managed_secret = lambda *a, **k: "x"
    sys.modules["managed_secret"] = ms
    book = FakeBook("bank_enrich")
    bank = FakeBank("fmp_bank")
    sys.modules["bank_enrich"] = book
    sys.modules["fmp_bank"] = bank
    sys.modules.pop("lambda_function", None)
    import lambda_function as lf
    lf._book, lf._bank, lf.S3 = book, bank, s3
    return lf, s3, book, bank


def test_bank_first_earnings_book_drift_candidates_and_labels():
    universe = [{"symbol": "AAA", "cap_bucket": "mid", "sector": "Technology", "market_cap": 5e9},
                {"symbol": "CCC", "cap_bucket": "small", "sector": "Industrials", "market_cap": 1e9}]
    lf, s3, book, bank = boot(universe)
    bank.earnings = {"AAA": quarters(5)}                                     # walker file: 5 beats with revenue
    book.docs = {"BBB": {"symbol": "BBB", "surprises": [{"date": d(-(20 + 91 * i)), "actualEarningResult": 1.2 - 0.05 * i, "estimatedEarning": 1.0} for i in range(4)],
                         "calendar": {"earnings": [{"date": d(40), "epsEstimated": 1.1, "revenueEstimated": 90.0}]}}}
    book.rows = {"AAA": {"ret_5d": 0.01, "ret_21d": 0.12, "ret_63d": 0.2, "mcap": 5e9, "sector": "Technology", "altman_z": 4.1, "piotroski": 8, "short_pct_float": 2.5, "pt_upside_pct": 15.0, "pt_n_1m": 12, "consensus_score": 0.7, "beat_rate_4q": 1.0, "avg_surprise_pct_4q": 22.5, "composite_growth": 90.0, "composite_street": 80.0, "composite_quality": 85.0},
                 "BBB": {"ret_5d": 0.0, "ret_21d": -0.08, "ret_63d": 0.0, "mcap": 400e6, "sector": "Healthcare", "beat_rate_4q": 1.0, "avg_surprise_pct_4q": 12.5, "is_adr": False},
                 "DDD": {"mcap": 9e9, "beat_rate_4q": 0.5, "avg_surprise_pct_4q": 30.0},                       # beat rate too low -> not a candidate
                 "EEE": {"mcap": 10e6, "beat_rate_4q": 1.0, "avg_surprise_pct_4q": 30.0}}                      # below the cap floor
    live = []
    lf.fetch_url = lambda url, timeout=15: live.append(url) or (quarters(3, rev=False) if "symbol=CCC" in url else [])
    lf.lambda_handler({}, None)
    out = s3.written[lf.S3_KEY]
    assert out["version"] == "1.1.0" and out["method"] == "pead_detector_v1.1" and out["book_as_of"]["fmp-bank"] == "2026-10-25"
    assert out["stats"]["n_universe_file"] == 2 and out["stats"]["n_book_candidates"] == 1 and out["stats"]["n_universe"] == 3, out["stats"]
    assert "factor book beat-streak candidates (1" in out["universe_basis"], out["universe_basis"]
    basis = out["stats"]["earnings_basis"]
    assert basis == {"fmp-walker earnings": 1, "factor-book surprises+calendar": 1, "fmp live": 1, "none": 0}, basis
    assert [u for u in live if "stable/earnings?symbol=CCC" in u] and not [u for u in live if "symbol=AAA" in u or "symbol=BBB" in u], live
    rows = {r["symbol"]: r for r in out["all_qualifying"]}
    assert set(rows) == {"AAA", "BBB", "CCC"}, set(rows)
    aaa = rows["AAA"]
    assert aaa["earnings_basis"] == "fmp-walker earnings" and aaa["metrics"]["streak"] == 5 and aaa["flags"][0] == "STREAK_5Q+"
    assert aaa["drift_basis"] == "factor-book ret_5d" and aaa["metrics"]["post_earnings_drift_pct"] == 1.0      # 20 days after the report - 35 -> 0 days -> ret_5d
    assert aaa["bank_context"]["altman_z"] == 4.1 and aaa["bank_context"]["source"] == "factor-book"
    assert aaa["metrics"]["next_earnings_date"] == d(45) and 40 <= aaa["metrics"]["days_to_next_earnings"] <= 45
    assert any(h["actual"] == 1.3 for h in aaa["history"])
    bbb = rows["BBB"]
    assert bbb["earnings_basis"] == "factor-book surprises+calendar" and bbb["metrics"]["streak"] == 4 and bbb["metrics"]["cap_bucket"] == "small"   # universe-builder bands: $300M-$2B = small
    assert bbb["metrics"]["sector"] == "Healthcare" and bbb["metrics"]["market_cap"] == 400e6 and bbb["metrics"]["next_earnings_date"] == d(40)
    assert rows["CCC"]["earnings_basis"] == "fmp live" and rows["CCC"]["metrics"]["streak"] == 3 and rows["CCC"]["bank_context"] is None
    assert out["stats"]["n_tier_s"] >= 1 and all(r["tier"] in ("TIER_S_DRIFTING", "TIER_A_BEATING", "TIER_B_BUILDING", "QUIET") for r in out["all_qualifying"])


def test_book_off_keeps_the_live_path_and_says_so():
    universe = [{"symbol": "AAA", "cap_bucket": "mid", "sector": "Technology", "market_cap": 5e9}]
    lf, s3, book, bank = boot(universe)
    book.on = False
    bank.earnings = {}
    hist = [{"date": d(-i), "close": 100.0 + i} for i in range(89, -1, -1)]
    lf.fetch_url = lambda url, timeout=15: quarters(4) if "stable/earnings" in url else (hist if "historical-price-eod" in url else [])
    lf.lambda_handler({}, None)
    out = s3.written[lf.S3_KEY]
    assert out["book_as_of"] is None and out["universe_basis"].endswith("factor book unavailable") and out["stats"]["n_book_candidates"] == 0
    row = out["all_qualifying"][0]
    assert row["earnings_basis"] == "fmp live" and row["bank_context"] is None and row["drift_basis"] == "fmp live history"
    assert row["metrics"]["streak"] == 4 and row["metrics"]["post_earnings_drift_pct"] is not None
    assert out["stats"]["earnings_basis"]["fmp live"] == 1


if __name__ == "__main__":
    test_bank_first_earnings_book_drift_candidates_and_labels()
    print("ok test_bank_first_earnings_book_drift_candidates_and_labels")
    test_book_off_keeps_the_live_path_and_says_so()
    print("ok test_book_off_keeps_the_live_path_and_says_so")
    print("2 tests passed")
