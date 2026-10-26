"""justhodl-starmine offline tests (v1.1.0 bank fusion): the universe gains the factor book's analyst-covered names,
prices come from the bank close, grades / targets / surprises come from the walker bank or the book before any live
call, every row is labelled, and the book-off path is the old live engine.  No network, no AWS, no key.

Run: python3 aws/lambdas/justhodl-starmine/tests/run_tests.py
"""
import json
import os
import sys
import types
from datetime import datetime, timedelta, timezone

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "source"))
os.environ["FMP_KEY"] = "test-key-not-real"
os.environ["TELEGRAM_TOKEN"] = ""

TODAY = datetime.now(timezone.utc).date()


def d(days_ago):
    return (TODAY - timedelta(days=days_ago)).isoformat()


def grades(n_up, n_down):
    rows = [{"symbol": "X", "date": d(5 + i), "gradingCompany": "Goldman Sachs" if i % 2 else "Needham", "action": "upgrade", "newGrade": "Buy", "previousGrade": "Hold"} for i in range(n_up)]
    rows += [{"symbol": "X", "date": d(9 + i), "gradingCompany": "UBS", "action": "downgrade", "newGrade": "Neutral", "previousGrade": "Buy"} for i in range(n_down)]
    rows.append({"symbol": "X", "date": d(400), "gradingCompany": "Citi", "action": "upgrade", "newGrade": "Buy", "previousGrade": "Hold"})   # outside 90 d
    return rows


def earnings(n_beats, n_miss):
    rows = [{"date": d(30 + 91 * i), "epsActual": 1.1, "epsEstimated": 1.0} for i in range(n_beats)]
    rows += [{"date": d(30 + 91 * (n_beats + i)), "epsActual": 0.9, "epsEstimated": 1.0} for i in range(n_miss)]
    rows.append({"date": (TODAY + timedelta(days=60)).isoformat(), "epsActual": None, "epsEstimated": 1.2})
    return rows


class FakeS3:
    def __init__(self):
        self.written = {}

    def put_object(self, **k):
        self.written[k["Key"]] = json.loads(k["Body"])

    def get_object(self, **k):
        raise KeyError(k)


class FakeBook(types.ModuleType):
    on = True
    rows = {}
    docs = {}

    def available(self):
        return self.on

    def as_of(self):
        return {"fmp-bank": "2026-10-25"} if self.on else None

    def lookup(self, symbol, fields=None):
        r = self.rows.get(str(symbol).upper())
        return None if r is None else {k: r.get(k) for k in (fields or r)} | {"symbol": str(symbol).upper()}

    def factor(self, symbol):
        return self.docs.get(str(symbol).upper())

    def prescreen_rows(self, where, order_by=None, descending=True, limit=1500, fields=None, common_only=True):
        hits = [dict(r, symbol=s) for s, r in self.rows.items() if where(dict(r, symbol=s))]
        hits.sort(key=lambda r: r.get(order_by) or 0, reverse=descending)
        return hits[:limit]

    def prescreen(self, where, order_by=None, descending=True, limit=1500, fields=None, common_only=True):
        return [r["symbol"] for r in self.prescreen_rows(where, order_by, descending, limit, fields, common_only)]   # the real SDK returns SYMBOLS


class FakeBank(types.ModuleType):
    files = {}

    def walker(self, dataset, entity=None, period=None, day=None, exchange=None):
        return self.files.get((dataset, entity))


def boot():
    import boto3
    s3 = FakeS3()
    boto3.client = lambda *a, **k: s3
    sys.modules["fmp_bankfirst"] = types.ModuleType("fmp_bankfirst")
    sys.modules["fmp_bankfirst"].install = lambda: None
    book, bank = FakeBook("bank_enrich"), FakeBank("fmp_bank")
    sys.modules["bank_enrich"], sys.modules["fmp_bank"] = book, bank
    sys.modules.pop("lambda_function", None)
    import lambda_function as lf
    lf._book, lf._bank = book, bank
    lf.FMP_SLEEP_SEC = 0
    lf.telegram_notify = lambda msg: None
    return lf, s3, book, bank


def test_book_universe_bank_first_inputs_and_labels():
    lf, s3, book, bank = boot()
    static = [u["symbol"] for u in lf.STATIC_TOP50_SPX]
    # the book prices every static name; 20 analyst-covered book names join the universe (B00..B19); C00 fails the gates
    for i, sym in enumerate(static):
        book.rows[sym] = {"price": 100.0 + i, "mcap": 1e12 - i * 1e9, "sector": "Technology"}
    for i in range(20):
        sym = "B%02d" % i
        book.rows[sym] = {"price": 50.0 + i, "mcap": 50e9 - i * 1e9, "pt_n_1q": 8, "sector": "Healthcare", "pt_avg_1q": 70.0 + i, "pt_n_1m": 3,
                          "consensus_score": 0.5, "pt_upside_pct": 20.0, "beat_rate_4q": 0.75, "altman_z": 3.5, "short_pct_float": 2.0, "composite_street": 70.0, "ret_63d": 0.1}
        bank.files[("grades", sym)] = grades(n_up=3 + (i % 3), n_down=i % 2)
        if i % 2 == 0:
            bank.files[("price-target-consensus", sym)] = [{"symbol": sym, "targetConsensus": 80.0 + i, "numberOfAnalysts": 12}]
        if i % 3 == 0:
            bank.files[("earnings", sym)] = earnings(6, 2)
        else:
            book.docs[sym] = {"surprises": [{"date": d(30 + 91 * q), "actualEarningResult": 1.05, "estimatedEarning": 1.0} for q in range(8)]}
    book.rows["C00"] = {"price": 10.0, "mcap": 1e9, "pt_n_1q": 9}            # below the $5B floor
    book.rows["C01"] = {"price": 10.0, "mcap": 9e9, "pt_n_1q": 2}            # too few targets
    live = []
    lf.http_json = lambda url, retries=2: live.append(url) or {"_error": "HTTP 429", "_code": 429}
    lf.lambda_handler({}, None)
    out = s3.written[lf.S3_KEY]
    assert out["ok"] and out["version"] == "1.1.0" and out["n_book_candidates"] == 20 and out["book_as_of"] == {"fmp-bank": "2026-10-25"}
    assert out["universe_source_tier"].endswith("+factor_book_20") and "analyst-covered" in out["universe_basis"], out["universe_source_tier"]
    assert out["n_universe_analyzed"] == 70 and out["n_scored"] >= 17 and len(out["top_25_conviction"]) == 25
    bc = out["basis_counts"]
    assert bc["price"] == {"factor-book close": 70}, bc["price"]
    assert bc["grades"]["fmp-walker grades"] == 20 and bc["grades"].get("fmp live", 0) == 0 and bc["grades"]["none"] == 50, bc["grades"]   # static names: live 429 -> none
    assert bc["pt"]["fmp-walker price-target-consensus"] == 10 and bc["pt"]["factor-book price-target summary (1q avg)"] == 10, bc["pt"]
    assert bc["esp"]["fmp-walker earnings"] == 7 and bc["esp"]["factor-book surprises"] == 13, bc["esp"]
    assert not [u for u in live if "/quote?" in u], "every price came from the book - no live quote batch"
    top = out["top_25_conviction"]
    book_rows = [t for t in top if t["from_book"]]
    assert book_rows and all(t["bank_context"]["source"] == "factor-book" for t in book_rows) and all(t["price_source"] == "factor-book close" for t in top)
    assert all(t["basis"]["grades"] == "fmp-walker grades" for t in book_rows) and all(t["n_upgrades_90d"] >= 3 for t in book_rows)
    assert any(t["ptd_pct"] is not None and t["pt_consensus"] is not None for t in book_rows)
    assert out["methodology"]["universe"] == out["universe_basis"] and "walker bank" in out["sources"]["grades"]


def test_book_off_is_the_old_live_engine_with_labels():
    lf, s3, book, bank = boot()
    book.on = False
    bank.files = {}
    static = [u["symbol"] for u in lf.STATIC_TOP50_SPX]

    def http_json(url, retries=2):
        if "/quote?" in url:
            syms = url.split("symbol=")[1].split("&")[0].split(",")
            return [{"symbol": s, "price": 100.0, "marketCap": 1e11} for s in syms]
        if "/grades?" in url:
            return grades(2, 1)
        if "/price-target-consensus?" in url:
            return [{"targetConsensus": 120.0, "numberOfAnalysts": 10}]
        if "/earnings?" in url:
            return earnings(5, 3)
        return {"_error": "unexpected"}
    lf.http_json = http_json
    lf.lambda_handler({}, None)
    out = s3.written[lf.S3_KEY]
    assert out["n_book_candidates"] == 0 and out["book_as_of"] is None and out["universe_basis"].endswith("factor book unavailable")
    assert out["universe_source_tier"] == "static_top50_live_prices" and out["n_universe_analyzed"] == 50
    assert out["basis_counts"]["grades"] == {"fmp live": 50} and out["basis_counts"]["price"] == {"fmp live": 50}
    assert all(t["bank_context"] is None and t["price_source"] == "fmp live" for t in out["top_25_conviction"])
    assert len(out["top_25_conviction"]) == 25 and out["median_esp_hit_pct"] == 71.4      # the future report takes one of the 8 slots (pre-existing), 5 beats of 7 reported


if __name__ == "__main__":
    test_book_universe_bank_first_inputs_and_labels()
    print("ok test_book_universe_bank_first_inputs_and_labels")
    test_book_off_is_the_old_live_engine_with_labels()
    print("ok test_book_off_is_the_old_live_engine_with_labels")
    print("2 tests passed")
