"""justhodl-smart-beta offline tests (v1.1.0 bank fusion): value/quality facts from the factor book, 252-session closes
from the Polygon EOD bank, a labelled whole-market pass; with the banks absent the live FMP path runs unchanged.
No network, no AWS, no key.

Run: python3 aws/lambdas/justhodl-smart-beta/tests/run_tests.py
"""
import gzip
import io
import json
import math
import os
import sys
import types
from datetime import datetime, timedelta, timezone

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "source"))
os.environ["FMP_KEY"] = "test-key-not-real"
os.environ.pop("TELEGRAM_TOKEN", None)


def eod_rows(n=300, start=100.0, drift=0.0005, wobble=0.01):
    """n trading-day rows [t_ms, o, h, l, c, v, vw, n] ending yesterday, a gentle uptrend with a deterministic wobble."""
    rows, d, px = [], datetime.now(timezone.utc).date() - timedelta(days=1), start
    i = 0
    while len(rows) < n:
        if d.weekday() < 5:
            px = px * (1 + drift + wobble * math.sin(i))
            t = int(datetime(d.year, d.month, d.day, 5, tzinfo=timezone.utc).timestamp() * 1000)
            rows.append([t, px, px, px, round(px, 4), 1000, px, 10])
            i += 1
        d -= timedelta(days=1)
    return list(reversed(rows))


class FakeS3:
    def __init__(self):
        self.store = {"data/warm/polygon-full/eod/AAPL.json.gz": gzip.compress(json.dumps({"ticker": "AAPL", "rows": eod_rows()}).encode()),
                      "data/warm/polygon-full/eod/BRK-B.json.gz": gzip.compress(json.dumps({"ticker": "BRK-B", "rows": eod_rows(n=120)}).encode())}
        self.puts = {}

    def get_object(self, Bucket, Key):
        if Key not in self.store:
            raise KeyError(Key)
        return {"Body": io.BytesIO(self.store[Key])}

    def put_object(self, Bucket, Key, Body, **kw):
        self.puts[Key] = json.loads(Body)


def row(mcap, pe, pb, roic, gm, r252, r21, vol, sector="Technology", **extra):
    return dict({"mcap": mcap, "pe": pe, "pb": pb, "roic": roic, "gross_margin": gm, "ret_252d": r252, "ret_21d": r21, "volatility_20d": vol, "sector": sector}, **extra)


class FakeBook(types.ModuleType):
    on = True
    rows = {}

    def available(self):
        return self.on

    def as_of(self):
        return {"fmp-bank": "2026-10-25T23:40:00+00:00", "tape": "2026-10-24"} if self.on else None

    def lookup(self, symbol, fields=None):
        r = self.rows.get(str(symbol).upper())
        return None if r is None else dict({k: r.get(k) for k in (fields or r)}, symbol=str(symbol).upper())

    def screen(self, where, fields=None, limit=None, common_only=True):
        out = []
        for s, r in self.rows.items():
            rr = dict(r, symbol=s)
            if common_only and rr.get("is_etf"):
                continue
            if where(rr):
                out.append(rr)
        return out[:limit] if limit else out


def boot(book_on=True):
    import boto3
    s3 = FakeS3()
    boto3.client = lambda *a, **k: s3
    sys.modules["fmp_bankfirst"] = types.ModuleType("fmp_bankfirst")
    sys.modules["fmp_bankfirst"].install = lambda: None
    peod = types.ModuleType("polygon_bankfirst")
    peod.ROOT, peod.BUCKET = "data/warm/polygon-full/eod/", "justhodl-dashboard-live"
    sys.modules["polygon_bankfirst"] = peod
    book = FakeBook("bank_enrich")
    book.on = book_on
    book.rows = {"AAPL": row(3.9e12, 30.0, 45.0, 0.55, 0.46, 20.0, 2.0, 22.0),
                 "BRK-B": row(1.0e12, 12.0, 1.5, 0.08, None, 10.0, 1.0, 15.0, sector="Financial Services"),
                 "M1": row(5e9, 10.0, 1.2, 0.20, 0.40, 30.0, 5.0, 18.0, sector="Industrials"),
                 "M2": row(6e9, 18.0, 3.0, 0.12, 0.30, -5.0, -2.0, 40.0, sector="Energy"),
                 "M3": row(7e9, 25.0, 6.0, 0.30, 0.60, 15.0, 1.0, 20.0, sector="Healthcare"),
                 "M4": row(8e9, 8.0, 0.9, 0.15, 0.35, 12.0, 0.5, 12.0, sector="Utilities"),
                 "M5": row(9e9, 40.0, 10.0, 0.05, 0.20, 50.0, 10.0, 60.0, sector="Technology"),
                 "SMALL": row(1e9, 5.0, 0.5, 0.5, 0.5, 5.0, 1.0, 10.0),
                 "ADRX": row(9e10, 5.0, 0.5, 0.5, 0.5, 5.0, 1.0, 10.0, is_adr=True),
                 "ETFX": row(9e10, None, None, None, None, 5.0, 1.0, 10.0, is_etf=True)}
    sys.modules["bank_enrich"] = book
    sys.modules.pop("lambda_function", None)
    import lambda_function as lf
    lf._book, lf._peod, lf._EOD_S3 = book, peod, None
    lf.FMP_SLEEP_SEC = 0
    lf.STATIC_TOP50_SPX = [{"symbol": "AAPL", "sector": "Technology"}, {"symbol": "BRK-B", "sector": "Financial Services"},
                           {"symbol": "LIVE", "sector": "Healthcare"}]
    lf.live = []
    lf.http_json = lambda url, retries=4: lf.live.append(url) or {"_error": "HTTP 429"}      # live FMP answers 429 today
    return lf, s3, book


def test_book_facts_bank_prices_market_pass_and_labels():
    lf, s3, book = boot()
    r = lf.lambda_handler({}, None)
    assert json.loads(r["body"])["ok"] is True
    out = s3.puts["data/smart-beta.json"]
    assert out["version"] == "1.1.0" and out["book_as_of"]["tape"] == "2026-10-24" and out["n_universe"] == 3
    by = {x["ticker"]: x for x in out["all_tickers"]}
    a = by["AAPL"]
    assert a["facts_source"] == "factor-book (bank close)" and a["price_source"].startswith("polygon-eod bank (last session ")
    assert a["value_pct"] is None and a["composite"] is None                                  # < 5 valid names -> ranks are None by the engine's own rule (unchanged)
    assert not [u for u in lf.live if "AAPL" in u], "a banked name must cost no live call: %s" % lf.live
    b = by["BRK-B"]
    assert b["facts_source"] == "factor-book (bank close)" and b["price_source"] == "unavailable"        # 120 banked sessions < 252 -> live tried -> 429
    assert any("BRK-B" in u and "historical-price-eod" in u for u in lf.live), "120 banked sessions are too few: the live price path must be tried"
    assert by["LIVE"]["facts_source"] == "live unavailable" and by["LIVE"]["price_source"] == "unavailable"
    assert out["facts_source_counts"] == {"factor-book": 2, "live unavailable": 1} and out["price_source_counts"]["polygon-eod bank"] == 1
    # the whole-market pass: 7 candidates (>= $2B, common, not ADR); 4-factor ranks from the book alone, no live call
    m = out["market"]
    assert m["available"] and m["n_candidates"] == 7 and m["n_valid"] == 7, m
    assert set(x["ticker"] for x in m["top_25"]) == {"AAPL", "BRK-B", "M1", "M2", "M3", "M4", "M5"}
    top = m["top_25"][0]
    assert top["ticker"] in ("M1", "M4") and top["n_factors_valid"] == 4 and top["low_vol_pct"] is not None
    assert m["factor_notes"]["low_vol"].startswith("PROXY") and m["universe_basis"].startswith("factor book: US common stocks >= $2B")
    mk = {x["ticker"]: x for x in m["top_25"]}
    assert mk["M1"]["twelve_minus_one_pct"] == 25.0 and mk["M1"]["vol_20d_annualized_pct"] == 18.0 and mk["BRK-B"]["n_factors_valid"] == 3   # no gross margin -> 3 factors
    assert m["factor_leaders"]["low_vol"][0]["ticker"] == "M4" and m["factor_leaders"]["momentum"][0]["ticker"] == "M5"
    assert not [u for u in lf.live if any(k in u for k in ("M1", "M2", "M3", "M4", "M5"))], "the market pass must not go live"


def test_without_the_book_the_live_path_runs_unchanged():
    lf, s3, book = boot(book_on=False)
    s3.store.clear()                                                                       # no Polygon EOD bank either

    def http_json(url, retries=4):
        lf.live.append(url)
        if "AAPL" not in url:
            return {"_error": "HTTP 429"}
        if "key-metrics-ttm" in url:
            return [{"returnOnInvestedCapitalTTM": 0.5}]
        if "ratios-ttm" in url:
            return [{"priceToEarningsRatioTTM": 28.0, "priceToBookRatioTTM": 40.0, "grossProfitMarginTTM": 0.45}]
        if "historical-price-eod" in url:
            return [{"date": r[0] and datetime.fromtimestamp(r[0] / 1000, tz=timezone.utc).date().isoformat(), "close": r[4]} for r in eod_rows()]
        return {"_error": "x"}
    lf.http_json = http_json
    lf.lambda_handler({}, None)
    out = s3.puts["data/smart-beta.json"]
    by = {x["ticker"]: x for x in out["all_tickers"]}
    assert by["AAPL"]["facts_source"] == "fmp live" and by["AAPL"]["price_source"] == "fmp live historical-price-eod"
    assert by["LIVE"]["facts_source"] == "factor book unavailable; live unavailable"
    assert out["market"] == {"available": False, "reason": "factor book unavailable"} and out["book_as_of"] is None
    assert out["universe_basis"].startswith("STATIC_TOP50_SPX; TTM facts live FMP (factor book unavailable)")
    # the core factor functions are untouched: 12-1 momentum and 252-day vol computed from the live closes
    raw = next(x for x in out["top_25_diversified"] + out["bottom_25_laggards"] if x["ticker"] == "AAPL") if (out["top_25_diversified"] or out["bottom_25_laggards"]) else None
    assert raw is None or (raw["twelve_minus_one_pct"] is not None and raw["annualized_vol_pct"] is not None)


if __name__ == "__main__":
    test_book_facts_bank_prices_market_pass_and_labels()
    print("ok test_book_facts_bank_prices_market_pass_and_labels")
    test_without_the_book_the_live_path_runs_unchanged()
    print("ok test_without_the_book_the_live_path_runs_unchanged")
    print("2 tests passed")
