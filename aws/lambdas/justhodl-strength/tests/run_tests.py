"""justhodl-strength offline tests: an in-memory S3 holding a small Polygon EOD bank (gzip JSON per ticker, the bank's own
row layout), a native ETF desk packet, a canonical flow packet, the 13F per-ticker table and the bank manifest; FMP's
profile bulk and the S&P 500 walker list faked. The math (horizons, RS metrics, base lengths, coil score, posture,
pattern, ranks), the flow / 13F joins, the universe assembly, the boards and the full build() + handler are checked.
No network, no AWS.

Run: python3 aws/lambdas/justhodl-strength/tests/run_tests.py
"""
from datetime import date, datetime, timedelta, timezone
from types import SimpleNamespace
import gzip
import importlib.util
import io
import json
import math
import os
import random
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
SRC = os.path.join(HERE, "..", "source")
SHARED = os.path.join(HERE, "..", "..", "..", "shared")
sys.path[:0] = [SRC]
BEGUN = []
sys.modules["chain_guard"] = SimpleNamespace(begin=lambda event, context=None: BEGUN.append(event) or 0)
for name in ("boto3", "botocore", "botocore.config"):
    if name not in sys.modules:
        try:
            __import__(name)
        except ImportError:
            sys.modules[name] = SimpleNamespace(client=lambda *a, **k: None, Config=lambda **k: None)
_spec = importlib.util.spec_from_file_location("polygon_bankfirst", os.path.join(SHARED, "polygon_bankfirst.py"))
pb = importlib.util.module_from_spec(_spec)
sys.modules["polygon_bankfirst"] = pb
_spec.loader.exec_module(pb)
sys.modules["etf_desk_catalog"] = SimpleNamespace(DESK=("SPY", "QQQ", "GLD", "TLT", "XLE"))
PROFILES = [
    {"symbol": "AAPL", "companyName": "Apple Inc.", "sector": "Technology", "industry": "Consumer Electronics", "isEtf": "false", "exchangeShortName": "NASDAQ"},
    {"symbol": "NVDA", "companyName": "NVIDIA Corporation", "sector": "Technology", "industry": "Semiconductors", "isEtf": "false", "exchangeShortName": "NASDAQ"},
    {"symbol": "COIL", "companyName": "Coil Industries", "sector": "Industrials", "industry": "Specialty Industrial Machinery", "isEtf": "false", "exchangeShortName": "NYSE"},
    {"symbol": "LAGG", "companyName": "Laggard Corp", "sector": "Consumer Cyclical", "industry": "Apparel Retail", "isEtf": "false", "exchangeShortName": "NYSE"},
    {"symbol": "QQQ", "companyName": "Invesco QQQ Trust", "sector": "", "industry": "", "isEtf": "true", "exchangeShortName": "NASDAQ"},
    {"symbol": "GLD", "companyName": "SPDR Gold Shares", "sector": "", "industry": "", "isEtf": "true", "exchangeShortName": "NYSEARCA"},
    {"symbol": "NEWB", "companyName": "New Listing Inc", "sector": "Technology", "industry": "Software - Application", "isEtf": "false", "exchangeShortName": "NASDAQ"},
]
SP500 = [{"symbol": "AAPL", "name": "Apple Inc."}, {"symbol": "NVDA", "name": "NVIDIA"}, {"symbol": "COIL", "name": "Coil Industries"}, {"symbol": "LAGG", "name": "Laggard Corp"},
         {"symbol": "BRK-B", "name": "Berkshire Hathaway"}, {"symbol": "NEWB", "name": "New Listing Inc"}, {"symbol": "SPLT", "name": "Reverse Split Co"}, {"symbol": "RCRD", "name": "Recorded Split Co"}, {"symbol": "DEAL", "name": "Pending Deal Co"}]
sys.modules["fmp_bank"] = SimpleNamespace(walker=lambda dataset, entity=None, **kw: [dict(r) for r in SP500] if dataset == "sp500-constituent" else None,
                                          snapshot=lambda path, symbol=None: [dict(r) for r in PROFILES] if path == "profile" and symbol is None else None)

import lambda_function as lf  # noqa: E402
lf.THESIS_MIN_ROWS = 5   # the synthetic bank ranks ten names; production needs 20 per session

NOW = datetime(2026, 11, 4, 3, 40, tzinfo=timezone.utc)
START = date(2024, 10, 1)


class NoSuchKey(Exception):
    pass


class S3Stub:
    def __init__(self):
        self.store, self.puts = {}, []

    def put(self, key, doc):
        body = json.dumps(doc).encode()
        self.store[key] = gzip.compress(body) if key.endswith(".gz") else body

    def get_object(self, Bucket, Key):
        if Key not in self.store:
            raise NoSuchKey("NoSuchKey: " + Key)
        return {"Body": io.BytesIO(self.store[Key]), "LastModified": datetime(2026, 11, 4, 3, 13, tzinfo=timezone.utc), "ContentLength": len(self.store[Key])}

    def head_object(self, Bucket, Key):
        if Key not in self.store:
            raise NoSuchKey("NoSuchKey: " + Key)
        return {"ContentLength": len(self.store[Key]), "LastModified": datetime(2026, 11, 4, 3, 13, tzinfo=timezone.utc)}

    def put_object(self, **kw):
        self.puts.append(kw)
        self.store[kw["Key"]] = kw["Body"]


def sessions(n):
    out, d = [], START
    while len(out) < n:
        if d.weekday() < 5:
            out.append(d)
        d += timedelta(days=1)
    return out


DATES = sessions(300)


def series(kind, n=300, seed=1):
    """Synthetic daily closes: 'bench' a steady 0.04%/day drift with noise; 'leader' bench + 0.12%/day; 'laggard' bench - 0.10%/day;
    'coil' a 150-session uptrend then 150 sessions flat inside a 4% band with shrinking volume; 'newb' 30 sessions."""
    rnd = random.Random(seed)
    px, out = 100.0, []
    for i in range(n):
        if kind == "bench":
            r = 0.0004 + rnd.gauss(0, 0.003)
        elif kind == "leader":
            r = 0.0060 + rnd.gauss(0, 0.003)
        elif kind == "laggard":
            r = -0.0020 + rnd.gauss(0, 0.003)
        elif kind == "coil":
            r = 0.003 + rnd.gauss(0, 0.004) if i < 150 else 0.0
        else:
            r = rnd.gauss(0, 0.01)
        px *= (1 + r)
        if kind == "coil" and i >= 150:
            px = 200.0 * (1 + 0.012 * math.sin(i / 3.0))   # +-1.2% around 200: a very tight base
        out.append(px)
    return out


def bank_rows(closes, vol_profile=None):
    rows = []
    for i, (d, c) in enumerate(zip(DATES[-len(closes):], closes)):
        t = int(datetime(d.year, d.month, d.day, 5, tzinfo=timezone.utc).timestamp() * 1000)
        v = vol_profile(i) if vol_profile else 1_000_000.0
        rows.append([t, c * 0.998, c * 1.006, c * 0.994, c, v, c, 1000])
    return rows


def desk_fund(t, w1, w5, w21, aum=1e10, eligible=True, valid="2026-11-05T00:00:00+00:00", aligned=True, own_end="2026-11-02"):
    def win(n, usd, end):
        return {"status": "matched_reporting_window", "requested_observations": n, "start_date": "2026-10-27", "end_date": end, "flow_usd_decimal": str(usd),
                "end_reported_assets_usd_decimal": str(aum), "flow_to_end_assets_pct_decimal": str(round(usd / aum * 100, 4))}
    fl = {"ticker": t, "source_valid_until": valid, "latest_effective_date": own_end, "latest_processed_date": own_end, "quality": {"status": "complete_acquired_history"},
          "latest_observation": {"effective_date": own_end, "reported_assets_usd_decimal": str(aum)},
          "latest_windows": {"1": win(1, w1, own_end), "5": win(5, w5, own_end), "21": win(21, w21, own_end)},
          "aligned_windows": {k: (win(int(k), v, "2026-11-02") if aligned else {"status": "incomplete", "reasons": ["missing_reference_observations"]}) for k, v in (("1", w1), ("5", w5), ("21", w21))}}
    return {"ticker": t, "flows": fl, "quality": {"flow_current_eligible": eligible}, "profiles": {"current": {"summary": {"text": {"description": {"value": t + " fund"}}}}}}


def seed():
    s3 = S3Stub()
    bench = series("bench")
    s3.put(pb.MANIFEST, {"last_session": DATES[-1].isoformat(), "updated_at": "2026-11-04T03:13:33+00:00", "tickers": 21088, "first_session": "2021-08-26"})
    s3.put(lf.bank_key("SPY"), {"rows": bank_rows(bench)})
    s3.put(lf.bank_key("NVDA"), {"rows": bank_rows(series("leader", seed=2))})
    s3.put(lf.bank_key("AAPL"), {"rows": bank_rows(series("bench", seed=3))})
    s3.put(lf.bank_key("LAGG"), {"rows": bank_rows(series("laggard", seed=4))})
    s3.put(lf.bank_key("COIL"), {"rows": bank_rows(series("coil", seed=5), vol_profile=lambda i: 2_000_000.0 if i < 280 else 900_000.0)})
    s3.put(lf.bank_key("QQQ"), {"rows": bank_rows(series("leader", seed=6))})
    s3.put(lf.bank_key("GLD"), {"rows": bank_rows(series("bench", seed=7))})
    s3.put(lf.bank_key("NEWB"), {"rows": bank_rows(series("newb", n=30, seed=8))})
    s3.put(lf.bank_key("BRK.B"), {"rows": bank_rows(series("bench", seed=9))})
    # SPLT: a bench-like series printed with a 1-for-3 reverse split at session 280 (price x3, volume /3) and NO corporate-actions record
    raw = series("bench", seed=10)
    s3.put(lf.bank_key("SPLT"), {"rows": bank_rows([c / 3.0 if i < 280 else c for i, c in enumerate(raw)], vol_profile=lambda i: 3_000_000.0 if i < 280 else 1_000_000.0)})
    # RCRD: a 2-for-1 split at session 250 recorded by the corporate-actions producer (price /2 after, volume x2)
    raw2 = series("bench", seed=11)
    s3.put(lf.bank_key("RCRD"), {"rows": bank_rows([c * 2.0 if i < 250 else c for i, c in enumerate(raw2)], vol_profile=lambda i: 500_000.0 if i < 250 else 1_000_000.0)})
    # DEAL: a stock pinned by a pending acquisition - the last 120 sessions inside 0.6%, almost no intraday range
    pinned = series("bench", seed=12)
    rows = bank_rows([c if i < 180 else 50.0 * (1 + 0.003 * math.sin(i)) for i, c in enumerate(pinned)])
    for i, r in enumerate(rows):
        if i >= 180:
            r[2], r[3] = r[4] * 1.001, r[4] * 0.999
    s3.put(lf.bank_key("DEAL"), {"rows": rows})
    s3.put(lf.ACTIONS_INDEX_KEY, {"generated_at": "2026-11-04T06:40:00+00:00", "covered": 2, "tickers": {"RCRD": {"n_splits": 1, "n_events": 3}, "AAPL": {"n_splits": 0, "n_events": 4}}})
    s3.put(lf.ACTIONS_PREFIX + "RCRD.json", {"ticker": "RCRD", "events": [{"date": DATES[250].isoformat(), "type": "split", "ratio_from": 1, "ratio_to": 2, "factor": 0.5, "source": "polygon"},
                                                                          {"date": DATES[100].isoformat(), "type": "dividend_cash", "factor": 0.995}]})
    # TLT, XLE: configured desk funds with no bank file (unbanked) ; SIVR unbanked too
    s3.put(lf.DESK_KEY, {"contract": "etf-desk-original-research.v1", "generated_at": "2026-11-03T23:06:42+00:00", "reference": {"end_date": "2026-11-02"},
                         "funds": {"SPY": desk_fund("SPY", 5e9, 2e9, 1.5e9), "QQQ": desk_fund("QQQ", 2.4e8, 5.7e9, 3.9e9, aligned=False, own_end="2026-10-30"),
                                   "GLD": desk_fund("GLD", -1.4e8, -3.2e7, -1.9e9), "TLT": desk_fund("TLT", 8e8, 1.2e9, -1.4e9, eligible=False),
                                   "XLE": desk_fund("XLE", 0, 0, 0, valid="2026-11-03T00:00:00+00:00")}})
    s3.put(lf.CANON_KEY, {"contract": "provider-fund-flow-research.v1", "generated_at": "2026-11-03T22:01:03+00:00", "reference": {"end_date": "2026-11-02"},
                          "funds": {"SIVR": desk_fund("SIVR", 1e6, 5e6, 2e7)["flows"], "IBIT": desk_fund("IBIT", 1e8, 4e8, 9e8)["flows"]}})
    s3.put(lf.LEADERS_KEY, {"engine": "justhodl-leaders", "generated_at": "2026-11-04T03:51:00+00:00", "industries": [
        {"industry": "Semiconductors", "sector": "Technology", "strength": 88.0, "vs": {"m": 4.2, "q": 9.1}, "ew_vs": {"m": 3.0}, "breadth": {"pct_above_200": 80}, "ad": {"label": "ACCUMULATION"}, "n_leaders": 5},
        {"industry": "Apparel Retail", "sector": "Consumer Cyclical", "strength": 12.0, "vs": {"m": -3.1, "q": -6.0}, "ew_vs": {"m": -2.0}, "breadth": {"pct_above_200": 20}, "ad": {"label": "DISTRIBUTION"}, "n_leaders": 5}]})
    s3.put(lf.TICKERS_KEY, {"contract": "13f-tickers.v1", "period": "2026-06-30", "generated_at": "2026-11-04T16:37:00+00:00", "n_tickers": 4, "columns": lf.F13_COLUMNS,
                            "by_ticker": {"NVDA": [40, 36, 4, 1.2e11, 1.0e11, 9e9, 2e9, 7e9, 22, 6, "ACCUMULATING", "stock", "equities", "Technology", "Semiconductors", 1, 20, 2],
                                          "AAPL": [38, 39, -1, 9e10, 9.5e10, 1e9, 4e9, -3e9, 8, 17, "DISTRIBUTING", "stock", "equities", "Technology", "Consumer Electronics", 1, 18, -1],
                                          "COIL": [5, 3, 2, 2e9, 1.5e9, 6e8, 1e8, 5e8, 4, 1, "ACCUMULATING", "stock", "equities", "Industrials", "Specialty Industrial Machinery", 1, 2, 1],
                                          "SPY": [12, 12, 0, 3e10, 2.9e10, 1e9, 1e9, 0, 3, 3, "CONTESTED", "fund", "equities", None, None, 1, 4, 0]}})
    return s3


def built(**kw):
    s3 = seed()
    return s3, lf.build(s3, NOW, log=lambda *a: None, **kw)


def test_math_horizons_rs_and_base_lengths():
    c = [100.0 * (1.01 ** i) for i in range(300)]
    h = lf.horizons(c)
    assert h["d"] == 1.0 and h["w"] == round((1.01 ** 5 - 1) * 100, 2) and h["y"] == round((1.01 ** 252 - 1) * 100, 2)
    assert lf.horizons([100.0])["d"] is None
    s = [100.0] * 300
    rs = lf.rs_metrics(c, s)
    assert rs["rs_trend_63"] is not None and rs["rs_trend_63"] > 200 and rs["rs_consistency_63"] == 100.0 and rs["rs_new_high"] is True and rs["rs_52w_pos"] == 100.0
    assert rs["ir_63"] is None and rs["vol_63"] in (None, 0.0)   # a constant log-return has no tracking volatility: no ratio is invented
    flat = [200.0 + (i % 2) * 2.0 for i in range(300)]          # closes 200 / 202: range 1% forever
    found, ranges = lf.base_lengths(flat)
    assert found[6.0] == 260 and found[10.0] == 260 and found[15.0] == 260 and ranges[6.0] < 1.1
    trend = [100.0 * (1.003 ** i) for i in range(300)]          # 0.3%/day, range measured off the last close: 6% ~ 21 sessions, 10% ~ 36, 15% ~ 55
    found, _ = lf.base_lengths(trend)
    assert found == {6.0: 21, 10.0: 36, 15.0: 55}
    assert lf.length_points(19) == 0 and lf.length_points(20) == 10.0 and lf.length_points(250) == 35.0 and 20 < lf.length_points(60) < 22
    assert lf.tight_points(3.9) == 30 and lf.tight_points(6.0) == 25 and lf.tight_points(15.0) == 7 and lf.tight_points(15.1) == 0
    assert lf.tier_of({6.0: 40, 10.0: 60, 15.0: 200}) == ("VERY TIGHT", 6.0) and lf.tier_of({6.0: 39, 10.0: 40, 15.0: 50}) == ("TIGHT", 10.0)
    assert lf.tier_of({6.0: 10, 10.0: 25, 15.0: 130}) == ("LONG BASE", 15.0) and lf.tier_of({6.0: 10, 10.0: 25, 15.0: 60}) == ("FORMING", 10.0) and lf.tier_of({6.0: 5, 10.0: 19, 15.0: 60}) == (None, None)
    # a base that is not a contraction of the instrument's own yearly range is no coil (a T-bill fund sits inside 1% all year)
    assert lf.tier_of({6.0: 260, 10.0: 260, 15.0: 260}, {6.0: 0.9, 10.0: 0.9, 15.0: 0.9}, 1.1) == (None, None)
    assert lf.tier_of({6.0: 45, 10.0: 80, 15.0: 200}, {6.0: 5.5, 10.0: 9.0, 15.0: 14.0}, 30.0) == ("VERY TIGHT", 6.0)
    assert lf.tier_of({6.0: 45, 10.0: 80, 15.0: 200}, {6.0: 5.5, 10.0: 9.0, 15.0: 14.0}, 8.0) == (None, None)
    cash = [{"c": 100.0 + 0.002 * i, "h": 100.0 + 0.002 * i + 0.01, "l": 100.0 + 0.002 * i - 0.01, "v": 1e6} for i in range(300)]
    c0 = lf.coil(cash, [b["c"] for b in cash], [100.0] * 300, 0.0)
    assert c0["tier"] is None and c0["len_6"] == 260 and c0["range_252_pct"] is not None and c0["range_252_pct"] < 1.0
    assert lf.percentile_of(5, [1, 2, 3, 4, 5, 6]) == 75.0 and lf.percentile_of(None, [1]) is None
    assert lf.composite({"d": 1.0, "w": 2.0, "m": 3.0, "q": 4.0, "h": 5.0}) == 3.15 and lf.composite({"d": None, "w": None, "m": None, "q": None, "h": None}) is None


def test_split_adjustment_recorded_and_inferred():
    bars = [{"date": DATES[i].isoformat(), "o": 10.0, "h": 10.5, "l": 9.5, "c": 10.0, "v": 1000.0} for i in range(10)]
    adj = lf.apply_splits(bars, [{"date": DATES[5].isoformat(), "factor": 0.5}])
    assert [b["c"] for b in adj] == [5.0] * 5 + [10.0] * 5 and [b["v"] for b in adj] == [2000.0] * 5 + [1000.0] * 5 and bars[0]["c"] == 10.0   # input untouched
    assert lf.apply_splits(bars, []) is bars
    jump = [{"date": DATES[i].isoformat(), "c": (20.0 if i < 40 else 60.0) * (1 + 0.001 * (i % 3)), "v": 3000.0 if i < 40 else 1000.0} for i in range(60)]
    found = lf.infer_splits(jump)
    assert len(found) == 1 and found[0]["date"] == DATES[40].isoformat() and found[0]["factor"] == 3.0 and found[0]["source"] == "inferred"
    crash = [{"date": DATES[i].isoformat(), "c": 20.0 if i < 40 else 8.0, "v": 1000.0 if i < 40 else 9000.0} for i in range(60)]   # a real -60% day: volume explodes, no split
    assert lf.infer_splits(crash) == []
    squeeze = [{"date": DATES[i].isoformat(), "c": 20.0 if i < 40 else 45.0, "v": 1000.0 if i < 40 else 1000.0} for i in range(60)]   # +125% with flat volume: not a split
    assert lf.infer_splits(squeeze) == []
    odd = [{"date": DATES[i].isoformat(), "c": 20.0 if i < 40 else 20.0 * 2.37, "v": 2370.0 if i < 40 else 1000.0} for i in range(60)]   # an unusual ratio is kept as measured
    assert lf.infer_splits(odd)[0]["factor"] == 2.37 and lf.infer_splits(odd)[0]["evidence"] == "volume"
    etha = [{"date": DATES[i].isoformat(), "c": 20.43 if i < 40 else 60.89, "v": 24_072_024.0 if i < 40 else 15_469_252.0} for i in range(60)]   # ETHA 2026-10-06: x2.98, volume 0.64x
    got = lf.infer_splits(etha)
    assert len(got) == 1 and got[0]["factor"] == 3.0 and got[0]["evidence"] == "ratio"
    wrongway = [{"date": DATES[i].isoformat(), "c": 20.0 if i < 40 else 60.0, "v": 1000.0 if i < 40 else 1500.0} for i in range(60)]   # exactly x3 but volume rose: a squeeze, not a split
    assert lf.infer_splits(wrongway) == []
    halved = [{"date": DATES[i].isoformat(), "c": 20.0 if i < 40 else 10.0, "v": 1000.0 if i < 40 else 8000.0} for i in range(60)]   # 1.0.6: exactly -50% on 8x the volume is a crash, not a 2-for-1
    assert lf.infer_splits(halved) == []
    two_for_one = [{"date": DATES[i].isoformat(), "c": 20.0 if i < 40 else 10.0, "v": 1000.0 if i < 40 else 3000.0} for i in range(60)]   # a 2-for-1 with volume x3: still a split (ratio evidence)
    assert lf.infer_splits(two_for_one)[0]["factor"] == 0.5


def test_posture_and_pattern():
    assert lf.posture(0.5, -1.0)["code"] == "BID" and lf.posture(-0.1, -1.0)["code"] == "FLAT" and lf.posture(-0.3, -1.0)["code"] == "HOLD"
    assert lf.posture(0.2, -0.3)["code"] == "BID" and lf.posture(-0.1, -0.3)["code"] == "HOLD" and lf.posture(1.0, 0.2)["code"] == "LEAD"
    assert lf.posture(-0.5, 0.2)["code"] == "LAG" and lf.posture(0.3, 0.2)["code"] == "INLINE" and lf.posture(None, 0.2)["code"] == "NA"
    flat = [{"c": 100.0 + (i % 2) * 0.5, "h": 100.8, "l": 99.8, "v": 1} for i in range(60)]
    assert lf.pattern(flat) == "COIL"
    steady = [{"c": 100.0 * 1.004 ** i, "h": 100.0 * 1.004 ** i * 1.003, "l": 100.0 * 1.004 ** i * 0.997, "v": 1} for i in range(60)]   # a steady climb is an UPTREND, not a double bottom
    assert lf.pattern(steady) == "UPTREND"
    w = [{"c": 100.0, "h": 101.0, "l": 99.0, "v": 1} for _ in range(60)]
    for i in (45, 52):
        w[i] = {"c": 92.0, "h": 93.0, "l": 90.0, "v": 1}                                         # two separate lows a week apart ...
    for i in range(53, 60):
        w[i] = {"c": 96.0 + 0.3 * (i - 53), "h": 97.0 + 0.3 * (i - 53), "l": 95.0, "v": 1}     # ... then a bounce
    assert lf.pattern(w) == "DOUBLE BOTTOM"
    up = [{"c": 100.0 * 1.01 ** i, "h": 100.0 * 1.01 ** i * 1.004, "l": 100.0 * 1.01 ** i * 0.996, "v": 1} for i in range(60)]
    up[-1]["h"] = up[-1]["c"]                                   # closing on the 20-session high
    assert lf.pattern(up) == "BREAKOUT" and lf.pattern(up[:30]) is None
    down = [{"c": 100.0 * 0.99 ** i, "h": 100.0 * 0.99 ** i * 1.004, "l": 100.0 * 0.99 ** i * 0.996, "v": 1} for i in range(60)]
    down[-1]["l"] = down[-1]["c"]                               # closing on the 20-session low
    assert lf.pattern(down) == "BREAKDOWN"


def test_coil_detector_on_a_long_tight_base():
    closes = series("coil", seed=5)
    bars = [dict(zip(["t", "o", "h", "l", "c", "v", "vw", "n"], r)) for r in bank_rows(closes, vol_profile=lambda i: 2_000_000.0 if i < 280 else 900_000.0)]
    bench = series("bench")
    out = lf.coil(bars, closes, bench, rs_trend=1.0)
    assert out["tier"] == "VERY TIGHT" and out["base_len"] >= 120 and out["base_range_pct"] is not None and out["base_range_pct"] <= 2.6
    assert out["len_6"] >= 120 and out["len_15"] >= out["len_10"] >= out["len_6"] and out["wick_range_pct"] > out["base_range_pct"]
    assert out["vol_dry"] is not None and out["vol_dry"] < 0.6 and out["points"]["dry_up"] == 8.0 and out["points"]["tightness"] == 30.0
    assert out["score"] is not None and out["score"] >= 70 and out["bb_width_pctile_252"] is not None and out["pos_in_base"] is not None
    assert out["atr14_pct"] is not None and 0 < out["atr14_pct"] < 3
    # a strong uptrend has no base at all
    trend = [dict(zip(["t", "o", "h", "l", "c", "v", "vw", "n"], r)) for r in bank_rows([100.0 * 1.01 ** i for i in range(300)])]
    none = lf.coil(trend, [b["c"] for b in trend], bench, rs_trend=50.0)
    assert none["tier"] is None and none["base_len"] == 0 and none["score"] is None and none["len_15"] < 20
    short = lf.coil(trend[:15], [b["c"] for b in trend[:15]], bench[:15], None)
    assert short["tier"] is None and short["atr14_pct"] is None


def test_flows_desk_and_canonical():
    s3 = seed()
    desk = json.loads(s3.store[lf.DESK_KEY])
    flows, names = lf.flows_from_desk(desk, NOW.isoformat())
    assert names["QQQ"] == "QQQ fund"
    spy = flows["SPY"]
    assert spy["current"] is True and spy["w1"]["usd"] == 5e9 and spy["w5"]["usd"] == 2e9 and spy["w21"]["pct_aum"] == 15.0 and spy["aligned"] == {"1": True, "5": True, "21": True}
    assert spy["effective"] == "2026-11-02" and spy["aum_usd"] == 1e10 and spy["source"] == "etf-desk" and spy["reference_end"] == "2026-11-02"
    qqq = flows["QQQ"]                                   # own windows matched although the SPY-aligned ones are not
    assert qqq["current"] is True and qqq["w5"]["usd"] == 5.7e9 and qqq["w5"]["end"] == "2026-10-30" and qqq["aligned"] == {"1": False, "5": False, "21": False}
    assert flows["TLT"]["current"] is False and flows["TLT"]["w5"] is None       # not eligible
    assert flows["XLE"]["current"] is False and flows["XLE"]["w1"] is None       # validity passed
    canon = lf.flows_from_canonical(json.loads(s3.store[lf.CANON_KEY]), NOW.isoformat())
    assert canon["IBIT"]["source"] == "provider-flow-research" and canon["IBIT"]["current"] is True and canon["IBIT"]["w21"]["usd"] == 9e8
    assert lf.window_of({"status": "incomplete"}) is None and lf.window_of(None) is None


def test_universe_assembly_and_ranks():
    f13 = lf.f13_rows(json.loads(seed().store[lf.TICKERS_KEY]))
    assert f13["NVDA"]["stance"] == "ACCUMULATING" and f13["NVDA"]["n_holders"] == 40 and f13["AAPL"]["holder_delta"] == -1 and f13["SPY"]["kind"] == "fund"
    u = lf.universe(["SPY", "QQQ", "GLD", "TLT", "XLE"], SP500, f13, {"QQQ": "QQQ fund"})
    assert u["QQQ"]["scopes"] == ["desk", "etf-desk"] and u["QQQ"]["name"] == "Nasdaq 100" and u["QQQ"]["sleeve"] == "ETF"
    assert u["GLD"]["sleeve"] == "ETF" and set(u["GLD"]["scopes"]) == {"desk", "etf-desk"}
    assert u["COIL"]["scopes"] == ["sp500", "13f"] and u["COIL"]["sleeve"] == "STOCK" and u["BRK.B"]["scopes"] == ["sp500"] and "BRK-B" not in u   # FMP's dash becomes the bank's dot
    assert u["NVDA"]["scopes"] == ["desk", "sp500", "13f"] and "SPY" in u and u["SPY"]["scopes"] == ["etf-desk"]
    n_desk = len({t for items in lf.SLEEVES.values() for t, _ in items})
    assert u["AAPL"]["name"] == "Apple" and len(u) == n_desk + 8 and n_desk >= 160   # every distinct desk name (sleeves overlap) + SPY, COIL, LAGG, BRK.B, NEWB, SPLT, RCRD, DEAL
    assert u["HYG"]["sleeve"] == "ETF" and u["JNK"]["sleeve"] == "HIGH YIELD" and u["SIVR"]["sleeve"] == "METAL" and u["USO"]["sleeve"] == "COMMODITY" and u["VXX"]["sleeve"] == "VOLATILITY"
    rows = [{"ticker": t, "vs": ({"d": d, "w": d * 2, "m": d * 3, "q": d * 4, "h": d * 5} if d is not None else None)} for t, d in (("A", 1.0), ("B", 2.0), ("C", 3.0), ("D", -1.0), ("E", None))]
    stats = lf.rank_rows(rows)
    by = {r["ticker"]: r for r in rows}
    assert by["C"]["strength"] == 87.5 and by["D"]["strength"] == 12.5 and by["E"]["strength"] is None and by["E"]["z"] is None and stats["d"]["mean"] == 1.25


def test_build_rows_boards_flows_and_13f():
    s3, out = built()
    assert out["contract"] == "strength.v1" and out["version"] == "1.0.8" and out["as_of"] == DATES[-1].isoformat() and out["benchmark"]["ticker"] == "SPY"
    assert out["benchmark"]["sessions"] == 300 and out["benchmark"]["h"]["q"] is not None and out["universe"]["n"] == len({t for items in lf.SLEEVES.values() for t, _ in items}) + 8
    by = {r["ticker"]: r for r in out["rows"]}
    # unbanked desk names carry no row; the bank manifest's last session is the as-of
    assert "TLT" in out["universe"]["unbanked"] and "XLE" in out["universe"]["unbanked"] and "SIVR" in out["universe"]["unbanked"] and "TLT" not in by
    assert set(by) == {"NVDA", "AAPL", "LAGG", "COIL", "QQQ", "GLD", "NEWB", "BRK.B", "SPLT", "RCRD", "DEAL"} and out["universe"]["n_rows"] == 11
    # strength ranks: the leader above the benchmark-like names above the laggard; the 30-session listing is unranked
    assert by["NVDA"]["strength"] > by["AAPL"]["strength"] > by["LAGG"]["strength"] and by["NEWB"]["strength"] is None and by["NEWB"]["vs"] is None and by["NEWB"]["sessions"] == 30
    assert by["NVDA"]["vs"]["q"] > 0 and by["LAGG"]["vs"]["q"] < 0 and by["NVDA"]["composite_pp"] > 0 and by["NVDA"]["rs_trend_63"] > 0 and by["NVDA"]["rs_consistency_63"] > 50
    assert by["NVDA"]["ir_63"] is not None and by["NVDA"]["vol_63"] is not None and by["NVDA"]["pos_52w"] is not None and by["NVDA"]["above_200"] is True
    assert by["NVDA"]["posture"]["code"] in ("LEAD", "LAG", "INLINE", "BID", "HOLD", "FLAT") and by["NVDA"]["pattern"] is not None
    assert by["NVDA"]["name"] == "NVIDIA" and by["NVDA"]["sector"] == "Technology" and by["NVDA"]["industry"] == "Semiconductors" and by["NVDA"]["kind"] == "stock"
    assert by["NVDA"]["scopes"] == ["desk", "sp500", "13f"] and by["COIL"]["scopes"] == ["sp500", "13f"] and by["COIL"]["name"] == "Coil Industries"
    assert out["rows"][0]["strength"] == max(r["strength"] for r in out["rows"] if r["strength"] is not None)
    # the coil board finds the long tight base first
    assert out["boards"]["coil"][0]["ticker"] == "COIL" and out["boards"]["coil"][0]["coil"]["tier"] == "VERY TIGHT" and by["COIL"]["coil"]["score"] >= 70
    assert by["COIL"]["coil"]["base_len"] >= 120 and by["COIL"]["coil"]["rs_hold_base"] is not None and by["COIL"]["f13"]["stance"] == "ACCUMULATING"
    assert by["NVDA"]["coil"]["tier"] is None
    # fund flows: the fund's own windows (QQQ matched although the SPY grid was not), stocks carry the 13F read instead
    assert by["QQQ"]["flow"]["current"] is True and by["QQQ"]["flow"]["w5"]["usd"] == 5.7e9 and by["QQQ"]["flow"]["aligned"]["5"] is False and by["QQQ"]["kind"] == "fund"
    assert by["GLD"]["flow"]["w21"]["usd"] == -1.9e9 and by["GLD"]["flow"]["w21"]["pct_aum"] == -19.0 and by["GLD"]["f13"] is None
    assert by["NVDA"]["flow"] is None and by["NVDA"]["f13"]["n_holders"] == 40 and by["NVDA"]["f13"]["net_usd"] == 7e9 and by["AAPL"]["f13"]["stance"] == "DISTRIBUTING"
    assert by["BRK.B"]["flow"] is None and by["BRK.B"]["f13"] is None and by["BRK.B"]["name"] == "Berkshire Hathaway"
    fs = out["flows"]
    assert fs["n_configured"] == 5 and fs["n_current_now"] == 5 and fs["n_in_universe"] == 2 and fs["n_universe_current"] == 2 and fs["n_universe_w5"] == 2 and fs["canonical_funds"] == 2
    assert out["f13"] == {"period": "2026-06-30", "generated_at": "2026-11-04T16:37:00+00:00", "n_tickers": 4, "last_modified": "2026-11-04T03:13:00+00:00", "n_in_universe": 3}
    # splits: the unrecorded 1-for-3 reverse split is inferred from the volume-confirmed jump, the recorded 2-for-1 comes from corporate-actions; both series read continuous
    sp, rc = by["SPLT"], by["RCRD"]
    assert sp["splits"] == [{"date": DATES[280].isoformat(), "factor": 3.0, "ratio": sp["splits"][0]["ratio"], "source": "inferred", "evidence": "volume"}] and 2.9 <= sp["splits"][0]["ratio"] <= 3.1
    assert rc["splits"][0]["source"] == "corporate-actions" and rc["splits"][0]["factor"] == 0.5 and len(rc["splits"]) == 1
    assert abs(sp["h"]["y"] - by["BRK.B"]["h"]["y"]) < 40 and abs(sp["vs"]["q"]) < 30 and abs(rc["vs"]["q"]) < 30 and abs(rc["h"]["y"]) < 60   # no 200% artefacts
    assert out["splits"]["n_rows_adjusted"] == 2 and out["splits"]["recorded"] == 1 and out["splits"]["inferred"] == 1 and out["splits"]["index_covered"] == 2
    assert by["NVDA"].get("splits") is None
    # boards and breadth
    b = out["boards"]
    assert b["leaders"][0]["ticker"] in ("NVDA", "QQQ") and b["laggards"][0]["ticker"] == "LAGG" and b["n_ranked"] == 10
    assert b["breadth"]["universe"]["n"] == 10 and b["breadth"]["universe"]["coiled"] == 5 and b["breadth"]["universe"]["pinned"] == 1 and b["breadth"]["desk"]["n"] == 4 and "pct_q" in b["breadth"]["desk"]
    assert by["DEAL"]["coil"]["flag"] == "PINNED" and by["DEAL"]["coil"]["tier"] == "VERY TIGHT" and "DEAL" not in [x["ticker"] for x in b["coil"]] and "deal-like" in by["DEAL"]["coil"]["flag_note"]
    assert [x["ticker"] for x in b["coil"]] == ["COIL", "RCRD", "GLD", "BRK.B", "LAGG"] and b["coil"][-1]["coil"]["tier"] == "FORMING"
    assert by["AAPL"]["coil"]["tier"] is None and by["AAPL"]["coil"]["len_6"] >= 40 and by["AAPL"]["coil"]["range_252_pct"] < 10   # inside 6% for months, but its whole year is 7%: no contraction, no coil
    assert by["COIL"]["coil"]["contraction"] < 0.1 and by["GLD"]["coil"]["contraction"] <= 0.6
    assert all("spark" in by[t] for t in ("NVDA", "AAPL", "QQQ", "GLD", "LAGG")) and "spark" in by["COIL"] and len(by["NVDA"]["spark"]) == 60
    # sources and observability
    assert [s["engine"] for s in out["sources"]] == ["justhodl-polygon-eod", "justhodl-etf-global-desk", "justhodl-etf-fund-flows", "justhodl-13f-book", "justhodl-leaders"]
    assert all(s["tier"] == "fresh" for s in out["sources"] if s["engine"] != "justhodl-leaders") and out["n_errors"] == 0 and out["sleeves"]["METAL"][0] == {"ticker": "GLD", "name": "Gold"}
    assert out["universe"]["scopes"]["etf-desk"] == 5 and out["universe"]["scopes"]["sp500"] == 9 and out["universe"]["scopes"]["13f"] == 3 and out["universe"]["scopes"]["desk"] >= 160
    # 1.0.7: asset classes on every row, in-class percentiles, the class board, the thesis block
    assert by["QQQ"]["asset_class"] == "equity_etf" and by["QQQ"]["class_sub"] == "broad" and by["GLD"]["asset_class"] == "gold" and by["NVDA"]["asset_class"] == "stock"
    assert by["BRK.B"]["asset_class"] == "stock" and by["SPLT"]["asset_class"] == "stock" and by["NEWB"]["asset_class"] == "stock"
    assert by["NVDA"]["strength_in_class"] > by["LAGG"]["strength_in_class"] and by["GLD"]["strength_in_class"] is None   # one gold row: no in-class percentile
    assert out["classes"]["ranked"]["stock"] == 8 and out["classes"]["ranked"]["gold"] == 1 and out["classes"]["counts"]["leveraged"] == 0
    bc = b["by_class"]
    assert bc["stock"]["n"] == 8 and bc["stock"]["best"][0]["ticker"] == "NVDA" and bc["stock"]["worst"][0]["ticker"] == "LAGG" and bc["gold"]["n"] == 1 and b["class_order"][0] == "stock"
    assert out["thesis"]["window"]["sessions"] > 100 and out["thesis"]["windows"]["252"]["sessions"] == out["thesis"]["window"]["sessions"] and "thesis" in out["definitions"]
    assert set(out["thesis"]["windows"]) == {"21", "63", "252"} and out["thesis"]["windows"]["21"]["sessions"] == 21 and out["thesis"]["windows"]["63"]["sessions"] == 63
    # 1.0.8: capture / asymmetry on every ranked row, the asymmetry rank, the industry read (universe + leaders table), the boards
    cap = by["NVDA"]["capture"]["63"]
    assert cap["n"] == 63 and cap["n_down"] + cap["n_up"] <= 63 and cap["down_excess_pp"] is not None and cap["up_excess_pp"] is not None and cap["asym_pp"] == by["NVDA"]["asym_pp"]
    assert by["NVDA"]["asym_pp"] > by["LAGG"]["asym_pp"] and by["NVDA"]["asym_rank"] > by["LAGG"]["asym_rank"] and by["NEWB"]["capture"] is None and by["NEWB"]["asym_rank"] is None
    ir = by["NVDA"]["industry_read"]
    assert ir["industry"] == "Semiconductors" and ir["leaders_strength"] == 88.0 and ir["leaders_vs_m"] == 4.2 and ir["confirmed"] is True and ir["n_universe"] == 1 and ir["rank"] is None
    assert by["LAGG"]["industry_read"]["leaders_strength"] == 12.0 and by["LAGG"]["industry_read"]["confirmed"] is False and by["QQQ"]["industry_read"] is None
    assert b["industries"]["leaders_industries"] == 2 and b["industries"]["n"] == 0   # one stock per industry here: no in-universe industry row (IND_MIN 3)
    assert b["asymmetry"]["leaders"][0]["asym_rank"] >= b["asymmetry"]["leaders"][-1]["asym_rank"] and "asym_pp" in b["leaders"][0] and "industry_confirmed" in b["leaders"][0]
    assert isinstance(b["asymmetry"]["confirmed_leaders"], list)


def test_build_without_the_optional_inputs():
    s3 = seed()
    for k in (lf.DESK_KEY, lf.CANON_KEY, lf.TICKERS_KEY):
        del s3.store[k]
    out = lf.build(s3, NOW, log=lambda *a: None)
    by = {r["ticker"]: r for r in out["rows"]}
    assert by["QQQ"]["flow"] is None and by["NVDA"]["f13"] is None and out["n_errors"] == 3 and {e["input"] for e in out["errors"]} == {"etf-desk", "canonical-flows", "13f-tickers"}
    assert by["QQQ"]["kind"] == "fund" and by["NVDA"]["strength"] is not None and out["flows"]["n_configured"] == 0 and out["f13"]["n_in_universe"] == 0
    assert out["universe"]["scopes"]["13f"] == 0 and len(out["boards"]["leaders"]) == 10


def test_bank_read_and_time_budget():
    s3 = seed()
    rows = lf.read_bars(s3, "SPY", sessions=10)
    assert len(rows) == 10 and rows[-1]["date"] == DATES[-1].isoformat() and set(rows[0]) >= {"date", "o", "h", "l", "c", "v"}
    assert lf.read_bars(s3, "NOPE") == [] and lf.bank_key("BRK.B").endswith("/BRK.B.json.gz") and lf.bank_key("x y") .endswith("/X_Y.json.gz")
    out = lf.build(s3, NOW, log=lambda *a: None, remaining_s=0.0)      # budget exhausted: the benchmark is read, the universe is skipped, nothing is guessed
    assert out["universe"]["n_rows"] == 0 and len(out["universe"]["skipped_for_time"]) >= 50 and out["boards"]["n_ranked"] == 0


def test_handler():
    s3 = seed()
    lf._s3 = lambda: s3
    res = lf.lambda_handler({}, SimpleNamespace(get_remaining_time_in_millis=lambda: 299000))
    assert res["statusCode"] == 200 and res["key"] == "data/strength.json" and res["rows"] == 11 and res["ranked"] == 10 and res["errors"] == 0
    put = [x for x in s3.puts if x["Key"] == "data/strength.json"][-1]
    assert put["CacheControl"] == "max-age=300" and BEGUN and BEGUN[-1] == {}
    doc = json.loads(put["Body"])
    assert doc["trigger"] == {"kind": "scheduled"} and doc["contract"] == "strength.v1" and len(doc["rows"]) == 11 and doc["boards"]["coil"][0]["ticker"] == "COIL"
    assert "api.polygon.io" not in json.dumps(doc)
    # 1.0.7: every run keeps the artifact as of its session
    hist = [x for x in s3.puts if x["Key"].startswith(lf.HISTORY_PREFIX)]
    assert len(hist) == 1 and hist[0]["Key"] == lf.HISTORY_PREFIX + DATES[-1].isoformat() + ".json.gz" and hist[0]["ContentEncoding"] == "gzip"
    assert json.loads(gzip.decompress(hist[0]["Body"]))["as_of"] == doc["as_of"]



def test_asset_classes_and_leveraged_exclusion():
    """1.0.7: the explicit map, the name fallback, stocks; a leveraged fund is labelled and kept out of the ranks and boards."""
    cases = {("HYG", "fund", ""): ("high_yield", "corporate"), ("JNK", "fund", ""): ("high_yield", "corporate"), ("TLT", "fund", ""): ("bond", "treasury"),
             ("BIL", "fund", ""): ("cash", "t_bill"), ("GLD", "fund", ""): ("gold", "bullion"), ("SLV", "fund", ""): ("silver", "bullion"), ("PPLT", "fund", ""): ("metals", "pgm"),
             ("GDX", "fund", ""): ("metals", "gold_miners"), ("USO", "fund", ""): ("commodity", "energy"), ("UUP", "fund", ""): ("currency", None), ("IBIT", "fund", ""): ("crypto", None),
             ("VXX", "fund", ""): ("volatility", None), ("TQQQ", "fund", ""): ("leveraged", None), ("XLK", "fund", ""): ("equity_etf", "sector"), ("EWJ", "fund", ""): ("country", None),
             ("ZZZ1", "fund", "iShares 20+ Year Treasury Bond ETF"): ("bond", None), ("ZZZ2", "fund", "SPDR Bloomberg High Yield Bond ETF"): ("high_yield", None),
             ("ZZZ3", "fund", "VanEck Gold Miners ETF"): ("metals", "miners"), ("ZZZ4", "fund", "abrdn Physical Silver Shares"): ("silver", "bullion"),
             ("ZZZ5", "fund", "ProShares UltraPro Short QQQ"): ("leveraged", None), ("ZZZ6", "fund", "iShares MSCI Brazil ETF"): ("country", None),
             ("ZZZ7", "fund", "Invesco DB Agriculture Fund"): ("commodity", None), ("ZZZ8", "fund", "Vanguard Growth ETF"): ("equity_etf", None),
             ("AAPL", "stock", "Apple"): ("stock", None), ("ZZZ9", None, None): (None, None)}
    for (t, k, n), want in cases.items():
        assert lf.asset_class_of(t, k, n) == want, (t, lf.asset_class_of(t, k, n), want)
    assert set(lf.ASSET_CLASS[t][0] for t in ("SQQQ", "SPXU", "SOXS", "UVXY", "SVXY", "TBT")) == {"leveraged"}
    # a leveraged fund in the ranks: strength None, excluded, absent from the boards, present in boards.excluded with its day move
    rows = [{"ticker": t, "asset_class": cls, "vs": {"d": d, "w": d * 2, "m": d * 3, "q": d * 4, "h": d * 5}} for t, cls, d in
            (("A", "stock", 1.0), ("B", "stock", 2.0), ("C", "stock", 3.0), ("D", "stock", -1.0), ("E", "stock", 0.5), ("SQQQ", "leveraged", 9.0), ("G", "gold", 0.0))]
    lf.rank_rows(rows)
    by = {r["ticker"]: r for r in rows}
    assert by["SQQQ"]["strength"] is None and by["SQQQ"]["excluded"] == "leveraged" and by["C"]["strength"] == max(r["strength"] for r in rows if r["strength"] is not None)
    assert by["C"]["strength_in_class"] == 90.0 and by["D"]["strength_in_class"] == 10.0 and by["G"]["strength_in_class"] is None


def test_thesis_walk_forward_on_a_known_universe():
    """A bench with real dump days AND rally days; 'strong' names fall 40% as much on dump days, rise 1.6x on rally days and
    drift up; 'weak' names the opposite; bonds barely move; two industries, one strong and one weak: both predictors must
    put the strong names in the top tenth, the top tenth must hold up on every dump day and beat SPY on every rally day,
    the industry-confirmed leaders must be the strong industry's names, and the windows must nest (21 < 63 < 252)."""
    rnd = random.Random(21)
    n = 300
    bench_r = []
    for i in range(n):
        if i in (200, 231, 262, 280):
            bench_r.append(-0.025)
        elif i in (215, 250, 292):
            bench_r.append(-0.012)
        elif i in (205, 240, 270, 296):
            bench_r.append(0.015)
        else:
            bench_r.append(0.0005 + rnd.gauss(0, 0.004))

    def closes(rets):
        px, out = 100.0, []
        for r in rets:
            px *= (1 + r)
            out.append(px)
        return out
    bench = [{"date": d.isoformat(), "c": c} for d, c in zip(DATES[-n:], closes(bench_r))]
    rows, bars_by = [], {}

    def add(t, cls, ind, beta_up, beta_dump, beta_rally, drift):
        rets = []
        for br in bench_r:
            if br <= -0.01:
                rets.append(br * beta_dump + rnd.gauss(0, 0.001))
            elif br >= 0.01:
                rets.append(br * beta_rally + rnd.gauss(0, 0.001))
            else:
                rets.append(br * beta_up + drift + rnd.gauss(0, 0.002))
        bars_by[t] = [{"date": d.isoformat(), "c": c} for d, c in zip(DATES[-n:], closes(rets))]
        rows.append({"ticker": t, "asset_class": cls, "industry": ind, "strength": 50.0, "excluded": None})
    for k in range(10):
        add("STR%02d" % k, "stock", "Semiconductors", 1.0, 0.4, 1.6, 0.003)      # strong: up-drift, dumps less, rallies more
    for k in range(10):
        add("WEAK%02d" % k, "stock", "Apparel Retail", 1.0, 2.0, 0.5, -0.002)    # weak: down-drift, dumps more, rallies less
    for k in range(20):
        add("MID%02d" % k, "stock", "Banks - Regional" if k < 7 else "Software - Application" if k < 14 else "Insurance - Property & Casualty", 1.0, 1.0, 1.0, 0.0)
    for k in range(6):
        add("BND%02d" % k, "bond", None, 0.1, -0.2, 0.1, 0.0002)                 # bonds: barely move, rise a little on dump days
    old_min = lf.THESIS_MIN_ROWS
    lf.THESIS_MIN_ROWS = 10
    try:
        th = lf.thesis_block(rows, bench, bars_by, log=lambda *a: None)
    finally:
        lf.THESIS_MIN_ROWS = old_min
    assert th["window"]["sessions"] > 100 and set(th["windows"]) == {"21", "63", "252"} and th["predictors"]["asymmetry"].startswith("trailing 63-session")
    y = th["windows"]["252"]
    first = len(bench) - th["window"]["sessions"]
    spy_pct = [(bench[i]["c"] / bench[i - 1]["c"] - 1) * 100 for i in range(first, len(bench))]
    n_dump, n_deep, n_rally = sum(1 for x in spy_pct if x <= -1.0), sum(1 for x in spy_pct if x <= -2.0), sum(1 for x in spy_pct if x >= 1.0)
    assert n_dump >= 7 and n_deep == 4 and n_rally >= 4
    assert y["n_dump_days"] == n_dump and y["n_deep_dump_days"] == n_deep and y["n_rally_days"] == n_rally, (y["n_dump_days"], y["n_deep_dump_days"], y["n_rally_days"], n_dump, n_deep, n_rally)
    assert th["windows"]["63"]["n_dump_days"] == sum(1 for x in spy_pct[-63:] if x <= -1.0) and th["windows"]["21"]["n_dump_days"] == sum(1 for x in spy_pct[-21:] if x <= -1.0)
    assert th["windows"]["21"]["n_rally_days"] == sum(1 for x in spy_pct[-21:] if x >= 1.0) and th["windows"]["21"]["sessions"] == 21 and th["windows"]["63"]["sessions"] == 63
    for pred in ("strength", "asymmetry"):
        d = y["dump"][pred]["all"]
        assert d["n_days"] == n_dump and d["top_decile"]["days_beat_spy"] == n_dump and d["top_decile"]["held_up_pct"] >= 95 and d["top_decile"]["mean_excess_pp"] > 1.0, (pred, d)
        assert d["bottom_decile"]["mean_excess_pp"] < -1.0 and d["bottom_decile"]["held_up_pct"] <= 5, (pred, d["bottom_decile"])
        r_ = y["rally"][pred]["all"]
        assert r_["n_days"] == n_rally and r_["top_decile"]["days_beat_spy"] == n_rally and r_["top_decile"]["hit_pct"] >= 95 and r_["top_decile"]["mean_excess_pp"] > 0.5, (pred, r_)
        assert y["both_sides"][pred]["both_sides"] is True
        st = y["dump"][pred]["stocks"]
        assert st["n_days"] == n_dump and st["top_decile"]["days_beat_spy"] == n_dump
    ic = y["industry"]["dump"]
    assert ic["industry_confirmed"]["n_days"] == n_dump and ic["industry_confirmed"]["held_up_pct"] >= 95 and ic["industry_confirmed"]["avg_n"] >= 3, ic
    day = th["dump_days"][0]
    assert day["dump"] is True and day["spy_ret_pct"] <= -1.0 and all(x["ticker"].startswith("STR") for x in day["strength"]["all"]["top_names"])
    assert all(x["ticker"].startswith("STR") for x in day["asymmetry"]["all"]["top_names"]) and all(x["ticker"].startswith("WEAK") for x in day["strength"]["all"]["bottom_names"])
    assert all(x["industry"] == "Semiconductors" for x in day["confirmed"]["confirmed_names"]) and day["confirmed"]["confirmed"]["n"] >= 3
    rally = th["rally_days"][0]
    assert rally["rally"] is True and rally["spy_ret_pct"] >= 1.0 and all(x["ticker"].startswith("STR") for x in rally["strength"]["all"]["top_names"])
    bc = y["by_class_on_dumps"]
    assert bc["bond"]["held_up_pct"] >= 95 and bc["bond"]["mean_excess_pp"] > 1.0 and bc["stock"]["n_days"] == 7
    assert th["last_session"]["date"] == bench[-1]["date"] and th["last_session"]["ranked_as_of"] == bench[-2]["date"] and th["days"][-1]["date"] == bench[-1]["date"]
    assert y["all_days"]["strength"]["all"]["by_decile_mean_excess_pp"][9] > y["all_days"]["strength"]["all"]["by_decile_mean_excess_pp"][0]
    assert "industry-confirmed leaders on dump days" in th["verdict"] and "asymmetry rank" in th["verdict"], th["verdict"]
    assert len(json.dumps(th)) < 900000, len(json.dumps(th))


if __name__ == "__main__":
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            fn()
            print("ok", name)
    print("justhodl-strength tests: all green")
