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

# 1.1.0: the factor book (identity of every listed security) faked for the whole-market pass; tests swap BOOK's rows
def book_row(sym, name, kind="stock", sector="Technology", industry="Software - Application", mcap=5e9, dv=5e7, ipo="2010-01-04", **flags):
    r = {"symbol": sym, "name": name, "sector": sector, "industry": industry, "exchange": "NYSE", "is_etf": kind == "etf", "is_otc": False, "is_fund": False, "is_preferred": False,
         "is_warrant_unit": False, "is_shell": False, "is_adr": False, "is_debt_or_structured": False, "is_common": kind == "stock", "security_type": "ETF" if kind == "etf" else "CS",
         "mcap": mcap, "dollar_vol_20d": dv, "ipo_date": ipo}
    r.update(flags)
    return r


BOOK = []
sys.modules["bank_enrich"] = SimpleNamespace(rows=lambda fields=None, **kw: [dict(r) for r in BOOK])

import lambda_function as lf  # noqa: E402
import wide  # noqa: E402
lf.THESIS_MIN_ROWS = 5   # the synthetic bank ranks ten names; production needs 20 per session
wide.MIN_SCANNED = 3     # production refuses a table of fewer than 2,500 names

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
    # 1.0.11: Polygon's split reference (monthly windows) - SPLT's 1-for-3 at session 280 is recorded AND printed by the bars (applied, source polygon-splits);
    # AAPL's "2-for-1" at session 100 is recorded but the bars never print it (already adjusted when the bank was pulled -> left alone)
    s3.put(lf.BANK_SPLITS_ROOT + DATES[280].isoformat()[:7] + ".json.gz", {"results": [{"ticker": "SPLT", "execution_date": DATES[280].isoformat(), "split_from": 3, "split_to": 1}]})
    s3.put(lf.BANK_SPLITS_ROOT + DATES[100].isoformat()[:7] + ".json.gz", [{"ticker": "AAPL", "execution_date": DATES[100].isoformat(), "split_from": 1, "split_to": 2}])
    s3.put("data/warm/polygon-bank/manifest.json", {"engine": "justhodl-polygon-bank", "generated_at": "2026-11-03T22:45:00+00:00", "splits": {"windows": 2}})
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
    # 1.0.13 - and the session must OPEN at the new price (ops 6855: JDZG 2026-10-28 opened 6.30, closed 1.50 on 4x the shares)
    opened = [dict(b, o=b["c"]) for b in jump]                                                           # in force at the open: a reverse split
    assert len(lf.infer_splits(opened)) == 1
    inside = [dict(b, o=(20.0 if i <= 40 else b["c"])) for i, b in enumerate(jump)]                      # that session opens at the old price: a spike that built inside it
    turned = []
    assert lf.infer_splits(inside, refused=turned) == [] and lf.infer_splits(inside) == []
    assert len(turned) == 1 and turned[0]["date"] == DATES[40].isoformat() and turned[0]["open_gap"] < 1.01 and turned[0]["ratio"] > 2.9
    assert lf.at_open(100, 49, 0.25) and not lf.at_open(100, 51, 0.25) and lf.at_open(100, 210, 4.0) and not lf.at_open(100, 190, 4.0) and lf.at_open(100, None, 0.25) and not lf.at_open(100, 0, 0.25)
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
    # 1.0.9: a FRESH split on the last bar (no 5-session window after it) - the iShares splits of 2026-11-05
    mtum = [{"date": DATES[i].isoformat(), "c": 240.0 + (i % 5) if i < 59 else 55.06, "v": 1_000_000.0 if i < 59 else 4_600_000.0} for i in range(60)]   # MTUM 4:1 on a -8% day (x0.229), shares x4.6
    got = lf.infer_splits(mtum, fund=True)
    assert len(got) == 1 and got[0]["date"] == DATES[59].isoformat() and got[0]["factor"] == 0.25 and got[0]["evidence"].startswith("fresh"), got
    assert lf.infer_splits(mtum, fund=False) == []                                                   # a stock at x0.229 is a crash: a stock's fresh snap reaches only 6% around 1/4
    soxx = [{"date": DATES[i].isoformat(), "c": 300.0 if i < 59 else 87.9, "v": 2_000_000.0 if i < 59 else 7_000_000.0} for i in range(60)]   # SOXX 3:1 with semis -11% (x0.293), shares x3.5
    got = lf.infer_splits(soxx, fund=True)
    assert len(got) == 1 and got[0]["factor"] == round(1 / 3, 6) and got[0]["evidence"].startswith("fresh")
    fresh_crash = [{"date": DATES[i].isoformat(), "c": 20.0 if i < 59 else 8.0, "v": 1000.0 if i < 59 else 9000.0} for i in range(60)]        # a -60% last day on 9x the shares: a crash, fund or not (x0.40 is near no common ratio)
    assert lf.infer_splits(fresh_crash, fund=True) == [] and lf.infer_splits(fresh_crash) == []
    qual = [{"date": DATES[i].isoformat(), "c": 222.0 + (i % 7) if i < 59 else 54.79, "v": 834_000.0 if i < 59 else 18_842_909.0} for i in range(60)]   # QUAL 4:1 2026-11-05: x0.2413 on 22.6x the shares (1.0.10: a fund is not volume-bounded)
    got = lf.infer_splits(qual, fund=True)
    assert len(got) == 1 and got[0]["factor"] == 0.25, got
    assert lf.infer_splits(qual, fund=False) == []                                                   # a stock on 22x the shares stays a crash (bounded x2.5 past the ratio)
    fresh_flat = [{"date": DATES[i].isoformat(), "c": 20.0 if i < 59 else 5.0, "v": 1000.0 if i < 59 else 1000.0} for i in range(60)]          # x0.25 on unchanged shares: not a split (shares must multiply)
    assert lf.infer_splits(fresh_flat, fund=True) == []
    fresh_stock = [{"date": DATES[i].isoformat(), "c": 20.0 if i < 59 else 10.0, "v": 1000.0 if i < 59 else 2100.0} for i in range(60)]       # a stock's clean 2-for-1 on the last bar, shares x2.1: inferred
    assert lf.infer_splits(fresh_stock)[0]["factor"] == 0.5 and lf.infer_splits(fresh_stock)[0]["evidence"] == "fresh-volume"
    fresh_rev = [{"date": DATES[i].isoformat(), "c": 2.0 if i < 59 else 20.0, "v": 10_000.0 if i < 59 else 1_100.0} for i in range(60)]        # a 1-for-10 reverse split on the last bar, shares /9
    assert lf.infer_splits(fresh_rev)[0]["factor"] == 10.0


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
    assert out["contract"] == "strength.v1" and out["version"] == "1.1.0" and out["as_of"] == DATES[-1].isoformat() and out["benchmark"]["ticker"] == "SPY"
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
    assert sp["splits"][0]["date"] == DATES[280].isoformat() and sp["splits"][0]["factor"] == 3.0 and sp["splits"][0]["source"] == "polygon-splits" and 2.9 <= sp["splits"][0]["ratio"] <= 3.1 and len(sp["splits"]) == 1   # 1.0.11: recorded by Polygon's reference (the inferred rule would have found the same)
    assert rc["splits"][0]["source"] == "corporate-actions" and rc["splits"][0]["factor"] == 0.5 and len(rc["splits"]) == 1
    assert abs(sp["h"]["y"] - by["BRK.B"]["h"]["y"]) < 40 and abs(sp["vs"]["q"]) < 30 and abs(rc["vs"]["q"]) < 30 and abs(rc["h"]["y"]) < 60   # no 200% artefacts
    assert out["splits"]["n_rows_adjusted"] == 2 and out["splits"]["recorded"] == 1 and out["splits"]["polygon"] == 1 and out["splits"]["inferred"] == 0 and out["splits"]["index_covered"] == 2
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
    assert [s["engine"] for s in out["sources"]] == ["justhodl-polygon-eod", "justhodl-etf-global-desk", "justhodl-etf-fund-flows", "justhodl-13f-book", "justhodl-leaders", "justhodl-polygon-bank"]
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
    # 1.0.11: the Polygon reference is the first recorded source - SPLT's reverse split is polygon-splits now (the inferred rule stays for what the reference lacks)
    sp_ev = by["SPLT"]["splits"]
    assert sp_ev and sp_ev[0]["source"] == "polygon-splits" and sp_ev[0]["factor"] == 3.0 and sp_ev[0]["date"] == DATES[280].isoformat() and len(sp_ev) == 1, sp_ev
    assert not by["AAPL"].get("splits"), by["AAPL"].get("splits")
    assert out["splits"]["polygon"] == 1 and out["splits"]["polygon_windows_read"] >= 2 and out["splits"]["polygon_reference_rows"] == 2
    assert any(s_["engine"] == "justhodl-polygon-bank" for s_ in out["sources"])
    # 1.0.8: capture / asymmetry on every ranked row, the asymmetry rank, the industry read (universe + leaders table), the boards
    cap = by["NVDA"]["capture"]["63"]
    assert cap["n"] == 63 and cap["n_down"] + cap["n_up"] <= 63 and cap["down_excess_pp"] is not None and cap["up_excess_pp"] is not None and cap["asym_pp"] == by["NVDA"]["asym_pp"]
    assert cap["both_pp"] == min(cap["down_excess_pp"], cap["up_excess_pp"]) == by["NVDA"]["both_pp"] and "126" in by["NVDA"]["capture"] and "bigday_pp" in by["NVDA"]["capture"]["126"]
    assert by["NVDA"]["asym_pp"] > by["LAGG"]["asym_pp"] and by["NVDA"]["both_pp"] > by["LAGG"]["both_pp"] and by["NVDA"]["asym_rank"] > by["LAGG"]["asym_rank"] and by["NEWB"]["capture"] is None and by["NEWB"]["asym_rank"] is None
    assert "both_pp" in b["leaders"][0] and "bigday_pp" in b["leaders"][0]
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


def test_a_thin_day_is_not_a_session():
    """1.0.12 - Saturday 2026-11-07: the bank folded a grouped file of stragglers and its manifest named the Saturday the
    last session; the desk ranked 18 of 852 names on a bar that is not a close. The session is read from the bars."""
    sat = DATES[-1] + timedelta(days=1)
    while sat.weekday() != 5:
        sat += timedelta(days=1)
    sat_ms = int(datetime(sat.year, sat.month, sat.day, 5, tzinfo=timezone.utc).timestamp() * 1000)
    # pure: 2 of 10 series carry the straggler day, the benchmark does not
    fri, s_iso = DATES[-1].isoformat(), sat.isoformat()
    mk = lambda last: [{"date": DATES[-2].isoformat(), "c": 1.0}, {"date": fri, "c": 1.0}] + ([{"date": s_iso, "c": 9.0}] if last == s_iso else [])   # noqa: E731
    bars_by = {"T%d" % i: mk(s_iso if i < 2 else fri) for i in range(10)}
    bars_by["UNBANKED"], bars_by["SKIPPED"] = [], None
    session, thin = lf.effective_session(s_iso, mk(fri), bars_by)
    assert session == fri and thin == [{"day": s_iso, "tickers": 2, "of": 10, "benchmark": False}], (session, thin)
    # a broad day the benchmark does not carry is not a session either; an ordinary day sets nothing aside
    session, thin = lf.effective_session(s_iso, mk(fri), {"T%d" % i: mk(s_iso) for i in range(10)})
    assert session == fri and thin[0]["tickers"] == 10 and thin[0]["benchmark"] is False
    assert lf.effective_session(fri, mk(fri), bars_by) == (fri, []) and lf.effective_session(s_iso, mk(s_iso), {"A": mk(s_iso), "B": mk(s_iso), "C": mk(fri)}) == (s_iso, [])
    # never ahead of the manifest (a fold in progress), and with no banked series the manifest's word stands
    assert lf.effective_session(fri, mk(s_iso), {"A": mk(s_iso), "B": mk(s_iso)}) == (fri, [])
    assert lf.effective_session(s_iso, mk(fri), {"A": None, "B": []}) == (s_iso, [])
    assert lf.through(mk(s_iso), fri)[-1]["date"] == fri and len(lf.through(mk(s_iso), fri)) == 2 and lf.through(mk(fri), fri) == mk(fri) and lf.through([], fri) == []
    # the whole build: NVDA and LAGG print a Saturday straggler bar (a wild one), the manifest says Saturday
    s3, base = built()
    s3 = seed()
    s3.put(pb.MANIFEST, {"last_session": s_iso, "updated_at": "2026-11-08T03:10:35+00:00", "tickers": 21104, "first_session": "2021-08-26"})
    for tk, kind, sd in (("NVDA", "leader", 2), ("LAGG", "laggard", 4)):
        rows = bank_rows(series(kind, seed=sd))
        c = rows[-1][4] * 3.0
        s3.put(lf.bank_key(tk), {"rows": rows + [[sat_ms, c, c, c, c, 100.0, c, 1]]})
    logs = []
    out = lf.build(s3, NOW, log=logs.append)
    assert out["version"] == lf.VERSION == "1.1.0" and out["as_of"] == fri and out["bank"]["last_session"] == fri and out["bank"]["manifest_last_session"] == s_iso
    assert out["bank"]["thin_days"] == [{"day": s_iso, "tickers": 2, "of": 11, "benchmark": False}] and out["benchmark"]["as_of"] == fri
    assert any("set aside as thin" in str(m) for m in logs)
    by, by0 = {r["ticker"]: r for r in out["rows"]}, {r["ticker"]: r for r in base["rows"]}
    assert out["boards"]["n_ranked"] == base["boards"]["n_ranked"] == 10 and not any(r["stale"] for r in out["rows"])
    # the straggler bar is out of the run: the desk reads exactly what it read on the Friday
    for tk in ("NVDA", "LAGG", "AAPL", "COIL"):
        assert by[tk]["as_of"] == fri and by[tk]["px"] == by0[tk]["px"] and by[tk]["strength"] == by0[tk]["strength"] and by[tk]["sessions"] == by0[tk]["sessions"], tk
    # and before 1.0.12 it did not: the manifest's Saturday left two names ranked
    old = lf.effective_session
    lf.effective_session = lambda manifest_last, bench, bars_by, share=0.25: (manifest_last, [])
    try:
        was = lf.build(s3, NOW, log=lambda *a: None)
    finally:
        lf.effective_session = old
    assert was["as_of"] == s_iso and was["boards"]["n_ranked"] == 2 and sum(1 for r in was["rows"] if r["stale"]) == 9


def test_handler():
    s3 = seed()
    BOOK[:] = []                    # no factor book: the whole-market pass must report it and leave the desk alone
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
    # 1.1.0: with no factor book the whole-market pass says so and publishes nothing; the desk's artifact is already written
    assert "error" in res["wide"] and "factor book" in res["wide"]["error"] and not [x for x in s3.puts if x["Key"] == lf.WIDE_KEY]



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
    assert th["window"]["sessions"] > 100 and set(th["windows"]) == {"21", "63", "252"} and th["predictors"]["asymmetry"].startswith("both sides") and th["predictor_order"] == ["strength", "asymmetry", "bigdays"]
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
        bot = y["dump"][pred]["stocks"]["bottom_decile"]      # among stocks: the WEAK names (the both-sides rank's bottom tenth of ALL names also holds bonds, whose up-day side is the weaker one)
        assert bot["mean_excess_pp"] < -1.0 and bot["held_up_pct"] <= 5, (pred, bot)
        r_ = y["rally"][pred]["all"]
        assert r_["n_days"] == n_rally and r_["top_decile"]["days_beat_spy"] == n_rally and r_["top_decile"]["hit_pct"] >= 95 and r_["top_decile"]["mean_excess_pp"] > 0.5, (pred, r_)
        assert y["both_sides"][pred]["both_sides"] is True
        st = y["dump"][pred]["stocks"]
        assert st["n_days"] == n_dump and st["top_decile"]["days_beat_spy"] == n_dump
    # the big-days rank exists once the trailing 126 sessions hold >= 3 dump days and >= 3 rally days; on its sessions the STR names are the top tenth
    bd = y["dump"]["bigdays"]["all"]
    assert 1 <= bd["n_days"] < n_dump and bd["top_decile"]["days_beat_spy"] == bd["n_days"] and bd["top_decile"]["mean_excess_pp"] > 1.0, bd
    br = y["rally"]["bigdays"]["all"]
    assert br["n_days"] >= 1 and br["top_decile"]["days_beat_spy"] == br["n_days"], br
    assert y["both_sides"]["bigdays"]["both_sides"] is True and th["last_session"]["bigdays"] is not None
    assert all(x["ticker"].startswith("STR") for x in th["last_session"]["bigdays"]["all"]["top_names"])
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
    assert "industry-confirmed leaders on dump days" in th["verdict"] and "both-sides rank" in th["verdict"] and "big-days rank" in th["verdict"] and "None" not in th["verdict"], th["verdict"]
    assert len(json.dumps(th)) < 900000, len(json.dumps(th))


# ------------------------------------------------------------------------------------------------ 1.1.0: the whole market
RULES = {"flat_pct": lf.FLAT_PCT, "dump_pct": lf.DUMP_PCT, "rally_pct": lf.RALLY_PCT, "weights": dict(lf.WEIGHTS)}


def closes_of(seed_, drift=0.0004, n=300, days=None):
    """Closes from daily returns: a drift with noise, and `days` = {index from the end (1 = last session): return %}."""
    rnd = random.Random(seed_)
    rets = [drift + rnd.gauss(0, 0.003) for _ in range(n)]
    for back, pct in (days or {}).items():
        rets[n - back] = pct / 100.0
    px, out = 100.0, []
    for r in rets:
        px *= 1 + r
        out.append(px)
    return out


# SPY: four dump days (the last session -2%, then 10, 20 and 30 sessions back) and one rally day 15 back
SPY_DAYS = {1: -2.0, 10: -1.5, 20: -1.2, 30: -1.1, 15: 1.4}


def wide_seed():
    """A bank of eleven names around SPY's three dump days, and the factor book that names them."""
    s3 = S3Stub()
    s3.put(pb.MANIFEST, {"last_session": DATES[-1].isoformat(), "updated_at": "2026-11-04T03:13:33+00:00", "tickers": 21088, "first_session": "2021-08-26"})
    plan = {
        "SPY": (1, 0.0004, SPY_DAYS),
        "HOLD": (2, 0.0030, {1: 1.0, 10: 0.4, 20: 0.1, 30: 0.2, 15: 2.0}),        # strong coming in; up on every dump day, beats the rally
        "FLAT": (3, 0.0004, {1: -0.1, 10: -0.2, 20: 0.0, 30: 0.0, 15: 1.0}),      # flat on the dump days
        "LESS": (4, 0.0010, {1: -0.9, 10: -0.5, 20: -3.0, 30: -0.5, 15: 1.5}),    # down less on three, down MORE on one
        "MORE": (5, -0.0030, {1: -5.0, 10: -4.0, 20: -3.0, 30: -3.0, 15: 0.2}),   # weak coming in; down more every time
        "SMAL": (6, 0.0004, {1: 0.6, 10: -2.5, 20: -2.0, 30: -2.0, 15: 1.0}),     # a $300M stock: counted among stocks, not among $1B+
        "QQQE": (7, 0.0004, {1: -1.0, 10: -1.0, 20: -1.0, 30: -1.0, 15: 1.0}),    # an ETF, down less
        "BOND": (8, 0.0001, {1: 0.4, 10: 0.3, 20: 0.2, 30: 0.1, 15: -0.3}),       # an ETF, up
        "TINY": (9, 0.0004, {1: 3.0}),                                            # an ETF under the liquid floor
        "SHRT": (18, 0.0, {1: 2.0, 10: 1.5, 20: 1.2, 30: 1.1, 15: -1.4}),         # an inverse fund: up on every dump day by construction
        "VOLX": (19, 0.0, {1: 6.0}),                                              # a long-volatility fund
    }
    for t, (sd, drift, days) in plan.items():
        s3.put(lf.bank_key(t), {"rows": bank_rows(closes_of(sd, drift, days=days))})
    # DEAL: pinned to a deal price for the last 120 sessions (see seed()); NEWB: 30 sessions; LATE: its last bar is the session before
    pinned = closes_of(12)
    rows = bank_rows([c if i < 180 else 50.0 * (1 + 0.003 * math.sin(i)) for i, c in enumerate(pinned)])
    for i, r in enumerate(rows):
        if i >= 180:
            r[2], r[3] = r[4] * 1.001, r[4] * 0.999
    s3.put(lf.bank_key("DEAL"), {"rows": rows})
    s3.put(lf.bank_key("NEWB"), {"rows": bank_rows(closes_of(13, n=30, days={1: 2.0}))})
    s3.put(lf.bank_key("LATE"), {"rows": bank_rows(closes_of(14))[:-1]})
    # SPLT: a 1-for-4 reverse split 40 sessions back that Polygon's reference records; without the rebase it is a +300% session
    raw = closes_of(15, days={1: -0.5})
    s3.put(lf.bank_key("SPLT"), {"rows": bank_rows([c / 4.0 if i < 260 else c for i, c in enumerate(raw)], vol_profile=lambda i: 4_000_000.0 if i < 260 else 1_000_000.0)})
    # JUMP: a 5-to-1 print 30 sessions back that no split explains (volume does not move): measured, set aside
    rawj = closes_of(16, days={5: 3.0})                  # a 3% day inside the month: the tape is not a deal's
    s3.put(lf.bank_key("JUMP"), {"rows": bank_rows([c / 5.0 if i < 270 else c for i, c in enumerate(rawj)])})
    # THIN: no bar on six of the last sixty-three sessions
    rt = bank_rows(closes_of(17))
    s3.put(lf.bank_key("THIN"), {"rows": [r for i, r in enumerate(rt) if i not in (245, 250, 255, 260, 265, 270)]})
    s3.put(wide.BANK_SPLITS_ROOT + DATES[260].isoformat()[:7] + ".json.gz", {"results": [{"ticker": "SPLT", "execution_date": DATES[260].isoformat(), "split_from": 4, "split_to": 1}]})
    ahead = (DATES[-1] + timedelta(days=3)).isoformat()
    key = wide.BANK_SPLITS_ROOT + ahead[:7] + ".json.gz"
    prior = json.loads(gzip.decompress(s3.store[key])) if key in s3.store else {"results": []}
    prior["results"].append({"ticker": "HOLD", "execution_date": ahead, "split_from": 1, "split_to": 10})
    s3.put(key, prior)
    BOOK[:] = [
        book_row("SPY", "SPDR S&P 500 ETF Trust", "etf", mcap=7e11, dv=4e10),
        book_row("HOLD", "Holding Co", mcap=50e9), book_row("FLAT", "Flat Corp", sector="Utilities", industry="Regulated Electric", mcap=20e9),
        book_row("LESS", "Less Inc", sector="Utilities", industry="Regulated Electric", mcap=2e9), book_row("MORE", "More Corp", mcap=3e9),
        book_row("SMAL", "Small Co", mcap=3e8), book_row("QQQE", "Equal Weight Nasdaq ETF", "etf", mcap=1e9, dv=6e6),
        book_row("BOND", "Long Treasury Bond ETF", "etf", mcap=5e9, dv=9e8), book_row("TINY", "Tiny Thematic ETF", "etf", mcap=5e7, dv=4e5),
        book_row("SHRT", "ProShares Short S&P500", "etf", mcap=2e9, dv=5e8), book_row("VOLX", "Short-Term VIX Futures ETF", "etf", mcap=5e8, dv=2e8),
        book_row("DEAL", "Pending Deal Co", mcap=8e9), book_row("NEWB", "New Listing Inc", mcap=4e9, ipo=DATES[-30].isoformat()), book_row("LATE", "Late Print Co", mcap=2e9),
        book_row("SPLT", "Reverse Split Co", mcap=1.5e9), book_row("JUMP", "Jump Co", mcap=6e9), book_row("THIN", "Thin Co", mcap=2e9),
        book_row("GEAR", "ProShares UltraPro 3x Whatever", "etf"), book_row("OTCX", "Otc Co", is_otc=True), book_row("PREF", "Bank 6.5% Preferred", is_common=False, is_preferred=True),
        book_row("DUST", "Dust Co", dv=1e5), book_row("UNBK", "Unbanked Co"),
    ]
    return s3


def wide_built(**kw):
    s3 = wide_seed()
    doc = wide.build(s3, NOW, DATES[-1].isoformat(), lf.VERSION, RULES, classify=lf.asset_class_of, class_labels=lf.CLASS_LABEL, log=lambda *a: None, **kw)
    return s3, doc, {r[0]: dict(zip(doc["cols"], r)) for r in doc["rows"]}


def test_wide_record_is_the_capture_arithmetic():
    """wide.record keeps the counts; on any series it must agree with the desk's capture() - one definition of a dump day."""
    rnd = random.Random(7)
    for trial in range(40):
        s, c = [100.0], [50.0]
        for _ in range(140):
            sr = rnd.gauss(0, 0.012)
            s.append(s[-1] * (1 + sr))
            c.append(c[-1] * (1 + 0.6 * sr + rnd.gauss(0, 0.01)))
        for n in (21, 63, 126):
            a, b = wide.record(c, s, n, lf.DUMP_PCT, lf.RALLY_PCT, lf.FLAT_PCT), lf.capture(c, s, n)
            assert a["dn"] == b["dump_days"] and a["rn"] == b["rally_days"] and a["dx"] == b["dump_excess_pp"] and a["rx"] == b["rally_excess_pp"], (trial, n, a, b)
            assert a["dnx"] == b["down_excess_pp"] and a["upx"] == b["up_excess_pp"]
            assert b["dump_held_pct"] == (lf.f(100.0 * a["dh"] / a["dn"], 0) if a["dn"] else None) and b["rally_beat_pct"] == (lf.f(100.0 * a["rb"] / a["rn"], 0) if a["rn"] else None)
            assert 0 <= a["ds"] <= a["dh"] <= a["dn"]
    assert wide.record([1.0] * 10, [1.0] * 10, 21, -1.0, 1.0, 0.25) is None
    # the streak counts the most recent dump days held, newest first
    s = [100, 98, 99, 97, 98, 96]                    # dump days at 1, 3, 5
    assert wide.record([100, 99, 99, 99, 99, 95], s, 5, -1.0, 1.0, 0.25)["ds"] == 0          # the newest one broke
    r = wide.record([100, 95, 95, 95, 95, 95], s, 5, -1.0, 1.0, 0.25)
    assert (r["dn"], r["dh"], r["ds"]) == (3, 2, 2)


def test_wide_universe_and_the_rows():
    s3, doc, by = wide_built()
    u = doc["universe"]
    assert doc["contract"] == "strength-wide.v1" and doc["engine"] == "justhodl-strength" and doc["version"] == lf.VERSION and doc["as_of"] == DATES[-1].isoformat() and doc["prior_session"] == DATES[-2].isoformat()
    assert doc["cols"] == wide.COLS and all(len(r) == len(wide.COLS) for r in doc["rows"])
    # who is drawn: common stocks and plain ETFs that trade; the benchmark is the yardstick, not a row
    assert u["left_out"] == {"geared or inverse fund": 1, "otc": 1, "not a common stock": 1, "thin (under the dollar-volume floor)": 1, "the benchmark": 1}, u["left_out"]
    assert "SPY" not in by and "GEAR" not in by and "OTCX" not in by and "PREF" not in by and "DUST" not in by and "TINY" in by
    assert u["unbanked"] == 1 and u["not_current"] == 1 and u["not_current_sample"] == ["LATE"] and "LATE" not in by and "UNBK" not in by
    assert u["scanned"] == len(doc["rows"]) == 15 and u["scanned_stock"] == 10 and u["scanned_etf"] == 5 and u["eligible"] == 17
    # the last session's bar and the close before it: the page needs nothing else to state a move
    h = by["HOLD"]
    assert round((h["c"] / h["pc"] - 1) * 100, 2) == 1.0 and h["o"] < h["c"] < h["h"] and h["l"] < h["c"] and h["v"] == 1_000_000 and h["av"] == 1_000_000 and h["vw"] == h["c"]
    assert h["kind"] == "stock" and h["cls"] == "stock" and h["sp"] == 0 and h["mcap"] == 50e9 and h["sector"] == "Technology"
    assert by["BOND"]["cls"] == "bond" and by["QQQE"]["cls"] == "equity_etf" and by["BOND"]["kind"] == "etf"
    # excess vs SPY on the last session: the name's move minus SPY's -2%
    assert h["x1"] == 3.0 and by["MORE"]["x1"] == -3.0 and by["FLAT"]["x1"] == 1.9 and by["QQQE"]["x1"] == 1.0
    # the dump-day record: four dump days, one rally day
    assert [d["date"] for d in doc["dump_days"]] == [DATES[-30].isoformat(), DATES[-20].isoformat(), DATES[-10].isoformat(), DATES[-1].isoformat()] and [d["date"] for d in doc["rally_days"]] == [DATES[-15].isoformat()]
    assert (h["dn"], h["dh"], h["ds"], h["rn"], h["rb"]) == (4, 4, 4, 1, 1) and abs(h["dx"] - (3.0 + 1.9 + 1.3 + 1.3) / 4) <= 0.006
    assert (by["FLAT"]["dh"], by["FLAT"]["ds"]) == (4, 4) and (by["LESS"]["dh"], by["LESS"]["ds"]) == (3, 2) and (by["MORE"]["dh"], by["MORE"]["ds"], by["MORE"]["rb"]) == (0, 0, 0)
    assert by["SMAL"]["dh"] == 1 and by["SMAL"]["ds"] == 1                      # held today, broke on the three before
    # the record each name carried INTO the last session: the three dump days before it
    assert (h["dnp"], h["dhp"]) == (3, 3) and (by["LESS"]["dnp"], by["LESS"]["dhp"]) == (3, 2) and (by["MORE"]["dnp"], by["MORE"]["dhp"]) == (3, 0) and by["NEWB"]["dnp"] is None
    assert [wide.record_bucket(*x) for x in ((3, 3), (3, 2), (6, 4), (6, 3), (6, 2), (6, 1), (3, 0), (2, 2), (None, None))] == ["all", "most", "most", "some", "some", "few", "few", None, None]
    # ranks: among the name's kind; the leader ranks first, the laggard last; the rank carried in is one session older
    st_ranks = [r["rs"] for r in by.values() if r["kind"] == "stock" and r["rs"] is not None]
    assert by["HOLD"]["rs"] == max(st_ranks) and by["MORE"]["rs"] == min(st_ranks) and by["HOLD"]["rs"] > 80 > 20 > by["MORE"]["rs"]
    assert by["HOLD"]["rsp"] is not None and by["BOND"]["rs"] is not None and by["QQQE"]["rs"] is not None
    assert u["ranked"] == {"stock": 6, "etf": 4}, u["ranked"]
    # set aside, measured and shown, never ranked: a deal tape, an unexplained 5-to-1 print, thin trading; a new listing is short, not set aside
    assert by["DEAL"]["why"] == "pinned" and by["DEAL"]["rs"] is None and by["JUMP"]["why"] == "jump" and by["JUMP"]["jd"] == DATES[270].isoformat() and by["JUMP"]["rs"] is None
    assert by["THIN"]["why"] == "thin" and by["THIN"]["rs"] is None and by["NEWB"]["why"] is None and by["NEWB"]["rs"] is None and by["NEWB"]["n"] == 30 and by["NEWB"]["x63"] is None
    assert u["set_aside"] == {"pinned": 1, "jump": 1, "thin": 1, "geared": 1}
    # an inverse fund is measured and set aside (it tops every dump day by construction); a long-volatility fund is ranked in its class
    assert by["SHRT"]["why"] == "geared" and by["SHRT"]["cls"] == "leveraged" and by["SHRT"]["rs"] is None and by["SHRT"]["dh"] == 4 and by["VOLX"]["cls"] == "volatility" and by["VOLX"]["why"] is None
    # a recorded reverse split is rebased (no +300% session, no jump flag); an announced one is handed to the page
    assert by["SPLT"]["why"] is None and by["SPLT"]["rs"] is not None and abs(by["SPLT"]["x21"]) < 15
    assert doc["splits_ahead"] == {"HOLD": {"date": (DATES[-1] + timedelta(days=3)).isoformat(), "from": 1, "to": 10}}
    assert by["HOLD"]["ma"] == 3 and by["MORE"]["ma"] == 0 and 0 <= by["HOLD"]["p52"] <= 100 and by["HOLD"]["atr"] > 0
    assert doc["benchmark"]["ticker"] == "SPY" and round(doc["benchmark"]["ret"], 2) == -2.0 and doc["benchmark"]["av"] == 1_000_000
    assert doc["rules"]["flat_pct"] == lf.FLAT_PCT and doc["rules"]["dump_pct"] == lf.DUMP_PCT and doc["rules"]["suspect"] == [0.56, 1.8]
    assert "api.polygon.io" not in json.dumps(doc) and doc["n_errors"] == 0 and not s3.puts


def test_wide_read_of_the_last_session():
    s3, doc, by = wide_built()
    ls = doc["last_session"]
    assert ls["date"] == DATES[-1].isoformat() and ls["rank_as_of"] == DATES[-2].isoformat() and ls["regime"] == "dump" and round(ls["spy_ret"], 2) == -2.0 and ls["rank_col"] == "rsp"
    bm = doc["benchmark"]
    assert ls["spy_ret"] == bm["ret"] == round((bm["c"] / bm["pc"] - 1) * 100, 4), "SPY's move is stated from the published prices, so the page restates it exactly"
    g = ls["groups"]
    # counted: HOLD up, FLAT flat, LESS down less, MORE down more, SMAL up, NEWB up, SPLT down less (-0.5% > -2%); DEAL / JUMP / THIN are set aside
    assert g["stocks"] == {"n": 7, "up": 3, "flat": 1, "down_less": 2, "down_more": 1, "held": 6, "held_pct": 85.7, "up_pct": 42.9, "beat": 6, "median_ret": -0.1,
                           "mean_excess": g["stocks"]["mean_excess"], "beat_pct": 85.7}, g["stocks"]
    assert (g["stocks_1b"]["n"], g["stocks_1b"]["held"], g["stocks_1b"]["down_more"]) == (6, 5, 1) and (g["stocks_10b"]["n"], g["stocks_10b"]["up"], g["stocks_10b"]["flat"]) == (2, 1, 1)
    assert g["etfs"]["n"] == 4 and g["etfs"]["up"] == 3 and g["etfs"]["down_less"] == 1 and g["etfs_liquid"]["n"] == 3 and g["etfs_liquid"]["up"] == 2
    assert (g["sp500"]["n"], g["sp500"]["up"], g["sp500"]["down_less"]) == (2, 1, 1) and by["SPLT"]["sp"] == 1 and by["NEWB"]["sp"] == 1      # the banked list names NEWB, SPLT and DEAL; DEAL is set aside
    assert ls["n_counted"] == 11 and ls["mcap_1b"]["up"] == 50e9 + 4e9 and ls["mcap_1b"]["down_more"] == 3e9 and ls["mcap_1b"]["flat"] == 20e9 and ls["mcap_1b"]["down_less"] == 2e9 + 1.5e9
    sec = {x["sector"]: x for x in ls["by_sector"]}
    assert sec["Utilities"]["n"] == 2 and sec["Utilities"]["held_pct"] == 100.0 and sec["Technology"]["n"] == 4 and sec["Technology"]["down_more"] == 1 and ls["by_sector"][0]["sector"] == "Utilities"
    cls = {x["cls"]: x for x in ls["by_class"]}
    assert cls["bond"]["up"] == 1 and cls["bond"]["label"] == "Bonds" and cls["equity_etf"]["down_less"] == 1 and [x["cls"] for x in ls["by_class"]] == ["volatility", "bond", "equity_etf"]
    assert "leveraged" not in cls, "a geared fund is in no class count"
    b = ls["boards"]
    assert b["up"] == ["NEWB", "HOLD"] and b["down_less"] == ["SPLT", "LESS"] and b["etf_up"] == ["BOND"] and b["etf_down_most"] == ["QQQE", "BOND"]
    assert b["repeat"] == ["HOLD", "FLAT", "SPLT"]               # held up on every dump day BEFORE the session, and again on it
    assert "VOLX" not in b["etf_up"] and "SHRT" not in b["etf_up"]            # long volatility is counted in its class and kept off the board; the inverse fund is set aside
    rec = {x["key"]: x for x in ls["by_record"]}
    assert ls["record_cols"] == ["dnp", "dhp"] and [x["key"] for x in ls["by_record"]] == ["all", "most", "some", "few"]
    assert (rec["all"]["n"], rec["all"]["held"]) == (3, 3) and (rec["most"]["n"], rec["most"]["down_less"]) == (1, 1) and rec["some"] == {"n": 0, "key": "some", "label": "on a third to two thirds"} and (rec["few"]["n"], rec["few"]["down_more"]) == (1, 1)
    assert "MORE" not in b["strong_holding"] and b["strong_breaking"] == [] and ls["by_rank"] == []          # six ranked $1B+ names: too few for deciles
    # the read can be restated from the published document alone (the page does exactly this when the market is shut)
    assert wide.last_read(json.loads(json.dumps(doc)), class_labels=lf.CLASS_LABEL) == json.loads(json.dumps(ls))
    # the same function reads the page's live join: outcome words and the regime
    assert [wide.outcome(x, -2.0, 0.25) for x in (0.26, 0.25, -0.25, -0.26, -1.99, -2.0, -3.0)] == ["up", "flat", "flat", "down_less", "down_less", "down_more", "down_more"]
    assert [wide.outcome(x, 1.0, 0.25) for x in (2.0, 0.5, 0.0, -0.5)] == ["up", "up", "flat", "down_more"]
    assert [wide.regime_of(x, RULES) for x in (-1.0, -0.99, -0.25, 0.25, 0.26, 1.0)] == ["dump", "down", "flat", "flat", "up", "rally"]


def test_wide_by_rank_is_walk_forward():
    """With enough ranked $1B+ names the read carries ten buckets by the rank each name held BEFORE the session."""
    rows = [{"t": "S%03d" % i, "kind": "stock", "mcap": 2e9, "adv": 1e7, "sp": 0, "sector": "X", "industry": "Y", "why": None, "rsp": float(i), "rs": 100.0 - i,
             "ret": (-3.0 if i < 20 else 0.5), "dn": 0, "dh": 0} for i in range(100)]
    rd = wide.session_read(rows, -2.0, RULES, rank_col="rsp")
    assert len(rd["by_rank"]) == 10 and all(d["n"] == 10 for d in rd["by_rank"]) and rd["n_ranked_1b"] == 100
    assert rd["by_rank"][0]["held_pct"] == 0.0 and rd["by_rank"][1]["held_pct"] == 0.0 and rd["by_rank"][2]["held_pct"] == 100.0 and rd["by_rank"][9]["up_pct"] == 100.0
    assert rd["industries"]["n"] == 1 and rd["industries"]["holding"][0]["industry"] == "Y" and rd["industries"]["holding"][0]["n"] == 100
    live = wide.session_read(rows, -2.0, RULES, rank_col="rs")           # the page's live read ranks by the table's current rank
    assert live["by_rank"][9]["held_pct"] == 0.0 and live["rank_col"] == "rs"
    assert wide.session_read([dict(r, ret=None) for r in rows], -2.0, RULES)["n_counted"] == 0


def test_wide_never_publishes_a_partial_table():
    s3 = wide_seed()
    try:
        wide.build(s3, NOW, DATES[-1].isoformat(), lf.VERSION, RULES, log=lambda *a: None, remaining_s=90.0)
        raise SystemExit("a run with no time left must not start the pass")
    except RuntimeError as exc:
        assert "not started" in str(exc), exc
    clock = {"t": 1000.0}

    def tick():
        clock["t"] += 200.0                       # every look at the clock costs 200 seconds: the deadline passes mid-read
        return clock["t"]
    real_time = wide.time
    wide.time = SimpleNamespace(time=tick)
    try:
        wide.build(s3, NOW, DATES[-1].isoformat(), lf.VERSION, RULES, log=lambda *a: None, remaining_s=600.0)
        raise SystemExit("a read that ran out of time must not return a table")
    except RuntimeError as exc:
        assert "partial table is not published" in str(exc), exc
    finally:
        wide.time = real_time
    wide.MIN_SCANNED = 50
    try:
        wide.build(s3, NOW, DATES[-1].isoformat(), lf.VERSION, RULES, log=lambda *a: None)
        raise SystemExit("a short universe must not return a table")
    except RuntimeError as exc:
        assert "book is short" in str(exc), exc
    finally:
        wide.MIN_SCANNED = 3
    try:
        wide.build(s3, NOW, DATES[-2].isoformat(), lf.VERSION, RULES, log=lambda *a: None)   # the desk stands on the session before: bars after it are left out
    except RuntimeError as exc:
        raise SystemExit("an earlier session must be readable: %s" % exc)
    try:
        wide.build(s3, NOW, (DATES[-1] + timedelta(days=1)).isoformat(), lf.VERSION, RULES, log=lambda *a: None)
        raise SystemExit("a session the bank does not hold must not return a table")
    except RuntimeError as exc:
        assert "mid-fold" in str(exc), exc


def test_wide_stands_on_the_desks_session():
    """Bars after the session the desk stands on are left out of every series (the thin-day rule reaches the wide table)."""
    s3 = wide_seed()
    doc = wide.build(s3, NOW, DATES[-2].isoformat(), lf.VERSION, RULES, classify=lf.asset_class_of, log=lambda *a: None)
    by = {r[0]: dict(zip(doc["cols"], r)) for r in doc["rows"]}
    assert doc["as_of"] == DATES[-2].isoformat() and doc["prior_session"] == DATES[-3].isoformat() and "LATE" in by and doc["last_session"]["regime"] != "dump"
    assert [d["date"] for d in doc["dump_days"]] == [DATES[-30].isoformat(), DATES[-20].isoformat(), DATES[-10].isoformat()]


def test_handler_writes_the_wide_table_after_the_desk():
    s3 = wide_seed()
    for k, v in seed().store.items():                  # the desk's own inputs beside the wide bank
        s3.store.setdefault(k, v)
    lf._s3 = lambda: s3
    ctx = SimpleNamespace(get_remaining_time_in_millis=lambda: 880000)
    res = lf.lambda_handler({}, ctx)
    keys = [x["Key"] for x in s3.puts]
    assert res["statusCode"] == 200 and res["key"] == "data/strength.json" and res["wide"]["key"] == lf.WIDE_KEY == "data/strength-wide.json" and res["wide"]["rows"] == 15 and "error" not in res["wide"]
    assert keys.index("data/strength.json") < keys.index(lf.WIDE_KEY), keys          # the desk's artifact is safe before the whole-market pass starts
    put = [x for x in s3.puts if x["Key"] == lf.WIDE_KEY][-1]
    doc = json.loads(put["Body"])
    assert put["CacheControl"] == "max-age=300" and doc["contract"] == "strength-wide.v1" and doc["as_of"] == json.loads([x for x in s3.puts if x["Key"] == "data/strength.json"][-1]["Body"])["as_of"]
    hist = [x for x in s3.puts if x["Key"] == lf.HISTORY_PREFIX + DATES[-1].isoformat() + ".wide.json.gz"]
    assert len(hist) == 1 and hist[0]["ContentEncoding"] == "gzip" and json.loads(gzip.decompress(hist[0]["Body"]))["as_of"] == doc["as_of"]
    assert round(res["wide"]["spy_ret"], 2) == -2.0 and res["wide"]["spy_ret"] == doc["benchmark"]["ret"] and res["wide"]["held_up_1b_pct"] == doc["last_session"]["groups"]["stocks_1b"]["held_pct"]
    # the desk alone
    s3.puts.clear()
    res = lf.lambda_handler({"wide": False}, ctx)
    assert "skipped" in res["wide"] and [x["Key"] for x in s3.puts if x["Key"] == lf.WIDE_KEY] == []
    # the whole-market table alone, on the session the published desk stands on
    s3.puts.clear()
    res = lf.lambda_handler({"wide_only": True}, ctx)
    assert res["wide"]["rows"] == 15 and "data/strength.json" not in [x["Key"] for x in s3.puts] and lf.WIDE_KEY in [x["Key"] for x in s3.puts]
    # a fault in the whole-market pass never costs the desk its artifact
    s3.puts.clear()
    real = sys.modules["bank_enrich"].rows
    sys.modules["bank_enrich"].rows = lambda fields=None, **kw: (_ for _ in ()).throw(RuntimeError("book unreachable"))
    try:
        res = lf.lambda_handler({}, ctx)
    finally:
        sys.modules["bank_enrich"].rows = real
    assert res["statusCode"] == 200 and res["rows"] > 0 and "book unreachable" in res["wide"]["error"] and "data/strength.json" in [x["Key"] for x in s3.puts] and lf.WIDE_KEY not in [x["Key"] for x in s3.puts]


def test_wide_fund_class_and_what_is_geared():
    """Real names from the live table of 2026-11-19: an "Ultra-Short" bond fund is cash-like, not leveraged (32 of the first
    table's 37 geared funds were such bond funds); a fund short the market by its name is geared whatever its class."""
    fc = lambda t, n: wide.fund_class(t, n, lf.asset_class_of)   # noqa: E731
    cases = {("JPST", "JPMorgan Ultra-Short Income ETF"): ("cash", False), ("ICSH", "iShares Ultra Short Duration Bond Active ETF"): ("cash", False),
             ("PUSH", "PGIM Ultra Short Municipal Bond ETF"): ("cash", False), ("VGUS", "Vanguard Ultra-Short Treasury ETF"): ("cash", False),
             ("BSV", "Vanguard Short-Term Bond ETF"): ("bond", False), ("SMB", "VanEck Short Muni ETF"): ("bond", False), ("TUA", "Simplify Short Term Treasury Futures Strategy ETF"): ("bond", False),
             ("SHYD", "VanEck Short High Yield Muni ETF"): ("high_yield", False), ("NEAR", "iShares Short Duration Bond Active ETF"): ("bond", False),
             ("VIXY", "ProShares - VIX Short-Term Futures ETF"): ("volatility", False), ("CLSE", "Convergence Long/Short Equity ETF"): ("equity_etf", False),
             ("RWM", "ProShares - Short Russell2000"): ("equity_etf", True), ("MYY", "ProShares - Short MidCap400"): ("equity_etf", True), ("REK", "ProShares - Short Real Estate"): ("equity_etf", True),
             ("TBF", "ProShares - Short 20+ Year Treasury"): ("bond", True), ("SJB", "ProShares - Short High Yield"): ("high_yield", True), ("EUM", "ProShares - Short MSCI Emerging Markets"): ("country", True),
             ("SETH", "ProShares - Short Ether ETF"): ("crypto", True), ("DIPS", "YieldMax Short NVDA Option Income Strategy ETF"): ("equity_etf", True),
             ("WNTR", "YieldMax MSTR Short Option Income Strategy ETF"): ("equity_etf", True), ("DWSH", "AdvisorShares Dorsey Wright Short ETF"): ("equity_etf", True),
             ("SH", "ProShares - Short S&P500"): ("leveraged", True), ("PSQ", "ProShares - Short QQQ"): ("leveraged", True), ("BITI", "ProShares Short Bitcoin ETF"): ("leveraged", True),
             ("SVXY", "ProShares - Short VIX Short-Term Futures ETF"): ("leveraged", True), ("SLTY", "YieldMax Ultra Short Option Income Strategy ETF"): ("leveraged", True),
             ("XLP", "Consumer Staples Select Sector SPDR Fund"): ("equity_etf", False), ("TLT", "iShares 20+ Year Treasury Bond ETF"): ("bond", False),
             # a stock fund chosen or weighted by volatility is a stock fund (26 of the 33 funds the second table of 2026-11-20 had under
             # "volatility" - SPLV and SPHD among them, kept off the ETF boards on a dump day); the funds that hold volatility stay
             ("SPLV", "Invesco S&P 500 Low Volatility ETF"): ("equity_etf", False), ("SPHD", "Invesco S&P 500 High Dividend Low Volatility ETF"): ("equity_etf", False),
             ("VFMV", "Vanguard U.S. Minimum Volatility ETF"): ("equity_etf", False), ("CFA", "VictoryShares US 500 Volatility Wtd ETF"): ("equity_etf", False),
             ("HUSV", "First Trust Horizon Managed Volatility Domestic ETF"): ("equity_etf", False), ("BDVL", "iShares Disciplined Volatility Equity Active ETF"): ("equity_etf", False),
             ("EELV", "Invesco S&P Emerging Markets Low Volatility ETF"): ("country", False), ("LVHI", "Franklin International Low Volatility High Dividend Index ETF"): ("country", False),
             ("VXX", "iPath Series B S&P 500 VIX Short-Term Futures ETN"): ("volatility", False), ("VXZ", "iPath Series B S&P 500 VIX Mid-Term Futures ETN"): ("volatility", False),
             ("SVOL", "Simplify Volatility Premium ETF"): ("volatility", False), ("IVOL", "Quadratic Interest Rate Volatility and Inflation Hedge ETF"): ("volatility", False)}
    for (t, n), want in cases.items():
        assert fc(t, n) == want, (t, n, fc(t, n), want)
    assert wide.fund_class("ABCD", "Some Equity Fund", None) == ("equity_etf", False)
    assert wide.fund_class("ABCD", "Some Low Volatility Fund", lambda t, k, n: ("volatility", None)) == ("equity_etf", False)   # a class map that cannot tell: a stock fund


def test_wide_live_join_is_the_pages_arithmetic():
    """Today's forming bars over the table: the move is the last price over the table's close; an announced split divides
    the close; a move no split explains is shown, not counted; a name that has not printed is not counted."""
    s3, doc, by = wide_built()
    nxt = (DATES[-1] + timedelta(days=3)).isoformat()                 # the session after the table's (a Monday): HOLD splits 10-for-1 that day
    spy_c = doc["benchmark"]["c"]

    def bar(prev, pct, o_pct=0.0, hi_pct=None, lo_pct=None, v=500000.0):
        c = prev * (1 + pct / 100.0)
        o = prev * (1 + o_pct / 100.0)
        return [o, max(o, c) * (1 + (hi_pct or 0) / 100.0), min(o, c) * (1 - (lo_pct or 0) / 100.0), c, v, (o + c) / 2, 1795196128833]
    today = {"session": nxt, "as_of": nxt + "T17:35:28+00:00", "delay_minutes": 15, "fields": ["o", "h", "l", "c", "v", "vw", "updated_ms"], "rows": {
        "SPY": bar(spy_c, -2.0, o_pct=-0.5, hi_pct=0.1, lo_pct=0.2),
        "HOLD": bar(by["HOLD"]["c"] / 10.0, 1.5),                    # trades at a tenth: the announced 10-for-1
        "FLAT": bar(by["FLAT"]["c"], 0.1), "LESS": bar(by["LESS"]["c"], -1.0), "MORE": bar(by["MORE"]["c"], -6.0),
        "SMAL": bar(by["SMAL"]["c"], -80.0),                         # no split on record: unconfirmed
        "BOND": bar(by["BOND"]["c"], 0.8), "QQQE": bar(by["QQQE"]["c"], -2.5), "DEAL": bar(by["DEAL"]["c"], 0.0),
    }}
    j = wide.live_join(doc, today)
    rows = {r["t"]: r for r in j["rows"]}
    assert j["session"] == nxt and j["as_of"] == doc["as_of"] and round(j["spy"]["ret"], 6) == -2.0 and round(j["spy"]["fo"], 4) == round((0.98 / 0.995 - 1) * 100, 4)
    assert round(rows["HOLD"]["ret"], 6) == 1.5 and rows["HOLD"]["split"]["to"] == 10 and round(rows["HOLD"]["live"]["x"], 6) == 3.5
    assert rows["SMAL"]["unconfirmed"] is True and rows["SMAL"]["ret"] is None and rows["NEWB"]["ret"] is None and rows["NEWB"].get("bar") is None and rows["NEWB"]["unconfirmed"] is False
    assert round(rows["LESS"]["live"]["fo"], 6) == -1.0 and rows["LESS"]["live"]["pos"] == 0.0 and round(rows["BOND"]["live"]["vwx"], 4) == round((1.008 / 1.004 - 1) * 100, 4)
    rd = wide.live_read(doc, today, class_labels=lf.CLASS_LABEL)
    g = rd["groups"]
    assert rd["date"] == nxt and rd["rank_as_of"] == doc["as_of"] and rd["rank_col"] == "rs" and rd["regime"] == "dump" and rd["bars_as_of"] == today["as_of"]
    # counted: HOLD up, FLAT flat, LESS down less, MORE down more (DEAL is pinned, SMAL unconfirmed, the rest have not printed)
    assert (g["stocks"]["n"], g["stocks"]["up"], g["stocks"]["flat"], g["stocks"]["down_less"], g["stocks"]["down_more"]) == (4, 1, 1, 1, 1) and g["etfs"]["n"] == 2 and g["etfs"]["down_more"] == 1
    assert rd["n_unconfirmed"] == 1 and rd["unconfirmed"] == ["SMAL"] and rd["n_no_print"] == len(doc["rows"]) - 8 and rd["record_cols"] == ["dn", "dh"] and rd["boards"]["up"] == ["HOLD"] and rd["boards"]["strong_holding"] == ["HOLD"]
    # no live session: the forming file is the table's own session (after the nightly run) or older
    assert wide.live_join(doc, dict(today, session=doc["as_of"])) is None and wide.live_read(doc, {"session": DATES[-5].isoformat(), "rows": today["rows"]}) is None
    assert wide.live_join(doc, {"session": nxt, "rows": {}}) is None and wide.live_join(doc, None) is None
    # one bar, read alone
    b = wide.bar_read([100.0, 104.0, 99.0, 103.0, 1e6, 101.0], 100.0, {"ret": -2.0, "fo": -1.0})
    assert [round(b[k], 6) for k in ("ret", "fo", "pos", "x", "fox")] == [3.0, 3.0, 80.0, 5.0, 4.0] and round(b["vwx"], 4) == 1.9802
    assert wide.bar_read([100.0, 100.0, 100.0, 100.0, 0, None], 100.0)["pos"] is None


def test_tape_module_is_the_leaders_engines():
    import hashlib
    a = open(os.path.join(SRC, "leaders_tape.py"), "rb").read()
    b = open(os.path.join(HERE, "..", "..", "justhodl-leaders", "source", "leaders_tape.py"), "rb").read()
    assert hashlib.sha256(a).hexdigest() == hashlib.sha256(b).hexdigest(), "leaders_tape.py must stay byte-identical to the leaders engine's"
    assert dict(wide.T.WEIGHTS) == dict(lf.WEIGHTS) and dict(wide.T.HORIZONS) == dict(lf.HORIZONS), "one composite: the desk's weights and horizons"


if __name__ == "__main__":
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            fn()
            print("ok", name)
    print("justhodl-strength tests: all green")
