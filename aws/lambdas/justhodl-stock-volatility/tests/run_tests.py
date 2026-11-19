"""justhodl-stock-volatility offline tests.  An in-memory S3 holds a small Polygon EOD bank built from REAL banked bars
(tests/fixtures/campaign-scan-bars.json: QQQ, NVDA, PFE, LULU, XOM, TSLA as read by ops 6842), the bank manifest and a
split reference window; the factor book is faked as rows.

Checked: every measure against a second, independent way of computing it on those bars (the standard library's
statistics module for volatility, brute force for the rolling windows); the rules that set a measure aside; ranks and
boards; build() and the handler; a split the bank still holds unadjusted is rebased before anything is measured; a
partial read is never published.  No network, no AWS.

Run: python3 aws/lambdas/justhodl-stock-volatility/tests/run_tests.py
"""
from datetime import datetime, timezone
from types import SimpleNamespace
import gzip
import hashlib
import importlib.util
import io
import json
import math
import os
import statistics
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
SRC = os.path.join(HERE, "..", "source")
ROOT = os.path.abspath(os.path.join(HERE, "..", "..", "..", ".."))
SHARED = os.path.join(ROOT, "aws", "shared")
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
_m = importlib.util.module_from_spec(_spec)
sys.modules["polygon_bankfirst"] = _m
_spec.loader.exec_module(_m)
BOOK = []
sys.modules["bank_enrich"] = SimpleNamespace(rows=lambda fields=None, where=None, limit=None: [dict(r) for r in BOOK])

import vol_measures as M         # noqa: E402
import lambda_function as LF     # noqa: E402

BARS = json.load(open(os.path.join(ROOT, "tests", "fixtures", "campaign-scan-bars.json")))["bars"]
LAST = "2026-11-18"
PASSED = []
SQ = math.sqrt(252) * 100


def ok(name, cond, detail=""):
    if not cond:
        raise AssertionError("%s %s" % (name, detail))
    PASSED.append(name)


def near(a, b, tol=1e-9):
    return a is not None and b is not None and abs(a - b) <= tol * max(1.0, abs(a), abs(b))


def iso(ms):
    return datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc).date().isoformat()


def dict_bars(rows):
    return [{"date": iso(r[0]), "o": r[1], "h": r[2], "l": r[3], "c": r[4], "v": r[5]} for r in rows]


def cols(t):
    b = LF.clean(dict_bars(BARS[t]))
    return [x["date"] for x in b], [x["o"] for x in b], [x["h"] for x in b], [x["l"] for x in b], [x["c"] for x in b], b


class FakeS3:
    def __init__(self):
        self.store, self.puts, self.gets = {}, [], 0

    def put_raw(self, key, obj, gz=False):
        body = json.dumps(obj).encode()
        self.store[key] = gzip.compress(body) if gz else body

    def get_object(self, Bucket, Key):
        self.gets += 1
        if Key not in self.store:
            raise KeyError("NoSuchKey: " + Key)
        return {"Body": io.BytesIO(self.store[Key])}

    def put_object(self, Bucket, Key, Body, **kw):
        self.puts.append(Key)
        self.store[Key] = Body


def meta(t, kind="stock", sector="Technology", mcap=1e11):
    return {"ticker": t, "name": t + " Corp", "kind": kind, "sector": sector, "industry": "Test", "mcap": mcap, "adv_usd": 5e8, "ipo_date": None}


def book_row(t, **kw):
    r = {"symbol": t, "name": t + " Corp", "sector": "Technology", "industry": "Test", "is_common": True, "is_etf": False, "is_otc": False, "mcap": 1e11, "dollar_vol_20d": 5e8, "ipo_date": None}
    r.update(kw)
    return r


def bank(s3, tickers=None, last=LAST, edit=None):
    for t, rows in BARS.items():
        if tickers is None or t in tickers:
            rows = edit(t, rows) if edit else rows
            s3.put_raw(LF.bank_key(t), {"rows": [list(r) + [None, None] for r in rows]}, gz=True)
    s3.put_raw(LF.bank_key("SPY"), {"rows": [r + [None, None] for r in BARS["QQQ"]]}, gz=True)     # the session calendar: any full series
    s3.put_raw(LF.polygon_bankfirst.MANIFEST, {"last_session": last, "first_session": "2021-08-26"})


def row_of(out, t):
    i = out["cols"].index("t")
    r = next(x for x in out["rows"] if x[i] == t)
    return dict(zip(out["cols"], r))


# ----------------------------------------------------------------------------------------------------------- volatility
def test_hv_is_the_sample_stdev_of_log_returns():
    for t in BARS:
        _, _, _, _, C, _ = cols(t)
        for k in (21, 63, 252):
            want = statistics.stdev([math.log(C[i] / C[i - 1]) for i in range(len(C) - k, len(C))]) * SQ
            ok("hv%d %s" % (k, t), near(M.hv(C, k), want), "%s vs %s" % (M.hv(C, k), want))
    ok("hv needs k + 1 closes", M.hv([10.0] * 21, 21) is None and M.hv([10.0] * 22, 21) == 0.0)
    ok("a stock that moves 1% every day reads about 16% a year", 15.9 < M.hv([100.0, 101.0] * 11, 21) < 16.3, str(M.hv([100.0, 101.0] * 11, 21)))


def test_hv_series_is_hv_on_every_bar():
    _, _, _, _, C, _ = cols("NVDA")
    C = C[-340:]
    s = M.hv_series(C, 21)
    ok("hv_series is None until 21 returns exist", s[:21] == [None] * 21 and s[21] is not None)
    for i in (21, 22, 100, 250, len(C) - 1):
        ok("hv_series[%d]" % i, near(s[i], M.hv(C[:i + 1], 21), 1e-7), "%s vs %s" % (s[i], M.hv(C[:i + 1], 21)))
    ok("hv_series of a flat price is zero, never negative", all(v == 0.0 for v in M.hv_series([5.0] * 40, 21)[21:]))


def test_yang_zhang():
    for t in ("NVDA", "PFE", "QQQ"):
        _, O, H, L, C, _ = cols(t)
        k, n = 21, len(C)
        on = [math.log(O[i] / C[i - 1]) for i in range(n - k, n)]
        oc = [math.log(C[i] / O[i]) for i in range(n - k, n)]
        rs = statistics.fmean(math.log(H[i] / O[i]) * math.log(H[i] / C[i]) + math.log(L[i] / O[i]) * math.log(L[i] / C[i]) for i in range(n - k, n))
        kk = 0.34 / (1.34 + (k + 1) / (k - 1))
        want = math.sqrt((statistics.variance(on) + kk * statistics.variance(oc) + (1 - kk) * rs) * 252) * 100
        ok("yang-zhang " + t, near(M.yang_zhang(O, H, L, C, k), want, 1e-9), "%s vs %s" % (M.yang_zhang(O, H, L, C, k), want))
    # no overnight gap and no wick beyond the body: the range term is zero and what is left is k x the open-to-close variance
    C = [100.0]
    O, H, L = [100.0], [100.0], [100.0]
    for i in range(30):
        o = C[-1]
        c = o * (1.02 if i % 3 else 0.97)
        O.append(o), C.append(c), H.append(max(o, c)), L.append(min(o, c))
    kk = 0.34 / (1.34 + 22 / 20)
    want = math.sqrt(kk * statistics.variance([math.log(C[i] / O[i]) for i in range(len(C) - 21, len(C))]) * 252) * 100
    ok("yang-zhang, bodies only", near(M.yang_zhang(O, H, L, C, 21), want))
    ok("yang-zhang needs k + 1 bars", M.yang_zhang(O[:21], H[:21], L[:21], C[:21], 21) is None)


# --------------------------------------------------------------------------------------------------------------- spread
def test_rolling_windows_against_brute_force():
    _, _, H, L, C, _ = cols("TSLA")
    H, L, C = H[-330:], L[-330:], C[-330:]
    for k in (1, 5, 21, 63):
        hi, lo, sp = M.rolling_extreme(H, k, True), M.rolling_extreme(L, k, False), M.spread_series(H, L, k)
        ok("window %d is None until full" % k, hi[:k - 1] == [None] * (k - 1) and hi[k - 1] is not None)
        for i in range(k - 1, len(H)):
            if not (hi[i] == max(H[i - k + 1:i + 1]) and lo[i] == min(L[i - k + 1:i + 1])):
                raise AssertionError("rolling extreme %d at %d" % (k, i))
            if not near(sp[i], (max(H[i - k + 1:i + 1]) / min(L[i - k + 1:i + 1]) - 1) * 100):
                raise AssertionError("spread series %d at %d" % (k, i))
        PASSED.append("rolling %d-session high, low and spread = brute force on every bar" % k)
        one = M.spread(H, L, C, k)
        ok("spread %d = its own series' last value" % k, near(one["sp"], sp[-1]) and one["hi"] == max(H[-k:]) and one["lo"] == min(L[-k:]))
        ok("position %d inside 0..100" % k, 0 <= one["pos"] <= 100 and near(one["pos"], (C[-1] - one["lo"]) / (one["hi"] - one["lo"]) * 100))
    ok("the day's spread is the last bar's own", near(M.spread(H, L, C, 1)["sp"], (H[-1] / L[-1] - 1) * 100))
    ok("a wider window is never narrower", M.spread(H, L, C, 1)["sp"] <= M.spread(H, L, C, 5)["sp"] <= M.spread(H, L, C, 21)["sp"] <= M.spread(H, L, C, 63)["sp"])
    ok("a flat bar has no position", M.spread([5.0], [5.0], [5.0], 1) == {"sp": 0.0, "hi": 5.0, "lo": 5.0, "pos": None})
    ok("too few bars: no spread", M.spread(H[:4], L[:4], C[:4], 5) is None)


def test_small_helpers():
    ok("pct_rank", M.pct_rank(3, [1, 2, 3, 4]) == 62.5 and M.pct_rank(9, [1, 2, 3]) == 100.0 and M.pct_rank(0, [1, 2, 3]) == 0.0 and M.pct_rank(1, []) is None and M.pct_rank(None, [1]) is None)
    ok("pct_rank ignores blanks", M.pct_rank(2, [None, 1, 3]) == 50.0)
    ok("max_move keeps the sign and the date", M.max_move([10, 11, 5.5, 6], ["a", "b", "c", "d"], 3) == (-50.0, "c") and M.max_move([10, 11, 5.5, 6], ["a", "b", "c", "d"], 1)[1] == "d")
    ok("median, quantile", M.median([3, None, 1, 2]) == 2 and M.median([1, 2, 3, 4]) == 2.5 and M.median([]) is None and M.quantile([1, 2, 3, 4, 5], 0.25) == 2 and M.quantile([1, 2], 0.5) == 1.5)
    ok("bands cover every reading once", [M.band_of(v) for v in (0, 14.99, 15, 24.9, 25, 40, 60, 99.9, 100, 950)] == [0, 0, 1, 1, 2, 3, 4, 4, 5, 5] and M.band_of(None) is None)


# ---------------------------------------------------------------------------------------------------------------- rows
def test_measure_on_real_bars():
    dates_q = cols("QQQ")[0]
    for t in ("NVDA", "PFE", "XOM"):
        dates, O, H, L, C, bars = cols(t)
        row, tail = LF.measure(meta(t), bars, None, dates_q[-63:])
        ok("identity " + t, (row["t"], row["kind"], row["n"], row["px"]) == (t, "stock", len(C), round(C[-1], 4 if C[-1] < 10 else 2)))
        for k in (21, 63, 252):
            want = statistics.stdev([math.log(C[i] / C[i - 1]) for i in range(len(C) - k, len(C))]) * SQ
            ok("row hv%d %s" % (k, t), row["hv%d" % k] == round(want, 2))
        ok("hv21 21 sessions ago " + t, row["hv21p"] == round(M.hv(C[:-21], 21), 2), "%s vs %s" % (row["hv21p"], M.hv(C[:-21], 21)))
        year = [M.hv(C[:len(C) - j], 21) for j in range(252)]
        ok("own percentile of hv21 " + t, near(row["hvp"], round(M.pct_rank(year[0], year), 1), 1e-6), "%s vs %s" % (row["hvp"], M.pct_rank(year[0], year)))
        trs = [max(H[i] - L[i], abs(H[i] - C[i - 1]), abs(L[i] - C[i - 1])) for i in range(len(C) - 14, len(C))]
        ok("ATR %% is the chart's: mean true range of 14 over the last close " + t, row["atr"] == round(100 * sum(trs) / 14 / C[-1], 2))
        for key, k in (("d", 1), ("w", 5), ("m", 21), ("q", 63)):
            hi, lo = max(H[-k:]), min(L[-k:])
            ok("spread %s %s" % (key, t), row["sp_" + key] == round((hi / lo - 1) * 100, 2) and row["hi_" + key] == LF._px(hi) and row["lo_" + key] == LF._px(lo))
            wins = [(max(H[len(H) - j - k:len(H) - j]) / min(L[len(L) - j - k:len(L) - j]) - 1) * 100 for j in range(252)]
            ok("own percentile of spread %s %s" % (key, t), near(row["own_" + key], round(M.pct_rank(wins[0], wins), 1), 1e-6))
        big = max(range(len(C) - 63, len(C)), key=lambda i: abs(C[i] / C[i - 1] - 1))
        ok("largest move in 63 " + t, row["mx"] == round((C[big] / C[big - 1] - 1) * 100, 2) and row["mxd"] == dates[big])
        ok("clean row " + t, row["miss"] == 0 and row["aside"] is None and row["jd"] is None and all(row[c] is None for c in LF.RANK_COL.values()))
        ok("tail " + t, len(tail) == 252 and tail[-1][0] == dates[-1] and near(tail[-1][1], M.hv(C, 21), 1e-7) and near(tail[-1][2], (H[-1] / L[-1] - 1) * 100))
    ok("every column of the artifact is a key of the row", set(LF.COLS) == set(row))


def test_short_history_leaves_blanks():
    dates, O, H, L, C, bars = cols("LULU")
    row, tail = LF.measure(meta("LULU"), bars[-30:], None, dates[-63:])
    ok("30 sessions: a month, no quarter, no year", row["hv21"] is not None and row["hv63"] is None and row["hv252"] is None and row["sp_m"] is not None and row["sp_q"] is None and row["hi_q"] is None)
    ok("30 sessions: no own history, no prior reading", row["hvp"] is None and row["own_d"] is None and row["hv21p"] is None)
    ok("sessions before the first bar are not missed", row["miss"] == 0 and row["aside"] is None and len(tail) == 30)


def test_aside_rules():
    ok("no jump, not thin: nothing aside", LF.aside_of(None, 0) == [] and LF.aside_of(None, 3) == [])
    ok("thin: every ranked measure", LF.aside_of(None, 4) == ["hv21", "hv63", "sp_d", "sp_w", "sp_m", "sp_q"])
    j = lambda ago: LF.aside_of({"date": "x", "ratio": 5.0, "ago": ago}, 0)   # noqa: E731
    ok("a jump on the last bar", j(0) == ["hv21", "hv63", "sp_w", "sp_m", "sp_q"])
    ok("the day's own bar is never set aside for a jump", all("sp_d" not in j(a) for a in range(0, 70)))
    ok("week: a bar from before the jump is inside 5 sessions up to 3 back", "sp_w" in j(3) and "sp_w" not in j(4))
    ok("month spread up to 19 back, hv21 up to 20", "sp_m" in j(19) and "sp_m" not in j(20) and "hv21" in j(20) and "hv21" not in j(21))
    ok("quarter spread up to 61 back, hv63 up to 62", "sp_q" in j(61) and "sp_q" not in j(62) and "hv63" in j(62) and j(63) == [])
    ok("flagged jump: the most recent beyond 3-to-1 inside 300 sessions", LF.flagged_jump([{"date": "a", "ratio": 4.0, "ago": 200}, {"date": "b", "ratio": 0.25, "ago": 40}, {"date": "c", "ratio": 2.5, "ago": 5}]) == {"date": "b", "ratio": 0.25, "ago": 40}
       and LF.flagged_jump([{"date": "a", "ratio": 9.0, "ago": 301}]) is None and LF.flagged_jump([]) is None)
    ok("missed sessions", LF.missed_sessions(["d1", "d2", "d4"], ["d1", "d2", "d3", "d4"]) == 1 and LF.missed_sessions(["d3", "d4"], ["d1", "d2", "d3", "d4"]) == 0)


def test_rank_and_boards():
    def r(t, kind, hv21, mcap=5e9, aside=None, hv21p=None, ja=None, **kw):
        row = {c: None for c in LF.COLS}
        row.update({"t": t, "kind": kind, "hv21": hv21, "hv63": hv21, "sp_d": hv21, "sp_w": hv21, "sp_m": hv21, "sp_q": hv21, "mcap": mcap, "aside": aside, "hv21p": hv21p, "ja": ja})
        row.update(kw)
        return row
    rows = [r("A", "stock", 50, hv21p=20), r("B", "stock", 30, hv21p=45), r("C", "stock", 80, mcap=2e8), r("D", "stock", 90, aside="hv21,hv63", ja=5, hv21p=10),
            r("E", "stock", None), r("F", "etf", 12), r("G", "etf", 40), r("H", "stock", 30, hv21p=30)]
    counts = LF.rank(rows)
    by = {x["t"]: x for x in rows}
    ok("1 is the most volatile, inside the kind", (by["C"]["rk_hv21"], by["A"]["rk_hv21"], by["B"]["rk_hv21"], by["H"]["rk_hv21"]) == (1, 2, 3, 4) and (by["G"]["rk_hv21"], by["F"]["rk_hv21"]) == (1, 2))
    ok("a measure set aside has no rank; the others of the row do", by["D"]["rk_hv21"] is None and by["D"]["rk_hv63"] is None and by["D"]["rk_d"] == 1 and by["E"]["rk_hv21"] is None)
    ok("rank counts", counts["stock"]["hv21"] == 4 and counts["stock"]["sp_d"] == 5 and counts["etf"]["hv21"] == 2)
    b = LF.boards_of(rows, n=2)
    ok("boards are drawn from $1B+ common stocks that are ranked", b["most_volatile"] == ["A", "B"] and b["least_volatile"] == ["H", "B"] and "C" not in b["widest_d"] and "F" not in b["most_volatile"])
    ok("the day board ranks a row whose volatility is set aside", b["widest_d"] == ["D", "A"])
    ok("rising / falling is hv21 against 21 sessions ago", b["vol_rising"] == ["A", "H"] and b["vol_falling"] == ["B", "H"])
    ok("twelve boards", sorted(b) == sorted(["most_volatile", "least_volatile", "vol_rising", "vol_falling"] + [p + "_" + w for p in ("widest", "narrowest") for w in "dwmq"]))
    g = LF.group_stats([x for x in rows if x["kind"] == "stock"])
    ok("group medians skip what is set aside", g["n"] == 6 and g["med"]["hv21"] == 40.0 and g["med"]["sp_d"] == 50.0 and g["rising"] == 1 and g["falling"] == 1)
    bands = LF.bands_of(rows)
    ok("bands hold every ranked stock once", sum(x["n"] for x in bands) == 4 and [x["n"] for x in bands] == [0, 0, 2, 1, 1, 0] and bands[4]["mcap"] == 2e8)
    ok("sizes", [LF.size_of(v) for v in (3e11, 2e11, 5e10, 5e9, 1e9, 1e8, None)] == ["mega", "mega", "large", "mid", "small", "micro", None])


def test_universe_rule():
    rows = [book_row("AAA"), book_row("BBB", is_otc=True), book_row("CCC", is_common=False), book_row("ETF1", is_etf=True, is_common=False, name="Plain Index ETF"),
            book_row("ETF2", is_etf=True, is_common=False, name="ProShares UltraPro 3x Bull"), book_row("THIN", dollar_vol_20d=1000), book_row("WAR-WS"), book_row("BRK-B")]
    u, st = LF.select_universe(rows)
    ok("universe", sorted(u) == ["AAA", "BRK.B", "ETF1"] and st["stock"] == 2 and st["etf"] == 1 and u["ETF1"]["kind"] == "etf")
    ok("left out, with the reason", st["left_out"] == {"otc": 1, "not a common stock": 2, "geared or inverse fund": 1, "thin (under the dollar-volume floor)": 1})


def test_tape_module_is_the_leaders_copy():
    a = open(os.path.join(SRC, "leaders_tape.py"), "rb").read()
    b = open(os.path.join(ROOT, "aws", "lambdas", "justhodl-leaders", "source", "leaders_tape.py"), "rb").read()
    ok("leaders_tape.py is byte-identical to the leaders engine's", hashlib.sha256(a).hexdigest() == hashlib.sha256(b).hexdigest())


# --------------------------------------------------------------------------------------------------------------- build
def fresh(edit=None, book=None):
    s3 = FakeS3()
    bank(s3, edit=edit)
    BOOK[:] = book or ([book_row(t) for t in ("NVDA", "PFE", "LULU", "XOM", "TSLA")] + [book_row("QQQ", is_etf=True, is_common=False, name="Invesco QQQ Trust"),
                                                                                         book_row("SPY", is_etf=True, is_common=False, name="SPDR S&P 500 ETF Trust")])
    return s3


def test_build_and_handler():
    old, old_s3 = LF.MIN_SCANNED, LF._s3
    LF.MIN_SCANNED = 5
    try:
        s3 = fresh()
        now = datetime(2026, 11, 19, 4, 6, tzinfo=timezone.utc)
        out = LF.build(s3, now, log=lambda *_: None)
        ok("build writes nothing", s3.puts == [])
        ok("header", (out["engine"], out["contract"], out["as_of"], out["version"]) == ("justhodl-stock-volatility", "stock-volatility.v1", LAST, LF.VERSION))
        ok("one row a symbol, every row as wide as the columns", len(out["rows"]) == 7 and all(len(r) == len(out["cols"]) for r in out["rows"]) and out["cols"] == LF.COLS)
        u = out["universe"]
        ok("universe counts", (u["scanned"], u["scanned_stock"], u["scanned_etf"], u["ranked"]["stock"]["hv21"], u["ranked"]["etf"]["sp_q"]) == (7, 5, 2, 5, 2))
        rows = [dict(zip(out["cols"], r)) for r in out["rows"]]
        st = [r for r in rows if r["kind"] == "stock"]
        ok("ranks are a permutation inside each kind", sorted(r["rk_hv21"] for r in st) == [1, 2, 3, 4, 5] and sorted(r["rk_q"] for r in rows if r["kind"] == "etf") == [1, 2])
        top = min(st, key=lambda r: r["rk_hv21"])
        ok("rank 1 holds the largest hv21", top["hv21"] == max(r["hv21"] for r in st))
        ok("market medians are the rows' medians", out["market"]["stock"]["med"]["hv21"] == round(statistics.median(r["hv21"] for r in st), 2)
           and out["market"]["stock"]["med"]["sp_w"] == round(statistics.median(r["sp_w"] for r in st), 2) and out["market"]["stock"]["n"] == 5)
        ok("bands add up to the ranked stocks", sum(b["n"] for b in out["bands"]) == 5 and round(sum(b["mcap"] for b in out["bands"])) == 5e11)
        ok("sector and size tables", [g["sector"] for g in out["by_sector"]] == ["Technology"] and out["by_sector"][0]["n"] == 5 and sum(g["n"] for g in out["by_size"]) == 5)
        h = out["history"]
        ok("history: 252 sessions ending on the bank's last", len(h) == 252 and h[-1]["date"] == LAST and h[-1]["n"] == 5 and h[-1]["med"] == out["market"]["stock"]["med"]["hv21"]
           and h[-1]["sp_d"] == out["market"]["stock"]["med"]["sp_d"] and all(x["p25"] <= x["med"] <= x["p75"] for x in h))
        ok("boards name ranked $1B+ stocks", set(out["boards"]["most_volatile"]) == {r["t"] for r in st} and out["boards"]["most_volatile"][0] == top["t"] and out["boards"]["least_volatile"][0] == max(st, key=lambda r: r["rk_hv21"])["t"])
        ok("benchmark is SPY's own row", out["market"]["benchmark"]["t"] == "SPY" and out["market"]["benchmark"]["hv21"] == row_of(out, "SPY")["hv21"])
        ok("first publication has no previous one", out["changes"] == {"prev_as_of": None})
        ok("the artifact is JSON", json.loads(json.dumps(out))["rows"][0][0] == out["rows"][0][0])
        nv = row_of(out, "NVDA")
        _, _, H, L, C, _ = cols("NVDA")
        ok("NVDA through build() equals the direct figures", nv["hv21"] == round(M.hv(C, 21), 2) and nv["sp_q"] == round((max(H[-63:]) / min(L[-63:]) - 1) * 100, 2) and nv["name"] == "NVDA Corp")

        LF._s3 = lambda: s3
        ctx = SimpleNamespace(get_remaining_time_in_millis=lambda: 600000)
        res = LF.lambda_handler({"trigger": "test"}, ctx)
        ok("handler: one write, the artifact", s3.puts == [LF.OUT_KEY] and res["statusCode"] == 200 and res["scanned"] == 7 and res["stocks"] == 5 and BEGUN[-1] == {"trigger": "test"})
        doc = json.loads(s3.store[LF.OUT_KEY])
        ok("handler stamps the trigger", doc["trigger"] == {"kind": "test"} and doc["contract"] == "stock-volatility.v1")
        prev = dict(doc)
        prev["as_of"], prev["boards"] = "2026-11-17", dict(doc["boards"], most_volatile=["ZZZ"] + doc["boards"]["most_volatile"][1:])
        s3.put_raw(LF.OUT_KEY, prev)
        out2 = LF.build(s3, now, log=lambda *_: None)
        ok("changes against the previous session", out2["changes"]["prev_as_of"] == "2026-11-17" and out2["changes"]["most_volatile"] == {"new": [doc["boards"]["most_volatile"][0]], "gone": ["ZZZ"]}
           and out2["changes"]["least_volatile"] == {"new": [], "gone": []})
    finally:
        LF.MIN_SCANNED, LF._s3 = old, old_s3


def test_an_unadjusted_split_is_rebased_first():
    """The bank can hold the sessions before a split at the old price. NVDA's real bars, with everything before a
    session 30 back multiplied by 10 and its volume divided by 10 (a 10-for-1 the bank did not adjust), must measure
    exactly as the real bars do once the recorded split is applied."""
    old = LF.MIN_SCANNED
    LF.MIN_SCANNED = 5
    try:
        cut = len(BARS["NVDA"]) - 30
        day = iso(BARS["NVDA"][cut][0])

        def edit(t, rows):
            if t != "NVDA":
                return rows
            return [[r[0], r[1] * 10, r[2] * 10, r[3] * 10, r[4] * 10, r[5] / 10] if i < cut else r for i, r in enumerate(rows)]
        s3 = fresh(edit=edit)
        s3.put_raw(LF.BANK_SPLITS_ROOT + day[:7] + ".json.gz", [{"ticker": "NVDA", "execution_date": day, "split_from": 1, "split_to": 10}], gz=True)
        out = LF.build(s3, datetime(2026, 11, 19, tzinfo=timezone.utc), log=lambda *_: None)
        nv, _, = row_of(out, "NVDA"), None
        _, _, H, L, C, _ = cols("NVDA")
        ok("a recorded split is rebased before volatility is read", near(nv["hv63"], round(M.hv(C, 63), 2), 1e-3) and nv["jd"] is None and nv["aside"] is None, "%s vs %s (%s)" % (nv["hv63"], M.hv(C, 63), nv["jd"]))
        ok("... and before the quarter's spread", near(nv["sp_q"], round((max(H[-63:]) / min(L[-63:]) - 1) * 100, 2), 1e-3))
    finally:
        LF.MIN_SCANNED = old


def test_an_unexplained_jump_is_flagged_and_set_aside():
    """A one-session move of 3.7-to-1 with no change in the shares traded is not a split any record or inference explains."""
    old = LF.MIN_SCANNED
    LF.MIN_SCANNED = 5
    try:
        cut = len(BARS["PFE"]) - 10

        def edit(t, rows):
            if t != "PFE":
                return rows
            return [[r[0], r[1] * 3.7, r[2] * 3.7, r[3] * 3.7, r[4] * 3.7, r[5]] if i < cut else r for i, r in enumerate(rows)]
        out = LF.build(fresh(edit=edit), datetime(2026, 11, 19, tzinfo=timezone.utc), log=lambda *_: None)
        p = row_of(out, "PFE")
        ok("the jump is on the row", p["jd"] == iso(BARS["PFE"][cut][0]) and p["ja"] == 9 and near(p["jr"], BARS["PFE"][cut][4] / (BARS["PFE"][cut - 1][4] * 3.7), 1e-3), json.dumps({k: p[k] for k in ("jd", "jr", "ja", "aside")}))
        ok("windows that span it are set aside, the figure still prints", p["aside"] == "hv21,hv63,sp_m,sp_q" and p["hv21"] > 300 and p["rk_hv21"] is None and p["rk_m"] is None)
        ok("windows after it are ranked", p["rk_d"] is not None and p["rk_w"] is not None)
        ok("it is counted, and kept out of boards and history", out["universe"]["flagged_jump"]["stock"] == 1 and out["universe"]["set_aside"]["stock"] == 1 and "PFE" not in out["boards"]["most_volatile"]
           and "PFE" in out["boards"]["widest_d"] and out["history"][-1]["n"] == 4 and out["universe"]["ranked"]["stock"]["hv21"] == 4)
        ok("the market's median skips it", out["market"]["stock"]["med"]["hv21"] < 200)
    finally:
        LF.MIN_SCANNED = old


def test_a_thin_symbol_is_set_aside():
    old = LF.MIN_SCANNED
    LF.MIN_SCANNED = 5
    try:
        drop = {len(BARS["XOM"]) - k for k in (5, 12, 20, 33, 41)}

        def edit(t, rows):
            return [r for i, r in enumerate(rows) if i not in drop] if t == "XOM" else rows
        out = LF.build(fresh(edit=edit), datetime(2026, 11, 19, tzinfo=timezone.utc), log=lambda *_: None)
        x = row_of(out, "XOM")
        ok("five missed sessions of 63: thin, every ranked measure aside", x["miss"] == 5 and x["aside"] == "hv21,hv63,sp_d,sp_w,sp_m,sp_q" and x["rk_d"] is None and x["hv21"] is not None)
        ok("thin rows are counted", out["universe"]["thin"]["stock"] == 1 and out["universe"]["ranked"]["stock"]["sp_d"] == 4)
    finally:
        LF.MIN_SCANNED = old


def test_nothing_partial_is_published():
    old = LF.MIN_SCANNED
    try:
        s3 = fresh()
        for name, fn in (("a short read", lambda: LF.build(s3, datetime(2026, 11, 19, tzinfo=timezone.utc), log=lambda *_: None)),):
            try:
                fn()
                raise AssertionError(name + " was published")
            except RuntimeError as e:
                ok(name + " raises", "symbols" in str(e))
        LF.MIN_SCANNED = 5
        real_time, ticks = LF.time, iter(range(0, 10 ** 9, 400))
        LF.time = SimpleNamespace(time=lambda: next(ticks))                 # a clock that jumps 400 seconds a reading: the deadline passes mid-read
        try:
            LF.build(s3, datetime(2026, 11, 19, tzinfo=timezone.utc), log=lambda *_: None, remaining_s=600.0)
            raise AssertionError("a timed-out read was published")
        except RuntimeError as e:
            ok("out of time raises", "ran out of time" in str(e) and "partial table is not published" in str(e), str(e))
        finally:
            LF.time = real_time
        s3b = fresh()
        s3b.put_raw(LF.polygon_bankfirst.MANIFEST, {"last_session": "2026-11-19", "first_session": "2021-08-26"})
        try:
            LF.build(s3b, datetime(2026, 11, 20, tzinfo=timezone.utc), log=lambda *_: None)
            raise AssertionError("a mid-fold bank was read")
        except RuntimeError as e:
            ok("a bank mid-fold raises", "mid-fold" in str(e))
        ok("none of them wrote", s3.puts == [] and s3b.puts == [])
        BOOK[:] = []
        try:
            LF.build(s3, datetime(2026, 11, 19, tzinfo=timezone.utc), log=lambda *_: None)
            raise AssertionError("an empty book was read")
        except RuntimeError as e:
            ok("an unreadable factor book raises", "factor book" in str(e))
    finally:
        LF.MIN_SCANNED = old


def test_stale_and_short_symbols_are_not_measured():
    old = LF.MIN_SCANNED
    LF.MIN_SCANNED = 4
    try:
        def edit(t, rows):
            return rows[:-6] if t == "LULU" else rows[-15:] if t == "TSLA" else rows
        out = LF.build(fresh(edit=edit), datetime(2026, 11, 19, tzinfo=timezone.utc), log=lambda *_: None)
        u = out["universe"]
        ok("a symbol with no bar in the last 3 sessions and one with 15 sessions are left out, and counted", (u["scanned"], u["not_trading"], u["short_history"]) == (5, 1, 1)
           and not any(r[0] in ("LULU", "TSLA") for r in out["rows"]))
    finally:
        LF.MIN_SCANNED = old


def main():
    tests = [v for k, v in sorted(globals().items()) if k.startswith("test_") and callable(v)]
    for t in tests:
        t()
    print("OK %d checks in %d tests" % (len(PASSED), len(tests)))


if __name__ == "__main__":
    main()
