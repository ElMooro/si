"""justhodl-campaign-scan offline tests.  An in-memory S3 holds a small Polygon EOD bank built from REAL banked bars
(tests/fixtures/campaign-scan-bars.json, six tickers read by ops 6842), the bank manifest, a split reference window and
a bottom-desk board; the factor book is faked as rows.

Checked: the port reproduces what the browser's own files mark on those bars (tests/fixtures/campaign-scan-marks.json,
written from the browser files and kept current by tests/campaign-scan-parity.test.js); the universe rule; each board
row against the bars it was read from; the bottom desk's dates merged the way the chart merges them; a split the bank
still holds unadjusted is rebased before the scan; build() and the handler; a partial scan is never published.
No network, no AWS.

Run: python3 aws/lambdas/justhodl-campaign-scan/tests/run_tests.py
"""
from datetime import datetime, timezone
from types import SimpleNamespace
import gzip
import hashlib
import importlib.util
import io
import json
import os
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

import chart_scans as S          # noqa: E402
import lambda_function as LF     # noqa: E402

BARS = json.load(open(os.path.join(ROOT, "tests", "fixtures", "campaign-scan-bars.json")))["bars"]
MARKS = json.load(open(os.path.join(ROOT, "tests", "fixtures", "campaign-scan-marks.json")))["marks"]
LAST = "2026-11-18"
PASSED = []


def ok(name, cond, detail=""):
    if not cond:
        raise AssertionError("%s %s" % (name, detail))
    PASSED.append(name)


def iso(ms):
    return datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc).date().isoformat()


def arrays(rows):
    return [iso(r[0]) for r in rows], [r[1] for r in rows], [r[2] for r in rows], [r[3] for r in rows], [r[4] for r in rows], [r[5] for r in rows]


def dict_bars(rows):
    return [{"date": iso(r[0]), "o": r[1], "h": r[2], "l": r[3], "c": r[4], "v": r[5]} for r in rows]


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


def bank(s3, tickers=None, last=LAST):
    for t, rows in BARS.items():
        if tickers is None or t in tickers:
            s3.put_raw(LF.bank_key(t), {"rows": [r + [None, None] for r in rows]}, gz=True)
    s3.put_raw(LF.bank_key("SPY"), {"rows": [r + [None, None] for r in BARS["QQQ"]]}, gz=True)     # the session calendar: any full series
    s3.put_raw(LF.polygon_bankfirst.MANIFEST, {"last_session": last, "first_session": "2021-08-26"})


# ------------------------------------------------------------------------------------------------- the port is the chart
def test_port_marks_what_the_browser_marks():
    for t, rows in BARS.items():
        T, O, H, L, C, V = arrays(rows)
        st, sc, ds, camp, dist = S.scan(T, O, H, L, C, V)
        ok("campaign marks " + t, [[m[0], m[1]] for m in camp] == MARKS[t]["campaign"], "%s vs %s" % ([[m[0], m[1]] for m in camp][-3:], MARKS[t]["campaign"][-3:]))
        ok("distribution marks " + t, [[m[0], m[1]] for m in dist] == MARKS[t]["distribution"])
    ok("fixture carries every kind of mark", {m[1] for t in MARKS for k in MARKS[t] for m in MARKS[t][k]} == {"CLIMAX", "BOTTOM", "ACCUM", "DIST", "D-TOP"})


def test_port_floors():
    T, O, H, L, C, V = arrays(BARS["NVDA"][:279])
    ok("distribution needs 280 bars", S.distribution_scan(T, O, H, L, C, V) == [])
    ok("structure needs 300 bars", S.structure_scan(*arrays(BARS["NVDA"][:299])) == [])
    ok("climax needs 90 bars", S.selling_climax_scan(*arrays(BARS["NVDA"][:89])) == [])
    ok("median is the browser's (positive values, element n // 2)", S._median_pos([3, 0, None, 1, 2, float("nan")]) == 2 and S._median_pos([]) == 0)


def test_tape_module_is_the_leaders_copy():
    a = open(os.path.join(SRC, "leaders_tape.py"), "rb").read()
    b = open(os.path.join(ROOT, "aws", "lambdas", "justhodl-leaders", "source", "leaders_tape.py"), "rb").read()
    ok("leaders_tape.py is byte-identical to the leaders engine's", hashlib.sha256(a).hexdigest() == hashlib.sha256(b).hexdigest())


# ---------------------------------------------------------------------------------------------------------- the universe
def test_universe_rule():
    rows = [book_row("AAA"), book_row("BBB", is_otc=True), book_row("CCC", is_common=False, is_preferred=True), book_row("DDD", dollar_vol_20d=100000.0),
            book_row("EEE", is_common=False, is_etf=True, name="Plain Sector ETF"), book_row("FFF", is_common=False, is_etf=True, name="Daily Semiconductor Bull 3X Shares"),
            book_row("GGG", is_common=False, is_etf=True, name="ProShares UltraPro QQQ"), book_row("HHH-WT"), book_row("III", is_common=None), book_row("JJJ", dollar_vol_20d=None),
            book_row("BRK-B")]
    uni, stats = LF.select_universe(rows)
    ok("universe keeps the listed common stock and the plain ETF", sorted(uni) == ["AAA", "BRK.B", "EEE"], str(sorted(uni)))
    ok("kinds", uni["AAA"]["kind"] == "stock" and uni["EEE"]["kind"] == "etf")
    ok("reasons add up", sum(stats["left_out"].values()) + len(uni) == len(rows) and stats["left_out"]["geared or inverse fund"] == 2 and stats["left_out"]["otc"] == 1
       and stats["left_out"]["thin (under the dollar-volume floor)"] == 2 and stats["left_out"]["not a common stock"] == 3, json.dumps(stats))
    ok("a plain fund name is not read as geared", LF.kind_of(book_row("X", is_common=False, is_etf=True, name="Vanguard Total Bond Market ETF"))[0] == "etf")


# ------------------------------------------------------------------------------------------------------------- the rows
def test_rows_against_their_bars():
    for t in BARS:
        bars = LF.clean(dict_bars(BARS[t]))
        rows = LF.rows_of(meta(t), bars)
        n, C, L = len(bars), [b["c"] for b in bars], [b["l"] for b in bars]
        by_date = {b["date"]: i for i, b in enumerate(bars)}
        for board, r in rows.items():
            i = by_date[r["date"]]
            ok("%s %s ago" % (t, board), r["ago"] == n - 1 - i and r["last"] == round(C[-1], 4) and r["px"] == round(C[i], 4))
            ok("%s %s since" % (t, board), abs(r["since_pct"] - (C[-1] / C[i] - 1) * 100) < 0.006)
            ok("%s %s is a mark the browser prints on that date" % (t, board), [r["date"], r["mark"]] in MARKS[t]["campaign"] + MARKS[t]["distribution"])
            if board == "bottom":
                held = min(L[i + 1:], default=L[i]) >= L[i]
                ok("%s bottom status" % t, r["status"] == ("HELD" if held else "UNDERCUT" if C[-1] >= L[i] else "BROKEN") and r["low"] == round(L[i], 4))
                ok("%s bottom window" % t, r["ago"] <= (LF.CLIMAX_SESSIONS if r["mark"] == "CLIMAX" else LF.BOTTOM_SESSIONS))
            if board == "accumulation":
                ok("%s accumulation" % t, r["ago"] <= LF.ACCUM_SESSIONS and r["status"] in ("ABOVE", "BASE", "LOST") and r["base_sessions"] >= 8 and r["gain_from_low_pct"] >= 6)
            if board == "distribution":
                ok("%s distribution" % t, r["ago"] <= LF.DIST_SESSIONS and r["status"] == ("RECLAIMED" if C[-1] >= r["high"] else "BELOW" if C[-1] < C[i] else "RANGE")
                   and abs(r["below_high_pct"] - (C[-1] / r["high"] - 1) * 100) < 0.006 and by_date[r["high_date"]] < i)
    nv = LF.rows_of(meta("NVDA"), LF.clean(dict_bars(BARS["NVDA"])))
    ok("NVDA: a live climax and a distribution, as on the chart", nv["bottom"]["mark"] == "CLIMAX" and nv["bottom"]["date"] == "2026-11-12" and nv["distribution"]["date"] == "2026-09-18" and "accumulation" not in nv)
    ok("NVDA climax facts", nv["bottom"]["vol_x"] > 1.2 and nv["bottom"]["decline_pct"] <= -10 and nv["bottom"]["score"] >= 4 and nv["bottom"]["source"] == "scan")
    lu = LF.rows_of(meta("LULU"), LF.clean(dict_bars(BARS["LULU"])))
    ok("LULU: the confirmed low with its climax and reversal", lu["bottom"]["mark"] == "BOTTOM" and lu["bottom"]["date"] == "2026-09-17" and lu["bottom"]["climax_date"] == "2026-06-05" and lu["bottom"]["rev_up_date"] > "2026-09-17")
    pf = LF.rows_of(meta("PFE"), LF.clean(dict_bars(BARS["PFE"])))
    ok("PFE: a low that already ended its base says so", pf["bottom"]["mark"] == "BOTTOM" and pf["bottom"]["accum_date"] == "2026-08-05" and pf["distribution"]["why"] in ("more volume bought less", "the push barely cleared the last high"))


def test_windows_move_with_the_tape():
    bars = LF.clean(dict_bars(BARS["TSLA"]))
    cut = next(i for i, b in enumerate(bars) if b["date"] == "2026-09-01")
    ok("TSLA on 2026-08-31: the low of 2026-07-29 is 23 sessions old and cannot be read yet, so neither can its ACCUM", "accumulation" not in LF.rows_of(meta("TSLA"), bars[:cut]))
    cut = next(i for i, b in enumerate(bars) if b["date"] == "2026-10-15")
    early = LF.rows_of(meta("TSLA"), bars[:cut])
    ok("TSLA on 2026-10-14: the ACCUM of 2026-08-19 is on the board", early.get("accumulation", {}).get("date") == "2026-08-19" and early["accumulation"]["low_date"] == "2026-07-29"
       and early["accumulation"]["base_sessions"] == 15, json.dumps(early.get("accumulation")))
    now = LF.rows_of(meta("TSLA"), bars)
    ok("TSLA today: that ACCUM is older than the window; the live climax is listed", "accumulation" not in now and now["bottom"]["mark"] == "CLIMAX" and now["bottom"]["prior_low_date"] == "2026-07-29")


def test_harvest_dates_merge_like_the_chart():
    bars = LF.clean(dict_bars(BARS["XOM"]))
    dates = [b["date"] for b in bars]
    ok("snap: an exact date, a weekend, a date too far", LF.snap(dates, "2026-11-10") == dates.index("2026-11-10") and dates[LF.snap(dates, "2026-11-08")] in ("2026-11-06", "2026-11-09")
       and LF.snap(dates, "2031-01-01") is None and LF.snap(dates, None) is None and LF.snap(dates, "junk") is None)
    plain = LF.rows_of(meta("XOM"), bars)
    hv = {"ticker": "XOM", "state": "TRIGGERED", "grade": "A", "frame": "D", "sc_date": "2026-10-13", "st_date": "2026-10-27", "trigger_date": "2026-11-10", "sc_decline_pct": -21.0, "sc_vol_x": 3.1, "sc_range_x": 2.2}
    rows = LF.rows_of(meta("XOM"), bars, hv)
    ok("the scan alone lists no bottom or accumulation for XOM", "bottom" not in plain and "accumulation" not in plain)
    ok("the desk's secondary test is the BOTTOM mark", rows["bottom"]["mark"] == "BOTTOM" and rows["bottom"]["date"] == "2026-10-27" and rows["bottom"]["source"] == "harvest"
       and rows["bottom"]["climax_date"] == "2026-10-13" and rows["bottom"]["harvest_state"] == "TRIGGERED" and rows["bottom"]["decline_pct"] == -21.0)
    acc = rows["accumulation"]
    lows = {d: b["l"] for d, b in zip(dates, bars)}
    ok("the desk's trigger is the ACCUM mark, on the lower of its climax and its test", acc["date"] == "2026-11-10" and acc["source"] == "harvest"
       and acc["low_date"] == min(("2026-10-13", "2026-10-27"), key=lambda d: lows[d]) and acc["low"] == round(lows[acc["low_date"]], 4))
    old = LF.rows_of(meta("XOM"), bars, dict(hv, sc_date="2025-01-10", st_date="2025-02-03", trigger_date="2025-02-20"))
    ok("desk dates outside the windows list nothing", "bottom" not in old and "accumulation" not in old)
    ok("merge: the later bar, both inside five sessions", LF._merge(10, 12) == (12, "both") and LF._merge(10, 40) == (40, "harvest") and LF._merge(40, 10) == (40, "scan")
       and LF._merge(None, 3) == (3, "harvest") and LF._merge(3, None) == (3, "scan") and LF._merge(None, None) == (None, None))
    ok("the board is indexed like the chart: first row of a ticker", LF.harvest_index({"board": [{"ticker": "A", "frame": "W"}, {"ticker": "A", "frame": "D"}, {"x": 1}]}) == {"A": {"ticker": "A", "frame": "W"}})


def test_unadjusted_split_is_rebased():
    rows = BARS["PFE"]
    cut = next(i for i, r in enumerate(rows) if iso(r[0]) == "2026-11-02")
    raw = [[r[0], r[1] * 2, r[2] * 2, r[3] * 2, r[4] * 2, r[5] / 2] if i < cut else list(r) for i, r in enumerate(rows)]      # the bank before a 2-for-1 it has not rebased
    s3 = FakeS3()
    s3.put_raw(LF.bank_key("PFE"), {"rows": [r + [None, None] for r in raw]}, gz=True)
    order = {iso(r[0]): i for i, r in enumerate(rows)}
    order["__last__"] = len(rows) - 1
    rec = [{"date": "2026-11-02", "split_from": 1, "split_to": 2}]
    st, with_split, n = LF.one_ticker(s3, meta("PFE"), rec, None, order, [], lambda t: None)
    truth = LF.rows_of(meta("PFE"), LF.clean(dict_bars(rows)))
    ok("recorded split rebased: the same rows as the adjusted tape", st == "scanned" and n == len(rows) and json.dumps(with_split, sort_keys=True) == json.dumps(truth, sort_keys=True),
       json.dumps(with_split.get("bottom"))[:300])
    st2, inferred, _ = LF.one_ticker(s3, meta("PFE"), None, None, order, [], lambda t: None)
    ok("an unrecorded split is inferred from price and shares", json.dumps(inferred, sort_keys=True) == json.dumps(truth, sort_keys=True))
    broken = LF.rows_of(meta("PFE"), LF.clean([{"date": iso(r[0]), "o": r[1], "h": r[2], "l": r[3], "c": r[4], "v": r[5]} for r in raw]))
    ok("without the rebase the confirmed low would read as broken, at twice its price", truth["bottom"]["status"] == "HELD" and broken["bottom"]["status"] == "BROKEN"
       and abs(broken["bottom"]["low"] - truth["bottom"]["low"] * 2) < 0.001, json.dumps(broken.get("bottom"))[:200])


def test_unexplained_jump_is_flagged_not_hidden():
    rows = BARS["NVDA"]
    order = {iso(r[0]): i for i, r in enumerate(rows)}
    order["__last__"] = len(rows) - 1
    s3 = FakeS3()
    s3.put_raw(LF.bank_key("NVDA"), {"rows": [r + [None, None] for r in rows]}, gz=True)
    _, clean_rows, _ = LF.one_ticker(s3, meta("NVDA"), None, None, order, [], lambda t: None)
    ok("a tape without such a move carries no flag", clean_rows and all("jump" not in r for r in clean_rows.values()))
    cut = len(rows) - 6
    odd = [list(r) if i < cut else [r[0], r[1] * 43.3, r[2] * 43.3, r[3] * 43.3, r[4] * 43.3, r[5]] for i, r in enumerate(rows)]     # x43.3 with the same shares traded: no split ratio, no volume signature
    s3.put_raw(LF.bank_key("NVDA"), {"rows": [r + [None, None] for r in odd]}, gz=True)
    st, flagged, _ = LF.one_ticker(s3, meta("NVDA"), None, None, order, [], lambda t: None)
    day = iso(rows[cut][0])
    ok("a x43 session no split explains stays on the board with the flag", st == "scanned" and flagged and all(r.get("jump") == {"date": day, "ratio": r["jump"]["ratio"]} and 40 < r["jump"]["ratio"] < 47 for r in flagged.values()),      # x43.3 times that session's own move
       json.dumps({k: v.get("jump") for k, v in flagged.items()}))
    ok("the flag rule", LF.flagged_jump([{"date": "2026-01-02", "ratio": 2.4, "ago": 5}]) is None and LF.flagged_jump([{"date": "2026-01-02", "ratio": 0.3, "ago": 5}]) == {"date": "2026-01-02", "ratio": 0.3}
       and LF.flagged_jump([{"date": "2025-01-02", "ratio": 9.0, "ago": 301}]) is None and LF.flagged_jump([]) is None and LF.flagged_jump(None) is None)


def test_one_ticker_gates():
    s3 = FakeS3()
    bank(s3)
    order = {iso(r[0]): i for i, r in enumerate(BARS["QQQ"])}
    order["__last__"] = len(BARS["QQQ"]) - 1
    ok("unbanked", LF.one_ticker(s3, meta("ZZZZ"), None, None, order, [], None)[0] == "unbanked")
    s3.put_raw(LF.bank_key("SHRT"), {"rows": [r + [None, None] for r in BARS["NVDA"][-200:]]}, gz=True)
    ok("short history", LF.one_ticker(s3, meta("SHRT"), None, None, order, [], None)[:1] == ("short",))
    s3.put_raw(LF.bank_key("GONE"), {"rows": [r + [None, None] for r in BARS["NVDA"][:-10]]}, gz=True)
    ok("a name that stopped trading is not scanned", LF.one_ticker(s3, meta("GONE"), None, None, order, [], None)[0] == "stale")
    s3.put_raw(LF.bank_key("JUNK"), {"rows": [[r[0], 0, 0, 0, 0, 0, None, None] for r in BARS["NVDA"]]}, gz=True)
    ok("bars with no price are unreadable", LF.one_ticker(s3, meta("JUNK"), None, None, order, [], None)[0] == "unreadable")
    ok("clean drops a bar with a zero or inverted price", len(LF.clean([{"date": "2026-01-02", "o": 1, "h": 2, "l": 1, "c": 1.5, "v": None}, {"date": "2026-01-05", "o": 1, "h": 0.5, "l": 1, "c": 1, "v": 5},
                                                                           {"date": "2026-01-06", "o": 1, "h": 2, "l": 0, "c": 1, "v": 5}, {"date": "2026-01-07", "h": 2, "l": 1, "c": 1, "v": 5}])) == 1)


# -------------------------------------------------------------------------------------------------------------- build()
def prepared(extra_book=()):
    s3 = FakeS3()
    bank(s3)
    BOOK[:] = [book_row("NVDA", mcap=3.9e12), book_row("PFE", sector="Healthcare", mcap=1.6e11), book_row("LULU", sector="Consumer Cyclical", mcap=1.3e10), book_row("XOM", sector="Energy", mcap=4.6e11),
               book_row("TSLA", sector="Consumer Cyclical", mcap=1.2e12), book_row("QQQ", is_common=False, is_etf=True, name="Invesco QQQ Trust", sector=None, mcap=3.2e11)] + list(extra_book)
    s3.put_raw(LF.BOTTOM_KEY, {"board": [{"ticker": "XOM", "state": "TRIGGERED", "grade": "A", "frame": "D", "sc_date": "2026-10-13", "st_date": "2026-10-27", "trigger_date": "2026-11-10"}]})
    return s3


def test_build_and_handler():
    old = LF.MIN_SCANNED
    LF.MIN_SCANNED = 5
    try:
        s3 = prepared([book_row("OTCX", is_otc=True), book_row("THIN", dollar_vol_20d=10.0), book_row("NOBK")])
        out = LF.build(s3, datetime(2026, 11, 19, 4, 2, tzinfo=timezone.utc), log=lambda m: None)
        u, c, b = out["universe"], out["counts"], out["boards"]
        ok("as of the bank's last session", out["as_of"] == LAST and out["contract"] == "campaign-scan.v1" and out["engine"] == "justhodl-campaign-scan")
        ok("universe counts", u["eligible"] == 7 and u["scanned"] == 6 and u["scanned_stock"] == 5 and u["scanned_etf"] == 1 and u["unbanked"] == 1 and u["left_out"] == {"otc": 1, "thin (under the dollar-volume floor)": 1}, json.dumps(u))
        for name in LF.BOARDS:
            rows = b[name]
            ok(name + " counts add up", c[name]["n"] == len(rows) == c[name]["stock"]["n"] + c[name]["etf"]["n"] and sum(c[name]["stock"]["by_status"].values()) == c[name]["stock"]["n"])
            ok(name + " one row a ticker, newest first", len({r["ticker"] for r in rows}) == len(rows) and [r["ago"] for r in rows] == sorted(r["ago"] for r in rows))
            ok(name + " prints add up to the board", sum((p["climax"] + p["bottom"]) if name == "bottom" else p[name] for p in out["prints"]) == len(rows))
            ok(name + " flags are counted", c[name]["stock"]["flagged_jump"] == len([r for r in rows if r["kind"] == "stock" and r.get("jump")]))
            ok(name + " sector table adds up", sum(v[name] for v in out["by_sector"].values()) == c[name]["stock"]["n"])
        ok("sector table counts every scanned stock", sum(v["scanned"] for v in out["by_sector"].values()) == 5)
        ok("the ETF is on its boards as an etf", {r["ticker"]: r["kind"] for r in b["bottom"]}.get("QQQ") == "etf" and any(r["ticker"] == "QQQ" for r in b["distribution"]))
        ok("the bottom desk's dates reached the boards", any(r["ticker"] == "XOM" and r["source"] == "harvest" for r in b["accumulation"]) and u["harvest_rows"] == 1)
        ok("first print has no prior", out["changes"] == {"prev_as_of": None} and out["n_errors"] == 0)
        ok("market cap of a board is the sum of its rows", c["bottom"]["stock"]["mcap"] == sum(r["mcap"] for r in b["bottom"] if r["kind"] == "stock"))
    finally:
        LF.MIN_SCANNED = old


def test_handler_publishes_and_changes_follow():
    old, old_s3 = LF.MIN_SCANNED, LF._s3
    LF.MIN_SCANNED = 5
    try:
        s3 = prepared()
        LF._s3 = lambda: s3
        n0 = len(BEGUN)
        res = LF.lambda_handler({"trigger": "first-light"}, SimpleNamespace(get_remaining_time_in_millis=lambda: 600000))
        doc = json.loads(s3.store[LF.OUT_KEY])
        ok("handler writes the feed once and passes the trigger cap", res["statusCode"] == 200 and s3.puts == [LF.OUT_KEY] and len(BEGUN) == n0 + 1 and doc["trigger"] == {"kind": "first-light"}
           and res["bottom"] == doc["counts"]["bottom"]["n"] and res["scanned"] == 6)
        again = LF.build(s3, datetime(2026, 11, 19, 9, 0, tzinfo=timezone.utc), log=lambda m: None)
        ok("the same session keeps the change record", again["changes"] == doc["changes"])
        prev = json.loads(s3.store[LF.OUT_KEY])
        prev["as_of"] = "2026-11-17"
        prev["boards"]["bottom"] = [r for r in prev["boards"]["bottom"] if r["ticker"] != "NVDA"] + [dict(prev["boards"]["bottom"][0], ticker="OLDX")]
        s3.store[LF.OUT_KEY] = json.dumps(prev).encode()
        nxt = LF.build(s3, datetime(2026, 11, 19, 9, 0, tzinfo=timezone.utc), log=lambda m: None)
        ok("a new session reports who joined and who left", nxt["changes"]["prev_as_of"] == "2026-11-17" and nxt["changes"]["bottom"]["new"] == ["NVDA"] and nxt["changes"]["bottom"]["gone"] == ["OLDX"]
           and nxt["changes"]["distribution"] == {"new": [], "gone": [], "moved": []}, json.dumps(nxt["changes"]))
    finally:
        LF.MIN_SCANNED, LF._s3 = old, old_s3


def test_nothing_partial_is_published():
    s3 = prepared()
    try:
        LF.build(s3, datetime(2026, 11, 19, tzinfo=timezone.utc), log=lambda m: None)
        ok("a short universe raises", False)
    except RuntimeError as exc:
        ok("a short universe raises", "previous publication stands" in str(exc))
    old = LF.MIN_SCANNED
    LF.MIN_SCANNED = 5
    try:
        real_time, ticks = LF.time, iter(range(0, 10 ** 9, 400))
        LF.time = SimpleNamespace(time=lambda: next(ticks))                 # a clock that jumps 400 seconds a reading: the deadline passes mid-scan
        try:
            LF.build(s3, datetime(2026, 11, 19, tzinfo=timezone.utc), log=lambda m: None, remaining_s=600.0)
            ok("out of time raises", False)
        except RuntimeError as exc:
            ok("out of time raises", "partial board is not published" in str(exc), str(exc))
        finally:
            LF.time = real_time
        s3.put_raw(LF.polygon_bankfirst.MANIFEST, {"last_session": "2026-11-19", "first_session": "2021-08-26"})
        try:
            LF.build(s3, datetime(2026, 11, 20, tzinfo=timezone.utc), log=lambda m: None)
            ok("a bank mid-fold raises", False)
        except RuntimeError as exc:
            ok("a bank mid-fold raises", "mid-fold" in str(exc))
        BOOK[:] = []
        try:
            LF.build(s3, datetime(2026, 11, 20, tzinfo=timezone.utc), log=lambda m: None)
            ok("no factor book raises", False)
        except RuntimeError as exc:
            ok("no factor book raises", "factor book" in str(exc))
        ok("nothing was written by a failed build", s3.puts == [])
    finally:
        LF.MIN_SCANNED = old


def test_contract_text():
    cfg = json.load(open(os.path.join(HERE, "..", "config.json")))
    ok("config", len(cfg["description"]) <= 256 and cfg["timeout"] == 900 and cfg["env"]["OUT_KEY"] == "data/campaign-scan.json" and "release_validation" not in cfg
       and cfg["eventbridge_scheduler"]["cron"].startswith("cron(") and cfg["eventbridge_scheduler"]["schedule_name"] == "justhodl-campaign-scan-daily")
    src = open(os.path.join(SRC, "lambda_function.py")).read()
    ok("reads S3 only: no provider call, no invoke, no event", all(x not in src for x in ("urllib", "requests", "lambda_client", ".invoke(", "put_events", "http://", "https://")))
    ok("every mark is defined", set(LF.DEFINITIONS) >= {"CLIMAX", "BOTTOM", "ACCUM", "DIST", "D-TOP", "status", "source", "universe", "jump"} and len(LF.LIMITATIONS) >= 4)


if __name__ == "__main__":
    tests = [v for k, v in sorted(globals().items()) if k.startswith("test_") and callable(v)]
    for t in tests:
        t()
    print("justhodl-campaign-scan: %d checks in %d tests passed" % (len(PASSED), len(tests)))
