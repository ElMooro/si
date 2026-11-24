"""justhodl-regime-map offline tests.

An in-memory S3 holds a cut of the REAL warehouse, taken by ops 6936 from the very bytes one build of this engine read
on the runner (tests/fixtures/risk-map-warehouse.json.gz: the Polygon EOD bank's bars of 43 funds - the first map's 35
and eight more, with a 4-for-1 split the bank still holds unadjusted and a 1-for-2 reverse split - the split reference
rows, the forming-bar file, six coins, CoinMarketCap's listing, and seven FMP price-bank series). The same cut carries
what that build published for those instruments; the engine must reproduce it here, figure for figure.

Checked besides: the calendar windows, the score against the first engine's arithmetic, the two held states, the
row's own check, splits, the forming bar, coins, currencies turned over, late and stale instruments, the publication
guards, the handler, and that the engine asks no provider for anything. No network, no AWS.

Run: python3 aws/lambdas/justhodl-regime-map/tests/run_tests.py
"""
from datetime import date, datetime, timedelta, timezone
from types import SimpleNamespace
import gzip
import hashlib
import io
import json
import math
import os
import statistics
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
SRC = os.path.join(HERE, "..", "source")
ROOT = os.path.abspath(os.path.join(HERE, "..", "..", "..", ".."))
sys.path[:0] = [SRC]
if "boto3" not in sys.modules:
    try:
        import boto3  # noqa: F401
    except ImportError:
        sys.modules["boto3"] = SimpleNamespace(client=lambda *a, **k: None)

import lambda_function as L  # noqa: E402
import risk_math as M  # noqa: E402
import risk_universe as U  # noqa: E402

FIX = json.loads(gzip.decompress(open(os.path.join(ROOT, "tests", "fixtures", "risk-map-warehouse.json.gz"), "rb").read()))
NOW = datetime.fromisoformat(FIX["cut_at"])
TESTS = []


def test(fn):
    TESTS.append(fn)
    return fn


class FakeS3:
    def __init__(self, objects):
        self.objects, self.puts = dict(objects), {}

    def get_object(self, Bucket, Key):
        if Key not in self.objects:
            raise KeyError("NoSuchKey: " + Key)
        return {"Body": io.BytesIO(self.objects[Key])}

    def list_objects_v2(self, **kw):
        return {"Contents": [{"Key": k} for k in sorted(self.objects) if k.startswith(kw.get("Prefix") or "")]}

    def put_object(self, Bucket, Key, Body, **kw):
        self.puts[Key] = Body
        return {}


def gz(obj):
    return gzip.compress(json.dumps(obj).encode())


def warehouse(change=None):
    """The fixture as S3 objects. `change(fix)` may edit a deep copy first."""
    fix = json.loads(json.dumps(FIX))
    if change:
        change(fix)
    objs = {}
    for t, rows in fix["bank"].items():
        objs[L.bank_key(t)] = gz({"rows": rows})
    for month, rows in fix["splits"].items():
        objs[L.SPLITS_ROOT + month + ".json.gz"] = gz(rows)
    if fix.get("today"):
        objs[L.TODAY_KEY] = gz(fix["today"])
    for c, doc in fix["coins"].items():
        objs[L.COIN_ROOT + c + ".json.gz"] = gz(doc)
    for sym, rows in fix["fmp"].items():
        objs[L.fmp_key(sym)] = gz(rows)
    if fix.get("cmc"):
        objs[L.CMC_ROOT + fix["cmc_day"] + ".json.gz"] = gz({"data": fix["cmc"]})
    return FakeS3(objs)


def cut_members():
    """The universe's members the cut holds bars for, in the universe's order."""
    return [m for m in ALL_MEMBERS if (m["kind"] == "fund" and m["sym"] in FIX["bank"]) or (m["kind"] != "fund" and m["sym"] in FIX["fmp"])]


ALL_MEMBERS = U.members()
U.members = cut_members            # every build below runs on the cut's instruments


def build(change=None, prev="fixture", now=NOW):
    s3 = warehouse(change)
    return L.build(s3, now=now, prev=FIX["prev"] if prev == "fixture" else prev, log=lambda *_: None), s3


DOC, _S3 = build()
BY = {t["t"]: t for t in DOC["map"]["tiles"]}


def first_engine_score(closes):
    """justhodl-regime-map 1.0.0's score(), as it was deployed until 2026-11-24."""
    last = closes[-1]

    def ret(n):
        return (last / closes[-1 - n] - 1) if len(closes) > n else 0.0
    r21, r63, r126 = ret(21), ret(63), ret(126)
    ma50 = sum(closes[-50:]) / 50
    ma200 = sum(closes[-200:]) / 200 if len(closes) >= 200 else sum(closes) / len(closes)
    pts = (1 if last > ma50 else 0) + (1 if last > ma200 else 0) + (1 if ma50 > ma200 else 0)
    trend = (pts - 1.5) / 1.5
    mom = 0.45 * r63 + 0.30 * r126 + 0.25 * r21
    return round(max(-100, min(100, mom * 100 * 2.2 + trend * 25))), r21 * 100, r63 * 100, r126 * 100, pts


def walk(n, step, start=date(2026, 11, 23), weekdays=True, px=100.0):
    days, d = [], start
    while len(days) < n:
        if not weekdays or d.weekday() < 5:
            days.append(d)
        d -= timedelta(days=1)
    out = []
    for i, dd in enumerate(reversed(days)):
        out.append((dd.isoformat(), px * step(i)))
    return out


# ------------------------------------------------------------------------------------------------------------ the math
@test
def calendar_windows():
    assert M.months_back(date(2026, 3, 31), 1) == date(2026, 2, 28) and M.months_back(date(2024, 3, 31), 1) == date(2024, 2, 29)
    assert M.months_back(date(2026, 1, 15), 3) == date(2025, 10, 15) and M.months_back(date(2026, 11, 24), 12) == date(2025, 11, 24)
    assert M.targets("2026-11-24") == {"w": "2026-11-17", "ytd": "2025-12-31", "m": "2026-10-24", "q": "2026-08-24", "h": "2026-05-24", "y": "2025-11-24"}
    fund = walk(400, lambda i: 1.0 + i / 1000.0)
    coin = walk(560, lambda i: 1.0 + i / 1000.0, weekdays=False)
    a, b = M.anchors(fund), M.anchors(coin)
    assert a["d"][0] == "2026-11-20" and b["d"][0] == "2026-11-22"                          # the bar before, whatever the week
    assert a["m"][0] == "2026-10-23" and b["m"][0] == "2026-10-23"                          # one month back is the same day for both
    assert a["q"][0] == "2026-08-21" and b["q"][0] == "2026-08-23"                          # a Sunday for the coin, the Friday before for the fund
    assert a["h"][0] == "2026-05-22" and b["h"][0] == "2026-05-23" and a["ytd"][0] == "2025-12-31" and b["ytd"][0] == "2025-12-31"
    assert a["q"][2] == 66 and b["q"][2] == 92, "bars since the anchor: %s, %s (the first engine took 63 for both)" % (a["q"][2], b["q"][2])
    assert "y" in a and M.anchors(fund[-200:]).keys() == {"d", "w", "m", "q", "h"}, "a window the history does not reach is absent"


@test
def a_hole_in_the_history_is_not_an_anchor():
    s = walk(400, lambda i: 100.0)
    holed = [p for p in s if not ("2026-07-20" <= p[0] <= "2026-08-24")]
    a = M.anchors(holed)
    assert "q" not in a and "m" in a and "h" in a, "the last close before a five-week hole is not 'three months ago'"
    row, why = M.measure(holed)
    assert row is None and "3 months" in why


@test
def changes_are_the_close_over_the_anchor():
    s = walk(400, lambda i: math.exp(0.002 * i + 0.01 * math.sin(i)))
    row, _ = M.measure(s)
    for k, (ad, ac, nb) in row["a"].items():
        assert abs(row["r"][k] - (row["c"] / ac - 1) * 100) < 0.006 and dict(s)[ad] == ac and s[-1 - nb][0] == ad
    assert row["p"] == s[-2][1] and row["c"] == s[-1][1] and M.check(row) == []


@test
def score_is_the_first_engines():
    for seed, drift in enumerate((0.004, 0.001, 0.0, -0.001, -0.004, 0.0007, -0.0023)):
        closes = [c for _, c in walk(300, lambda i, d=drift, s=seed: math.exp(d * i + 0.03 * math.sin(i / (3.0 + s))))]
        old, r21, r63, r126, pts = first_engine_score(closes)
        ma50, ma200 = M.averages(closes)
        assert M.trend_points(closes[-1], ma50, ma200) == pts
        assert M.score_of(r21, r63, r126, pts) == old, "given the same three returns and trend the score is the first engine's"
    assert M.score_of(100, 100, 100, 3) == 100 and M.score_of(-100, -100, -100, 0) == -100
    assert [M.state_of(v) for v in (45, 44, 15, 14, -14, -15, -44, -45)] == ["BOOMING", "RISK-ON", "RISK-ON", "NEUTRAL", "NEUTRAL", "RISK-OFF", "RISK-OFF", "DESTROYED"]


@test
def booming_and_destroyed_are_held_to_the_record():
    above, below = (110.0, 100.0, 95.0), (90.0, 100.0, 105.0)             # last, 50-day, 200-day
    assert M.held_state(60, 6.0, 12.0, *above) == ("BOOMING", None)
    assert M.held_state(60, 3.45, 1.12, *above)[0] == "RISK-ON", "up 1.1% in three months and 3.5% in one is not booming (IBB, ops 6936)"
    assert M.held_state(60, 5.0, 0.1, *above)[0] == "BOOMING" and M.held_state(60, 0.1, 5.0, *above)[0] == "BOOMING"
    for r1, r3, last, why in ((-0.1, 12.0, above, "month is down"), (6.0, -0.1, above, "three months are down"),
                              (6.0, 12.0, (99.0, 100.0, 95.0), "under its 50-day"), (6.0, 12.0, (101.0, 100.0, 102.0), "under its 200-day")):
        state, held = M.held_state(60, r1, r3, *last)
        assert state == "RISK-ON" and why in held and "+60" in held, (state, held)
    assert M.held_state(-60, -6.0, -12.0, *below) == ("DESTROYED", None)
    assert M.held_state(-60, -4.9, -4.9, *below)[0] == "RISK-OFF"
    for r1, r3, last, why in ((0.1, -12.0, below, "month is up"), (-6.0, 0.1, below, "three months are up"),
                              (-6.0, -12.0, (101.0, 100.0, 105.0), "above its 50-day"), (-6.0, -12.0, (99.0, 100.0, 98.0), "above its 200-day")):
        state, held = M.held_state(-60, r1, r3, *last)
        assert state == "RISK-OFF" and why in held, (state, held)
    for score in (44, 20, 0, -20, -44):
        assert M.held_state(score, -50.0, -50.0, *above) == (M.state_of(score), None), "only the two ends are held"


@test
def usual_move_is_the_standard_deviation_of_daily_log_changes():
    closes = [c for _, c in walk(400, lambda i: math.exp(0.02 * math.sin(i * 1.7) + 0.0005 * i))]
    rets = [math.log(closes[i] / closes[i - 1]) for i in range(len(closes) - 252, len(closes))]
    assert abs(M.sigma_daily(closes) - statistics.stdev(rets) * 100) < 1e-9
    assert M.sigma_daily(closes[:50]) is None
    sd = M.sigma_daily(closes)
    assert abs(M.unusual(10.0, sd, 63) - math.log(1.1) * 100 / (sd * math.sqrt(63))) < 1e-12 and M.unusual(None, sd, 63) is None and M.unusual(5.0, None, 63) is None


@test
def a_row_that_contradicts_itself_is_caught():
    row, _ = M.measure(walk(400, lambda i: math.exp(0.004 * i)))
    assert row["state"] == "BOOMING" and M.check(row) == []
    for edit, word in ((lambda r: r["r"].__setitem__("q", r["r"]["q"] + 1), "anchor"), (lambda r: r.__setitem__("state", "DESTROYED"), "destroyed"),
                       (lambda r: r["a"]["m"].__setitem__(0, r["d"]), "not before"), (lambda r: r.__setitem__("c", r["hi"] * 2), "52-week")):
        bad = json.loads(json.dumps(row))
        edit(bad)
        assert any(word in x for x in M.check(bad)), (word, M.check(bad))
    falling, _ = M.measure(walk(400, lambda i: math.exp(-0.004 * i)))
    lie = dict(falling, state="BOOMING")
    assert M.check(lie), "a falling series called booming does not pass"


# ---------------------------------------------------------------------------------------------- the build on real bars
@test
def the_cut_reproduces_what_the_runner_published():
    exp = FIX["expect"]
    assert DOC["regime"] == exp["regime"], (DOC["regime"], exp["regime"])
    assert [(x["ticker"], x["risk_on"], x["state"], x["r1m"], x["r3m"], x["r6m"]) for x in DOC["ranked"]] == [(x["ticker"], x["risk_on"], x["state"], x["r1m"], x["r3m"], x["r6m"]) for x in exp["ranked"]]
    assert [x["ticker"] for x in DOC["booming"]] == [x["ticker"] for x in exp["booming"]] and [x["ticker"] for x in DOC["destroyed"]] == [x["ticker"] for x in exp["destroyed"]]
    assert DOC["transitions"] == exp["transitions"]
    assert len(exp["tiles"]) >= 50
    for tk, want in exp["tiles"].items():
        got = BY.get(tk)
        assert got, "%s is missing from the build on the cut" % tk
        for k in ("score", "state", "c", "d", "p", "pd", "r", "a", "u", "sd", "v50", "v200", "off_hi", "off_lo", "k", "sym", "role"):
            assert got[k] == want[k], "%s %s: %r here, %r on the runner" % (tk, k, got[k], want[k])
        for k in ("held", "f", "splits", "jump", "inv", "chart"):          # `late` is relative to the kind's newest bar, so to the universe: tested on its own
            assert got.get(k) == want.get(k), "%s %s: %r here, %r on the runner" % (tk, k, got.get(k), want.get(k))
    assert len(DOC["ranked"]) == 38 and DOC["n_instruments"] == 38


@test
def what_the_first_engine_published_is_still_published():
    need = ["booming", "destroyed", "elapsed_s", "engine", "generated_at", "n_instruments", "n_transitions", "ok", "prev_regime", "ranked", "regime",
            "regime_changed", "sleeves", "thesis", "transitions", "version", "windows"]              # config/engine-contracts.json required_keys
    assert all(k in DOC for k in need) and DOC["ok"] is True and DOC["engine"] == "justhodl-regime-map" and DOC["version"] == "2.0.0"
    assert list(DOC["sleeves"]) == list(U.LEGACY) and sum(len(v) for v in DOC["sleeves"].values()) == 38
    for sleeve, rows in DOC["sleeves"].items():
        assert {x["ticker"] for x in rows} == {t for t, _ in U.LEGACY[sleeve]}, sleeve
        assert [x["risk_on"] for x in rows] == sorted((x["risk_on"] for x in rows), reverse=True)
        for x in rows:
            assert set(x) >= {"ticker", "name", "sleeve", "risk_on", "state", "r1m", "r3m", "r6m"} and x["sleeve"] == sleeve and x["name"] == dict(U.LEGACY[sleeve])[x["ticker"]]
            assert x["risk_on"] == BY[x["ticker"]]["score"] and x["state"] == BY[x["ticker"]]["state"], "one score and one state for an instrument, in both views"
    assert set(DOC["regime"]) == {"label", "summary", "concentration_spread_3m", "equity_breadth_pct", "equity_avg", "crypto_avg", "rates_avg", "commodities_avg", "intl_avg"}
    assert DOC["regime"]["label"] in ("BIFURCATED", "BROAD RISK-ON", "NARROW / CONCENTRATED", "BROAD RISK-OFF", "MIXED / ROTATIONAL")
    eq = [x["risk_on"] for s, rows in DOC["sleeves"].items() if s.startswith("Equity") for x in rows]
    assert DOC["regime"]["equity_avg"] == round(sum(eq) / len(eq), 1) and DOC["regime"]["equity_breadth_pct"] == round(100 * sum(1 for v in eq if v > 0) / len(eq))
    assert DOC["regime"]["crypto_avg"] == round(sum(BY[c]["score"] for c in ("BTC", "ETH", "SOL")) / 3, 1)
    spy, rsp = [next(x for x in DOC["ranked"] if x["ticker"] == t) for t in ("SPY", "RSP")]
    assert DOC["regime"]["concentration_spread_3m"] == round(spy["r3m"] - rsp["r3m"], 1)


@test
def the_lists_hold_only_what_is_in_that_state():
    m = DOC["map"]
    assert set(m["booming"]) == {t["t"] for t in m["tiles"] if t["state"] == "BOOMING"} and set(m["destroyed"]) == {t["t"] for t in m["tiles"] if t["state"] == "DESTROYED"}
    assert m["destroyed"], "the cut holds instruments in the state DESTROYED"
    for t in m["tiles"]:
        if t["state"] == "BOOMING":
            assert t["score"] >= 45 and t["r"]["m"] > 0 and t["r"]["q"] > 0 and max(t["r"]["m"], t["r"]["q"]) >= 5 and t["v50"] > 0 and t["v200"] > 0, t
        if t["state"] == "DESTROYED":
            assert t["score"] <= -45 and t["r"]["m"] < 0 and t["r"]["q"] < 0 and min(t["r"]["m"], t["r"]["q"]) <= -5 and t["v50"] < 0 and t["v200"] < 0, t
        if t.get("held"):
            assert t["state"] in ("RISK-ON", "RISK-OFF") and abs(t["score"]) >= 45
    assert all(x["state"] == "BOOMING" for x in DOC["booming"]) and all(x["state"] == "DESTROYED" for x in DOC["destroyed"])
    assert [x["ticker"] for x in DOC["strongest"]] == [x["ticker"] for x in DOC["ranked"][:6]] and [x["ticker"] for x in DOC["weakest"]] == [x["ticker"] for x in DOC["ranked"][::-1][:6]]
    scores = [BY[t]["score"] for t in m["destroyed"]]
    assert scores == sorted(scores), "most destroyed first"
    assert [BY[t]["score"] for t in m["strongest"]] == sorted((t["score"] for t in m["tiles"]), reverse=True)[:12]


@test
def a_split_the_bank_holds_unadjusted_is_rebased_first():
    raw = L.fund_bars({"rows": FIX["bank"]["QUAL"]})
    i = next(k for k, b in enumerate(raw) if b["date"] == "2026-11-05")
    assert 0.2 < raw[i]["c"] / raw[i - 1]["c"] < 0.3, "the bank's bars print the 4-for-1 as a 76%% fall: %.3f" % (raw[i]["c"] / raw[i - 1]["c"])
    naive, _ = M.measure([(b["date"], b["c"]) for b in raw])
    assert naive["r"]["q"] < -70, "read as they are, the bars say the fund lost three quarters in three months (%s)" % naive["r"]["q"]
    q = BY["QUAL"]
    assert q["splits"] == [{"date": "2026-11-05", "factor": 0.25, "source": "polygon-splits"}] and -15 < q["r"]["q"] < 10 and -15 < q["r"]["m"] < 10 and not q.get("jump")
    assert abs(q["a"]["q"][1] - dict((b["date"], b["c"]) for b in raw)[q["a"]["q"][0]] * 0.25) < 1e-6, "the anchor is the old close times the split's factor"
    v = BY["VIXM"]
    assert v["splits"][0]["factor"] == 2.0 and v["splits"][0]["date"] == "2026-11-19", "a 1-for-2 reverse split doubles the older closes"
    assert all(not t.get("jump") for t in DOC["map"]["tiles"] if t["k"] == "fund")


@test
def todays_forming_bar_is_the_last_bar_of_a_fund():
    session = FIX["today"]["session"]
    funds = [t for t in DOC["map"]["tiles"] if t["k"] == "fund"]
    formed = [t for t in funds if t.get("f")]
    assert len(formed) >= 40 and DOC["as_of"]["funds_forming"] == len(formed) and DOC["as_of"]["funds_forming_as_of"] == FIX["today"]["as_of"]
    for t in formed:
        assert t["d"] == session and t["c"] == L.sig(FIX["today"]["rows"][t["t"]][3]) and t["pd"] == DOC["as_of"]["bank_last_session"]
        assert t["a"]["d"][0] == t["pd"] and abs(t["r"]["d"] - (t["c"] / t["p"] - 1) * 100) < 0.006
    assert DOC["as_of"]["bank_last_session"] < session and DOC["as_of"]["fund"] == session

    def split_in_progress(fix):
        fix["today"]["rows"]["SPY"][3] = fix["today"]["rows"]["SPY"][3] / 4.0
        del fix["today"]["rows"]["XLK"]
    doc, _ = build(split_in_progress)
    by = {t["t"]: t for t in doc["map"]["tiles"]}
    assert not by["SPY"].get("f") and by["SPY"]["d"] == doc["as_of"]["bank_last_session"] and any("SPY" in n and "not used" in n for n in doc["notes"]), doc["notes"]
    assert not by["XLK"].get("f") and by["XLK"]["late"] == (date.fromisoformat(session) - date.fromisoformat(by["XLK"]["d"])).days, "a fund with no bar yet today is dated and marked late"
    doc, _ = build(lambda fix: fix.__setitem__("today", None))
    assert doc["as_of"]["funds_forming"] == 0 and doc["as_of"]["fund"] == doc["as_of"]["bank_last_session"] and all(not t.get("f") and not t.get("late") for t in doc["map"]["tiles"] if t["k"] == "fund")


@test
def dollar_volume_is_measured_and_a_thin_fund_is_left_out():
    spy = BY["SPY"]
    bars = [b for b in L.fund_bars({"rows": FIX["bank"]["SPY"]})][-20:]
    assert abs(spy["dv"] - round(sum(b["c"] * b["v"] for b in bars) / 20)) <= 1, "20 completed sessions; the forming bar's volume is not in it"

    def thin(fix):
        for row in fix["bank"]["KRE"]:
            row[5] = 10.0
    doc, _ = build(thin)
    assert "KRE" not in {t["t"] for t in doc["map"]["tiles"]} and any(x["t"] == "KRE" and "floor" in x["why"] for x in doc["map"]["left_out"])


@test
def coins_come_from_the_banked_bars_by_market_value():
    coins = [t for t in DOC["map"]["tiles"] if t["k"] == "coin"]
    assert [t["t"] for t in coins] == ["BTC", "ETH", "XRP", "SOL", "DOGE", "ADA"], "CoinMarketCap's order among the coins with banked bars; USDT, BNB, USDC, TRX are not banked here"
    assert all(t["cls"] == "crypto" and t["role"] == "risk" and t["sym"] == "X:%sUSD" % t["t"] and t["chart"] == t["t"] + "-USD" for t in coins)
    mc = {c["symbol"]: c["quote"]["USD"]["market_cap"] for c in FIX["cmc"]}
    assert all(t["mc"] == round(mc[t["t"]]) for t in coins) and coins[0]["n"] == "Bitcoin"
    last = FIX["coins"]["BTC"]["rows"][-1]
    assert BY["BTC"]["d"] == last[0] and BY["BTC"]["c"] == L.sig(last[4]) and DOC["as_of"]["coin"] == last[0] and not BY["BTC"].get("f")
    assert BY["BTC"]["a"]["h"][0] == M.months_back(date.fromisoformat(last[0]), 6).isoformat(), "six months is six calendar months for a coin too"
    listing = [{"symbol": "USDT", "tags": ["stablecoin"], "quote": {"USD": {"market_cap": 9e11}}}, {"symbol": "BTC", "name": "Bitcoin", "quote": {"USD": {"market_cap": 5.0}}},
               {"symbol": "ETH", "name": "Other Ether", "quote": {"USD": {"market_cap": 9.0}}}, {"symbol": "ETH", "name": "A namesake", "quote": {"USD": {"market_cap": 1.0}}},
               {"symbol": "WBTC", "tags": ["wrapped-tokens"], "quote": {"USD": {"market_cap": 7.0}}}, {"symbol": "NOPE", "quote": {"USD": {"market_cap": 3.0}}}]
    assert L.pick_coins(listing, {"USDT", "BTC", "ETH", "WBTC", "SOL"}) == [("BTC", "Bitcoin", 5.0), ("ETH", "Other Ether", 9.0), ("SOL", "SOL", None)]
    assert [s for s, _, _ in L.pick_coins(None, {"SOL", "BTC", "XRP"})] == ["BTC", "XRP", "SOL"], "no listing: a fixed order, no market value"
    doc, _ = build(lambda fix: fix.__setitem__("cmc", None))
    assert all("mc" not in t for t in doc["map"]["tiles"] if t["k"] == "coin") and {"BTC", "ETH", "SOL"} <= {t["t"] for t in doc["map"]["tiles"]}


@test
def a_currency_tile_is_the_currency_against_the_dollar():
    usdjpy = FIX["fmp"]["USDJPY"][-1]
    eur = FIX["fmp"]["EURUSD"][-1]
    assert BY["JPY"]["inv"] is True and abs(BY["JPY"]["c"] - 1.0 / usdjpy["close"]) < 1e-9 and BY["JPY"]["d"] == usdjpy["date"]
    assert "inv" not in BY["EUR"] and BY["EUR"]["c"] == L.sig(eur["close"]) and BY["EUR"]["d"] == eur["date"]
    rows = {r["date"]: r["close"] for r in FIX["fmp"]["USDJPY"]}
    ad, ac, _ = BY["JPY"]["a"]["q"]
    assert abs(BY["JPY"]["r"]["q"] - (rows[ad] / usdjpy["close"] - 1) * 100) < 0.006, "the yen's change is the reciprocal's: dollars per yen, not yen per dollar"
    assert U.inverted("USDJPY") and not U.inverted("EURUSD") and not U.inverted("DX-Y.NYB")
    assert BY["DXY"]["k"] == "idx" and BY["VIX"]["role"] == "haven" and BY["JPY"]["role"] == "haven" and BY["MXN"]["role"] == "risk" and BY["KC"]["k"] == "fut"
    assert all("chart" not in BY[t] and "dv" not in BY[t] for t in ("EUR", "JPY", "DXY", "VIX", "KC"))


@test
def an_instrument_behind_its_kind_is_dated_and_a_stale_one_left_out():
    late = {t["t"]: t["late"] for t in DOC["map"]["tiles"] if t.get("late")}
    newest = max(BY[t]["d"] for t in ("EUR", "JPY", "CHF", "MXN"))
    for t in ("EUR", "JPY", "CHF", "MXN"):
        assert late.get(t, 0) == (date.fromisoformat(newest) - date.fromisoformat(BY[t]["d"])).days

    def old(fix):
        fix["fmp"]["USDMXN"] = fix["fmp"]["USDMXN"][:-12]
    doc, _ = build(old)
    assert "MXN" not in {t["t"] for t in doc["map"]["tiles"]} and any(x["t"] == "MXN" and "no bar since" in x["why"] for x in doc["map"]["left_out"])

    def short(fix):
        fix["fmp"]["KCUSX"] = fix["fmp"]["KCUSX"][-100:]
    doc, _ = build(short)
    assert any(x["t"] == "KC" and "130" in x["why"] for x in doc["map"]["left_out"])
    doc, _ = build(lambda fix: fix["fmp"].pop("^VIX"))
    assert any(x["t"] == "VIX" and "no banked prices" in x["why"] for x in doc["map"]["left_out"])


@test
def an_unbelievable_move_is_not_called():
    def spike(fix):
        rows = fix["fmp"]["EURUSD"]
        for r in rows[-10:]:
            r["close"] = r["close"] * 1.6                          # a currency does not gain 60% in a day
    doc, _ = build(spike)
    t = {x["t"]: x for x in doc["map"]["tiles"]}["EUR"]
    assert t["score"] >= 45 and t["state"] == "RISK-ON" and "no split on record" in t["held"] and t["jump"]["ratio"] > 1.5 and "EUR" not in doc["map"]["booming"]
    assert L.big_move([("2026-01-0%d" % i, c) for i, c in enumerate((1.0, 1.05, 1.3, 1.31), 1)], 0.12) == {"date": "2026-01-03", "ratio": round(1.3 / 1.05, 4)}
    assert L.big_move([("a", 1.0), ("b", 1.1)], 0.12) is None
    row, _ = M.measure(walk(400, lambda i: math.exp(0.004 * i)))
    assert L.demote(row, "why")["state"] == "RISK-ON" and L.demote(row, "why")["held"] == "why" and row["state"] == "BOOMING"
    assert L.demote(dict(row, state="NEUTRAL"), "why")["state"] == "NEUTRAL"


@test
def too_little_read_is_never_published():
    for change, word in ((lambda fix: fix.__setitem__("bank", {}), "no fund"), (lambda fix: [fix["bank"].pop(t) for t in list(fix["bank"])[:30]], "previous publication stands")):
        try:
            build(change)
        except RuntimeError as e:
            assert word in str(e), e
        else:
            raise AssertionError("published on a broken read")

    def few_legacy(fix):
        for t in ("XLK", "XLC", "XLY", "XLF", "XLI", "XLE", "XLB", "XLV", "XLP", "XLU", "XLRE", "MTUM", "QUAL"):
            fix["bank"].pop(t)
    try:
        build(few_legacy)
    except RuntimeError as e:
        assert "38" in str(e) or "instruments could be measured" in str(e), e
    else:
        raise AssertionError("published with 25 of the first map's instruments")


@test
def turns_follow_the_first_engines_rule():
    names = {"A": {"name": "a"}, "B": {"name": "b"}, "C": {"name": "c"}, "D": {"name": "d"}, "E": {"name": "e"}}
    now = {"A": {"risk_on": 50, "state": "BOOMING"}, "B": {"risk_on": -50, "state": "DESTROYED"}, "C": {"risk_on": 5, "state": "NEUTRAL"},
           "D": {"risk_on": 20, "state": "RISK-ON"}, "E": {"risk_on": 3, "state": "NEUTRAL"}}
    prev = {"A": {"risk_on": 20, "state": "RISK-ON"}, "B": {"risk_on": -20, "state": "RISK-OFF"}, "C": {"risk_on": -30, "state": "RISK-OFF"},
            "D": {"risk_on": 21, "state": "RISK-ON"}, "E": {"risk_on": 30, "state": "RISK-ON"}}
    got = {x["ticker"]: x["kind"] for x in L.turns(now, prev, names)}
    assert got == {"A": "ACCELERATING", "B": "CAPITULATING", "C": "BOTTOMING / RECOVERING", "E": "ROLLING OVER"}, got
    assert L.turns(now, None, names) == [] and L.turns(now, {}, names) == []
    assert DOC["map"]["n_transitions"] == 0 or all(set(x) >= {"ticker", "name", "from", "to", "delta", "kind"} for x in DOC["map"]["transitions"])
    snap = L.snapshot(DOC)
    assert set(snap) == {"generated_at", "label", "states", "tiles"} and set(snap["states"]) == {x["ticker"] for x in DOC["ranked"]} and len(snap["tiles"]) == DOC["map"]["n"]
    again, _ = build(prev=snap)
    assert again["transitions"] == [] and again["map"]["transitions"] == [] and again["regime_changed"] is False and again["prev_regime"] == DOC["regime"]["label"]


@test
def the_cross_asset_read_follows_its_rule():
    def tiles(risk, ust, gold, yen, dollar):
        return [{"t": "SPY", "grp": "us_size", "role": "risk", "score": risk}, {"t": "EEM", "grp": "world_region", "role": "risk", "score": risk},
                {"t": "TLT", "grp": "bond_ust", "role": "haven", "score": ust}, {"t": "BIL", "grp": "bond_ust", "role": "none", "score": 99},
                {"t": "GLD", "grp": "cm_precious", "role": "haven", "score": gold}, {"t": "JPY", "grp": "fx_major", "role": "haven", "score": yen},
                {"t": "CHF", "grp": "fx_major", "role": "haven", "score": yen}, {"t": "DXY", "grp": "fx_usd", "role": "haven", "score": dollar}]
    for args, label in (((-40, 30, 30, 30, 0), "FLIGHT TO SAFETY"), ((-40, -10, -60, -10, 30), "LIQUIDATION"), ((-40, 0, 0, 0, 0), "BROAD SELL-OFF"),
                        ((40, -20, 0, 0, -20), "RISK-ON"), ((40, 30, 30, 30, 0), "EVERYTHING UP"), ((0, 50, 50, 50, 50), "MIXED"), ((-15, 15, 15, 15, 0), "FLIGHT TO SAFETY"), ((-14.9, 50, 50, 50, 0), "MIXED")):
        got = L.cross_asset(tiles(*args))
        assert got["label"] == label, (args, got["label"])
        assert got["risk"] == args[0] and got["treasuries"] == args[1] and got["safe"] == round((args[1] + args[2] + args[3]) / 3.0, 1) and got["dollar"] == args[4]
    assert L.cross_asset([{"t": "SPY", "grp": "us_size", "role": "risk", "score": 1}])["label"] == "NOT READ"
    c = DOC["cross_asset"]
    assert c["label"] in ("FLIGHT TO SAFETY", "LIQUIDATION", "BROAD SELL-OFF", "RISK-ON", "EVERYTHING UP", "MIXED") and c["rule"] and str(c["risk"]) in c["summary"].replace("+", "")


@test
def group_figures_are_counts_and_medians_of_their_tiles():
    m = DOC["map"]
    assert [c["id"] for c in m["classes"]] == ["us", "world", "bonds", "fx", "cmdty", "crypto", "realestate", "vol", "alts"]
    for c in m["classes"]:
        mine = [t for t in m["tiles"] if t["cls"] == c["id"]]
        st = m["stats"]["classes"][c["id"]]
        assert st["n"] == len(mine) and sum(st["states"].values()) == len(mine)
        if mine:
            assert st["score"] == round(sum(t["score"] for t in mine) / len(mine), 1) and st["rising_pct"] == round(100.0 * sum(1 for t in mine if t["score"] > 0) / len(mine))
            assert st["r"]["q"] == round(statistics.median(t["r"]["q"] for t in mine), 2)
        for g in c["groups"]:
            assert m["stats"]["groups"][g["id"]]["n"] == sum(1 for t in mine if t["grp"] == g["id"])
    assert m["n"] == len(m["tiles"]) == sum(m["counts"].values()) and m["n_universe"] == len(cut_members()) + 6


# ----------------------------------------------------------------------------------------------- the handler, the file
@test
def the_handler_writes_the_feed_and_the_snapshot_and_a_dry_run_writes_nothing():
    s3 = warehouse()
    s3.objects[L.PREV_KEY] = json.dumps(FIX["prev"]).encode()
    real, L._s3 = L._s3, (lambda: s3)
    try:
        out = L.lambda_handler({"dry_run": True}, None)
        assert out["dry_run"] is True and s3.puts == {} and out["tiles"] == DOC["map"]["n"]
        out = L.lambda_handler({}, None)
    finally:
        L._s3 = real
    assert out["statusCode"] == 200 and json.loads(out["body"])["regime"] == DOC["regime"]["label"] and set(s3.puts) == {L.OUT_KEY, L.PREV_KEY}
    feed, snap = json.loads(s3.puts[L.OUT_KEY]), json.loads(s3.puts[L.PREV_KEY])
    assert feed["contract"] == "risk-map.v2" and feed["map"]["n"] == DOC["map"]["n"] and snap["label"] == feed["regime"]["label"] and len(snap["states"]) == 38
    assert len(s3.puts[L.OUT_KEY]) < 400_000


@test
def nothing_is_asked_of_a_provider():
    text = "".join(open(os.path.join(SRC, n), encoding="utf-8").read() for n in ("lambda_function.py", "risk_math.py", "risk_universe.py"))
    for word in ("urllib", "requests", "http://", "https://", "managed_secret", "apiKey", "api.polygon.io", "financialmodelingprep"):
        assert word not in text, "the engine reads the warehouse only: %r" % word
    reads = set()

    class Spy(FakeS3):
        def get_object(self, Bucket, Key):
            reads.add(Key)
            return FakeS3.get_object(self, Bucket, Key)
    s3 = Spy(warehouse().objects)
    L.build(s3, now=NOW, prev=None, log=lambda *_: None)
    assert reads and all(k.startswith(("data/warm/polygon-full/", "data/warm/polygon-bank/splits/", "data/warm/katlin/crypto-bars/", "data/warm/fmp/historical-price-eod-full/",
                                       "data/warm/provider-bank/cmc/listings-latest/daily/")) for k in reads), sorted(reads)[:5]
    assert s3.puts == {}, "build() writes nothing"


@test
def the_universe_is_whole_and_says_what_it_is():
    assert len(ALL_MEMBERS) == len({m["t"] for m in ALL_MEMBERS}) >= 240
    assert {m["kind"] for m in ALL_MEMBERS} == {"fund", "fx", "fut", "idx"} and {m["role"] for m in ALL_MEMBERS} == {"risk", "haven", "none"}
    have = {m["t"] for m in ALL_MEMBERS}
    assert set(U.legacy_tickers()) - set(U.LEGACY_COINS) <= have and len(U.legacy_tickers()) == 38
    by = {m["t"]: m for m in ALL_MEMBERS}
    for t, role in (("SPY", "risk"), ("TLT", "haven"), ("BIL", "none"), ("GLD", "haven"), ("SLV", "none"), ("DXY", "haven"), ("JPY", "haven"), ("EUR", "none"), ("VIX", "haven"), ("HYG", "risk"), ("EMB", "risk"), ("TAIL", "haven")):
        assert by[t]["role"] == role, (t, by[t]["role"])
    for m in ALL_MEMBERS:
        if m["kind"] == "fx":
            assert len(m["sym"]) == 6 and "USD" in m["sym"] and m["t"] == m["sym"].replace("USD", "", 1), m
    layout = U.layout()
    assert [c["id"] for c in layout] == ["us", "world", "bonds", "fx", "cmdty", "crypto", "realestate", "vol", "alts"] and sum(len(c["groups"]) for c in layout) == 25
    assert {g["id"] for c in layout for g in c["groups"]} >= {m["grp"] for m in ALL_MEMBERS}
    for word in ("2x", "3x", "ultra", "inverse", "leveraged", "bear "):
        assert not any(word in m["name"].lower() for m in ALL_MEMBERS), "no geared or inverse fund: %s" % word
    assert not have & {"SVXY", "UVXY", "SQQQ", "TQQQ", "SPXU", "UPRO", "SH", "PSQ", "TBT", "TMF", "UCO", "SCO", "BOIL", "KOLD"}


@test
def the_tape_rules_are_the_leaders_engines_file():
    mine = open(os.path.join(SRC, "leaders_tape.py"), "rb").read()
    theirs = open(os.path.join(ROOT, "aws", "lambdas", "justhodl-leaders", "source", "leaders_tape.py"), "rb").read()
    assert hashlib.sha256(mine).hexdigest() == hashlib.sha256(theirs).hexdigest(), "leaders_tape.py must stay byte-identical to the leaders engine's"


def main():
    failed = 0
    for fn in TESTS:
        try:
            fn()
            print("ok   %s" % fn.__name__)
        except Exception as exc:  # noqa: BLE001
            failed += 1
            import traceback
            print("FAIL %s: %s" % (fn.__name__, exc))
            traceback.print_exc()
    print("%d tests, %d failed" % (len(TESTS), failed))
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
