"""justhodl-bottom -- state-machine and contract tests (dependency-free; boto3 is stubbed with a fail-on-any-call client).

The bars below are synthetic test fixtures that encode the paper's scenarios (textbook sequence, failed test on rising
volume, higher-low test, climax without a rally). They exist only to prove the detector's logic; the engine itself
never scores anything but warehouse bars.
"""
from __future__ import annotations

import importlib.util
import json
import random
import sys
import types
from pathlib import Path

HERE = Path(__file__).resolve().parent
SRC = HERE.parent / "source" / "lambda_function.py"
sys.path.insert(0, str(HERE.parents[2] / "shared"))
sys.path.insert(0, str(HERE.parent / "source"))   # sibling modules the handler imports (capitulation)


class _NoAWS:
    def __getattr__(self, name):
        def _boom(*a, **k):
            raise AssertionError("AWS call attempted in unit test: %s" % name)
        return _boom


def _load():
    fake = types.ModuleType("boto3")
    fake.client = lambda *a, **k: _NoAWS()
    fake.resource = lambda *a, **k: _NoAWS()
    sys.modules["boto3"] = fake
    spec = importlib.util.spec_from_file_location("bottom_under_test", SRC)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _seq(seed=7, test_vol=350_000, break_after_test=False, fail_vol=None, higher_low=False, no_rally=False, n_markup=70):
    """textbook: 80 flat bars, 30-bar decline, climax, 6-bar rally, 9-bar quiet drift to the low, test candle, turn, markup."""
    random.seed(seed)
    o, h, l, c, v = [], [], [], [], []
    px = 100.0

    def bar(open_, close, rng, vol):
        hi = max(open_, close) + rng * 0.4
        lo = min(open_, close) - rng * 0.4
        o.append(open_); c.append(close); h.append(hi); l.append(lo); v.append(vol)
    for _ in range(80):
        nx = px * (1 + random.uniform(-0.006, 0.006)); bar(px, nx, 1.2, 1_000_000); px = nx
    for i in range(30):
        nx = px * (1 - 0.0095 + random.uniform(-0.004, 0.004)); bar(px, nx, 1.5, 1_200_000 + i * 20000); px = nx
    op = px * 0.97; cl = px * 0.955; hi = op * 1.005; lo = px * 0.92
    o.append(op); c.append(cl); h.append(hi); l.append(lo); v.append(4_500_000); px = cl
    sc_low = lo
    if no_rally:
        for i in range(20):   # keeps bleeding on light volume: no automatic rally
            nx = px * 0.995; bar(px, nx, 0.9, 700_000); px = nx
        return o, h, l, c, v, sc_low
    for i in range(6):
        nx = px * 1.015; bar(px, nx, 1.4, 1_800_000 - i * 150000); px = nx
    target = sc_low * (1.012 if higher_low else 1.003)
    steps = 9
    for i in range(steps):
        nx = px + (target - px) / (steps - i); bar(px, nx, 1.0 - 0.05 * i, 900_000 - i * 70000); px = nx
    if fail_vol:
        # the test arrives on RISING volume and closes through the low
        o.append(px); c.append(sc_low * 0.975); h.append(px * 1.002); l.append(sc_low * 0.965); v.append(fail_vol); px = c[-1]
        for i in range(10):
            nx = px * 0.99; bar(px, nx, 1.3, fail_vol * 0.8); px = nx
        return o, h, l, c, v, sc_low
    o.append(px); c.append(px * 1.002); h.append(px * 1.008); l.append((sc_low * 1.012) if higher_low else (sc_low * 1.001)); v.append(test_vol); px = c[-1]
    o.append(px); c.append(h[-1] * 1.012); h.append(h[-1] * 1.02); l.append(px * 0.998); v.append(1_300_000); px = c[-1]
    for i in range(n_markup):
        nx = px * 1.006; bar(px, nx, 1.2, 1_100_000); px = nx
    return o, h, l, c, v, sc_low


def test_textbook_sequence(m):
    o, h, l, c, v, sc_low = _seq()
    ev, act = m.detect(o, h, l, c, v, m.P["D"], "D")
    E = act or ev[-1]
    assert E["sc_low"] == sc_low or abs(E["sc_low"] - sc_low) < 1e-9, E["sc_low"]
    assert E["sc_vol_x"] > 3.0 and E["sc_range_x"] > 1.5
    assert E["ar_valid_i"] is not None and E["ar_rally_atr"] >= 2.0
    assert E["st_i"] is not None and E["st_vol_ratio_sc"] < 0.2, E["st_vol_ratio_sc"]
    assert E["approach_vol_slope"] < 0 and E["approach_range_x"] < 0.9
    assert E["trig_i"] is not None and E["trig_i"] > E["st_i"]
    assert c[E["trig_i"]] > h[E["st_i"]], "trigger must be a close above the test candle's high"
    assert E["stop"] < E["st_low"] and E["target_1"] == E["ar_high"]
    assert E["sos_i"] is not None, "markup should have closed above the rally high"
    assert E["ret_21"] is not None and E["ret_21"] > 0 and E["t1_hit"] is True and E["stop_hit"] is False
    assert E["ar_ret_21"] is not None and E["ar_hole"] is False
    score, grade, parts, reasons, risks = m.score_event(E, m.P["D"])
    assert grade == "A" and score >= 80, (score, grade)
    assert any("secondary test on" in r for r in reasons)
    return "textbook: SC %.1fx vol -> AR %.1f%% -> ST %.0f%% of climax vol -> trigger -> markup, score %.0f%s" % (E["sc_vol_x"], E["ar_rally_pct"], 100 * E["st_vol_ratio_sc"], score, grade)


def test_failed_test_on_rising_volume(m):
    o, h, l, c, v, sc_low = _seq(fail_vol=3_900_000)
    ev, act = m.detect(o, h, l, c, v, m.P["D"], "D")
    states = [e["state"] for e in ev]
    assert "FAILED" in states, states
    E = [e for e in ev if e["state"] == "FAILED"][0]
    assert E["trig_i"] is None, "a failed test must never trigger"
    assert "rising" in E["why_closed"]
    assert E["ar_hole"] is True, "the crowd's bounce entry saw the hole get deeper"
    assert E["ar_ret_21"] is not None and E["ar_ret_21"] < 0
    score, grade, *_ = m.score_event(E, m.P["D"])
    assert score <= 25
    return "failed test: state FAILED (%s), AR buyer -%.1f%% at +21, no trigger" % (E["why_closed"], -E["ar_ret_21"])


def test_higher_low(m):
    o, h, l, c, v, sc_low = _seq(higher_low=True)
    ev, act = m.detect(o, h, l, c, v, m.P["D"], "D")
    E = act or ev[-1]
    assert E["st_depth_class"] == "HIGHER_LOW", (E["st_depth_class"], E["st_depth_atr"])
    assert E["st_low"] > E["sc_low"]
    score, grade, parts, reasons, risks = m.score_event(E, m.P["D"])
    assert any("higher low" in r for r in reasons)
    return "higher-low test recognised (%.2f ATR above the climax low)" % E["st_depth_atr"]


def test_no_rally(m):
    o, h, l, c, v, sc_low = _seq(no_rally=True)
    ev, act = m.detect(o, h, l, c, v, m.P["D"], "D")
    assert act is None or act["state"] in ("CLIMAX",)
    assert ev and ev[0]["state"] == "NO_RALLY", [e["state"] for e in ev]
    return "climax without a bounce -> NO_RALLY (not a bottom)"


def test_weekly_resample_and_row(m):
    o, h, l, c, v, sc_low = _seq(n_markup=20)
    b = m.Bars()
    from datetime import date, timedelta
    d0 = date(2021, 1, 4)
    dates = []
    k = 0
    while len(dates) < len(c):
        d = d0 + timedelta(days=k); k += 1
        if d.weekday() < 5:
            dates.append(d.isoformat())
    for i in range(len(c)):
        b.d.append(i); b.o.append(o[i]); b.h.append(h[i]); b.l.append(l[i]); b.c.append(c[i]); b.v.append(v[i])
    W = m.resample_weekly(b, dates)
    assert 20 <= len(W["c"]) <= 40 and all(W["h"][i] >= W["l"][i] for i in range(len(W["c"])))
    assert abs(sum(W["v"]) - sum(v)) < 1e-6, "weekly volume must sum the daily volume"
    m.P["min_sessions"] = 100   # the fixture is short
    F = {"asof": {}, "finviz": {"TEST": {"company": "Test Co", "sector": "Technology", "industry": "Software - Application", "market_cap": "2500"}},
         "accum": {"TEST": {"bottom_score": 71, "phase": "ACCUMULATION", "signal": "LIKELY_BOTTOM"}}, "phase": {}, "fortress": {}, "katlin": {},
         "f13": {"TEST": {"net_usd_m": 120.0}}, "dark": {}, "insider": {"TEST": {"n_buys": 3, "usd": 2.4e6, "cluster": True}}, "flows": {},
         "authority": {"mode": "SELECTIVE", "allows_new_entries": True}, "risk_gate": {"posture": "NEUTRAL"}, "katlin_posture": "SELECTIVE"}
    r, evD, evW = m.build_row("TEST", "stock", "stock", b, dates, F, len(dates) - 1)
    assert r is not None and r["state"] in ("TRIGGERED", "MARKUP") and r["frame"] == "D"
    assert r["plan"]["stop"] < r["plan"]["entry"] < r["plan"]["target_1"]
    assert r["n_confirm"] >= 3 and any(x["src"] == "13F" for x in r["confirm"])
    assert r["chart"]["marks"].get("SC") is not None and r["chart"]["marks"].get("ST") is not None and len(r["chart"]["c"]) <= 110
    assert "climax" in r["why"].lower() and "Fleet confirmation" in r["why"]
    js = json.dumps(r, allow_nan=False, default=str)
    assert len(js) < 40000, len(js)
    hist = [m.hist_row("TEST", "stocks", "stock", E, "D", m.P["D"]) for E in evD]
    br = m.base_rates(hist)
    assert br["n_sequences"] == len(evD) and "crowd_vs_pro" in br
    mc = m.market_context([r], {}, F, 1)
    assert mc["breadth"]["in_bottom_process"] == 1
    return "weekly resample ok (%d weeks), row %s/%s score %s, %d confirmations, %d bytes, base rates over %d sequences" % (
        len(W["c"]), r["state"], r["grade"], r["score"], r["n_confirm"], len(js), br["n_sequences"])


def test_classify_wrappers(m):
    cases = {"SPDR Gold Shares": "gold", "VanEck Gold Miners ETF": "gold_miners", "iShares Silver Trust": "silver", "abrdn Platinum ETF Trust": "pgm",
             "United States Copper Index Fund": "copper", "Global X Uranium ETF": "industrial_metals", "iShares 20+ Year Treasury Bond ETF": "bond_govt",
             "iShares iBoxx $ High Yield Corporate Bond ETF": "bond_credit", "iShares J.P. Morgan USD Emerging Markets Bond ETF": "bond_intl",
             "United States Oil Fund": "energy", "Invesco DB Agriculture Fund": "agriculture", "Invesco DB Commodity Index Tracking Fund": "commodity_broad",
             "Invesco DB US Dollar Index Bullish Fund": "currency", "iShares Bitcoin Trust": "crypto_etf", "iShares MSCI Brazil ETF": "country",
             "Technology Select Sector SPDR Fund": "sector", "Vanguard Real Estate ETF": "real_estate", "ProShares UltraPro QQQ": None,
             "JPMorgan Equity Premium Income ETF": None, "iShares 0-3 Month Treasury Bond ETF": None}
    bad = []
    for name, want in cases.items():
        got = m.classify_etf({"company": name, "etf_type": "Equity" if want in ("sector", "country", "real_estate", None) else "Commodity" if want in ("gold", "silver", "pgm", "copper", "energy", "agriculture", "commodity_broad") else "Bond" if want and want.startswith("bond") else ""})
        if got != want:
            bad.append((name, got, want))
    assert not bad, bad
    return "%d wrapper names classified into the right desks; leveraged/overlay/money-market excluded" % len(cases)


def _uptrend_pullback(seed=5):
    """IVES-style: a strong uptrend (+60% over 150 bars) with an 9% pullback to a 3-week low on a 2.5x volume down bar, then the
    uptrend resumes. v1.1 called this a selling climax; v1.2 must not (not a new 120-bar low, above the 50-bar average, in the
    top of the yearly range, not a prolonged decline)."""
    random.seed(seed)
    o, h, l, c, v = [], [], [], [], []
    px = 100.0

    def bar(open_, close, rng, vol):
        hi = max(open_, close) + rng * 0.4
        lo = min(open_, close) - rng * 0.4
        o.append(open_); c.append(close); h.append(hi); l.append(lo); v.append(vol)
    for i in range(150):
        nx = px * (1 + 0.0032 + random.uniform(-0.006, 0.006)); bar(px, nx, 1.5, 1_000_000 + i * 4000); px = nx
    for i in range(8):
        nx = px * 0.988; bar(px, nx, 1.6, 1_300_000); px = nx
    op = px * 0.99; cl = px * 0.975; hi = op * 1.003; lo = px * 0.965
    o.append(op); c.append(cl); h.append(hi); l.append(lo); v.append(4_000_000); px = cl
    for i in range(60):
        nx = px * 1.006; bar(px, nx, 1.4, 1_100_000); px = nx
    return o, h, l, c, v


def test_uptrend_pullback_is_not_a_climax(m):
    o, h, l, c, v = _uptrend_pullback()
    diag = {}
    ev, act = m.detect(o, h, l, c, v, m.P["D"], "D", diag=diag)
    assert not ev and act is None, "a pullback in an uptrend must never be a selling climax: %s" % [(e["state"], e["sc_i"]) for e in ev]
    assert diag.get("vol_x_ok", 0) >= 1 and diag.get("climax", 0) == 0, diag
    rejected = {k: n for k, n in diag.items() if k.startswith("rej_")}
    assert rejected, diag
    return "uptrend pullback rejected (%s)" % ", ".join("%s=%d" % kv for kv in sorted(rejected.items()))


def test_resolved_sequence_is_not_actionable(m):
    o, h, l, c, v, sc_low = _seq(n_markup=40)   # ran far above the range after the trigger
    b = m.Bars()
    from datetime import date, timedelta
    d0 = date(2021, 1, 4); dates = []; k = 0
    while len(dates) < len(c):
        d = d0 + timedelta(days=k); k += 1
        if d.weekday() < 5:
            dates.append(d.isoformat())
    for i in range(len(c)):
        b.d.append(i); b.o.append(o[i]); b.h.append(h[i]); b.l.append(l[i]); b.c.append(c[i]); b.v.append(v[i])
    m.P["min_sessions"] = 100
    F = {"asof": {}, "finviz": {}, "accum": {}, "phase": {}, "fortress": {}, "katlin": {}, "f13": {}, "dark": {}, "insider": {}, "flows": {},
         "authority": {}, "risk_gate": {}, "katlin_posture": None}
    r, evD, evW = m.build_row("RAN", "stock", "stock", b, dates, F, len(dates) - 1)
    assert r is not None and r["state"] == "MARKUP" and r["actionable"] is False and r["still_in_range"] is False
    assert r["pos_52w_pct"] is not None and r["dist_52w_high_pct"] is not None
    o, h, l, c, v, sc_low = _seq(n_markup=1)     # fresh trigger, price still in the range
    b2 = m.Bars()
    for i in range(len(c)):
        b2.d.append(i); b2.o.append(o[i]); b2.h.append(h[i]); b2.l.append(l[i]); b2.c.append(c[i]); b2.v.append(v[i])
    r2, _, _ = m.build_row("FRESH", "stock", "stock", b2, dates[:len(c)], F, len(c) - 1)
    assert r2 is not None and r2["state"] == "TRIGGERED" and r2["actionable"] is True and r2["still_in_range"] is True
    return "resolved MARKUP row not actionable (%.0f%% above the trigger level); fresh trigger actionable" % ((r["last"] / r["trigger_level"] - 1) * 100)


# ---- 1.3.0: the cross-asset capitulation scan (capitulation.py) ------------------------------------------------------
def _calendar(n, start=(2021, 1, 4)):
    from datetime import date, timedelta
    d0 = date(*start)
    out, k = [], 0
    while len(out) < n:
        d = d0 + timedelta(days=k); k += 1
        if d.weekday() < 5:
            out.append(d.isoformat())
    return out


def _flat(n, px=100.0, vol=1_000_000, seed=3, drift=0.0, noise=0.004):
    """n quiet bars: small noise, steady volume, a 1% range."""
    random.seed(seed)
    o, h, l, c, v = [], [], [], [], []
    p = px
    for _ in range(n):
        nx = p * (1 + drift + random.uniform(-noise, noise))
        hi, lo = max(p, nx) * 1.004, min(p, nx) * 0.996
        o.append(p); c.append(nx); h.append(hi); l.append(lo); v.append(vol * random.uniform(0.85, 1.15)); p = nx
    return o, h, l, c, v


def _panic_bar(o, h, l, c, v, drop_pct, vol_x, close_loc=0.1, range_mult=3.0):
    """append one panic bar: gaps down, wide range (range_mult x the recent range), closes near its low on vol_x x volume."""
    p = c[-1]
    rng = (sum(h[-14:]) - sum(l[-14:])) / 14.0 * range_mult
    close = p * (1 + drop_pct / 100.0)
    low = close - close_loc * rng
    high = low + rng
    o.append(min(high, p * 0.995)); c.append(close); h.append(high); l.append(low); v.append(sum(v[-50:]) / 50.0 * vol_x)


def test_capitulation_print_detected(m):
    cap = m.CAP
    o, h, l, c, v = _flat(120)
    # a 20-session decline first (so the context is DECLINE), then the panic bar
    p = c[-1]
    for i in range(20):
        nx = p * 0.991; o.append(p); c.append(nx); h.append(p * 1.002); l.append(nx * 0.997); v.append(1_050_000); p = nx
    _panic_bar(o, h, l, c, v, -6.0, 4.5, close_loc=0.08)
    found = cap.scan(o, h, l, c, v, "stocks")
    assert len(found) == 1, [f["i"] for f in found]
    f = found[0]
    assert f["i"] == len(c) - 1 and f["vol_x"] >= 4.0 and f["vol_dominant"] and f["range_x"] >= 1.5 and f["loc"] <= 0.3
    assert f["new_low_20"] and f["new_low_60"] and f["context"] == "DECLINE", f
    assert f["bars_down"] >= 20 and f["dd20_pct"] < -15 and 60 <= f["score"] <= 100, (f["bars_down"], f["dd20_pct"], f["score"])
    # the same bar with ordinary volume is not a print; a down bar closing in its upper half is not a print
    v2 = list(v); v2[-1] = v2[-2] * 1.3
    assert cap.scan(o, h, l, c, v2, "stocks") == []
    c3, h3 = list(c), list(h)
    c3[-1] = l[-1] + 0.7 * (h[-1] - l[-1])
    assert cap.scan(o, h3, l, c3, v, "stocks") == []
    return "panic print: -%.1f%% on %.1fx volume, %.1f ATR range, close %.0f%% up the bar, score %s (DECLINE)" % (-f["ret_pct"], f["vol_x"], f["range_x"], 100 * f["loc"], f["score"])


def test_capitulation_shock_and_desk_floors(m):
    cap = m.CAP
    # an event gap out of a flat tape: SHOCK, qualifies through the crash rule even with no decline before it
    o, h, l, c, v = _flat(120)
    _panic_bar(o, h, l, c, v, -7.0, 3.0, close_loc=0.15)
    f = cap.scan(o, h, l, c, v, "stocks")
    assert len(f) == 1 and f[0]["context"] == "SHOCK" and f[0]["crash"] is True, f
    # desk floors: a -1.4% bar on panic volume is a print for bonds, never for stocks (floor -3%)
    o, h, l, c, v = _flat(120, noise=0.002)
    _panic_bar(o, h, l, c, v, -1.4, 3.2, close_loc=0.1)
    assert len(cap.scan(o, h, l, c, v, "bonds")) == 1 and cap.scan(o, h, l, c, v, "stocks") == []
    # crypto needs -5%
    o, h, l, c, v = _flat(120)
    _panic_bar(o, h, l, c, v, -4.0, 3.5, close_loc=0.1)
    assert cap.scan(o, h, l, c, v, "crypto") == [] and len(cap.scan(o, h, l, c, v, "equity_etfs")) == 1
    # the ATR gate uses the bars BEFORE the print: a second panic bar right after the first still measures against the quiet range
    o, h, l, c, v = _flat(120)
    _panic_bar(o, h, l, c, v, -6.0, 4.0)
    _panic_bar(o, h, l, c, v, -6.0, 4.0)
    assert len(cap.scan(o, h, l, c, v, "stocks")) == 2
    return "SHOCK vs DECLINE, desk floors (bonds -1% / stocks -3% / crypto -5%), prior-ATR gate"


def test_capitulation_follow_through_and_breadth(m):
    cap = m.CAP
    dates = _calendar(181)    # every instrument below carries 181 bars on this calendar
    acc = cap.Accumulator(dates)
    # instrument A: a print 10 sessions ago whose low held and whose volume dried up; B: a print that was undercut; C: quiet
    o, h, l, c, v = _flat(150)
    for i in range(20):
        p = c[-1]; nx = p * 0.99; o.append(p); c.append(nx); h.append(p * 1.002); l.append(nx * 0.997); v.append(1_050_000)
    _panic_bar(o, h, l, c, v, -6.0, 4.5, close_loc=0.05)
    plow = l[-1]
    p = c[-1]
    for i in range(10):
        nx = p * 1.01; o.append(p); c.append(nx); h.append(nx * 1.003); l.append(max(p * 0.997, plow * 1.001)); v.append(600_000); p = nx
    n = len(c)
    acc.add("AAA", "stocks", "stock", "Held Inc", o, h, l, c, v, list(range(n)), dates)
    o2, h2, l2, c2, v2 = _flat(150, seed=5)
    for i in range(20):
        p = c2[-1]; nx = p * 0.99; o2.append(p); c2.append(nx); h2.append(p * 1.002); l2.append(nx * 0.997); v2.append(1_050_000)
    _panic_bar(o2, h2, l2, c2, v2, -6.0, 4.5, close_loc=0.05)
    p = c2[-1]
    pv = v2[-1]
    for i in range(10):   # keeps falling on volume heavier than the print's: no dry-up, the low gone
        nx = p * 0.985; o2.append(p); c2.append(nx); h2.append(p * 1.002); l2.append(nx * 0.997); v2.append(pv * 1.2); p = nx
    acc.add("BBB", "stocks", "stock", "Broke Corp", o2, h2, l2, c2, v2, list(range(len(c2))), dates)
    o3, h3, l3, c3, v3 = _flat(181, seed=9)
    acc.add("CCC", "equity_etfs", "etf", "Quiet Fund", o3, h3, l3, c3, v3, list(range(len(c3))), dates)
    # a crypto print on a Saturday counts toward the following Monday's session
    o4, h4, l4, c4, v4 = _flat(150, seed=11)
    for i in range(20):
        p = c4[-1]; nx = p * 0.985; o4.append(p); c4.append(nx); h4.append(p * 1.002); l4.append(nx * 0.997); v4.append(1_050_000)
    _panic_bar(o4, h4, l4, c4, v4, -12.0, 5.0, close_loc=0.05)
    from datetime import date, timedelta
    end = date(2021, 6, 19)   # a Saturday
    cdates = [(end - timedelta(days=len(c4) - 1 - k)).isoformat() for k in range(len(c4))]
    sat = cdates[-1]
    assert date.fromisoformat(sat).weekday() == 5, sat
    acc.add("XYZ", "crypto", "crypto", "XYZ", o4, h4, l4, c4, v4, list(range(len(c4))), cdates)
    wy = {"AAA": {"sc_date": dates[n - 11], "state": "TESTING", "grade": "B", "score": 61.0, "frame": "D", "actionable": False}}
    confirms = {"AAA": ([{"src": "13F", "read": "institutions net +$10M", "bull": True}], 1)}
    block = cap.build(acc, wy, lambda t, cls: confirms.get(t, ([], 0)), lambda t, cls: {"f13_net_usd_m": 10.0} if t == "AAA" else {}, {})
    rows = {r["ticker"]: r for r in block["boards"]["stocks"]}
    a, b = rows["AAA"], rows["BBB"]
    assert a["after"]["sessions_since"] == 10 and a["after"]["held"] is True and a["after"]["vol_after_x"] < 0.3 and a["after"]["bounce_pct"] > 5
    assert b["after"]["held"] is False and b["after"]["undercut_pct"] < -5 and b["after"]["now_vs_print_pct"] < -10
    assert a["wyckoff"] and a["wyckoff"]["same_bar"] is True and a["wyckoff"]["state"] == "TESTING" and b["wyckoff"] is None
    assert a["n_confirm"] == 1 and a["f13_net_usd_m"] == 10.0
    assert block["counts"]["stocks"]["scored"] == 2 and block["counts"]["equity_etfs"]["scored"] == 1 and block["counts"]["crypto"]["scored"] == 1
    br = block["breadth"]
    assert br["n_scored"] == 4 and br["window_sessions"] == 181 and len(br["sessions"]) == 120
    assert br["sessions"][-11]["total"] == 2 and br["sessions"][-11]["by_desk"] == {"stocks": 2}, br["sessions"][-11]
    nxt_monday = next(d for d in dates if d > sat)
    sess = {s["date"]: s for s in br["sessions"]}
    assert sess[nxt_monday]["by_desk"].get("crypto") == 1, (nxt_monday, sess[nxt_monday])
    assert br["regime"] in ("QUIET", "NORMAL") and br["sum_5"] == 0
    ft = block["follow_through"]   # the crypto print is the last bar: too young to judge
    assert ft["n"] == 2 and ft["held_pct"] == 50.0 and ft["by_context"]["DECLINE"]["n"] == 2 and ft["volume_dried_up_pct"] == 50.0, ft
    assert block["read"].startswith("selective panic") and block["definitions"]["not_a_call"]
    assert len(block["boards"]["crypto"]) == 1 and block["boards"]["crypto"][0]["date"] == sat and block["boards"]["bonds"] == []
    slim = cap.slim(block)
    assert slim["key"] == "data/capitulation-desk.json" and slim["n_prints_60"] == 3 and "boards" not in slim
    js = json.dumps(block, allow_nan=False)
    assert len(js) < 200000, len(js)
    return "follow-through (held / undercut / volume after), Wyckoff same-bar link, fleet joins, breadth on the equity calendar (crypto weekend -> next session), slim pointer %d bytes" % len(json.dumps(slim))


def test_capitulation_breadth_regime(m):
    cap = m.CAP
    # 40 instruments on one calendar (300 sessions), each with one old print on its own day (the window's base rate) and
    # 30 of them printing on the same session two bars before the end
    N = 300
    dates = _calendar(N)
    acc = cap.Accumulator(dates)
    for k in range(40):
        o, h, l, c, v = _flat(N, seed=100 + k)
        idx = 100 + 3 * k
        o2, h2, l2, c2, v2 = o[:idx], h[:idx], l[:idx], c[:idx], v[:idx]
        _panic_bar(o2, h2, l2, c2, v2, -7.0, 4.5, close_loc=0.1)
        tail = _flat(N - idx - 1, px=c2[-1], seed=200 + k)
        for arr, t in zip((o2, h2, l2, c2, v2), tail):
            arr.extend(t)
        if k < 30:
            o2, h2, l2, c2, v2 = o2[:-2], h2[:-2], l2[:-2], c2[:-2], v2[:-2]
            _panic_bar(o2, h2, l2, c2, v2, -7.0, 4.5, close_loc=0.1)
            tail = _flat(1, px=c2[-1], seed=300 + k)
            for arr, t in zip((o2, h2, l2, c2, v2), tail):
                arr.extend(t)
        assert len(c2) == N
        acc.add("S%02d" % k, "stocks", "stock", "S%02d" % k, o2, h2, l2, c2, v2, list(range(N)), dates)
    block = cap.build(acc, {}, None, None, {})
    br = block["breadth"]
    assert br["n_scored"] == 40 and br["sum_5"] >= 25, (br["sum_5"], br["last_session"])
    assert br["sum_5_percentile"] >= 97 and br["regime"] == "EXTREME", (br["sum_5_percentile"], br["regime"])
    assert br["n_clusters_window"] >= 1 and br["clusters"][-1]["pct_of_scored"] >= 2.5
    assert block["read"].startswith("panic is broad") and "cluster" in block["read"]
    assert sum(block["counts"][d]["prints_5"] for d in cap.DESK_ORDER) == len(block["latest"]) and len(block["latest"]) >= 25
    return "breadth regime: %d prints in 5 sessions = %s (percentile %s), %d cluster sessions" % (br["sum_5"], br["regime"], br["sum_5_percentile"], br["n_clusters_window"])


def test_coinbase_candles_shape(m):
    """the fallback parses Coinbase's [time, low, high, open, close, volume] into the bank's [date, o, h, l, c, v] and drops the partial day."""
    import datetime as _dt
    calls = []

    def fake_get(url, timeout=40):
        calls.append(url)
        t0 = int(_dt.datetime(2026, 11, 3, tzinfo=_dt.timezone.utc).timestamp())
        return [[t0 + 86400, 99.0, 110.0, 105.0, 101.0, 1234.5], [t0, 90.0, 100.0, 95.0, 98.0, 2000.0]]
    real = m.poly_get
    m.poly_get = fake_get
    try:
        rows = m.cb_candles("BTC-USD", "2026-11-01", "2026-11-04")
    finally:
        m.poly_get = real
    assert rows == [["2026-11-03", 95.0, 100.0, 90.0, 98.0, 2000.0]], rows
    assert "products/BTC-USD/candles?granularity=86400&start=2026-11-01T00:00:00Z&end=2026-11-04T00:00:00Z" in calls[0]
    return "coinbase rows -> bank shape [date,o,h,l,c,v], the in-progress UTC day dropped"


def main():
    m = _load()
    tests = [test_textbook_sequence, test_failed_test_on_rising_volume, test_higher_low, test_no_rally, test_weekly_resample_and_row, test_classify_wrappers,
             test_uptrend_pullback_is_not_a_climax, test_resolved_sequence_is_not_actionable,
             test_capitulation_print_detected, test_capitulation_shock_and_desk_floors, test_capitulation_follow_through_and_breadth, test_capitulation_breadth_regime,
             test_coinbase_candles_shape]
    failed = 0
    for t in tests:
        try:
            msg = t(m)
            print("PASS %-40s %s" % (t.__name__, msg))
        except Exception as e:
            failed += 1
            import traceback
            print("FAIL %-40s %s: %s" % (t.__name__, type(e).__name__, e))
            traceback.print_exc()
    print("%d/%d passed" % (len(tests) - failed, len(tests)))
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
