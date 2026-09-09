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


def main():
    m = _load()
    tests = [test_textbook_sequence, test_failed_test_on_rising_volume, test_higher_low, test_no_rally, test_weekly_resample_and_row, test_classify_wrappers]
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
