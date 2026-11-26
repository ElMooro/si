"""Dependency-free checks for the breadth engine's arithmetic (v1.1). The fixture is the engine's own kept history and
published reading of 2026-11-25, so the first test proves the state machine reads what it read before the change."""
import ast
import json
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
SRC = HERE.parent / "source"
sys.path.insert(0, str(SRC))
import breadth_math as BM  # noqa: E402

FX = json.loads((HERE / "fixtures" / "history-2026-11-25.json").read_text())
LOW, HIGH, WINDOW = 0.40, 0.615, 10


def check(cond, msg):
    if not cond:
        raise SystemExit("FAIL " + msg)


def row(t, o, c, v=500000):
    return {"T": t, "o": o, "c": c, "v": v}


def test_state_machine_reads_what_it_read_before():
    rows = sorted(FX["history"], key=lambda r: r["date"])
    z = BM.zweig_reading([r["ratio"] for r in rows[-60:]], LOW, HIGH, WINDOW)
    pub = FX["published"]["current_readings"]
    check(z["current_ema"] == pub["zweig_10d_ema"], "10-session average equals the published one (%s vs %s)" % (z["current_ema"], pub["zweig_10d_ema"]))
    check(z["min_in_window"] == pub["zweig_window_min"] and z["max_in_window"] == pub["zweig_window_max"], "window low and high equal the published ones")
    check(z["state"] == FX["published"]["state"] == "ARMED", "the state is the published one")
    # a deeper cache must not move it: the engine hands the state machine the last 60 sessions only
    older = [{"date": "2026-0%d-%02d" % (5 + i // 28, 1 + i % 28), "ratio": 0.9} for i in range(70)]
    deep = sorted(older + rows, key=lambda r: r["date"])[-60:]
    check(BM.zweig_reading([r["ratio"] for r in deep], LOW, HIGH, WINDOW) == z, "70 older sessions in the cache change nothing")


def test_thrust_rule():
    check(BM.zweig_reading([0.5] * 10, LOW, HIGH, WINDOW)["state"] == "INSUFFICIENT_DATA", "eleven sessions are needed")
    check(BM.zweig_reading([0.5] * 30, LOW, HIGH, WINDOW)["state"] == "NULL", "no oversold reading: waiting")
    check(BM.zweig_reading([0.5] * 20 + [0.2] * 6, LOW, HIGH, WINDOW)["state"] == "ARMED", "the average under 0.40: armed")
    fired = BM.zweig_reading([0.5] * 20 + [0.2] * 6 + [0.95] * 6, LOW, HIGH, WINDOW)
    check(fired["state"] == "FIRED" and fired["current_ema"] > HIGH and fired["min_in_window"] < LOW, "from under 0.40 to over 0.615 inside ten sessions: fired")
    late = BM.zweig_reading([0.5] * 20 + [0.2] * 6 + [0.62] * 14, LOW, HIGH, WINDOW)
    check(late["state"] != "FIRED", "a climb that takes longer than the window does not fire")
    e = BM.ema([1.0, 2.0, 3.0], 3)
    check(e == [1.0, 1.5, 2.25], "the average is seeded with the first value, alpha 2 / (n + 1)")


def test_open_to_close_counts():
    g = [row("A", 10, 11, 400000), row("B", 10, 9, 100000), row("C", 5, 5), row("D", 1, 2, 99999), row("E", 3, 4, 600000), {"T": "F"}]
    d = BM.detail(g)
    check((d["adv"], d["dec"], d["unch"]) == (2, 1, 1), "rose, fell and unchanged; an issue under 100,000 shares is not counted")
    check(d["up_vol_share"] == round(1000000 / 1100000.0, 4), "share of volume in the issues that rose")
    check(BM.detail([])["up_vol_share"] is None and BM.detail([row("A", 1, 1)])["up_vol_share"] is None, "no rising or falling volume: no share, not a zero")


def test_day_over_day_counts():
    prev = BM.closes_of([row("A", 9, 10), row("B", 9, 10), row("C", 9, 10), row("S", 99, 100), row("R", 1, 1), row("Z", 0, 0), {"c": 5}])
    check(prev == {"A": 10, "B": 10, "C": 10, "S": 100, "R": 1}, "prior closes: every issue with a close, whatever its volume")
    g = [row("A", 12, 11, 400000),      # fell from its open, but above yesterday's close: an advance
         row("B", 8, 9, 200000),        # rose from its open, but under yesterday's close: a decline
         row("C", 10, 10), row("N", 5, 6), row("S", 24, 25), row("R", 9, 10), row("T", 1, 2, 5000)]
    d = BM.day_over_day(g, prev)
    check((d["adv_dod"], d["dec_dod"], d["unch_dod"]) == (1, 1, 1), "advance and decline are measured against the prior close, not the open")
    check(d["new_dod"] == 1 and d["set_aside_dod"] == 2, "an issue with no prior close is new; a likely split (x0.25, x10) is set aside, not counted")
    check(d["ratio_dod"] == 0.5 and d["up_vol_share_dod"] == round(400000 / 600000.0, 4), "ratio and up-volume share on the counted issues")
    o = BM.detail(g)
    check((o["adv"], o["dec"]) == (4, 1), "the same session reads differently open-to-close: the two bases are not the same number")
    check(BM.day_over_day([], prev)["ratio_dod"] is None, "nothing counted: no ratio")


def test_bank_plan():
    dates = ["2026-11-%02d" % d for d in range(2, 22)]          # 20 banked sessions
    kept = {d: {"date": d, "ratio": 0.5, "n_stocks": 6000} for d in dates[10:]}
    work, read, left = BM.bank_plan(dates, kept, 19, 140)
    check(work == dates[1:] and left == 0, "every kept session lacks its counts: all 19 are worked, the 20th is only the look-back")
    check(read == set(dates), "each worked session and the one before it are read")
    done = {d: dict(r, adv=1, dec=1, unch=0, adv_dod=1) for d, r in kept.items()}
    done.update({d: {"date": d, "ratio": 0.5, "adv": 1, "adv_dod": 1} for d in dates[1:10]})
    check(BM.bank_plan(dates, done, 19, 140) == ([], set(), 0), "nothing to do once every kept session is complete")
    done.pop(dates[-1])
    work, read, left = BM.bank_plan(dates, done, 19, 140)
    check(work == [dates[-1]] and read == {dates[-2], dates[-1]}, "a new session costs two files: itself and the one before")
    work, read, left = BM.bank_plan(dates, kept, 19, 7)
    check(len(read) <= 7 and work == dates[-6:] and left == 13, "a budget keeps the newest sessions and leaves the rest for the next run")
    short = BM.bank_plan(dates[:5], {}, 19, 140)
    check(short[0] == dates[:5], "a bank shorter than the window: every session is kept, the first has no look-back")


def test_mcclellan():
    rows = [{"date": "2026-%02d-%02d" % (1 + i // 28, 1 + i % 28), "adv_dod": 3000, "dec_dod": 3000} for i in range(38)]
    check(BM.mcclellan(rows)["available"] is False, "39 sessions are needed")
    rows.append({"date": "2026-03-01", "adv_dod": 3000, "dec_dod": 3000})
    m = BM.mcclellan(rows)
    check(m["available"] and m["oscillator"] == 0.0 and not m["seasoned"] and "warm_up_note" in m, "flat breadth reads zero; under 100 sessions it says it is still warming up")
    rows += [{"date": "2026-03-%02d" % d, "adv_dod": 4500, "dec_dod": 1500} for d in range(2, 12)]
    m = BM.mcclellan(rows)
    rana = [0.0] * 39 + [500.0] * 10
    want = round(BM.ema(rana, 19)[-1] - BM.ema(rana, 39)[-1], 1)
    check(m["oscillator"] == want > 0 and m["previous"] < m["oscillator"], "ten strong sessions lift it: the 19-session average less the 39-session one (%s)" % want)
    check(len(m["series"]) == 49 and m["series"][-1] == ["2026-03-11", want] and m["as_of_session"] == "2026-03-11", "the series is dated, newest last")
    check(BM.mcclellan([dict(r, adv_dod=None) for r in rows])["available"] is False, "sessions without day-over-day counts are not used")


def test_day_over_day_reading():
    rows = [{"date": "2026-10-%02d" % d, "ratio": 0.5, "ratio_dod": 0.3} for d in range(1, 9)]
    r = BM.day_over_day_reading(rows, LOW, HIGH, WINDOW)
    check(r["available"] is False and r["n_sessions"] == 8 and "would_read" not in r, "under eleven sessions it says so and reads nothing")
    rows += [{"date": "2026-10-%02d" % d, "ratio": 0.5, "ratio_dod": None} for d in range(9, 12)]
    rows += [{"date": "2026-10-%02d" % d, "ratio": 0.5, "ratio_dod": 0.3} for d in range(12, 20)]
    r = BM.day_over_day_reading(rows, LOW, HIGH, WINDOW)
    check(r["available"] and r["n_sessions"] == 16 and r["would_read"] == "ARMED" and r["ema_10d"] == 0.3, "it reads the day-over-day ratio only, never the open-to-close one")
    check("Information only" in r["note"], "it says it moves no state")


def test_preliminary_reading():
    rows = sorted(FX["history"], key=lambda r: r["date"])
    doc = {"session": "2026-11-25", "as_of": "2026-11-25T21:40:00Z", "delay_minutes": 15,
           "rows": dict([("U%d" % i, [10, 11, 9, 10.5, 300000, 10.2, 0]) for i in range(3600)] + [("D%d" % i, [10, 11, 9, 9.5, 300000, 10.2, 0]) for i in range(2400)] + [("T", [1, 1, 1, 2, 50, 1, 0]), ("X", "bad")])}
    p = BM.preliminary(doc, rows, LOW, HIGH, WINDOW)
    check(p["status"] == "ahead_of_settled" and p["ratio"] == 0.6 and p["n_stocks"] == 6000 and (p["adv"], p["dec"]) == (3600, 2400), "a session newer than the last settled one is read from the forming bars")
    want = BM.zweig_reading([r["ratio"] for r in rows] + [0.6], LOW, HIGH, WINDOW)
    check(p["ema_10d_with_it"] == want["current_ema"] and p["would_read"] == want["state"], "what the average would read with it")
    check(0.9 < p["coverage_vs_typical"] < 1.1 and "Information only" in p["note"], "it says how many issues it covers against a usual session")
    same = BM.preliminary(dict(doc, session=rows[-1]["date"]), rows, LOW, HIGH, WINDOW)
    check(same["status"] == "settled" and same["gap_to_settled"] == round(0.6 - rows[-1]["ratio"], 4) and "would_read" not in same, "the same session as the settled one: only the gap is reported")
    check(BM.preliminary(dict(doc, session="2026-01-02"), rows, LOW, HIGH, WINDOW)["status"] == "older_than_settled", "an old file is called old")
    check(BM.preliminary({"session": "2026-11-25", "rows": {}}, rows, LOW, HIGH, WINDOW) is None and BM.preliminary(None, rows, LOW, HIGH, WINDOW) is None, "no rows, no reading")


def test_engine_source_keeps_its_promises():
    src = (SRC / "lambda_function.py").read_text(encoding="utf-8")
    ast.parse(src)
    check("zweig = evaluate_zweig(zweig_rows)" in src and "[-ZWEIG_ROWS:]" in src and "ZWEIG_ROWS = 60" in src, "the state machine reads the last 60 sessions")
    check("ZWEIG_LOW = 0.40" in src and "ZWEIG_HIGH = 0.615" in src and "ZWEIG_WINDOW = 10" in src, "the thresholds are untouched")
    handler = src[src.index("def lambda_handler"):]
    check(handler.count("if not dry:") == 2 and handler.index("if dry:") < handler.index("s3.put_object") < handler.index("send_telegram(msg)"), "a dry run returns before the report is written or an alert is sent")
    check("if not dry:\n        save_breadth_cache(history)" in handler and "if not dry:\n        save_persistent_state(persistent)" in handler, "a dry run writes neither the cache nor the state")
    check('zweig_state = zweig.get("state", "NULL")' in handler and "day_over_day.get(" not in handler and "preliminary.get(" not in handler and "mcclellan.get(" not in handler, "the state comes from the open-to-close reading alone")
    tree = ast.parse(src)
    fn = next(n for n in ast.walk(tree) if isinstance(n, ast.FunctionDef) and n.name == "evaluate_zweig")
    code = ast.get_source_segment(src, fn)
    ns = {"ZWEIG_WINDOW": WINDOW, "ZWEIG_LOW": LOW, "ZWEIG_HIGH": HIGH, "compute_ema": BM.ema}
    exec(code, ns)
    rows = sorted(FX["history"], key=lambda r: r["date"])
    z = ns["evaluate_zweig"](rows)
    pub = FX["published"]["current_readings"]
    check(z["state"] == "ARMED" and z["current_ema"] == pub["zweig_10d_ema"] and z["min_in_window"] == pub["zweig_window_min"], "the engine's own function gives the published reading on the kept history")
    check(z["current_ema"] == BM.zweig_reading([r["ratio"] for r in rows], LOW, HIGH, WINDOW)["current_ema"], "and the shared arithmetic agrees with it")
    check("11-of-11" not in handler, "the alert no longer states a record the engine did not compute")


def main():
    tests = [v for k, v in sorted(globals().items()) if k.startswith("test_")]
    for t in tests:
        t()
    print("OK %d check groups" % len(tests))


if __name__ == "__main__":
    main()
