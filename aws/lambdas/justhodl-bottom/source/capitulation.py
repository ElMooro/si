"""capitulation -- the panic-print scan behind the chart's "Capitulation · panic volume at the lows" desk (Khalid
2026-11-05: "audit it and see if we can improve it. maybe add etfs, crypto, metal, bonds and other asset class too").

A capitulation print is ONE session, read from the tape alone, on any instrument the bottom desk scores (stocks, equity /
sector / country ETF wrappers, bonds, gold and metals, commodities, currencies, spot crypto):

  * a down session: the close-to-close return at or under the desk's floor (stocks -3%, equity ETFs -2%, countries
    -2.5%, bonds -1%, gold & metals -2.5%, commodities -2.5%, currencies -0.8%, crypto -5%);
  * panic volume: >= 2.5x the prior 50-session average AND the heaviest of the prior 20 sessions (or >= 4x the average
    on its own);
  * a wide spread: the bar's range >= 1.5x the ATR(14) of the bars before it;
  * closed at the lows of the bar: the close in the bottom 30% of the session's range;
  * at the lows of the move: the session's low at or under the prior 20-session low (within 0.5%), or a crash of at
    least twice the desk's floor.

Every print carries its tape (return, volume multiple, range multiple, close location, new 20 / 60-session low, the
drawdown from the prior 20 / 60-session highs, down sessions in a row), a context word -- DECLINE when the sell-off was
already underway before the bar (the prior close at least the desk's decline floor under the prior 20-session high),
SHOCK when the bar itself is the drop (an event gap; a shock in an uptrend is not a bottom process) -- a score 0-100
(volume <= 35, spread <= 20, close location <= 15, new low <= 10, decline depth <= 20), and what the tape did after it
(sessions since, the best close since vs the print's close, the worst low since vs the print's low, whether the low
held, the next five sessions' volume vs the print's).  Prints are not Wyckoff selling climaxes: the full climax gates
(a prolonged decline, a new 120-session low under the 50-session average, the heaviest volume of 60, ...) live in the
bottom desk's detector; a print that is also a climax is linked to that sequence's state.

Breadth: prints per session across the whole loaded window, by desk, as a share of the instruments scored; the 5-session
sum's percentile against every 5-session window loaded gives the regime (QUIET / NORMAL / ELEVATED / HIGH / EXTREME) and
single sessions where a large share of the universe printed are cluster sessions -- the market-wide panic read that the
per-name board cannot give.  Pure functions; the engine feeds bars in and gets the block out.
"""
from __future__ import annotations

from collections import deque
import bisect
import math

VERSION = "1.0.0"
CONTRACT = "capitulation.v1"

P = {
    "atr_n": 14, "vol_n": 50, "vol_dom_n": 20, "hi_short": 20, "hi_long": 60, "lo_short": 20, "lo_long": 60,
    "vol_x_min": 2.5, "vol_x_alone": 4.0, "range_x_min": 1.5, "loc_max": 0.30, "near_low_pct": 0.5,
    "keep_sessions": 60,          # prints carried on the boards
    "breadth_sessions": 120,      # breadth series carried in the artifact (the regime percentile uses the whole window)
    "board_rows": 80,             # per desk
    "latest_rows": 120,
    "latest_sessions": 5,
    "cluster_share_pct": 2.5,     # a single session where this share of the scored universe printed
    "after_n": 5,                 # sessions of volume read after the print
    "bench_sessions": 250,        # benchmark prints carried
    "min_bars": 70,
}
DESK = {
    "stocks": {"ret_min": -3.0, "decline_min": -8.0},
    "equity_etfs": {"ret_min": -2.0, "decline_min": -5.0},
    "countries": {"ret_min": -2.5, "decline_min": -6.0},
    "bonds": {"ret_min": -1.0, "decline_min": -3.0},
    "gold_metals": {"ret_min": -2.5, "decline_min": -6.0},
    "commodities": {"ret_min": -2.5, "decline_min": -6.0},
    "currencies": {"ret_min": -0.8, "decline_min": -2.5},
    "crypto": {"ret_min": -5.0, "decline_min": -12.0},
}
DESK_ORDER = ["stocks", "equity_etfs", "countries", "bonds", "gold_metals", "commodities", "currencies", "crypto"]
REGIMES = ((97.0, "EXTREME"), (90.0, "HIGH"), (75.0, "ELEVATED"), (25.0, "NORMAL"))


def thresholds(desk):
    return DESK.get(desk) or DESK["stocks"]


def rnd(v, n=2):
    if v is None:
        return None
    try:
        v = float(v)
    except (TypeError, ValueError):
        return None
    if math.isnan(v) or math.isinf(v):
        return None
    return round(v, n)


def px(v):
    return rnd(v, 4 if (v is not None and v < 1) else 2)


def median(xs):
    xs = sorted(x for x in xs if x is not None)
    if not xs:
        return None
    n = len(xs)
    return xs[n // 2] if n % 2 else 0.5 * (xs[n // 2 - 1] + xs[n // 2])


# ---- rolling helpers: the bottom desk's own semantics -- a bar never feeds its own gate ------------------------------
def atr_prev(h, l, c, n):
    """Wilder ATR(n) of the bars BEFORE i (bar i's own range is what the gate measures, so it never inflates the ATR)."""
    out = [None] * len(c)
    if len(c) < n + 2:
        return out
    tr = [None] + [max(h[i] - l[i], abs(h[i] - c[i - 1]), abs(l[i] - c[i - 1])) for i in range(1, len(c))]
    a = sum(tr[1:n + 1]) / n
    out[n + 1] = a
    for i in range(n + 2, len(c)):
        a = (a * (n - 1) + tr[i - 1]) / n
        out[i] = a
    return out


def sma_prev(vals, n):
    """mean of the n bars before i."""
    out = [None] * len(vals)
    s = 0.0
    for i in range(len(vals)):
        if i >= n:
            out[i] = s / n
            s -= vals[i - n]
        s += vals[i]
    return out


def rolling_max_prev(vals, n, min_bars):
    """max of the n bars before i (a partial window once min_bars are available)."""
    out = [None] * len(vals)
    dq = deque()
    for i in range(len(vals)):
        while dq and dq[0] < i - n:
            dq.popleft()
        if dq and i >= min(n, min_bars):
            out[i] = vals[dq[0]]
        while dq and vals[dq[-1]] <= vals[i]:
            dq.pop()
        dq.append(i)
    return out


def rolling_min_prev(vals, n, min_bars):
    out = [None] * len(vals)
    dq = deque()
    for i in range(len(vals)):
        while dq and dq[0] < i - n:
            dq.popleft()
        if dq and i >= min(n, min_bars):
            out[i] = vals[dq[0]]
        while dq and vals[dq[-1]] >= vals[i]:
            dq.pop()
        dq.append(i)
    return out


# ---- the print ----------------------------------------------------------------------------------------------------
def score_print(vol_x, range_x, loc, new_low_60, new_low_20, dd60):
    """0-100: volume <= 35 (2.5x -> 10, 7.5x -> 35), spread <= 20 (1.5 ATR -> 8, 4 ATR -> 20), close location <= 15
    (at the low -> 15, 30% up the bar -> 0), new low <= 10 (60-session 10, 20-session 6), decline depth <= 20 (-30% -> 20)."""
    s = min(35.0, 10.0 + 25.0 * max(0.0, vol_x - 2.5) / 5.0)
    s += min(20.0, 8.0 + 12.0 * max(0.0, range_x - 1.5) / 2.5)
    s += 15.0 * max(0.0, 1.0 - loc / P["loc_max"])
    s += 10.0 if new_low_60 else (6.0 if new_low_20 else 0.0)
    s += min(20.0, 20.0 * max(0.0, -dd60) / 30.0) if dd60 is not None else 0.0
    return min(100.0, s)


def scan(o, h, l, c, v, desk):
    """Every capitulation print in the series (dicts with `i` = the bar's index), oldest first. Pure."""
    n = len(c)
    if n < P["min_bars"]:
        return []
    t = thresholds(desk)
    atr = atr_prev(h, l, c, P["atr_n"])
    vavg = sma_prev(v, P["vol_n"])
    vdom = rolling_max_prev(v, P["vol_dom_n"], P["vol_dom_n"])
    hi20 = rolling_max_prev(h, P["hi_short"], P["hi_short"])
    hi60 = rolling_max_prev(h, P["hi_long"], 40)
    lo20 = rolling_min_prev(l, P["lo_short"], P["lo_short"])
    lo60 = rolling_min_prev(l, P["lo_long"], 40)
    out = []
    start = max(P["vol_n"] + 1, P["atr_n"] + 2, P["hi_short"] + 1)
    crash_ret = 2.0 * t["ret_min"]
    for i in range(start, n):
        pc = c[i - 1]
        if pc <= 0 or c[i] <= 0:
            continue
        ret = 100.0 * (c[i] / pc - 1.0)
        if ret > t["ret_min"]:
            continue
        a, va = atr[i], vavg[i]
        if not a or not va or a <= 0 or va <= 0:
            continue
        vol_x = v[i] / va
        dom = vdom[i] is not None and v[i] >= vdom[i]
        if vol_x < P["vol_x_min"] or not (dom or vol_x >= P["vol_x_alone"]):
            continue
        rng = h[i] - l[i]
        range_x = rng / a
        if range_x < P["range_x_min"]:
            continue
        loc = (c[i] - l[i]) / rng if rng > 0 else 1.0
        if loc > P["loc_max"]:
            continue
        lp20, lp60 = lo20[i], lo60[i]
        new_low_20 = lp20 is not None and l[i] <= lp20 * (1.0 + P["near_low_pct"] / 100.0)
        new_low_60 = lp60 is not None and l[i] <= lp60
        crash = ret <= crash_ret
        if not (new_low_20 or crash):
            continue
        hp20, hp60 = hi20[i], hi60[i]
        dd20 = 100.0 * (c[i] / hp20 - 1.0) if hp20 else None
        dd60 = 100.0 * (c[i] / hp60 - 1.0) if hp60 else None
        pre_dd20 = 100.0 * (pc / hp20 - 1.0) if hp20 else None
        context = "DECLINE" if (pre_dd20 is not None and pre_dd20 <= t["decline_min"]) else "SHOCK"
        down, k = 0, i
        while k >= 1 and c[k] < c[k - 1]:
            down += 1
            k -= 1
        out.append({"i": i, "ret_pct": rnd(ret), "vol_x": rnd(vol_x, 1), "vol_dominant": bool(dom), "range_x": rnd(range_x), "loc": rnd(loc, 2),
                    "new_low_20": bool(new_low_20), "new_low_60": bool(new_low_60), "crash": bool(crash),
                    "dd20_pct": rnd(dd20, 1), "dd60_pct": rnd(dd60, 1), "pre_dd20_pct": rnd(pre_dd20, 1),
                    "context": context, "bars_down": down, "score": rnd(score_print(vol_x, range_x, loc, new_low_60, new_low_20, dd60), 1),
                    "close": px(c[i]), "low": px(l[i]), "high": px(h[i]), "volume": rnd(v[i], 0), "atr": rnd(a, 4)})
    return out


def follow_through(i, c, l, v, n_after=None):
    """What the tape did after the print at bar i: sessions since, the best close since vs the print's close (%), the
    worst low since vs the print's low (%), held = no low under the print's low, the next sessions' average volume vs
    the print's, where price is now vs the print's close. None fields when the print is the last bar. Pure."""
    n = len(c)
    since = n - 1 - i
    out = {"sessions_since": since, "bounce_pct": None, "undercut_pct": None, "held": None, "vol_after_x": None, "now_vs_print_pct": None}
    if since <= 0:
        return out
    best = max(c[i + 1:])
    worst = min(l[i + 1:])
    out["bounce_pct"] = rnd(100.0 * (best / c[i] - 1.0))
    out["undercut_pct"] = rnd(100.0 * (worst / l[i] - 1.0))
    out["held"] = bool(worst >= l[i])
    out["now_vs_print_pct"] = rnd(100.0 * (c[-1] / c[i] - 1.0))
    m = n_after or P["after_n"]
    seg = list(v[i + 1:i + 1 + m])
    if seg and v[i] > 0:
        out["vol_after_x"] = rnd((sum(seg) / len(seg)) / v[i])
    return out


def percentile_of(value, population):
    if value is None or not population:
        return None
    below = sum(1 for x in population if x < value)
    ties = sum(1 for x in population if x == value)
    return 100.0 * (below + 0.5 * ties) / len(population)


def regime_of(pct):
    if pct is None:
        return None
    for lim, word in REGIMES:
        if pct >= lim:
            return word
    return "QUIET"


def _print_row(p, c, l, v, sym, desk, asset_class, name, date):
    row = {k: val for k, val in p.items() if k != "i"}
    row.update({"ticker": sym, "name": name, "desk": desk, "asset_class": asset_class, "date": date, "last": px(c[-1]),
                "after": follow_through(p["i"], c, l, v)})
    return row


class Accumulator:
    """Feed every scored instrument in; get the block out. Counts per session are kept for the whole window; the prints
    themselves only inside keep_sessions."""

    def __init__(self, dates):
        self.dates = list(dates)                      # the equity calendar (ISO dates, oldest first)
        self.date_set = set(self.dates)
        self.counts = {}                              # session date -> {desk: n}
        self.scored = {}                              # desk -> instruments scanned
        self.prints = []                              # recent prints with their instrument
        self.errors = 0

    def _bump(self, date, desk):
        d = self.counts.setdefault(date, {})
        d[desk] = d.get(desk, 0) + 1

    def _to_session(self, date):
        """A print on a non-session date (crypto weekends) counts toward the next equity session."""
        if date in self.date_set:
            return date
        k = bisect.bisect_left(self.dates, date)
        return self.dates[k] if k < len(self.dates) else date

    def add(self, sym, desk, asset_class, name, o, h, l, c, v, d_idx, dates):
        """o/h/l/c/v: the instrument's bars; d_idx[k] indexes `dates` (the calendar those bars sit on)."""
        self.scored[desk] = self.scored.get(desk, 0) + 1
        try:
            found = scan(o, h, l, c, v, desk)
        except Exception:  # noqa: BLE001
            self.errors += 1
            return 0
        keep_from = len(c) - 1 - P["keep_sessions"]
        for p in found:
            date = dates[d_idx[p["i"]]]
            self._bump(self._to_session(date), desk)
            if p["i"] >= keep_from:
                self.prints.append(_print_row(p, c, l, v, sym, desk, asset_class, name, date))
        return len(found)

    def breadth(self):
        """Per-session counts on the equity calendar, the 5-session sum, its percentile over the window, the regime."""
        n_scored = sum(self.scored.values()) or 1
        series = []
        for d in self.dates:
            cnt = self.counts.get(d, {})
            series.append({"date": d, "total": sum(cnt.values()), "by_desk": dict(cnt)})
        totals = [s["total"] for s in series]
        roll = [sum(totals[max(0, i - 4):i + 1]) for i in range(len(totals))]
        for s, r5 in zip(series, roll):
            s["sum_5"] = r5
            s["pct_of_scored"] = rnd(100.0 * s["total"] / n_scored, 2)
        pop = roll[P["vol_n"]:] if len(roll) > P["vol_n"] + 60 else roll
        last5 = roll[-1] if roll else 0
        pct = percentile_of(last5, pop)
        clusters = [{"date": s["date"], "total": s["total"], "pct_of_scored": s["pct_of_scored"], "by_desk": s["by_desk"]}
                    for s in series if s["pct_of_scored"] is not None and s["pct_of_scored"] >= P["cluster_share_pct"]]
        spop = sorted(pop)
        return {"n_scored": n_scored, "scored_by_desk": dict(self.scored), "sessions": series[-P["breadth_sessions"]:],
                "last_session": series[-1] if series else None, "sum_5": last5, "sum_5_pct_of_scored": rnd(100.0 * last5 / n_scored, 2),
                "sum_5_percentile": rnd(pct, 1), "regime": regime_of(pct), "window_sessions": len(series),
                "window_max_sum_5": max(pop) if pop else None, "window_median_sum_5": spop[len(spop) // 2] if spop else None,
                "window_p90_sum_5": spop[min(len(spop) - 1, int(0.9 * len(spop)))] if spop else None,
                "clusters": clusters[-12:], "n_clusters_window": len(clusters)}

    def boards(self, wyckoff_by_ticker=None, confirm_fn=None, extra_fn=None):
        """Per-desk boards (newest first, then score) and the latest prints across every desk; each print joined to the
        bottom desk's sequence for the name (same bar = the print IS the selling climax) and to the fleet's evidence."""
        wy = wyckoff_by_ticker or {}
        rows = []
        for p in self.prints:
            r = dict(p)
            w = wy.get(p["ticker"])
            r["wyckoff"] = None
            if w and w.get("sc_date") == p["date"]:
                r["wyckoff"] = {"same_bar": True, "state": w.get("state"), "grade": w.get("grade"), "score": w.get("score"), "frame": w.get("frame"), "actionable": w.get("actionable")}
            elif w and w.get("state"):
                r["wyckoff"] = {"same_bar": False, "state": w.get("state"), "sc_date": w.get("sc_date"), "frame": w.get("frame")}
            r["confirm"], r["n_confirm"] = None, 0
            if confirm_fn:
                try:
                    r["confirm"], r["n_confirm"] = confirm_fn(p["ticker"], p["asset_class"])
                except Exception:  # noqa: BLE001
                    pass
            if extra_fn:
                try:
                    r.update(extra_fn(p["ticker"], p["asset_class"]) or {})
                except Exception:  # noqa: BLE001
                    pass
            rows.append(r)
        rows.sort(key=lambda r: (r["date"], r["score"] or 0), reverse=True)
        by_desk = {}
        for r in rows:
            by_desk.setdefault(r["desk"], []).append(r)
        boards = {d: by_desk.get(d, [])[:P["board_rows"]] for d in DESK_ORDER}
        cut = self.dates[-P["latest_sessions"]] if len(self.dates) >= P["latest_sessions"] else (self.dates[0] if self.dates else "")
        latest = sorted([r for r in rows if r["date"] >= cut], key=lambda r: -(r["score"] or 0))[:P["latest_rows"]]
        return {"by_desk": boards, "counts_60": {d: len(by_desk.get(d, [])) for d in DESK_ORDER}, "latest": latest, "latest_cut": cut, "n_prints_60": len(rows), "rows": rows}


def follow_stats(rows, min_since=5):
    """Of the prints old enough to judge: how many lows held, the median bounce and undercut, how often volume dried up."""
    old = [r for r in rows if (r.get("after") or {}).get("sessions_since", 0) >= min_since]
    if not old:
        return {"n": 0, "min_sessions_since": min_since}
    held = [r for r in old if r["after"].get("held")]
    dry = [r for r in old if r["after"].get("vol_after_x") is not None and r["after"]["vol_after_x"] < 1.0]
    by_ctx = {}
    for ctx in ("DECLINE", "SHOCK"):
        sub = [r for r in old if r["context"] == ctx]
        by_ctx[ctx] = {"n": len(sub), "held_pct": rnd(100.0 * sum(1 for r in sub if r["after"].get("held")) / len(sub), 0) if sub else None,
                       "median_now_vs_print_pct": rnd(median([r["after"].get("now_vs_print_pct") for r in sub]), 1)}
    return {"n": len(old), "min_sessions_since": min_since, "held_pct": rnd(100.0 * len(held) / len(old), 0),
            "median_bounce_pct": rnd(median([r["after"].get("bounce_pct") for r in old]), 1),
            "median_undercut_pct": rnd(median([r["after"].get("undercut_pct") for r in old]), 1),
            "median_now_vs_print_pct": rnd(median([r["after"].get("now_vs_print_pct") for r in old]), 1),
            "volume_dried_up_pct": rnd(100.0 * len(dry) / len(old), 0), "by_context": by_ctx}


def bench_prints(sym, desk, o, h, l, c, v, d_idx, dates, keep=None):
    """The market's own panic prints (benchmarks): every print inside `keep` sessions, newest first."""
    keep = keep or P["bench_sessions"]
    found = scan(o, h, l, c, v, desk)
    keep_from = len(c) - 1 - keep
    out = [_print_row(p, c, l, v, sym, desk, "etf", sym, dates[d_idx[p["i"]]]) for p in found if p["i"] >= keep_from]
    out.reverse()
    return {"last": px(c[-1]), "n_prints_window": len(found), "prints": out}


def read_text(br):
    n5, reg = br["sum_5"], br["regime"]
    last = br.get("last_session") or {}
    by = last.get("by_desk") or {}
    top = sorted(by.items(), key=lambda kv: -kv[1])[:3]
    desks = ", ".join("%s %d" % (k, val) for k, val in top) or "none"
    tail = "last session %s: %d (%s)" % (last.get("date"), last.get("total", 0), desks)
    if reg in ("EXTREME", "HIGH"):
        return "panic is broad: %d prints in the last 5 sessions (%s percentile of the window, %s) -- a market-wide capitulation cluster; %s" % (n5, br["sum_5_percentile"], reg.lower(), tail)
    if reg == "ELEVATED":
        return "panic is spreading: %d prints in the last 5 sessions (%s percentile of the window); %s" % (n5, br["sum_5_percentile"], tail)
    return "selective panic: %d prints in the last 5 sessions (%s percentile of the window, %s); %s" % (n5, br["sum_5_percentile"], (reg or "n/a").lower(), tail)


DEFINITIONS = {
    "print": "one session: a down close at or under the desk's floor (stocks -3%, equity ETFs -2%, countries / gold & metals / commodities -2.5%, bonds -1%, currencies -0.8%, crypto -5%), volume >= 2.5x the prior 50-session average and the heaviest of the prior 20 (or >= 4x on its own), range >= 1.5x the ATR(14) of the bars before it, the close in the bottom 30% of the bar, the low at the prior 20-session low (within 0.5%) or a crash of twice the floor",
    "context": "DECLINE = the sell-off was already underway before the bar (the prior close at least the desk's decline floor under the prior 20-session high: stocks -8%, equity ETFs -5%, countries / gold & metals / commodities -6%, bonds -3%, currencies -2.5%, crypto -12%); SHOCK = the bar itself is the drop (an event gap) -- a shock in an uptrend is not a bottom process",
    "score": "0-100: volume (<= 35: 2.5x = 10, 7.5x = 35) + spread (<= 20: 1.5 ATR = 8, 4 ATR = 20) + close location (<= 15: at the low = 15) + new low (<= 10: 60-session 10, 20-session 6) + decline depth from the prior 60-session high (<= 20: -30% = 20)",
    "after": "sessions since the print; bounce = the best close since vs the print's close; undercut = the worst low since vs the print's low; held = no low under the print's low; vol_after_x = the next five sessions' average volume vs the print's (diminishing volume is what a climax needs); now = the last close vs the print's close",
    "wyckoff": "the bottom desk's sequence for the name when its selling-climax date is the print date (same bar: the print IS the climax; state CLIMAX / TESTING / ST_CONFIRMED / TRIGGERED / MARKUP / FAILED ...); a print without one is a panic session that did not pass the full climax gates (a prolonged decline, a new 120-session low under the 50-session average, the heaviest volume of 60)",
    "breadth": "prints per session across every scored instrument, as a share of the universe; sum_5 = the last five sessions' prints; its percentile against every 5-session window loaded (~5 years) gives the regime: EXTREME >= 97th, HIGH >= 90th, ELEVATED >= 75th, NORMAL, QUIET < 25th; a cluster session is one where >= 2.5% of the universe printed on the same day",
    "follow_through": "of the prints at least 5 sessions old: the share whose low held, the median bounce and undercut, the share whose next-five-session volume ran under the print's -- the base rate of this window's panic, by context",
    "not_a_call": "a capitulation print is where the crowd gave up; it is not a buy. The bottom desk's sequence (automatic rally, quiet secondary test, trigger above the test candle) is the entry logic; the capital authority and risk gate decide whether new risk may be taken at all.",
}


def build(acc, wyckoff_by_ticker=None, confirm_fn=None, extra_fn=None, benchmarks=None):
    br = acc.breadth()
    bd = acc.boards(wyckoff_by_ticker, confirm_fn, extra_fn)
    counts = {}
    for d in DESK_ORDER:
        scored = acc.scored.get(d, 0)
        n5 = sum(1 for r in bd["latest"] if r["desk"] == d)
        last = (br.get("last_session") or {}).get("by_desk", {}).get(d, 0)
        counts[d] = {"scored": scored, "prints_60": bd["counts_60"].get(d, 0), "prints_5": n5, "prints_last_session": last,
                     "prints_5_pct_of_desk": rnd(100.0 * n5 / scored, 1) if scored else None}
    block = {"contract": CONTRACT, "version": VERSION, "desk_order": DESK_ORDER, "thresholds_by_desk": DESK,
             "params": {k: P[k] for k in ("vol_x_min", "vol_x_alone", "range_x_min", "loc_max", "near_low_pct", "keep_sessions", "latest_sessions", "cluster_share_pct", "after_n")},
             "breadth": br, "counts": counts, "latest": bd["latest"], "latest_cut": bd["latest_cut"], "boards": bd["by_desk"],
             "n_prints_60": bd["n_prints_60"], "follow_through": follow_stats(bd["rows"]), "benchmarks": benchmarks or {},
             "scan_errors": acc.errors, "definitions": DEFINITIONS}
    block["read"] = read_text(br)
    return block


def slim(block):
    """The pointer block carried inside bottom.json: the verdict without the boards and the breadth series."""
    br = block["breadth"]
    return {"contract": block["contract"], "version": block["version"], "key": "data/capitulation-desk.json", "read": block["read"],
            "regime": br["regime"], "sum_5": br["sum_5"], "sum_5_percentile": br["sum_5_percentile"], "sum_5_pct_of_scored": br["sum_5_pct_of_scored"],
            "n_scored": br["n_scored"], "last_session": br.get("last_session"), "clusters": br["clusters"][-3:], "counts": block["counts"],
            "n_prints_60": block["n_prints_60"], "n_latest": len(block["latest"]), "follow_through": block["follow_through"],
            "latest": [{k: r.get(k) for k in ("ticker", "name", "desk", "asset_class", "date", "ret_pct", "vol_x", "range_x", "loc", "context", "score", "dd20_pct", "last", "wyckoff", "n_confirm")} for r in block["latest"][:40]]}
