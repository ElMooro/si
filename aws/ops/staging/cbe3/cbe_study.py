"""cbe_study - what the stock did after each kind of credit reading. Pure: no I/O.

The board's premise is that the bond desk reprices a company before the equity desk does. This module measures it on the
board's own history instead of asserting it. The history is the bond desk's month-end holdings since 2021 (kept under
credit/cbe-holdings/), read with the engine's own arithmetic: at every month-end the board is rebuilt exactly as the live
run builds it - the same spread, the same company roll-up, the same book, the same rule - and each listed company's reading
is set beside what its stock did afterwards.

  the panel     one row a listed company a month-end: the reading as it stood that day, and the stock's return afterwards
  the entry     one session AFTER the holdings day (a fund publishes a day's holdings the next morning), at the close
  the horizons  21 and 63 sessions from the entry
  the measure   the stock's return less SPY's over the same sessions ("against the market"), and less the middle stock of
                its own industry on the board that month ("against its industry"). Price returns: dividends are not in them.
  a stock that stopped trading inside the horizon (bought, merged, bankrupt) leaves at its last print and is counted

What it cannot see, and says so wherever a figure is shown: a company is tied to a listed stock through TODAY's listings, so
one that has since been delisted is not in the panel - which leaves out the bankruptcies (the worst outcomes after a bad
credit reading) and the take-overs (among the best). Five years is one credit cycle at most.
"""
from collections import defaultdict
from bisect import bisect_left, bisect_right
import math

import cbe_core as C

HORIZONS = (("h1", "1 month", 21), ("h3", "3 months", 63))
SKIP = 1                    # sessions between the holdings day and the entry
MIN_BUCKET_MONTH = 5        # a month counts toward a reading's month-by-month average when it holds this many companies
MIN_FIT_MONTHS = 24         # the walk-forward model predicts a month only from this many earlier months
VOL_SESSIONS = 63           # the stock's own volatility is read over this many sessions before the reading
PANEL_COLS = ["day", "t", "book", "ind", "v", "n", "s", "d1", "x1", "p1", "d3", "x3", "dy", "py", "stress", "rl", "ry", "rm", "dir", "px1", "px3", "sig", "cell", "dist",
              "due2y", "step", "curve", "pxb", "vol", "r1", "m1", "e1", "dd1", "r3", "m3", "e3", "dd3", "end1", "end3"]

# the traded CDS, week by week
CDS_STEP = 5                # sessions between two observations of a CDS
CDS_HORIZONS = (("w1", "1 week", 5), ("w2", "2 weeks", 10), ("m1", "1 month", 21))
CDS_MIN_PRINTS = 20         # a name with fewer prints than this is not studied
CDS_PANEL_COLS = ["day", "t", "s", "d", "p", "px", "rw1", "ew1", "rw2", "ew2", "rm1", "em1"]


class Tape:
    """The session calendar (the market fund's own days) and returns measured on it."""

    def __init__(self, market):
        self.market = market or {}
        self.days = sorted(self.market)
        self._sorted = {}                # a stock's days, sorted once: id(its closes) -> (the closes, how many, the days)

    def session_on_or_before(self, day):
        i = bisect_right(self.days, day) - 1
        return i if i >= 0 else None

    def window(self, day, n, skip=SKIP):
        """(entry day, exit day) for a reading dated `day`: entry `skip` sessions later, exit `n` sessions after the entry. None when the calendar ends first."""
        i = self.session_on_or_before(day)
        if i is None or i + skip + n >= len(self.days):
            return None, None
        return self.days[i + skip], self.days[i + skip + n]

    def days_of(self, closes):
        key = id(closes)
        got = self._sorted.get(key)
        if got is None or got[0] is not closes or got[1] != len(closes):          # the dict itself is held, so its id cannot be handed to another
            got = self._sorted[key] = (closes, len(closes), sorted(closes))
        return got[2]

    def close_on(self, closes, day, slack=5):
        """(the close on `day`, or the last one before it inside `slack` calendar days, None otherwise; the day used)."""
        if not closes:
            return None, None
        if day in closes:
            return closes[day], day
        days = self.days_of(closes)
        i = bisect_right(days, day) - 1
        if i < 0 or C.days_between(days[i], day) > slack:
            return None, None
        return closes[days[i]], days[i]

    def forward(self, closes, day, n, skip=SKIP):
        """(return % from the entry close to the exit close, ended?) for one stock. A stock whose prints stop before the exit
        leaves at its last print (ended True). None when it has no entry print or the calendar ends before the exit."""
        entry, exit_ = self.window(day, n, skip)
        if entry is None or not closes:
            return None, False
        a, _ = self.close_on(closes, entry, slack=5)
        if a is None or a <= 0:
            return None, False
        b, used = self.close_on(closes, exit_, slack=5)
        if b is not None:
            return round(100.0 * (b / a - 1.0), 2), False
        last = self.days_of(closes)[-1]
        if entry < last < exit_:                      # it stopped trading inside the horizon
            return round(100.0 * (closes[last] / a - 1.0), 2), True
        return None, False

    def volatility(self, closes, day, n=VOL_SESSIONS):
        """The stock's own volatility before `day`: the standard deviation of its last n daily changes, as a yearly percent. None under 40 changes."""
        if not closes:
            return None
        days = self.days_of(closes)
        i = bisect_right(days, day)
        window = [closes[d] for d in days[max(0, i - n - 1):i]]
        moves = [math.log(b / a) for a, b in zip(window, window[1:]) if a > 0 and b > 0]
        if len(moves) < 40:
            return None
        m = sum(moves) / len(moves)
        return round(100.0 * math.sqrt(sum((x - m) ** 2 for x in moves) / (len(moves) - 1)) * math.sqrt(252.0), 1)

    def worst(self, closes, day, n, skip=SKIP):
        """The stock's lowest close inside the horizon against its entry close, in percent (0 when it never closed under it). None with no entry print."""
        entry, exit_ = self.window(day, n, skip)
        if entry is None or not closes:
            return None
        a, used = self.close_on(closes, entry, slack=5)
        if a is None or a <= 0:
            return None
        days = self.days_of(closes)
        inside = [closes[d] for d in days[bisect_left(days, used):bisect_right(days, exit_)]]
        return round(100.0 * (min(inside) / a - 1.0), 2) if inside else None

    def change(self, closes, a, b):
        """Return % between the last close on or before `a` and the last on or before `b` (each inside 6 days)."""
        x, _ = self.close_on(closes, a, slack=6)
        y, _ = self.close_on(closes, b, slack=6)
        if x is None or y is None or x <= 0:
            return None
        return round(100.0 * (y / x - 1.0), 2)


def panel_rows(day, windows, rows, closes, tape):
    """The panel's rows for one month-end: every listed company of that day's board with a stock in the price bank."""
    out = []
    market = {}
    for key, _, n in HORIZONS:
        market[key] = tape.forward(tape.market, day, n)[0]
    for r in rows:
        tk = r.get("ticker")
        cl = closes.get(tk) if tk else None
        if not cl or r.get("spread_bp") is None:
            continue
        win = r.get("win") or {}
        m1, m3, ytd = win.get("m1") or {}, win.get("m3") or {}, win.get("ytd") or {}
        px1 = tape.change(cl, windows.get("m1"), day) if windows.get("m1") else None
        px3 = tape.change(cl, windows.get("m3"), day) if windows.get("m3") else None
        direction, _ = C.credit_direction(m1)
        signal, _ = C.signal_of(direction, px1)
        row = {"day": day, "t": tk, "book": r["book"], "ind": r.get("industry"), "v": r.get("value"), "n": r.get("n_bonds"), "s": r["spread_bp"],
               "d1": m1.get("d_bp"), "x1": m1.get("x_bp"), "p1": m1.get("d_pct"), "d3": m3.get("d_bp"), "x3": m3.get("x_bp"), "dy": ytd.get("d_bp"), "py": ytd.get("d_pct"),
               "stress": r.get("stress"), "rl": r.get("rk_level"), "ry": r.get("rk_year"), "rm": r.get("rk_month"), "dir": direction, "px1": px1, "px3": px3,
               "sig": signal, "cell": C.cell_of(direction, px1), "dist": 1 if r.get("distressed") else 0,
               "due2y": r.get("due2y"), "step": r.get("step_bp"), "curve": r.get("curve_bp"), "pxb": r.get("px"), "vol": tape.volatility(cl, day)}
        for key, _, n in HORIZONS:
            ret, ended = tape.forward(cl, day, n)
            k = key[1:]
            row["r" + k], row["m" + k], row["end" + k] = ret, market[key], 1 if ended else 0
            row["e" + k] = round(ret - market[key], 2) if ret is not None and market[key] is not None else None
            row["dd" + k] = tape.worst(cl, day, n) if ret is not None else None
        out.append(row)
    return out


def cds_panel(series, closes, tape):
    """The traded CDS of each tied name, one row a week: what the CDS did over the week, what the stock did the same week, and
    what the stock did afterwards.

    series: {ticker: [[day, spread in bp], ...]} - the CDS desk's prints. A week counts when the CDS printed inside it and inside
    the week before it; the weeks are the same for every name (every fifth session, counted back from the newest)."""
    sessions = tape.days
    last = len(sessions) - 1
    rows = []
    for tk in sorted(series):
        cl = closes.get(tk)
        pts = sorted((p[0], float(p[1])) for p in series[tk] or [] if isinstance(p, (list, tuple)) and len(p) >= 2 and isinstance(p[1], (int, float)) and 0 < p[1] <= C.CDS_BELIEVABLE_BP)
        if not cl or len(pts) < CDS_MIN_PRINTS:
            continue
        days = [p[0] for p in pts]

        def level(i):                       # the last print inside the week that ends at session i
            j = bisect_right(days, sessions[i]) - 1
            return pts[j][1] if j >= 0 and i - CDS_STEP >= 0 and days[j] > sessions[i - CDS_STEP] else None

        first = bisect_left(sessions, days[0])
        i = last - ((last - first) // CDS_STEP) * CDS_STEP
        while i <= last:
            a = level(i - CDS_STEP) if i - CDS_STEP >= 0 else None
            b = level(i)
            if a is not None and b is not None:
                row = {"day": sessions[i], "t": tk, "s": round(b, 1), "d": round(b - a, 1), "p": round(100.0 * (b - a) / a, 1), "px": tape.change(cl, sessions[i - CDS_STEP], sessions[i])}
                for key, _, n in CDS_HORIZONS:
                    ret, _ = tape.forward(cl, sessions[i], n)
                    mkt, _ = tape.forward(tape.market, sessions[i], n)
                    row["r" + key], row["e" + key] = ret, (round(ret - mkt, 2) if ret is not None and mkt is not None else None)
                rows.append(row)
            i += CDS_STEP
    return rows


def to_table(panel, cols=None):
    cols = cols or PANEL_COLS
    return {"cols": cols, "rows": [[r.get(c) for c in cols] for r in panel]}


def from_table(table):
    cols = table["cols"]
    return [dict(zip(cols, r)) for r in table["rows"]]


# ------------------------------------------------------------------------------------------------------- statistics
def mean(xs):
    xs = [x for x in xs if x is not None]
    return sum(xs) / len(xs) if xs else None


def median(xs):
    xs = sorted(x for x in xs if x is not None)
    if not xs:
        return None
    n = len(xs)
    return xs[n // 2] if n % 2 else (xs[n // 2 - 1] + xs[n // 2]) / 2.0


def stdev(xs):
    xs = [x for x in xs if x is not None]
    if len(xs) < 2:
        return None
    m = sum(xs) / len(xs)
    return math.sqrt(sum((x - m) ** 2 for x in xs) / (len(xs) - 1))


def t_stat(xs):
    """The mean of a series of monthly figures over its standard error: how far from nothing the average is."""
    xs = [x for x in xs if x is not None]
    sd = stdev(xs)
    if sd is None or sd == 0:
        return None
    return (sum(xs) / len(xs)) / (sd / math.sqrt(len(xs)))


def r2(x, places=2):
    return None if x is None else round(x, places)


def with_industry(panel):
    """Add to every row its return less the middle stock of its industry on the board that month (i1, i3). In place."""
    for key in ("1", "3"):
        per = defaultdict(list)
        for r in panel:
            if r.get("r" + key) is not None and r.get("ind"):
                per[(r["day"], r["ind"])].append(r["r" + key])
        mid = {k: median(v) for k, v in per.items() if len(v) >= 5}
        for r in panel:
            m = mid.get((r["day"], r.get("ind")))
            r["i" + key] = round(r["r" + key] - m, 2) if r.get("r" + key) is not None and m is not None else None
    return panel


def describe(rows, key="1"):
    """What the stocks of a set of panel rows did over one horizon: counts, averages, and the month-by-month test."""
    e = [r["e" + key] for r in rows if r.get("e" + key) is not None]
    if not e:
        return {"n": 0}
    by_month = defaultdict(list)
    for r in rows:
        if r.get("e" + key) is not None:
            by_month[r["day"]].append(r)
    monthly = [mean([r["e" + key] for r in rs]) for d, rs in sorted(by_month.items()) if len(rs) >= MIN_BUCKET_MONTH]
    ind = [r["i" + key] for r in rows if r.get("i" + key) is not None]
    return {"n": len(e), "months": len(by_month), "months_counted": len(monthly),
            "mean": r2(mean(e)), "median": r2(median(e)), "lagged_pct": r2(100.0 * sum(1 for x in e if x < 0) / len(e), 1),
            "raw": r2(mean([r["r" + key] for r in rows if r.get("r" + key) is not None])),
            "vs_industry": r2(mean(ind)), "vs_industry_median": r2(median(ind)),
            "monthly_mean": r2(mean(monthly)), "t": r2(t_stat(monthly)), "months_lagged_pct": r2(100.0 * sum(1 for x in monthly if x < 0) / len(monthly), 1) if monthly else None,
            "worst_decile": r2(sorted(e)[int(0.1 * (len(e) - 1))]), "best_decile": r2(sorted(e)[int(0.9 * (len(e) - 1))]),
            "ended": sum(1 for r in rows if r.get("end" + key))}


def quantile_edges(values, k):
    xs = sorted(v for v in values if v is not None)
    if len(xs) < k:
        return []
    return [xs[int(len(xs) * i / k)] for i in range(1, k)]


def bucket_of(v, edges):
    return None if v is None else bisect_right(edges, v)
