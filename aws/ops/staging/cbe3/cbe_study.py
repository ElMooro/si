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


def month_ends(days, today):
    """The kept days that close a month: the last kept day of every calendar month before the month `today` is in."""
    last = {}
    for d in sorted(days):
        if d[:7] < today[:7]:
            last[d[:7]] = d
    return [last[m] for m in sorted(last)]


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


# ------------------------------------------------------------------------------------------------------- the evidence
LEVEL_BANDS = ((0, 100), (100, 200), (200, 400), (400, 700), (700, 1000), (1000, None))     # spread over Treasuries, basis points
STRESS_BANDS = ((0, 20), (20, 40), (40, 60), (60, 80), (80, 90), (90, 100.01))
BIG = 20.0                  # a fall, a lag or a lead of this many percent is a big move
MIN_UNIVERSE = 50           # a month's "average stock of the board" needs this many stocks
MIN_TAIL_MONTH = 15         # a month counts toward a reading's tail test when the reading holds this many companies
MIN_CELL = 150              # a risk cell with fewer company-months than this borrows its band's figure
MIN_TRAIN_MONTHS = 24       # the walk-forward check forecasts a month only from this many earlier, finished months
THIRD_WORDS = ("calm third", "middle third", "volatile third")


def band_of(spread_bp):
    """The index of the level band a spread falls in."""
    if spread_bp is None:
        return None
    for i, (lo, hi) in enumerate(LEVEL_BANDS):
        if spread_bp >= lo and (hi is None or spread_bp < hi):
            return i
    return None


def band_words(i):
    lo, hi = LEVEL_BANDS[i]
    return ("under %d bp" % hi) if lo == 0 else ("%s bp or more" % format(lo, ",")) if hi is None else ("%d to %s bp" % (lo, format(hi, ",")))


def thirds(values):
    """The two cuts that split a set of figures into thirds; None under 30 figures."""
    xs = sorted(v for v in values if v is not None)
    return (xs[len(xs) // 3], xs[2 * len(xs) // 3]) if len(xs) >= 30 else None


def third_of(v, cuts):
    return None if v is None or cuts is None else 0 if v < cuts[0] else 1 if v < cuts[1] else 2


def prepare(panel):
    """Add to every row what the evidence reads: its return less the board's average stock that month (u1, u3), less the
    middle stock of its industry (i1, i3), its level band, and the third its own volatility stood in that month. In place."""
    by = defaultdict(list)
    for r in panel:
        by[r["day"]].append(r)
    for day, rows in by.items():
        cuts = thirds([r.get("vol") for r in rows])
        for r in rows:
            r["band"], r["vt"] = band_of(r.get("s")), third_of(r.get("vol"), cuts)
        for k in ("1", "3"):
            xs = [r["r" + k] for r in rows if r.get("r" + k) is not None]
            m = sum(xs) / len(xs) if len(xs) >= MIN_UNIVERSE else None
            for r in rows:
                r["u" + k] = round(r["r" + k] - m, 2) if m is not None and r.get("r" + k) is not None else None
    with_industry(panel)
    return panel


def pctl(xs, p):
    xs = sorted(xs)
    return xs[min(len(xs) - 1, max(0, int(round(p * (len(xs) - 1)))))] if xs else None


def _share(flags):
    return 100.0 * sum(1 for x in flags if x) / len(flags) if flags else None


def summary(rows, k, universe=None):
    """What the stocks of a set of company-months did over one horizon (k "1" or "3"), against the board's average stock.

    mean / median / p10 / p90: the return less the board's average stock that month, percent. lag_big / beat_big: the share that
    ended 20% or more behind / ahead of it. fell_big: the share whose stock was at some point 20% or more under where it started.
    t: the month-by-month average of the mean over its standard error. t_fell: the same for the share that fell, less the
    whole board's share that month - how sure the difference from the board is. vs_market: the mean against SPY instead.
    """
    u = [r["u" + k] for r in rows if r.get("u" + k) is not None]
    if not u:
        return {"n": 0}
    months = defaultdict(list)
    for r in rows:
        if r.get("u" + k) is not None:
            months[r["day"]].append(r)
    monthly = [mean([r["u" + k] for r in rs]) for _, rs in sorted(months.items()) if len(rs) >= MIN_BUCKET_MONTH]
    dd = [r["dd" + k] for r in rows if r.get("dd" + k) is not None]
    out = {"n": len(u), "months": len(months), "mean": r2(mean(u)), "median": r2(median(u)), "p10": r2(pctl(u, 0.10), 1), "p90": r2(pctl(u, 0.90), 1),
           "lag_big": r2(_share([x <= -BIG for x in u]), 1), "beat_big": r2(_share([x >= BIG for x in u]), 1), "fell_big": r2(_share([x <= -BIG for x in dd]), 1),
           "t": r2(t_stat(monthly)), "months_counted": len(monthly),
           "vs_market": r2(mean([r["e" + k] for r in rows if r.get("e" + k) is not None])), "vs_industry": r2(mean([r["i" + k] for r in rows if r.get("i" + k) is not None])),
           "ended": sum(1 for r in rows if r.get("end" + k))}
    if universe is not None:
        diffs = []
        for day, rs in sorted(months.items()):
            mine = [r["dd" + k] <= -BIG for r in rs if r.get("dd" + k) is not None]
            every = universe.get((day, k))
            if len(mine) >= MIN_TAIL_MONTH and every is not None:
                diffs.append(_share(mine) - every)
        out["t_fell"], out["fell_vs_board"] = r2(t_stat(diffs)), r2(mean(diffs), 1)
    return out


def fell_by_month(panel):
    """{(month-end, horizon): the share of the whole board whose stock fell 20% at some point} - what a reading's own share is set against."""
    out = {}
    by = defaultdict(list)
    for r in panel:
        by[r["day"]].append(r)
    for day, rows in by.items():
        for k in ("1", "3"):
            flags = [r["dd" + k] <= -BIG for r in rows if r.get("dd" + k) is not None]
            if len(flags) >= MIN_UNIVERSE:
                out[(day, k)] = _share(flags)
    return out


def reading_keys(r):
    """The measured readings a company-month (or a company today) falls under, as keys."""
    keys = ["book:" + r["book"]] if r.get("book") else []
    if r.get("cell"):
        keys.append("cell:" + r["cell"])
    if r.get("sig") in C.LEADS:
        keys.append("signal:" + r["sig"])
    b = r.get("band") if "band" in r else band_of(r.get("s"))
    if b is not None:
        keys.append("level:%d" % b)
    if r.get("stress") is not None:
        for lo, hi in STRESS_BANDS:
            if lo <= r["stress"] < hi:
                keys.append("stress:%g-%g" % (lo, min(hi, 100)))
    if r.get("dist"):
        keys.append("distressed")
    return keys


READING_GROUPS = (("level", "By how wide the spread is"), ("cells", "By what credit and the stock just did"), ("signals", "The board's leads"), ("stress", "By stress rank"), ("book", "By book"))


def reading_words(key):
    kind, _, rest = key.partition(":")
    if kind == "cell":
        return C.CELLS.get(rest, rest)
    if kind == "signal":
        return {"CREDIT_LEADS_DOWN": "Lead: credit worse, the stock has not fallen", "CREDIT_LEADS_UP": "Lead: credit better, the stock has not risen"}.get(rest, rest)
    if kind == "level":
        return "Spread " + band_words(int(rest))
    if kind == "stress":
        return "Stress rank " + rest.replace("-", " to ")
    if kind == "book":
        return C.BOOK_WORDS.get(rest, rest)
    return "Distressed (1,000 bp or more)" if key == "distressed" else key


def reading_group(key):
    kind = key.partition(":")[0]
    return {"cell": "cells", "signal": "signals", "level": "level", "stress": "stress", "book": "book", "distressed": "level"}.get(kind, kind)


def risk_table(rows, k="3"):
    """The chance that a stock falls 20% at some point inside the horizon, by how wide its bonds trade and by the third its own
    volatility stands in: {band: {"n", "fell", "thirds": [{"n", "fell"} or None x3]}}. A thin cell is left empty: its band's figure stands."""
    out = {}
    for b in range(len(LEVEL_BANDS)):
        mine = [r for r in rows if r.get("band") == b and r.get("dd" + k) is not None]
        if not mine:
            continue
        cell = {"n": len(mine), "fell": r2(_share([r["dd" + k] <= -BIG for r in mine]), 1), "thirds": []}
        for t in range(3):
            sub = [r for r in mine if r.get("vt") == t]
            cell["thirds"].append({"n": len(sub), "fell": r2(_share([r["dd" + k] <= -BIG for r in sub]), 1)} if len(sub) >= MIN_CELL else None)
        out[b] = cell
    return out


def chance(table, band, third):
    """(the chance from the risk table for a band and a volatility third, what it rests on): the cell when it is thick enough, else the band."""
    cell = (table or {}).get(band) if band is not None else None
    if not cell:
        return None, None
    sub = cell["thirds"][third] if third is not None and third < len(cell["thirds"]) else None
    return (sub["fell"], "band and volatility") if sub else (cell["fell"], "band")


CHECK_EDGES = (0, 8, 18, 35, 101)          # the forecast, in four bands, for setting what was said beside what happened


def _table_from(counts):
    """A risk table from running counts {(band, third or None): [n, fell]} - the same figures risk_table() gives."""
    out = {}
    for b in range(len(LEVEL_BANDS)):
        n, fell = counts.get((b, None), (0, 0))
        if not n:
            continue
        cell = {"n": n, "fell": r2(100.0 * fell / n, 1), "thirds": []}
        for t in range(3):
            tn, tf = counts.get((b, t), (0, 0))
            cell["thirds"].append({"n": tn, "fell": r2(100.0 * tf / tn, 1)} if tn >= MIN_CELL else None)
        out[b] = cell
    return out


def walk_forward(panel, k="3"):
    """The risk table tried the only honest way: each month's chances come from a table built on earlier months whose outcome was
    already known that day, and are then set beside what happened. Returns the calibration (said / happened, by how high the
    chance said was), and how often a stock that went on to fall had been given a higher chance than one that did not (auc)."""
    days = sorted({r["day"] for r in panel})
    lag = {"1": 2, "3": 4}[k]                     # month-ends that must pass before a month's outcome is known
    by = defaultdict(list)
    for r in panel:
        if r.get("dd" + k) is not None and r.get("band") is not None:
            by[r["day"]].append(r)
    counts, added, scored = defaultdict(lambda: [0, 0]), 0, []
    for i, day in enumerate(days):
        known = max(0, i - lag + 1)               # the months whose outcome was known on this day
        while added < known:
            for r in by.get(days[added], ()):
                fell = 1 if r["dd" + k] <= -BIG else 0
                counts[(r["band"], None)][0] += 1                          # the band, whatever the stock's own volatility
                counts[(r["band"], None)][1] += fell
                if r.get("vt") is not None:                                # and the band's cell for its volatility third
                    counts[(r["band"], r["vt"])][0] += 1
                    counts[(r["band"], r["vt"])][1] += fell
            added += 1
        if known < MIN_TRAIN_MONTHS:
            continue
        table = _table_from(counts)
        for r in by.get(day, ()):
            p, _ = chance(table, r["band"], r.get("vt"))
            if p is not None:
                scored.append((p, r["dd" + k] <= -BIG, day))
    if len(scored) < 500:
        return None
    buckets = []
    for lo, hi in zip(CHECK_EDGES, CHECK_EDGES[1:]):
        mine = [x for x in scored if lo <= x[0] < hi]
        if mine:
            buckets.append({"from": lo, "to": min(hi, 100), "n": len(mine), "said": r2(mean([x[0] for x in mine]), 1), "happened": r2(_share([x[1] for x in mine]), 1)})
    # how often the faller had the higher chance: the rank statistic, ties counting half
    order = sorted(scored, key=lambda x: x[0])
    rank_sum, i, n = 0.0, 0, len(order)
    while i < n:
        j = i
        while j + 1 < n and order[j + 1][0] == order[i][0]:
            j += 1
        rank_sum += ((i + j) / 2.0 + 1.0) * sum(1 for x in order[i:j + 1] if x[1])
        i = j + 1
    pos = sum(1 for x in scored if x[1])
    neg = n - pos
    auc = (rank_sum - pos * (pos + 1) / 2.0) / (pos * neg) if pos and neg else None
    base = _share([x[1] for x in scored])
    brier = mean([((x[0] / 100.0) - (1.0 if x[1] else 0.0)) ** 2 for x in scored])
    flat = mean([((base / 100.0) - (1.0 if x[1] else 0.0)) ** 2 for x in scored])
    return {"n": n, "months": len({x[2] for x in scored}), "first": min(x[2] for x in scored), "last": max(x[2] for x in scored), "fell": r2(base, 1), "buckets": buckets,
            "auc": r2(100.0 * auc, 1) if auc is not None else None, "skill": r2(100.0 * (1.0 - brier / flat), 1) if flat else None, "train_months": MIN_TRAIN_MONTHS}


def evidence(panel, cds_panel_rows=None):
    """Everything the board measured about what stocks did after each kind of reading. The panel is read, never changed in meaning."""
    prepare(panel)
    days = sorted({r["day"] for r in panel})
    universe = fell_by_month(panel)
    done = {k: sorted({r["day"] for r in panel if r.get("u" + k) is not None}) for k in ("1", "3")}
    buckets = defaultdict(list)
    for r in panel:
        for key in reading_keys(r):
            buckets[key].append(r)
    readings = []
    for key in sorted(buckets, key=lambda x: (reading_group(x), x != "distressed", x)):
        rows = buckets[key]
        readings.append({"key": key, "group": reading_group(key), "words": reading_words(key), "h1": summary(rows, "1", universe), "h3": summary(rows, "3", universe)})
    order = {"level": [("level:%d" % i) for i in range(len(LEVEL_BANDS))] + ["distressed"], "cells": ["cell:" + c for c in C.CELLS],
             "signals": ["signal:CREDIT_LEADS_DOWN", "signal:CREDIT_LEADS_UP"], "stress": ["stress:%g-%g" % (lo, min(hi, 100)) for lo, hi in STRESS_BANDS], "book": ["book:ig", "book:hy", "book:em"]}
    rank = {key: (gi, i) for gi, (g, _) in enumerate(READING_GROUPS) for i, key in enumerate(order[g])}
    readings.sort(key=lambda x: rank.get(x["key"], (9, 0)))
    risk = {"h1": risk_table(panel, "1"), "h3": risk_table(panel, "3")}
    out = {
        "first": days[0] if days else None, "last": days[-1] if days else None, "month_ends": len(days), "company_months": len(panel), "companies": len({r["t"] for r in panel}),
        "horizons": [{"key": key, "words": words, "sessions": n, "k": key[1:], "months": len(done[key[1:]]), "last_scored": done[key[1:]][-1] if done[key[1:]] else None} for key, words, n in HORIZONS],
        "big": BIG, "entry": "the close one session after the holdings day", "groups": [{"key": g, "words": w} for g, w in READING_GROUPS],
        "board": {"h1": summary(panel, "1"), "h3": summary(panel, "3")},
        "readings": readings,
        "risk": {"bands": [{"i": i, "words": band_words(i), "from": lo, "to": hi} for i, (lo, hi) in enumerate(LEVEL_BANDS)], "thirds": list(THIRD_WORDS), "min_cell": MIN_CELL,
                 "h1": {str(b): v for b, v in risk["h1"].items()}, "h3": {str(b): v for b, v in risk["h3"].items()}},
        "check": {"h1": walk_forward(panel, "1"), "h3": walk_forward(panel, "3")},
        "stress_bands": [[lo, min(hi, 100)] for lo, hi in STRESS_BANDS],
    }
    out["direction"] = direction_verdict(out)
    if cds_panel_rows is not None:
        out["cds"] = cds_evidence(cds_panel_rows)
    return out


def direction_verdict(ev):
    """Whether any reading that looks at what credit just did tells the direction of the stock, in a sentence that the figures back."""
    tested = [x for x in ev["readings"] if x["group"] in ("cells", "signals")]
    sure = [x for x in tested for h in ("h1", "h3") if (x[h].get("t") is not None and abs(x[h]["t"]) >= 3.0)]
    down = next((x for x in ev["readings"] if x["key"] == "signal:CREDIT_LEADS_DOWN"), None)
    up = next((x for x in ev["readings"] if x["key"] == "signal:CREDIT_LEADS_UP"), None)
    return {"tested": len(tested) * 2, "past_three": len(sure), "bar": 3.0,
            "words": ("Of the %d readings of what credit and the stock just did, tried over one month and over three, %s the stock's direction with a margin past chance (3 standard errors, the bar for "
                      "%d tries at once)." % (len(tested), "none tells" if not sure else "%d tell" % len(sure), len(tested) * 2)),
            "lead_down": (down or {}).get("h1"), "lead_up": (up or {}).get("h1")}


def cds_evidence(rows):
    """What the stock did after a week in which its traded CDS moved: the board's CDS rule (10 bp and 10% of its level), week by week."""
    by = defaultdict(list)
    for r in rows:
        by[r["day"]].append(r)
    for day, rs in by.items():
        for key, _, _ in CDS_HORIZONS:
            xs = [r["r" + key] for r in rs if r.get("r" + key) is not None]
            m = sum(xs) / len(xs) if len(xs) >= 15 else None
            for r in rs:
                r["u" + key] = round(r["r" + key] - m, 2) if m is not None and r.get("r" + key) is not None else None

    def wider(r):
        return r["d"] >= C.CDS_MOVE_BP and r["p"] >= 100.0 * C.CDS_MOVE_REL

    def tighter(r):
        return r["d"] <= -C.CDS_MOVE_BP and r["p"] <= -100.0 * C.CDS_MOVE_REL

    def stock(r):
        return None if r.get("px") is None else "down" if r["px"] <= -C.FLAT_BAND_PCT else "up" if r["px"] >= C.FLAT_BAND_PCT else "flat"

    tests = [("wider", "CDS wider over the week", wider), ("wider/up", "CDS wider, stock rose", lambda r: wider(r) and stock(r) == "up"), ("wider/flat", "CDS wider, stock flat", lambda r: wider(r) and stock(r) == "flat"),
             ("wider/down", "CDS wider, stock already fell", lambda r: wider(r) and stock(r) == "down"), ("tighter", "CDS tighter over the week", tighter),
             ("tighter/down", "CDS tighter, stock fell", lambda r: tighter(r) and stock(r) == "down"), ("tighter/flat", "CDS tighter, stock flat", lambda r: tighter(r) and stock(r) == "flat"),
             ("tighter/up", "CDS tighter, stock already rose", lambda r: tighter(r) and stock(r) == "up")]
    out = []
    for key, words, test in tests:
        mine = [r for r in rows if test(r)]
        row = {"key": key, "words": words}
        for hk, _, _ in CDS_HORIZONS:
            u = [r["u" + hk] for r in mine if r.get("u" + hk) is not None]
            weeks = defaultdict(list)
            for r in mine:
                if r.get("u" + hk) is not None:
                    weeks[r["day"]].append(r["u" + hk])
            weekly = [mean(v) for _, v in sorted(weeks.items()) if len(v) >= 3]
            row[hk] = {"n": len(u), "weeks": len(weekly), "mean": r2(mean(u)), "median": r2(median(u)), "t": r2(t_stat(weekly))}
        out.append(row)
    weeks = sorted(by)
    return {"first": weeks[0] if weeks else None, "last": weeks[-1] if weeks else None, "weeks": len(weeks), "names": len({r["t"] for r in rows}), "name_weeks": len(rows),
            "horizons": [{"key": k, "words": w, "sessions": n} for k, w, n in CDS_HORIZONS], "readings": out,
            "rule": "wider or tighter by %g bp and by %g%% of its level over five sessions" % (C.CDS_MOVE_BP, 100.0 * C.CDS_MOVE_REL)}
