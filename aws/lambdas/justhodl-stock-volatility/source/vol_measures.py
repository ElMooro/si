"""vol_measures - the pure arithmetic of the stock volatility desk: realized volatility and the price spread.

Every function reads plain lists of daily prices (oldest first) and returns a number or None. Nothing reads S3 and
nothing is estimated: a figure that needs more sessions than the lists hold is None.

VOLATILITY   how much a price moves from one close to the next. Stated the way an option desk states it: the sample
             standard deviation of daily log returns, times the square root of 252, in percent a year.
               hv(C, 21)          close to close over the last 21 returns (about a month); 63 a quarter; 252 a year
               yang_zhang(...)    the same quantity from the whole bar - overnight gap, open-to-close move and the
                                  intraday range (Yang and Zhang, 2000) - so a stock that gaps and reverses inside the
                                  day is not read as quiet
PRICE SPREAD how far apart the high and the low of a window stand: (highest high / lowest low - 1) x 100, the percent
             the high stands above the low. One session is the day's own bar; 5 sessions a week, 21 a month, 63 a
             quarter. `pos` is where the last close sits inside that spread, 0 at the low and 100 at the high.
"""
from __future__ import annotations

import math
from collections import deque

TRADING_DAYS = 252
WINDOWS = (("d", 1), ("w", 5), ("m", 21), ("q", 63))      # the price-spread windows, in sessions
HV_WINDOWS = (21, 63, 252)
BANDS = ((0.0, 15.0, "under 15%"), (15.0, 25.0, "15-25%"), (25.0, 40.0, "25-40%"), (40.0, 60.0, "40-60%"), (60.0, 100.0, "60-100%"), (100.0, None, "100% and over"))


def _ann(var):
    return math.sqrt(max(var, 0.0) * TRADING_DAYS) * 100.0


def _sample_var(xs):
    n = len(xs)
    if n < 2:
        return None
    m = sum(xs) / n
    return sum((x - m) ** 2 for x in xs) / (n - 1)


def log_returns(C):
    """ln(C[i] / C[i-1]) for i >= 1; the list is one shorter than C."""
    return [math.log(C[i] / C[i - 1]) for i in range(1, len(C))]


def hv(C, k):
    """Close-to-close realized volatility over the last k returns, annualized, in percent. Needs k + 1 closes."""
    if k < 2 or len(C) < k + 1:
        return None
    var = _sample_var([math.log(C[i] / C[i - 1]) for i in range(len(C) - k, len(C))])
    return None if var is None else _ann(var)


def hv_series(C, k):
    """hv(C[:i+1], k) for every bar i, None where there are not k returns yet. Running sums: one pass."""
    n = len(C)
    out = [None] * n
    if k < 2 or n < k + 1:
        return out
    r = log_returns(C)                       # r[j] is the return INTO bar j + 1
    s1 = s2 = 0.0
    for j, x in enumerate(r):
        s1 += x
        s2 += x * x
        if j >= k:
            y = r[j - k]
            s1 -= y
            s2 -= y * y
        if j >= k - 1:
            out[j + 1] = _ann((s2 - s1 * s1 / k) / (k - 1))
    return out


def yang_zhang(O, H, L, C, k):
    """Yang-Zhang realized volatility over the last k sessions, annualized, in percent. Needs k + 1 bars: each session's
    overnight return is measured from the close before it."""
    n = len(C)
    if k < 2 or n < k + 1:
        return None
    on, oc, rs = [], [], 0.0
    for i in range(n - k, n):
        if not (O[i] > 0 and H[i] > 0 and L[i] > 0 and C[i] > 0 and C[i - 1] > 0):
            return None
        o, c = math.log(O[i] / C[i - 1]), math.log(C[i] / O[i])
        u, d = math.log(H[i] / O[i]), math.log(L[i] / O[i])
        on.append(o)
        oc.append(c)
        rs += u * (u - c) + d * (d - c)
    kk = 0.34 / (1.34 + (k + 1.0) / (k - 1.0))
    return _ann(_sample_var(on) + kk * _sample_var(oc) + (1.0 - kk) * rs / k)


def rolling_extreme(xs, k, largest=True):
    """The largest (or smallest) of xs[i-k+1 .. i] for every i, None before the window is full. Monotonic deque."""
    out, q = [None] * len(xs), deque()
    for i, x in enumerate(xs):
        while q and ((xs[q[-1]] <= x) if largest else (xs[q[-1]] >= x)):
            q.pop()
        q.append(i)
        if q[0] <= i - k:
            q.popleft()
        if i >= k - 1:
            out[i] = xs[q[0]]
    return out


def spread_series(H, L, k):
    """The k-session price spread ending at every bar, in percent; None before the window is full."""
    hi, lo = rolling_extreme(H, k, True), rolling_extreme(L, k, False)
    return [None if (a is None or b is None or b <= 0) else (a / b - 1.0) * 100.0 for a, b in zip(hi, lo)]


def spread(H, L, C, k):
    """The price spread of the last k sessions: {sp, hi, lo, pos} or None when there are fewer than k bars."""
    if k < 1 or len(C) < k:
        return None
    hi, lo = max(H[-k:]), min(L[-k:])
    if not (lo > 0 and hi >= lo):
        return None
    return {"sp": (hi / lo - 1.0) * 100.0, "hi": hi, "lo": lo, "pos": None if hi == lo else (C[-1] - lo) / (hi - lo) * 100.0}


def pct_rank(value, population):
    """Where a value stands among a population, 0-100: the share below it plus half the share equal to it."""
    pop = [p for p in population if p is not None]
    if value is None or not pop:
        return None
    below = sum(1 for p in pop if p < value)
    ties = sum(1 for p in pop if p == value)
    return 100.0 * (below + 0.5 * ties) / len(pop)


def max_move(C, dates, k):
    """The largest one-session close-to-close move, either way, inside the last k returns: (percent, date)."""
    best, when = None, None
    for i in range(max(1, len(C) - k), len(C)):
        m = (C[i] / C[i - 1] - 1.0) * 100.0
        if best is None or abs(m) > abs(best):
            best, when = m, dates[i]
    return best, when


def band_of(v):
    """The volatility band a reading falls in (index into BANDS), None for no reading."""
    if v is None:
        return None
    for i, (lo, hi, _) in enumerate(BANDS):
        if v >= lo and (hi is None or v < hi):
            return i
    return None


def median(xs):
    xs = sorted(x for x in xs if x is not None)
    n = len(xs)
    if not n:
        return None
    return xs[n // 2] if n % 2 else (xs[n // 2 - 1] + xs[n // 2]) / 2.0


def quantile(xs, q):
    """Linear-interpolated quantile (the 'inclusive' method), None for an empty list."""
    xs = sorted(x for x in xs if x is not None)
    if not xs:
        return None
    pos = (len(xs) - 1) * q
    lo = int(math.floor(pos))
    hi = min(lo + 1, len(xs) - 1)
    return xs[lo] + (xs[hi] - xs[lo]) * (pos - lo)
