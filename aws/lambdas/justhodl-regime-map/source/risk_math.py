"""risk_math - the Risk Map's arithmetic. Pure functions of daily closes; nothing reads S3, nothing guesses.

A series is [(iso date, close)], oldest first, one row a day the instrument traded (five a week for a fund, six for a
currency pair, seven for a coin). Because the kinds keep different weeks, every window is taken by the CALENDAR: "three
months" is the last close on or before the same day three months earlier, for a fund and for a coin alike. (The first
engine counted bars - 63 for three months - which for a coin's seven-day week is nine weeks.)

Score and state:
  score  = clamp(220 x (0.45 x r3m + 0.30 x r6m + 0.25 x r1m) + 25 x trend, -100, 100), returns as decimals, where
           trend = (points - 1.5) / 1.5 and a point is given for each of: last close above its 50-bar average, above
           its 200-bar average, the 50 above the 200.  This is the first engine's formula, unchanged.
  state  = BOOMING at 45 or more, RISK-ON at 15, NEUTRAL above -15, RISK-OFF above -45, DESTROYED below.
  A score alone can call something booming that has turned down, so two states are also held to the plain record:
    BOOMING   needs the last month and the last three months both up, and the close above both averages
    DESTROYED needs the last month and the last three months both down, and the close below both averages
  otherwise the state is the next one in (RISK-ON, RISK-OFF) and `held` says which test was not met.
"""
from datetime import date, timedelta
import math

WINDOWS = ("d", "w", "m", "q", "h", "ytd", "y")
WINDOW_NAME = {"d": "1 day", "w": "1 week", "m": "1 month", "q": "3 months", "h": "6 months", "ytd": "year to date", "y": "1 year"}
MONTHS = {"m": 1, "q": 3, "h": 6, "y": 12}
ANCHOR_SLACK_DAYS = 10          # an anchor further than this before the day asked for is a hole in the history, not a close
MIN_BARS = 130                  # the first engine's floor: under this there is no score
BOOM, ON, OFF, BUST = 45, 15, -15, -45
STATES = ("DESTROYED", "RISK-OFF", "NEUTRAL", "RISK-ON", "BOOMING")
PLAIN = {"BOOMING": "Booming", "RISK-ON": "Rising", "NEUTRAL": "Flat", "RISK-OFF": "Falling", "DESTROYED": "Destroyed"}
SIGMA_BARS, SIGMA_MIN = 252, 60


def clamp(v, lo, hi):
    return max(lo, min(hi, v))


def months_back(d, n):
    """The same day `n` months earlier; the month's last day when it has no such day (31 March - 1 month = 28 February)."""
    y, m = d.year, d.month - n
    while m <= 0:
        y, m = y - 1, m + 12
    day = d.day
    while True:
        try:
            return date(y, m, day)
        except ValueError:
            day -= 1


def clean(rows, invert=False):
    """[(iso, close)] oldest first from rows of (date, close): finite closes above zero, one row a day (the last kept).
    `invert` turns a units-per-dollar pair into the currency's own price in dollars."""
    seen = {}
    for d, c in rows:
        try:
            c = float(c)
        except (TypeError, ValueError):
            continue
        d = str(d)[:10]
        if len(d) != 10 or not c > 0 or c != c or c == float("inf"):
            continue
        seen[d] = (1.0 / c) if invert else c
    return sorted(seen.items())


def targets(last_iso):
    """{window: the latest date its anchor may carry} for a series ending on `last_iso`. The day window has none."""
    last = date.fromisoformat(last_iso)
    out = {"w": last - timedelta(days=7), "ytd": date(last.year - 1, 12, 31)}
    for k, n in MONTHS.items():
        out[k] = months_back(last, n)
    return {k: v.isoformat() for k, v in out.items()}


def anchors(series):
    """{window: (iso date, close, bars since)} - the close each window's change is measured from. A window the history
    does not reach, or whose anchor would sit more than ANCHOR_SLACK_DAYS before the day asked for, is absent."""
    if len(series) < 2:
        return {}
    dates = [d for d, _ in series]
    n = len(series)
    out = {"d": (series[-2][0], series[-2][1], 1)}
    want = targets(series[-1][0])
    for k in WINDOWS:
        if k == "d":
            continue
        lo, hi = 0, n - 1                     # the last index whose date is on or before the target
        if dates[0] > want[k]:
            continue
        while lo < hi:
            mid = (lo + hi + 1) // 2
            if dates[mid] <= want[k]:
                lo = mid
            else:
                hi = mid - 1
        if lo >= n - 1:
            continue
        gap = (date.fromisoformat(want[k]) - date.fromisoformat(dates[lo])).days
        if gap > ANCHOR_SLACK_DAYS:
            continue
        out[k] = (dates[lo], series[lo][1], n - 1 - lo)
    return out


def pct(a, b):
    """Percent change from a to b."""
    return (b / a - 1.0) * 100.0


def sigma_daily(closes, bars=SIGMA_BARS, floor=SIGMA_MIN):
    """Sample standard deviation of the last `bars` daily log returns, in percent; None under `floor` returns."""
    tail = closes[-(bars + 1):]
    rets = [math.log(tail[i] / tail[i - 1]) for i in range(1, len(tail))]
    if len(rets) < floor:
        return None
    mean = sum(rets) / len(rets)
    return math.sqrt(sum((x - mean) ** 2 for x in rets) / (len(rets) - 1)) * 100.0


def unusual(ret_pct, sigma_pct, bars):
    """How many of its own usual moves a change is: the log change over the usual log change for that many bars."""
    if ret_pct is None or not sigma_pct or not bars or ret_pct <= -100:
        return None
    return math.log(1.0 + ret_pct / 100.0) * 100.0 / (sigma_pct * math.sqrt(bars))


def averages(closes):
    """(50-bar average, 200-bar average - of every bar when there are fewer than 200, as the first engine did)."""
    ma50 = sum(closes[-50:]) / len(closes[-50:])
    ma200 = sum(closes[-200:]) / 200 if len(closes) >= 200 else sum(closes) / len(closes)
    return ma50, ma200


def trend_points(last, ma50, ma200):
    return (1 if last > ma50 else 0) + (1 if last > ma200 else 0) + (1 if ma50 > ma200 else 0)


def score_of(r1m, r3m, r6m, points):
    """The first engine's score from three returns in percent and the trend points (0..3)."""
    mom = 0.45 * r3m + 0.30 * r6m + 0.25 * r1m
    return int(round(clamp(mom * 2.2 + (points - 1.5) / 1.5 * 25.0, -100.0, 100.0)))


def state_of(score):
    return "BOOMING" if score >= BOOM else "RISK-ON" if score >= ON else "NEUTRAL" if score > OFF else "RISK-OFF" if score > BUST else "DESTROYED"


def held_state(score, r1m, r3m, last, ma50, ma200):
    """(state, held): the state by the score, held to the plain record at both ends. `held` is None, or the tests a
    booming / destroyed score failed."""
    state = state_of(score)
    if state == "BOOMING":
        missing = [text for ok, text in ((r1m > 0, "the last month is down"), (r3m > 0, "the last three months are down"),
                                         (last > ma50, "the close is under its 50-day average"), (last > ma200, "the close is under its 200-day average")) if not ok]
        if missing:
            return "RISK-ON", "scores as booming (%+d) but %s" % (score, " and ".join(missing))
    elif state == "DESTROYED":
        missing = [text for ok, text in ((r1m < 0, "the last month is up"), (r3m < 0, "the last three months are up"),
                                         (last < ma50, "the close is above its 50-day average"), (last < ma200, "the close is above its 200-day average")) if not ok]
        if missing:
            return "RISK-OFF", "scores as destroyed (%+d) but %s" % (score, " and ".join(missing))
    return state, None


def year_range(series):
    """(high, low) of the closes of the last 365 days."""
    floor = (date.fromisoformat(series[-1][0]) - timedelta(days=365)).isoformat()
    tail = [c for d, c in series if d >= floor]
    return max(tail), min(tail)


def measure(series):
    """Everything the map says about one instrument, from its closes: None when it cannot be scored, with the reason.

    Returns (row, None) or (None, why). The row: last close and date, previous close, the anchors and changes of every
    window the history reaches, the usual daily move, both averages, the 52-week range, the score and the state."""
    n = len(series)
    if n < MIN_BARS:
        return None, "%d daily bars; a score needs %d" % (n, MIN_BARS)
    closes = [c for _, c in series]
    last_d, last = series[-1]
    a = anchors(series)
    for k in ("m", "q", "h"):
        if k not in a:
            return None, "no close to measure %s from" % WINDOW_NAME[k]
    r = {k: pct(a[k][1], last) for k in a}
    ma50, ma200 = averages(closes)
    pts = trend_points(last, ma50, ma200)
    score = score_of(r["m"], r["q"], r["h"], pts)
    state, held = held_state(score, r["m"], r["q"], last, ma50, ma200)
    sd = sigma_daily(closes)
    hi, lo = year_range(series)
    return {
        "c": last, "d": last_d, "p": series[-2][1], "pd": series[-2][0], "bars": n,
        "a": {k: [a[k][0], a[k][1], a[k][2]] for k in a},
        "r": {k: round(r[k], 2) for k in r},
        "u": {k: (None if unusual(r[k], sd, a[k][2]) is None else round(unusual(r[k], sd, a[k][2]), 2)) for k in r},
        "sd": None if sd is None else round(sd, 3),
        "ma50": ma50, "ma200": ma200, "pts": pts,
        "v50": round(pct(ma50, last), 2), "v200": round(pct(ma200, last), 2),
        "hi": hi, "lo": lo, "off_hi": round(pct(hi, last), 2), "off_lo": round(pct(lo, last), 2),
        "score": score, "state": state, "raw_state": state_of(score), "held": held,
    }, None


def check(row):
    """The reasons a measured row contradicts itself; [] when it does not. Run on every row before anything is published."""
    bad = []
    for k, (ad, ac, _) in row["a"].items():
        if ad >= row["d"]:
            bad.append("%s anchor %s is not before the last bar %s" % (k, ad, row["d"]))
        if abs(pct(ac, row["c"]) - row["r"][k]) > 0.011:
            bad.append("%s change %s is not the close over its anchor (%.4f)" % (k, row["r"][k], pct(ac, row["c"])))
    if row["state"] == "BOOMING" and not (row["score"] >= BOOM and row["r"]["m"] > 0 and row["r"]["q"] > 0 and row["c"] > row["ma50"] and row["c"] > row["ma200"]):
        bad.append("called booming against the record")
    if row["state"] == "DESTROYED" and not (row["score"] <= BUST and row["r"]["m"] < 0 and row["r"]["q"] < 0 and row["c"] < row["ma50"] and row["c"] < row["ma200"]):
        bad.append("called destroyed against the record")
    if row["state"] != row["raw_state"] and not row["held"]:
        bad.append("state differs from the score's with no reason given")
    if not row["lo"] <= row["c"] <= row["hi"]:
        bad.append("last close outside its own 52-week range")
    return bad


def median(xs):
    xs = sorted(x for x in xs if x is not None)
    if not xs:
        return None
    m = len(xs) // 2
    return xs[m] if len(xs) % 2 else (xs[m - 1] + xs[m]) / 2.0


def mean(xs):
    xs = [x for x in xs if x is not None]
    return sum(xs) / len(xs) if xs else None
