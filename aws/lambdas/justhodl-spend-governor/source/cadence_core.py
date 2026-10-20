"""cadence_core - pure schedule-expression analysis (no AWS calls, no I/O).

One implementation shared by
  * the governor's daily cadence sentinel (lambda_function.py), which reports any enabled EventBridge rule or
    Scheduler schedule that fires more than once a day, and
  * scripts/cadence_plan.py, which rewrites the fleet's schedules to "at most once per 24 hours".

The rule (Khalid, 2026-10-20): every engine runs at most once every 24 hours.  A cron expression complies when it
fires at most once on any single day (minutes x hours <= 1); a rate() expression complies when its period is at
least 24 hours.  Weekly and monthly cron expressions comply (they fire less often than daily).

Expression grammar covered (the only forms EventBridge accepts):
    rate(N minute|minutes|hour|hours|day|days)
    cron(min hour day-of-month month day-of-week year)  -- *, ?, a, a-b, a/n, */n, a-b/n, lists, JAN-DEC, SUN-SAT,
                                                           L, W, # (these three count as "once a month")
    at(yyyy-mm-ddThh:mm:ss)                              -- one-time, always compliant
"""
import hashlib
import re

MAX_PER_DAY = 1

_MONTHS = {m: i + 1 for i, m in enumerate("JAN FEB MAR APR MAY JUN JUL AUG SEP OCT NOV DEC".split())}
_DOWS = {d: i + 1 for i, d in enumerate("SUN MON TUE WED THU FRI SAT".split())}  # EventBridge: SUN = 1


class ExpressionError(ValueError):
    pass


def _num(token, names):
    t = token.strip().upper()
    if names and t in names:
        return names[t]
    if not re.fullmatch(r"\d+", t):
        raise ExpressionError("bad token %r" % token)
    return int(t)


def _expand(field, lo, hi, names=None):
    """-> (sorted values, is_wildcard, is_step_or_wild).  `a/n` = from a to the maximum, every n."""
    f = field.strip().upper()
    if f in ("*", "?"):
        return list(range(lo, hi + 1)), True, True
    values, spread = set(), False
    for part in f.split(","):
        step = None
        if "/" in part:
            part, s = part.split("/", 1)
            step = int(s)
            if step < 1:
                raise ExpressionError("bad step in %r" % field)
            spread = True
        if part in ("*", "?"):
            a, b = lo, hi
        elif "-" in part:
            x, y = part.split("-", 1)
            a, b = _num(x, names), _num(y, names)
        else:
            a = _num(part, names)
            b = hi if step else a
        if a < lo or b > hi or a > b:
            raise ExpressionError("out of range in %r" % field)
        values.update(range(a, b + 1, step or 1))
    return sorted(values), False, spread


def _special_day(field):
    """L, LW, 15W, L-3, 6L, FRI#3 ... : "the last/nearest/nth day" forms, which fire once a month."""
    for tok in field.upper().split(","):
        if "#" in tok or tok in ("L", "LW") or tok.startswith("L-") or re.fullmatch(r"\d+[LW]", tok):
            return True
    return False


def parse(expr):
    """Parse a schedule expression -> dict.  Raises ExpressionError.

    keys: kind ('rate'|'cron'|'at'), per_active_day (int), fires_per_day (float), plus for cron: minutes, hours,
    dom, month, dow, year (raw strings), minute_set, hour_set (lists), hours_spread / minutes_spread (a step or a
    wildcard: the set covers the whole day rather than naming a window), day_fraction (share of days that fire).
    """
    if not isinstance(expr, str) or not expr.strip():
        raise ExpressionError("empty expression")
    e = expr.strip()
    m = re.fullmatch(r"rate\(\s*(\d+)\s+(minute|minutes|hour|hours|day|days)\s*\)", e)
    if m:
        n, unit = int(m.group(1)), m.group(2).rstrip("s")
        if n < 1:
            raise ExpressionError("rate must be positive")
        minutes = n * {"minute": 1, "hour": 60, "day": 1440}[unit]
        return {"kind": "rate", "period_min": minutes, "per_active_day": max(1, -(-1440 // minutes)) if minutes < 1440 else 1,
                "fires_per_day": 1440.0 / minutes}
    if re.fullmatch(r"at\(.*\)", e):
        return {"kind": "at", "per_active_day": 1, "fires_per_day": 0.0}
    m = re.fullmatch(r"cron\((.*)\)", e)
    if not m:
        raise ExpressionError("unrecognised expression")
    f = m.group(1).split()
    if len(f) != 6:
        raise ExpressionError("cron needs six fields")
    mi, hr, dom, mon, dow, year = f
    minute_set, mw, ms = _expand(mi, 0, 59)
    hour_set, hw, hs = _expand(hr, 0, 23)
    months, _, _ = _expand(mon, 1, 12, _MONTHS)
    day_fraction = 1.0
    if dom not in ("*", "?"):
        day_fraction = 1.0 / 30.4 if _special_day(dom) else min(1.0, len(_expand(dom, 1, 31)[0]) / 30.4)
    elif dow not in ("*", "?"):
        day_fraction = 1.0 / 30.4 if _special_day(dow) else len(_expand(dow, 1, 7, _DOWS)[0]) / 7.0
    month_fraction = len(months) / 12.0
    per_active_day = len(minute_set) * len(hour_set)
    return {"kind": "cron", "minutes": mi, "hours": hr, "dom": dom, "month": mon, "dow": dow, "year": year,
            "minute_set": minute_set, "hour_set": hour_set, "minutes_spread": ms, "hours_spread": hs,
            "per_active_day": per_active_day, "day_fraction": day_fraction,
            "fires_per_day": per_active_day * day_fraction * month_fraction}


def per_active_day(expr):
    return parse(expr)["per_active_day"]


def fires_per_day(expr):
    return parse(expr)["fires_per_day"]


def violates(expr, max_per_day=MAX_PER_DAY):
    """True when the expression can fire more than `max_per_day` times on a single day."""
    return parse(expr)["per_active_day"] > max_per_day


# ----------------------------------------------------------------------------------------------- daily rewrite --
def stable_int(key, salt=""):
    return int(hashlib.sha256((salt + "|" + key).encode()).hexdigest()[:12], 16)


# Hour of day (UTC) by data-flow depth: an engine whose inputs come from other engines runs later in the day than
# the engines it reads, so a once-a-day pipeline is at most one day stale instead of one day per hop.
BANDS = ((0, (1, 2, 3, 4, 5, 6, 7, 8)),
         (1, (9, 10, 11, 12, 13)),
         (3, (14, 15, 16, 17, 18)),
         (10, (19, 20, 21, 22, 23)))


def band_hours(depth):
    chosen = BANDS[0][1]
    for floor, hours in BANDS:
        if depth is not None and depth >= floor:
            chosen = hours
    return chosen


def to_daily(expr, key, depth=None):
    """Return an expression that fires at most once per day, keeping as much of the original as is meaningful:
      * a window of hours (13-21, 1,7,13,19, 0,6,12,16,20) becomes its LAST hour (the end-of-window run sees the
        whole window's data); a step or wildcard over the full day (rate(15 minutes), 0/2, *) takes an hour from
        the data-flow band (`depth`) and a stable per-engine minute, so 700 engines do not start at one second;
      * a single named minute is kept; day-of-week / day-of-month / month filters are kept untouched.
    An already-compliant expression is returned unchanged.  `key` makes the choice stable per trigger."""
    p = parse(expr)
    if p["per_active_day"] <= MAX_PER_DAY:
        return expr.strip()
    h = stable_int(key, "minute")
    minute = h % 60
    if p["kind"] == "rate":
        hour = band_hours(depth)[stable_int(key, "hour") % len(band_hours(depth))]
        return "cron(%d %d * * ? *)" % (minute, hour)
    if len(p["minute_set"]) == 1:
        minute = p["minute_set"][0]
    elif not p["minutes_spread"]:
        minute = p["minute_set"][0]          # an explicit list (5,35): keep its first named minute
    if len(p["hour_set"]) == 1:
        hour = p["hour_set"][0]
    elif p["hours_spread"]:
        hours = band_hours(depth)
        hour = hours[stable_int(key, "hour") % len(hours)]
    else:
        hour = p["hour_set"][-1]            # window / list: its last hour
    dom, dow = p["dom"], p["dow"]
    return "cron(%d %d %s %s %s %s)" % (minute, hour, dom, p["month"], dow, p["year"])


def describe(expr):
    """Short human text for reports: '1/day', '288/day', '~0.7/day (5 days a week)'."""
    p = parse(expr)
    fpd = p["fires_per_day"]
    if p["per_active_day"] > 1:
        return "%d/active day (~%.1f/day avg)" % (p["per_active_day"], fpd)
    return "~%.2f/day" % fpd if fpd < 0.99 else "1/day"
