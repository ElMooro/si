"""canary_record - every canary rebuilt through history and set against what the S&P 500 did next.

Pure arithmetic: no network, no clock, no storage. The handler hands it the series; the tests hand it a real cut.

What it does
  1. Rebuilds each canary's 0-100 stress at every month-end with its engine's OWN rule, using only what was
     published by that day:
       macro grid   transform (yoy / mom / diff / level) -> z against its own window -> 50 +/- 22 z   (canary-grid)
       plumbing     percentile of the level inside its trailing window, by polarity                     (plumbing-aggregator)
       CISS         percentile of the level in all history to date                                      (ciss-stress)
     Daily series arrive as the last observation of each ISO week; windows counted in daily observations are
     divided by five, so the span is the same.
  2. Dates what followed on the S&P 500's daily closes: a fall of 10% or more from that month-end's close inside six
     months (126 sessions), and of 20% inside twelve (252).
  3. Scores each canary on the months that matter for an early warning - the market within 5% of its one-year high -
     by how well its stress ranked the months that were followed by a fall (the area under the curve, 0.5 = no
     better than chance), with a leave-one-year-out error.
  4. Builds the barometer the same way through history, three ways (every canary equal, every mechanism equal, and
     weighted by what each canary had shown by then), and reports what each reading was followed by.

Nothing here forecasts. It counts what happened.
"""
from bisect import bisect_right

VERSION = "1.0.0"

FALL, FALL_SESSIONS = -0.10, 126          # the fall the record is scored on: 10% inside six months
DEEP, DEEP_SESSIONS = -0.20, 252          # and the deeper one it also counts
NEAR_HIGH = 0.95                          # "early": the close within 5% of its one-year high
YEAR_SESSIONS = 252
LAG_DAYS = {"d": 1, "w": 8, "m": 45, "q": 75}   # days from an observation's date to its publication, by frequency
LAG_CAP = 130
MIN_MONTHS = 120                          # a canary is judged on ten years of months or more
MIN_EPISODES = 3                          # and three falls or more inside its record
FIRE = {"macro_grid": 50.0, "plumbing": 55.0, "ciss": 60.0}   # the war room's own firing lines
BANDS = [("CALM", 0.0), ("GUARDED", 40.0), ("ELEVATED", 70.0), ("HIGH RISK", 90.0), ("CRISIS", 97.0)]  # by percentile of its own history
WEIGHT_FLOOR, WEIGHT_CEIL, WEIGHT_SLOPE, SHRINK_EPISODES = 0.25, 2.0, 5.0, 5.0
WALK_START_MONTHS = 120                   # the walk-forward starts once ten years are behind it
TURN_MONTHS = 3                           # the turn: the barometer against itself three month-ends earlier
TURN_FAMILY = (1, 3, 6, 12)               # every span the turn was tried on; all four are published
TURN_BANDS = [("EASING", 0.0), ("STEADY", 30.0), ("RISING", 70.0), ("RISING FAST", 90.0)]   # by percentile of its own history
GRID_LEVEL = [("low", 0.0), ("middle", 40.0), ("high", 70.0)]
GRID_TURN = [("easing", 0.0), ("steady", 30.0), ("rising", 70.0)]
MIN_MECH_CANARIES = 5                     # a mechanism is read from five tested canaries or more
GAP_FLAG = 15.0                           # a rebuilt reading this far from the engine's own is flagged
SEAM_MONTHS = 18                          # and, for a monthly or quarterly series, its newest months are left out


# ── dates ───────────────────────────────────────────────────────────────────────────────────────────────────────────
def _ord(ymd):
    """Days since 0001-01-01 for an integer yyyymmdd (proleptic Gregorian), without the datetime module."""
    y, m, d = ymd // 10000, ymd // 100 % 100, ymd % 100
    if m < 3:
        y -= 1
        m += 12
    return 365 * y + y // 4 - y // 100 + y // 400 + (153 * (m - 3) + 2) // 5 + d - 307


def month_of(ymd):
    return ymd // 100


def iso(ymd):
    return "%04d-%02d-%02d" % (ymd // 10000, ymd // 100 % 100, ymd % 100)


# ── the market side ─────────────────────────────────────────────────────────────────────────────────────────────────
QUARTER_STARTS = (301, 601, 901, 1201)
STRAY_GAP = 0.035


def clean_closes(rows):
    """The index's daily closes with the stray rows taken out; returns (closes, what was dropped).

    The banked index file carries quarterly bars among its daily ones: a row dated the first day of March, June,
    September or December holding that quarter's LAST close - a figure from up to three months later. Left in, each
    one is a peak that never traded and a fall that never happened. So: weekend rows go; of several rows on one day
    the one nearest the previous close stays; and a row on a quarter's first day stays only if it sits within 3.5%
    of the previous close (a real move that large on such a day costs one session, never a false fall)."""
    def weekday(ymd):
        return (_ord(ymd) + 0) % 7            # _ord(2024-01-01, a Monday) % 7 == 0

    out, dropped = [], {"weekend": 0, "second_row": 0, "stray_quarter_row": 0}
    i, n = 0, len(rows)
    while i < n:
        day = int(rows[i][0])
        j = i
        while j < n and int(rows[j][0]) == day:
            j += 1
        cands = [float(rows[k][1]) for k in range(i, j) if rows[k][1] is not None]
        i = j
        if not cands:
            continue
        if weekday(day) >= 5:
            dropped["weekend"] += len(cands)
            continue
        prev = out[-1][1] if out else None
        pick = min(cands, key=lambda v: abs(v / prev - 1.0)) if prev else cands[0]
        dropped["second_row"] += len(cands) - 1
        if prev and day % 10000 in QUARTER_STARTS and abs(pick / prev - 1.0) > STRAY_GAP:
            dropped["stray_quarter_row"] += 1
            continue
        out.append([day, pick])
    return out, dropped


def market(closes):
    """closes: [[yyyymmdd, close], ...] daily, oldest first -> month-ends with what followed each."""
    days = [d for d, _ in closes]
    px = [float(c) for _, c in closes]
    n = len(px)
    ends = [i for i in range(n) if i == n - 1 or month_of(days[i + 1]) != month_of(days[i])]
    out = []
    for i in ends:
        hi = max(px[max(0, i - YEAR_SESSIONS + 1):i + 1])
        row = {"day": days[i], "i": i, "close": px[i], "near_high": px[i] >= NEAR_HIGH * hi, "from_high": px[i] / hi - 1.0}
        for tag, cut, span in (("fall", FALL, FALL_SESSIONS), ("deep", DEEP, DEEP_SESSIONS)):
            if i + span < n:
                lo = min(px[i + 1:i + span + 1])
                row[tag] = (lo / px[i] - 1.0) <= cut
                row[tag + "_dd"] = lo / px[i] - 1.0
            else:
                row[tag] = None                  # the window has not closed yet
                row[tag + "_dd"] = None
        if i + FALL_SESSIONS < n:
            row["ret6"] = px[i + FALL_SESSIONS] / px[i] - 1.0
        else:
            row["ret6"] = None
        out.append(row)
    return out


def episodes(closes, cut=FALL):
    """Every fall of 10% or more from a high: the high, the day the line was crossed, the low, the depth.

    A new high is looked for again once the index has risen 10% off its low, so the falls inside a long stretch
    under an old record (2002, 2010, 2011) are counted too."""
    px = [float(c) for _, c in closes]
    days = [d for d, _ in closes]
    out, rising, hi_i, lo_i, cross_i = [], True, 0, 0, None
    for i in range(1, len(px)):
        if rising:
            if px[i] >= px[hi_i]:
                hi_i = i
            elif px[i] / px[hi_i] - 1.0 <= cut:
                rising, lo_i, cross_i = False, i, i
        else:
            if px[i] < px[lo_i]:
                lo_i = i
            elif px[i] / px[lo_i] - 1.0 >= -cut:
                out.append({"peak": days[hi_i], "crossed": days[cross_i], "low": days[lo_i], "depth": round(px[lo_i] / px[hi_i] - 1.0, 4), "turned": days[i]})
                rising, hi_i = True, i
    if not rising:
        out.append({"peak": days[hi_i], "crossed": days[cross_i], "low": days[lo_i], "depth": round(px[lo_i] / px[hi_i] - 1.0, 4), "turned": None})
    return out


# ── one canary's stress through time ────────────────────────────────────────────────────────────────────────────────
def lag_days(freq, pts, cut_day):
    """Days from an observation's date to the day it is taken as public.

    The newest observation is public today, so its age is a lag the series is known to meet; that age is used.
    A series that has stopped (its newest observation older than the cap) falls back to its frequency's usual lag."""
    base = LAG_DAYS.get(freq, 45)
    if not pts or not cut_day:
        return base
    age = _ord(cut_day) - _ord(pts[-1][0])
    return base if age > LAG_CAP else max(1, age)


def _scaled(count, freq):
    """A window counted in daily observations, for a series kept weekly."""
    return max(2, int(round(count / 5.0))) if freq == "d" else int(count)


def transform(pts, kind, win, freq):
    """The macro grid's own transform, oldest first: [(day, value)]."""
    if kind == "level":
        return list(pts)
    w = 12 if kind == "yoy" else _scaled(win, freq)
    out = []
    for i in range(w, len(pts)):
        d, v = pts[i]
        vo = pts[i - w][1]
        if kind == "diff":
            out.append((d, v - vo))
        elif vo not in (0, None):
            out.append((d, v / vo - 1.0))
    return out


def combine(spec, series):
    """A grid source spec -> ([(day, value)], frequency) from plain series: ID, spread:A:B, ratio:A:B, sum:A+B-C."""
    def get(sid):
        s = series.get(sid)
        return ([(int(d), float(v)) for d, v in s["p"]], s.get("f", "m")) if s and s.get("p") else (None, None)

    def asof(a, b, fn):
        bd = [d for d, _ in b]
        out = []
        for d, v in a:
            i = bisect_right(bd, d) - 1
            if i >= 0:
                r = fn(v, b[i][1])
                if r is not None:
                    out.append((d, r))
        return out

    if spec.startswith("spread:") or spec.startswith("ratio:"):
        kind, rest = spec.split(":", 1)
        a_id, b_id = rest.split(":", 1)
        (a, fa), (b, _fb) = get(a_id), get(b_id)
        if not a or not b:
            return None, None
        if kind == "spread":
            return asof(a, b, lambda x, y: x - y), fa
        return asof(a, b, lambda x, y: (x / y) if y else None), fa
    if spec.startswith("sum:"):
        legs, cur, sign = [], "", "+"
        for ch in spec[4:] + "+":
            if ch in "+-":
                if cur:
                    legs.append((sign, cur))
                cur, sign = "", ch
            else:
                cur += ch
        base, fbase = get(legs[0][1])
        if not base:
            return None, None
        total = [(d, v if legs[0][0] == "+" else -v) for d, v in base]
        for sg, sid in legs[1:]:
            other, _ = get(sid)
            if not other:
                return None, None
            total = asof(total, other, (lambda x, y: x + y) if sg == "+" else (lambda x, y: x - y))
        return total, fbase
    return get(spec)


def _avail(pts, lag):
    """The day each observation is taken to be public."""
    return [_ord(d) + lag for d, _ in pts]


def grid_path(defn, series, days, cut_day):
    """{month-end day: stress} for one macro-grid canary, by the grid's own rule."""
    for spec in defn["sources"]:
        if spec.startswith(("feed:", "ecb:", "ecbspread:")) or "/" in spec:
            continue
        pts, freq = combine(spec, series)
        if not pts or len(pts) < 30:
            continue
        tr = transform(pts, defn["kind"], defn.get("win") or 12, freq)
        if len(tr) < 24:
            continue
        w = 0 if defn["kind"] == "level" else (12 if defn["kind"] == "yoy" else _scaled(defn.get("win") or 12, freq))
        n = max(24, _scaled(defn["limit"], freq) - w)          # the grid z-scores the whole transformed window it fetched
        lag = lag_days(freq, pts, cut_day)
        av = _avail(tr, lag)
        s1, s2 = [0.0], [0.0]
        for _, v in tr:
            s1.append(s1[-1] + v)
            s2.append(s2[-1] + v * v)
        out = {}
        for day in days:
            j = bisect_right(av, _ord(day)) - 1
            if j < 23:
                continue
            if _ord(day) - (av[j] - lag) > LAG_CAP + 2 * lag:      # the series had stopped by then: no reading
                continue
            lo = max(0, j - n + 1)
            k = j - lo + 1
            if k < 24:
                continue
            mu = (s1[j + 1] - s1[lo]) / k
            var = ((s2[j + 1] - s2[lo]) - k * mu * mu) / (k - 1)
            if var <= 1e-18:
                continue
            z = (tr[j][1] - mu) / var ** 0.5
            s = 50 + z * 22 if defn["dir"] == "rise" else 50 - z * 22
            out[day] = round(max(0.0, min(100.0, s)), 1)
        return out, spec, freq, lag
    return {}, None, None, None


def pct_path(pts, freq, days, cut_day, polarity=1, window=None):
    """{month-end day: stress} as the percentile of the level in its trailing window (all history when window is None)."""
    if not pts or len(pts) < 30:
        return {}, None
    lag = lag_days(freq, pts, cut_day)
    av = _avail(pts, lag)
    vals = [v for _, v in pts]
    n = _scaled(window, freq) if window else None
    out = {}
    for day in days:
        j = bisect_right(av, _ord(day)) - 1
        if j < 30:
            continue
        if _ord(day) - (av[j] - lag) > LAG_CAP + 2 * lag:
            continue
        lo = 0 if n is None else max(0, j - n + 1)
        hist = vals[lo:j]
        if len(hist) < 29:
            continue
        cur = vals[j]
        pct = 100.0 * sum(1 for v in hist if v < cur) / len(hist)
        out[day] = round(pct if polarity >= 0 else 100.0 - pct, 1)
    return out, lag


# ── scoring ─────────────────────────────────────────────────────────────────────────────────────────────────────────
def auc(pairs):
    """Area under the curve for (score, happened) pairs; ties share their rank. None without both kinds."""
    pos = sum(1 for _, y in pairs if y)
    neg = len(pairs) - pos
    if not pos or not neg:
        return None
    order = sorted(pairs, key=lambda p: p[0])
    rank_sum, i = 0.0, 0
    while i < len(order):
        j = i
        while j < len(order) and order[j][0] == order[i][0]:
            j += 1
        avg = (i + 1 + j) / 2.0
        rank_sum += avg * sum(1 for k in range(i, j) if order[k][1])
        i = j
    return (rank_sum - pos * (pos + 1) / 2.0) / (pos * neg)


def auc_with_error(rows):
    """rows: [(year, score, happened)] -> (auc, leave-one-year-out standard error, falls counted, months)."""
    whole = auc([(s, y) for _, s, y in rows])
    if whole is None:
        return None, None
    years = sorted(set(y for y, _, _ in rows))
    parts = []
    for yr in years:
        a = auc([(s, y) for g, s, y in rows if g != yr])
        if a is not None:
            parts.append(a)
    if len(parts) < 5:
        return whole, None
    mean = sum(parts) / len(parts)
    se = ((len(parts) - 1) / float(len(parts)) * sum((a - mean) ** 2 for a in parts)) ** 0.5
    return whole, se


def judge(path, mkt_by_day, eps, fire, errors=True):
    """One canary's record: what its stress was followed by. errors=False skips the leave-one-year-out pass."""
    rows = [(day, s, mkt_by_day[day]) for day, s in sorted(path.items()) if day in mkt_by_day]
    known = [(d, s, m) for d, s, m in rows if m["fall"] is not None]
    if not known:
        return {"months": 0}
    early = [(d, s, m) for d, s, m in known if m["near_high"]]
    fired = [(d, s, m) for d, s, m in known if s >= fire]
    quiet = [(d, s, m) for d, s, m in known if s < fire]
    e_fired = [(d, s, m) for d, s, m in early if s >= fire]
    e_quiet = [(d, s, m) for d, s, m in early if s < fire]

    def share(part):
        return round(sum(1 for _, _, m in part if m["fall"]) / float(len(part)), 4) if part else None

    if errors:
        a_all, se_all = auc_with_error([(d // 10000, s, m["fall"]) for d, s, m in known])
        a_early, se_early = auc_with_error([(d // 10000, s, m["fall"]) for d, s, m in early])
    else:
        a_all, se_all = auc([(s, m["fall"]) for _, s, m in known]), None
        a_early, se_early = auc([(s, m["fall"]) for _, s, m in early]), None
    first, last = known[0][0], known[-1][0]
    # the falls inside this canary's record: was it firing at a month-end in the six months up to the high, or by the 10% line?
    month_fire = {month_of(d): (s >= fire) for d, s, _ in rows}
    covered = warned_top = warned_line = 0
    leads = []
    for ep in eps:
        if ep["peak"] < first or ep["peak"] > last:
            continue
        covered += 1
        pm = month_of(ep["peak"])
        before = []
        for k in range(6, 0, -1):                     # the six month-ends before the high's month
            y, m = pm // 100, pm % 100 - k
            while m <= 0:
                y, m = y - 1, m + 12
            before.append(y * 100 + m)
        fired_before = [mm for mm in before if month_fire.get(mm)]
        if fired_before:
            warned_top += 1
            leads.append(len(before) - before.index(fired_before[0]))
        between = [mm for mm in month_fire if pm <= mm < month_of(ep["crossed"]) and month_fire[mm]]
        if fired_before or between:
            warned_line += 1
    false_alarms = None
    if e_fired:
        false_alarms = round(sum(1 for _, _, m in e_fired if not m["fall"]) / float(len(e_fired)), 4)
    out = {"months": len(known), "from": first, "to": last, "fired_share": round(len(fired) / float(len(known)), 4),
           "fall_after_firing": share(fired), "fall_after_quiet": share(quiet), "fall_any": share(known),
           "early_months": len(early), "early_fall_after_firing": share(e_fired), "early_fall_after_quiet": share(e_quiet), "early_fall_any": share(early),
           "auc": None if a_all is None else round(a_all, 4), "auc_se": None if se_all is None else round(se_all, 4),
           "auc_early": None if a_early is None else round(a_early, 4), "auc_early_se": None if se_early is None else round(se_early, 4),
           "falls_covered": covered, "warned_before_high": warned_top, "warned_by_line": warned_line,
           "lead_months": round(sum(leads) / float(len(leads)), 1) if leads else None, "false_alarm_share": false_alarms}
    out["verdict"] = verdict(out)
    return out


def verdict(rec):
    if not rec.get("months"):
        return "no record"
    if rec["months"] < MIN_MONTHS or rec["falls_covered"] < MIN_EPISODES or rec.get("auc_early") is None:
        return "short record"
    if rec["fired_share"] > 0.90 or rec["fired_share"] < 0.02:
        return "stuck"
    se = rec.get("auc_early_se") or 0.0
    if rec["auc_early"] - 1.64 * se > 0.5:
        return "leads"
    if rec["auc_early"] + 1.64 * se < 0.5:
        return "wrong way"
    return "no edge shown"


def weight_of(rec):
    """What a canary's record earns it in the weighted barometer: 1 with no record, more for a lead, less for none."""
    if not rec or rec.get("auc_early") is None or rec.get("months", 0) < MIN_MONTHS:
        return 1.0
    shrink = rec["falls_covered"] / (rec["falls_covered"] + SHRINK_EPISODES)
    w = 1.0 + WEIGHT_SLOPE * (rec["auc_early"] - 0.5) * shrink
    return round(max(WEIGHT_FLOOR, min(WEIGHT_CEIL, w)), 3)


# ── the barometer through history ───────────────────────────────────────────────────────────────────────────────────
def blend(paths, mech_of, day, weights=None):
    """(per canary, per mechanism, canaries counted, mechanisms counted) at one day; weights turn both into weighted means."""
    by = {}
    tot = wsum = 0.0
    n = 0
    for cid, path in paths.items():
        s = path.get(day)
        if s is None:
            continue
        w = 1.0 if weights is None else weights.get(cid, 1.0)
        tot += w * s
        wsum += w
        n += 1
        b = by.setdefault(mech_of[cid], [0.0, 0.0])
        b[0] += w * s
        b[1] += w
    if not n or wsum <= 0:
        return None, None, 0, 0
    mech = [a / b for a, b in by.values() if b > 0]
    return tot / wsum, sum(mech) / len(mech), n, len(mech)


def percentile_of(value, history):
    if value is None or not history:
        return None
    return 100.0 * sum(1 for v in history if v <= value) / len(history)


def band_of(pct):
    return band_in(pct, BANDS) or "NO DATA"


def band_in(pct, bands):
    if pct is None:
        return None
    name = bands[0][0]
    for label, floor in bands:
        if pct >= floor:
            name = label
    return name


def reliability(rows, bands=None):
    """rows: [(percentile, month)] -> what each band was followed by."""
    bands = bands or BANDS
    out = []
    for idx, (label, floor) in enumerate(bands):
        ceil = bands[idx + 1][1] if idx + 1 < len(bands) else 100.01
        part = [m for p, m in rows if p is not None and floor <= p < ceil and m["fall"] is not None]
        early = [m for m in part if m["near_high"]]
        out.append({"band": label, "from_pct": floor, "to_pct": min(ceil, 100.0), "months": len(part),
                    "fell": sum(1 for m in part if m["fall"]), "fall_share": round(sum(1 for m in part if m["fall"]) / float(len(part)), 4) if part else None,
                    "deep": sum(1 for m in part if m["deep"]), "deep_share": round(sum(1 for m in part if m["deep"]) / float(len(part)), 4) if part else None,
                    "median_low": _median([m["fall_dd"] for m in part]), "median_ret6": _median([m["ret6"] for m in part if m["ret6"] is not None]),
                    "early_months": len(early), "early_fell": sum(1 for m in early if m["fall"]),
                    "early_fall_share": round(sum(1 for m in early if m["fall"]) / float(len(early)), 4) if early else None})
    return out


def cross(rows):
    """rows: [(level percentile, turn percentile, month)] -> the nine cells of level against turn."""
    out = []
    for li, (lname, lfloor) in enumerate(GRID_LEVEL):
        lceil = GRID_LEVEL[li + 1][1] if li + 1 < len(GRID_LEVEL) else 100.01
        for ti, (tname, tfloor) in enumerate(GRID_TURN):
            tceil = GRID_TURN[ti + 1][1] if ti + 1 < len(GRID_TURN) else 100.01
            part = [m for lp, tp, m in rows if lp is not None and tp is not None and lfloor <= lp < lceil and tfloor <= tp < tceil and m["fall"] is not None]
            early = [m for m in part if m["near_high"]]
            out.append({"level": lname, "turn": tname, "months": len(part), "fell": sum(1 for m in part if m["fall"]),
                        "fall_share": round(sum(1 for m in part if m["fall"]) / float(len(part)), 4) if part else None,
                        "early_months": len(early), "early_fell": sum(1 for m in early if m["fall"]),
                        "early_fall_share": round(sum(1 for m in early if m["fall"]) / float(len(early)), 4) if early else None})
    return out


def _median(xs):
    xs = sorted(x for x in xs if x is not None)
    if not xs:
        return None
    k = len(xs) // 2
    return round(xs[k] if len(xs) % 2 else (xs[k - 1] + xs[k]) / 2.0, 4)


def _months_back(day, k):
    y, m = day // 10000, day // 100 % 100 - k
    while m <= 0:
        y, m = y - 1, m + 12
    return y * 100 + m


def build(defs, series, closes, ciss=None, cut_day=None, live=None):
    """The whole record.

    defs    {"grid": [...], "plumbing": [...]} as scripts/build_canary_defs.py writes them
    series  {id: {"f": "d|w|m|q", "p": [[yyyymmdd, value], ...]}} from the warehouse (daily ones kept weekly)
    closes  the S&P 500's daily closes, already cleaned
    ciss    {key: {"label", "category", "p", "votes_live"}} from the CISS engine's document
    live    {canary id: the engine's own stress today}; a rebuilt reading far from it is flagged
    """
    mkt = market(closes)
    days = [m["day"] for m in mkt]
    by_day = {m["day"]: m for m in mkt}
    eps = episodes(closes)
    cut_day = cut_day or closes[-1][0]
    live = live or {}
    paths, mech_of, meta = {}, {}, {}
    for d in defs.get("grid") or []:
        cid = "macro_grid:" + d["key"]
        path, used, freq, lag = grid_path(d, series, days, cut_day)
        meta[cid] = {"id": cid, "mechanism": "macro_grid", "key": d["key"], "name": d["name"], "group": d.get("grid"),
                     "rule": "%s, z against %s observations, %s is stress" % (d["kind"], d["limit"], "a rise" if d["dir"] == "rise" else "a fall"),
                     "source": used, "frequency": freq, "lag_days": lag, "fire_at": FIRE["macro_grid"], "lead_claimed": d.get("lead"), "votes_live": True,
                     "why_not": None if path else "its series is not in the warehouse, or its kept history is too short to test"}
        if path:
            paths[cid], mech_of[cid] = path, "macro_grid"
    for d in defs.get("plumbing") or []:
        cid = "plumbing:" + d["id"]
        s = series.get(d["id"])
        path, lag = ({}, None)
        if s and s.get("p"):
            path, lag = pct_path([(int(a), float(b)) for a, b in s["p"]], s.get("f", "m"), days, cut_day, d.get("polarity", 1), 1300)
        meta[cid] = {"id": cid, "mechanism": "plumbing", "key": d["id"], "name": d.get("label") or d["id"], "group": d.get("layer"),
                     "rule": "percentile of the level in its last 1,300 observations, %s is stress" % ("high" if d.get("polarity", 1) >= 0 else "low"),
                     "source": d["id"] if path else None, "frequency": (s or {}).get("f"), "lag_days": lag, "fire_at": FIRE["plumbing"], "lead_claimed": None,
                     "votes_live": not d.get("deduped"), "why_not": None if path else "not a warehouse series (%s)" % d.get("source", "?")}
        if path:
            paths[cid], mech_of[cid] = path, "plumbing"
    for key, s in sorted((ciss or {}).items(), key=lambda kv: str(kv[0])):
        label = str(s.get("label") or key)
        cid = "ciss:" + label
        if cid in meta:
            continue
        freq = "m" if str(s.get("freq") or "").upper().startswith("M") else "w"      # the CISS document keeps daily series weekly
        path, lag = pct_path([(int(a), float(b)) for a, b in s.get("p") or []], freq, days, cut_day, 1, None)
        meta[cid] = {"id": cid, "mechanism": "ciss", "key": label, "name": "CISS: %s" % label, "group": s.get("category"),
                     "rule": "percentile of the level in all its history to date, high is stress", "source": "data/ciss-stress.json" if path else None,
                     "frequency": freq, "lag_days": lag, "fire_at": FIRE["ciss"], "lead_claimed": None, "votes_live": bool(s.get("votes_live", True)),
                     "why_not": None if path else "too little history in the CISS document"}
        if path:
            paths[cid], mech_of[cid] = path, "ciss"

    # a rebuilt reading set against the engine's own: far apart, it is flagged; a monthly or quarterly one also loses its newest months
    gaps = {}
    for cid, path in paths.items():
        if cid not in live or live[cid] is None or not path:
            continue
        last = max(path)
        gap = round(path[last] - float(live[cid]), 1)
        gaps[cid] = gap
        if abs(gap) > GAP_FLAG and meta[cid]["frequency"] in ("m", "q"):
            floor = _months_back(cut_day, SEAM_MONTHS)
            for day in [d for d in path if month_of(d) > floor]:
                del path[day]
            meta[cid]["flag"] = ("the warehouse copy of its series does not give the engine's own reading today (%s against %s): "
                                 "its newest %d months are left out" % (path[max(path)] if path else None, live[cid], SEAM_MONTHS))
        elif abs(gap) > GAP_FLAG:
            meta[cid]["flag"] = "rebuilt %s against the engine's own %s today" % (path[last], live[cid])

    canaries = []
    for cid, m in meta.items():
        rec = judge(paths[cid], by_day, eps, m["fire_at"]) if paths.get(cid) else {"months": 0, "verdict": "no record"}
        row = dict(m)
        row.update(rec)
        row["weight"] = weight_of(rec)
        last = max(paths[cid]) if paths.get(cid) else None
        row["last_day"] = last
        row["last_stress"] = paths[cid][last] if last else None
        row["live_stress"] = live.get(cid)
        row["gap_to_live"] = gaps.get(cid)
        canaries.append(row)

    # ── the barometer, month by month ──
    # level: every canary equal, every mechanism equal, and canaries weighted by what each had shown by that January
    comp, weights_by_year, hist_e, hist_m, hist_w = [], {}, [], [], []
    mech_mean = {}                                    # day -> {mechanism: mean stress of its tested canaries}
    first_day = None
    for day in days:
        e, mm, n, nm = blend(paths, mech_of, day)
        if e is None or n < 20:
            continue
        first_day = first_day or day
        by_m = {}
        for cid, path in paths.items():
            s = path.get(day)
            if s is not None:
                by_m.setdefault(mech_of[cid], []).append(s)
        mech_mean[day] = dict((k, sum(v) / len(v)) for k, v in by_m.items() if len(v) >= MIN_MECH_CANARIES)
        yr = day // 10000
        if yr not in weights_by_year:
            wts = None
            if len(comp) >= WALK_START_MONTHS:
                wts = {}
                for cid, path in paths.items():
                    part = {d: s for d, s in path.items() if d < day and by_day[d]["i"] + FALL_SESSIONS < by_day[day]["i"]}
                    rec = judge(part, by_day, eps, meta[cid]["fire_at"], errors=False) if len(part) >= MIN_MONTHS else None
                    wts[cid] = weight_of(rec) if rec else 1.0
            weights_by_year[yr] = wts
        wts = weights_by_year[yr]
        wm = blend(paths, mech_of, day, wts)[1] if wts else None
        ok = len(hist_e) >= 60                       # a percentile needs five years behind it
        comp.append({"day": day, "per_canary": round(e, 2), "per_mechanism": round(mm, 2), "earned": None if wm is None else round(wm, 2),
                     "pct_per_canary": round(percentile_of(e, hist_e), 1) if ok else None, "pct_per_mechanism": round(percentile_of(mm, hist_m), 1) if ok else None,
                     "pct_earned": round(percentile_of(wm, hist_w), 1) if (wm is not None and len(hist_w) >= 60) else None, "n": n, "n_mech": nm,
                     "mech": dict((k, round(v, 2)) for k, v in sorted(mech_mean[day].items()))})
        hist_e.append(e)
        hist_m.append(mm)
        if wm is not None:
            hist_w.append(wm)

    # the turn: each mechanism against itself k month-ends earlier; mechanisms weighted by how well their own turn had led by that January
    cdays = [c["day"] for c in comp]
    pos = dict((d, i) for i, d in enumerate(cdays))
    mechs = sorted(set(m for v in mech_mean.values() for m in v))

    def mech_turn(day, m, k):
        i = pos[day]
        if i < k or month_of(cdays[i - k]) != _months_back(day, k):
            return None
        a, b = mech_mean[day].get(m), mech_mean[cdays[i - k]].get(m)
        return None if a is None or b is None else a - b

    def turn_weights(k, before_day=None):
        """Each mechanism's weight for the turn from months whose window had closed before a day (all closed months when None)."""
        out = {}
        for m in mechs:
            part = []
            for d in cdays:
                mk = by_day[d]
                if mk["fall"] is None or (before_day is not None and not (d < before_day and mk["i"] + FALL_SESSIONS < by_day[before_day]["i"])):
                    continue
                tv = mech_turn(d, m, k)
                if tv is not None:
                    part.append((tv, mk))
            if len(part) < MIN_MONTHS:
                out[m] = None
                continue
            a = auc([(s, mk["fall"]) for s, mk in part if mk["near_high"]])
            out[m] = round(max(0.0, (a if a is not None else 0.5) - 0.5), 4)
        return out

    def turn_series(k):
        rows, wy = [], {}
        for d in cdays:
            yr = d // 10000
            if yr not in wy:
                wy[yr] = turn_weights(k, d)
            w = wy[yr]
            ts = dict((m, mech_turn(d, m, k)) for m in mechs)
            ts = dict((m, v) for m, v in ts.items() if v is not None)
            if not ts:
                continue
            equal = sum(ts.values()) / len(ts)
            judged = dict((m, w[m]) for m in ts if w.get(m) is not None)
            earned = None
            if judged:
                ws = sum(judged.values())
                earned = (sum(judged[m] * ts[m] for m in judged) / ws) if ws > 0 else sum(ts[m] for m in judged) / len(judged)
            rows.append((d, earned, equal, ts))
        return rows

    def score_rows(rows, since=None, until=None):
        known_rows = [(d, s, by_day[d]) for d, s in rows if s is not None and by_day[d]["fall"] is not None and (since is None or d >= since) and (until is None or d <= until)]
        a, se = auc_with_error([(d // 10000, s, m["fall"]) for d, s, m in known_rows])
        ae, see = auc_with_error([(d // 10000, s, m["fall"]) for d, s, m in known_rows if m["near_high"]])
        return {"months": len(known_rows), "from": known_rows[0][0] if known_rows else None, "to": known_rows[-1][0] if known_rows else None,
                "early_months": sum(1 for _, _, m in known_rows if m["near_high"]),
                "auc": None if a is None else round(a, 4), "auc_se": None if se is None else round(se, 4),
                "auc_early": None if ae is None else round(ae, 4), "auc_early_se": None if see is None else round(see, 4)}

    turns = {}
    for k in TURN_FAMILY:
        rows = turn_series(k)
        turns[str(k)] = {"months_back": k, "earned": score_rows([(d, e) for d, e, _, _ in rows]), "equal": score_rows([(d, q) for d, _, q, _ in rows])}
    rows3 = turn_series(TURN_MONTHS)
    hist_t, rel_rows, cross_rows = [], [], []
    by_comp = dict((c["day"], c) for c in comp)
    for d, earned, equal, ts in rows3:
        c = by_comp[d]
        c["turn_equal"] = round(equal, 2)
        c["turn_by_mech"] = dict((m, round(v, 2)) for m, v in sorted(ts.items()))
        if earned is None:
            continue
        c["turn"] = round(earned, 2)
        if len(hist_t) >= 60:
            c["pct_turn"] = round(percentile_of(earned, hist_t), 1)
            rel_rows.append((c["pct_turn"], by_day[d]))
            cross_rows.append((c.get("pct_per_mechanism"), c["pct_turn"], by_day[d]))
        hist_t.append(earned)
    turn3 = turns[str(TURN_MONTHS)]
    turn3["reliability"] = reliability(rel_rows, TURN_BANDS)
    turn3["values"] = sorted(round(v, 2) for v in hist_t)
    turn3["weights_now"] = turn_weights(TURN_MONTHS)
    scored_rows = [(d, e) for d, e, _, _ in rows3]
    third = len([1 for _, e in scored_rows if e is not None]) // 3
    have = [d for d, e in scored_rows if e is not None]
    if third:
        turn3["by_period"] = [dict(score_rows(scored_rows, have[i * third], have[min(len(have) - 1, (i + 1) * third - 1)] if i < 2 else None)) for i in range(3)]
    mech_record = []
    for m in mechs:
        lvl = score_rows([(d, mech_mean[d].get(m)) for d in cdays])
        trn = score_rows([(d, mech_turn(d, m, TURN_MONTHS)) for d in cdays])
        mech_record.append({"mechanism": m, "canaries": sum(1 for cid in paths if mech_of[cid] == m and paths[cid]), "level": lvl, "turn": trn,
                            "turn_weight_now": turn3["weights_now"].get(m)})

    def scored(field, pct_field):
        have_rows = [(c, by_day[c["day"]]) for c in comp if c.get(field) is not None and c.get(pct_field) is not None]
        v = score_rows([(c["day"], c[field]) for c, _ in have_rows])
        return have_rows, v

    views = {}
    for name in ("per_canary", "per_mechanism", "earned"):
        have_rows, v = scored(name, "pct_" + name)
        v["reliability"] = reliability([(c["pct_" + name], m) for c, m in have_rows])
        v["values"] = sorted(round(c[name], 2) for c in comp if c[name] is not None)
        views[name] = v
    grid = cross(cross_rows)

    live_now = None
    got = {cid: float(s) for cid, s in live.items() if cid in paths and paths[cid] and s is not None}
    if got and comp:
        rebuilt = [paths[cid][max(paths[cid])] for cid in got]
        live_now = {"canaries": len(got), "rebuilt": round(sum(rebuilt) / len(rebuilt), 2), "engines_own": round(sum(got.values()) / len(got), 2)}
        live_now["gap"] = round(live_now["rebuilt"] - live_now["engines_own"], 2)
        ab = sorted(abs(g) for g in gaps.values())
        live_now["median_gap"] = ab[len(ab) // 2] if ab else None
        live_now["flagged"] = sorted(cid for cid, g in gaps.items() if abs(g) > GAP_FLAG)

    counts = {}
    for c in canaries:
        counts[c["verdict"]] = counts.get(c["verdict"], 0) + 1
    known = [m for m in mkt if m["fall"] is not None and (first_day is None or m["day"] >= first_day)]
    early_known = [m for m in known if m["near_high"]]
    return {"version": VERSION, "cut_day": cut_day, "method": {
                "fall": "a close 10% or more under the month-end's close inside 126 sessions", "deep": "20% or more inside 252 sessions",
                "early": "month-ends with the close within 5% of its one-year high", "score": "area under the curve of the reading against the fall; 0.5 is chance",
                "error": "leave one calendar year out", "lag": "an observation counts from the day it was public: its date plus the age of the series' newest observation today",
                "sampling": "daily series as the last observation of each ISO week, windows counted in daily observations divided by five",
                "weights": "1 + %s x (early score - 0.5) x falls / (falls + %s), kept inside %s to %s; 1 with under ten years of record" % (WEIGHT_SLOPE, SHRINK_EPISODES, WEIGHT_FLOOR, WEIGHT_CEIL),
                "walk_forward": "the weighted barometer at any month uses weights worked out that January from months whose six-month window had already closed",
                "level_bands": [{"band": b, "from_percentile": f} for b, f in BANDS], "turn_bands": [{"band": b, "from_percentile": f} for b, f in TURN_BANDS],
                "turn": ("each mechanism's mean stress less its own reading %d month-ends earlier; the mechanisms are then weighted by how far their own turn's early "
                         "score stood above chance in the months already closed that January (none for a mechanism at or under chance)" % TURN_MONTHS),
                "turn_family": list(TURN_FAMILY), "fire": FIRE, "level": "every mechanism's mean stress, each mechanism one vote",
                "judged_on": "%d months and %d falls or more" % (MIN_MONTHS, MIN_EPISODES)},
            "market": {"closes": len(closes), "from": closes[0][0], "to": closes[-1][0], "last_close": closes[-1][1], "month_ends": len(mkt),
                       "falls": [e for e in eps if first_day is None or e["peak"] >= first_day],
                       "from_high_now": round(mkt[-1]["from_high"], 4) if mkt else None,
                       "months": len(known), "fell": sum(1 for m in known if m["fall"]),
                       "fall_share": round(sum(1 for m in known if m["fall"]) / float(len(known)), 4) if known else None,
                       "early_months": len(early_known), "early_fell": sum(1 for m in early_known if m["fall"]),
                       "early_fall_share": round(sum(1 for m in early_known if m["fall"]) / float(len(early_known)), 4) if early_known else None},
            "canaries": canaries, "verdicts": counts, "weights": {c["id"]: c["weight"] for c in canaries},
            "barometer": {"months": comp, "views": views, "turns": turns, "level_against_turn": grid, "mechanisms": mech_record, "from": first_day},
            "live_check": live_now, "tested": sum(1 for c in canaries if c.get("months")), "defined": len(meta)}


def live_reading(record, live, today=None, base=None):
    """Today's barometer on the record's own scale.

    live   {canary id: the engine's own stress now}; only canaries with a record count
    today  yyyymmdd; the record's cut day when not given
    base   {"day", "mech": {mechanism: mean stress then}, "from"} about three months back; without one the record's
           own month-end nearest to 91 days before today is used
    """
    mech = dict((c["id"], c["mechanism"]) for c in record.get("canaries") or [] if c.get("months"))
    w = record.get("weights") or {}
    got = dict((cid, float(s)) for cid, s in (live or {}).items() if cid in mech and s is not None)
    if not got:
        return None
    baro = record.get("barometer") or {}
    today = today or record.get("cut_day")
    out = {"canaries": len(got), "of_tested": len(mech), "record_cut_day": record.get("cut_day"), "day": today}
    means = {}
    for name, weights in (("per_canary", None), ("per_mechanism", None), ("earned", w)):
        by, tot, ws = {}, 0.0, 0.0
        for cid, s in got.items():
            x = 1.0 if weights is None else weights.get(cid, 1.0)
            tot += x * s
            ws += x
            b = by.setdefault(mech[cid], [0.0, 0.0, 0])
            b[0] += x * s
            b[1] += x
            b[2] += 1
        per_mech = dict((k, a / b) for k, (a, b, n) in by.items() if b > 0 and n >= MIN_MECH_CANARIES)
        if not per_mech:
            return None
        if name == "per_mechanism":
            means = per_mech
        value = (tot / ws) if name == "per_canary" else (sum(per_mech.values()) / len(per_mech))
        view = (baro.get("views") or {}).get(name) or {}
        pct = percentile_of(value, view.get("values") or [])
        band = band_of(pct)
        out[name] = {"score": round(value, 1), "percentile": None if pct is None else round(pct, 1), "band": band,
                     "after": next((r for r in view.get("reliability") or [] if r["band"] == band), None),
                     "by_mechanism": dict((k, round(v, 1)) for k, v in sorted(per_mech.items()))}
    out["level"] = out["per_mechanism"]
    # the turn
    months = baro.get("months") or []
    if base is None and months and today:
        want = _ord(today) - 91
        row = min(months, key=lambda m: abs(_ord(m["day"]) - want))
        if abs(_ord(row["day"]) - want) <= 31 and row.get("mech"):
            base = {"day": row["day"], "mech": row["mech"], "from": "the record's own month-end rebuild"}
    turn_view = (baro.get("turns") or {}).get(str(TURN_MONTHS)) or {}
    weights_now = turn_view.get("weights_now") or {}
    if base and base.get("mech"):
        ts = dict((m, means[m] - float(base["mech"][m])) for m in means if base["mech"].get(m) is not None)
        judged = dict((m, weights_now[m]) for m in ts if weights_now.get(m) is not None)
        if ts and judged:
            ws = sum(judged.values())
            change = (sum(judged[m] * ts[m] for m in judged) / ws) if ws > 0 else sum(ts[m] for m in judged) / len(judged)
            pct = percentile_of(change, turn_view.get("values") or [])
            band = band_in(pct, TURN_BANDS) or "NO DATA"
            out["turn"] = {"change": round(change, 1), "against_day": base["day"], "against_from": base.get("from"), "days_back": _ord(today) - _ord(base["day"]),
                           "by_mechanism": dict((m, round(v, 1)) for m, v in sorted(ts.items())), "weights": judged,
                           "percentile": None if pct is None else round(pct, 1), "band": band,
                           "after": next((r for r in turn_view.get("reliability") or [] if r["band"] == band), None)}
            lp = out["level"]["percentile"]
            if lp is not None and pct is not None:
                lname, tname = band_in(lp, GRID_LEVEL), band_in(pct, GRID_TURN)
                out["cell"] = next((c for c in baro.get("level_against_turn") or [] if c["level"] == lname and c["turn"] == tname), None)
    return out
