"""leaders_tape - the pure tape arithmetic of the leaders engine: returns, relative strength, moving averages and their
crosses, tape accumulation / distribution, signed dollar volume, the tight-base (coil) detector and split rebasing.

Everything here is a pure function of daily bars ({date, o, h, l, c, v}, oldest first). Nothing reads S3, nothing
guesses: a figure that needs more sessions than the bars hold is None.

The coil detector and the split rebasing are the strength desk's own definitions (justhodl-strength 1.0.5), kept
identical on purpose - tests/deployment/test_leaders_engine.py runs both engines on the same bars and fails on drift.
"""
from __future__ import annotations

import math
from datetime import date, datetime, timedelta

HORIZONS = (("d", 1), ("w", 5), ("m", 21), ("q", 63), ("h", 126), ("y", 252))
WEIGHTS = (("d", 0.10), ("w", 0.20), ("m", 0.30), ("q", 0.25), ("h", 0.15))      # the strength desk's composite
MA_WINDOWS = (20, 50, 100, 200, 250)
CROSS_FRESH = 20                 # sessions: a 50/200 cross this recent is "being seen"
CROSS_LOOKBACK = 260
NEAR_GAP_PCT = 1.5               # |50 DMA / 200 DMA - 1| at most this, and closing, to call a cross near
NEAR_SESSIONS = 15
BASE_TIERS = ((6.0, 40), (10.0, 40), (15.0, 120))
BASE_MAX = 260
CONTRACTION_MAX = 0.6
DEAL_ATR_PCT, DEAL_RANGE_PCT = 0.6, 3.5
COMMON_RATIOS = (2.0, 3.0, 4.0, 5.0, 6.0, 8.0, 10.0, 15.0, 20.0, 25.0, 50.0, 1.5, 0.5, 1 / 3, 0.25, 0.2, 0.125, 0.1, 2 / 3, 0.05, 0.04, 0.02)
AD_WEIGHTS = (("cmf21", 0.25), ("cmf63", 0.20), ("ud50", 0.25), ("obv63", 0.15), ("days", 0.15))
AD_STRONG, AD_LEAN = 60.0, 20.0                              # calibrated on the live universe (ops 6744, 2026-11-04, 672 leaders): +-60 holds the
                                                            # strongest tenth on the accumulation side, +-20 leaves about a third NEUTRAL
AD_DAY_MOVE, AD_DAY_VOLX, AD_DAY_WINDOW = 0.2, 1.2, 25      # a session up / down at least 0.2% on volume at least 1.2x its 50-session average, last 25 sessions
GAP_DAYS = 10                    # consecutive bars further apart than this are two histories (no US closure has reached a week)
IPO_SLACK_DAYS = 5               # bars older than the listing date by more than this belong to the ticker's previous holder
EVENT_NEAR_DAYS = 7              # how close a recorded ticker change must sit to the break it explains
HANDOVER_MIN, HANDOVER_MAX = 0.7, 1.4      # first close under the new ticker over the last close under the old one
JUMP_UP, JUMP_DOWN = 1.8, 0.56             # the band infer_splits scans: a close-to-close move outside it is an event
SEPARATION_DAYS = 3                        # a new listing this close to a collapse is weighed as what was handed out
SEPARATION_VALUE = (0.5, 2.0)              # ... when its market value is this multiple of the value that vanished
STITCH_DEPTH = 3


# ----------------------------------------------------------------------------------------------------------------- basics
def f(v, d=2):
    try:
        if v is None or (isinstance(v, float) and (math.isnan(v) or math.isinf(v))):
            return None
        return round(float(v), d)
    except (TypeError, ValueError):
        return None


def pct_change(a, b):
    return None if a in (None, 0) or b is None else (b / a - 1.0) * 100.0


def ret_n(c, n):
    return pct_change(c[-1 - n], c[-1]) if len(c) > n else None


def horizons(c):
    return {k: f(ret_n(c, n)) for k, n in HORIZONS}


def sma(c, n, end=None):
    """Mean of the n closes ending at index `end` (exclusive; the last close by default); None when short."""
    end = len(c) if end is None else end
    if n <= 0 or end < n:
        return None
    return sum(c[end - n:end]) / n


def sma_series(c, n):
    """Rolling mean aligned to c: None before the n-th close."""
    out = [None] * len(c)
    if n <= 0 or len(c) < n:
        return out
    s = sum(c[:n])
    out[n - 1] = s / n
    for i in range(n, len(c)):
        s += c[i] - c[i - n]
        out[i] = s / n
    return out


def stdev(xs):
    n = len(xs)
    if n < 2:
        return None
    m = sum(xs) / n
    return math.sqrt(sum((x - m) ** 2 for x in xs) / (n - 1))


def slope(ys):
    n = len(ys)
    if n < 3:
        return None
    xm = (n - 1) / 2.0
    ym = sum(ys) / n
    sxx = sum((i - xm) ** 2 for i in range(n))
    return sum((i - xm) * (y - ym) for i, y in enumerate(ys)) / sxx if sxx else None


def percentile_of(value, population):
    if value is None or not population:
        return None
    below = sum(1 for p in population if p < value)
    ties = sum(1 for p in population if p == value)
    return 100.0 * (below + 0.5 * ties) / len(population)


def clamp(x, lo=-1.0, hi=1.0):
    return max(lo, min(hi, x))


def hi(b):
    return float(b.get("h", b["c"]))


def lo(b):
    return float(b.get("l", b["c"]))


def vol(b):
    return float(b.get("v") or 0)


def align(bars, bench):
    """Closes of a ticker and the benchmark on their common sessions, oldest first."""
    bd = {b["date"]: b for b in bench}
    dates, c, s = [], [], []
    for b in bars:
        x = bd.get(b["date"])
        if x is None:
            continue
        dates.append(b["date"])
        c.append(float(b["c"]))
        s.append(float(x["c"]))
    return dates, c, s


def true_ranges(bars):
    out = [None]
    for i in range(1, len(bars)):
        h, l, pc = hi(bars[i]), lo(bars[i]), float(bars[i - 1]["c"])
        out.append(max(h - l, abs(h - pc), abs(l - pc)))
    return out


def atr_pct(bars, n=14):
    if len(bars) < n + 1:
        return None
    trs = true_ranges(bars)[-n:]
    c = float(bars[-1]["c"])
    return f(100.0 * (sum(trs) / n) / c) if c else None


def atr_pct_median(bars, window=120):
    """Median daily true range as % of the prior close over the last `window` sessions - the instrument's ordinary
    daily swing, used to scale every pattern threshold."""
    xs = []
    start = max(1, len(bars) - window)
    for i in range(start, len(bars)):
        pc = float(bars[i - 1]["c"])
        if pc > 0:
            xs.append(100.0 * max(hi(bars[i]) - lo(bars[i]), abs(hi(bars[i]) - pc), abs(lo(bars[i]) - pc)) / pc)
    if len(xs) < 20:
        return None
    xs.sort()
    m = len(xs) // 2
    return xs[m] if len(xs) % 2 else (xs[m - 1] + xs[m]) / 2.0


# --------------------------------------------------------------------------------------------------- relative strength
def rs_metrics(c, s):
    """RS line metrics on aligned closes (the strength desk's): trend = annualized slope of the log RS line over 63
    sessions (% per year), Mansfield RS (RS line vs its 252-session mean), the RS line's 52-week position and whether
    it sits at a 52-week high."""
    n = len(c)
    out = {"rs_trend_63": None, "rs_vs_52w": None, "rs_52w_pos": None, "rs_new_high": None, "rs_new_low": None}
    if n < 22 or any(x <= 0 for x in c[-260:]) or any(x <= 0 for x in s[-260:]):
        return out
    rs = [c[i] / s[i] for i in range(n)]
    if n >= 63:
        sl = slope([math.log(x) for x in rs[-63:]])
        out["rs_trend_63"] = f(sl * 252 * 100) if sl is not None else None
    if n >= 252:
        win = rs[-252:]
        mean = sum(win) / 252
        top, bot = max(win), min(win)
        out["rs_vs_52w"] = f((rs[-1] / mean - 1) * 100) if mean else None
        out["rs_52w_pos"] = f(100.0 * (rs[-1] - bot) / (top - bot), 1) if top > bot else None
        out["rs_new_high"] = bool(rs[-1] >= top * 0.995)
        out["rs_new_low"] = bool(rs[-1] <= bot * 1.005)
    return out


def composite(vs):
    """Weighted excess return in pp (10 / 20 / 30 / 25 / 15 on 1d / 5d / 21d / 63d / 126d)."""
    s = w = 0.0
    for k, wt in WEIGHTS:
        if vs.get(k) is not None:
            s += vs[k] * wt
            w += wt
    return f(s / w) if w else None


def rank_rows(rows, key="strength"):
    """Cross-sectional z-scores of the excess returns per horizon (winsorized at +-3) -> weighted z -> percentile 0-100
    within these rows. Mutates rows (z, <key>); returns the per-horizon mean / sd."""
    stats = {}
    for k, _ in WEIGHTS:
        xs = [r["vs"][k] for r in rows if r.get("vs") and r["vs"].get(k) is not None]
        sd = stdev(xs)
        stats[k] = (sum(xs) / len(xs), sd) if xs and sd else None
    zs = []
    for r in rows:
        if not r.get("vs"):
            r["z"] = None
            continue
        s = w = 0.0
        for k, wt in WEIGHTS:
            st = stats.get(k)
            v = r["vs"].get(k)
            if st and v is not None:
                s += clamp((v - st[0]) / st[1], -3.0, 3.0) * wt
                w += wt
        r["z"] = f(s / w, 3) if w else None
        if r["z"] is not None:
            zs.append(r["z"])
    for r in rows:
        r[key] = f(percentile_of(r["z"], zs), 1) if r.get("z") is not None else None
    return {k: {"mean": f(v[0]), "sd": f(v[1])} if v else None for k, v in stats.items()}


def context_52w(bars):
    c = [float(b["c"]) for b in bars]
    out = {"hi_252": None, "lo_252": None, "pos_52w": None, "off_high_pct": None}
    if len(c) < 130:
        return out
    win = bars[-min(252, len(c)):]
    top = max(hi(b) for b in win)
    bot = min(lo(b) for b in win)
    out.update(hi_252=f(top), lo_252=f(bot), pos_52w=f(100.0 * (c[-1] - bot) / (top - bot), 1) if top > bot else None, off_high_pct=f((c[-1] / top - 1) * 100) if top else None)
    return out


# ----------------------------------------------------------------------------------------------------- moving averages
def ma_block(c):
    """Where the last close sits against its 20 / 50 / 100 / 200 / 250-session simple averages.

    s<N> the average, d<N> the last close's distance from it in %; above = how many of the averages the close is over
    (of = how many exist); stack_n = how many of the five orderings close > 20 > 50 > 100 > 200 > 250 hold, stack =
    BULL when all five do, BEAR when all five are reversed, MIXED otherwise (ties are neither); s50_slope / s200_slope =
    the average's change over 10 / 20 sessions in %."""
    out = {"above": None, "of": 0, "stack": None, "stack_n": None, "s50_slope": None, "s200_slope": None}
    if not c:
        for n in MA_WINDOWS:
            out["s%d" % n] = out["d%d" % n] = None
        return out
    px = c[-1]
    vals = {}
    for n in MA_WINDOWS:
        s = sma(c, n)
        vals[n] = s
        out["s%d" % n] = f(s, 4)
        out["d%d" % n] = f((px / s - 1) * 100) if s else None
    have = [n for n in MA_WINDOWS if vals[n]]
    out["of"] = len(have)
    out["above"] = sum(1 for n in have if px > vals[n]) if have else None
    chain = [px] + [vals[n] for n in MA_WINDOWS]
    if all(x is not None for x in chain):
        up = sum(1 for i in range(5) if chain[i] > chain[i + 1])
        down = sum(1 for i in range(5) if chain[i] < chain[i + 1])
        out["stack_n"] = up
        out["stack"] = "BULL" if up == 5 else "BEAR" if down == 5 else "MIXED"
    s50_then, s200_then = sma(c, 50, len(c) - 10), sma(c, 200, len(c) - 20)
    if vals[50] and s50_then:
        out["s50_slope"] = f((vals[50] / s50_then - 1) * 100)
    if vals[200] and s200_then:
        out["s200_slope"] = f((vals[200] / s200_then - 1) * 100)
    return out


def cross_50_200(c, dates, lookback=CROSS_LOOKBACK, fresh=CROSS_FRESH):
    """The 50 / 200-session average cross: the most recent one inside `lookback` sessions (GOLDEN = the 50 crossing
    above the 200, DEATH = below), how many sessions ago, the close then and since; whether one is near (the gap at most
    1.5% and closing at a pace that meets inside 15 sessions); how many crosses the last 120 sessions hold (a whipsaw
    count). None when the bars are shorter than 201 sessions."""
    n = len(c)
    if n < 201:
        return None
    a, b = sma_series(c, 50), sma_series(c, 200)
    gap = (a[-1] / b[-1] - 1) * 100 if b[-1] else None
    out = {"regime": "50>200" if a[-1] > b[-1] else "50<200", "gap_pct": f(gap, 3), "type": None, "date": None, "ago": None, "fresh": False, "px_at": None, "since_pct": None,
           "near": None, "eta": None, "n_120": 0, "s200_rising": None}
    start = max(200, n - lookback)
    found = None
    for i in range(n - 1, start - 1, -1):
        d1, d0 = a[i] - b[i], a[i - 1] - b[i - 1]
        if d0 <= 0 < d1 or d0 >= 0 > d1:
            if found is None:
                found = (i, "GOLDEN" if d1 > 0 else "DEATH")
            if i >= n - 120:
                out["n_120"] += 1
        if found is not None and i < n - 120:
            break
    s200_then = b[n - 21] if n - 21 >= 199 else None
    out["s200_rising"] = bool(b[-1] > s200_then) if s200_then else None
    if found:
        i, kind = found
        ago = n - 1 - i
        out.update(type=kind, date=dates[i] if dates else None, ago=ago, fresh=ago <= fresh, px_at=f(c[i]), since_pct=f(pct_change(c[i], c[-1])))
    if not (found and out["fresh"]) and gap is not None and abs(gap) <= NEAR_GAP_PCT and n - 11 >= 199:
        gap_then = (a[n - 11] / b[n - 11] - 1) * 100
        if gap_then * gap > 0 and abs(gap) < abs(gap_then):
            pace = (abs(gap_then) - abs(gap)) / 10.0
            eta = abs(gap) / pace if pace > 0 else None
            if eta is not None and eta <= NEAR_SESSIONS:
                out["near"] = "GOLDEN NEAR" if gap < 0 else "DEATH NEAR"
                out["eta"] = f(eta, 1)
    return out


# ------------------------------------------------------------------------------------------ accumulation / distribution
def cmf(bars):
    num = den = 0.0
    for b in bars:
        h, l, c, v = hi(b), lo(b), float(b["c"]), vol(b)
        if h == l or v <= 0:
            continue
        num += (((c - l) - (h - c)) / (h - l)) * v
        den += v
    return (num / den) if den else None


def updown_volume(bars):
    up = dn = 0.0
    for i in range(1, len(bars)):
        v = vol(bars[i])
        if bars[i]["c"] > bars[i - 1]["c"]:
            up += v
        elif bars[i]["c"] < bars[i - 1]["c"]:
            dn += v
    return (up / dn) if dn > 0 else (None if up == 0 else 9.99)


def obv_slope(bars):
    """OBV change over the window as a fraction of the window's total volume (-1..1)."""
    obv = tot = 0.0
    for i in range(1, len(bars)):
        v = vol(bars[i])
        tot += v
        if bars[i]["c"] > bars[i - 1]["c"]:
            obv += v
        elif bars[i]["c"] < bars[i - 1]["c"]:
            obv -= v
    return (obv / tot) if tot > 0 else None


def footprint_days(bars, window=AD_DAY_WINDOW):
    """Institutional-footprint sessions in the last `window`: accumulation = up at least 0.2% on volume at least 1.2x
    the prior 50 sessions' average; distribution = down at least 0.2% on such volume."""
    n = len(bars)
    if n < 52:
        return None, None
    acc = dist = 0
    for i in range(max(51, n - window), n):
        prior = [vol(b) for b in bars[i - 50:i]]
        avg = sum(prior) / 50.0
        pc = float(bars[i - 1]["c"])
        if avg <= 0 or pc <= 0:
            continue
        move = (float(bars[i]["c"]) / pc - 1) * 100
        if vol(bars[i]) >= AD_DAY_VOLX * avg:
            if move >= AD_DAY_MOVE:
                acc += 1
            elif move <= -AD_DAY_MOVE:
                dist += 1
    return acc, dist


def ad_label(score):
    if score is None:
        return None
    if score >= AD_STRONG:
        return "STRONG ACCUMULATION"
    if score >= AD_LEAN:
        return "ACCUMULATION"
    if score <= -AD_STRONG:
        return "STRONG DISTRIBUTION"
    if score <= -AD_LEAN:
        return "DISTRIBUTION"
    return "NEUTRAL"


def ad_grade(score):
    if score is None:
        return None
    return "A" if score >= AD_STRONG else "B" if score >= AD_LEAN else "E" if score <= -AD_STRONG else "D" if score <= -AD_LEAN else "C"


def ad_block(bars, with_prior=True):
    """Tape accumulation / distribution, -100..+100, from five price-and-volume reads:

      cmf21  Chaikin money flow, 21 sessions (/0.20)        cmf63  the same over 63 (/0.15)
      ud50   up-volume / down-volume over 50 sessions (ln ratio / ln 1.5)
      obv63  net on-balance volume over 63 sessions as a share of the volume traded (/0.15)
      days   accumulation minus distribution sessions in the last 25 (/5)

    each clipped to +-1, weighted 25 / 20 / 25 / 15 / 15 over the reads the bars support. >= +35 STRONG ACCUMULATION
    (A), >= +12 ACCUMULATION (B), <= -12 DISTRIBUTION (D), <= -35 STRONG DISTRIBUTION (E), else NEUTRAL (C). delta_21 is
    the score now minus the score 21 sessions ago."""
    out = {"score": None, "label": None, "grade": None, "cmf21": None, "cmf63": None, "ud50": None, "obv63": None, "acc_days": None, "dist_days": None, "delta_21": None, "score_21": None}
    n = len(bars)
    if n < 30:
        return out
    comp = {}
    m21 = cmf(bars[-21:])
    if m21 is not None:
        out["cmf21"] = f(m21, 3)
        comp["cmf21"] = clamp(m21 / 0.20)
    if n >= 63:
        m63 = cmf(bars[-63:])
        if m63 is not None:
            out["cmf63"] = f(m63, 3)
            comp["cmf63"] = clamp(m63 / 0.15)
    if n >= 51:
        ud = updown_volume(bars[-51:])
        if ud is not None and ud > 0:
            out["ud50"] = f(ud)
            comp["ud50"] = clamp(math.log(ud) / math.log(1.5))
    if n >= 64:
        ob = obv_slope(bars[-64:])
        if ob is not None:
            out["obv63"] = f(ob, 3)
            comp["obv63"] = clamp(ob / 0.15)
    acc, dist = footprint_days(bars)
    if acc is not None:
        out["acc_days"], out["dist_days"] = acc, dist
        comp["days"] = clamp((acc - dist) / 5.0)
    if comp:
        w = sum(wt for k, wt in AD_WEIGHTS if k in comp)
        score = 100.0 * sum(comp[k] * wt for k, wt in AD_WEIGHTS if k in comp) / w
        out.update(score=f(score, 1), label=ad_label(score), grade=ad_grade(score))
    if with_prior and n >= 30 + 21 and out["score"] is not None:
        prior = ad_block(bars[:-21], with_prior=False)
        if prior["score"] is not None:
            out["score_21"] = prior["score"]
            out["delta_21"] = f(out["score"] - prior["score"], 1)
    return out


def money_flow(bars, n):
    """Signed dollar volume over the last n sessions: a session's dollar volume (typical price x shares) counts as
    money in when the typical price rose from the session before, money out when it fell. A tape estimate of buying
    against selling pressure - not a reported flow, and every share bought was also sold."""
    if len(bars) < n + 1:
        return None
    mi = mo = 0.0
    for i in range(len(bars) - n, len(bars)):
        tp = (hi(bars[i]) + lo(bars[i]) + float(bars[i]["c"])) / 3.0
        ptp = (hi(bars[i - 1]) + lo(bars[i - 1]) + float(bars[i - 1]["c"])) / 3.0
        m = tp * vol(bars[i])
        if tp > ptp:
            mi += m
        elif tp < ptp:
            mo += m
    tot = mi + mo
    return {"in": f(mi, 0), "out": f(mo, 0), "net": f(mi - mo, 0), "net_pct": f(100.0 * (mi - mo) / tot, 1) if tot > 0 else None}


def money_flows(bars):
    """Signed dollar volume on 5 / 21 / 63 sessions + the 14-session money flow index."""
    out = {"w": money_flow(bars, 5), "m": money_flow(bars, 21), "q": money_flow(bars, 63), "mfi14": None}
    x = money_flow(bars, 14)
    if x and x["in"] is not None and x["out"] is not None:
        out["mfi14"] = f(100.0 if x["out"] == 0 and x["in"] > 0 else (100.0 - 100.0 / (1.0 + x["in"] / x["out"])) if x["out"] > 0 else None, 1)
    return out


def volume_now(bars):
    """The heaviest session of the last five against its own prior 50-session average volume: {x, date, move_pct, dir}."""
    n = len(bars)
    if n < 56:
        return None
    best = None
    for i in range(n - 5, n):
        avg = sum(vol(b) for b in bars[i - 50:i]) / 50.0
        if avg <= 0:
            continue
        x = vol(bars[i]) / avg
        if best is None or x > best[0]:
            best = (x, i)
    if best is None:
        return None
    x, i = best
    move = pct_change(float(bars[i - 1]["c"]), float(bars[i]["c"]))
    return {"x": f(x), "date": bars[i]["date"], "move_pct": f(move), "dir": "UP" if (move or 0) > 0 else "DOWN" if (move or 0) < 0 else "FLAT"}


# ----------------------------------------------------------------------------------------- the coil (strength desk's)
def bb_width(c, n=20, k=2.0, end=None):
    end = len(c) if end is None else end
    if end < n:
        return None
    win = c[end - n:end]
    m = sum(win) / n
    sd = math.sqrt(sum((x - m) ** 2 for x in win) / n)
    return 100.0 * (2 * k * sd) / m if m else None


def bb_width_series(c, n=20, k=2.0, back=252):
    out = []
    for end in range(max(n, len(c) - back + 1), len(c) + 1):
        w = bb_width(c, n, k, end)
        if w is not None:
            out.append(w)
    return out


def base_lengths(c, tiers=BASE_TIERS, n_max=BASE_MAX):
    """For each tier threshold, the longest trailing run of sessions whose closes all sit within that % of the last close."""
    last = c[-1]
    if not last or last <= 0:
        return {t: 0 for t, _ in tiers}, {}
    found = {t: 0 for t, _ in tiers}
    ranges = {}
    mx = mn = last
    open_t = [t for t, _ in tiers]
    for L in range(1, min(n_max, len(c)) + 1):
        x = c[-L]
        if x > mx:
            mx = x
        if x < mn:
            mn = x
        rng = 100.0 * (mx - mn) / last
        still = []
        for t in open_t:
            if rng <= t:
                found[t] = L
                ranges[t] = rng
            else:
                continue
            still.append(t)
        open_t = still
        if not open_t:
            break
    return found, ranges


def tier_of(found, ranges=None, range_252=None):
    def contracted(t):
        if range_252 is None or ranges is None or t not in ranges:
            return True
        return range_252 > 0 and ranges[t] / range_252 <= CONTRACTION_MAX
    if found.get(6.0, 0) >= 40 and contracted(6.0):
        return "VERY TIGHT", 6.0
    if found.get(10.0, 0) >= 40 and contracted(10.0):
        return "TIGHT", 10.0
    if found.get(15.0, 0) >= 120 and contracted(15.0):
        return "LONG BASE", 15.0
    if found.get(10.0, 0) >= 20 and contracted(10.0):
        return "FORMING", 10.0
    return None, None


def length_points(L):
    return 0.0 if L < 20 else min(35.0, 10.0 + 25.0 * math.log(L / 20.0) / math.log(12.5))


def tight_points(rng):
    for lim, pts in ((4.0, 30), (6.0, 25), (8.0, 19), (10.0, 14), (15.0, 7)):
        if rng <= lim:
            return float(pts)
    return 0.0


def coil(bars, c_al, s_al, rs_trend):
    """The long-accumulation / tight-price read (the strength desk's coil, same thresholds and score)."""
    c = [float(b["c"]) for b in bars]
    out = {"tier": None, "base_len": 0, "base_range_pct": None, "wick_range_pct": None, "len_6": 0, "len_10": 0, "len_15": 0, "atr14_pct": None, "range_252_pct": None, "contraction": None,
           "bb_width_pct": None, "bb_width_pctile_252": None, "vol_dry": None, "cmf_base": None, "updown_vol": None, "obv_slope_base": None,
           "rs_hold_base": None, "pos_in_base": None, "score": None, "points": None}
    if len(c) < 20:
        return out
    found, ranges = base_lengths(c)
    year = c[-252:]
    range_252 = 100.0 * (max(year) - min(year)) / c[-1] if len(year) >= 120 and c[-1] else None
    out.update(len_6=found[6.0], len_10=found[10.0], len_15=found[15.0], atr14_pct=atr_pct(bars), range_252_pct=f(range_252))
    bw = bb_width(c)
    out["bb_width_pct"] = f(bw)
    series = bb_width_series(c)
    if bw is not None and len(series) >= 60:
        out["bb_width_pctile_252"] = f(percentile_of(bw, series), 1)
    if len(c) >= 140:
        recent = [vol(b) for b in bars[-20:]]
        prior = [vol(b) for b in bars[-140:-20]]
        pa = sum(prior) / len(prior)
        out["vol_dry"] = f((sum(recent) / 20) / pa, 2) if pa > 0 else None
    tier, thr = tier_of(found, ranges, range_252)
    if tier is None:
        return out
    L, rng = found[thr], ranges[thr]
    win = bars[-L:]
    out.update(tier=tier, base_len=L, base_range_pct=f(rng), contraction=f(rng / range_252, 2) if range_252 else None)
    top = max(hi(b) for b in win)
    bot = min(lo(b) for b in win)
    out["wick_range_pct"] = f(100.0 * (top - bot) / c[-1])
    cmx, cmn = max(c[-L:]), min(c[-L:])
    out["pos_in_base"] = f(100.0 * (c[-1] - cmn) / (cmx - cmn), 1) if cmx > cmn else None
    m = cmf(win)
    ud = updown_volume(win)
    ob = obv_slope(win)
    out.update(cmf_base=f(m, 3), updown_vol=f(ud), obv_slope_base=f(ob, 3))
    if len(c_al) > L and len(s_al) > L:
        out["rs_hold_base"] = f(pct_change(c_al[-1 - L], c_al[-1]) - pct_change(s_al[-1 - L], s_al[-1]))
    squeeze = out["bb_width_pctile_252"]
    sq = 10.0 if squeeze is not None and squeeze <= 10 else 6.0 if squeeze is not None and squeeze <= 25 else 2.0 if squeeze is not None and squeeze <= 50 else 0.0
    vd = out["vol_dry"]
    dry = 8.0 if vd is not None and vd <= 0.6 else 5.0 if vd is not None and vd <= 0.8 else 2.0 if vd is not None and vd <= 0.95 else 0.0
    acc = (7.0 if m is not None and m > 0.10 else 4.0 if m is not None and m > 0.03 else 0.0) + (5.0 if ud is not None and ud >= 1.25 else 2.0 if ud is not None and ud >= 1.05 else 0.0)
    held = out["rs_hold_base"] is not None and out["rs_hold_base"] >= 0
    trend = rs_trend is not None and rs_trend >= 0
    rs_pts = 5.0 if (held and trend) else 2.0 if (held or trend) else 0.0
    out["points"] = {"length": f(length_points(L), 1), "tightness": tight_points(rng), "squeeze": sq, "dry_up": dry, "accumulation": acc, "rs_hold": rs_pts}
    out["score"] = f(min(100.0, length_points(L) + tight_points(rng) + sq + dry + acc + rs_pts), 1)
    return out


def pin_flag(cl, is_fund):
    """A stock printing almost no daily range inside a sliver of a base trades like a pending deal, not an accumulation."""
    if cl.get("tier") and not is_fund and cl.get("atr14_pct") is not None and cl["atr14_pct"] <= DEAL_ATR_PCT and (cl.get("base_range_pct") or 99) <= DEAL_RANGE_PCT:
        cl["flag"] = "PINNED"
        cl["flag_note"] = "deal-like: ATR %.2f%% <= %.1f%% and base range %.2f%% <= %.1f%%; kept off the tight board" % (cl["atr14_pct"], DEAL_ATR_PCT, cl["base_range_pct"], DEAL_RANGE_PCT)
    return cl


# ---------------------------------------------------------------------------------------------------------- continuity
# The bank keys bars by ticker, and a ticker is not a security: it changes hands (BNY was a municipal fund until Bank
# of New York Mellon took it), a company trades under another symbol for a while (Fiserv: FISV, FI, FISV), a listing
# starts. A horizon measured across such a break is not this instrument's return (ops 6745: +1,524% "one year" on BNY).
# own_history cuts a series down to its last continuous stretch; predecessor + stitch put the earlier symbol's bars
# back in front of it when the record of the change and the tape agree; separation_cut removes the history of a
# company that handed most of itself to a newly listed one. Nothing is guessed: without the record the cut stands.
def _days(a, b):
    try:
        return (date.fromisoformat(str(b)[:10]) - date.fromisoformat(str(a)[:10])).days
    except ValueError:
        return None


def _near(a, b, n):
    d = _days(a, b)
    return d is not None and abs(d) <= n


def own_history(bars, ipo_date=None):
    """The part of a banked series that is one instrument's continuous history: everything after the last calendar gap
    of more than GAP_DAYS, and nothing older than the listing date. A listing date can sit a session or two after the
    first real print (a when-issued bar, a provider's date), so bars up to IPO_SLACK_DAYS before it are kept while
    each one hands over to the next within x0.7..x1.4 - the previous holder of a ticker does not. An ipo_date later
    than every bar contradicts the tape and is ignored. Returns (bars, cut) - cut is None or {reason: 'gap' |
    'listing', date, dropped, ...}. Pure."""
    start, cut = 0, None
    for i in range(1, len(bars)):
        g = _days(bars[i - 1]["date"], bars[i]["date"])
        if g is not None and g > GAP_DAYS:
            start, cut = i, {"reason": "gap", "date": bars[i]["date"], "prev": bars[i - 1]["date"], "gap_days": g, "dropped": i}
    if ipo_date and len(str(ipo_date)) >= 10:
        try:
            listed = date.fromisoformat(str(ipo_date)[:10])
        except ValueError:
            listed = None
        if listed:
            floor = (listed - timedelta(days=IPO_SLACK_DAYS)).isoformat()
            j = next((k for k in range(start, len(bars)) if bars[k]["date"] >= listed.isoformat()), None)
            if j is not None and j > start:
                while j > start and bars[j - 1]["date"] >= floor:
                    pc, c = float(bars[j - 1]["c"]), float(bars[j]["c"])
                    if not (pc > 0 and HANDOVER_MIN <= c / pc <= HANDOVER_MAX):
                        break
                    j -= 1
                if j > start:
                    start, cut = j, {"reason": "listing", "date": bars[j]["date"], "ipo_date": listed.isoformat(), "dropped": j}
    return (bars[start:] if start else bars), cut


def ticker_events(doc):
    """[(date, TICKER)] oldest first from a Polygon ticker-events document ({results: {events: [{type: 'ticker_change',
    date, ticker_change: {ticker}}]}}); the ticker is '' while the security had none. Pure."""
    res = doc.get("results") if isinstance(doc, dict) else None
    events = res.get("events") if isinstance(res, dict) else None
    out = []
    for e in events if isinstance(events, list) else []:
        if isinstance(e, dict) and e.get("type") == "ticker_change" and len(str(e.get("date") or "")) >= 10:
            out.append((str(e["date"])[:10], str((e.get("ticker_change") or {}).get("ticker") or "").upper().strip().replace("-", ".")))
    return sorted(out)


def predecessor(ticker, start, cut, events, changes):
    """The symbol this security traded under right before `start` (the first bar of its own history), from the
    records only: (old ticker, source) or None.

    Polygon's ticker events for the security name the symbol it carried before - accepted when the record also
    places the change at the break: either the event that gave it `ticker` sits within EVENT_NEAR_DAYS of `start`,
    or (a company returning to a symbol it used before, where the record stops at the departure) the earlier
    symbol's own event sits within EVENT_NEAR_DAYS of the bar the gap began on. A stretch with no symbol at all is
    not a predecessor. FMP's symbol-change list answers the same question for the last hundred changes. Pure."""
    t = str(ticker).upper()
    ev = [(d, x) for d, x in (events or []) if d < str(start) or x == t]
    took = [d for d, x in ev if x == t and _near(d, start, EVENT_NEAR_DAYS)]
    prior = [(d, x) for d, x in ev if x != t and d < str(start)]
    if prior:
        d_old, old = prior[-1]
        if old:
            left = bool(cut) and cut.get("reason") == "gap" and _near(cut.get("prev"), d_old, EVENT_NEAR_DAYS)
            if took or left:
                return old, "polygon-ticker-events"
    for row in changes or []:
        if not isinstance(row, dict):
            continue
        new = str(row.get("newSymbol") or "").upper().strip().replace("-", ".")
        old = str(row.get("oldSymbol") or "").upper().strip().replace("-", ".")
        if new == t and old and old != t and _near(row.get("date"), start, EVENT_NEAR_DAYS):
            return old, "fmp-symbol-change"
    return None


def stitch(own, old_bars, old_ticker, source):
    """The predecessor's bars in front of the instrument's own - only when the tape agrees with the record: the old
    symbol's last bar is within GAP_DAYS before the first new one, it prints nothing in the GAP_DAYS after (it went
    quiet, it did not carry on as another security), and the first close under the new symbol is within x0.7..x1.4 of
    the last close under the old one. Returns (bars, info, refusal): info is None when refused and refusal says why.
    Pure."""
    if not own or not old_bars:
        return own, None, "no predecessor bars"
    start = own[0]["date"]
    limit = (date.fromisoformat(start) + timedelta(days=GAP_DAYS)).isoformat()
    if any(start <= b["date"] <= limit for b in old_bars):
        return own, None, "the old symbol kept trading"
    before, _ = own_history([b for b in old_bars if b["date"] < start])
    if not before:
        return own, None, "no predecessor bars before the change"
    gap = _days(before[-1]["date"], start)
    last, first = float(before[-1]["c"]), float(own[0]["c"])
    if gap is None or gap > GAP_DAYS or last <= 0 or first <= 0:
        return own, None, "the old symbol stopped %s days earlier" % gap
    x = first / last
    if not (HANDOVER_MIN <= x <= HANDOVER_MAX):
        return own, None, "hand-over x%.3f is not one security continuing" % x
    return before + own, {"from": old_ticker, "on": start, "source": source, "sessions": len(before), "handover_x": f(x, 4), "first": before[0]["date"]}, None


def separation_cut(bars, applied, ticker, mcap, sector, listings):
    """A company that hands most of itself to a newly listed one prints an overnight collapse that no holder lost
    (Corteva 2026-10-01: x0.16 the day Vylor listed). The collapse is read as a separation - and the history before it
    as another company's - only on evidence: a close-to-close move of x0.56 or less that is not a split, and a common
    stock of the same sector whose listing date is within SEPARATION_DAYS of it and whose market value is 0.5x-2x the
    value that vanished. listings = [(date, ticker, mcap, sector)]. Returns (bars, cut). Pure."""
    if not bars or not mcap or not listings:
        return bars, None
    last = float(bars[-1]["c"])
    if last <= 0:
        return bars, None
    shares = float(mcap) / last
    done = {e.get("date") for e in applied or []}
    for i in range(len(bars) - 1, 0, -1):
        pc, c = float(bars[i - 1]["c"]), float(bars[i]["c"])
        if pc <= 0 or c <= 0 or c / pc > JUMP_DOWN or bars[i]["date"] in done:
            continue
        vanished = (pc - c) * shares
        for d, t, m, sec in listings:
            near = _days(d, bars[i]["date"])
            if t != ticker and m and sec == sector and near is not None and abs(near) <= SEPARATION_DAYS and SEPARATION_VALUE[0] <= m / vanished <= SEPARATION_VALUE[1]:
                return bars[i:], {"reason": "separation", "date": bars[i]["date"], "ratio": f(c / pc, 4), "listing": t, "listing_date": d, "listing_mcap_usd": f(m, 0), "vanished_usd": f(vanished, 0), "dropped": i}
    return bars, None


def jumps_of(bars, applied=None):
    """The close-to-close moves beyond x1.8 / x0.56 still inside a series after rebasing - a crash, a bid, a result:
    [{date, ratio, ago}]. They are real prints; a horizon that spans one says so. Pure."""
    done = {e.get("date") for e in applied or []}
    out = []
    for i in range(1, len(bars)):
        pc, c = float(bars[i - 1]["c"]), float(bars[i]["c"])
        if pc > 0 and c > 0 and not (JUMP_DOWN < c / pc < JUMP_UP) and bars[i]["date"] not in done:
            out.append({"date": bars[i]["date"], "ratio": f(c / pc, 4), "ago": len(bars) - 1 - i})
    return out


# -------------------------------------------------------------------------------------------------------------- splits
def apply_splits(bars, events):
    """Backward split adjustment of unadjusted bank bars: every bar BEFORE a split date is multiplied by the split's
    price factor (a 2-for-1 split -> 0.5, a 1-for-3 reverse split -> 3.0) and its volume divided by it. Pure."""
    ev = sorted([(str(e["date"]), float(e["factor"])) for e in events or [] if e.get("date") and e.get("factor")], key=lambda x: x[0])
    if not ev:
        return bars
    out = []
    for b in bars:
        k = 1.0
        for d, x in ev:
            if b["date"] < d:
                k *= x
        if k == 1.0:
            out.append(b)
            continue
        nb = dict(b)
        for fld in ("o", "h", "l", "c", "vw"):
            if nb.get(fld) is not None:
                nb[fld] = float(nb[fld]) * k
        if nb.get("v") is not None:
            nb["v"] = float(nb["v"]) / k
        out.append(nb)
    return out


def infer_splits(bars):
    """Unrecorded splits in unadjusted bars (the strength desk's rule): a close-to-close ratio >= 1.8 or <= 0.56 is a
    split when the shares traded (5 sessions each side) moved inversely by about the same amount (within x1.6), OR
    when the ratio lands within 2% of a common split ratio and volume moved in the split's direction by no more than
    x2.5 away from it - a 2-for-1 doubles the shares traded; a 50% crash multiplies them many times over and stays a
    crash."""
    found = []
    n = len(bars)
    for i in range(1, n):
        pc, c = float(bars[i - 1]["c"]), float(bars[i]["c"])
        if pc <= 0 or c <= 0:
            continue
        r = c / pc
        if 0.56 < r < 1.8:
            continue
        before = [vol(b) for b in bars[max(0, i - 5):i]]
        after = [vol(b) for b in bars[i:i + 5]]
        if len(before) < 3 or len(after) < 3 or not sum(before) or not sum(after):
            continue
        vr = (sum(before) / len(before)) / (sum(after) / len(after))
        snap = min(COMMON_RATIOS, key=lambda x: abs(math.log(x) - math.log(r)))
        exact = abs(math.log(snap) - math.log(r)) <= math.log(1.02)
        volume_matches = abs(math.log(vr) - math.log(r)) <= math.log(1.6)
        volume_direction = (vr > 1.0) if r > 1 else (vr < 1.0)
        volume_bounded = abs(math.log(vr) - math.log(r)) <= math.log(2.5)
        if not (volume_matches or (exact and volume_direction and volume_bounded)):
            continue
        ratio = snap if abs(math.log(snap) - math.log(r)) <= math.log(1.04) else r
        found.append({"date": bars[i]["date"], "factor": f(ratio, 6), "ratio": f(r, 4), "source": "inferred", "evidence": "volume" if volume_matches else "ratio"})
    return found


RECORDED_MIN_LN = math.log(1.25)       # a recorded split smaller than 5-for-4 is left alone: its print is inside ordinary daily moves
RECORDED_TOL_LN = math.log(1.15)


def confirm_recorded(bars, recorded):
    """Recorded splits (Polygon's reference file: execution date, split_from, split_to) the bank's bars still carry
    UNADJUSTED. The bank's history was pulled adjusted, so a split that executed before the pull is already inside the
    bars and must not be applied twice; only sessions appended after the pull are raw. A recorded split is applied when
    the bars print it: the close-to-close ratio on the execution session (or the next two) lands within 15% of the
    split's price factor. Returns the events to apply ({date, factor, ratio, source: 'polygon-splits'}). Pure."""
    out = []
    if not bars or not recorded:
        return out
    idx = {b["date"]: i for i, b in enumerate(bars)}
    dates = [b["date"] for b in bars]
    for e in recorded:
        try:
            factor = float(e["split_from"]) / float(e["split_to"])
            day = str(e["date"])
        except (KeyError, TypeError, ValueError, ZeroDivisionError):
            continue
        if factor <= 0 or abs(math.log(factor)) < RECORDED_MIN_LN:
            continue
        i = idx.get(day)
        if i is None:
            later = [k for k, d in enumerate(dates) if d > day]
            i = later[0] if later else None
        if i is None or i < 1:
            continue
        for j in range(i, min(i + 3, len(bars))):
            pc, c = float(bars[j - 1]["c"]), float(bars[j]["c"])
            if pc > 0 and c > 0 and abs(math.log(c / pc) - math.log(factor)) <= RECORDED_TOL_LN:
                out.append({"date": bars[j]["date"], "factor": f(factor, 6), "ratio": f(c / pc, 4), "source": "polygon-splits", "split_from": e.get("split_from"), "split_to": e.get("split_to")})
                break
    return out


def rebase(bars, recorded=None):
    """Bars with every split the bank left unadjusted rebased: recorded splits the bars still print first, then
    inferred ones not within three days of a recorded one. Returns (bars, events)."""
    if not bars:
        return bars, []
    ev = confirm_recorded(bars, recorded or [])
    adjusted = apply_splits(bars, ev) if ev else bars
    inferred = [x for x in infer_splits(adjusted) if not any(abs((datetime.fromisoformat(x["date"]) - datetime.fromisoformat(r["date"])).days) <= 3 for r in ev)]
    if inferred:
        adjusted = apply_splits(adjusted, inferred)
        ev = ev + inferred
    return adjusted, ev
