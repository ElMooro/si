"""leaders_patterns - swing pivots, double tops / double bottoms and Wyckoff buying / selling climaxes on daily bars.

Pure functions of split-rebased daily bars ({date, o, h, l, c, v}, oldest first). Every threshold scales with the
instrument's own ordinary daily swing (the median true range of the last 120 sessions), so a utility and a
semiconductor are read by the same rule.

DOUBLE TOP     two swing highs within tol of each other, 10-150 sessions apart, with a swing low between them at
               least max(5%, the swing threshold) below the lower high; the first high was a 90-session high (a top of
               an advance, not a wiggle inside a range). FORMING while the closes hold the swing low between them (the
               neckline); CONFIRMED on a close 0.5% under it; void once a close clears the higher high by 0.5%.
DOUBLE BOTTOM  the mirror: two swing lows within tol, a swing high between them at least max(5%, threshold) above the
               higher low; the first low was a 90-session low. CONFIRMED on a close 0.5% over the neckline; void on a
               close 0.5% under the lower low. vol_ratio = volume around the second pivot over volume around the first -
               a second low on lighter volume is the secondary test of Khalid's bottom paper.
CLIMAX         the bottom desk's selling climax (justhodl-bottom 1.2.0) and its mirror, on the last 30 sessions:
               SELLING  a new 120-session low, the close under the 50-session average and in the bottom 35% of the
                        252-session range, a decline of at least 6 ATR and 12% (funds 10%) off the 120-session high
                        that printed at least 15 sessions earlier (or a 25% crash), the heaviest volume of the prior 60
                        sessions and at least 2x the 250-session average, a range of at least 1.5 ATR - and absorption:
                        the close off the low (upper 60% of the bar) or a higher close inside three sessions.
               BUYING   a new 120-session high, the close over the 50-session average and in the top 35% of the range,
                        an advance of at least 6 ATR and 20% (funds 12%) off the 120-session low that printed at least
                        15 sessions earlier, the same volume and range gates - and supply: the close off the high
                        (lower 60% of the bar) or a lower close inside three sessions.
"""
from __future__ import annotations

from leaders_tape import atr_pct_median, f, hi, lo, pct_change, sma, true_ranges, vol

THETA_MIN, THETA_MAX, THETA_ATR = 4.0, 12.0, 3.0          # swing threshold %: 3x the median daily true range, inside 4..12
TOL_MIN, TOL_MAX, TOL_ATR = 3.0, 5.0, 1.5                 # how close the two highs / lows must be, %
SEP_MIN, SEP_MAX = 10, 150
DEPTH_MIN = 5.0
PRIOR_EXTREME = 90                                        # the first pivot must be a 90-session high / low
PATTERN_MAX_AGE = 60                                      # the second pivot at most this many sessions ago
BREAK_FRESH = 20                                          # a neckline break at most this many sessions ago
BREAK_PCT = 0.5
PROVISIONAL_PULLBACK_MIN, PROVISIONAL_PULLBACK_ATR = 2.0, 1.0
CLIMAX_WINDOW = 30
SC_DECLINE = {"stock": 12.0, "fund": 10.0}
BC_ADVANCE = {"stock": 20.0, "fund": 12.0}


def theta_pct(bars):
    """The swing threshold for this instrument, %."""
    m = atr_pct_median(bars)
    if m is None:
        return None
    return max(THETA_MIN, min(THETA_MAX, THETA_ATR * m))


def pivots(bars, theta):
    """Alternating swing highs and lows of the daily highs / lows: a high is confirmed once a later low sits theta%
    under it, a low once a later high sits theta% over it. The last pivot is the running extreme of the open leg and is
    marked provisional. [{i, kind: 'H'|'L', px, date, provisional?}]"""
    n = len(bars)
    if n < 3 or not theta:
        return []
    th = theta / 100.0
    piv = []
    up = None
    max_i = min_i = ext = 0
    for i in range(1, n):
        if up is None:
            if hi(bars[i]) > hi(bars[max_i]):
                max_i = i
            if lo(bars[i]) < lo(bars[min_i]):
                min_i = i
            if max_i > min_i and hi(bars[max_i]) >= lo(bars[min_i]) * (1 + th):
                piv.append({"i": min_i, "kind": "L", "px": lo(bars[min_i]), "date": bars[min_i]["date"]})
                up, ext = True, max_i
            elif min_i > max_i and lo(bars[min_i]) <= hi(bars[max_i]) * (1 - th):
                piv.append({"i": max_i, "kind": "H", "px": hi(bars[max_i]), "date": bars[max_i]["date"]})
                up, ext = False, min_i
        elif up:
            if hi(bars[i]) > hi(bars[ext]):
                ext = i
            elif lo(bars[i]) <= hi(bars[ext]) * (1 - th):
                piv.append({"i": ext, "kind": "H", "px": hi(bars[ext]), "date": bars[ext]["date"]})
                up, ext = False, i
        else:
            if lo(bars[i]) < lo(bars[ext]):
                ext = i
            elif hi(bars[i]) >= lo(bars[ext]) * (1 + th):
                piv.append({"i": ext, "kind": "L", "px": lo(bars[ext]), "date": bars[ext]["date"]})
                up, ext = True, i
    if up is not None:
        piv.append({"i": ext, "kind": "H" if up else "L", "px": hi(bars[ext]) if up else lo(bars[ext]), "date": bars[ext]["date"], "provisional": True})
    return piv


def _around(bars, i, k=2):
    win = bars[max(0, i - k):i + k + 1]
    return sum(vol(b) for b in win) / len(win) if win else None


def _double(bars, p1, mid, p2, top, atr_m):
    """One candidate (first pivot, the swing between, second pivot) -> the pattern dict or None."""
    n = len(bars)
    c = [float(b["c"]) for b in bars]
    last = c[-1]
    tol = max(TOL_MIN, min(TOL_MAX, TOL_ATR * atr_m))
    diff = abs(p2["px"] - p1["px"]) / p1["px"] * 100.0
    sep = p2["i"] - p1["i"]
    if diff > tol or not (SEP_MIN <= sep <= SEP_MAX) or n - 1 - p2["i"] > PATTERN_MAX_AGE:
        return None
    inner = min(p1["px"], p2["px"]) if top else max(p1["px"], p2["px"])
    outer = max(p1["px"], p2["px"]) if top else min(p1["px"], p2["px"])
    neck = mid["px"]
    depth = (inner - neck) / inner * 100.0 if top else (neck - inner) / inner * 100.0
    if depth < max(DEPTH_MIN, max(THETA_MIN, min(THETA_MAX, THETA_ATR * atr_m))):
        return None
    before = bars[max(0, p1["i"] - PRIOR_EXTREME):p1["i"]]
    if len(before) < 40:
        return None
    if top and p1["px"] < max(hi(b) for b in before):
        return None                                           # the first high was not a 90-session high
    if not top and p1["px"] > min(lo(b) for b in before):
        return None
    if p2.get("provisional"):
        need = max(PROVISIONAL_PULLBACK_MIN, PROVISIONAL_PULLBACK_ATR * atr_m)
        moved = (p2["px"] - last) / p2["px"] * 100.0 if top else (last - p2["px"]) / p2["px"] * 100.0
        if moved < need:
            return None                                       # price is still at the second extreme: a retest, not yet a second top / bottom
    after = range(p2["i"] + 1, n)
    brk = None
    for j in after:
        if top and c[j] > outer * (1 + BREAK_PCT / 100.0):
            return None                                       # cleared the highs: not a double top
        if not top and c[j] < outer * (1 - BREAK_PCT / 100.0):
            return None                                       # undercut the lows: not a double bottom
        if brk is None and ((top and c[j] < neck * (1 - BREAK_PCT / 100.0)) or (not top and c[j] > neck * (1 + BREAK_PCT / 100.0))):
            brk = j
    if brk is not None and n - 1 - brk > BREAK_FRESH:
        return None                                           # the break is old news
    height = abs(outer - neck)
    target = neck - height if top else neck + height
    v1, v2 = _around(bars, p1["i"]), _around(bars, p2["i"])
    return {
        "kind": "DOUBLE TOP" if top else "DOUBLE BOTTOM", "state": "CONFIRMED" if brk is not None else "FORMING",
        "p1": {"date": p1["date"], "px": f(p1["px"])}, "p2": {"date": p2["date"], "px": f(p2["px"]), "provisional": bool(p2.get("provisional"))},
        "neck": {"date": mid["date"], "px": f(neck)}, "diff_pct": f(diff), "depth_pct": f(depth), "sep": sep, "ago": n - 1 - p2["i"],
        "break": {"date": bars[brk]["date"], "ago": n - 1 - brk} if brk is not None else None,
        "to_neck_pct": f((neck / last - 1) * 100.0) if last else None,
        "target": f(target) if target > 0 else None, "target_pct": f((target / last - 1) * 100.0) if last and target > 0 else None,
        "vol_ratio": f(v2 / v1) if v1 and v2 else None,
    }


def double_patterns(bars):
    """The most recent double top and the most recent double bottom still in play: {double_top, double_bottom, theta_pct}."""
    out = {"double_top": None, "double_bottom": None, "theta_pct": None}
    if len(bars) < 130:
        return out
    atr_m = atr_pct_median(bars)
    theta = theta_pct(bars)
    if atr_m is None or theta is None:
        return out
    out["theta_pct"] = f(theta)
    piv = pivots(bars, theta)
    for k in range(len(piv) - 1, 1, -1):
        p1, mid, p2 = piv[k - 2], piv[k - 1], piv[k]
        if p2["kind"] == "H" and mid["kind"] == "L" and p1["kind"] == "H" and out["double_top"] is None:
            out["double_top"] = _double(bars, p1, mid, p2, True, atr_m)
        elif p2["kind"] == "L" and mid["kind"] == "H" and p1["kind"] == "L" and out["double_bottom"] is None:
            out["double_bottom"] = _double(bars, p1, mid, p2, False, atr_m)
        if len(bars) - 1 - p2["i"] > PATTERN_MAX_AGE + SEP_MAX:
            break
    return out


def climax(bars, kind="stock", window=CLIMAX_WINDOW):
    """The most recent buying or selling climax inside the last `window` sessions, or None. Needs 260 sessions (the
    250-session volume average)."""
    n = len(bars)
    if n < 262:
        return None
    c = [float(b["c"]) for b in bars]
    trs = true_ranges(bars)
    dec_min = SC_DECLINE.get(kind, SC_DECLINE["stock"])
    adv_min = BC_ADVANCE.get(kind, BC_ADVANCE["stock"])
    for i in range(n - 1, max(260, n - window) - 1, -1):
        atr = sum(trs[i - 13:i + 1]) / 14.0
        v = vol(bars[i])
        vmax60 = max(vol(b) for b in bars[i - 60:i])
        vavg250 = sum(vol(b) for b in bars[i - 250:i]) / 250.0
        rng = hi(bars[i]) - lo(bars[i])
        if not (atr > 0 and vavg250 > 0 and v >= vmax60 and v >= 2.0 * vavg250 and rng >= 1.5 * atr):
            continue
        s50 = sma(c, 50, i + 1)
        year = bars[i - 251:i + 1]
        top, bot = max(hi(b) for b in year), min(lo(b) for b in year)
        pos = (c[i] - bot) / (top - bot) if top > bot else None
        close_pos = (c[i] - lo(bars[i])) / rng if rng > 0 else None
        prior = bars[i - 120:i]
        nxt = c[i + 1:i + 4]
        base = {"date": bars[i]["date"], "ago": n - 1 - i, "vol_x": f(v / vavg250), "range_atr": f(rng / atr), "close_pos": f(100.0 * close_pos, 0) if close_pos is not None else None,
                "px": f(c[i]), "since_pct": f(pct_change(c[i], c[-1]))}
        # selling climax
        hi120 = max(hi(b) for b in prior)
        hi_i = max(range(i - 120, i), key=lambda k: hi(bars[k]))
        decline = (1 - lo(bars[i]) / hi120) * 100.0 if hi120 > 0 else 0.0
        if (lo(bars[i]) <= min(lo(b) for b in prior) and s50 is not None and c[i] < s50 and pos is not None and pos <= 0.35
                and (hi120 - lo(bars[i])) >= 6.0 * atr and decline >= dec_min and (i - hi_i >= 15 or decline >= 25.0)):
            absorbed = (close_pos is not None and close_pos >= 0.4) or any(x > c[i] for x in nxt)
            if absorbed:
                return {**base, "type": "SELLING", "move_pct": f(-decline), "extreme": f(lo(bars[i])), "held": bool(all(lo(b) >= lo(bars[i]) for b in bars[i + 1:])),
                        "absorption": "close off the low" if (close_pos is not None and close_pos >= 0.4) else "higher close inside three sessions"}
        # buying climax
        lo120 = min(lo(b) for b in prior)
        lo_i = min(range(i - 120, i), key=lambda k: lo(bars[k]))
        advance = (hi(bars[i]) / lo120 - 1) * 100.0 if lo120 > 0 else 0.0
        if (hi(bars[i]) >= max(hi(b) for b in prior) and s50 is not None and c[i] > s50 and pos is not None and pos >= 0.65
                and (hi(bars[i]) - lo120) >= 6.0 * atr and advance >= adv_min and i - lo_i >= 15):
            supplied = (close_pos is not None and close_pos <= 0.6) or any(x < c[i] for x in nxt)
            if supplied:
                return {**base, "type": "BUYING", "move_pct": f(advance), "extreme": f(hi(bars[i])), "held": bool(all(hi(b) <= hi(bars[i]) for b in bars[i + 1:])),
                        "absorption": "close off the high" if (close_pos is not None and close_pos <= 0.6) else "lower close inside three sessions"}
    return None
