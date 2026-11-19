"""The chart's own Wyckoff scans, in Python - one port, kept equal to the browser by test.

chart.html marks a symbol with CLIMAX / BOTTOM (the Bot icon), ACCUM (the Acc icon) and DIST / D-TOP (the Dist icon).
Those marks come from three browser files:

    jh-chart-vol-events.js       sellingClimaxScan, structureScan
    jh-chart-campaign-marks.js   fromScan   (which climax, bottom and end-of-accumulation become marks)
    jh-chart-distribution.js     distributionScan, jhDistributionMarks

This module is a line-for-line port of those functions so the engine can run them over every stock in the warehouse and
list the names the icons would mark.  It adds no rule of its own.  tests/campaign-scan-parity.test.js runs the browser
files and this port over the same real bars and requires identical marks, so a change on the chart that is not made
here fails the suite.

Bars are parallel lists (t seconds, o, h, l, c, v), oldest first, every price finite and above zero.
"""
from __future__ import annotations

BIG = 1e99


def _median_pos(vals):
    """jh-chart-vol-events.js median(): finite values above zero, the element at floor(n / 2)."""
    b = sorted(x for x in vals if x is not None and x == x and x > 0 and x != float("inf"))
    return b[len(b) // 2] if b else 0


def selling_climax_scan(T, O, H, L, C, V):
    """sellingClimaxScan(d): one selling climax per cluster of qualifying bars."""
    n = len(C)
    if n < 90:
        return []
    R = [H[i] - L[i] for i in range(n)]
    raw = []
    for i in range(80, n):
        cl, pc = C[i], C[i - 1]
        if not cl or not pc:
            continue
        lo, hi = L[i], H[i]
        ret = cl / pc - 1
        undercut = lo < min(O[i], pc) * 0.995
        if not (ret <= -0.012 or undercut):                       # sold
            continue
        lo20 = min(L[max(0, i - 20):i + 1])
        lo60 = min(L[max(0, i - 60):i + 1])
        lo126 = min(L[max(0, i - 126):i + 1])
        at20 = (lo - lo20) / abs(lo20) if lo20 else 9
        at60 = (lo - lo60) / abs(lo60) if lo60 else 9
        at126 = (lo - lo126) / abs(lo126) if lo126 else 9
        at_lo = at20 <= 0.006 or at60 <= 0.010 or at126 <= 0.008
        if not at_lo:
            continue
        y60 = max(H[max(0, i - 60):i])
        y20 = max(H[max(0, i - 20):i])
        y126 = max(H[max(0, i - 126):i])
        drop60 = lo / y60 - 1 if y60 else 0
        drop20 = lo / y20 - 1 if y20 else 0
        drop126 = lo / y126 - 1 if y126 else 0
        if not (drop60 <= -0.10 or drop20 <= -0.085 or drop126 <= -0.14):   # markdown
            continue
        rng = (hi - lo) or 1e-12
        loc = (cl - lo) / rng
        bn8 = 0
        for k in range(1, 9):
            if i + k >= n:
                break
            b = C[i + k] / lo - 1
            if b > bn8:
                bn8 = b
        live = i >= n - 8
        off_low = loc >= 0.38
        ar = bn8 >= 0.035 if loc < 0.35 else bn8 >= 0.025
        demand = off_low or ar or (live and off_low)
        if loc >= 0.85 and bn8 < 0.03 and not live:
            demand = False
        if not demand:
            continue
        vol = V[i] or 0
        m_v20 = _median_pos(V[max(0, i - 20):i])
        m_vpre = _median_pos(V[max(0, i - 80):max(0, i - 20)])
        m_r20 = _median_pos(R[max(0, i - 20):i])
        m_rpre = _median_pos(R[max(0, i - 80):max(0, i - 20)])
        rvol20 = min(12, vol / m_v20) if m_v20 else 0
        rvol_pre = min(12, vol / m_vpre) if m_vpre else 0
        wide20 = rng / m_r20 if m_r20 else 1
        wide_pre = rng / m_rpre if m_rpre else 1
        effort = ((rvol_pre >= 1.45 and (wide_pre >= 1.80 or wide20 >= 1.55)) or (wide_pre >= 2.40 and rvol20 >= 1.20)
                  or (rvol20 >= 1.70 and wide20 >= 1.70))
        terminal = ((drop60 <= -0.18 or drop126 <= -0.20) and at_lo and bn8 >= 0.045
                    and (rvol_pre >= 1.20 or loc >= 0.38 or wide_pre >= 1.50))
        if effort or terminal:
            raw.append({"time": T[i], "kind": "sc", "i": i, "vol": vol, "rvol": rvol_pre, "score": rvol_pre * max(wide_pre, wide20),
                        "ret": ret, "loc": loc, "wide": wide_pre, "px": lo, "drop60": drop60, "drop126": drop126})
    groups, g = [], []
    for hit in raw:
        if not g or hit["i"] - g[-1]["i"] <= 8:
            g.append(hit)
        else:
            groups.append(g)
            g = [hit]
    if g:
        groups.append(g)
    out = []
    for g in groups:
        low = min(x["px"] for x in g)
        cand = [x for x in g if x["px"] <= low * 1.006]
        sold = [x for x in cand if x["loc"] <= 0.78 or x["ret"] <= -0.01]
        pool = sorted(sold or cand, key=lambda x: -x["score"])
        if pool:
            out.append(pool[0])
    return out


def _swing_low(L, i, left, right, strict):
    n = len(L)
    if i < left or i + right >= n:
        return False
    lo = L[i]
    for j in range(i - left, i + right + 1):
        if j != i and (L[j] <= lo if strict else L[j] < lo):
            return False
    return True


def _swing_high(H, i, left, right, strict):
    n = len(H)
    if i < left or i + right >= n:
        return False
    hi = H[i]
    for j in range(i - left, i + right + 1):
        if j != i and (H[j] >= hi if strict else H[j] > hi):
            return False
    return True


def _cluster(arr, lower, w):
    """clusterExt(): one extreme per cluster - a later one inside w bars replaces the kept one only when it is more extreme."""
    keep = []
    for e in arr:
        last = keep[-1] if keep else None
        if last is not None and e["i"] - last["i"] <= w:
            if (e["px"] < last["px"]) if lower else (e["px"] > last["px"]):
                keep[-1] = e
        else:
            keep.append(e)
    return keep


def structure_scan(T, O, H, L, C, V):
    """structureScan(d): confirmed cycle lows and highs, the reversals that confirm them, and the bars that end the
    accumulation (EOA) or the distribution (EOD) that followed.  A turn is only read 40 sessions after it."""
    n = len(C)
    if n < 300:
        return []
    bottoms, tops = [], []
    for i in range(260, n - 40):
        lo, hi = L[i], H[i]
        is_low = _swing_low(L, i, 15, 10, False)
        is_high = _swing_high(H, i, 15, 10, False)
        if not is_low and not is_high:
            continue
        a252, a126 = max(0, i - 252), max(0, i - 126)
        y_hi = max(H[a252:i])
        y_lo252 = min(L[a252:i])
        y_lo126 = min(L[a126:i])
        if is_low:
            drop = lo / y_hi - 1 if y_hi else 0
            at_lo = (y_lo126 and lo <= y_lo126 * 1.008) or (y_lo252 and lo <= y_lo252 * 1.008)
            if at_lo and drop <= -0.12:
                m = 0
                for k in range(1, 13):
                    if i + k >= n:
                        break
                    m = max(m, C[i + k] / lo - 1)
                if m >= 0.04:
                    bottoms.append({"i": i, "time": T[i], "px": lo, "vol": V[i] or 0, "drop": drop})
        if is_high:
            rally = hi / y_lo252 - 1 if y_lo252 else 0
            at_hi = y_hi and hi >= y_hi * 0.995
            if at_hi and rally >= 0.12:
                fade = 0
                for k in range(1, 21):
                    if i + k >= n:
                        break
                    fade = min(fade, C[i + k] / hi - 1)
                rec = max(H[i + 1:i + 41]) if i + 1 < n else -BIG
                if fade <= -0.06 and rec < hi * 1.005:
                    tops.append({"i": i, "time": T[i], "px": hi, "vol": V[i] or 0, "rally": rally})
    bottoms = _cluster(bottoms, True, 100)
    tops = _cluster(tops, False, 120)
    out = []
    for b in bottoms:
        bi, px = b["i"], b["px"]
        out.append({"time": b["time"], "kind": "bottom", "i": bi, "vol": b["vol"], "score": -b["drop"] * 10, "ret": b["drop"], "px": px})
        conf = None
        for k in range(1, 16):
            if bi + k >= n:
                break
            if L[bi + k] < px * 0.997:
                break
            if C[bi + k] >= px * 1.04:
                conf = bi + k
                break
        if conf is not None:
            out.append({"time": T[conf], "kind": "revup", "i": conf, "vol": V[conf] or 0, "score": 8, "ret": C[conf] / px - 1})
        creek, eoa = H[bi], None
        for k in range(1, 51):
            j = bi + k
            if j >= n:
                break
            if H[j] > creek:
                creek = H[j]
            if k < 8:
                continue
            if max(H[bi:j]) >= H[j]:
                continue
            if C[j] > O[j] and C[j] >= px * 1.06 and C[j] >= creek * 0.998:
                eoa = j
                break
        if eoa is not None:
            out.append({"time": T[eoa], "kind": "eoa", "i": eoa, "vol": V[eoa] or 0, "score": 8, "ret": C[eoa] / px - 1, "from": bi})
    for t in tops:
        ti, px = t["i"], t["px"]
        out.append({"time": t["time"], "kind": "top", "i": ti, "vol": t["vol"], "score": t["rally"] * 10, "ret": t["rally"], "px": px})
        conf = None
        for k in range(1, 21):
            if ti + k >= n:
                break
            if H[ti + k] > px * 1.003:
                break
            if C[ti + k] <= px * 0.96:
                conf = ti + k
                break
        if conf is not None:
            out.append({"time": T[conf], "kind": "revdn", "i": conf, "vol": V[conf] or 0, "score": 8, "ret": C[conf] / px - 1})
        eod = None
        for k in range(1, 51):
            j = ti + k
            if j >= n:
                break
            if k < 8:
                continue
            if min(L[ti:j]) <= L[j]:
                continue
            if C[j] < O[j] and C[j] <= px * 0.94:
                eod = j
                break
        if eod is not None:
            out.append({"time": T[eod], "kind": "eod", "i": eod, "vol": V[eod] or 0, "score": 8, "ret": C[eod] / px - 1, "from": ti})
    out.sort(key=lambda e: e["i"])
    return out


def campaign_marks(T, structure, climaxes):
    """jh-chart-campaign-marks.js fromScan(): [(time, text, index, source event)] with text CLIMAX / BOTTOM / ACCUM, in
    the order the browser builds them."""
    n = len(T)
    sc = [e for e in climaxes if e["kind"] == "sc"]
    bot = [e for e in structure if e["kind"] == "bottom"]
    eoa = [e for e in structure if e["kind"] == "eoa"]
    out, used = [], set()
    for b in bot:
        best = None
        for s in sc:
            if s["i"] >= b["i"] or s["time"] == b["time"]:
                continue
            if b["i"] - s["i"] > 80:
                continue
            if best is None or (s["score"] or 0) > (best["score"] or 0):
                best = s
        if best is not None and best["time"] != b["time"]:
            used.add(best["i"])
            out.append((best["time"], "CLIMAX", best["i"], best))
        out.append((b["time"], "BOTTOM", b["i"], b))
    for e in eoa:
        out.append((e["time"], "ACCUM", e["i"], e))
    if n:
        last, live = n - 1, None
        for s in sc:
            if s["i"] in used or last - s["i"] > 40:
                continue
            if (s["score"] or 0) < 4:
                continue
            if any(b["i"] > s["i"] for b in bot):
                continue
            if live is None or (s["score"] or 0) > (live["score"] or 0):
                live = s
        if live is not None:
            out.append((live["time"], "CLIMAX", live["i"], live))
    return out


def distribution_scan(T, O, H, L, C, V):
    """jh-chart-distribution.js distributionScan(d): a rally near the highs whose next push buys less, gives back half
    of the wave, is not reclaimed and reacts worse than the reaction before it."""
    n = len(C)
    if n < 280:
        return []
    highs, lows = [], []
    for i in range(40, n - 12):
        if _swing_high(H, i, 8, 5, True):
            highs.append(i)
        if _swing_low(L, i, 8, 5, True):
            lows.append(i)
    out, last_emit, last_high_px = [], -999, 0
    for idx, h in enumerate(highs):
        if h + 20 >= n:
            continue
        hh = H[h]
        if h - last_emit < 40 and hh <= last_high_px:
            continue
        y_hi = max(H[max(0, h - 189):h + 1])
        y_lo = min(L[max(0, h - 252):h]) if h > 0 else BIG
        y_lo2 = min(L[max(0, h - 504):h]) if h > 0 else BIG
        if not (y_hi > 0) or hh < y_hi * 0.97:
            continue
        adv1 = y_lo > 0 and hh / y_lo - 1 >= 0.15
        adv2 = y_lo2 > 0 and hh / y_lo2 - 1 >= 0.30
        if not adv1 and not adv2:
            continue
        prev = -1
        for k in range(idx - 1, -1, -1):
            if 8 <= h - highs[k] <= 80:
                prev = highs[k]
                break
        if prev < 0:
            continue
        low_b = low_a = -1
        for k in range(len(lows) - 1, -1, -1):
            lw = lows[k]
            if lw < h and lw > prev and low_b < 0:
                low_b = lw
            if lw < prev and h - lw <= 120 and low_a < 0:
                low_a = lw
        if low_b < 0:
            low_b = prev
            for k in range(prev, h):
                if L[k] < L[low_b]:
                    low_b = k
        if low_a < 0:
            low_a = max(0, prev - 40)
            for k in range(low_a, prev):
                if L[k] < L[low_a]:
                    low_a = k
        res_b = hh / L[low_b] - 1 if L[low_b] else 0
        res_a = H[prev] / L[low_a] - 1 if L[low_a] else 0
        if res_b <= 0.005 or res_a <= 0.005:
            continue
        seg_b = [(v or 0) for v in V[low_b:h + 1]]
        seg_a = [(v or 0) for v in V[low_a:prev + 1]]
        eff_b = sum(seg_b) / len(seg_b) if seg_b else 0
        eff_a = sum(seg_a) / len(seg_a) if seg_a else 0
        worse_effort = eff_a > 0 and eff_b > eff_a * 1.1 and res_b < res_a * 0.85
        small_net = (hh / H[prev] - 1) < 0.025 and res_b < res_a
        if not worse_effort and not small_net:
            continue
        wave = hh - L[low_b]
        if wave <= 0:
            continue
        gave = -1
        for t in range(h + 1, min(n - 1, h + 18) + 1):
            off_high = (hh - C[t]) / hh if hh else 0
            if hh - C[t] >= wave * 0.5 and off_high >= 0.035:
                gave = t
                break
        if gave < 0:
            continue
        confirm = min(n - 1, gave + 6)
        if any(C[t] >= hh * 0.998 for t in range(gave, confirm + 1)):
            continue
        prior_drop = L[low_b] / H[prev] - 1 if H[prev] else 0
        new_low = gave
        for t in range(h + 1, confirm + 1):
            if L[t] < L[new_low]:
                new_low = t
        new_drop = L[new_low] / hh - 1 if hh else 0
        worse_reaction = new_drop < prior_drop - 0.005 or new_drop <= -0.045
        if not worse_reaction and new_drop > -0.03:
            continue
        out.append({"i": confirm, "time": T[confirm], "highI": h, "highTime": T[h], "high": hh, "prevI": prev, "lowB": low_b, "gave": gave,
                    "push_pct": res_b * 100, "prev_push_pct": res_a * 100, "reaction_pct": new_drop * 100, "prior_reaction_pct": prior_drop * 100,
                    "effort_x": (eff_b / eff_a) if eff_a else None, "why": "more volume bought less" if worse_effort else "the push barely cleared the last high"})
        last_emit = h
        last_high_px = hh
    return out


def distribution_marks(T, structure, dist):
    """jhDistributionMarks(): [(time, text, index, source event)] with text DIST, and D-TOP for the structure top within
    40 bars of the rally high."""
    tops = [e for e in structure if e["kind"] == "top"]
    out = []
    for e in dist:
        out.append((e["time"], "DIST", e["i"], e))
        top, best = None, 1e9
        for t in tops:
            di = abs((t["i"] or 0) - e["highI"])
            if di <= 40 and di < best:
                best, top = di, t
        if top is not None:
            out.append((top["time"], "D-TOP", top["i"], top))
    return out


def scan(T, O, H, L, C, V):
    """Everything the three icons would mark on one symbol: (structure, climaxes, distributions, campaign marks,
    distribution marks)."""
    st = structure_scan(T, O, H, L, C, V)
    sc = selling_climax_scan(T, O, H, L, C, V)
    ds = distribution_scan(T, O, H, L, C, V)
    return st, sc, ds, campaign_marks(T, st, sc), distribution_marks(T, st, ds)
