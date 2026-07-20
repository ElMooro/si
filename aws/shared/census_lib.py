"""census_lib — shared quant kernel for the census family
(ops 3556). Extracted verbatim from the proven fundamental-census
v1.8.0: weekly pattern detection (neckline+extremeness double
top/bottom), tech_series, beta_vs, momentum, cross-sectional
percentiles. Imported by justhodl-etf-census and justhodl-fi-census;
the flagship keeps its inline copy (never touch working engines).
"""

def _peaks(px, trough=False):
    out = []
    for i in range(2, len(px) - 2):
        seg = px[i-2:i+3]
        if (min(seg) == px[i]) if trough else (max(seg) == px[i]):
            out.append((i, px[i]))
    return out

def detect_double(price, kind="top", look=78, tol=0.03, depth=0.05,
                  recent=12, extreme=0.12):
    """Weekly double-top/bottom. Two comparable extrema (within tol),
    separated by a counter-move of >= depth, second extremum within
    `recent` weeks, and price now confirming (below peaks / above
    troughs). Returns 0/1; None if series too short."""
    px = [v for _, v in (price or []) if isinstance(v, (int, float))
          and v > 0]
    if len(px) < 30:
        return None
    px = px[-look:]
    ex = _peaks(px, trough=(kind == "bottom"))
    if len(ex) < 2:
        return 0
    rng_hi, rng_lo = max(px), min(px)
    band = (rng_hi - rng_lo) or 1e-9
    for a in range(len(ex) - 1):
        for b in range(a + 1, len(ex)):
            (i1, v1), (i2, v2) = ex[a], ex[b]
            if i2 < len(px) - recent or i2 - i1 < 6:
                continue
            hi, lo = max(v1, v2), min(v1, v2)
            if (hi - lo) / hi > tol:
                continue
            # extremeness: both peaks near the 78w extreme
            if kind == "top":
                if min(v1, v2) < rng_hi - extreme * band:
                    continue
            else:
                if max(v1, v2) > rng_lo + extreme * band:
                    continue
            between = px[i1:i2 + 1]
            if kind == "top":
                neck = min(between)
                if neck <= lo * (1 - depth) and px[-1] < neck:
                    return 1
            else:
                neck = max(between)
                if neck >= hi * (1 + depth) and px[-1] > neck:
                    return 1
    return 0

def tech_series(price):
    """Weekly technical columns from the doc's price series."""
    rows = [(d, v) for d, v in (price or [])
            if isinstance(v, (int, float)) and v > 0]
    px = [v for _, v in rows]
    out = {}
    if len(px) < 30:
        return out
    hi52 = max(px[-52:]); lo52 = min(px[-52:])
    out["dist_52w_high_pct"] = round((px[-1] / hi52 - 1) * 100, 2)
    out["dist_52w_low_pct"] = round((px[-1] / lo52 - 1) * 100, 2)
    ma10 = [sum(px[i-9:i+1]) / 10 for i in range(9, len(px))]
    ma40 = ([sum(px[i-39:i+1]) / 40 for i in range(39, len(px))]
            if len(px) >= 40 else [])
    if ma40:
        off = len(ma10) - len(ma40)
        rel = [ma10[off + i] - ma40[i] for i in range(len(ma40))]
        out["above_ma40w"] = 1 if px[-1] > (ma40[-1]) else 0
        gc = 0
        for i in range(max(1, len(rel) - 8), len(rel)):
            if rel[i - 1] <= 0 < rel[i]:
                gc = 1
        out["golden_cross_10_40w"] = gc
    out["breakout_20w"] = 1 if (len(px) >= 24 and
                                max(px[-4:]) >= max(px[-24:-4])) else 0
    if len(px) >= 15:
        g, l = [], []
        for i in range(len(px) - 14, len(px)):
            ch = px[i] - px[i - 1]
            (g if ch > 0 else l).append(abs(ch))
        ag = sum(g) / 14; al = sum(l) / 14
        out["rsi_14w"] = round(100 - 100 / (1 + ag / al), 1) if al > 0             else 100.0
    if len(px) >= 53:
        rets = [px[i] / px[i - 1] - 1 for i in range(len(px) - 52,
                                                     len(px))]
        mu = sum(rets) / len(rets)
        var = sum((r - mu) ** 2 for r in rets) / (len(rets) - 1)
        out["vol_52w_pct"] = round((var ** 0.5) * (52 ** 0.5) * 100, 1)
    dt = detect_double(price, "top")
    db = detect_double(price, "bottom")
    if dt is not None:
        out["double_top"] = dt
    if db is not None:
        out["double_bottom"] = db
    return out

def beta_vs(price, spx_map, weeks=104):
    """Weekly beta vs SPX using date-matched closes (nearest <=)."""
    rows = [(str(d)[:10], v) for d, v in (price or [])
            if isinstance(v, (int, float)) and v > 0]
    if len(rows) < weeks // 2 or not spx_map:
        return None
    sdates = sorted(spx_map.keys())
    import bisect
    pairs = []
    for d, v in rows[-weeks - 1:]:
        j = bisect.bisect_right(sdates, d) - 1
        if j >= 0:
            pairs.append((v, spx_map[sdates[j]]))
    if len(pairs) < 40:
        return None
    ra = [pairs[i][0] / pairs[i-1][0] - 1 for i in range(1, len(pairs))]
    rb = [pairs[i][1] / pairs[i-1][1] - 1 for i in range(1, len(pairs))]
    mb = sum(rb) / len(rb); ma = sum(ra) / len(ra)
    cov = sum((ra[i] - ma) * (rb[i] - mb)
              for i in range(len(ra))) / (len(ra) - 1)
    var = sum((x - mb) ** 2 for x in rb) / (len(rb) - 1)
    return round(cov / var, 2) if var > 0 else None

def momentum(price, weeks):
    """Price momentum over `weeks` (12-1 style handled by caller):
    pct change last close vs close `weeks` back; needs weekly rows."""
    px = [v for _, v in (price or []) if isinstance(v, (int, float))
          and v > 0]
    if len(px) < weeks + 1:
        return None
    return round((px[-1] / px[-1 - weeks] - 1.0) * 100.0, 2)

def mom_12_1(price):
    px = [v for _, v in (price or []) if isinstance(v, (int, float))
          and v > 0]
    if len(px) < 53:
        return None
    return round((px[-5] / px[-53] - 1.0) * 100.0, 2)

def cross_pct(col, low=False):
    """Percentile per index with avg-rank ties; None-safe; low flips."""
    idx = [(v, i) for i, v in enumerate(col)
           if isinstance(v, (int, float))]
    idx.sort(key=lambda x: x[0])
    out = [None] * len(col)
    n = len(idx)
    i = 0
    while i < n:
        j = i
        while j + 1 < n and idx[j + 1][0] == idx[i][0]:
            j += 1
        pv = 100.0 * ((i + j) / 2 + 0.5) / n
        for q2 in range(i, j + 1):
            out[idx[q2][1]] = round(100.0 - pv if low else pv, 2)
        i = j + 1
    return out
