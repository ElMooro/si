"""cycle_composite -- multi-pillar business-cycle composite for
justhodl-global-business-cycle v3 (ops 5100).

Construction (per country, monthly grid):
  1. every feature from data/cycle/features.json.gz is standardised against its
     OWN prior history: z_t = (x_t - mean(x_{t-120..t-1})) / sd(...), >= 36 prior
     obs, clipped to +-3, multiplied by the feature's cyclical sign
  2. quarterly features are forward-filled for at most their max lag; a feature
     whose latest print is older than its max lag is excluded from the nowcast
     (still used in history where it was current)
  3. pillar z = mean of its feature z's; composite z = weighted mean over the
     pillars present (survey 0.35, financial 0.25, activity 0.20, trade 0.10,
     equity 0.10), weights renormalised over what is present
  4. CLI = 100 + 20*tanh(composite_z / 2): 1 sd above trend ~ 109, bounded
     (80, 120), same 100 boundary the v2 consumers key on
  5. phase from level (>= 100) and the 3-month change of the composite z
     (rising > +0.05, falling < -0.05, else flat -> v2's flat rule)
  6. confidence = coverage (pillars present / 5) x agreement (share of feature
     z's on the composite's side)
Global: GDP-weighted composite z, breadth, pillar breadth, and P(downturn in
6 months) from a logistic fit of [composite_z, 3m change] on the history
against "GDP-weighted IP y/y six months ahead < 0" -- reported WITH its
in-sample fit statistics; it is a calibration on this platform's own data,
not a forecast track record.
"""
import math
from collections import defaultdict

PILLAR_WEIGHTS = {"survey": 0.35, "financial": 0.25, "activity": 0.20, "trade": 0.10, "equity": 0.10}
Z_WINDOW = 120
Z_MIN_OBS = 36
Z_CLIP = 3.0
TREND_EPS = 0.05


def rolling_z(vals, window=Z_WINDOW, min_obs=Z_MIN_OBS, clip=Z_CLIP):
    """z of each point vs the prior `window` valid observations (excluding
    itself). vals: list with None for missing. Returns list aligned to vals."""
    out = [None] * len(vals)
    hist = []          # (index, value) of prior valid obs
    for i, v in enumerate(vals):
        if v is not None:
            # trailing window by observation count, not calendar (sparse quarterly ok)
            win = hist[-window:]
            if len(win) >= min_obs:
                m = sum(x for _, x in win) / len(win)
                var = sum((x - m) ** 2 for _, x in win) / len(win)
                sd = var ** 0.5
                if sd > 1e-9:
                    z = (v - m) / sd
                    out[i] = max(-clip, min(clip, z))
            hist.append((i, v))
    return out


def ffill(vals, max_gap):
    """Forward-fill None gaps up to max_gap positions (quarterly -> monthly)."""
    out = list(vals)
    last, last_i = None, None
    for i, v in enumerate(vals):
        if v is not None:
            last, last_i = v, i
        elif last is not None and i - last_i <= max_gap:
            out[i] = last
    return out


def cli_from_z(z):
    return 100.0 + 20.0 * math.tanh(float(z) / 2.0)


def phase_from(cli, d3):
    if d3 is None or abs(d3) < TREND_EPS:
        trend = "flat"
    elif d3 > 0:
        trend = "rising"
    else:
        trend = "falling"
    if cli >= 100 and trend == "rising":
        ph = "EXPANSION"
    elif cli >= 100 and trend == "falling":
        ph = "AT_RISK"
    elif cli < 100 and trend == "falling":
        ph = "RECESSION"
    elif cli < 100 and trend == "rising":
        ph = "RECOVERY"
    else:
        ph = "EXPANSION" if cli >= 100 else "RECOVERY"
    return ph, trend


def build_country(country_feats, grid, equity_monthly=None, end_idx=None):
    """country_feats: features dict from the feature store ({name: {values, sign, pillar, freq, max_lag_months, latest_period, ...}})
    equity_monthly: list aligned to grid with the equity composite pct (or None)
    Returns dict(history=[...], nowcast={...})."""
    n = len(grid)
    end_idx = n - 1 if end_idx is None else end_idx
    feat_z = {}          # name -> list of signed z aligned to grid
    meta = {}
    for name, f in (country_feats or {}).items():
        if f.get("pillar") in (None, "side"):
            continue
        vals = f.get("values") or []
        if len(vals) != n:
            continue
        # a print stands until the next one arrives, for at most the feature's max lag
        # (monthly 4, quarterly 7): history months then carry exactly what a nowcast
        # would have seen, and the last history row equals the nowcast
        gap = int(f.get("max_lag_months") or 4)
        z = rolling_z(vals)
        z = ffill(z, gap)
        sign = f.get("sign", 1) or 1
        feat_z[name] = [None if v is None else sign * v for v in z]
        meta[name] = f
    if equity_monthly is not None and len(equity_monthly) == n:
        z = rolling_z(equity_monthly, min_obs=24)
        feat_z["equity_momentum"] = ffill(z, 1)      # a month-end print stands until the next bar lands (max lag 1)
        meta["equity_momentum"] = {"pillar": "equity", "sign": 1, "freq": "M", "max_lag_months": 1,
                                   "label": "equity composite (12m/3m/1m return + 200d distance), own-history z",
                                   "latest_period": grid[end_idx], "months_stale": 0, "values": equity_monthly}

    def composite_at(i):
        pill = defaultdict(list)
        for name, zs in feat_z.items():
            if zs[i] is not None:
                pill[meta[name]["pillar"]].append((name, zs[i]))
        if not pill:
            return None
        pz = {p: sum(v for _, v in xs) / len(xs) for p, xs in pill.items()}
        wsum = sum(PILLAR_WEIGHTS.get(p, 0.1) for p in pz)
        cz = sum(PILLAR_WEIGHTS.get(p, 0.1) * z for p, z in pz.items()) / wsum if wsum > 0 else None
        return {"z": cz, "pillars": pz, "members": dict(pill), "n_features": sum(len(x) for x in pill.values())}

    history = []
    comp_series = [None] * n
    for i in range(n):
        c = composite_at(i)
        if c is None or c["z"] is None:
            continue
        comp_series[i] = c["z"]
        d3 = (c["z"] - comp_series[i - 3]) if i >= 3 and comp_series[i - 3] is not None else None
        cli = cli_from_z(c["z"])
        ph, tr = phase_from(cli, d3)
        history.append({"period": grid[i], "composite_z": round(c["z"], 3), "cli": round(cli, 2), "phase": ph, "trend": tr,
                        "d3": round(d3, 3) if d3 is not None else None,
                        "pillars": {p: round(z, 3) for p, z in c["pillars"].items()}, "n_features": c["n_features"]})

    # nowcast: use each feature's latest VALID z within its max lag, evaluated at end_idx
    now_members = defaultdict(list)
    components = []
    for name, zs in feat_z.items():
        f = meta[name]
        max_lag = int(f.get("max_lag_months") or 4)
        vals = f.get("values") or []
        jr = None                      # index of the last actual print at or before end_idx
        for k in range(min(end_idx, len(vals) - 1), -1, -1):
            if vals[k] is not None:
                jr = k
                break
        stale = None if jr is None else end_idx - jr
        used = zs[end_idx] is not None and stale is not None and stale <= max_lag
        latest_val = vals[jr] if jr is not None else f.get("latest_value")
        comp = {"name": name, "pillar": f.get("pillar"), "label": f.get("label"), "sign": f.get("sign", 1), "freq": f.get("freq"),
                "latest_period": f.get("latest_period"), "months_stale": stale, "source": f.get("source"),
                "value": (round(latest_val, 3) if isinstance(latest_val, (int, float)) else None),
                "level_latest": f.get("level_latest"), "z": (round(zs[end_idx], 3) if used else None),
                "used": used, "reason": None if used else (f"last print {stale} months ago > max lag {max_lag}" if stale is not None
                                                            else "no print" if jr is None else "no standardised value (history too short)")}
        components.append(comp)
        if used:
            now_members[f["pillar"]].append((name, zs[end_idx]))
    if not now_members:
        return {"history": history, "nowcast": None, "components": components}
    pz = {p: sum(v for _, v in xs) / len(xs) for p, xs in now_members.items()}
    wsum = sum(PILLAR_WEIGHTS.get(p, 0.1) for p in pz)
    cz = sum(PILLAR_WEIGHTS.get(p, 0.1) * z for p, z in pz.items()) / wsum
    cli = cli_from_z(cz)
    prev = [h for h in history if h["period"] <= grid[max(0, end_idx - 3)]]
    d3 = (cz - prev[-1]["composite_z"]) if prev else None
    ph, tr = phase_from(cli, d3)
    all_z = [v for xs in now_members.values() for _, v in xs]
    agree = sum(1 for v in all_z if (v >= 0) == (cz >= 0)) / len(all_z)
    rising = sum(1 for name, _ in [(nm, v) for xs in now_members.values() for nm, v in xs]
                 if _feature_rising(feat_z[name], end_idx))
    coverage = len([p for p in pz if p != "equity"]) / 4.0
    for comp in components:
        if comp["used"]:
            w = PILLAR_WEIGHTS.get(comp["pillar"], 0.1) / wsum / max(1, len(now_members[comp["pillar"]]))
            comp["weight"] = round(w, 4)
            comp["contribution"] = round(w * comp["z"], 3)
    nowcast = {"composite_z": round(cz, 3), "cli": round(cli, 2), "phase": ph, "trend": tr, "d3": round(d3, 3) if d3 is not None else None,
               "pillars": {p: {"z": round(z, 3), "n": len(now_members[p]), "weight": round(PILLAR_WEIGHTS.get(p, 0.1) / wsum, 3),
                               "features": [nm for nm, _ in now_members[p]]} for p, z in pz.items()},
               "n_features": len(all_z), "coverage": round(coverage, 2), "agreement": round(agree, 2),
               "confidence": round(min(1.0, coverage) * agree, 2), "diffusion_rising": round(rising / len(all_z), 2),
               "basis": "multi-pillar" if len([p for p in pz if p != "equity"]) >= 2 else "thin",
               "as_of": grid[end_idx]}
    return {"history": history, "nowcast": nowcast, "components": components}


def _feature_rising(zs, i):
    prev = None
    for k in range(i - 1, max(-1, i - 7), -1):
        if zs[k] is not None:
            prev = zs[k]
            break
    return zs[i] is not None and prev is not None and zs[i] > prev


# ── global aggregation + calibration ────────────────────────────────────────
def aggregate_history(country_hist, weights, grid):
    """GDP-weighted global composite per month. country_hist: {iso: [history rows]}."""
    by_period = defaultdict(list)
    for iso, rows in country_hist.items():
        w = weights.get(iso, 0)
        for h in rows:
            by_period[h["period"]].append((iso, w, h["composite_z"], h["phase"]))
    out = []
    for p in grid:
        rows = by_period.get(p)
        if not rows:
            continue
        wsum = sum(w for _, w, _, _ in rows)
        if wsum <= 0:
            continue
        gz = sum(w * z for _, w, z, _ in rows) / wsum
        exp = sum(w for _, w, _, ph in rows if ph in ("EXPANSION", "RECOVERY")) / wsum * 100
        out.append({"period": p, "global_z": round(gz, 3), "cli": round(cli_from_z(gz), 2), "n": len(rows),
                    "weight_covered": round(wsum, 1), "expansion_breadth_pct": round(exp, 1),
                    "breadth_unweighted_pct": round(100 * sum(1 for _, _, z, _ in rows if z > 0) / len(rows), 1)})
    return out


def fit_logistic(X, y, iters=60, l2=1e-3):
    """Newton-Raphson logistic regression with a tiny ridge; X rows include the intercept."""
    k = len(X[0])
    w = [0.0] * k
    for _ in range(iters):
        g = [0.0] * k
        H = [[l2 if i == j else 0.0 for j in range(k)] for i in range(k)]
        for xi, yi in zip(X, y):
            s = sum(wj * xj for wj, xj in zip(w, xi))
            p = 1 / (1 + math.exp(-max(-30, min(30, s))))
            for i in range(k):
                g[i] += (p - yi) * xi[i] + (l2 * w[i] if i else 0)
                for j in range(k):
                    H[i][j] += p * (1 - p) * xi[i] * xi[j]
        step = _solve(H, g)
        if step is None:
            break
        w = [wi - si for wi, si in zip(w, step)]
        if max(abs(s) for s in step) < 1e-7:
            break
    return w


def _solve(A, b):
    n = len(b)
    M = [row[:] + [b[i]] for i, row in enumerate(A)]
    for c in range(n):
        piv = max(range(c, n), key=lambda r: abs(M[r][c]))
        if abs(M[piv][c]) < 1e-12:
            return None
        M[c], M[piv] = M[piv], M[c]
        for r in range(n):
            if r != c:
                f = M[r][c] / M[c][c]
                for j in range(c, n + 1):
                    M[r][j] -= f * M[c][j]
    return [M[i][n] / M[i][i] for i in range(n)]


def calibrate_downturn(global_hist, target_series, grid, horizon=6):
    """P(target_{t+h} < 0 | global_z_t, d3_t). target_series: {period: value} (e.g. GDP-weighted IP y/y).
    Returns dict with coefficients, in-sample stats and the current probability."""
    gz = {h["period"]: h["global_z"] for h in global_hist}
    X, y, periods = [], [], []
    for i, p in enumerate(grid):
        if p not in gz or i + horizon >= len(grid):
            continue
        p3 = grid[i - 3] if i >= 3 else None
        d3 = gz[p] - gz[p3] if p3 in gz else 0.0
        t = target_series.get(grid[i + horizon])
        if t is None:
            continue
        X.append([1.0, gz[p], d3])
        y.append(1.0 if t < 0 else 0.0)
        periods.append(p)
    if len(X) < 60 or sum(y) < 5 or sum(y) > len(y) - 5:
        return {"ok": False, "reason": f"insufficient calibration sample (n={len(X)}, positives={int(sum(y))})"}
    w = fit_logistic(X, y)

    def prob(x):
        s = sum(wi * xi for wi, xi in zip(w, x))
        return 1 / (1 + math.exp(-max(-30, min(30, s))))
    ps = [prob(x) for x in X]
    pos = [p for p, yy in zip(ps, y) if yy == 1]
    neg = [p for p, yy in zip(ps, y) if yy == 0]
    auc = sum(1 for a in pos for b in neg if a > b) / (len(pos) * len(neg)) if pos and neg else None
    hit = sum(1 for p, yy in zip(ps, y) if (p >= 0.5) == (yy == 1)) / len(y)
    last = global_hist[-1]
    d3_now = last["global_z"] - (gz.get(grid[grid.index(last["period"]) - 3]) if grid.index(last["period"]) >= 3 else last["global_z"])
    p_now = prob([1.0, last["global_z"], d3_now if d3_now is not None else 0.0])
    return {"ok": True, "horizon_months": horizon, "n_obs": len(X), "base_rate": round(sum(y) / len(y), 3),
            "coefficients": {"intercept": round(w[0], 4), "global_z": round(w[1], 4), "d3": round(w[2], 4)},
            "in_sample_auc": round(auc, 3) if auc is not None else None, "in_sample_hit_rate": round(hit, 3),
            "sample_start": periods[0], "sample_end": periods[-1], "probability_now": round(p_now, 3),
            "target": "GDP-weighted industrial production y/y (OECD KEI) six months ahead < 0",
            "caveat": "in-sample calibration on this platform's own history; a monitoring signal, not a validated forecast"}
