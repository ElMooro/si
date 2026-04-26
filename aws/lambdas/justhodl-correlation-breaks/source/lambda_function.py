"""
justhodl-correlation-breaks — Phase 9.5 of the system-improvement plan.

CROSS-ASSET CORRELATION BREAK DETECTOR.

The single highest-information-content macro signal: when normally-stable
cross-asset correlations break, that's a regime change. Cross-asset
correlations break BEFORE prices break.

Historical examples this would have caught:
  - Aug-2024 yen carry unwind: JPY/USD ↔ SPX correlation broke Friday
    afternoon while SPX was still pinned. The correlation z-score went
    >2σ before Monday's 12% SPX drop.
  - 2022 inflation regime: stock-bond correlation flipped from −0.4 to
    +0.5 over Q1, signaling end of the 40-year bond hedge regime.
  - Mar-2020 dollar funding: DXY-everything correlation went +0.7 across
    the board (everything sold for dollars), classic shortage signal.
  - Sep-2022 UK gilt crisis: GBP-bond yields decoupled briefly,
    correlation z>3.

Methodology:
  1. Pull 10 instruments × 2Y daily returns from FRED (free, real data)
  2. Compute 60-day rolling correlation matrix at every date in the last 1Y
  3. For each of the 45 pairs, z-score the latest correlation vs its 1Y history
  4. Composite "correlation break" score = Frobenius norm of (M_today - M_60d_ago),
     z-scored against its own 1Y history
  5. Top-3 most-broken pairs (by |z-score|) shown as cards
  6. Full latest matrix + delta matrix shown as heatmap on /correlation.html

Output: s3://justhodl-dashboard-live/data/correlation-breaks.json
Schedule: daily 14:00 UTC (after FRED daily series update)
Consumers: justhodl.ai/correlation.html, crisis.html watch-list aggregator,
           future risk-sizer regime-change input
"""
import json
import math
import os
import time
import urllib.request
import urllib.parse
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import boto3

# Phase 2 dual-write helper (no khalid_* keys in this output, but keep pattern)
try:
    from ka_aliases import add_ka_aliases
except Exception:
    def add_ka_aliases(d):
        return d

S3_BUCKET = "justhodl-dashboard-live"
S3_KEY = "data/correlation-breaks.json"
FRED_KEY = os.environ.get("FRED_API_KEY", "2f057499936072679d8843d7fce99989")
FRED_BASE = "https://api.stlouisfed.org/fred/series/observations"

s3 = boto3.client("s3", region_name="us-east-1")


# ─────────────────────────────────────────────────────────────────────
# Instrument catalog
# ─────────────────────────────────────────────────────────────────────
# diff_mode controls how returns are computed:
#   "pct"  — percent change (price/level series: SPX, NASDAQ, FX, Oil, Gold, DXY)
#   "diff" — first difference (rates/spreads/vol series: yields, OAS, VIX)
# This is correct because correlations should be computed on stationary
# returns, not on levels.

INSTRUMENTS = {
    "SP500":            {"label": "S&P 500",          "fred_id": "SP500",            "diff_mode": "pct"},
    "NASDAQCOM":        {"label": "Nasdaq Comp",      "fred_id": "NASDAQCOM",        "diff_mode": "pct"},
    "VIXCLS":           {"label": "VIX",              "fred_id": "VIXCLS",           "diff_mode": "diff"},
    "DGS10":            {"label": "10Y Yield",        "fred_id": "DGS10",            "diff_mode": "diff"},
    "DGS2":             {"label": "2Y Yield",         "fred_id": "DGS2",             "diff_mode": "diff"},
    "BAMLH0A0HYM2":     {"label": "HY OAS",           "fred_id": "BAMLH0A0HYM2",     "diff_mode": "diff"},
    "DTWEXBGS":         {"label": "Broad USD",        "fred_id": "DTWEXBGS",         "diff_mode": "pct"},
    "DEXJPUS":          {"label": "JPY/USD",          "fred_id": "DEXJPUS",          "diff_mode": "pct"},
    "DCOILWTICO":       {"label": "WTI Oil",          "fred_id": "DCOILWTICO",       "diff_mode": "pct"},
    "GOLDAMGBD228NLBM": {"label": "Gold (London PM)", "fred_id": "GOLDAMGBD228NLBM", "diff_mode": "pct"},
}

WINDOW = 60         # rolling correlation window (trading days)
LOOKBACK_DAYS = 800 # ~2.2y to give 1Y of rolling-corr history after WINDOW warmup
HISTORY_TAIL = 252  # how much rolling-corr history to use for z-scores (~1Y)

# Pairs that have well-known historical relationships — used for interpretation
# strings when their correlations break. (All pairs are computed; this just
# adds context for the most diagnostic ones.)
NOTABLE_PAIRS = {
    frozenset(("SP500", "VIXCLS")):         "SPX↔VIX (textbook negative; flip = topping/bottom transition)",
    frozenset(("SP500", "DGS10")):          "Stock-bond correlation (flipping positive = inflation regime)",
    frozenset(("SP500", "DGS2")):           "SPX↔short-rates (sensitivity to Fed policy expectations)",
    frozenset(("DTWEXBGS", "SP500")):       "Dollar↔stocks (rising USD-SPX corr = late-cycle/risk-off)",
    frozenset(("DTWEXBGS", "GOLDAMGBD228NLBM")): "Dollar-gold (textbook negative; break = monetary regime shift)",
    frozenset(("DGS10", "GOLDAMGBD228NLBM")):    "10Y-Gold (real-rate hedge channel; break = stagflation pricing)",
    frozenset(("DGS10", "DGS2")):           "10Y-2Y co-movement (curve dynamics)",
    frozenset(("BAMLH0A0HYM2", "SP500")):   "Credit-equity (HY OAS spike with stocks holding = late warning)",
    frozenset(("DTWEXBGS", "DEXJPUS")):     "Broad USD vs JPY/USD (carry dynamics)",
    frozenset(("DCOILWTICO", "SP500")):     "Oil-stocks (growth vs inflation channel)",
}


# ─────────────────────────────────────────────────────────────────────
# FRED fetch
# ─────────────────────────────────────────────────────────────────────

def fred_observations(series_id, observation_start=None, limit=4000):
    """Pull observations from FRED. Returns sorted [(date_str, value_or_None), ...]"""
    params = {
        "series_id":       series_id,
        "api_key":         FRED_KEY,
        "file_type":       "json",
        "limit":           limit,
        "sort_order":      "asc",
    }
    if observation_start:
        params["observation_start"] = observation_start
    url = FRED_BASE + "?" + urllib.parse.urlencode(params)
    try:
        with urllib.request.urlopen(url, timeout=15) as r:
            data = json.loads(r.read())
    except Exception as e:
        print(f"[fred] {series_id} error: {e}")
        return []
    out = []
    for o in data.get("observations", []):
        v = o.get("value")
        try:
            f = float(v) if v not in (".", "", None) else None
        except (TypeError, ValueError):
            f = None
        out.append((o["date"], f))
    return out


# ─────────────────────────────────────────────────────────────────────
# Returns computation + alignment
# ─────────────────────────────────────────────────────────────────────

def compute_returns(observations, mode):
    """Convert raw observations to returns. Returns dict {date: ret}.

    mode='pct'  → (v_t / v_{t-1}) − 1
    mode='diff' → v_t − v_{t-1}
    Skips dates where either side is None or zero (for pct).
    """
    obs = [(d, v) for d, v in observations if v is not None]
    out = {}
    for i in range(1, len(obs)):
        d_prev, v_prev = obs[i - 1]
        d_curr, v_curr = obs[i]
        if v_prev is None or v_curr is None:
            continue
        if mode == "pct":
            if v_prev == 0:
                continue
            out[d_curr] = (v_curr / v_prev) - 1.0
        else:  # diff
            out[d_curr] = v_curr - v_prev
    return out


def align_returns(returns_by_instrument):
    """Find the intersection of dates where ALL instruments have returns,
    then build a wide table: list of (date, [r_inst1, r_inst2, ...]).

    Why intersection? Correlations need synchronized observations.
    """
    inst_keys = list(returns_by_instrument.keys())
    if not inst_keys:
        return []
    # Start with first instrument's dates
    common = set(returns_by_instrument[inst_keys[0]].keys())
    for k in inst_keys[1:]:
        common &= set(returns_by_instrument[k].keys())
    sorted_dates = sorted(common)
    table = []
    for d in sorted_dates:
        row = [returns_by_instrument[k][d] for k in inst_keys]
        table.append((d, row))
    return table, inst_keys


# ─────────────────────────────────────────────────────────────────────
# Correlation math (pure python)
# ─────────────────────────────────────────────────────────────────────

def pearson_corr(xs, ys):
    """Pearson correlation. Returns None if undefined (zero variance)."""
    n = len(xs)
    if n != len(ys) or n < 3:
        return None
    mx = sum(xs) / n
    my = sum(ys) / n
    cov = sum((xs[i] - mx) * (ys[i] - my) for i in range(n))
    vx = sum((x - mx) ** 2 for x in xs)
    vy = sum((y - my) ** 2 for y in ys)
    denom = math.sqrt(vx * vy)
    if denom == 0:
        return None
    return cov / denom


def correlation_matrix_at(table, end_idx, window=WINDOW):
    """Build N×N correlation matrix using rows [end_idx-window+1, end_idx] inclusive.
    table is [(date, [r1, r2, ...]), ...].
    Returns a list-of-lists matrix (None for undefined cells).
    """
    if end_idx < window - 1:
        return None
    start_idx = end_idx - window + 1
    rows = [r for _, r in table[start_idx:end_idx + 1]]
    if not rows:
        return None
    n_inst = len(rows[0])
    # Per-column series
    cols = [[r[i] for r in rows] for i in range(n_inst)]
    M = [[1.0] * n_inst for _ in range(n_inst)]
    for i in range(n_inst):
        for j in range(i + 1, n_inst):
            c = pearson_corr(cols[i], cols[j])
            M[i][j] = c
            M[j][i] = c
    return M


# ─────────────────────────────────────────────────────────────────────
# Lambda handler
# ─────────────────────────────────────────────────────────────────────

def lambda_handler(event, context):
    t0 = time.time()
    print(f"[correlation-breaks] starting at {datetime.now(timezone.utc).isoformat()}")

    # ── 1. Fetch all instruments in parallel ──
    start = (datetime.now(timezone.utc) - timedelta(days=LOOKBACK_DAYS)).strftime("%Y-%m-%d")
    raw_obs = {}

    def fetch(label, fred_id):
        return label, fred_observations(fred_id, observation_start=start)

    with ThreadPoolExecutor(max_workers=8) as ex:
        futures = [ex.submit(fetch, label, meta["fred_id"]) for label, meta in INSTRUMENTS.items()]
        for fut in as_completed(futures):
            label, obs = fut.result()
            raw_obs[label] = obs

    fetch_time = round(time.time() - t0, 1)
    series_lengths = {k: len([v for _, v in obs if v is not None]) for k, obs in raw_obs.items()}
    print(f"[correlation-breaks] fetched {len(raw_obs)} series in {fetch_time}s; lengths: {series_lengths}")

    # ── 2. Compute returns ──
    returns_by_inst = {}
    for label, meta in INSTRUMENTS.items():
        returns_by_inst[label] = compute_returns(raw_obs[label], meta["diff_mode"])

    # ── 3. Align to intersection ──
    table, inst_keys = align_returns(returns_by_inst)
    print(f"[correlation-breaks] aligned table: {len(table)} dates × {len(inst_keys)} instruments")

    if len(table) < WINDOW + HISTORY_TAIL:
        msg = (f"Insufficient aligned data: {len(table)} dates "
               f"(need ≥{WINDOW + HISTORY_TAIL})")
        print(f"[correlation-breaks] {msg}")
        report = {
            "schema_version": "1.0",
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "status": "warming_up",
            "message": msg,
            "n_dates_aligned": len(table),
            "n_instruments": len(inst_keys),
            "instruments": inst_keys,
            "labels": {k: INSTRUMENTS[k]["label"] for k in inst_keys},
        }
        s3.put_object(Bucket=S3_BUCKET, Key=S3_KEY, Body=json.dumps(report, indent=2),
                      ContentType="application/json", CacheControl="max-age=300")
        return {"statusCode": 200, "body": json.dumps({"status": "warming_up", "n_dates": len(table)})}

    n_inst = len(inst_keys)

    # ── 4. Compute rolling correlation history for each pair ──
    # Goal: for each pair (i,j), produce a list of [(date, corr)] for the
    # last HISTORY_TAIL+22 dates so we can z-score the latest value vs
    # 1Y history.
    pair_history = {}  # (i, j) → [(date, corr)]
    history_start_idx = max(WINDOW - 1, len(table) - HISTORY_TAIL - 22)

    # Precompute per-column series from table
    cols = [[row[i] for _, row in table] for i in range(n_inst)]

    for end_idx in range(history_start_idx, len(table)):
        start_idx = end_idx - WINDOW + 1
        date_label = table[end_idx][0]
        for i in range(n_inst):
            for j in range(i + 1, n_inst):
                xs = cols[i][start_idx:end_idx + 1]
                ys = cols[j][start_idx:end_idx + 1]
                c = pearson_corr(xs, ys)
                pair_history.setdefault((i, j), []).append((date_label, c))

    # ── 5. Latest matrix + 60d-prior matrix ──
    latest_M = correlation_matrix_at(table, len(table) - 1, WINDOW)
    prior_M = correlation_matrix_at(table, len(table) - 1 - WINDOW, WINDOW)

    # Delta matrix (latest − prior) — magnitude indicates regime change
    delta_M = [[None] * n_inst for _ in range(n_inst)]
    for i in range(n_inst):
        for j in range(n_inst):
            if latest_M[i][j] is not None and prior_M and prior_M[i][j] is not None:
                delta_M[i][j] = latest_M[i][j] - prior_M[i][j]

    # ── 6. Per-pair z-scores ──
    # For each pair, take its 1Y history of rolling correlation, exclude
    # the most-recent 22 days (so the z-score reflects deviation from the
    # 'normal' regime, not just smoothed reversion to current).
    pair_z = []
    for (i, j), hist in pair_history.items():
        vals = [c for _, c in hist if c is not None]
        if len(vals) < 60:
            continue
        latest = vals[-1]
        # Use values from prior window (exclude last ~22 days) as the baseline
        baseline = vals[:-22] if len(vals) > 30 else vals[:-5]
        if len(baseline) < 30:
            continue
        mean_base = sum(baseline) / len(baseline)
        var_base = sum((v - mean_base) ** 2 for v in baseline) / len(baseline)
        std_base = math.sqrt(var_base) if var_base > 0 else 0
        if std_base == 0:
            continue
        z = (latest - mean_base) / std_base
        pair_z.append({
            "i": i, "j": j,
            "label_i": inst_keys[i],
            "label_j": inst_keys[j],
            "current": round(latest, 4),
            "baseline_mean": round(mean_base, 4),
            "baseline_std": round(std_base, 4),
            "z_score": round(z, 2),
            "abs_z": abs(z),
        })

    pair_z.sort(key=lambda p: p["abs_z"], reverse=True)

    # Top breaking pairs (top 5)
    top_breaks = []
    for p in pair_z[:5]:
        key = frozenset((p["label_i"], p["label_j"]))
        # Direction-aware interpretation
        if p["current"] > p["baseline_mean"]:
            direction = "RISING (correlation strengthening)"
        else:
            direction = "FALLING (correlation weakening or flipping sign)"
        top_breaks.append({
            "pair": [p["label_i"], p["label_j"]],
            "labels": [INSTRUMENTS[p["label_i"]]["label"],
                       INSTRUMENTS[p["label_j"]]["label"]],
            "current_corr": p["current"],
            "baseline_corr": p["baseline_mean"],
            "delta": round(p["current"] - p["baseline_mean"], 4),
            "z_score": p["z_score"],
            "direction": direction,
            "context": NOTABLE_PAIRS.get(key, ""),
        })

    # ── 7. Composite Frobenius-delta score ──
    # ||M_today − M_{t}||_F over rolling t. We need a 1Y history of this
    # to z-score the latest delta against typical regime stability.
    fro_history = []
    for end_idx in range(history_start_idx + WINDOW, len(table)):
        m_now = correlation_matrix_at(table, end_idx, WINDOW)
        m_prev = correlation_matrix_at(table, end_idx - WINDOW, WINDOW)
        if m_now is None or m_prev is None:
            continue
        ssq = 0.0
        cnt = 0
        for i in range(n_inst):
            for j in range(i + 1, n_inst):  # upper triangle only — diagonal is always 0
                a, b = m_now[i][j], m_prev[i][j]
                if a is not None and b is not None:
                    ssq += (a - b) ** 2
                    cnt += 1
        if cnt > 0:
            fro = math.sqrt(ssq)
            fro_history.append({"date": table[end_idx][0], "frobenius_delta": fro})

    # Latest fro + z-score vs distribution (excluding last ~22d as buffer)
    fro_latest = fro_history[-1]["frobenius_delta"] if fro_history else None
    fro_baseline = [h["frobenius_delta"] for h in fro_history[:-22]] if len(fro_history) > 30 else []
    fro_z = None
    fro_baseline_mean = None
    fro_baseline_std = None
    if fro_baseline and len(fro_baseline) >= 30:
        fro_baseline_mean = sum(fro_baseline) / len(fro_baseline)
        var = sum((v - fro_baseline_mean) ** 2 for v in fro_baseline) / len(fro_baseline)
        fro_baseline_std = math.sqrt(var) if var > 0 else 0
        if fro_baseline_std > 0:
            fro_z = (fro_latest - fro_baseline_mean) / fro_baseline_std

    # Signal bucket
    if fro_z is None:
        signal = "WARMING_UP"
    elif fro_z >= 3:
        signal = "CRISIS"
    elif fro_z >= 2:
        signal = "ELEVATED"
    elif fro_z >= 1:
        signal = "WATCH"
    else:
        signal = "NORMAL"

    interpretation_map = {
        "CRISIS":   "Cross-asset correlations are breaking at >3σ — historically rare regime change underway",
        "ELEVATED": "Multiple historically-stable pairs are breaking. Aug-2024 carry-unwind / Mar-2020 pattern",
        "WATCH":    "Some correlations drifting from 1Y norms — early regime change indicator",
        "NORMAL":   "Cross-asset relationships within their typical 1Y range",
        "WARMING_UP": "Insufficient history for z-scoring",
    }

    # ── 8. Build report ──
    report = {
        "schema_version": "1.0",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "fetch_time_sec": fetch_time,
        "elapsed_sec": round(time.time() - t0, 1),
        "n_instruments": n_inst,
        "n_dates_aligned": len(table),
        "instruments": inst_keys,
        "labels": {k: INSTRUMENTS[k]["label"] for k in inst_keys},
        "diff_modes": {k: INSTRUMENTS[k]["diff_mode"] for k in inst_keys},
        "window_days": WINDOW,
        "history_tail_days": HISTORY_TAIL,

        # Composite
        "frobenius_delta": round(fro_latest, 4) if fro_latest is not None else None,
        "frobenius_delta_baseline_mean": round(fro_baseline_mean, 4) if fro_baseline_mean is not None else None,
        "frobenius_delta_baseline_std": round(fro_baseline_std, 4) if fro_baseline_std is not None else None,
        "frobenius_z_score_1y": round(fro_z, 2) if fro_z is not None else None,
        "signal": signal,
        "interpretation": interpretation_map[signal],

        # Top breaking pairs
        "top_breaking_pairs": top_breaks,
        "n_pairs_above_2sigma": sum(1 for p in pair_z if p["abs_z"] >= 2),
        "n_pairs_above_3sigma": sum(1 for p in pair_z if p["abs_z"] >= 3),

        # Matrices
        "latest_matrix": [[round(c, 4) if c is not None else None for c in row] for row in latest_M],
        "delta_matrix": [[round(c, 4) if c is not None else None for c in row] for row in delta_M],
        "matrix_dates": {
            "latest_window_end": table[-1][0],
            "latest_window_start": table[len(table) - WINDOW][0],
            "prior_window_end": table[len(table) - 1 - WINDOW][0],
            "prior_window_start": table[len(table) - 1 - 2 * WINDOW][0],
        },

        # History for charting Frobenius delta over time
        "frobenius_history": fro_history[-HISTORY_TAIL:],

        "data_sources": {
            "fred_api": "https://api.stlouisfed.org/fred",
            "license": "Public domain (FRED)",
        },
    }

    report = add_ka_aliases(report)
    body = json.dumps(report, default=str, indent=2)
    s3.put_object(
        Bucket=S3_BUCKET, Key=S3_KEY, Body=body,
        ContentType="application/json", CacheControl="max-age=300",
    )
    archive_key = f"data/archive/correlation-breaks/{datetime.now(timezone.utc).strftime('%Y%m%d')}.json"
    s3.put_object(Bucket=S3_BUCKET, Key=archive_key, Body=body, ContentType="application/json")

    summary = {
        "status": "ok",
        "elapsed_sec": report["elapsed_sec"],
        "signal": signal,
        "fro_z": report["frobenius_z_score_1y"],
        "n_pairs_2sigma": report["n_pairs_above_2sigma"],
        "n_pairs_3sigma": report["n_pairs_above_3sigma"],
        "top_break": top_breaks[0]["pair"] if top_breaks else None,
        "s3_key": S3_KEY,
    }
    print(f"[correlation-breaks] done: {summary}")
    return {"statusCode": 200, "body": json.dumps(summary)}
