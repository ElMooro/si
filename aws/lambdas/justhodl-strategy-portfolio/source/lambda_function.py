"""
justhodl-strategy-portfolio — STRATEGY-OF-STRATEGIES: the combined proven-alpha book
═══════════════════════════════════════════════════════════════════════════════════
The scorecard grades engines INDIVIDUALLY. This answers the question that actually
governs capital deployment: what is the Sharpe / drawdown / capacity of trading the
proven-alpha engines TOGETHER, net of cost, accounting for the CORRELATION between
them — and what are the optimal weights?

Pipeline (all on real graded outcomes, no new data):
  1. Read data/engine-alpha.json → proven set (FDR net-of-cost) + a positive-net candidate set.
  2. Reconstruct each engine's weekly NET-of-cost excess-vs-SPY return stream from
     justhodl-outcomes (price_at_signal/at_check signed by predicted_dir, minus SPY over the
     same window, minus COST_RT_PCT). 0-fill idle weeks = "flat when not trading" (honest book).
  3. Cross-engine correlation + Ledoit-Wolf-style shrunk covariance.
  4. Five weightings: equal · inverse-vol · risk-parity (ERC) · long-only max-Sharpe
     (constrained tangency) · HRP (Hierarchical Risk Parity, López de Prado 2016 — the
     institutional standard for combining strategies; robust to estimation error, no inversion).
  5. Combined equity curves + annualised Sharpe/Sortino/maxDD/Calmar/hit-rate +
     diversification ratio + effective bets + a breadth-based capacity tier per engine.
  6. Publish data/strategy-portfolio.json + SSM /justhodl/calibration/strategy-weights
     (the recommended HRP allocator vector for downstream sizing / future execution).

Honest framing: per-pick forward returns assigned to entry-week (not a continuously
rebalanced daily NAV); short histories on some engines → HRP is the recommended weighting
precisely because it does not trust a noisy covariance the way mean-variance does.
"""
import json
import math
import os
import time
import urllib.request
from collections import defaultdict
from datetime import date, datetime, timezone

import boto3

REGION = "us-east-1"
BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
OUT_KEY = "data/strategy-portfolio.json"
ALPHA_KEY = "data/engine-alpha.json"
OUTCOMES_TABLE = "justhodl-outcomes"
POLY = os.environ.get("POLYGON_KEY", "zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d")
COST_RT_PCT = 0.30          # round-trip cost per pick (matches scorecard)
MAX_ENGINES = 12            # cap universe for readable matrices
MIN_WEEKS = 8               # exclude engines with too little history
ANN = 52.0                  # weekly → annual

S3 = boto3.client("s3", REGION)
SSM = boto3.client("ssm", REGION)
DDB = boto3.resource("dynamodb", REGION)

UP = {"UP", "LONG", "BULL", "BULLISH", "BUY", "POSITIVE", "RISK_ON", "1", "+1", "OVERWEIGHT"}
DOWN = {"DOWN", "SHORT", "BEAR", "BEARISH", "SELL", "NEGATIVE", "RISK_OFF", "-1", "UNDERWEIGHT"}


def num(x):
    try:
        if x is None or x == "":
            return None
        return float(x)
    except Exception:
        return None


def get_field(o, key):
    """Outcome rows nest prices under 'outcome'; check top-level then sub-maps."""
    if key in o and o[key] not in (None, ""):
        return o[key]
    for sub in ("outcome", "signal_value", "detail"):
        d = o.get(sub)
        if isinstance(d, dict) and key in d and d[key] not in (None, ""):
            return d[key]
    return None


def dpart(s):
    if not s:
        return None
    return str(s)[:10]


# ── SPY benchmark series ──
def spy_series(start_iso):
    out = {}
    try:
        end = date.today().isoformat()
        with urllib.request.urlopen(urllib.request.Request(
            f"https://api.polygon.io/v2/aggs/ticker/SPY/range/1/day/{start_iso}/{end}"
            f"?adjusted=true&sort=asc&limit=50000&apiKey={POLY}", headers={"User-Agent": "jh/1"}), timeout=30) as r:
            for x in json.loads(r.read()).get("results") or []:
                d = datetime.fromtimestamp(x["t"] / 1000, timezone.utc).strftime("%Y-%m-%d")
                out[d] = x["c"]
    except Exception as e:
        print(f"[sp] SPY fetch err {str(e)[:60]}")
    return out


def spy_on(spy, d):
    if not d:
        return None
    if d in spy:
        return spy[d]
    cur = datetime.strptime(d, "%Y-%m-%d").date()
    for _ in range(7):
        cur = date.fromordinal(cur.toordinal() - 1)
        k = cur.isoformat()
        if k in spy:
            return spy[k]
    return None


def isoweek(d):
    y, w, _ = datetime.strptime(d, "%Y-%m-%d").date().isocalendar()
    return f"{y}-W{w:02d}"


def _held_weeks(d_sig, d_chk):
    """ISO weeks a position spans from entry to check (horizon-aware return spreading)."""
    a = datetime.strptime(d_sig, "%Y-%m-%d").date()
    b = datetime.strptime(d_chk, "%Y-%m-%d").date()
    if b < a:
        b = a
    out, cur = set(), a
    while cur <= b:
        out.add(isoweek(cur.isoformat()))
        cur = date.fromordinal(cur.toordinal() + 7)
    out.add(isoweek(b.isoformat()))
    return sorted(out)


# ── pure-python linear algebra ──
def mean(v):
    return sum(v) / len(v) if v else 0.0


def std(v):
    if len(v) < 2:
        return 0.0
    m = mean(v)
    return (sum((x - m) ** 2 for x in v) / (len(v) - 1)) ** 0.5


def cov_matrix(R):
    """R: list of columns (each = engine weekly series, equal length). Returns covariance."""
    k = len(R)
    n = len(R[0]) if k else 0
    mu = [mean(c) for c in R]
    C = [[0.0] * k for _ in range(k)]
    for i in range(k):
        for j in range(i, k):
            s = sum((R[i][t] - mu[i]) * (R[j][t] - mu[j]) for t in range(n))
            C[i][j] = C[j][i] = s / (n - 1) if n > 1 else 0.0
    return C, mu


def corr_from_cov(C):
    k = len(C)
    out = [[0.0] * k for _ in range(k)]
    for i in range(k):
        for j in range(k):
            di = C[i][i] ** 0.5
            dj = C[j][j] ** 0.5
            out[i][j] = C[i][j] / (di * dj) if di > 0 and dj > 0 else (1.0 if i == j else 0.0)
    return out


def shrink(C, delta=0.2):
    """Shrink covariance toward its diagonal for stability with short series."""
    k = len(C)
    return [[(C[i][j] if i == j else (1 - delta) * C[i][j]) for j in range(k)] for i in range(k)]


def matvec(A, x):
    return [sum(A[i][j] * x[j] for j in range(len(x))) for i in range(len(A))]


def dot(a, b):
    return sum(a[i] * b[i] for i in range(len(a)))


def mat_inv(A):
    n = len(A)
    M = [row[:] + [1.0 if i == j else 0.0 for j in range(n)] for i, row in enumerate(A)]
    for col in range(n):
        piv = max(range(col, n), key=lambda r: abs(M[r][col]))
        if abs(M[piv][col]) < 1e-12:
            raise ValueError("singular")
        M[col], M[piv] = M[piv], M[col]
        pv = M[col][col]
        M[col] = [v / pv for v in M[col]]
        for r in range(n):
            if r != col and abs(M[r][col]) > 1e-15:
                f = M[r][col]
                M[r] = [M[r][c] - f * M[col][c] for c in range(2 * n)]
    return [row[n:] for row in M]


def normalize(w):
    s = sum(w)
    return [x / s for x in w] if s > 0 else [1.0 / len(w)] * len(w)


def w_equal(C):
    return [1.0 / len(C)] * len(C)


def w_invvol(C):
    iv = [1.0 / (C[i][i] ** 0.5) if C[i][i] > 0 else 0.0 for i in range(len(C))]
    return normalize(iv)


def w_riskparity(C):
    n = len(C)
    w = [1.0 / n] * n
    for _ in range(800):
        Sw = matvec(C, w)
        pv = dot(w, Sw)
        if pv <= 0:
            break
        target = pv / n
        for i in range(n):
            rc = w[i] * Sw[i]
            rc = rc if rc > 1e-9 else 1e-9          # hedge legs (neg RC) clamped positive
            mult = (target / rc) ** 0.5
            w[i] *= min(max(mult, 0.7), 1.4)        # damp to prevent divergence
        w = normalize(w)
    return w


def w_maxsharpe(C, mu, cap=0.40):
    try:
        inv = mat_inv(C)
    except Exception:
        return w_invvol(C)
    raw = matvec(inv, mu)
    w = [max(0.0, x) for x in raw]
    if sum(w) == 0:
        return w_invvol(C)
    w = normalize(w)
    for _ in range(60):
        over = [i for i in range(len(w)) if w[i] > cap + 1e-9]
        if not over:
            break
        for i in over:
            w[i] = cap
        rem = 1.0 - sum(w)
        under = [i for i in range(len(w)) if w[i] < cap - 1e-9]
        tot = sum(w[i] for i in under)
        if tot <= 0:
            break
        for i in under:
            w[i] += rem * w[i] / tot
    return normalize(w)


def w_hrp(C):
    n = len(C)
    if n == 1:
        return [1.0]
    corr = corr_from_cov(C)
    D = [[(0.5 * (1 - corr[i][j])) ** 0.5 for j in range(n)] for i in range(n)]
    clusters = {i: [i] for i in range(n)}
    children = {}
    active = list(range(n))
    nid = n
    while len(active) > 1:
        best = None
        for ai in range(len(active)):
            for bi in range(ai + 1, len(active)):
                ca, cb = active[ai], active[bi]
                dist = min(D[x][y] for x in clusters[ca] for y in clusters[cb])  # single linkage
                if best is None or dist < best[0]:
                    best = (dist, ca, cb)
        _, ca, cb = best
        clusters[nid] = clusters[ca] + clusters[cb]
        children[nid] = (ca, cb)
        active.remove(ca)
        active.remove(cb)
        active.append(nid)
        nid += 1
    order = []

    def rec(c):
        if c in children:
            rec(children[c][0])
            rec(children[c][1])
        else:
            order.append(c)
    rec(active[0])

    def cluster_var(idx):
        iv = [1.0 / C[i][i] if C[i][i] > 0 else 0.0 for i in idx]
        s = sum(iv) or 1.0
        ivw = [x / s for x in iv]
        return sum(ivw[a] * C[idx[a]][idx[b]] * ivw[b] for a in range(len(idx)) for b in range(len(idx)))

    w = {i: 1.0 for i in order}
    stack = [order]
    while stack:
        nxt = []
        for cl in stack:
            if len(cl) <= 1:
                continue
            half = len(cl) // 2
            c1, c2 = cl[:half], cl[half:]
            v1, v2 = cluster_var(c1), cluster_var(c2)
            a1 = 1 - v1 / (v1 + v2) if (v1 + v2) > 0 else 0.5
            for i in c1:
                w[i] *= a1
            for i in c2:
                w[i] *= (1 - a1)
            nxt.append(c1)
            nxt.append(c2)
        stack = nxt
    return [w[i] for i in range(n)]


def port_stats(R, w):
    """R columns = engine weekly series; w = weights. Returns book stats."""
    n = len(R[0])
    rp = [sum(w[e] * R[e][t] for e in range(len(w))) for t in range(n)]  # weekly % returns
    m, sd = mean(rp), std(rp)
    ann_ret = m * ANN
    ann_vol = sd * (ANN ** 0.5)
    sharpe = ann_ret / ann_vol if ann_vol > 0 else None
    downside = [x for x in rp if x < 0]
    dd = (sum(x * x for x in downside) / len(rp)) ** 0.5 if downside else 0.0
    sortino = (m * ANN) / (dd * (ANN ** 0.5)) if dd > 0 else None
    cum, peak, maxdd = 1.0, 1.0, 0.0
    curve = []
    for i, x in enumerate(rp):
        cum *= (1 + x / 100.0)
        peak = max(peak, cum)
        maxdd = min(maxdd, cum / peak - 1)
        curve.append(round((cum - 1) * 100, 2))
    calmar = (ann_ret / abs(maxdd * 100)) if maxdd < 0 else None
    hit = sum(1 for x in rp if x > 0) / len(rp) if rp else None
    return {
        "ann_return_pct": round(ann_ret, 2), "ann_vol_pct": round(ann_vol, 2),
        "sharpe": round(sharpe, 2) if sharpe else None,
        "sortino": round(sortino, 2) if sortino else None,
        "max_drawdown_pct": round(maxdd * 100, 2),
        "calmar": round(calmar, 2) if calmar else None,
        "hit_rate_pct": round(hit * 100, 1) if hit else None,
        "total_return_pct": curve[-1] if curve else 0.0,
    }, curve


def lambda_handler(event=None, context=None):
    t0 = time.time()
    alpha = json.loads(S3.get_object(Bucket=BUCKET, Key=ALPHA_KEY)["Body"].read())
    engmap = alpha.get("engines", {})
    proven = list(alpha.get("alpha_proven_signals") or [])
    # candidate set: proven + positive net-mean engines (n>=20), capped
    cand = list(proven)
    extras = sorted(
        [(k, v) for k, v in engmap.items()
         if k not in proven and (num(v.get("net_mean_excess_pct")) or -9) > 0 and (v.get("alpha_n") or 0) >= 20],
        key=lambda kv: -(num(kv[1].get("net_t_stat")) or 0))
    for k, _ in extras:
        if len(cand) >= MAX_ENGINES:
            break
        cand.append(k)

    # ── scan outcomes, build per-engine pick list (date, signed net excess) ──
    table = DDB.Table(OUTCOMES_TABLE)
    picks = defaultdict(list)   # engine -> [(week, weekly_contribution)]
    npicks = defaultdict(int)   # engine -> distinct pick count
    names_seen = defaultdict(set)
    earliest = "2030-01-01"
    rows = []
    kw = {}
    scanned = 0
    while True:
        r = table.scan(**kw)
        rows.extend(r.get("Items", []))
        scanned += len(r.get("Items", []))
        if "LastEvaluatedKey" not in r or scanned > 80000:
            break
        kw["ExclusiveStartKey"] = r["LastEvaluatedKey"]
    candset = set(cand)
    for o in rows:
        st = o.get("signal_type")
        if st not in candset:
            continue
        d_sig = dpart(o.get("logged_at"))
        if d_sig and d_sig < earliest:
            earliest = d_sig
    spy = spy_series(earliest if earliest < "2030-01-01" else "2024-01-01")

    for o in rows:
        st = o.get("signal_type")
        if st not in candset:
            continue
        pd = str(o.get("predicted_dir") or "").strip().upper()
        dm = 1.0 if pd in UP else (-1.0 if pd in DOWN else 0.0)
        if dm == 0.0:
            continue
        p_sig = num(get_field(o, "price_at_signal"))
        p_chk = num(get_field(o, "price_at_check"))
        d_sig = dpart(o.get("logged_at"))
        d_chk = dpart(o.get("checked_at"))
        if not (p_sig and p_chk and p_sig > 0 and d_sig and d_chk):
            continue
        asset_ret = (p_chk / p_sig - 1) * 100
        ss, sc = spy_on(spy, d_sig), spy_on(spy, d_chk)
        spy_ret = (sc / ss - 1) * 100 if ss and sc and ss > 0 else 0.0
        net_excess = dm * (asset_ret - spy_ret) - COST_RT_PCT
        wks = _held_weeks(d_sig, d_chk)
        contrib = net_excess / len(wks)          # horizon-aware: spread across held weeks
        for wk in wks:
            picks[st].append((wk, contrib))
        npicks[st] += 1
        tk = get_field(o, "ticker") or get_field(o, "symbol")
        if tk:
            names_seen[st].add(str(tk).upper())

    # weekly mean per engine, then 0-filled aligned matrix
    weekly = {}   # engine -> {week: mean_net_excess}
    for e, lst in picks.items():
        agg = defaultdict(list)
        for wk, x in lst:
            agg[wk].append(x)
        weekly[e] = {wk: mean(v) for wk, v in agg.items()}
    engines = [e for e in cand if e in weekly and len(weekly[e]) >= MIN_WEEKS]
    if len(engines) < 2:
        out = {"engine": "justhodl-strategy-portfolio", "ok": False,
               "reason": f"insufficient engines with >= {MIN_WEEKS} weeks (have {len(engines)})",
               "candidates": cand, "generated_at": datetime.now(timezone.utc).isoformat()}
        S3.put_object(Bucket=BUCKET, Key=OUT_KEY, Body=json.dumps(out, default=str).encode(), ContentType="application/json")
        return {"statusCode": 200, "body": json.dumps(out)}

    all_weeks = sorted({wk for e in engines for wk in weekly[e]})
    R = [[weekly[e].get(wk, 0.0) for wk in all_weeks] for e in engines]   # columns per engine
    C, mu = cov_matrix(R)
    Cs = shrink(C, 0.2)
    corr = corr_from_cov(C)

    methods = {
        "equal_weight": w_equal(Cs),
        "inverse_vol": w_invvol(Cs),
        "risk_parity": w_riskparity(Cs),
        "max_sharpe": w_maxsharpe(Cs, mu),
        "hrp": w_hrp(Cs),
    }
    results = {}
    curves = {}
    for name, w in methods.items():
        s, curve = port_stats(R, w)
        s["weights"] = {engines[i]: round(w[i], 4) for i in range(len(engines))}
        # diversification ratio + effective bets
        wv = matvec(Cs, w)
        pv = dot(w, wv)
        wavg_vol = sum(w[i] * (Cs[i][i] ** 0.5) for i in range(len(w)))
        s["diversification_ratio"] = round(wavg_vol / (pv ** 0.5), 2) if pv > 0 else None
        rc = [w[i] * wv[i] for i in range(len(w))]
        rsum = sum(rc) or 1.0
        s["effective_bets"] = round(1.0 / sum((x / rsum) ** 2 for x in rc), 2)
        results[name] = s
        curves[name] = curve

    # per-engine summary + capacity tier
    def cap_tier(e):
        nnames = len(names_seen.get(e, set()))
        macro_like = any(t in e for t in ("crisis", "macro", "regime", "dfii", "momentum", "yield", "plumbing", "khalid", "edge"))
        if macro_like:
            return "VERY_HIGH (liquid ETF/futures expression)"
        if nnames >= 30:
            return "HIGH (broad single-name breadth)"
        if nnames >= 8:
            return "MEDIUM"
        return "LOW (concentrated / few names)"

    per_engine = []
    for i, e in enumerate(engines):
        ev = engmap.get(e, {})
        per_engine.append({
            "engine": e, "alpha_status": ev.get("alpha_status"),
            "net_mean_excess_pct": num(ev.get("net_mean_excess_pct")),
            "net_t_stat": num(ev.get("net_t_stat")), "info_ratio": num(ev.get("info_ratio")),
            "alpha_n": ev.get("alpha_n"),
            "weekly_vol_pct": round(Cs[i][i] ** 0.5, 2), "n_weeks": len(weekly[e]),
            "n_picks": npicks[e], "n_names": len(names_seen.get(e, set())),
            "capacity_tier": cap_tier(e),
        })

    # downsample curves for the page (cap ~120 pts)
    def ds(c):
        if len(c) <= 120:
            return c
        step = len(c) / 120
        return [c[int(i * step)] for i in range(120)]

    recommended = "hrp"
    rec_w = results[recommended]["weights"]
    try:
        SSM.put_parameter(Name="/justhodl/calibration/strategy-weights",
                          Value=json.dumps({"method": recommended, "weights": rec_w,
                                            "as_of": datetime.now(timezone.utc).isoformat()}),
                          Type="String", Overwrite=True, Tier="Standard")
    except Exception as e:
        print(f"[sp] ssm err {str(e)[:60]}")

    payload = {
        "engine": "justhodl-strategy-portfolio", "version": "1.0.0", "ok": True,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "benchmark": "excess vs SPY, net of {:.2f}% round-trip cost".format(COST_RT_PCT),
        "thesis": ("Combined book of the proven-alpha engines: correlation-aware weights "
                   "answer 'what is the Sharpe/drawdown/capacity of trading these together' — "
                   "the number individual engine scorecards cannot give."),
        "proven_set": proven, "candidate_set": engines, "n_engines": len(engines),
        "n_weeks": len(all_weeks), "history_from": all_weeks[0], "history_to": all_weeks[-1],
        "per_engine": sorted(per_engine, key=lambda x: -(x["net_t_stat"] or 0)),
        "correlation_matrix": {"labels": engines,
                               "matrix": [[round(corr[i][j], 2) for j in range(len(engines))] for i in range(len(engines))]},
        "weightings": results,
        "equity_curves": {"weeks": ds(all_weeks), "series": {k: ds(v) for k, v in curves.items()}},
        "recommended": {"method": recommended, "weights": rec_w,
                        "why": ("HRP (López de Prado 2016): allocates by hierarchical clustering of the "
                                "correlation structure — robust to estimation error and needs no matrix "
                                "inversion, so it does not over-trust a short-history covariance the way "
                                "mean-variance does. Published to SSM for downstream sizing.")},
        "caveats": [
            "Per-pick forward excess assigned to entry-week, 0-filled when an engine is idle "
            "(book holds nothing) — an approximation of a continuously-rebalanced NAV, not a tick-level backtest.",
            "Only FDR-proven + positive-net-mean engines included; short histories make the covariance "
            "noisy → HRP is recommended over mean-variance for exactly that reason.",
            "Capacity is a breadth/expression-based TIER, not a market-impact model. Macro engines "
            "express via liquid ETFs/futures (very high capacity); concentrated single-name engines are lower.",
            "Excess is vs SPY for ALL engines (matches the scorecard), so macro/crypto picks are framed "
            "as long/short-vs-SPY — consistent, but read tiers, not absolute levels.",
        ],
        "elapsed_s": round(time.time() - t0, 1),
    }
    S3.put_object(Bucket=BUCKET, Key=OUT_KEY, Body=json.dumps(payload, default=str).encode(),
                  ContentType="application/json", CacheControl="public, max-age=3600")
    print(f"[sp] engines={len(engines)} weeks={len(all_weeks)} "
          f"HRP_sharpe={results['hrp']['sharpe']} maxSharpe={results['max_sharpe']['sharpe']} in {payload['elapsed_s']}s")
    return {"statusCode": 200, "body": json.dumps({
        "ok": True, "n_engines": len(engines), "n_weeks": len(all_weeks),
        "sharpe_by_method": {k: results[k]["sharpe"] for k in results},
        "hrp_weights": rec_w})}
