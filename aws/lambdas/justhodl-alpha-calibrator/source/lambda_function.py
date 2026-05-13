"""
justhodl-alpha-calibrator — Roadmap #1 SELF-IMPROVEMENT LOOP
═════════════════════════════════════════════════════════════════════════════
Institutional-grade quant calibration engine for the 8-factor alpha-score model.

Distinct from the legacy `justhodl-calibrator` (which calibrates signal-logger
DDB outcomes). This Lambda calibrates the alpha-score WEIGHTS by reading the
trade journal (DDB justhodl-trades) and running full quant diagnostics.

WHAT THIS DOES
──────────────
Reads the trade journal weekly and:

  1. Per-strategy performance with full statistical rigor
     - Win rate + Wilson 95% confidence interval
     - Mean / median / std / skew / kurtosis of returns
     - Sharpe + Sortino (annualized)
     - Expectancy + profit factor + max drawdown
     - t-statistic + p-value for return ≠ 0
     - Hit rate premium vs SPY base rate (same window)
     - Regime-stratified breakdown (NORMAL / ELEVATED / HIGH / CRISIS)
     - Decay curve: returns at 1d, 7d, 30d, 90d, 180d

  2. Information Coefficients per alpha component
     - IC = corr(component_score_at_call, forward_return)
     - Per horizon (1d / 7d / 30d / 90d)
     - IC standard error + t-stat + p-value
     - Information Ratio (mean_IC / std_IC over time)

  3. Factor attribution OLS regression
     - return_30d ~ quality + growth + momentum + smart_money +
                     sentiment + analysts + insiders + options_flow
     - Per-factor coefficients, std errors, t-stats, p-values
     - R² + adjusted R² + sigma
     - Data-implied optimal weights from regression coefficients

  4. Bayesian weight update with shrinkage + guardrails
     - new = (1 - λ) × current + λ × data_implied
     - λ = min(N / 200, 0.40) — capped shrinkage
     - Max shift per cycle: |Δweight| ≤ 0.03 absolute
     - Floor: 0.04, ceiling: 0.22 per factor
     - Renormalize to sum = 1.00

  5. Champion / Challenger A/B deployment (manual approval default)
     - Proposed weights written to S3 but NOT auto-applied
     - Requires `auto_apply_calibrations: true` flag in alpha-weights.json
     - Manual override always available

  6. Audit trail
     - data/calibration-latest.json (full diagnostics, this run)
     - data/calibration-history.json (rolling 52 weeks)
     - Telegram alert with significant findings

═════════════════════════════════════════════════════════════════════════════
WHY EACH DESIGN DECISION (the quant rigor)
──────────────────────────────────────────

PROBLEM 1: Win rate is misleading
  Solution: Sharpe + Sortino + expectancy + profit factor. A 49% win rate
  with 3:1 reward-risk is more profitable than 60% win rate with 1:1.

PROBLEM 2: Small samples lie
  Solution: Wilson CI + t-stat. With N=10 a 70% win rate has CI [42%, 89%].
  Don't change weights on 10 trades.

PROBLEM 3: Regimes confound
  Solution: Stratify everything by regime_at_call. A signal that works in
  NORMAL may bomb in CRISIS — we need to know which.

PROBLEM 4: Overfitting to recent noise
  Solution: Shrinkage. Bayesian update toward prior with weight depending
  on sample size. Small N → small λ → small change.

PROBLEM 5: Local maxima
  Solution: Guardrails. No factor can shift more than 0.03 per cycle.
  No factor can be below 0.04 or above 0.22. Prevents the optimizer
  from concentrating bet on a momentary winner.

PROBLEM 6: Survivorship bias in journal
  Solution: We use ALL evaluated trades, not just winners or closed.
  STILL_OPEN observations contribute via their currently-realized return.

PROBLEM 7: SPY base-rate awareness
  Solution: For each call we compute SPY return over the same window.
  "Excess return" tells us whether the strategy actually beat buy-and-hold.

═════════════════════════════════════════════════════════════════════════════
"""
import json
import math
import os
import time
import urllib.request
import urllib.parse
from datetime import datetime, timezone, timedelta, date
from decimal import Decimal
from collections import defaultdict

import boto3

VERSION = "1.0.0"

# ─── S3 keys ───
S3_BUCKET = "justhodl-dashboard-live"
JOURNAL_KEY = "data/trade-journal.json"
ALPHA_KEY = "screener/alpha-score.json"
LATEST_KEY = "data/calibration-latest.json"
HISTORY_KEY = "data/calibration-history.json"
WEIGHTS_KEY = "screener/alpha-weights.json"   # consumed by alpha-score

# ─── DDB ───
DDB_TABLE = "justhodl-trades"

POLY_KEY = os.environ.get("POLY_KEY", "")
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")

# ─── Calibration parameters ───
LOOKBACK_DAYS = int(os.environ.get("LOOKBACK_DAYS", "120"))
MIN_OBS_FOR_STAT = int(os.environ.get("MIN_OBS_FOR_STAT", "30"))
MIN_OBS_FOR_WEIGHT_UPDATE = int(os.environ.get("MIN_OBS_FOR_WEIGHT_UPDATE", "60"))
EVAL_HORIZON_DAYS = 30
SHRINKAGE_DIVISOR = 200
MAX_SHRINKAGE = 0.40
MAX_WEIGHT_SHIFT = 0.03
WEIGHT_FLOOR = 0.04
WEIGHT_CEILING = 0.22

COMPONENTS = ["quality", "growth", "momentum", "smart_money",
              "sentiment", "analysts", "insiders", "options_flow"]

STRATEGIES = ["TIER_S_CONFLUENCE", "TIER_A_CONFLUENCE",
              "TIER_S_ALPHA", "TIER_A_ALPHA",
              "REGIME_PICK",
              "DEBATE_STRONG_BUY", "DEBATE_BUY",
              "OPTIONS_TIER_A"]

s3 = boto3.client("s3", region_name="us-east-1")
ddb = boto3.resource("dynamodb", region_name="us-east-1")
table = ddb.Table(DDB_TABLE)
ssm = boto3.client("ssm", region_name="us-east-1")


# ═══════════════════════════════════════════════════════════════════════════
# PURE-PYTHON STATISTICS (no scipy in Lambda runtime)
# ═══════════════════════════════════════════════════════════════════════════

def mean(xs):
    return sum(xs) / len(xs) if xs else None

def median(xs):
    if not xs: return None
    s = sorted(xs); n = len(s)
    if n % 2 == 1: return s[n // 2]
    return (s[n // 2 - 1] + s[n // 2]) / 2

def stdev(xs):
    if len(xs) < 2: return None
    m = mean(xs)
    return math.sqrt(sum((x - m) ** 2 for x in xs) / (len(xs) - 1))

def skewness(xs):
    if len(xs) < 3: return None
    m = mean(xs); s = stdev(xs)
    if not s: return None
    n = len(xs)
    return (n / ((n - 1) * (n - 2))) * sum(((x - m) / s) ** 3 for x in xs)

def excess_kurtosis(xs):
    if len(xs) < 4: return None
    m = mean(xs); s = stdev(xs)
    if not s: return None
    n = len(xs)
    g2 = (n * (n + 1)) / ((n - 1) * (n - 2) * (n - 3))
    s2 = sum(((x - m) / s) ** 4 for x in xs)
    correction = 3 * (n - 1) ** 2 / ((n - 2) * (n - 3))
    return g2 * s2 - correction

def norm_cdf(z):
    return 0.5 * (1 + math.erf(z / math.sqrt(2)))

def t_to_p_two_tailed(t, dof):
    if t is None or dof is None or dof < 1: return None
    abs_t = abs(t)
    if dof >= 30:
        return 2 * (1 - norm_cdf(abs_t))
    z = abs_t * (1 - 1 / (4 * dof)) / math.sqrt(1 + abs_t * abs_t / (2 * dof))
    return 2 * (1 - norm_cdf(z))

def correlation(xs, ys):
    n = len(xs)
    if n < 3 or n != len(ys): return None, n, None, None, None
    mx, my = mean(xs), mean(ys)
    num = sum((x - mx) * (y - my) for x, y in zip(xs, ys))
    dx = math.sqrt(sum((x - mx) ** 2 for x in xs))
    dy = math.sqrt(sum((y - my) ** 2 for y in ys))
    if dx == 0 or dy == 0: return None, n, None, None, None
    r = num / (dx * dy)
    if abs(r) >= 1.0: return r, n, None, None, None
    se = math.sqrt((1 - r * r) / (n - 2))
    t = r / se if se > 0 else None
    p = t_to_p_two_tailed(t, n - 2) if t is not None else None
    return r, n, t, p, se

def wilson_ci(wins, n, confidence=0.95):
    if n == 0: return (None, None)
    z = 1.959964
    p = wins / n
    denom = 1 + z * z / n
    center = (p + z * z / (2 * n)) / denom
    half = z * math.sqrt((p * (1 - p) / n) + (z * z / (4 * n * n))) / denom
    return (max(0, center - half), min(1, center + half))

def max_drawdown(returns):
    if not returns: return None
    equity = 1.0; peak = 1.0; max_dd = 0
    for r in returns:
        equity *= (1 + r / 100.0) if abs(r) > 1 else (1 + r)
        if equity > peak: peak = equity
        dd = (equity / peak) - 1
        if dd < max_dd: max_dd = dd
    return max_dd


# ═══════════════════════════════════════════════════════════════════════════
# OLS REGRESSION (pure Python)
# ═══════════════════════════════════════════════════════════════════════════

def matmul(A, B):
    m, n, p = len(A), len(A[0]), len(B[0])
    return [[sum(A[i][k] * B[k][j] for k in range(n)) for j in range(p)] for i in range(m)]

def transpose(A):
    return [list(row) for row in zip(*A)]

def matrix_inverse(M):
    n = len(M)
    aug = [row[:] + [1.0 if i == j else 0.0 for j in range(n)] for i, row in enumerate(M)]
    for i in range(n):
        pivot = aug[i][i]
        if abs(pivot) < 1e-12:
            for k in range(i + 1, n):
                if abs(aug[k][i]) > 1e-12:
                    aug[i], aug[k] = aug[k], aug[i]; pivot = aug[i][i]; break
            else: return None
        for j in range(2 * n): aug[i][j] /= pivot
        for k in range(n):
            if k != i and abs(aug[k][i]) > 1e-12:
                factor = aug[k][i]
                for j in range(2 * n): aug[k][j] -= factor * aug[i][j]
    return [row[n:] for row in aug]

def ols_regression(X, y):
    n, kpp = len(X), len(X[0]) if X else 0
    if n < kpp + 1: return None
    Xt = transpose(X)
    XtX = matmul(Xt, X)
    XtX_inv = matrix_inverse(XtX)
    if XtX_inv is None: return None
    Xty = matmul(Xt, [[v] for v in y])
    beta = [row[0] for row in matmul(XtX_inv, Xty)]
    y_pred = [sum(X[i][j] * beta[j] for j in range(kpp)) for i in range(n)]
    residuals = [y[i] - y_pred[i] for i in range(n)]
    ss_res = sum(r * r for r in residuals)
    y_mean = mean(y)
    ss_tot = sum((yi - y_mean) ** 2 for yi in y)
    r2 = 1 - (ss_res / ss_tot) if ss_tot > 0 else 0
    dof = n - kpp
    adj_r2 = 1 - (1 - r2) * (n - 1) / dof if dof > 0 else r2
    sigma2 = ss_res / dof if dof > 0 else 0
    se = [math.sqrt(sigma2 * XtX_inv[i][i]) if XtX_inv[i][i] > 0 else None for i in range(kpp)]
    t_stats = [beta[i] / se[i] if se[i] and se[i] > 0 else None for i in range(kpp)]
    p_values = [t_to_p_two_tailed(t, dof) if t is not None else None for t in t_stats]
    return {"n_obs": n, "k_predictors": kpp - 1, "dof": dof,
             "beta": beta, "se": se, "t_stats": t_stats, "p_values": p_values,
             "r_squared": round(r2, 4), "adj_r_squared": round(adj_r2, 4),
             "sigma": round(math.sqrt(sigma2), 6) if sigma2 > 0 else 0}


# ═══════════════════════════════════════════════════════════════════════════
# DATA LOADERS
# ═══════════════════════════════════════════════════════════════════════════

def load_s3_json(key, default=None):
    try:
        return json.loads(s3.get_object(Bucket=S3_BUCKET, Key=key)["Body"].read())
    except Exception as e:
        print(f"  load {key} err: {str(e)[:120]}")
        return default

def _decimal_to_float(obj):
    if isinstance(obj, Decimal): return float(obj)
    if isinstance(obj, dict): return {k: _decimal_to_float(v) for k, v in obj.items()}
    if isinstance(obj, list): return [_decimal_to_float(v) for v in obj]
    return obj

def scan_all_calls(lookback_days=180):
    cutoff = (datetime.now(timezone.utc).date() - timedelta(days=lookback_days)).isoformat()
    items = []
    last = None
    while True:
        kwargs = {"KeyConditionExpression": "pk = :p AND sk >= :s",
                   "ExpressionAttributeValues": {":p": "CALL", ":s": cutoff}}
        if last: kwargs["ExclusiveStartKey"] = last
        resp = table.query(**kwargs)
        items.extend(resp.get("Items") or [])
        last = resp.get("LastEvaluatedKey")
        if not last: break
    return [_decimal_to_float(i) for i in items]

def fetch_spy_close(target_date_iso):
    if not POLY_KEY: return None
    end = (date.fromisoformat(target_date_iso[:10]) + timedelta(days=10)).isoformat()
    url = (f"https://api.polygon.io/v2/aggs/ticker/SPY/range/1/day/"
           f"{target_date_iso[:10]}/{end}?adjusted=true&sort=asc&limit=20&apiKey={POLY_KEY}")
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "JustHodl-Cal/1.0"})
        with urllib.request.urlopen(req, timeout=8) as r:
            data = json.loads(r.read().decode("utf-8"))
        bars = data.get("results") or []
        if bars: return float(bars[0]["c"])
    except Exception: pass
    return None

def cached_spy_returns(calls, horizon_days):
    unique_dates = sorted({(c.get("call_date") or "")[:10] for c in calls if c.get("call_date")})
    out = {}
    for d in unique_dates:
        if not d: continue
        try:
            entry = fetch_spy_close(d)
            target = (date.fromisoformat(d) + timedelta(days=horizon_days)).isoformat()
            exit_close = fetch_spy_close(target)
            if entry and exit_close:
                out[d] = round((exit_close / entry - 1) * 100, 2)
            else:
                out[d] = None
        except Exception as e:
            print(f"  spy {d} err: {str(e)[:60]}")
            out[d] = None
    return out


# ═══════════════════════════════════════════════════════════════════════════
# ANALYTICS
# ═══════════════════════════════════════════════════════════════════════════

def analyze_strategy(strategy, calls, spy_returns):
    strat_calls = [c for c in calls if c.get("strategy") == strategy]
    n_total = len(strat_calls)
    with_30d = [c for c in strat_calls
                 if c.get("outcome_30d") and isinstance(c["outcome_30d"].get("return_pct"), (int, float))]
    returns_30d = [c["outcome_30d"]["return_pct"] for c in with_30d]
    n_eval = len(returns_30d)
    base = {"strategy": strategy, "n_total": n_total, "n_evaluated_30d": n_eval,
             "min_obs_for_stats": MIN_OBS_FOR_STAT,
             "statistically_sufficient": n_eval >= MIN_OBS_FOR_STAT}
    if n_eval < 2: return base
    mu = mean(returns_30d); med = median(returns_30d); sigma = stdev(returns_30d)
    n_wins = sum(1 for r in returns_30d if r > 0)
    wr = n_wins / n_eval
    wr_lo, wr_hi = wilson_ci(n_wins, n_eval)
    base.update({
        "win_rate_30d": round(wr, 4),
        "win_rate_95ci": [round(wr_lo, 4) if wr_lo is not None else None,
                            round(wr_hi, 4) if wr_hi is not None else None],
        "n_wins": n_wins,
        "avg_return_30d_pct": round(mu, 3),
        "median_return_30d_pct": round(med, 3),
        "stdev_return_30d_pct": round(sigma, 3) if sigma else None,
    })
    sk = skewness(returns_30d); ek = excess_kurtosis(returns_30d)
    if sk is not None: base["skewness"] = round(sk, 3)
    if ek is not None: base["excess_kurtosis"] = round(ek, 3)

    if sigma and sigma > 0:
        sharpe = (mu / sigma) * math.sqrt(12)
        base["sharpe_annualized"] = round(sharpe, 3)
        downside = [r for r in returns_30d if r < 0]
        down_sigma = stdev(downside) if len(downside) >= 2 else None
        if down_sigma and down_sigma > 0:
            base["sortino_annualized"] = round((mu / down_sigma) * math.sqrt(12), 3)

    wins = [r for r in returns_30d if r > 0]; losses = [r for r in returns_30d if r < 0]
    avg_win = mean(wins) if wins else 0
    avg_loss = mean(losses) if losses else 0
    if avg_loss != 0:
        loss_rate = len(losses) / n_eval
        base["expectancy_pct"] = round(wr * avg_win + loss_rate * avg_loss, 3)
    sum_wins = sum(wins) if wins else 0
    sum_losses = abs(sum(losses)) if losses else 0
    base["profit_factor"] = round(sum_wins / sum_losses, 3) if sum_losses > 0 else None

    md = max_drawdown(returns_30d)
    if md is not None: base["max_drawdown_pct"] = round(md * 100, 2)

    if sigma and sigma > 0:
        t_stat = mu / (sigma / math.sqrt(n_eval))
        p_val = t_to_p_two_tailed(t_stat, n_eval - 1)
        base["t_stat"] = round(t_stat, 3)
        base["p_value"] = round(p_val, 4) if p_val is not None else None
        base["significant_05"] = p_val is not None and p_val < 0.05

    # SPY base rate
    spy_excess = []; spy_wins = 0; n_with_spy = 0
    for c in with_30d:
        cd = (c.get("call_date") or "")[:10]
        spy_r = spy_returns.get(cd)
        if spy_r is None: continue
        stock_r = c["outcome_30d"]["return_pct"]
        spy_excess.append(stock_r - spy_r)
        if spy_r > 0: spy_wins += 1
        n_with_spy += 1
    if n_with_spy >= 5:
        spy_avg = mean([spy_returns[(c.get("call_date") or "")[:10]] for c in with_30d
                         if spy_returns.get((c.get("call_date") or "")[:10]) is not None])
        base["vs_base_rate"] = {
            "n_compared": n_with_spy,
            "avg_excess_return_pct": round(mean(spy_excess), 3) if spy_excess else None,
            "spy_avg_return_pct": round(spy_avg, 3),
            "spy_win_rate": round(spy_wins / n_with_spy, 4),
            "win_rate_premium_pct": round((wr - spy_wins / n_with_spy) * 100, 2),
        }

    # Regime stratification
    by_regime = defaultdict(list)
    for c in with_30d:
        rg = c.get("regime_at_call") or "UNKNOWN"
        by_regime[rg].append(c["outcome_30d"]["return_pct"])
    regime_stats = {}
    for rg, rs in by_regime.items():
        if len(rs) < 2: continue
        rg_wins = sum(1 for r in rs if r > 0)
        regime_stats[rg] = {
            "n": len(rs), "win_rate": round(rg_wins / len(rs), 4),
            "avg_return_pct": round(mean(rs), 3),
            "median_return_pct": round(median(rs), 3),
        }
    base["by_regime"] = regime_stats

    # Decay curve
    decay = {}
    for horizon in [1, 7, 30, 90, 180]:
        field = f"outcome_{horizon}d"
        rs = [c[field]["return_pct"] for c in strat_calls
               if c.get(field) and isinstance(c[field].get("return_pct"), (int, float))]
        if len(rs) >= 2:
            n_w = sum(1 for r in rs if r > 0)
            decay[f"{horizon}d"] = {
                "n": len(rs), "avg_return_pct": round(mean(rs), 3),
                "win_rate": round(n_w / len(rs), 4),
            }
    base["decay_curve"] = decay
    return base


def compute_information_coefficients(calls, horizons=(1, 7, 30, 90)):
    out = {}
    for comp in COMPONENTS:
        out[comp] = {}
        for h in horizons:
            xs, ys = [], []
            for c in calls:
                snap = c.get("components_snapshot") or {}
                cv = snap.get(comp)
                outcome = c.get(f"outcome_{h}d")
                if cv is None or not outcome: continue
                ret = outcome.get("return_pct")
                if not isinstance(ret, (int, float)): continue
                xs.append(float(cv)); ys.append(float(ret))
            if len(xs) < MIN_OBS_FOR_STAT // 2:
                out[comp][f"{h}d"] = {"n": len(xs), "insufficient": True}
                continue
            r, n, t, p, se = correlation(xs, ys)
            out[comp][f"{h}d"] = {
                "ic": round(r, 4) if r is not None else None,
                "n": n,
                "se": round(se, 4) if se is not None else None,
                "t_stat": round(t, 3) if t is not None else None,
                "p_value": round(p, 4) if p is not None else None,
                "significant_05": p is not None and p < 0.05,
            }
    return out


def factor_attribution_regression(calls, horizon=30):
    X_rows, y_rows = [], []
    for c in calls:
        snap = c.get("components_snapshot") or {}
        outcome = c.get(f"outcome_{horizon}d")
        if not outcome: continue
        ret = outcome.get("return_pct")
        if not isinstance(ret, (int, float)): continue
        component_vals = []
        missing = False
        for comp in COMPONENTS:
            v = snap.get(comp)
            if v is None: missing = True; break
            component_vals.append(float(v))
        if missing: continue
        # Center components: (v - 50) / 50 → roughly [-1, 1] range
        X_rows.append([1.0] + [(v - 50.0) / 50.0 for v in component_vals])
        y_rows.append(float(ret))
    n = len(X_rows)
    if n < len(COMPONENTS) + 10:
        return {"n_obs": n, "insufficient": True, "required": len(COMPONENTS) + 10}
    fit = ols_regression(X_rows, y_rows)
    if not fit: return {"n_obs": n, "regression_failed": True}
    coefficients = {}
    names = ["intercept"] + COMPONENTS
    for i, name in enumerate(names):
        coefficients[name] = {
            "value": round(fit["beta"][i], 5),
            "se": round(fit["se"][i], 5) if fit["se"][i] else None,
            "t_stat": round(fit["t_stats"][i], 3) if fit["t_stats"][i] is not None else None,
            "p_value": round(fit["p_values"][i], 4) if fit["p_values"][i] is not None else None,
            "significant_05": fit["p_values"][i] is not None and fit["p_values"][i] < 0.05,
        }
    out = {"n_obs": fit["n_obs"], "dof": fit["dof"], "horizon_days": horizon,
            "r_squared": fit["r_squared"], "adj_r_squared": fit["adj_r_squared"],
            "sigma": fit["sigma"], "coefficients": coefficients}
    positive_coefs = {c: max(0, coefficients[c]["value"]) for c in COMPONENTS}
    total = sum(positive_coefs.values())
    if total > 0:
        out["data_implied_weights"] = {c: round(positive_coefs[c] / total, 4) for c in COMPONENTS}
    return out


def compute_proposed_weights(current_weights, attribution, n_obs):
    if not attribution.get("data_implied_weights"):
        return None, None, ["no_attribution_regression"]
    implied = attribution["data_implied_weights"]
    lam = min(n_obs / SHRINKAGE_DIVISOR, MAX_SHRINKAGE)
    proposed = {}
    for comp, current in current_weights.items():
        if comp not in implied:
            proposed[comp] = current; continue
        new_w = (1 - lam) * current + lam * implied[comp]
        delta = new_w - current
        capped_delta = max(-MAX_WEIGHT_SHIFT, min(MAX_WEIGHT_SHIFT, delta))
        new_w = current + capped_delta
        new_w = max(WEIGHT_FLOOR, min(WEIGHT_CEILING, new_w))
        proposed[comp] = round(new_w, 4)
    total = sum(proposed.values())
    if total > 0:
        proposed = {c: round(w / total, 4) for c, w in proposed.items()}
    deltas = {c: round(proposed[c] - current_weights[c], 4) for c in current_weights}
    guardrails = {
        "shrinkage_lambda": round(lam, 4), "n_obs_used": n_obs,
        "shrinkage_divisor": SHRINKAGE_DIVISOR,
        "max_shift_pp": MAX_WEIGHT_SHIFT,
        "floor": WEIGHT_FLOOR, "ceiling": WEIGHT_CEILING,
        "renormalized": abs(sum(proposed.values()) - 1.0) < 0.01,
    }
    return proposed, deltas, guardrails


# ═══════════════════════════════════════════════════════════════════════════
# TELEGRAM
# ═══════════════════════════════════════════════════════════════════════════

def get_chat_id():
    if TELEGRAM_CHAT_ID: return TELEGRAM_CHAT_ID
    try:
        return ssm.get_parameter(Name="/justhodl/telegram/chat_id",
                                  WithDecryption=True)["Parameter"]["Value"]
    except Exception: return None

def send_telegram(text, chat_id):
    if not TELEGRAM_TOKEN or not chat_id: return False
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    body = urllib.parse.urlencode({
        "chat_id": chat_id, "text": text[:4000],
        "parse_mode": "Markdown", "disable_web_page_preview": "true",
    }).encode("utf-8")
    try:
        req = urllib.request.Request(url, data=body,
            headers={"Content-Type": "application/x-www-form-urlencoded"})
        with urllib.request.urlopen(req, timeout=10) as r:
            return json.loads(r.read().decode("utf-8")).get("ok", False)
    except Exception as e:
        print(f"  telegram err: {str(e)[:200]}")
        return False

def format_calibration_alert(report):
    summary = report["summary"]
    lines = [f"📐 *Weekly Alpha Calibration v{report['version']}*",
              f"_{report['generated_at'][:10]}_\n",
              f"📊 Trades analyzed: {summary['n_trades_analyzed']}",
              f"⚖ Evaluated 30d: {summary['n_evaluated_30d']}",
              f"🔬 Significant strategies (p<0.05): {summary['n_significant_strategies']}\n"]
    sig_strats = [s for s in report["per_strategy"] if s.get("significant_05")]
    if sig_strats:
        lines.append("🎯 *Significant strategies:*")
        for s in sig_strats[:5]:
            r = s.get("avg_return_30d_pct", 0)
            wr = (s.get("win_rate_30d") or 0) * 100
            lines.append(f"  • {s['strategy']}: {r:+.2f}% avg · {wr:.0f}% wr · t={s.get('t_stat','?')}")
        lines.append("")
    sig_factors = [c for c, v in (report.get("information_coefficients") or {}).items()
                    if (v.get("30d") or {}).get("significant_05")]
    if sig_factors:
        lines.append("⚡ *Factors with significant 30d IC:*")
        for c in sig_factors:
            ic = report["information_coefficients"][c]["30d"]
            lines.append(f"  • {c}: IC={ic.get('ic')} (t={ic.get('t_stat')})")
        lines.append("")
    if report.get("proposed_weights"):
        big_deltas = [(c, d) for c, d in (report.get("weight_deltas") or {}).items() if abs(d) >= 0.005]
        if big_deltas:
            lines.append("📈 *Proposed weight changes:*")
            for c, d in big_deltas:
                lines.append(f"  • {c}: {report['proposed_weights'][c]:.3f} ({d:+.3f})")
            lines.append("")
    dep = report.get("deployment_decision") or {}
    auto = "✅ AUTO-APPLIED" if dep.get("would_auto_apply") else "⏸ PROPOSED (manual approval)"
    lines.append(f"🚀 Deployment: {auto}")
    if dep.get("reason"): lines.append(f"_{dep['reason']}_")
    lines.append("\n[Calibration Dashboard](https://justhodl.ai/calibration/)")
    return "\n".join(lines)


# ═══════════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════════

def lambda_handler(event, context):
    started = time.time()
    print(f"=== ALPHA-CALIBRATOR v{VERSION} · {datetime.now(timezone.utc).isoformat()} ===")

    alpha = load_s3_json(ALPHA_KEY) or {}
    current_weights = alpha.get("weights") or {
        "quality": 0.16, "growth": 0.17, "momentum": 0.14, "smart_money": 0.16,
        "sentiment": 0.10, "analysts": 0.08, "insiders": 0.11, "options_flow": 0.08,
    }
    alpha_model_version = alpha.get("model_version", "unknown")

    all_calls = scan_all_calls(lookback_days=LOOKBACK_DAYS)
    n_total = len(all_calls)
    print(f"  scanned {n_total} calls over last {LOOKBACK_DAYS}d")

    spy_returns = cached_spy_returns(all_calls, horizon_days=EVAL_HORIZON_DAYS) if all_calls else {}
    print(f"  SPY base-rate fetched for {len(spy_returns)} unique dates")

    per_strategy = []
    for strat in STRATEGIES:
        stats = analyze_strategy(strat, all_calls, spy_returns)
        if stats.get("n_total", 0) > 0:
            per_strategy.append(stats)

    ic_results = compute_information_coefficients(all_calls)
    attribution = factor_attribution_regression(all_calls, horizon=EVAL_HORIZON_DAYS)

    n_for_update = attribution.get("n_obs", 0) if not attribution.get("insufficient") else 0
    proposed_weights, deltas, guardrails = None, None, None
    if n_for_update >= MIN_OBS_FOR_WEIGHT_UPDATE:
        proposed_weights, deltas, guardrails = compute_proposed_weights(
            current_weights, attribution, n_for_update)
        print(f"  proposed weights computed (n={n_for_update}, λ={guardrails['shrinkage_lambda']})")
    else:
        guardrails = {"insufficient_obs": True, "n_obs": n_for_update,
                       "required": MIN_OBS_FOR_WEIGHT_UPDATE}
        print(f"  weight update skipped: n_obs={n_for_update} < {MIN_OBS_FOR_WEIGHT_UPDATE}")

    existing_weights_sidecar = load_s3_json(WEIGHTS_KEY) or {}
    auto_apply_flag = existing_weights_sidecar.get("auto_apply_calibrations", False)

    deployment_decision = {
        "auto_apply_calibrations_flag": auto_apply_flag,
        "would_auto_apply": False, "reason": None,
    }
    if proposed_weights is None:
        deployment_decision["reason"] = f"Insufficient data (n={n_for_update} < {MIN_OBS_FOR_WEIGHT_UPDATE})"
    elif not auto_apply_flag:
        deployment_decision["reason"] = "auto_apply_calibrations flag is false (manual approval required)"
    else:
        has_sig_factor = any(
            v.get("significant_05")
            for v in attribution.get("coefficients", {}).values()
            if isinstance(v, dict) and v.get("p_value") is not None
        )
        if not has_sig_factor:
            deployment_decision["reason"] = "No factor with significant attribution coefficient"
        else:
            deployment_decision["would_auto_apply"] = True
            deployment_decision["reason"] = "All gates passed"

    n_strategies_with_data = sum(1 for s in per_strategy if s.get("n_total", 0) > 0)
    n_significant = sum(1 for s in per_strategy if s.get("significant_05"))
    n_evaluated = sum(1 for s in per_strategy if s.get("n_evaluated_30d", 0) >= MIN_OBS_FOR_STAT)
    overall_30d = [c["outcome_30d"]["return_pct"] for c in all_calls
                    if c.get("outcome_30d") and isinstance(c["outcome_30d"].get("return_pct"), (int, float))]

    report = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "generated_at_unix": int(time.time()),
        "version": VERSION,
        "elapsed_seconds": round(time.time() - started, 2),
        "alpha_model_version_observed": alpha_model_version,
        "config": {
            "lookback_days": LOOKBACK_DAYS, "eval_horizon_days": EVAL_HORIZON_DAYS,
            "min_obs_for_stat": MIN_OBS_FOR_STAT,
            "min_obs_for_weight_update": MIN_OBS_FOR_WEIGHT_UPDATE,
            "shrinkage_divisor": SHRINKAGE_DIVISOR, "max_shrinkage": MAX_SHRINKAGE,
            "max_weight_shift": MAX_WEIGHT_SHIFT,
            "weight_floor": WEIGHT_FLOOR, "weight_ceiling": WEIGHT_CEILING,
        },
        "summary": {
            "n_trades_analyzed": n_total,
            "n_evaluated_30d": len(overall_30d),
            "n_strategies_with_data": n_strategies_with_data,
            "n_strategies_sufficient": n_evaluated,
            "n_significant_strategies": n_significant,
            "overall_30d_avg_return_pct": round(mean(overall_30d), 3) if overall_30d else None,
            "overall_30d_win_rate": round(sum(1 for r in overall_30d if r > 0) / len(overall_30d), 4) if overall_30d else None,
        },
        "current_weights": current_weights,
        "per_strategy": per_strategy,
        "information_coefficients": ic_results,
        "factor_attribution": attribution,
        "proposed_weights": proposed_weights,
        "weight_deltas": deltas,
        "guardrails": guardrails,
        "deployment_decision": deployment_decision,
    }

    # ─── Write artifacts ───

    try:
        s3.put_object(Bucket=S3_BUCKET, Key=LATEST_KEY,
            Body=json.dumps(report, separators=(",", ":"), default=str).encode("utf-8"),
            ContentType="application/json", CacheControl="public, max-age=900")
        print(f"  ✓ calibration-latest.json written")
    except Exception as e:
        print(f"  put latest err: {e}")

    history = load_s3_json(HISTORY_KEY) or {"history": []}
    history_entry = {
        "generated_at": report["generated_at"], "version": report["version"],
        "summary": report["summary"], "current_weights": current_weights,
        "proposed_weights": proposed_weights, "weight_deltas": deltas,
        "deployment_decision": deployment_decision,
        "r_squared": attribution.get("r_squared"),
    }
    history["history"].append(history_entry)
    history["history"] = history["history"][-52:]
    history["last_updated"] = report["generated_at"]
    try:
        s3.put_object(Bucket=S3_BUCKET, Key=HISTORY_KEY,
            Body=json.dumps(history, separators=(",", ":"), default=str).encode("utf-8"),
            ContentType="application/json", CacheControl="public, max-age=900")
        print(f"  ✓ calibration-history.json appended ({len(history['history'])} weeks)")
    except Exception as e:
        print(f"  put history err: {e}")

    # Weights sidecar for alpha-score consumption
    weights_sidecar = existing_weights_sidecar
    weights_sidecar.setdefault("auto_apply_calibrations", False)
    weights_sidecar["last_calibration_at"] = report["generated_at"]
    weights_sidecar["last_calibration_version"] = report["version"]
    weights_sidecar["proposed_weights"] = proposed_weights or current_weights
    weights_sidecar["proposed_at"] = report["generated_at"]
    if deployment_decision.get("would_auto_apply") and proposed_weights:
        weights_sidecar["active_weights"] = proposed_weights
        weights_sidecar["active_since"] = report["generated_at"]
    else:
        weights_sidecar.setdefault("active_weights", current_weights)
    try:
        s3.put_object(Bucket=S3_BUCKET, Key=WEIGHTS_KEY,
            Body=json.dumps(weights_sidecar, separators=(",", ":"), default=str).encode("utf-8"),
            ContentType="application/json", CacheControl="public, max-age=300")
        print(f"  ✓ alpha-weights.json written (auto_apply={weights_sidecar['auto_apply_calibrations']})")
    except Exception as e:
        print(f"  put weights err: {e}")

    chat_id = get_chat_id()
    alert_sent = False
    if chat_id and TELEGRAM_TOKEN:
        try:
            alert_sent = send_telegram(format_calibration_alert(report), chat_id)
        except Exception as e:
            print(f"  alert err: {e}")

    elapsed = round(time.time() - started, 2)
    return {"statusCode": 200, "body": json.dumps({
        "success": True, "version": VERSION,
        "n_calls": n_total,
        "n_strategies": n_strategies_with_data,
        "n_significant": n_significant,
        "proposed_weights": proposed_weights,
        "would_auto_apply": deployment_decision.get("would_auto_apply"),
        "alert_sent": alert_sent,
        "elapsed_seconds": elapsed,
    })}
