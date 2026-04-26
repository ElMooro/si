"""
justhodl-regime-anomaly — Phase 9.2 of system-improvement plan.

Two complementary models running on the same EventBridge schedule:

  1. HIDDEN MARKOV MODEL — fits a 4-state HMM on the KA Index time
     series (training on data/archive/intelligence/* JSON files +
     DynamoDB justhodl-signals + DynamoDB justhodl-outcomes for
     state labels). Returns probabilistic state membership +
     transition probabilities. Maps states to regime labels:
       state 0 → EXPANSION
       state 1 → LATE_CYCLE
       state 2 → CONTRACTION
       state 3 → CRISIS

  2. ANOMALY DETECTION — flags unusual signal behavior using:
       a) Per-signal Mahalanobis distance vs 90-day distribution
       b) Cross-asset rolling 60d correlation matrix Frobenius norm
          delta vs its own 1-year history (catches structural breaks
          like Aug 2024 yen carry unwind, 2022 stock-bond decoupling)

Output:
  s3://justhodl-dashboard-live/data/regime-anomaly.json

Schedule: rate(1 day) at 14:00 UTC (after morning data settles)

Schema:
  hmm.current_state            (0-3)
  hmm.state_label              (string)
  hmm.state_probabilities      ([p0, p1, p2, p3])
  hmm.transition_matrix        (4x4)
  hmm.expected_regime_duration (days)
  hmm.training_n               (samples used)
  hmm.fit_quality              (log-likelihood / n)
  hmm.is_warming_up            (bool — set True if training_n < 60)

  anomaly.per_signal           ({signal_name: {distance, percentile, is_anomaly}})
  anomaly.correlation_break    ({frobenius_norm, z_score_1y, is_break, top_breaks})
  anomaly.composite_anomaly_score (0-100)

  generated_at, training_window, fit_method
"""
import json
import os
import time
import math
from datetime import datetime, timezone, timedelta
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
import boto3
from boto3.dynamodb.conditions import Attr

try:
    from ka_aliases import add_ka_aliases
except Exception:
    def add_ka_aliases(obj, **_kwargs):
        return obj

REGION = "us-east-1"
S3_BUCKET = "justhodl-dashboard-live"
S3_KEY_OUT = "data/regime-anomaly.json"
S3_KEY_HISTORICAL = "intelligence-report.json"

s3 = boto3.client("s3", region_name=REGION)
dynamodb = boto3.resource("dynamodb", region_name=REGION)

# ─────────────────────────────────────────────────────────────────────
# Lightweight HMM implementation (no scipy/numpy in Lambda by default)
#
# We use Gaussian emissions with 4 hidden states. Training via
# Baum-Welch (EM) algorithm. All in pure Python so we don't have
# to package scipy in the Lambda zip.
#
# Inputs: 1-D time series of KA Index scores
# Output: state probabilities + transition matrix
# ─────────────────────────────────────────────────────────────────────

def _normal_pdf(x, mu, sigma):
    """Gaussian probability density function."""
    if sigma <= 0:
        sigma = 1e-6
    coef = 1.0 / (sigma * math.sqrt(2.0 * math.pi))
    exponent = -0.5 * ((x - mu) / sigma) ** 2
    # clip to avoid underflow
    return max(coef * math.exp(max(exponent, -700)), 1e-300)


def _normalize(row):
    s = sum(row)
    if s <= 0:
        return [1.0 / len(row)] * len(row)
    return [x / s for x in row]


def _matmul_row(v, M):
    """row-vector times matrix"""
    n = len(M[0])
    out = [0.0] * n
    for i, vi in enumerate(v):
        for j in range(n):
            out[j] += vi * M[i][j]
    return out


def hmm_baum_welch(observations, n_states=4, max_iter=30, tol=1e-4, prior_strength=2.0):
    """Fit a Gaussian-emission HMM via Baum-Welch EM algorithm.

    Includes a Dirichlet prior on the transition matrix to prevent
    state collapse on small/uniform datasets. Without the prior,
    when the data is sparse and similar in value, EM converges to
    a degenerate solution where one state captures all probability.

    The prior_strength parameter (default 2.0) acts as pseudo-counts
    on every transition. With 4 states and prior_strength=2, every
    transition starts with 2 pseudo-observations, ensuring all
    states retain some probability mass even if no actual transitions
    are observed.

    Returns:
      pi          initial state probabilities
      A           n_states x n_states transition matrix
      mu, sigma   per-state Gaussian emission params
      log_lik     final log-likelihood
      gamma       n_obs x n_states posterior state probabilities
    """
    n = len(observations)
    if n < n_states * 3:
        # Not enough data — return uniform-ish
        pi = [1.0 / n_states] * n_states
        A = [[1.0 / n_states] * n_states for _ in range(n_states)]
        # initialize means by quantiles
        sorted_obs = sorted(observations) if observations else [0]
        if sorted_obs:
            mu = [sorted_obs[i * len(sorted_obs) // n_states] for i in range(n_states)]
        else:
            mu = list(range(n_states))
        sigma = [1.0] * n_states
        return pi, A, mu, sigma, 0.0, [pi[:] for _ in range(n)]

    # Initialize: split data into n_states quantile groups, use means + stds
    sorted_obs = sorted(observations)
    mu = []
    sigma = []
    for k in range(n_states):
        lo = k * len(sorted_obs) // n_states
        hi = (k + 1) * len(sorted_obs) // n_states
        chunk = sorted_obs[lo:hi] if lo < hi else [sorted_obs[lo] if lo < len(sorted_obs) else 50]
        m = sum(chunk) / len(chunk)
        v = sum((x - m) ** 2 for x in chunk) / len(chunk) if len(chunk) > 1 else 1.0
        mu.append(m)
        sigma.append(max(v ** 0.5, 1.0))

    pi = [1.0 / n_states] * n_states
    # Slightly diagonal-biased initial transition matrix
    A = [[0.85 if i == j else 0.15 / (n_states - 1) for j in range(n_states)] for i in range(n_states)]

    prev_log_lik = -float('inf')
    for iteration in range(max_iter):
        # E-step: forward-backward
        # alpha[t][s] = P(o_1..o_t, q_t=s)
        alpha = [[0.0] * n_states for _ in range(n)]
        c = [0.0] * n  # scaling factors

        for s in range(n_states):
            alpha[0][s] = pi[s] * _normal_pdf(observations[0], mu[s], sigma[s])
        c[0] = sum(alpha[0]) or 1e-300
        alpha[0] = [a / c[0] for a in alpha[0]]

        for t in range(1, n):
            for s in range(n_states):
                alpha[t][s] = sum(alpha[t - 1][prev] * A[prev][s] for prev in range(n_states)) \
                              * _normal_pdf(observations[t], mu[s], sigma[s])
            c[t] = sum(alpha[t]) or 1e-300
            alpha[t] = [a / c[t] for a in alpha[t]]

        # beta[t][s] = P(o_{t+1}..o_T | q_t=s) (scaled)
        beta = [[1.0] * n_states for _ in range(n)]
        beta[n - 1] = [1.0 / c[n - 1]] * n_states
        for t in range(n - 2, -1, -1):
            for s in range(n_states):
                beta[t][s] = sum(
                    A[s][next_s] *
                    _normal_pdf(observations[t + 1], mu[next_s], sigma[next_s]) *
                    beta[t + 1][next_s]
                    for next_s in range(n_states)
                )
            beta[t] = [b / c[t] for b in beta[t]]

        # gamma[t][s] = P(q_t=s | obs)
        gamma = [[alpha[t][s] * beta[t][s] for s in range(n_states)] for t in range(n)]
        # normalize per t (should already be normalized via scaling)
        gamma = [_normalize(g) for g in gamma]

        # xi[t][i][j] = P(q_t=i, q_{t+1}=j | obs)
        # M-step: re-estimate
        new_pi = gamma[0][:]

        new_A = [[0.0] * n_states for _ in range(n_states)]
        for t in range(n - 1):
            denom = sum(gamma[t][i] for i in range(n_states))
            if denom == 0:
                continue
            for i in range(n_states):
                for j in range(n_states):
                    xi_tij = (alpha[t][i] * A[i][j]
                              * _normal_pdf(observations[t + 1], mu[j], sigma[j])
                              * beta[t + 1][j])
                    new_A[i][j] += xi_tij
        # Apply Dirichlet prior — adds pseudo-counts to every transition
        # so no state can be fully eliminated
        for i in range(n_states):
            for j in range(n_states):
                new_A[i][j] += prior_strength / n_states
        # Normalize rows of new_A
        for i in range(n_states):
            row_sum = sum(new_A[i]) or 1e-300
            new_A[i] = [x / row_sum for x in new_A[i]]

        # Re-estimate emissions with floor on sigma to prevent collapse
        new_mu = [0.0] * n_states
        new_sigma = [0.0] * n_states

        # Compute global std as floor reference
        global_mean = sum(observations) / n
        global_var = sum((x - global_mean) ** 2 for x in observations) / n
        global_std = max(global_var ** 0.5, 1.0)
        sigma_floor = max(global_std * 0.1, 0.5)  # at least 10% of global std

        for s in range(n_states):
            denom = sum(gamma[t][s] for t in range(n))
            # Apply pseudo-count for emission too
            denom_priored = denom + 0.5
            if denom_priored == 0:
                new_mu[s] = mu[s]
                new_sigma[s] = sigma[s]
                continue
            new_mu[s] = (sum(gamma[t][s] * observations[t] for t in range(n))
                         + 0.5 * mu[s]) / denom_priored
            v = (sum(gamma[t][s] * (observations[t] - new_mu[s]) ** 2 for t in range(n))
                 + 0.5 * sigma[s] ** 2) / denom_priored
            new_sigma[s] = max(math.sqrt(v), sigma_floor)

        # Compute log-likelihood
        log_lik = sum(math.log(c_t) for c_t in c if c_t > 0)

        pi = new_pi
        A = new_A
        mu = new_mu
        sigma = new_sigma

        if abs(log_lik - prev_log_lik) < tol:
            break
        prev_log_lik = log_lik

    return pi, A, mu, sigma, log_lik, gamma


def map_states_to_regimes(mu, sigma, n_states=4):
    """Sort state indices by mu (mean) and assign labels.
    Convention: KA Index higher = more risk-on, lower = more crisis.
    KA Index ranges 0-100, with higher meaning more risk taking.

    Actually our convention here is *risk regime*: KA Index appears to
    be a stress-style score where higher = more stress. We map:
      lowest mu  → EXPANSION  (low stress)
      next       → LATE_CYCLE
      next       → CONTRACTION
      highest    → CRISIS
    """
    # Sort state indices by mean
    sorted_states = sorted(range(n_states), key=lambda s: mu[s])
    labels = ["EXPANSION", "LATE_CYCLE", "CONTRACTION", "CRISIS"]
    state_to_label = {}
    for rank, state_idx in enumerate(sorted_states):
        state_to_label[state_idx] = labels[rank] if rank < len(labels) else f"STATE_{rank}"
    return state_to_label


# ─────────────────────────────────────────────────────────────────────
# Anomaly detection layer
# ─────────────────────────────────────────────────────────────────────

def mahalanobis_per_signal(signal_history):
    """For each signal, compute how many standard deviations the latest
    reading is from the 90-day mean. Returns dict.

    signal_history: {signal_name: [recent values, oldest first]}
    """
    out = {}
    for name, vals in signal_history.items():
        if not vals or len(vals) < 10:
            continue
        # use last 90 days as distribution
        dist = vals[-90:] if len(vals) > 90 else vals
        if len(dist) < 5:
            continue
        latest = dist[-1]
        # historical sample (excluding latest)
        sample = dist[:-1]
        if not sample:
            continue
        m = sum(sample) / len(sample)
        v = sum((x - m) ** 2 for x in sample) / len(sample)
        std = max(v ** 0.5, 1e-6)
        z = (latest - m) / std
        # percentile of |z|
        pct = sum(1 for x in sample if abs((x - m) / std) <= abs(z)) / len(sample) * 100
        out[name] = {
            "z_score": round(z, 2),
            "percentile": round(pct, 1),
            "is_anomaly": abs(z) > 2.0,
            "is_extreme": abs(z) > 3.0,
            "latest": latest,
            "mean_90d": round(m, 3),
            "std_90d": round(std, 3),
            "n_samples": len(dist),
        }
    return out


def correlation_break_detector(price_series_dict, window_days=60):
    """Compute rolling 60d correlation matrix across instruments and
    detect Frobenius norm shifts vs 1Y history.

    price_series_dict: {ticker: [returns over time, oldest first]}
    Returns:
      frobenius_norm_current
      frobenius_norm_z_score_1y (vs prior 1Y of Frobenius norms)
      is_break (True if z > 2)
      top_break_pairs (top correlation changes vs prior 60d window)
    """
    tickers = list(price_series_dict.keys())
    if len(tickers) < 4:
        return {"insufficient_data": True}

    # Find common length
    min_len = min(len(price_series_dict[t]) for t in tickers)
    if min_len < window_days * 2:
        return {"insufficient_data": True, "min_len": min_len}

    # Truncate all to common length
    series = {t: price_series_dict[t][-min_len:] for t in tickers}

    def correlation(a, b):
        ma = sum(a) / len(a)
        mb = sum(b) / len(b)
        cov = sum((a[i] - ma) * (b[i] - mb) for i in range(len(a))) / len(a)
        va = sum((x - ma) ** 2 for x in a) / len(a)
        vb = sum((x - mb) ** 2 for x in b) / len(b)
        denom = (va ** 0.5) * (vb ** 0.5)
        if denom < 1e-9:
            return 0
        return cov / denom

    def correlation_matrix(window_data):
        n = len(tickers)
        M = [[1.0] * n for _ in range(n)]
        for i in range(n):
            for j in range(i + 1, n):
                c = correlation(window_data[tickers[i]], window_data[tickers[j]])
                M[i][j] = c
                M[j][i] = c
        return M

    def frobenius_norm(M):
        return math.sqrt(sum(M[i][j] ** 2 for i in range(len(M)) for j in range(len(M))))

    # Current 60d window
    current = {t: series[t][-window_days:] for t in tickers}
    M_current = correlation_matrix(current)
    norm_current = frobenius_norm(M_current)

    # Prior 60d window (just before current)
    prior_start = -2 * window_days
    prior_end = -window_days
    prior = {t: series[t][prior_start:prior_end] for t in tickers}
    M_prior = correlation_matrix(prior)

    # Build distribution of Frobenius norms over last 1Y of rolling windows
    norms = []
    for offset in range(window_days, min(min_len - window_days, 365), 5):
        window = {t: series[t][-offset - window_days:-offset if offset > 0 else None] for t in tickers}
        try:
            M_w = correlation_matrix(window)
            norms.append(frobenius_norm(M_w))
        except Exception:
            continue

    # z-score
    z = 0
    if norms:
        m = sum(norms) / len(norms)
        v = sum((x - m) ** 2 for x in norms) / len(norms)
        std = max(v ** 0.5, 1e-6)
        z = (norm_current - m) / std

    # Top correlation breaks
    breaks = []
    for i in range(len(tickers)):
        for j in range(i + 1, len(tickers)):
            delta = M_current[i][j] - M_prior[i][j]
            breaks.append({
                "pair": f"{tickers[i]}/{tickers[j]}",
                "delta": round(delta, 3),
                "current": round(M_current[i][j], 3),
                "prior": round(M_prior[i][j], 3),
            })
    breaks.sort(key=lambda x: abs(x["delta"]), reverse=True)

    return {
        "frobenius_norm_current": round(norm_current, 3),
        "frobenius_norm_z_score_1y": round(z, 2),
        "is_break": abs(z) > 2.0,
        "is_severe_break": abs(z) > 3.0,
        "n_norm_samples": len(norms),
        "top_break_pairs": breaks[:8],
        "tickers": tickers,
    }


# ─────────────────────────────────────────────────────────────────────
# Data loading
# ─────────────────────────────────────────────────────────────────────

def load_ka_index_history(days_back=365):
    """Pull KA Index time series from S3 archive and DynamoDB outcomes.

    Strategy:
      1. List archive/intelligence/ keys
      2. Read each, extract ka_index/khalid_index from scores
      3. Drop entries where score == 0 (these are the result of a
         producer Lambda bug Mar 9 → Apr 24 that wrote 0 on every
         downstream computation failure; including them poisons HMM
         training with a fake bimodal 0/N distribution)
      4. Sort by date

    Phase 9.4: parallelize S3 reads (was sequential, capped at 200) and
    bump cap to 2000 to accumulate training data faster. With 16 worker
    threads, reading 2000 archives takes ~6s on Lambda. 240s timeout
    leaves plenty of margin.
    """
    series = []  # list of (date_str, ka_index_score)
    n_zero_filtered = 0
    n_no_score = 0
    n_read_errors = 0

    cutoff_date = datetime.now(timezone.utc) - timedelta(days=days_back)

    # 1. List recent keys
    paginator = s3.get_paginator("list_objects_v2")
    keys = []
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix="archive/intelligence/"):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if obj["LastModified"].replace(tzinfo=timezone.utc) >= cutoff_date:
                keys.append(key)

    print(f"[regime-anomaly] found {len(keys)} archive keys in last {days_back}d")

    # 2. Read in parallel (Phase 9.4: was sequential cap=200, now parallel cap=2000)
    READ_CAP = 2000
    keys_to_read = keys[-READ_CAP:]

    def read_archive(key):
        try:
            obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
            data = json.loads(obj["Body"].read())
            scores = data.get("scores", {})
            score = scores.get("ka_index") or scores.get("khalid_index")
            generated = data.get("generated_at") or data.get("date")
            return key, score, generated, None
        except Exception as e:
            return key, None, None, str(e)

    with ThreadPoolExecutor(max_workers=16) as ex:
        futures = [ex.submit(read_archive, k) for k in keys_to_read]
        for fut in as_completed(futures):
            key, score, generated, err = fut.result()
            if err:
                n_read_errors += 1
                continue
            if score is None:
                n_no_score += 1
                continue
            score_f = float(score)
            if score_f == 0:
                n_zero_filtered += 1
                continue
            if generated:
                series.append((generated, score_f))

    print(
        f"[regime-anomaly] loader: read={len(keys_to_read)} "
        f"zeros_filtered={n_zero_filtered} "
        f"no_score={n_no_score} "
        f"errors={n_read_errors} "
        f"valid={len(series)}"
    )

    # 3. Also include current intelligence-report.json (latest snapshot)
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=S3_KEY_HISTORICAL)
        data = json.loads(obj["Body"].read())
        scores = data.get("scores", {})
        score = scores.get("ka_index") or scores.get("khalid_index")
        generated = data.get("generated_at")
        if score is not None and float(score) != 0 and generated:
            series.append((generated, float(score)))
    except Exception as e:
        print(f"[load] latest report: {e}")

    # 4. Dedupe and sort
    series_dict = {}
    for date_str, score in series:
        try:
            t = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
            series_dict[t] = score
        except Exception:
            continue
    sorted_series = sorted(series_dict.items())
    return sorted_series


def load_signal_history():
    """Load recent values for major signals from latest intelligence-report.json
    plus archives. Returns dict {signal_name: [values]}.

    For simplicity, we extract from intelligence-report scores object.
    """
    series_by_signal = defaultdict(list)
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=S3_KEY_HISTORICAL)
        data = json.loads(obj["Body"].read())
        scores = data.get("scores", {})
        for k, v in scores.items():
            if isinstance(v, (int, float)):
                series_by_signal[k].append(float(v))
    except Exception as e:
        print(f"[load signals] {e}")

    # For now, single-snapshot — 90-day full history is what the
    # next iteration will pull from archive. This baseline gives
    # the framework.
    return dict(series_by_signal)


# ─────────────────────────────────────────────────────────────────────
# Main handler
# ─────────────────────────────────────────────────────────────────────

def lambda_handler(event, context):
    t0 = time.time()
    print(f"[regime-anomaly] starting at {datetime.now(timezone.utc).isoformat()}")

    # 1. Load KA Index time series
    history = load_ka_index_history(days_back=365)
    observations = [v for _, v in history]
    print(f"[regime-anomaly] loaded {len(observations)} KA Index observations")

    # 2. HMM fitting
    hmm_result = {}
    if len(observations) >= 12:  # absolute minimum
        try:
            pi, A, mu, sigma, log_lik, gamma = hmm_baum_welch(
                observations, n_states=4, max_iter=30
            )
            state_to_label = map_states_to_regimes(mu, sigma)

            # Current state probabilities (from last gamma)
            current_probs = gamma[-1] if gamma else pi
            current_state = max(range(len(current_probs)), key=lambda s: current_probs[s])

            # Expected duration in current state from transition matrix
            self_prob = A[current_state][current_state]
            expected_duration = (1.0 / (1.0 - self_prob)) if self_prob < 1 else 999

            hmm_result = {
                "current_state": current_state,
                "state_label": state_to_label[current_state],
                "state_probabilities": {state_to_label[s]: round(current_probs[s], 3) for s in range(4)},
                "transition_matrix": {
                    state_to_label[i]: {state_to_label[j]: round(A[i][j], 3) for j in range(4)}
                    for i in range(4)
                },
                "state_means": {state_to_label[s]: round(mu[s], 2) for s in range(4)},
                "state_stds": {state_to_label[s]: round(sigma[s], 2) for s in range(4)},
                "expected_regime_duration_obs": round(expected_duration, 1),
                "training_n": len(observations),
                "fit_log_likelihood": round(log_lik, 2),
                "fit_avg_loglik_per_obs": round(log_lik / len(observations), 3) if observations else 0,
                "is_warming_up": len(observations) < 60,
            }
        except Exception as e:
            hmm_result = {"error": f"HMM fit failed: {e}", "training_n": len(observations)}
    else:
        hmm_result = {
            "is_warming_up": True,
            "training_n": len(observations),
            "message": f"Only {len(observations)} samples, need >=12 to fit",
        }

    # 3. Anomaly detection
    signal_history = load_signal_history()
    per_signal = mahalanobis_per_signal(signal_history)
    n_anomalies = sum(1 for v in per_signal.values() if v.get("is_anomaly"))
    n_extremes = sum(1 for v in per_signal.values() if v.get("is_extreme"))

    # 4. Composite anomaly score (0-100)
    composite_anomaly = min(100, n_anomalies * 15 + n_extremes * 25)

    # 5. Build report
    # Phase 9.4 — training_window now includes per-day observation count
    # + ETA to "live" status (60 obs threshold), so the frontend can
    # show progress instead of just a binary warming_up flag.
    obs_per_day = {}
    for t, _ in history:
        d = t.date().isoformat()
        obs_per_day[d] = obs_per_day.get(d, 0) + 1
    obs_count_history = sorted(obs_per_day.items())
    n_obs = len(observations)
    target = 60
    # Median obs/day from the last 7 days (fallback to overall average)
    recent_daily = [c for _, c in obs_count_history[-7:]]
    if recent_daily:
        recent_daily.sort()
        median_per_day = recent_daily[len(recent_daily) // 2]
    else:
        median_per_day = 0
    days_to_live = (
        max(0, math.ceil((target - n_obs) / median_per_day))
        if median_per_day > 0 and n_obs < target
        else 0
    )
    eta_iso = None
    if days_to_live > 0:
        eta_iso = (datetime.now(timezone.utc) + timedelta(days=days_to_live)).isoformat()

    report = {
        "schema_version": "1.1",  # bumped: added training_progress
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "elapsed_sec": round(time.time() - t0, 2),
        "hmm": hmm_result,
        "anomaly": {
            "per_signal": per_signal,
            "n_anomalies": n_anomalies,
            "n_extremes": n_extremes,
            "composite_anomaly_score": composite_anomaly,
        },
        "training_window": {
            "ka_index_observations": len(observations),
            "signal_count": len(signal_history),
            "earliest": history[0][0].isoformat() if history else None,
            "latest": history[-1][0].isoformat() if history else None,
        },
        # Phase 9.4 — training progress (visible on regime.html)
        "training_progress": {
            "n_obs": n_obs,
            "target_n_obs": target,
            "pct_complete": round(100.0 * n_obs / target, 1) if target else None,
            "is_warming_up": n_obs < target,
            "median_obs_per_day_7d": median_per_day,
            "days_to_target": days_to_live,
            "eta_live_iso": eta_iso,
            "obs_per_day": obs_count_history[-30:],  # last 30 days for chart
        },
        "fit_method": "Baum-Welch EM (4-state Gaussian HMM, pure-python)",
    }

    report = add_ka_aliases(report)
    body = json.dumps(report, default=str, indent=2)
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=S3_KEY_OUT,
        Body=body,
        ContentType="application/json",
        CacheControl="max-age=300",
    )

    summary = {
        "status": "ok",
        "elapsed_sec": round(time.time() - t0, 2),
        "ka_index_n_obs": len(observations),
        "hmm_state": hmm_result.get("state_label"),
        "n_anomalies": n_anomalies,
        "anomaly_score": composite_anomaly,
        "s3_key": S3_KEY_OUT,
    }
    print(f"[regime-anomaly] done: {summary}")
    return {"statusCode": 200, "body": json.dumps(summary)}
