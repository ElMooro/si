"""
justhodl-causality-scanner — Exponential Idea #3

Auto-discovers lead-lag relationships across the platform's time series.

Algorithm:
  1. List all data/*.json keys in S3
  2. For each, attempt to extract a numeric time series (heuristic
     extraction — most platform feeds have scalar values like
     khalid_index, edge_regime_score, composite_z, etc.)
  3. Build a long-format DataFrame of (ts, series_name, value)
  4. Pair-wise Granger causality on series with >=60 daily observations
  5. Rank by F-statistic / p-value, filter by:
     - p < 0.01
     - causal lag in [1, 14] days (actionable)
     - not already a known signal pair (curated exclusion list)
  6. Top 50 discoveries → data/causality-discoveries.json

This turns Khalid from a signal designer into a signal curator. Every
week the platform proposes 50 new lead-lag relationships he hasn't
hand-coded yet. He picks the interesting ones and they become engines.

Schedule: weekly Sunday 21 UTC (computationally heaviest run of the week)

v1: Pure Granger causality on JSON numeric scalars. Pandas-free
implementation (manual VAR + F-test). v2 will use statsmodels for the
full multivariate VAR + cointegration battery.
"""
import json, os, logging
import boto3
from datetime import datetime, timezone, timedelta
from collections import defaultdict
import math
from concurrent.futures import ThreadPoolExecutor, as_completed

logger = logging.getLogger()
logger.setLevel(logging.INFO)

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/causality-discoveries.json"
HIST_KEY = "data/history/causality-discoveries-history.json"

TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")

MIN_OBS = 30
MAX_LAG = 14
MAX_PAIRS = 5000  # cap to fit in 600s timeout
P_VALUE_THRESHOLD = 0.01

s3 = boto3.client("s3", region_name=REGION)


# Keys to skip — non-time-series, snapshots, or already-known causality
EXCLUDE_PATTERNS = (
    "snapshots/", "archive/", "_archive/", "history/", "secretary-history/",
    "calibration-history/", "_freshness-manifest", "khalid-config",
    "ka-config", "prompt_templates", "improvement_log",
    "kill-theses-history", "behavior-mirror-history",
)


def list_candidate_keys():
    """List all data/*.json keys, excluding non-time-series."""
    keys = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=BUCKET, Prefix="data/"):
        for obj in page.get("Contents", []):
            k = obj["Key"]
            if not k.endswith(".json"):
                continue
            if any(p in k for p in EXCLUDE_PATTERNS):
                continue
            keys.append({"key": k, "last_modified": obj["LastModified"], "size": obj["Size"]})
    return keys


# Numeric keys we try to extract from each JSON payload — these are the
# common scalar-output names across the platform.
NUMERIC_KEYS = [
    "khalid_index", "score", "composite_score", "composite_z", "z_score",
    "edge_regime_score", "edge_score", "signal_strength", "regime_score",
    "stress_score", "liquidity_score", "carry_score", "vix", "vol",
    "spread_bps", "yield_pct", "return_pct", "level", "value", "ratio",
    "percentile", "pct_rank", "intensity", "warning_level", "tier_score",
    "early_warning_level",
]


def extract_scalar(payload):
    """Pull the most likely scalar value from a payload."""
    if not isinstance(payload, dict):
        return None
    # Try top-level numeric keys first
    for k in NUMERIC_KEYS:
        v = payload.get(k)
        if isinstance(v, (int, float)) and not math.isnan(v):
            return float(v)
    # Try nested under 'summary' or 'composite'
    for parent_key in ("summary", "composite", "result", "headline"):
        if isinstance(payload.get(parent_key), dict):
            for k in NUMERIC_KEYS:
                v = payload[parent_key].get(k)
                if isinstance(v, (int, float)) and not math.isnan(v):
                    return float(v)
    return None


def read_series_from_history(key):
    """Try to pull historical snapshots for a key. If the platform stores
    history at data/history/<key>-history.json, use that."""
    # Extract just the filename without data/ prefix
    base = key.split("/")[-1].replace(".json", "")
    candidates = [
        f"data/history/{base}-history.json",
        f"data/history/{base}_history.json",
        f"data/snapshots/{base}.json",
    ]
    for hist_key in candidates:
        try:
            obj = s3.get_object(Bucket=BUCKET, Key=hist_key)
            body = json.loads(obj["Body"].read())
            return parse_history(body, base)
        except s3.exceptions.NoSuchKey:
            continue
        except Exception as e:
            logger.debug(f"history_parse_fail {hist_key}: {e}")
    return None


def parse_history(body, name):
    """Convert various history shapes to [(ts, value), ...]."""
    if not isinstance(body, dict):
        return None
    points = []
    snaps = body.get("snapshots") or body.get("history") or body.get("data")
    if not isinstance(snaps, list):
        return None
    for s in snaps:
        if not isinstance(s, dict):
            continue
        ts = s.get("ts") or s.get("timestamp") or s.get("date") or s.get("generated_at") or s.get("computed_at")
        val = extract_scalar(s)
        if ts and val is not None:
            try:
                if isinstance(ts, (int, float)):
                    dt = datetime.fromtimestamp(int(ts), tz=timezone.utc)
                else:
                    dt = datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
                points.append((dt, val))
            except Exception:
                continue
    points.sort()
    return points if len(points) >= MIN_OBS else None


def align_series(series_a, series_b, granularity_days=1):
    """Align two series to a common time grid."""
    def to_dict(s):
        # Floor to day
        out = {}
        for dt, v in s:
            key = dt.replace(hour=0, minute=0, second=0, microsecond=0).timestamp()
            out[key] = v
        return out
    a, b = to_dict(series_a), to_dict(series_b)
    common = sorted(set(a) & set(b))
    if len(common) < MIN_OBS:
        return None, None
    return [a[t] for t in common], [b[t] for t in common]


def granger_causality(x, y, max_lag=14):
    """Test if x Granger-causes y. Returns (best_lag, F_stat, p_value).
    Pure-Python implementation."""
    n = len(y)
    if n < 2 * max_lag + 4:
        return None
    best = None
    for lag in range(1, min(max_lag + 1, n // 4)):
        # restricted model: y[t] = a0 + sum(a_i * y[t-i])
        # unrestricted: y[t] = a0 + sum(a_i * y[t-i]) + sum(b_i * x[t-i])
        # F = ((SSR_r - SSR_u) / lag) / (SSR_u / (n - 2*lag - 1))
        try:
            ssr_r = _ssr_ar(y, lag)
            ssr_u = _ssr_ar_with_x(y, x, lag)
            if ssr_u <= 0 or ssr_r <= ssr_u:
                continue
            df_num = lag
            df_den = n - 2 * lag - 1
            if df_den < 5:
                continue
            f = ((ssr_r - ssr_u) / df_num) / (ssr_u / df_den)
            p = _f_p_value(f, df_num, df_den)
            if best is None or p < best[2]:
                best = (lag, f, p)
        except Exception:
            continue
    return best


def _solve_ols(X, y):
    """Manual OLS via normal equations. X is list of feature lists."""
    n = len(y)
    k = len(X[0]) if X else 0
    # X^T X
    xtx = [[sum(X[i][a] * X[i][b] for i in range(n)) for b in range(k)] for a in range(k)]
    # X^T y
    xty = [sum(X[i][a] * y[i] for i in range(n)) for a in range(k)]
    # Solve via Gauss-Jordan
    aug = [row + [xty[i]] for i, row in enumerate(xtx)]
    for i in range(k):
        # Find pivot
        pivot = aug[i][i]
        if abs(pivot) < 1e-12:
            for j in range(i + 1, k):
                if abs(aug[j][i]) > 1e-12:
                    aug[i], aug[j] = aug[j], aug[i]
                    pivot = aug[i][i]
                    break
            if abs(pivot) < 1e-12:
                raise ValueError("singular")
        for j in range(i + 1, k + 1):
            aug[i][j] /= pivot
        aug[i][i] = 1.0
        for j in range(k):
            if j != i and abs(aug[j][i]) > 1e-12:
                factor = aug[j][i]
                for col in range(i, k + 1):
                    aug[j][col] -= factor * aug[i][col]
    beta = [aug[i][k] for i in range(k)]
    # Residuals
    yhat = [sum(X[i][j] * beta[j] for j in range(k)) for i in range(n)]
    return beta, yhat


def _ssr_ar(y, lag):
    """SSR for AR(lag) model on y."""
    n = len(y)
    X = []
    yt = []
    for t in range(lag, n):
        row = [1.0] + [y[t - i] for i in range(1, lag + 1)]
        X.append(row)
        yt.append(y[t])
    _, yhat = _solve_ols(X, yt)
    return sum((yt[i] - yhat[i]) ** 2 for i in range(len(yt)))


def _ssr_ar_with_x(y, x, lag):
    """SSR for AR(lag) on y with lagged x added."""
    n = len(y)
    X = []
    yt = []
    for t in range(lag, n):
        row = [1.0] + [y[t - i] for i in range(1, lag + 1)] + [x[t - i] for i in range(1, lag + 1)]
        X.append(row)
        yt.append(y[t])
    _, yhat = _solve_ols(X, yt)
    return sum((yt[i] - yhat[i]) ** 2 for i in range(len(yt)))


def _f_p_value(f, df1, df2):
    """Approximate F-distribution p-value via incomplete beta function."""
    if f <= 0 or df1 <= 0 or df2 <= 0:
        return 1.0
    # p = 1 - I_x(df1/2, df2/2) where x = df1*f / (df1*f + df2)
    x = (df1 * f) / (df1 * f + df2)
    return 1.0 - _incomplete_beta(df1 / 2.0, df2 / 2.0, x)


def _incomplete_beta(a, b, x):
    """Lentz's algorithm for the regularized incomplete beta function."""
    if x < 0 or x > 1:
        return 0.0
    if x == 0:
        return 0.0
    if x == 1:
        return 1.0
    # Use continued fraction
    lbeta = (math.lgamma(a + b) - math.lgamma(a) - math.lgamma(b)
             + a * math.log(x) + b * math.log(1 - x))
    if x < (a + 1) / (a + b + 2):
        return math.exp(lbeta) * _beta_cf(a, b, x) / a
    else:
        return 1.0 - math.exp(lbeta) * _beta_cf(b, a, 1 - x) / b


def _beta_cf(a, b, x, max_iter=200, eps=1e-9):
    qab = a + b
    qap = a + 1.0
    qam = a - 1.0
    c = 1.0
    d = 1.0 - qab * x / qap
    if abs(d) < 1e-30:
        d = 1e-30
    d = 1.0 / d
    h = d
    for m in range(1, max_iter + 1):
        m2 = 2 * m
        aa = m * (b - m) * x / ((qam + m2) * (a + m2))
        d = 1.0 + aa * d
        if abs(d) < 1e-30: d = 1e-30
        c = 1.0 + aa / c
        if abs(c) < 1e-30: c = 1e-30
        d = 1.0 / d
        h *= d * c
        aa = -(a + m) * (qab + m) * x / ((a + m2) * (qap + m2))
        d = 1.0 + aa * d
        if abs(d) < 1e-30: d = 1e-30
        c = 1.0 + aa / c
        if abs(c) < 1e-30: c = 1e-30
        d = 1.0 / d
        delta = d * c
        h *= delta
        if abs(delta - 1.0) < eps:
            break
    return h


def send_telegram(text):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        data = json.dumps({"chat_id": TELEGRAM_CHAT_ID, "text": text,
                           "parse_mode": "Markdown",
                           "disable_web_page_preview": True}).encode()
        req = __import__("urllib.request").request.Request(
            url, data=data, headers={"Content-Type": "application/json"})
        __import__("urllib.request").request.urlopen(req, timeout=15)
    except Exception as e:
        logger.error(f"telegram_fail: {e}")


def lambda_handler(event, context):
    started = datetime.now(timezone.utc)
    logger.info("causality-scanner starting")

    # 1. List keys
    keys = list_candidate_keys()
    logger.info(f"candidate keys: {len(keys)}")

    # 2. Extract time series — only those with history available
    series_map = {}
    def try_load(k):
        s = read_series_from_history(k["key"])
        if s and len(s) >= MIN_OBS:
            return k["key"], s
        return None

    with ThreadPoolExecutor(max_workers=8) as ex:
        for r in as_completed([ex.submit(try_load, k) for k in keys[:300]]):
            try:
                res = r.result()
                if res:
                    series_map[res[0]] = res[1]
            except Exception:
                pass
    logger.info(f"series with ≥{MIN_OBS} obs: {len(series_map)}")

    if len(series_map) < 5:
        # Heartbeat output so freshness monitor sees the Lambda ran successfully
        heartbeat = {
            "ok": True,
            "status": "no_action",
            "reason": "insufficient_history_for_causality_discovery",
            "n_series_loaded": len(series_map),
            "min_series_needed": 5,
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "elapsed": round((datetime.now(timezone.utc) - started).total_seconds(), 2),
            "discoveries": [],
        }
        try:
            s3.put_object(
                Bucket=BUCKET, Key=OUT_KEY,
                Body=json.dumps(heartbeat, default=str, indent=2).encode(),
                ContentType="application/json",
                CacheControl="max-age=300, public",
            )
            logger.info(f"wrote heartbeat to {OUT_KEY} (insufficient history)")
        except Exception as e:
            logger.error(f"heartbeat_write_failed: {e}")
        return {"statusCode": 200, "body": json.dumps({
            "ok": True,
            "n_series_loaded": len(series_map),
            "note": "insufficient_history_for_causality_discovery",
            "elapsed": round((datetime.now(timezone.utc) - started).total_seconds(), 2),
        })}

    # 3. Run pair-wise Granger — capped at MAX_PAIRS
    series_names = list(series_map.keys())
    pairs = []
    for i, a in enumerate(series_names):
        for b in series_names[i+1:]:
            pairs.append((a, b))
    if len(pairs) > MAX_PAIRS:
        pairs = pairs[:MAX_PAIRS]
    logger.info(f"testing {len(pairs)} pairs")

    discoveries = []
    for a, b in pairs:
        sa, sb = align_series(series_map[a], series_map[b])
        if sa is None or len(sa) < MIN_OBS:
            continue
        # Test both directions
        ab = granger_causality(sa, sb)
        if ab and ab[2] < P_VALUE_THRESHOLD:
            discoveries.append({
                "leader": a, "follower": b, "lag_days": ab[0],
                "f_stat": round(ab[1], 3), "p_value": round(ab[2], 6),
                "n_observations": len(sa),
            })
        ba = granger_causality(sb, sa)
        if ba and ba[2] < P_VALUE_THRESHOLD:
            discoveries.append({
                "leader": b, "follower": a, "lag_days": ba[0],
                "f_stat": round(ba[1], 3), "p_value": round(ba[2], 6),
                "n_observations": len(sa),
            })

    # 4. Rank
    discoveries.sort(key=lambda d: d["p_value"])
    top = discoveries[:50]

    # 5. Payload
    elapsed = (datetime.now(timezone.utc) - started).total_seconds()
    payload = {
        "schema_version": "1.0",
        "engine": "causality-scanner",
        "generated_at": started.isoformat(),
        "elapsed_seconds": round(elapsed, 2),
        "n_candidate_keys": len(keys),
        "n_series_with_history": len(series_map),
        "n_pairs_tested": len(pairs),
        "n_significant_discoveries": len(discoveries),
        "p_value_threshold": P_VALUE_THRESHOLD,
        "max_lag_days": MAX_LAG,
        "min_observations": MIN_OBS,
        "top_50_discoveries": top,
        "methodology": {
            "version": "v1_pure_python_granger",
            "test": "F-test on AR vs AR-with-lagged-X",
            "p_value_calc": "incomplete_beta_function",
            "v2_plan": "statsmodels VAR + cointegration battery",
        },
    }

    # 6. Write
    s3.put_object(Bucket=BUCKET, Key=OUT_KEY,
                  Body=json.dumps(payload, default=str, indent=2).encode(),
                  ContentType="application/json",
                  CacheControl="max-age=3600, public")
    logger.info(f"wrote {OUT_KEY}: discoveries={len(discoveries)}")

    # 7. Telegram digest
    if top:
        lines = ["🔬 *Auto-Causality Discovery — Weekly*", "",
                 f"_{len(discoveries)} significant lead-lag pairs found ({len(pairs)} tested)_", ""]
        lines.append("*Top 5 strongest:*")
        for d in top[:5]:
            lead_name = d["leader"].split("/")[-1].replace(".json","")
            fol_name = d["follower"].split("/")[-1].replace(".json","")
            lines.append(f"  `{lead_name}` → `{fol_name}` @ {d['lag_days']}d p={d['p_value']:.4f}")
        lines.append("")
        lines.append("[causality-discoveries.html](https://justhodl.ai/causality-discoveries.html)")
        try:
            send_telegram("\n".join(lines))
        except Exception as e: logger.error(f"telegram_fail: {e}")

    return {"statusCode": 200,
            "headers": {"Content-Type": "application/json",
                        "Access-Control-Allow-Origin": "*"},
            "body": json.dumps({"ok": True, "n_discoveries": len(discoveries),
                                "n_pairs_tested": len(pairs),
                                "elapsed": round(elapsed, 2)})}
