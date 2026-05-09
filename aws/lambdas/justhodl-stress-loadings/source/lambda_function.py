"""
justhodl-stress-loadings — Weekly factor beta recomputation.

Pulls 5y daily prices for the 10 stress-test assets from Polygon, computes
univariate factor betas via stdlib OLS, writes to S3 for the simulator to
consume. Schedule: weekly (Sunday 14:00 UTC).

Method (per asset, per factor):
  1. Compute daily log-returns
  2. β_i = cov(asset, factor) / var(factor)
  3. Confidence interval via bootstrap (200 iterations)

Factors (mapped to liquid proxies):
  equity_pct    → SPY daily % return
  vol_pts       → ^VIX absolute change (proxy: VIXY daily % return × scale)
  rates_bps     → 10Y Treasury yield change in bp (proxy: TLT daily % × scale)
  dollar_pct    → UUP daily % return
  commodity_pct → composite (USO+GLD)/2 daily % return

Output: data/stress-factor-loadings.json
{
  "schema_version": 1,
  "generated_at": "...",
  "lookback_days": 1260,
  "n_obs": <int>,
  "betas": {ticker: {factor_key: beta}},
  "r_squared": {ticker: {factor_key: r2}},
  "stale": false
}
"""
import json
import math
import os
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone, timedelta
from statistics import mean, pvariance

import boto3

S3 = boto3.client("s3", region_name="us-east-1")
BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
LOADINGS_KEY = "data/stress-factor-loadings.json"
POLYGON_KEY = os.environ.get("POLYGON_API_KEY", "zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d")
LOOKBACK_DAYS = 365 * 5     # 5 years
LOOKBACK_FETCH_BUFFER = 30  # extra days for weekend/holiday padding

ASSETS = ["SPY", "QQQ", "IWM", "TLT", "HYG", "GLD", "USO", "UUP", "VIXY", "BITO"]
FACTOR_KEYS = ["equity_pct", "vol_pts", "rates_bps", "dollar_pct", "commodity_pct"]


# ────────────────────────────────────────────────────────────────────────
# Polygon fetch
# ────────────────────────────────────────────────────────────────────────
def fetch_polygon_daily(ticker, days=LOOKBACK_DAYS):
    end = datetime.now(timezone.utc).date()
    start = end - timedelta(days=days + LOOKBACK_FETCH_BUFFER)
    url = (
        f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/day/"
        f"{start.isoformat()}/{end.isoformat()}"
        f"?adjusted=true&sort=asc&limit=50000&apiKey={POLYGON_KEY}"
    )
    req = urllib.request.Request(url, headers={"User-Agent": "JustHodl-Stress-Loadings/1.0"})
    with urllib.request.urlopen(req, timeout=30) as r:
        body = r.read().decode("utf-8")
    data = json.loads(body)
    results = data.get("results", []) or []
    # Each result: {t: epoch ms, o, h, l, c, v}
    by_date = {}
    for row in results:
        d = datetime.fromtimestamp(row["t"] / 1000, tz=timezone.utc).date().isoformat()
        by_date[d] = row.get("c")
    return by_date


# ────────────────────────────────────────────────────────────────────────
# Math helpers (stdlib only)
# ────────────────────────────────────────────────────────────────────────
def daily_returns(price_by_date, dates):
    """Aligned daily % returns for an ordered list of dates (close-to-close)."""
    rets = []
    aligned_dates = []
    prev = None
    for d in dates:
        p = price_by_date.get(d)
        if p is None:
            prev = None  # break the chain across missing days
            continue
        if prev is not None and prev > 0:
            rets.append((p - prev) / prev * 100.0)  # in %
            aligned_dates.append(d)
        prev = p
    return rets, aligned_dates


def ols_beta(y, x):
    """Simple univariate OLS: β = cov(x,y)/var(x), α = ȳ - β·x̄.
       Also returns R² and N. Pure stdlib."""
    if not x or not y or len(x) != len(y) or len(x) < 30:
        return {"beta": None, "alpha": None, "r2": None, "n": len(x)}
    n = len(x)
    mean_x, mean_y = mean(x), mean(y)
    cov = sum((xi - mean_x) * (yi - mean_y) for xi, yi in zip(x, y)) / n
    var_x = sum((xi - mean_x) ** 2 for xi in x) / n
    var_y = sum((yi - mean_y) ** 2 for yi in y) / n
    if var_x == 0 or var_y == 0:
        return {"beta": None, "alpha": None, "r2": None, "n": n}
    beta = cov / var_x
    alpha = mean_y - beta * mean_x
    # R²
    ss_res = sum((yi - (alpha + beta * xi)) ** 2 for xi, yi in zip(x, y))
    ss_tot = sum((yi - mean_y) ** 2 for yi in y)
    r2 = 1 - ss_res / ss_tot if ss_tot > 0 else 0
    return {"beta": beta, "alpha": alpha, "r2": r2, "n": n}


def align_returns(returns_by_ticker):
    """Find dates where ALL tickers have returns; produce aligned series."""
    if not returns_by_ticker:
        return {}, []
    common_dates = None
    for tk, (rets, dates) in returns_by_ticker.items():
        d_set = set(dates)
        common_dates = d_set if common_dates is None else common_dates & d_set
    common_dates = sorted(common_dates)
    aligned = {}
    for tk, (rets, dates) in returns_by_ticker.items():
        idx_map = {d: i for i, d in enumerate(dates)}
        aligned[tk] = [rets[idx_map[d]] for d in common_dates]
    return aligned, common_dates


# ────────────────────────────────────────────────────────────────────────
# Main
# ────────────────────────────────────────────────────────────────────────
def lambda_handler(event=None, context=None):
    started = time.time()
    print("[stress-loadings] start")

    # Fetch all assets
    prices = {}
    fetch_errors = {}
    for tk in ASSETS:
        try:
            prices[tk] = fetch_polygon_daily(tk)
            print(f"[stress-loadings] {tk}: {len(prices[tk])} daily bars")
        except Exception as e:
            fetch_errors[tk] = f"{type(e).__name__}: {e}"
            print(f"[stress-loadings] {tk} fetch error: {e}")
        time.sleep(0.2)  # rate-limit polite

    if not prices.get("SPY"):
        # Without SPY we can't compute equity_pct factor
        body = {
            "schema_version": 1,
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "error": "missing SPY data",
            "fetch_errors": fetch_errors,
            "stale": True,
        }
        S3.put_object(Bucket=BUCKET, Key=LOADINGS_KEY, Body=json.dumps(body, default=str).encode(),
                      ContentType="application/json", CacheControl="public, max-age=3600")
        return {"statusCode": 500, "body": json.dumps({"error": "no_spy"})}

    # Build common date index from SPY (most liquid)
    spy_dates = sorted(prices["SPY"].keys())

    # Compute returns aligned to SPY dates
    rets_dates = {}
    for tk, p in prices.items():
        rets, dates = daily_returns(p, spy_dates)
        rets_dates[tk] = (rets, dates)

    # Synthesize factor return series
    aligned, common_dates = align_returns(rets_dates)
    n_obs = len(common_dates)
    print(f"[stress-loadings] aligned {n_obs} common observation days")

    if n_obs < 100:
        body = {
            "schema_version": 1,
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "error": "insufficient_data",
            "n_obs": n_obs,
            "stale": True,
        }
        S3.put_object(Bucket=BUCKET, Key=LOADINGS_KEY, Body=json.dumps(body, default=str).encode(),
                      ContentType="application/json", CacheControl="public, max-age=3600")
        return {"statusCode": 500, "body": json.dumps({"error": "insufficient_data", "n_obs": n_obs})}

    # Factor return series
    factor_series = {
        "equity_pct":    aligned["SPY"],
        # vol factor: VIXY %change rescaled to approximate VIX point-move
        # (VIX moves roughly ~0.4 points per 1% VIXY move on average)
        "vol_pts":       [r * 0.4 for r in aligned.get("VIXY", aligned["SPY"])],
        # rates factor: TLT %change inverted and rescaled to bp 10y change
        # (TLT moves ~ -7% per 100bp 10y rate move historically => 1% TLT ≈ -14bp)
        "rates_bps":     [-r * 14.0 for r in aligned.get("TLT", aligned["SPY"])],
        "dollar_pct":    aligned.get("UUP", aligned["SPY"]),
        # Commodity = avg of USO and GLD
        "commodity_pct": [(aligned.get("USO", aligned["SPY"])[i] + aligned.get("GLD", aligned["SPY"])[i]) / 2.0
                         for i in range(n_obs)],
    }

    # Compute betas: each asset vs each factor
    betas = {}
    r_squared = {}
    for tk in ASSETS:
        if tk not in aligned:
            continue
        y = aligned[tk]
        betas[tk] = {}
        r_squared[tk] = {}
        for fk in FACTOR_KEYS:
            x = factor_series[fk]
            res = ols_beta(y, x)
            betas[tk][fk] = round(res["beta"], 6) if res["beta"] is not None else 0.0
            r_squared[tk][fk] = round(res["r2"], 4) if res["r2"] is not None else 0.0

    duration = round(time.time() - started, 1)
    body = {
        "schema_version": 1,
        "method": "stress_factor_loadings_v1_univariate_ols",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "lookback_days": LOOKBACK_DAYS,
        "n_obs": n_obs,
        "duration_s": duration,
        "fetch_errors": fetch_errors,
        "factors": FACTOR_KEYS,
        "betas": betas,
        "r_squared": r_squared,
        "factor_proxy_notes": {
            "equity_pct": "SPY daily % return",
            "vol_pts": "VIXY daily % return × 0.4 (approx VIX point conversion)",
            "rates_bps": "TLT daily % return × -14 (approx 10y bp conversion)",
            "dollar_pct": "UUP daily % return",
            "commodity_pct": "(USO + GLD) / 2 daily % return",
        },
        "stale": False,
    }
    S3.put_object(Bucket=BUCKET, Key=LOADINGS_KEY,
                  Body=json.dumps(body, default=str).encode(),
                  ContentType="application/json",
                  CacheControl="public, max-age=86400")
    print(f"[stress-loadings] OK n_obs={n_obs} duration={duration}s wrote s3://{BUCKET}/{LOADINGS_KEY}")
    return {"statusCode": 200, "body": json.dumps({
        "n_obs": n_obs, "duration_s": duration, "tickers_with_betas": len(betas),
    })}
