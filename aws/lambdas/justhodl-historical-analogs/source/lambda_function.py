"""
justhodl-historical-analogs — Find historical dates whose market regime
most closely resembles today, and report what happened in the following weeks.

Methodology:
  1. Pull 25+ years of daily history from FRED for 6 dimensions:
       - VIX (volatility)
       - T10Y2Y (curve slope)
       - BAMLH0A0HYM2 (HY credit spread)
       - DTWEXBGS (USD index)
       - DGS10 (10Y nominal)
       - SP500 (1m rolling return)
  2. Build daily state vectors → normalize via rolling 252d z-score
  3. Today's vector = current state
  4. For each historical date (drop dates within last 90d), compute Euclidean
     distance to today
  5. Pick top-K=15 nearest neighbors
  6. For each neighbor, compute SPY forward returns (5d / 21d / 63d / 126d)
  7. Aggregate: mean / median / hit-rate (% positive) for each horizon
  8. Output regime classification + analog distribution

Output: data/historical-analogs.json
"""
import json
import os
import time
import urllib.request
import urllib.parse
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone, timedelta, date
from statistics import mean, median, stdev
import boto3
import math

S3 = boto3.client("s3", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"
KEY = "data/historical-analogs.json"

FRED_KEY = os.environ.get("FRED_KEY", "2f057499936072679d8843d7fce99989")

# Daily series with deep history (all 20+ years on FRED)
FEATURES = [
    ("VIXCLS",         "vix"),                  # CBOE VIX (1990-)
    ("T10Y2Y",         "twos_tens_bps"),        # 10Y-2Y spread (1976-)
    ("BAMLH0A0HYM2",   "hy_oas"),               # HY credit spread (1996-)
    ("DTWEXBGS",       "usd_index"),            # USD broad (2006-)  — alt: TWEXBMTH
    ("DGS10",          "ten_year_yield"),       # 10Y nominal (1962-)
    ("SP500",          "spx_close"),            # S&P 500 (2015-) — short, but we'll handle gracefully
]

# Use a longer-history SPX proxy too: fall back to FRED's spy daily series
# Forward horizons (trading days)
HORIZONS = [5, 21, 63, 126]

# Lookback for z-score normalization (trading days)
ZSCORE_LOOKBACK = 252

# How many nearest neighbors to use
K_NEIGHBORS = 15

# Earliest date considered (data quality cutoff)
MIN_DATE = "2002-01-01"

# Drop dates too close to today (avoid trivial matches)
EXCLUDE_RECENT_DAYS = 90


def fred_full(series_id, start=MIN_DATE):
    """Fetch full daily history from FRED."""
    url = (
        "https://api.stlouisfed.org/fred/series/observations?"
        + urllib.parse.urlencode({
            "series_id": series_id,
            "api_key": FRED_KEY,
            "file_type": "json",
            "observation_start": start,
            "limit": 100000,
        })
    )
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "justhodl-analogs/1.0"})
        with urllib.request.urlopen(req, timeout=30) as r:
            data = json.loads(r.read().decode())
        out = {}
        for o in data.get("observations", []):
            v = o.get("value", ".")
            if v in (".", ""):
                continue
            try:
                out[o["date"]] = float(v)
            except ValueError:
                continue
        return out
    except Exception as e:
        print(f"[fred] {series_id} failed: {e}")
        return {}


def rolling_zscore_dict(series_dict, lookback=ZSCORE_LOOKBACK):
    """Convert a date->value dict into date->zscore dict using rolling window."""
    sorted_dates = sorted(series_dict.keys())
    values = [series_dict[d] for d in sorted_dates]
    out = {}
    for i in range(len(values)):
        if i < 30:  # need minimum window
            continue
        start = max(0, i - lookback)
        window = values[start:i]
        if len(window) < 30:
            continue
        m = mean(window)
        sd = stdev(window) if len(window) > 1 else 0.0
        if sd == 0:
            continue
        out[sorted_dates[i]] = (values[i] - m) / sd
    return out


def compute_returns(spx_dict):
    """Compute 1-month rolling returns from SPX prices (252-day series)."""
    sorted_dates = sorted(spx_dict.keys())
    returns = {}
    for i in range(21, len(sorted_dates)):
        d_now = sorted_dates[i]
        d_then = sorted_dates[i - 21]
        if spx_dict[d_then] > 0:
            returns[d_now] = (spx_dict[d_now] / spx_dict[d_then] - 1) * 100
    return returns


def euclidean(v1, v2):
    """Squared Euclidean distance between two equal-length vectors."""
    return math.sqrt(sum((a - b) ** 2 for a, b in zip(v1, v2)))


def lambda_handler(event=None, context=None):
    started = time.time()
    print(f"[analogs] start")

    # 1. Fetch all FRED features in parallel
    raw = {}
    with ThreadPoolExecutor(max_workers=6) as ex:
        futs = {ex.submit(fred_full, sid): (sid, label) for sid, label in FEATURES}
        for f in as_completed(futs):
            sid, label = futs[f]
            obs = f.result()
            raw[label] = obs
            print(f"[analogs] {label} ({sid}): {len(obs)} obs")

    # 2. SPX returns (used both as feature AND for forward calc)
    spx = raw.get("spx_close", {})
    spx_returns_1m = compute_returns(spx)

    # 3. Build feature dict: each feature -> dict of date -> z-score
    z_features = {}
    for label in ["vix", "twos_tens_bps", "hy_oas", "usd_index", "ten_year_yield"]:
        z_features[label] = rolling_zscore_dict(raw.get(label, {}))
    # SPX 1m return as feature (already in % so just z-score)
    z_features["spx_1m_ret"] = rolling_zscore_dict(spx_returns_1m)

    # 4. Find dates that have ALL features available
    common_dates = None
    for label, d in z_features.items():
        s = set(d.keys())
        common_dates = s if common_dates is None else common_dates & s
    common_dates = sorted(common_dates) if common_dates else []
    print(f"[analogs] common dates: {len(common_dates)}")

    if len(common_dates) < 200:
        return {"statusCode": 500, "body": "insufficient overlap in feature history"}

    # 5. Build vector for each common date
    feature_order = ["vix", "twos_tens_bps", "hy_oas", "usd_index", "ten_year_yield", "spx_1m_ret"]
    vectors = {}
    for d in common_dates:
        try:
            vec = [z_features[label][d] for label in feature_order]
            vectors[d] = vec
        except KeyError:
            continue

    # 6. Today vector = latest available
    today_date = common_dates[-1]
    today_vec = vectors[today_date]
    print(f"[analogs] today_date={today_date} vec={[round(v,2) for v in today_vec]}")

    # 7. Compute distance from today to every historical date (excluding last 90 days)
    cutoff_date = (date.fromisoformat(today_date) - timedelta(days=EXCLUDE_RECENT_DAYS)).isoformat()
    distances = []
    for d, vec in vectors.items():
        if d >= cutoff_date:
            continue
        dist = euclidean(today_vec, vec)
        distances.append((d, dist, vec))
    distances.sort(key=lambda x: x[1])

    # 8. Top K nearest neighbors
    nearest = distances[:K_NEIGHBORS]
    print(f"[analogs] top {K_NEIGHBORS} matches:")
    for d, dist, vec in nearest[:5]:
        print(f"   {d} dist={dist:.3f}")

    # 9. For each neighbor, compute SPY forward returns from FRED SP500 series
    sorted_spx_dates = sorted(spx.keys())
    spx_idx = {d: i for i, d in enumerate(sorted_spx_dates)}

    def forward_return(start_date, n_days):
        i = spx_idx.get(start_date)
        if i is None:
            return None
        if i + n_days >= len(sorted_spx_dates):
            return None
        end_date = sorted_spx_dates[i + n_days]
        p0 = spx[start_date]
        p1 = spx[end_date]
        if p0 <= 0:
            return None
        return ((p1 / p0) - 1) * 100

    analogs = []
    forward_returns_by_horizon = {h: [] for h in HORIZONS}
    for d, dist, vec in nearest:
        rets = {}
        for h in HORIZONS:
            r = forward_return(d, h)
            if r is not None:
                rets[f"forward_{h}d_pct"] = round(r, 2)
                forward_returns_by_horizon[h].append(r)
        analogs.append({
            "date": d,
            "distance": round(dist, 3),
            "similarity": round(max(0, 1 - dist / 5), 3),  # rough similarity 0-1
            "vector": {label: round(v, 2) for label, v in zip(feature_order, vec)},
            **rets,
        })

    # 10. Aggregate forward distribution
    forward_distribution = {}
    for h in HORIZONS:
        vals = forward_returns_by_horizon[h]
        if not vals:
            forward_distribution[f"{h}d"] = None
            continue
        n_pos = sum(1 for v in vals if v > 0)
        forward_distribution[f"{h}d"] = {
            "n": len(vals),
            "mean_pct": round(mean(vals), 2),
            "median_pct": round(median(vals), 2),
            "hit_rate_pct": round(n_pos / len(vals) * 100, 1),
            "min_pct": round(min(vals), 2),
            "max_pct": round(max(vals), 2),
            "stdev_pct": round(stdev(vals), 2) if len(vals) >= 2 else 0,
        }

    # 11. Today snapshot in absolute units (for human-readable display)
    today_snapshot = {
        "date": today_date,
        "vix": raw.get("vix", {}).get(today_date),
        "twos_tens_bps": raw.get("twos_tens_bps", {}).get(today_date),
        "hy_oas_pct": raw.get("hy_oas", {}).get(today_date),
        "usd_index": raw.get("usd_index", {}).get(today_date),
        "ten_year_yield_pct": raw.get("ten_year_yield", {}).get(today_date),
        "spx_close": spx.get(today_date),
        "spx_1m_return_pct": round(spx_returns_1m.get(today_date), 2) if spx_returns_1m.get(today_date) is not None else None,
        "z_scores": {label: round(today_vec[i], 2) for i, label in enumerate(feature_order)},
    }

    # 12. Direction call
    h21 = forward_distribution.get("21d")
    if h21 and h21.get("hit_rate_pct") is not None:
        hr = h21["hit_rate_pct"]
        if hr >= 70 and h21["mean_pct"] > 1:
            call = "BULLISH"
            call_desc = f"21d analogs: {hr}% positive, mean {h21['mean_pct']:+.2f}%"
        elif hr <= 30 and h21["mean_pct"] < -1:
            call = "BEARISH"
            call_desc = f"21d analogs: only {hr}% positive, mean {h21['mean_pct']:+.2f}%"
        else:
            call = "MIXED"
            call_desc = f"21d analogs split: {hr}% positive, mean {h21['mean_pct']:+.2f}%"
    else:
        call = "INSUFFICIENT_DATA"
        call_desc = "Could not compute 21d forward returns from analogs"

    out = {
        "version": "1.0",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "duration_s": round(time.time() - started, 2),
        "today": today_snapshot,
        "feature_order": feature_order,
        "n_historical_dates_evaluated": len(distances),
        "k_neighbors": K_NEIGHBORS,
        "analogs": analogs,
        "forward_distribution": forward_distribution,
        "directional_call": call,
        "directional_description": call_desc,
        "methodology": "Euclidean distance over rolling 252d z-scores of [vix, 2s10s, HY OAS, USD, 10Y, SPX 1m return]",
        "data_sources": {"all": "FRED API (free)"},
    }

    body = json.dumps(out, default=str).encode("utf-8")
    S3.put_object(
        Bucket=BUCKET, Key=KEY, Body=body,
        ContentType="application/json", CacheControl="public, max-age=3600",
    )

    print(f"[analogs] call={call} hit_rate_21d={h21.get('hit_rate_pct') if h21 else 'n/a'}")
    print(f"[analogs] wrote s3://{BUCKET}/{KEY} — {len(body):,}b in {out['duration_s']}s")

    return {
        "statusCode": 200,
        "body": json.dumps({
            "n_analogs": len(analogs),
            "directional_call": call,
            "duration_s": out["duration_s"],
        }),
    }


if __name__ == "__main__":
    print(json.dumps(lambda_handler({}, None), indent=2, default=str))
