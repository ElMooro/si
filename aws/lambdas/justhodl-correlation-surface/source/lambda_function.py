"""
justhodl-correlation-surface — Cross-asset rolling correlation matrix + regime break detection.

Pulls 250 trading days of price history for 14 key cross-asset proxies via
Polygon, computes log returns, then for each asset pair computes:
  - 30d, 90d, 252d Pearson correlation
  - delta_30d_vs_90d (correlation regime shift detector)
  - delta_30d_vs_252d (long-term decoupling detector)

Flags:
  - REGIME_BREAK if |delta_30d_vs_90d| >= 0.30 (massive correlation shift)
  - DECOUPLING   if |delta_30d_vs_252d| >= 0.40 (historical relationship inverted)
  - DEFAULT      otherwise

Asset universe (14 proxies covering all major macro factors):
  EQUITIES:    SPY, QQQ, IWM
  RATES:       TLT (long), IEF (intermediate), SHY (short)
  CREDIT:      HYG, LQD
  COMMODITIES: GLD, SLV, USO
  CRYPTO:      IBIT
  CURRENCY:    UUP (USD), FXE (EUR)

Output: data/correlation-surface.json
"""
import json
import os
import time
import urllib.request
import urllib.parse
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone, timedelta
from statistics import mean, stdev
import math
import boto3

S3 = boto3.client("s3", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"
KEY = "data/correlation-surface.json"

POLYGON_KEY = os.environ.get("POLYGON_KEY", "zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d")

UNIVERSE = [
    ("SPY",  "EQUITY_BROAD",   "S&P 500"),
    ("QQQ",  "EQUITY_TECH",    "Nasdaq 100"),
    ("IWM",  "EQUITY_SMALL",   "Russell 2000"),
    ("TLT",  "RATES_LONG",     "20Y+ Treasuries"),
    ("IEF",  "RATES_INT",      "7-10Y Treasuries"),
    ("SHY",  "RATES_SHORT",    "1-3Y Treasuries"),
    ("HYG",  "CREDIT_HY",      "High Yield Bonds"),
    ("LQD",  "CREDIT_IG",      "Investment Grade"),
    ("GLD",  "GOLD",           "Gold"),
    ("SLV",  "SILVER",         "Silver"),
    ("USO",  "OIL",            "Crude Oil"),
    ("IBIT", "BTC",            "Bitcoin"),
    ("UUP",  "USD",            "US Dollar"),
    ("FXE",  "EUR",            "Euro"),
]

LOOKBACKS = [30, 90, 252]


def polygon_bars(ticker, days=300):
    """Fetch daily aggregates for last `days` calendar days from Polygon."""
    end = datetime.now(timezone.utc).date().isoformat()
    start = (datetime.now(timezone.utc).date() - timedelta(days=days + 100)).isoformat()
    url = (
        f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/day/{start}/{end}"
        f"?adjusted=true&sort=asc&limit=5000&apiKey={POLYGON_KEY}"
    )
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "justhodl-correlation/1.0"})
        with urllib.request.urlopen(req, timeout=30) as r:
            data = json.loads(r.read().decode())
        if data.get("results"):
            return [(b["t"], b["c"]) for b in data["results"]]
    except Exception as e:
        print(f"[poly] {ticker} failed: {e}")
    return []


def log_returns(bars):
    """Convert (ts, close) tuples to date->log_return dict (skipping first day)."""
    out = {}
    for i in range(1, len(bars)):
        ts, c = bars[i]
        prev = bars[i - 1][1]
        if prev > 0 and c > 0:
            d = datetime.fromtimestamp(ts / 1000, timezone.utc).date().isoformat()
            out[d] = math.log(c / prev)
    return out


def pearson(xs, ys):
    """Pearson correlation between two equal-length lists."""
    n = len(xs)
    if n < 5:
        return None
    mx = sum(xs) / n
    my = sum(ys) / n
    cov = sum((x - mx) * (y - my) for x, y in zip(xs, ys))
    sx2 = sum((x - mx) ** 2 for x in xs)
    sy2 = sum((y - my) ** 2 for y in ys)
    denom = math.sqrt(sx2 * sy2)
    if denom == 0:
        return None
    return cov / denom


def rolling_corr(returns_a, returns_b, n_days):
    """Compute correlation over the last n_days where both series have data."""
    common = sorted(set(returns_a.keys()) & set(returns_b.keys()))
    if len(common) < n_days:
        # Use whatever we have (degraded)
        if len(common) < 5:
            return None
        n_days = len(common)
    window = common[-n_days:]
    xs = [returns_a[d] for d in window]
    ys = [returns_b[d] for d in window]
    return pearson(xs, ys)


def lambda_handler(event=None, context=None):
    started = time.time()
    print(f"[correlation] start, universe={len(UNIVERSE)}")

    # 1. Fetch bars in parallel
    bars_by_ticker = {}
    with ThreadPoolExecutor(max_workers=8) as ex:
        futs = {ex.submit(polygon_bars, t): t for t, _, _ in UNIVERSE}
        for f in as_completed(futs):
            t = futs[f]
            bars = f.result()
            if bars:
                bars_by_ticker[t] = bars
            print(f"[correlation] {t}: {len(bars)} bars")

    # 2. Convert to returns
    returns_by_ticker = {t: log_returns(bars) for t, bars in bars_by_ticker.items()}
    print(f"[correlation] returns built for {len(returns_by_ticker)} tickers")

    # 3. Compute correlation matrices for each lookback
    matrix_by_lookback = {}
    pair_data = {}
    for lb in LOOKBACKS:
        m = {}
        for t1, _, _ in UNIVERSE:
            m[t1] = {}
            if t1 not in returns_by_ticker:
                continue
            for t2, _, _ in UNIVERSE:
                if t2 not in returns_by_ticker:
                    continue
                if t1 == t2:
                    m[t1][t2] = 1.0
                    continue
                # Compute or copy from t2/t1
                rev = m.get(t2, {}).get(t1)
                if rev is not None:
                    m[t1][t2] = rev
                else:
                    c = rolling_corr(returns_by_ticker[t1], returns_by_ticker[t2], lb)
                    m[t1][t2] = round(c, 3) if c is not None else None
        matrix_by_lookback[f"corr_{lb}d"] = m

    # 4. Build pair-by-pair view with regime detection
    pairs = []
    seen = set()
    for t1, cat1, name1 in UNIVERSE:
        for t2, cat2, name2 in UNIVERSE:
            if t1 == t2:
                continue
            key = tuple(sorted([t1, t2]))
            if key in seen:
                continue
            seen.add(key)
            c30 = matrix_by_lookback.get("corr_30d", {}).get(t1, {}).get(t2)
            c90 = matrix_by_lookback.get("corr_90d", {}).get(t1, {}).get(t2)
            c252 = matrix_by_lookback.get("corr_252d", {}).get(t1, {}).get(t2)
            delta_30_vs_90 = None
            delta_30_vs_252 = None
            if c30 is not None and c90 is not None:
                delta_30_vs_90 = round(c30 - c90, 3)
            if c30 is not None and c252 is not None:
                delta_30_vs_252 = round(c30 - c252, 3)

            flag = "DEFAULT"
            if delta_30_vs_252 is not None and abs(delta_30_vs_252) >= 0.40:
                flag = "DECOUPLING"
            if delta_30_vs_90 is not None and abs(delta_30_vs_90) >= 0.30:
                flag = "REGIME_BREAK"

            pairs.append({
                "ticker_a": t1, "category_a": cat1, "name_a": name1,
                "ticker_b": t2, "category_b": cat2, "name_b": name2,
                "corr_30d": c30,
                "corr_90d": c90,
                "corr_252d": c252,
                "delta_30d_vs_90d": delta_30_vs_90,
                "delta_30d_vs_252d": delta_30_vs_252,
                "flag": flag,
            })

    # Sort by largest absolute regime-break delta
    pairs.sort(key=lambda p: -abs(p.get("delta_30d_vs_90d") or 0))

    # 5. Headline pairs (classic relationships)
    headline_pairs = [
        ("SPY", "TLT"),    # stock-bond (was negative ~ -0.5 historically)
        ("SPY", "GLD"),    # equity-gold
        ("SPY", "UUP"),    # equity-USD
        ("GLD", "UUP"),    # gold-USD (classic inverse)
        ("TLT", "USO"),    # bonds-oil (inflation proxy)
        ("IBIT", "SPY"),   # BTC-equity correlation
        ("IBIT", "GLD"),   # BTC-gold (digital gold thesis)
        ("HYG", "SPY"),    # credit-equity (risk-on)
        ("QQQ", "TLT"),    # tech-rates
        ("USO", "UUP"),    # oil-dollar
    ]
    headline = []
    for ta, tb in headline_pairs:
        # Find existing pair record (sorted alphabetically)
        match = next((p for p in pairs if {p["ticker_a"], p["ticker_b"]} == {ta, tb}), None)
        if match:
            headline.append({
                "name_a": match["name_a"] if match["ticker_a"] == ta else match["name_b"],
                "name_b": match["name_b"] if match["ticker_a"] == ta else match["name_a"],
                "ticker_a": ta, "ticker_b": tb,
                "corr_30d": match["corr_30d"],
                "corr_90d": match["corr_90d"],
                "corr_252d": match["corr_252d"],
                "delta_30d_vs_90d": match["delta_30d_vs_90d"],
                "delta_30d_vs_252d": match["delta_30d_vs_252d"],
                "flag": match["flag"],
            })

    # 6. Regime breaks + decouplings
    regime_breaks = [p for p in pairs if p["flag"] == "REGIME_BREAK"]
    decouplings = [p for p in pairs if p["flag"] == "DECOUPLING"]

    # 7. Avg cross-asset correlation (proxy for "macro all-on" regime)
    all_30d_corrs = [p["corr_30d"] for p in pairs if p["corr_30d"] is not None]
    avg_30d_corr = round(mean([abs(c) for c in all_30d_corrs]), 3) if all_30d_corrs else None
    if avg_30d_corr is not None and avg_30d_corr > 0.5:
        macro_regime = "MACRO_ALL_ON"
        macro_desc = "Cross-asset correlations elevated — single-factor macro regime, diversification benefit reduced"
    elif avg_30d_corr is not None and avg_30d_corr < 0.25:
        macro_regime = "DIVERSIFIED"
        macro_desc = "Low cross-asset correlation — assets moving on idiosyncratic factors"
    else:
        macro_regime = "NORMAL"
        macro_desc = "Normal cross-asset correlation regime"

    out = {
        "version": "1.0",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "duration_s": round(time.time() - started, 2),
        "universe": [{"ticker": t, "category": c, "name": n} for t, c, n in UNIVERSE],
        "lookbacks_trading_days": LOOKBACKS,
        "macro_regime": macro_regime,
        "macro_regime_description": macro_desc,
        "avg_30d_abs_correlation": avg_30d_corr,
        "matrices": matrix_by_lookback,
        "headline_pairs": headline,
        "regime_breaks": regime_breaks[:15],
        "decouplings": decouplings[:15],
        "all_pairs": pairs,
        "n_pairs": len(pairs),
        "n_regime_breaks": len(regime_breaks),
        "n_decouplings": len(decouplings),
        "data_sources": {"prices": "Polygon"},
        "flag_definitions": {
            "REGIME_BREAK": "|Δ corr 30d vs 90d| ≥ 0.30",
            "DECOUPLING":   "|Δ corr 30d vs 252d| ≥ 0.40",
            "DEFAULT":      "Stable correlation regime",
        },
    }

    body = json.dumps(out, default=str).encode("utf-8")
    S3.put_object(
        Bucket=BUCKET, Key=KEY, Body=body,
        ContentType="application/json", CacheControl="public, max-age=3600",
    )
    print(f"[correlation] regime={macro_regime} avg_corr={avg_30d_corr} breaks={len(regime_breaks)} decouplings={len(decouplings)}")
    print(f"[correlation] wrote s3://{BUCKET}/{KEY} — {len(body):,}b in {out['duration_s']}s")

    return {
        "statusCode": 200,
        "body": json.dumps({
            "macro_regime": macro_regime,
            "avg_30d_abs_correlation": avg_30d_corr,
            "n_regime_breaks": len(regime_breaks),
            "n_decouplings": len(decouplings),
            "duration_s": out["duration_s"],
        }),
    }


if __name__ == "__main__":
    print(json.dumps(lambda_handler({}, None), indent=2, default=str))
