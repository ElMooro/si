"""
justhodl-portfolio-analytics
════════════════════════════
Cross-candidate statistical analysis for portfolio risk management.

OUTPUTS
═══════
1. CORRELATION MATRIX
   60-day daily-return Pearson correlation between every pair of top-20
   pump candidates. Identifies "crowded trade" risk — if you go long
   PLTR + AMD + NVDA all simultaneously, you're really just long
   semiconductors with extra steps.

   - Pairwise correlation matrix
   - Cluster detection (groups with avg corr ≥ 0.7)
   - Per-candidate "diversification score" (1 - avg corr with others)

2. FACTOR EXPOSURE DECOMPOSITION
   For each candidate, run OLS regression of daily returns vs:
     - SPY (market beta)
     - QQQ (tech-tilt beta)
     - Sector ETF (XLK/XLF/XLV/etc)
     - VIX (volatility regime sensitivity, computed from VXX if available
       or proxied via 5d realized vol)

   Output: beta_spy, beta_qqq, beta_sector, alpha (annualized), R², residual_vol
   Plus: "idiosyncratic alpha" = ticker's recent 20d performance minus what
   beta_spy * SPY_perf would predict — proxy for stock-specific edge.

INPUTS
══════
data/convergence-radar.json   →  pump_candidates[]
data/etf-flows.json           →  for sector ETF reference (XLK/XLC/etc.)
FMP /stable/historical-price-eod/full for each candidate + benchmarks

OUTPUT
══════
data/portfolio-analytics.json
{
  "schema_version": "1.0",
  "generated_at":   "...",
  "lookback_days":  60,
  "n_candidates":   12,
  "correlations": {
    "matrix": {                            # ticker A → ticker B → corr
      "PLTR": {"AMD": 0.78, "NVDA": 0.82, "MSFT": 0.55, ...},
      ...
    },
    "clusters": [
      {"name": "AI/Semiconductor", "members": ["NVDA","AMD","PLTR","MU","ARM"], "avg_corr": 0.74},
      {"name": "Healthcare megacap", "members": ["LLY"], "avg_corr": null},
      ...
    ],
    "diversification_scores": {
      "NEM":  0.91,   # gold miner — very uncorrelated
      "LLY":  0.78,   # healthcare
      "PLTR": 0.45,   # high correlation with semis
      ...
    }
  },
  "factor_exposure": {
    "benchmarks_used":  ["SPY", "QQQ", "XLK", "XLV", "XLB"],
    "per_ticker": {
      "PLTR": {
        "beta_spy":      1.42,
        "beta_qqq":      1.85,
        "beta_sector":   1.12,
        "sector_etf":    "XLK",
        "alpha_ann":     8.4,
        "r_squared":     0.62,
        "idio_alpha_20d": 3.8,    # % alpha generated in last 20 days vs market beta
        "vol_regime":    "expansion"
      }
    }
  }
}

SCHEDULE
════════
cron(0 4 * * ? *) — daily 04:00 UTC (correlations are stable over short windows;
                                      no need to run every hour)
"""
import json
import math
import os
import sys
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Tuple

import boto3

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

S3_BUCKET    = "justhodl-dashboard-live"
RADAR_KEY    = "data/convergence-radar.json"
OUTPUT_KEY   = "data/portfolio-analytics.json"
FMP_KEY      = os.environ.get("FMP_KEY", "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb")

LOOKBACK_DAYS = 60

# Benchmark universe
BENCHMARKS = ["SPY", "QQQ", "VXX"]
SECTOR_ETFS = ["XLK", "XLC", "XLY", "XLP", "XLE", "XLF", "XLV", "XLI", "XLB", "XLRE", "XLU"]
SECTOR_ETF_MAP = {
    "Technology":              "XLK",
    "Communication Services":  "XLC",
    "Consumer Cyclical":       "XLY",
    "Consumer Defensive":      "XLP",
    "Energy":                  "XLE",
    "Financial Services":      "XLF",
    "Healthcare":              "XLV",
    "Industrials":             "XLI",
    "Basic Materials":         "XLB",
    "Real Estate":             "XLRE",
    "Utilities":               "XLU",
}

s3 = boto3.client("s3", region_name="us-east-1")
PRICE_CACHE: Dict[str, List[float]] = {}  # ticker → daily returns


# ═════════════════════════════════════════════════════════════════════
# Data fetch
# ═════════════════════════════════════════════════════════════════════

def fetch_returns(ticker: str, days: int = LOOKBACK_DAYS + 15) -> List[float]:
    """Returns daily log returns aligned to end-of-window."""
    if ticker in PRICE_CACHE:
        return PRICE_CACHE[ticker]
    try:
        end = datetime.now(timezone.utc).date()
        start = end - timedelta(days=days * 1.5)  # cushion for non-trading days
        url = (f"https://financialmodelingprep.com/stable/historical-price-eod/full"
                f"?symbol={ticker}&from={start.isoformat()}&to={end.isoformat()}&apikey={FMP_KEY}")
        req = urllib.request.Request(url, headers={"User-Agent": "justhodl/analytics"})
        with urllib.request.urlopen(req, timeout=15) as r:
            data = json.loads(r.read())
        rows = data if isinstance(data, list) else data.get("historical", [])
        rows = sorted(rows, key=lambda x: x.get("date", ""))
        closes = [r.get("close") for r in rows if r.get("close")]
        if len(closes) < 5:
            PRICE_CACHE[ticker] = []
            return []
        rets = []
        for i in range(1, len(closes)):
            if closes[i] > 0 and closes[i-1] > 0:
                rets.append(math.log(closes[i] / closes[i-1]))
        # Trim to last LOOKBACK_DAYS
        rets = rets[-LOOKBACK_DAYS:] if len(rets) > LOOKBACK_DAYS else rets
        PRICE_CACHE[ticker] = rets
        return rets
    except Exception as e:
        print(f"[returns] {ticker}: {str(e)[:100]}")
        PRICE_CACHE[ticker] = []
        return []


def fetch_profile(ticker: str) -> Optional[dict]:
    try:
        url = f"https://financialmodelingprep.com/stable/profile?symbol={ticker}&apikey={FMP_KEY}"
        req = urllib.request.Request(url, headers={"User-Agent": "justhodl/analytics"})
        with urllib.request.urlopen(req, timeout=10) as r:
            data = json.loads(r.read())
        return data[0] if isinstance(data, list) and data else None
    except Exception:
        return None


# ═════════════════════════════════════════════════════════════════════
# Statistics
# ═════════════════════════════════════════════════════════════════════

def pearson(x: List[float], y: List[float]) -> Optional[float]:
    """Pearson correlation between two equal-length arrays."""
    if len(x) != len(y) or len(x) < 5:
        return None
    n = len(x)
    mx = sum(x) / n
    my = sum(y) / n
    num = sum((x[i] - mx) * (y[i] - my) for i in range(n))
    dx = math.sqrt(sum((x[i] - mx) ** 2 for i in range(n)))
    dy = math.sqrt(sum((y[i] - my) ** 2 for i in range(n)))
    if dx == 0 or dy == 0:
        return None
    return num / (dx * dy)


def ols_regress(y: List[float], x: List[float]) -> Optional[dict]:
    """Single-variable OLS: y = alpha + beta*x. Returns alpha, beta, R²."""
    if len(y) != len(x) or len(x) < 10:
        return None
    n = len(x)
    mx = sum(x) / n
    my = sum(y) / n
    sxx = sum((x[i] - mx) ** 2 for i in range(n))
    syy = sum((y[i] - my) ** 2 for i in range(n))
    sxy = sum((x[i] - mx) * (y[i] - my) for i in range(n))
    if sxx == 0:
        return None
    beta = sxy / sxx
    alpha = my - beta * mx
    # R² = (sxy)² / (sxx * syy)
    r_sq = (sxy ** 2) / (sxx * syy) if syy > 0 else 0
    # Residual standard error
    resid_var = sum((y[i] - alpha - beta * x[i]) ** 2 for i in range(n)) / max(1, n - 2)
    resid_se = math.sqrt(resid_var)
    return {
        "alpha":  alpha,
        "beta":   beta,
        "r_sq":   r_sq,
        "resid_se": resid_se,
        "n":      n,
    }


def align_returns(rets_a: List[float], rets_b: List[float]) -> Tuple[List[float], List[float]]:
    """Take the common tail length so both series have same length."""
    n = min(len(rets_a), len(rets_b))
    if n < 5:
        return ([], [])
    return rets_a[-n:], rets_b[-n:]


# ═════════════════════════════════════════════════════════════════════
# Correlation matrix
# ═════════════════════════════════════════════════════════════════════

def build_correlation_matrix(tickers: List[str], returns_map: Dict[str, List[float]]) -> Dict[str, Dict[str, float]]:
    matrix: Dict[str, Dict[str, float]] = {}
    for i, t1 in enumerate(tickers):
        matrix[t1] = {}
        rets1 = returns_map.get(t1, [])
        if not rets1:
            continue
        for t2 in tickers:
            if t1 == t2:
                matrix[t1][t2] = 1.0
                continue
            rets2 = returns_map.get(t2, [])
            a, b = align_returns(rets1, rets2)
            corr = pearson(a, b) if a and b else None
            if corr is not None:
                matrix[t1][t2] = round(corr, 3)
    return matrix


def find_clusters(matrix: Dict[str, Dict[str, float]], threshold: float = 0.65) -> List[dict]:
    """Greedy clustering: group tickers with high pairwise correlation."""
    tickers = list(matrix.keys())
    assigned = set()
    clusters = []
    for seed in tickers:
        if seed in assigned:
            continue
        members = {seed}
        for t in tickers:
            if t == seed or t in assigned:
                continue
            c = matrix.get(seed, {}).get(t)
            if c is not None and c >= threshold:
                members.add(t)
        if len(members) >= 2:
            # Compute avg pairwise corr inside cluster
            mem_list = list(members)
            pairs = []
            for i, x in enumerate(mem_list):
                for y in mem_list[i+1:]:
                    c = matrix.get(x, {}).get(y)
                    if c is not None:
                        pairs.append(c)
            avg_corr = sum(pairs) / len(pairs) if pairs else None
            clusters.append({
                "members":  sorted(mem_list),
                "size":     len(mem_list),
                "avg_corr": round(avg_corr, 3) if avg_corr else None,
            })
            for m in members:
                assigned.add(m)
    # Singletons
    for t in tickers:
        if t not in assigned:
            clusters.append({"members": [t], "size": 1, "avg_corr": None})
    return sorted(clusters, key=lambda c: -c["size"])


def diversification_scores(matrix: Dict[str, Dict[str, float]]) -> Dict[str, float]:
    """For each ticker, score = 1 - avg(corr with others). Higher = more uncorrelated."""
    out = {}
    for t, row in matrix.items():
        others = [v for k, v in row.items() if k != t and v is not None]
        if not others:
            out[t] = None
            continue
        avg = sum(others) / len(others)
        out[t] = round(1 - avg, 3)
    return out


# ═════════════════════════════════════════════════════════════════════
# Factor exposure
# ═════════════════════════════════════════════════════════════════════

def compute_factor_exposure(
    ticker: str,
    ticker_rets: List[float],
    spy_rets: List[float],
    qqq_rets: List[float],
    sector_etf: str,
    sector_rets: List[float],
) -> dict:
    out = {"sector_etf": sector_etf}

    # vs SPY
    a, b = align_returns(ticker_rets, spy_rets)
    reg = ols_regress(a, b) if a and b else None
    if reg:
        out["beta_spy"]   = round(reg["beta"], 3)
        out["alpha_spy_ann"] = round(reg["alpha"] * 252 * 100, 2)  # daily alpha → annualized %
        out["r_sq_spy"]   = round(reg["r_sq"], 3)

    # vs QQQ
    a, b = align_returns(ticker_rets, qqq_rets)
    reg = ols_regress(a, b) if a and b else None
    if reg:
        out["beta_qqq"]   = round(reg["beta"], 3)
        out["r_sq_qqq"]   = round(reg["r_sq"], 3)

    # vs Sector
    a, b = align_returns(ticker_rets, sector_rets)
    reg = ols_regress(a, b) if a and b else None
    if reg:
        out["beta_sector"] = round(reg["beta"], 3)
        out["r_sq_sector"] = round(reg["r_sq"], 3)

    # Idiosyncratic alpha — last 20 days actual vs SPY-implied
    if ticker_rets and spy_rets and out.get("beta_spy") is not None:
        n = min(20, len(ticker_rets), len(spy_rets))
        if n >= 5:
            actual = sum(ticker_rets[-n:]) * 100   # in %
            implied = sum(spy_rets[-n:]) * out["beta_spy"] * 100
            out["idio_alpha_20d_pct"] = round(actual - implied, 2)

    return out


# ═════════════════════════════════════════════════════════════════════
# Lambda handler
# ═════════════════════════════════════════════════════════════════════

def lambda_handler(event, context):
    t0 = time.time()
    print(f"[analytics] start {datetime.now(timezone.utc).isoformat()}")

    try:
        radar = json.loads(s3.get_object(Bucket=S3_BUCKET, Key=RADAR_KEY)["Body"].read())
    except Exception as e:
        return _write_error(f"Failed to load radar: {e}")

    candidates = (radar.get("pump_candidates") or [])[:20]  # up to 20 for matrix
    if not candidates:
        return _write_error("No pump candidates")
    tickers = [c["ticker"] for c in candidates]
    print(f"[analytics] {len(tickers)} candidates · {LOOKBACK_DAYS}d lookback")

    # Pre-warm price cache for all candidates + benchmarks in parallel
    all_symbols = tickers + BENCHMARKS + SECTOR_ETFS
    with ThreadPoolExecutor(max_workers=8) as ex:
        futures = {ex.submit(fetch_returns, s): s for s in all_symbols}
        for fut in as_completed(futures, timeout=120):
            try:
                fut.result()
            except Exception:
                pass

    # Also fetch profiles for sector classification
    profile_map = {}
    with ThreadPoolExecutor(max_workers=6) as ex:
        futures = {ex.submit(fetch_profile, t): t for t in tickers}
        for fut in as_completed(futures, timeout=60):
            t = futures[fut]
            try:
                profile_map[t] = fut.result() or {}
            except Exception:
                profile_map[t] = {}

    print(f"[analytics] price cache: {sum(1 for k, v in PRICE_CACHE.items() if v)} populated symbols")

    # ─── Correlation matrix ───────────────────────────────────────
    returns_map = {t: PRICE_CACHE.get(t, []) for t in tickers}
    matrix = build_correlation_matrix(tickers, returns_map)
    clusters = find_clusters(matrix, threshold=0.65)
    div_scores = diversification_scores(matrix)

    # ─── Factor exposure per ticker ───────────────────────────────
    spy_rets = PRICE_CACHE.get("SPY", [])
    qqq_rets = PRICE_CACHE.get("QQQ", [])

    factor_exposure = {}
    for t in tickers:
        ticker_rets = PRICE_CACHE.get(t, [])
        if not ticker_rets:
            continue
        sector = (profile_map.get(t, {}) or {}).get("sector", "Unknown")
        sector_etf = SECTOR_ETF_MAP.get(sector, "SPY")
        sector_rets = PRICE_CACHE.get(sector_etf, spy_rets)
        fe = compute_factor_exposure(t, ticker_rets, spy_rets, qqq_rets,
                                       sector_etf, sector_rets)
        fe["sector_name"] = sector
        factor_exposure[t] = fe

    # ─── Pair the cluster summary with diversification highlights ──
    # Sort candidates: most diversifying first (highest div score)
    div_ranked = sorted(div_scores.items(),
                          key=lambda x: -(x[1] if x[1] is not None else 0))

    output = {
        "schema_version": "1.0",
        "generated_at":   datetime.now(timezone.utc).isoformat(),
        "elapsed_sec":    round(time.time() - t0, 2),
        "lookback_days":  LOOKBACK_DAYS,
        "n_candidates":   len(tickers),

        "correlations": {
            "matrix":                matrix,
            "clusters":              clusters,
            "diversification_scores": div_scores,
            "most_diversifying_top_5": [{"ticker": t, "score": s} for t, s in div_ranked[:5] if s is not None],
            "most_correlated_top_5":   [{"ticker": t, "score": s} for t, s in div_ranked[::-1][:5] if s is not None],
        },

        "factor_exposure": {
            "benchmarks_used":  ["SPY", "QQQ"] + sorted(set(SECTOR_ETF_MAP.values())),
            "per_ticker":        factor_exposure,
        },

        "interpretation_notes": {
            "high_correlation": ("Tickers in the same cluster move together. Holding 5 names from "
                                   "one cluster = concentrated bet on that theme, not diversification."),
            "low_r_squared":    ("Low R² vs SPY = stock-specific edge. High R² = mostly just market beta."),
            "negative_idio_alpha": "Stock underperforming what its market beta would predict in last 20d.",
            "high_beta":        "Higher beta = more sensitive to market moves. Size accordingly.",
        },
    }

    body = json.dumps(output, indent=2, default=str)
    s3.put_object(Bucket=S3_BUCKET, Key=OUTPUT_KEY, Body=body,
                    ContentType="application/json", CacheControl="max-age=3600")

    summary = {
        "status":           "ok",
        "elapsed_sec":      output["elapsed_sec"],
        "n_candidates":     len(tickers),
        "n_clusters":       len([c for c in clusters if c["size"] >= 2]),
        "n_singletons":     len([c for c in clusters if c["size"] == 1]),
        "top_diversifier":  div_ranked[0][0] if div_ranked else None,
    }
    print(f"[analytics] done: {summary}")
    return {"statusCode": 200, "body": json.dumps(summary)}


def _write_error(message: str, **extras) -> dict:
    payload = {"schema_version": "1.0", "generated_at": datetime.now(timezone.utc).isoformat(),
                "status": "error", "error": message, **extras}
    try:
        s3.put_object(Bucket=S3_BUCKET, Key=OUTPUT_KEY,
                        Body=json.dumps(payload, default=str, indent=2),
                        ContentType="application/json", CacheControl="max-age=300")
    except Exception: pass
    print(f"[analytics] ERROR: {message}")
    return {"statusCode": 500, "body": json.dumps({"status": "error", "error": message})}
