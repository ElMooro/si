"""
justhodl-pairs-scanner — Relative-value pairs trading scanner.

WHY THIS EXISTS
───────────────
None of the existing alpha systems do classic statistical-arbitrage
pairs trading. This is the canonical relative-value strategy that
works across regimes and is uncorrelated with directional bets.

ALGORITHM (per pair A, B)
─────────────────────────
  1. Fetch 252 trading days of daily closes for both legs (Polygon)
  2. Compute the ratio: r_t = close_A_t / close_B_t
  3. Rolling 60-day mean(r) and std(r)
  4. Current Z-score: z = (r_now - mean_60d) / std_60d
  5. Estimate half-life via Ornstein-Uhlenbeck regression
     of dr_t on r_{t-1}: half_life = -ln(2) / phi
  6. Compute 252d correlation (sanity check)
  7. Classify spread state:
        |z| < 1 → NORMAL
        |z| 1-2 → STRETCHED
        |z| > 2 → EXTENDED  (mean-reversion candidate)
        |z| > 3 → EXTREME   (high-conviction trade)

PAIR UNIVERSE (curated, ~30 pairs across 7 categories)
──────────────────────────────────────────────────────
  Tech mega-cap, Semiconductors, Banks, Consumer brands, Energy,
  Healthcare, Country ETFs, Style factors, Bond pairs, Commodities,
  Sector vs SPY benchmarks.

  These are picked because they have:
    - Long historical correlation (>0.6 over 5y)
    - Similar business drivers (same sector/region)
    - Comparable market caps OR sector ETF normalization

OUTPUT
──────
  data/pairs-scanner.json
  {
    n_pairs, n_extended, n_extreme,
    pairs: [{name, ticker_a/b, category, ratio, mean_60d, std_60d,
             spread_z, half_life_days, correlation_252d, state,
             trade, rr_estimate, rationale}, ...]
  }

SCHEDULE
────────
  rate(6 hours)  — refreshes 4x daily during market session windows

ZERO DETERIORATION
  ✓ New Lambda + new S3 path + new EB rule
  ✓ Reads only Polygon price data (existing key in env vars)
  ✓ Touches no existing Lambda
"""
import json
import math
import os
import time
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone, timedelta

import boto3

REGION = "us-east-1"
BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
S3_KEY_OUT = os.environ.get("S3_KEY_OUT", "data/pairs-scanner.json")
POLYGON_KEY = os.environ.get("POLYGON_KEY", "zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d")

S3 = boto3.client("s3", region_name=REGION)


# ─────────────────────────────────────────────────────────────────────────────
# THE PAIR UNIVERSE
# ─────────────────────────────────────────────────────────────────────────────
PAIRS = [
    # ── Consumer brand pairs (high correlation, mean-revert reliably) ──
    ("KO", "PEP", "consumer_brand", "Coca-Cola vs Pepsi"),
    ("MCD", "CMG", "consumer_brand", "McDonald's vs Chipotle"),
    ("V", "MA", "payments", "Visa vs Mastercard"),
    ("COST", "WMT", "retail", "Costco vs Walmart"),
    ("HD", "LOW", "home_improvement", "Home Depot vs Lowe's"),
    ("AMZN", "WMT", "retail", "Amazon vs Walmart"),

    # ── Tech mega-cap ──
    ("MSFT", "GOOGL", "tech_mega", "Microsoft vs Google"),
    ("AAPL", "MSFT", "tech_mega", "Apple vs Microsoft"),
    ("META", "GOOGL", "tech_mega", "Meta vs Google"),

    # ── Semiconductors (cyclical mean-reverters) ──
    ("NVDA", "AMD", "semis", "Nvidia vs AMD"),
    ("TSM", "INTC", "semis", "TSMC vs Intel"),
    ("AVGO", "QCOM", "semis", "Broadcom vs Qualcomm"),

    # ── Banks (rate cycle mean-reverters) ──
    ("JPM", "BAC", "banks_money_center", "JPMorgan vs Bank of America"),
    ("GS", "MS", "banks_ibank", "Goldman vs Morgan Stanley"),
    ("C", "WFC", "banks_money_center", "Citi vs Wells Fargo"),

    # ── Energy supermajors ──
    ("XOM", "CVX", "energy", "Exxon vs Chevron"),
    ("COP", "EOG", "energy_eandp", "ConocoPhillips vs EOG"),

    # ── Healthcare ──
    ("JNJ", "PFE", "pharma_big", "Johnson & Johnson vs Pfizer"),
    ("UNH", "CVS", "healthcare_managed", "UnitedHealth vs CVS"),
    ("LLY", "NVO", "pharma_glp1", "Eli Lilly vs Novo Nordisk"),

    # ── Country ETFs (regional pairs) ──
    ("EWZ", "EWW", "country_latam", "Brazil vs Mexico"),
    ("EWG", "EWQ", "country_europe_core", "Germany vs France"),
    ("EWP", "EWI", "country_piigs", "Spain vs Italy"),
    ("EWY", "EWT", "country_asia_tech", "Korea vs Taiwan"),

    # ── Style factors ──
    ("IWM", "SPY", "style_smallcap_vs_largecap", "Russell 2000 vs S&P 500"),
    ("IWF", "IWD", "style_growth_vs_value", "Russell Growth vs Value"),
    ("MTUM", "VLUE", "style_momentum_vs_value", "Momentum vs Value Factor"),
    ("QQQ", "SPY", "style_tech_vs_market", "Nasdaq 100 vs S&P 500"),

    # ── Bond pairs ──
    ("TLT", "IEF", "bonds_long_vs_int", "20Y Treasury vs 7-10Y Treasury"),
    ("HYG", "LQD", "bonds_hy_vs_ig", "High Yield vs Investment Grade"),

    # ── Commodities ──
    ("GLD", "SLV", "metals_gold_vs_silver", "Gold vs Silver"),
    ("USO", "DBC", "commodities_oil_vs_broad", "Oil vs Broad Commodities"),
    ("GDX", "GLD", "metals_miners_vs_metal", "Gold Miners vs Gold"),

    # ── Sector vs SPY (sector rotation candidates) ──
    ("XLF", "SPY", "sector_finl_vs_spy", "Financials vs S&P"),
    ("XLE", "SPY", "sector_energy_vs_spy", "Energy vs S&P"),
    ("XLU", "SPY", "sector_util_vs_spy", "Utilities vs S&P"),
    ("XLP", "SPY", "sector_staples_vs_spy", "Staples vs S&P"),
]


# ─────────────────────────────────────────────────────────────────────────────
# DATA FETCHING (Polygon)
# ─────────────────────────────────────────────────────────────────────────────
def fetch_polygon_closes(ticker, days=300):
    """Fetch daily closes for last N trading days. Returns list of floats (oldest→newest)."""
    end = datetime.now(timezone.utc).date().isoformat()
    start = (datetime.now(timezone.utc).date() - timedelta(days=days + 60)).isoformat()
    url = (
        f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/day/{start}/{end}"
        f"?adjusted=true&sort=asc&limit=400&apiKey={POLYGON_KEY}"
    )
    try:
        with urllib.request.urlopen(url, timeout=12) as resp:
            data = json.loads(resp.read().decode("utf-8"))
        results = data.get("results") or []
        closes = [r["c"] for r in results if r.get("c") is not None]
        return closes
    except Exception as e:
        print(f"[pairs] Polygon fail {ticker}: {e}")
        return []


def fetch_all_tickers_parallel(tickers):
    """Parallelize fetches."""
    results = {}
    with ThreadPoolExecutor(max_workers=8) as ex:
        futures = {ex.submit(fetch_polygon_closes, t): t for t in tickers}
        for fut in as_completed(futures):
            t = futures[fut]
            try:
                results[t] = fut.result()
            except Exception as e:
                print(f"[pairs] thread fail {t}: {e}")
                results[t] = []
    return results


# ─────────────────────────────────────────────────────────────────────────────
# STATISTICS (no scipy/numpy; pure Python for tiny zip + cold-start speed)
# ─────────────────────────────────────────────────────────────────────────────
def mean(xs):
    return sum(xs) / len(xs) if xs else 0.0


def stdev(xs, m=None):
    if len(xs) < 2:
        return 0.0
    m = mean(xs) if m is None else m
    var = sum((x - m) ** 2 for x in xs) / (len(xs) - 1)
    return math.sqrt(var)


def correlation(xs, ys):
    """Pearson correlation."""
    n = min(len(xs), len(ys))
    if n < 5:
        return 0.0
    xs, ys = xs[-n:], ys[-n:]
    mx, my = mean(xs), mean(ys)
    sx, sy = stdev(xs, mx), stdev(ys, my)
    if sx == 0 or sy == 0:
        return 0.0
    cov = sum((x - mx) * (y - my) for x, y in zip(xs, ys)) / (n - 1)
    return cov / (sx * sy)


def estimate_half_life(series, max_lookback=60):
    """OU-style half-life: dr_t = phi * (r_{t-1} - mean) + eps; HL = -ln(2)/ln(1+phi)."""
    if len(series) < 30:
        return None
    s = series[-max_lookback:] if len(series) > max_lookback else series
    m = mean(s)
    # Compute lag-1 autocorrelation as a proxy for phi+1
    deltas = [s[i] - s[i-1] for i in range(1, len(s))]
    lagged = [s[i-1] - m for i in range(1, len(s))]
    if not deltas or not lagged:
        return None
    sx = stdev(lagged)
    if sx == 0:
        return None
    sy = stdev(deltas)
    if sy == 0:
        return None
    # OLS slope of deltas on lagged (without intercept)
    num = sum(l * d for l, d in zip(lagged, deltas))
    den = sum(l * l for l in lagged)
    if den == 0:
        return None
    phi = num / den
    if phi >= 0:
        return None  # not mean-reverting
    half_life = -math.log(2) / math.log(1 + phi) if (1 + phi) > 0 and phi > -1 else None
    return round(half_life, 1) if half_life and half_life > 0 else None


# ─────────────────────────────────────────────────────────────────────────────
# PAIR ANALYSIS
# ─────────────────────────────────────────────────────────────────────────────
def analyze_pair(name, ticker_a, ticker_b, category, prices):
    closes_a = prices.get(ticker_a, [])
    closes_b = prices.get(ticker_b, [])

    if len(closes_a) < 80 or len(closes_b) < 80:
        return {
            "name": name,
            "ticker_a": ticker_a, "ticker_b": ticker_b,
            "category": category,
            "status": "insufficient_data",
            "n_a": len(closes_a), "n_b": len(closes_b),
        }

    # Align lengths
    n = min(len(closes_a), len(closes_b))
    a = closes_a[-n:]
    b = closes_b[-n:]

    # Compute ratios
    ratios = [a[i] / b[i] for i in range(n) if b[i] > 0]

    # Rolling 60d window for current spread Z
    last60_ratios = ratios[-60:]
    if len(last60_ratios) < 30:
        return {
            "name": name, "ticker_a": ticker_a, "ticker_b": ticker_b,
            "category": category, "status": "insufficient_data",
        }

    mean_60 = mean(last60_ratios)
    std_60 = stdev(last60_ratios, mean_60)
    cur_ratio = ratios[-1]
    z = (cur_ratio - mean_60) / std_60 if std_60 > 0 else 0.0

    # 252d correlation as quality check
    corr = correlation(a, b) if n >= 100 else None

    # Half-life of mean reversion
    hl = estimate_half_life(ratios, max_lookback=120)

    # 30d returns of each leg
    ret_30d_a = (a[-1] / a[-21] - 1) * 100 if len(a) >= 22 else None
    ret_30d_b = (b[-1] / b[-21] - 1) * 100 if len(b) >= 22 else None

    # Classify state
    abs_z = abs(z)
    if abs_z >= 3:
        state = "EXTREME"
    elif abs_z >= 2:
        state = "EXTENDED"
    elif abs_z >= 1:
        state = "STRETCHED"
    else:
        state = "NORMAL"

    # Trade direction
    trade = None
    if abs_z >= 2:
        if z > 0:
            trade = f"SHORT {ticker_a} / LONG {ticker_b}"
            trade_label = f"SHORT_{ticker_a}_LONG_{ticker_b}"
        else:
            trade = f"LONG {ticker_a} / SHORT {ticker_b}"
            trade_label = f"LONG_{ticker_a}_SHORT_{ticker_b}"
    else:
        trade = None
        trade_label = "NO_TRADE"

    # Risk-reward estimate (target = revert to mean, stop = +/- 1 sigma further)
    if abs_z >= 2 and std_60 > 0 and cur_ratio > 0:
        target_pct = abs(cur_ratio - mean_60) / cur_ratio * 100  # to mean
        stop_pct = std_60 / cur_ratio * 100  # 1 std further out = stop
        rr_estimate = round(target_pct / stop_pct, 1) if stop_pct > 0 else None
    else:
        rr_estimate = None

    # Rationale
    if state in ("EXTREME", "EXTENDED"):
        rationale = (
            f"{name} ratio is {abs_z:.2f}σ from 60d mean "
            f"({cur_ratio:.4f} vs μ={mean_60:.4f}). "
            f"30d: {ticker_a} {ret_30d_a:+.1f}% vs {ticker_b} {ret_30d_b:+.1f}%. "
            f"Mean-reversion candidate with R:R≈{rr_estimate}:1"
            f"{f', half-life {hl:.0f}d' if hl else ''}."
        )
    elif state == "STRETCHED":
        rationale = (
            f"Spread approaching 2σ ({z:+.2f}σ from mean). Watch for "
            f"continuation to extreme or reversal."
        )
    else:
        rationale = f"Spread within normal range ({z:+.2f}σ)."

    return {
        "name": name,
        "ticker_a": ticker_a, "ticker_b": ticker_b,
        "category": category,
        "status": "ok",
        "ratio": round(cur_ratio, 6),
        "mean_60d": round(mean_60, 6),
        "std_60d": round(std_60, 6),
        "spread_z": round(z, 3),
        "abs_z": round(abs_z, 3),
        "state": state,
        "trade": trade,
        "trade_label": trade_label,
        "rr_estimate": rr_estimate,
        "half_life_days": hl,
        "correlation_252d": round(corr, 3) if corr is not None else None,
        "price_a": round(a[-1], 2),
        "price_b": round(b[-1], 2),
        "ret_30d_a": round(ret_30d_a, 2) if ret_30d_a is not None else None,
        "ret_30d_b": round(ret_30d_b, 2) if ret_30d_b is not None else None,
        "n_obs": len(ratios),
        "rationale": rationale,
    }


def lambda_handler(event, context):
    started = time.time()

    # 1. Collect unique tickers
    tickers = set()
    for a, b, _cat, _name in PAIRS:
        tickers.add(a)
        tickers.add(b)
    print(f"[pairs] fetching {len(tickers)} unique tickers across {len(PAIRS)} pairs")

    # 2. Parallel fetch
    fetch_started = time.time()
    prices = fetch_all_tickers_parallel(tickers)
    fetch_duration = round(time.time() - fetch_started, 1)
    n_ok = sum(1 for v in prices.values() if len(v) >= 80)
    n_fail = len(tickers) - n_ok
    print(f"[pairs] fetch done: {n_ok} ok, {n_fail} fail in {fetch_duration}s")

    # 3. Analyze each pair
    pairs_out = []
    for ticker_a, ticker_b, category, name in PAIRS:
        pairs_out.append(analyze_pair(name, ticker_a, ticker_b, category, prices))

    # 4. Sort: ok pairs by abs_z desc, insufficient at bottom
    ok_pairs = [p for p in pairs_out if p.get("status") == "ok"]
    bad_pairs = [p for p in pairs_out if p.get("status") != "ok"]
    ok_pairs.sort(key=lambda p: p.get("abs_z") or 0, reverse=True)

    # 5. Summary
    n_extreme = sum(1 for p in ok_pairs if p.get("state") == "EXTREME")
    n_extended = sum(1 for p in ok_pairs if p.get("state") == "EXTENDED")
    n_stretched = sum(1 for p in ok_pairs if p.get("state") == "STRETCHED")
    n_normal = sum(1 for p in ok_pairs if p.get("state") == "NORMAL")

    by_category = {}
    for p in ok_pairs:
        c = p.get("category", "?")
        by_category.setdefault(c, {"total": 0, "extended": 0, "extreme": 0})
        by_category[c]["total"] += 1
        if p.get("state") == "EXTENDED":
            by_category[c]["extended"] += 1
        if p.get("state") == "EXTREME":
            by_category[c]["extreme"] += 1

    payload = {
        "schema_version": "1.0",
        "method": "pairs_scanner_v1",
        "as_of": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "duration_s": round(time.time() - started, 2),
        "fetch_stats": {
            "n_tickers": len(tickers), "n_ok": n_ok, "n_fail": n_fail,
            "fetch_duration_s": fetch_duration,
        },
        "summary": {
            "n_pairs": len(PAIRS),
            "n_analyzed": len(ok_pairs),
            "n_insufficient": len(bad_pairs),
            "n_extreme": n_extreme,
            "n_extended": n_extended,
            "n_stretched": n_stretched,
            "n_normal": n_normal,
            "by_category": by_category,
            "top_5_dislocations": [
                {
                    "name": p["name"], "trade": p.get("trade"),
                    "spread_z": p["spread_z"], "state": p["state"],
                    "rr": p.get("rr_estimate"),
                }
                for p in ok_pairs[:5]
            ],
        },
        "pairs": ok_pairs + bad_pairs,
    }

    body = json.dumps(payload, indent=2, default=str).encode()
    S3.put_object(
        Bucket=BUCKET, Key=S3_KEY_OUT, Body=body,
        ContentType="application/json",
        CacheControl="public, max-age=900",
    )

    return {
        "statusCode": 200,
        "body": json.dumps({
            "ok": True,
            "n_pairs": len(PAIRS),
            "n_analyzed": len(ok_pairs),
            "n_extreme": n_extreme,
            "n_extended": n_extended,
            "top_5": [p["name"] for p in ok_pairs[:5]],
            "duration_s": payload["duration_s"],
        }),
    }
