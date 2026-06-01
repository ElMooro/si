"""
justhodl-supply-inflection-scanner  (Layer 2 of nobrainer hunter)
=================================================================
Scans hard-data inputs that drive theme demand/supply tightness. The thesis:
the market often misses tier-2/3 plays because the underlying input data
(memory spot prices, lithium spot, ISM Prices Paid subcomponent, etc.) is
unsexy and unwatched. When such an input inflects, the picks-and-shovels
beneficiaries are the asymmetric trade.

This Lambda:
  1. Pulls 22 hard-data inflection signals from FRED + Polygon
  2. Computes 30/90/180-day % changes + 90-day percentile rank
  3. Scores each signal 0-100 for "tightening direction × magnitude"
  4. Maps each signal to themes that benefit (e.g., DRAM tightening → tech_semis)
  5. Aggregates per-theme inflection scores

Schedule: cron(0 7 * * ? *) — daily 07:00 UTC (after theme-detector at 06:00)
Input:    s3://justhodl-dashboard-live/data/themes-detected.json (Layer 1)
Output:   s3://justhodl-dashboard-live/data/supply-inflection.json

The downstream Layer 4 (asymmetric-hunter) consumes this to weight the
supply_inflection_score in its 5-factor scorecard.
"""
import json
import os
import time
import urllib.request
import urllib.error
from datetime import datetime, timezone, timedelta
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed

import boto3
import _fred_shim  # noqa: F401  — cache-first FRED + 429 backoff (ops/1074)

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
POLYGON_KEY = os.environ.get("POLYGON_KEY", "zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d")
POLYGON_BASE = "https://api.polygon.io"
FRED_KEY = os.environ.get("FRED_KEY", "2f057499936072679d8843d7fce99989")
FRED_BASE = "https://api.stlouisfed.org/fred"

S3 = boto3.client("s3", region_name=REGION)


# ─────────────────────────────────────────────────────────────────────────────
# INFLECTION SIGNAL CATALOG
# ─────────────────────────────────────────────────────────────────────────────
# Each signal:
#   • src           "polygon" or "fred"
#   • symbol/series ticker for polygon, FRED series_id for fred
#   • direction     "up_is_tight" or "down_is_tight"
#                   (e.g., DRAM price up = supply tight = bullish for MU;
#                    Inventory days down = supply tight = bullish)
#   • themes        list of theme ETFs this signal drives
#   • description   human-readable
#   • category      grouping for UI
#
# To add new signals, just append. Lambda auto-handles them.
INFLECTION_SIGNALS = {
    # ── COMMODITIES (proxied via ETFs/spot tickers Polygon supports) ──
    "OIL_WTI": {
        "src": "polygon", "symbol": "USO",
        "direction": "up_is_tight",
        "themes": ["XLE", "XOP", "OIH", "AMLP"],
        "description": "WTI crude proxy via USO ETF",
        "category": "commodity_energy",
    },
    "OIL_BRENT": {
        "src": "polygon", "symbol": "BNO",
        "direction": "up_is_tight",
        "themes": ["XLE", "XOP"],
        "description": "Brent crude proxy via BNO ETF",
        "category": "commodity_energy",
    },
    "NATGAS": {
        "src": "polygon", "symbol": "UNG",
        "direction": "up_is_tight",
        "themes": ["XLE", "AMLP"],
        "description": "Henry Hub natgas proxy via UNG ETF",
        "category": "commodity_energy",
    },
    "URANIUM": {
        "src": "polygon", "symbol": "URA",
        "direction": "up_is_tight",
        "themes": ["URA", "URNM", "NLR"],
        "description": "Uranium proxy via URA ETF (CCJ + DNN top weights)",
        "category": "commodity_nuclear",
    },
    "LITHIUM": {
        "src": "polygon", "symbol": "LIT",
        "direction": "up_is_tight",
        "themes": ["LIT", "REMX"],
        "description": "Lithium / battery proxy via LIT ETF",
        "category": "commodity_battery",
    },
    "COPPER": {
        "src": "polygon", "symbol": "COPX",
        "direction": "up_is_tight",
        "themes": ["COPX", "PICK", "GRID"],
        "description": "Copper proxy via COPX miners ETF",
        "category": "commodity_base",
    },
    "RARE_EARTH": {
        "src": "polygon", "symbol": "REMX",
        "direction": "up_is_tight",
        "themes": ["REMX", "ITA"],
        "description": "Rare earth proxy via REMX (MP, TROX, ALB)",
        "category": "commodity_strategic",
    },
    "GOLD": {
        "src": "polygon", "symbol": "GLD",
        "direction": "up_is_tight",
        "themes": ["GDX", "GDXJ"],
        "description": "Gold spot proxy via GLD ETF",
        "category": "commodity_precious",
    },
    "SILVER": {
        "src": "polygon", "symbol": "SLV",
        "direction": "up_is_tight",
        "themes": ["SIL"],
        "description": "Silver spot proxy via SLV ETF",
        "category": "commodity_precious",
    },
    "STEEL_BASKET": {
        "src": "polygon", "symbol": "SLX",
        "direction": "up_is_tight",
        "themes": ["SLX", "PICK"],
        "description": "Steel proxy via SLX ETF (NUE, STLD, X)",
        "category": "commodity_industrial",
    },
    "LUMBER": {
        "src": "polygon", "symbol": "WOOD",
        "direction": "up_is_tight",
        "themes": ["WOOD", "XHB"],
        "description": "Lumber proxy via WOOD ETF",
        "category": "commodity_building",
    },
    # ── SEMI / MEMORY (the MU/SNDK pattern) ──
    "MEMORY_DEMAND": {
        "src": "polygon", "symbol": "MU",
        "direction": "up_is_tight",
        "themes": ["SMH", "SOXX", "AIQ", "BOTZ"],
        "description": "Memory demand proxy via MU stock (HBM+DRAM bellwether)",
        "category": "tech_input",
    },
    "AI_INFRA_PROXY": {
        "src": "polygon", "symbol": "NVDA",
        "direction": "up_is_tight",
        "themes": ["AIQ", "BOTZ", "ROBO", "SMH"],
        "description": "AI compute demand proxy via NVDA",
        "category": "tech_input",
    },
    # ── POWER / GRID (AI datacenter electricity) ──
    "AI_POWER_PROXY": {
        "src": "polygon", "symbol": "VST",
        "direction": "up_is_tight",
        "themes": ["NLR", "GRID", "XLU"],
        "description": "AI datacenter power demand via VST (largest US power producer)",
        "category": "power_grid",
    },
    # ── DEFENSE BACKLOG ──
    "DEFENSE_DEMAND": {
        "src": "polygon", "symbol": "LMT",
        "direction": "up_is_tight",
        "themes": ["ITA", "PPA"],
        "description": "Defense rearmament demand via LMT",
        "category": "industrial_input",
    },
    # ── MACRO MANUFACTURING (FRED) ──
    "ISM_PMI": {
        "src": "fred", "symbol": "MANEMP",
        "direction": "up_is_tight",
        "themes": ["XLI", "PAVE", "AIRR"],
        "description": "US Manufacturing Employment (FRED MANEMP)",
        "category": "macro_manufacturing",
    },
    "INDUSTRIAL_PROD": {
        "src": "fred", "symbol": "INDPRO",
        "direction": "up_is_tight",
        "themes": ["XLI", "PAVE", "SLX", "COPX"],
        "description": "US Industrial Production Index",
        "category": "macro_manufacturing",
    },
    "TRUCK_TONNAGE": {
        "src": "fred", "symbol": "TRUCKD11",
        "direction": "up_is_tight",
        "themes": ["IYT", "XLI"],
        "description": "ATA Truck Tonnage Index — goods cycle",
        "category": "macro_logistics",
    },
    "RIG_COUNT": {
        "src": "fred", "symbol": "IPN213111N",
        "direction": "up_is_tight",
        "themes": ["XOP", "OIH"],
        "description": "US Oil & Gas Drilling Index (rig activity proxy)",
        "category": "macro_energy",
    },
    # ── MACRO RATES / FX ──
    "TREASURY_10Y": {
        "src": "fred", "symbol": "DGS10",
        "direction": "down_is_tight",  # Lower yields = easier credit = bullish risk
        "themes": ["XLRE", "REZ", "REM", "INDS", "XHB", "XBI", "ARKK"],
        "description": "10-year Treasury yield — duration-sensitive themes",
        "category": "macro_rates",
    },
    "DOLLAR_INDEX": {
        "src": "fred", "symbol": "DTWEXBGS",
        "direction": "down_is_tight",  # Weak dollar = bullish commodities, EM
        "themes": ["GDX", "GDXJ", "REMX", "EEM", "USO"],
        "description": "Trade-weighted USD — inverse to commodities and EM",
        "category": "macro_fx",
    },
    "REAL_RATES": {
        "src": "fred", "symbol": "DFII10",
        "direction": "down_is_tight",  # Lower real rates = bullish gold and risk
        "themes": ["GDX", "GDXJ", "ARKK", "XBI"],
        "description": "10y TIPS real yield — gold/risk-asset driver",
        "category": "macro_rates",
    },
}


# ─────────────────────────────────────────────────────────────────────────────
# DATA FETCHERS
# ─────────────────────────────────────────────────────────────────────────────
def fetch_polygon_aggs(ticker, days_back=400):
    end = datetime.now(timezone.utc).date()
    start = end - timedelta(days=days_back)
    url = (
        f"{POLYGON_BASE}/v2/aggs/ticker/{ticker}/range/1/day/"
        f"{start.isoformat()}/{end.isoformat()}"
        f"?adjusted=true&sort=asc&limit=5000&apiKey={POLYGON_KEY}"
    )
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "justhodl-supply-inflection/1.0"})
        with urllib.request.urlopen(req, timeout=20) as resp:
            data = json.loads(resp.read().decode())
        results = data.get("results") or []
        if not results:
            print(f"[poly] {ticker} no_results status={data.get('status')} count={len(results)} err={data.get('error')}")
            return []
        return [{"date": datetime.fromtimestamp(r["t"]/1000, tz=timezone.utc).date(), "close": r["c"]} for r in results]
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", errors="replace")[:400] if hasattr(e, "read") else ""
        print(f"[poly] {ticker} HTTPError {e.code} body={body}")
        return []
    except urllib.error.URLError as e:
        print(f"[poly] {ticker} URLError {e.reason}")
        return []
    except Exception as e:
        print(f"[poly] {ticker} other_error {type(e).__name__} {e}")
        return []


def fetch_fred(series_id, days_back=600, retries=4):
    """Fetch FRED series with retry/backoff on 5xx and 429."""
    end = datetime.now(timezone.utc).date()
    start = end - timedelta(days=days_back)
    url = (
        f"{FRED_BASE}/series/observations"
        f"?series_id={series_id}&api_key={FRED_KEY}"
        f"&observation_start={start.isoformat()}&observation_end={end.isoformat()}"
        f"&file_type=json&sort_order=asc"
    )
    last_err = None
    for attempt in range(retries):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "justhodl-supply-inflection/1.0"})
            with urllib.request.urlopen(req, timeout=20) as resp:
                data = json.loads(resp.read().decode())
            obs = data.get("observations") or []
            if not obs:
                print(f"[fred] {series_id} no observations")
                return []
            out = []
            for o in obs:
                try:
                    v = float(o["value"])
                    d = datetime.fromisoformat(o["date"]).date()
                    out.append({"date": d, "close": v})
                except (ValueError, TypeError):
                    continue  # FRED uses "." for missing values
            return out
        except urllib.error.HTTPError as e:
            last_err = f"HTTP{e.code}"
            if e.code in (429, 500, 502, 503, 504):
                wait = 0.7 * (2 ** attempt)
                print(f"[fred] {series_id} {last_err} retry {attempt+1}/{retries} wait={wait:.1f}s")
                time.sleep(wait)
                continue
            body = e.read().decode("utf-8", errors="replace")[:200] if hasattr(e, "read") else ""
            print(f"[fred] {series_id} HTTPError {e.code} body={body}")
            return []
        except urllib.error.URLError as e:
            last_err = f"URL:{e.reason}"
            time.sleep(0.7 * (2 ** attempt))
            continue
        except Exception as e:
            last_err = f"{type(e).__name__}:{e}"
            time.sleep(0.5)
            continue
    print(f"[fred] {series_id} all_retries_failed err={last_err}")
    return []


def fetch_signal(name, spec):
    """Dispatch fetch based on src."""
    if spec["src"] == "polygon":
        bars = fetch_polygon_aggs(spec["symbol"])
    elif spec["src"] == "fred":
        bars = fetch_fred(spec["symbol"])
    else:
        bars = []
    return name, bars


# ─────────────────────────────────────────────────────────────────────────────
# METRIC COMPUTATION
# ─────────────────────────────────────────────────────────────────────────────
def pct_change(series, days_back):
    """% change over `days_back` calendar days (use latest available bar before that)."""
    if not series or len(series) < 2:
        return None
    latest = series[-1]
    target_date = latest["date"] - timedelta(days=days_back)
    # find closest bar on or before target_date
    candidates = [b for b in series if b["date"] <= target_date]
    if not candidates:
        return None
    base = candidates[-1]
    if base["close"] == 0:
        return None
    return (latest["close"] / base["close"] - 1.0) * 100


def percentile_rank(series, lookback_days, current_value):
    """What percentile is current_value in vs last `lookback_days` of values?"""
    if not series:
        return None
    cutoff = series[-1]["date"] - timedelta(days=lookback_days)
    window = [b["close"] for b in series if b["date"] >= cutoff]
    if len(window) < 5:
        return None
    rank = sum(1 for v in window if v < current_value)
    return round(100.0 * rank / len(window), 1)


def realized_vol(series, lookback_days=90):
    """Annualized realized vol over lookback window. None if insufficient data."""
    if len(series) < lookback_days + 1:
        return None
    window = series[-(lookback_days + 1):]
    rets = []
    for i in range(1, len(window)):
        if window[i-1]["close"] == 0:
            continue
        r = window[i]["close"] / window[i-1]["close"] - 1.0
        rets.append(r)
    if len(rets) < 5:
        return None
    n = len(rets)
    mean = sum(rets) / n
    var = sum((r - mean) ** 2 for r in rets) / max(n - 1, 1)
    std = var ** 0.5
    return std * (252 ** 0.5) * 100  # %


def compute_metrics(bars):
    """Compute all metric snapshots from bar history."""
    if not bars:
        return None
    latest = bars[-1]
    return {
        "latest_value": round(latest["close"], 4),
        "latest_date": latest["date"].isoformat(),
        "n_bars": len(bars),
        "pct_change_30d": _round(pct_change(bars, 30)),
        "pct_change_90d": _round(pct_change(bars, 90)),
        "pct_change_180d": _round(pct_change(bars, 180)),
        "pct_change_365d": _round(pct_change(bars, 365)),
        "percentile_90d": percentile_rank(bars, 90, latest["close"]),
        "percentile_365d": percentile_rank(bars, 365, latest["close"]),
        "realized_vol_90d": _round(realized_vol(bars, 90)),
    }


def _round(v, n=2):
    if v is None:
        return None
    return round(v, n)


# ─────────────────────────────────────────────────────────────────────────────
# INFLECTION SCORING
# ─────────────────────────────────────────────────────────────────────────────
def score_inflection(metrics, direction):
    """
    Returns 0-100 inflection score where 100 = "max tight, big move, high percentile".

    Components (each 0-100):
      • Magnitude:   absolute % change on aligned direction (capped)
      • Persistence: aligned direction across 30d AND 90d
      • Percentile:  current value vs 1-year history
      • Acceleration:30d move > 90d-annualized rate

    Final = 0.30*mag + 0.25*persist + 0.30*pct + 0.15*accel
    """
    if not metrics:
        return 0, "no_data"

    pct_30 = metrics.get("pct_change_30d") or 0
    pct_90 = metrics.get("pct_change_90d") or 0
    pct_180 = metrics.get("pct_change_180d") or 0
    pctl_365 = metrics.get("percentile_365d")

    # Sign multiplier — "up_is_tight" means positive moves contribute positively
    sign = +1 if direction == "up_is_tight" else -1

    # Magnitude: 30d move sign-aligned, scaled (15% move = full credit)
    mag = max(0.0, min(100.0, sign * pct_30 * (100/15.0)))

    # Persistence: both 30d and 90d aligned in same direction
    persist = 0
    if (sign * pct_30 > 0) and (sign * pct_90 > 0):
        persist = 80
    if (sign * pct_30 > 0) and (sign * pct_90 > 0) and (sign * pct_180 > 0):
        persist = 100
    if (sign * pct_30 > 0) and (sign * pct_90 < 0):
        persist = 30  # divergent — early reversal signal

    # Percentile: for up_is_tight, high percentile (>80) is tight.
    #   for down_is_tight (rates), low percentile (<20) is tight.
    pct_score = 0
    if pctl_365 is not None:
        if direction == "up_is_tight":
            pct_score = pctl_365  # 0-100 directly
        else:
            pct_score = 100 - pctl_365

    # Acceleration: 30d annualized rate > 90d annualized rate
    accel = 0
    if pct_30 is not None and pct_90 is not None:
        # 30d % * (365/30) vs 90d % * (365/90)
        rate_30 = pct_30 * (365.0/30.0)
        rate_90 = pct_90 * (365.0/90.0)
        if sign * (rate_30 - rate_90) > 0:
            # accelerating in the tightening direction
            gap = abs(rate_30 - rate_90)
            accel = min(100.0, gap * 1.0)  # 100bps gap = 100 score

    raw = 0.30 * mag + 0.25 * persist + 0.30 * pct_score + 0.15 * accel
    score = round(max(0.0, min(100.0, raw)), 1)

    # Verbal flag
    if score >= 75:
        flag = "STRONG_TIGHTENING"
    elif score >= 55:
        flag = "TIGHTENING"
    elif score >= 35:
        flag = "NEUTRAL"
    elif score >= 20:
        flag = "EASING"
    else:
        flag = "STRONG_EASING"

    return score, flag


# ─────────────────────────────────────────────────────────────────────────────
# THEME AGGREGATION
# ─────────────────────────────────────────────────────────────────────────────
def aggregate_by_theme(signal_results):
    """For each theme ETF, aggregate the scores of all signals that map to it."""
    by_theme = defaultdict(list)
    for sig_name, sig_data in signal_results.items():
        if sig_data is None or sig_data.get("score") is None:
            continue
        spec = INFLECTION_SIGNALS[sig_name]
        for theme_etf in spec["themes"]:
            by_theme[theme_etf].append({
                "signal": sig_name,
                "score": sig_data["score"],
                "flag": sig_data["flag"],
                "direction": spec["direction"],
                "category": spec["category"],
                "description": spec["description"],
                "metrics": sig_data["metrics"],
            })

    out = {}
    for theme_etf, signals in by_theme.items():
        if not signals:
            continue
        # Theme inflection score = max(individual scores) weighted by count
        # (a single 90+ signal is plenty to qualify as "tight")
        scores = [s["score"] for s in signals]
        max_score = max(scores)
        avg_score = sum(scores) / len(scores)
        # Use max-weighted-avg (favor high-conviction single signal)
        composite = round(0.6 * max_score + 0.4 * avg_score, 1)

        n_strong = sum(1 for s in signals if s["score"] >= 75)
        n_tightening = sum(1 for s in signals if s["score"] >= 55)

        # Sort signals by score desc
        signals_sorted = sorted(signals, key=lambda s: -s["score"])

        out[theme_etf] = {
            "composite_inflection_score": composite,
            "max_signal_score": max_score,
            "avg_signal_score": round(avg_score, 1),
            "n_signals": len(signals),
            "n_strong_tightening": n_strong,
            "n_tightening": n_tightening,
            "top_signals": signals_sorted[:5],
            "all_signals": signals_sorted,
        }
    return out


# ─────────────────────────────────────────────────────────────────────────────
# MAIN HANDLER
# ─────────────────────────────────────────────────────────────────────────────
def lambda_handler(event=None, context=None):
    started = time.time()
    print(f"[supply-inflection] scanning {len(INFLECTION_SIGNALS)} signals")

    # Parallel-fetch all signals
    raw_bars = {}
    n_ok, n_fail = 0, 0
    fetch_started = time.time()
    with ThreadPoolExecutor(max_workers=10) as pool:
        futures = {pool.submit(fetch_signal, name, spec): name for name, spec in INFLECTION_SIGNALS.items()}
        for fut in as_completed(futures):
            try:
                name, bars = fut.result()
                if bars and len(bars) >= 30:
                    raw_bars[name] = bars
                    n_ok += 1
                else:
                    n_fail += 1
                    print(f"[fetch-fail] {name} bars={len(bars) if bars else 0}")
            except Exception as e:
                n_fail += 1
                print(f"[fetch-error] {futures[fut]} {type(e).__name__} {e}")
    fetch_dur = round(time.time() - fetch_started, 1)
    print(f"[supply-inflection] fetched {n_ok} ok / {n_fail} failed in {fetch_dur}s")

    # Compute metrics + score for each signal
    signal_results = {}
    for name, bars in raw_bars.items():
        spec = INFLECTION_SIGNALS[name]
        metrics = compute_metrics(bars)
        score, flag = score_inflection(metrics, spec["direction"])
        signal_results[name] = {
            "name": name,
            "src": spec["src"],
            "symbol": spec["symbol"],
            "category": spec["category"],
            "direction": spec["direction"],
            "themes": spec["themes"],
            "description": spec["description"],
            "metrics": metrics,
            "score": score,
            "flag": flag,
        }

    # Aggregate by theme
    by_theme = aggregate_by_theme(signal_results)

    # Build summary: which themes have the strongest supply-side inflection?
    theme_ranking = sorted(by_theme.items(), key=lambda kv: -kv[1]["composite_inflection_score"])
    top_inflecting = [
        {"theme": etf, "score": data["composite_inflection_score"],
         "n_strong": data["n_strong_tightening"], "n_tightening": data["n_tightening"]}
        for etf, data in theme_ranking[:15]
    ]

    # Top signals across all themes
    all_signals_sorted = sorted(signal_results.values(), key=lambda s: -s["score"])
    top_signals = [
        {"name": s["name"], "symbol": s["symbol"], "score": s["score"], "flag": s["flag"],
         "themes": s["themes"], "description": s["description"]}
        for s in all_signals_sorted[:15]
    ]

    output = {
        "schema_version": "1.0",
        "method": "supply_inflection_scanner_v1",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "duration_s": round(time.time() - started, 1),
        "fetch_stats": {
            "n_signals": len(INFLECTION_SIGNALS),
            "n_ok": n_ok,
            "n_fail": n_fail,
            "fetch_duration_s": fetch_dur,
        },
        "summary": {
            "n_signals_scored": len(signal_results),
            "n_strong_tightening": sum(1 for s in signal_results.values() if s["score"] >= 75),
            "n_tightening": sum(1 for s in signal_results.values() if s["score"] >= 55),
            "n_easing": sum(1 for s in signal_results.values() if s["score"] < 35),
            "top_signals": top_signals,
            "top_inflecting_themes": top_inflecting,
        },
        "signals": signal_results,
        "by_theme": by_theme,
        "schema": {
            "description": (
                "Layer 2 of nobrainer hunter pipeline. Pulls 22 hard-data inputs "
                "(commodity prices, semi/AI demand proxies, ISM/manufacturing FRED "
                "series, rates/FX) and scores each for 'tightening direction × "
                "magnitude × percentile × acceleration'. Maps to themes from Layer "
                "1 so Layer 4 (asymmetric-hunter) can weight supply_inflection_score."
            ),
            "score_components": {
                "magnitude_30d": "30%",
                "persistence_30d_90d_180d": "25%",
                "percentile_vs_1yr": "30%",
                "acceleration_30d_vs_90d": "15%",
            },
            "flags": {
                "STRONG_TIGHTENING": ">=75",
                "TIGHTENING": "55-74",
                "NEUTRAL": "35-54",
                "EASING": "20-34",
                "STRONG_EASING": "<20",
            },
        },
    }

    body = json.dumps(output, default=str)
    S3.put_object(
        Bucket=BUCKET,
        Key="data/supply-inflection.json",
        Body=body.encode("utf-8"),
        ContentType="application/json",
        CacheControl="max-age=60, public",
    )
    print(f"[supply-inflection] wrote {len(body)}b to data/supply-inflection.json")
    print(f"[supply-inflection] top tightening: {[s['name'] + ':' + str(s['score']) for s in top_signals[:5]]}")
    print(f"[supply-inflection] top theme inflections: {[(t['theme'], t['score']) for t in top_inflecting[:5]]}")

    return {
        "statusCode": 200,
        "body": json.dumps({
            "n_signals": len(signal_results),
            "n_strong_tightening": output["summary"]["n_strong_tightening"],
            "n_tightening": output["summary"]["n_tightening"],
            "top_signals": [s["name"] for s in top_signals[:5]],
            "top_inflecting_themes": [t["theme"] for t in top_inflecting[:5]],
            "duration_s": output["duration_s"],
        }),
    }


if __name__ == "__main__":
    print(json.dumps(lambda_handler(), indent=2))
