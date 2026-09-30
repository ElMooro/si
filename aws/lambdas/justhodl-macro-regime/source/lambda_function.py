"""justhodl-macro-regime

PHASE 2: MULTI-ASSET MACRO REGIME ENGINE

Pulls from THREE Polygon subscriptions plus FRED (free, via S3 cache):
  - Indices Basic (free): VIX, SPX, NDX, RUT, DJX, VVIX
  - Futures Starter ($29/mo): VIX1M/3M/6M futures, ES, NQ, TY (10Y), TU (2Y),
    US (30Y), CL (oil), GC (gold), HG (copper)
  - Currencies Starter ($49/mo): DXY, EURUSD, USDJPY, USDCNH, USDMXN,
    AUDUSD, EURGBP
  - FRED (free, S3 cache via justhodl-financial-secretary, no key needed
    here): T10Y2Y, DGS10/2, T5YIE, CPI, UNRATE, WALCL, RRPONTSYD, HY spreads

OUTPUTS:
  macro/regime.json         — current regime + 9 sub-regime signals
                              (6 market-price + 3 FRED macro)
  macro/term-structure.json — VIX & Treasury curve shape
  macro/cross-asset.json    — rolling 60d correlations matrix
  macro/history/{date}.json — historical archive

REGIME CLASSIFIER:
  9 sub-regimes combined into top-level classification
  (6 market-price + 3 FRED macro):
    1. VIX_REGIME: backwardation (stress) / contango (calm)
    2. CURVE_REGIME: inverted / steep / flat
    3. DOLLAR_REGIME: strong / weak / mixed
    4. CARRY_REGIME: risk-on / unwind / mixed (JPY signal)
    5. COMMODITY_REGIME: reflation / deflation / mixed
    6. EM_REGIME: bid / pressured / mixed
    7. INFLATION_REGIME: hot/rising / elevated / disinflation (FRED CPI YoY + T5YIE)
    8. LABOR_REGIME: deteriorating / tight / cooling / stable (FRED UNRATE)
    9. LIQUIDITY_REGIME: QT drain / QT / neutral / QE (FRED WALCL + RRPONTSYD)

  Top-level: GLOBAL_RISK_ON / GLOBAL_RISK_OFF / FLIGHT_TO_QUALITY /
             REFLATION / DEFLATION / TRANSITION / NEUTRAL

This is the foundational tag every other Lambda will use:
  - Research conviction adjusts by regime
  - Backtest attributes alpha by regime
  - Flow engine cross-references with regime
  - Critique pressure-tests with regime context
"""
import json
import os
import time
import urllib.request
import statistics
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, List
from concurrent.futures import ThreadPoolExecutor, as_completed

import boto3
from public_brain_projection import provider_failure, public_provider_diagnostics, macro_regime_public

S3_BUCKET = "justhodl-dashboard-live"
POLYGON_KEY = os.environ.get("POLYGON_KEY", "")
POLYGON_BASE = "https://api.polygon.io"
FETCH_TIMEOUT = 15
MAX_WORKERS = 6

s3 = boto3.client("s3", region_name="us-east-1")


# ═════════════════════════════════════════════════════════════════════
# UNIVERSE — ETF + FX proxies (Polygon entitlements verified)
# ═════════════════════════════════════════════════════════════════════
# After probe 1187: Polygon's futures API + most I: indices aren't entitled
# on the user's keys. The pivot: ETF proxies cover all the macro signals
# we need and work under Stocks Starter entitlement. Bonus: these are the
# same ETFs in the fund-flows universe so we can correlate price moves
# with capital flow signals later.

INDICES_UNIVERSE = {
    # Only I:NDX is entitled — keep it as direct index reading
    "I:NDX":  {"name": "NDX",   "role": "equity_tech",   "feed": "indices"},
}

# Equity / vol / curve / commodity ETF proxies (all stocks, all entitled)
ETF_PROXY_UNIVERSE = {
    # Equity indices
    "SPY":  {"name": "S&P 500",    "role": "equity_us",       "feed": "etf"},
    "QQQ":  {"name": "Nasdaq 100", "role": "equity_tech_etf", "feed": "etf"},
    "IWM":  {"name": "Russell 2k", "role": "equity_small",    "feed": "etf"},
    "DIA":  {"name": "Dow",        "role": "equity_value",    "feed": "etf"},
    # Vol proxies — the VIX term structure replacement
    "VIXY": {"name": "VIX 1M",     "role": "vol_short",       "feed": "etf"},
    "VXX":  {"name": "VIX Short",  "role": "vol_short_alt",   "feed": "etf"},
    "VIXM": {"name": "VIX Mid",    "role": "vol_mid",         "feed": "etf"},
    "UVXY": {"name": "VIX 1.5x",   "role": "vol_levered",     "feed": "etf"},
    # Treasury proxies — curve via SHY (2Y) / IEF (7-10Y) / TLT (20+Y)
    "SHY":  {"name": "1-3Y Tsy",   "role": "rates_short",     "feed": "etf"},
    "IEF":  {"name": "7-10Y Tsy",  "role": "rates_10y",       "feed": "etf"},
    "TLT":  {"name": "20+Y Tsy",   "role": "rates_long",      "feed": "etf"},
    "AGG":  {"name": "Agg Bond",   "role": "rates_agg",       "feed": "etf"},
    "TIP":  {"name": "TIPS",       "role": "inflation_break", "feed": "etf"},
    # Credit
    "HYG":  {"name": "High Yield", "role": "credit_hy",       "feed": "etf"},
    "LQD":  {"name": "IG Credit",  "role": "credit_ig",       "feed": "etf"},
    # Commodities — single-asset ETFs
    "GLD":  {"name": "Gold",       "role": "safe_haven",      "feed": "etf"},
    "SLV":  {"name": "Silver",     "role": "industrial_pm",   "feed": "etf"},
    "USO":  {"name": "Oil",        "role": "energy",          "feed": "etf"},
    "DBC":  {"name": "Broad Cmdy", "role": "commodity_broad", "feed": "etf"},
    "CPER": {"name": "Copper",     "role": "growth",          "feed": "etf"},
    # Dollar proxy (UUP tracks DXY)
    "UUP":  {"name": "Bullish USD","role": "dxy_proxy",       "feed": "etf"},
}

# FX direct (proven to work)
FX_UNIVERSE = {
    "C:EURUSD": {"name": "EUR/USD",  "role": "eur",            "feed": "fx"},
    "C:USDJPY": {"name": "USD/JPY",  "role": "jpy_carry",      "feed": "fx"},
    "C:USDCNH": {"name": "USD/CNH",  "role": "china_stress",   "feed": "fx"},
    "C:USDMXN": {"name": "USD/MXN",  "role": "em_risk",        "feed": "fx"},
    "C:AUDUSD": {"name": "AUD/USD",  "role": "commodity_fx",   "feed": "fx"},
    "C:GBPUSD": {"name": "GBP/USD",  "role": "gbp",            "feed": "fx"},
    "C:USDCHF": {"name": "USD/CHF",  "role": "chf_safe",       "feed": "fx"},
}

ALL_UNIVERSE = {**INDICES_UNIVERSE, **ETF_PROXY_UNIVERSE, **FX_UNIVERSE}


# ═════════════════════════════════════════════════════════════════════
# Polygon aggregates fetcher (works for indices / futures / fx)
# ═════════════════════════════════════════════════════════════════════
def fetch_daily_bars(ticker: str, days: int = 252) -> dict:
    """Fetch daily aggregates for one symbol. Returns latest + history."""
    if not POLYGON_KEY:
        return provider_failure(ticker, "PROVIDER_KEY_UNAVAILABLE")
    end_date = datetime.now(timezone.utc).date()
    start_date = end_date - timedelta(days=int(days * 1.5))  # buffer for non-trading days
    url = (
        f"{POLYGON_BASE}/v2/aggs/ticker/{ticker}/range/1/day/"
        f"{start_date.isoformat()}/{end_date.isoformat()}"
        f"?adjusted=true&sort=desc&limit=300&apiKey={POLYGON_KEY}"
    )
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "JustHodl-MacroRegime/1.0"})
        with urllib.request.urlopen(req, timeout=FETCH_TIMEOUT) as r:
            data = json.loads(r.read())
            results = data.get("results") or []
            if not results:
                return provider_failure(ticker, "PROVIDER_NO_RESULTS")
            # results already sort=desc, but be defensive
            results = sorted(results, key=lambda x: x.get("t", 0), reverse=True)
            latest = results[0]
            close = latest.get("c")
            ts = datetime.fromtimestamp(latest["t"] / 1000, timezone.utc) if latest.get("t") else None
            return {
                "ticker": ticker,
                "latest_close": close,
                "latest_date": ts.isoformat() if ts else None,
                "latest_volume": latest.get("v"),
                "bars": [
                    {"date": datetime.fromtimestamp(b["t"]/1000, timezone.utc).strftime("%Y-%m-%d"),
                     "open": b.get("o"), "high": b.get("h"), "low": b.get("l"),
                     "close": b.get("c"), "volume": b.get("v")}
                    for b in results[:days]
                ],
                "n_bars": len(results),
            }
    except urllib.error.HTTPError as e:
        return provider_failure(ticker, "PROVIDER_HTTP_ERROR", e.code)
    except Exception:
        return provider_failure(ticker, "PROVIDER_REQUEST_FAILED")


def fetch_universe() -> dict:
    """Parallel fetch all assets."""
    results = {}
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        future_to_ticker = {
            ex.submit(fetch_daily_bars, t, 252): t for t in ALL_UNIVERSE.keys()
        }
        for fut in as_completed(future_to_ticker):
            t = future_to_ticker[fut]
            try:
                results[t] = fut.result()
            except Exception as e:
                results[t] = provider_failure(t, "PROVIDER_REQUEST_FAILED")
    return results


# ═════════════════════════════════════════════════════════════════════
# Analytics
# ═════════════════════════════════════════════════════════════════════
def _pct_change(a, b):
    if a is None or b is None or b == 0:
        return None
    return round(100 * (a / b - 1), 2)


def _sma(prices: list, n: int):
    if len(prices) < n:
        return None
    return statistics.mean(prices[:n])


def _zscore(latest, history):
    if not history or len(history) < 20:
        return None
    try:
        mean = statistics.mean(history)
        stdev = statistics.stdev(history)
        if stdev == 0:
            return None
        return round((latest - mean) / stdev, 2)
    except Exception:
        return None


def compute_asset_metrics(snap: dict) -> dict:
    """Compute returns + trend metrics for one asset."""
    if snap.get("error"):
        return {**public_provider_diagnostics(snap), "metric_status": "missing"}
    bars = snap.get("bars", []) or []
    closes = [b["close"] for b in bars if b.get("close") is not None]
    if not closes:
        return {**provider_failure(snap.get("ticker"), "PROVIDER_NO_PRICES"), "metric_status": "no_closes"}
    latest = closes[0]
    return {
        "ticker": snap["ticker"],
        "name": ALL_UNIVERSE[snap["ticker"]]["name"],
        "role": ALL_UNIVERSE[snap["ticker"]]["role"],
        "feed": ALL_UNIVERSE[snap["ticker"]]["feed"],
        "latest_close": latest,
        "latest_date": snap.get("latest_date"),
        "ret_1d_pct": _pct_change(latest, closes[1]) if len(closes) >= 2 else None,
        "ret_5d_pct": _pct_change(latest, closes[5]) if len(closes) >= 6 else None,
        "ret_21d_pct": _pct_change(latest, closes[21]) if len(closes) >= 22 else None,
        "ret_63d_pct": _pct_change(latest, closes[63]) if len(closes) >= 64 else None,
        "ret_252d_pct": _pct_change(latest, closes[252]) if len(closes) >= 253 else None,
        "sma_50d": _sma(closes, 50),
        "sma_200d": _sma(closes, 200),
        "above_50d": (latest > _sma(closes, 50)) if _sma(closes, 50) else None,
        "above_200d": (latest > _sma(closes, 200)) if _sma(closes, 200) else None,
        "zscore_90d": _zscore(latest, closes[:90]) if len(closes) >= 30 else None,
        "n_bars": len(closes),
    }


def by_role(metrics: list) -> dict:
    return {m["role"]: m for m in metrics if not m.get("error") and not m.get("metric_status") == "missing"}


# ═════════════════════════════════════════════════════════════════════
# FRED MACRO OVERLAY — Bloomberg parity upgrade 1/10
# ═════════════════════════════════════════════════════════════════════
# Primary-source macro series from the S3 FRED cache populated by
# justhodl-financial-secretary (data/fred-cache-secretary.json).
# No API key needed here; the secretary owns the FRED key + refresh cadence.
#
# Cache entry shape per series (history is newest-first):
#   {"name": str, "value": float, "prev": float, "chg_1d": float,
#    "chg_1m": float, "date": "YYYY-MM-DD", "history": [float, ...]}
#
# Design rules:
#   - FAIL-SOFT: missing/empty/malformed data -> INSUFFICIENT_DATA, and the
#     ETF-proxy classifiers below remain as the fallback path.
#   - FRED never removes an existing signal; it only upgrades precision
#     (curve spread in bp instead of an ETF price proxy, etc.).

FRED_CACHE_KEY = "data/fred-cache-secretary.json"

# Series consumed here (subset of the ~26 the secretary caches)
FRED_SERIES_USED = [
    "T10Y2Y", "DGS10", "DGS2", "T5YIE",      # rates / curve / breakeven
    "CPIAUCSL", "CPILFESL",                   # inflation
    "FEDFUNDS", "UNRATE",                     # policy / labor
    "WALCL", "RRPONTSYD",                     # liquidity
    "BAMLH0A0HYM2",                           # credit
    "STLFSI2", "NFCI",                        # stress (informational)
    "NAPM",                                   # ISM PMI (informational)
]


def fetch_fred_macro() -> dict:
    """Read the S3 FRED cache.

    Returns {series_id: cache_entry} for the series we consume, or {}
    on any error (fail-soft: the engine runs on ETF proxies alone).
    """
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=FRED_CACHE_KEY)
        raw = json.loads(obj["Body"].read().decode("utf-8"))
    except Exception as e:
        print(f"[macro-regime] FRED cache read failed ({type(e).__name__}): "
              f"{e}; continuing without FRED")
        return {}
    if not isinstance(raw, dict):
        print("[macro-regime] FRED cache malformed (not a dict); continuing without FRED")
        return {}
    fred = {sid: raw[sid] for sid in FRED_SERIES_USED if isinstance(raw.get(sid), dict)}
    print(f"[macro-regime] FRED cache: {len(fred)}/{len(FRED_SERIES_USED)} series available")
    return fred


def _fred_entry(fred: dict, sid: str) -> dict:
    """Safely fetch one FRED cache entry; {} when missing/malformed."""
    e = (fred or {}).get(sid)
    return e if isinstance(e, dict) else {}


def _fred_value(fred: dict, sid: str):
    """Latest numeric value for a series, or None."""
    v = _fred_entry(fred, sid).get("value")
    return v if isinstance(v, (int, float)) else None


def _fred_history(fred: dict, sid: str) -> list:
    """Newest-first numeric history for a series, or []."""
    h = _fred_entry(fred, sid).get("history") or []
    return [x for x in h if isinstance(x, (int, float))]


def _fred_summary(fred: dict) -> dict:
    """Compact, history-free snapshot of the FRED overlay for S3 output."""
    series = {}
    for sid in FRED_SERIES_USED:
        e = _fred_entry(fred, sid)
        if not e:
            continue
        series[sid] = {
            "name": e.get("name"),
            "value": e.get("value"),
            "date": e.get("date"),
            "chg_1d": e.get("chg_1d"),
            "chg_1m": e.get("chg_1m"),
        }
    return {
        "source": f"S3 {S3_BUCKET}/{FRED_CACHE_KEY}",
        "n_series": len(series),
        "series": series,
    }


def classify_inflation_regime(fred: dict) -> dict:
    """Inflation sub-regime from FRED CPI (YoY) + 5Y breakeven (T5YIE).

    CPI YoY > 3.5% and rising  -> -30 (hot/rising inflation pressure).
    CPI YoY < 2.0% and falling -> +20 (disinflation tailwind).
    T5YIE (market-implied breakeven) nudges the score by +/-10.
    """
    cpi = _fred_history(fred, "CPIAUCSL")
    t5yie = _fred_value(fred, "T5YIE")
    if len(cpi) < 13 and t5yie is None:
        return {"label": "INSUFFICIENT_DATA", "score": None}

    score = 0
    yoy = None
    rising = falling = False
    if len(cpi) >= 13 and cpi[12]:
        yoy = round(100 * (cpi[0] / cpi[12] - 1), 2)
        if len(cpi) >= 16 and cpi[15]:
            yoy_then = 100 * (cpi[3] / cpi[15] - 1)
            rising = yoy > yoy_then + 0.05
            falling = yoy < yoy_then - 0.05
        else:  # short history: compare price levels 3 observations apart
            rising = cpi[0] > cpi[3]
            falling = cpi[0] < cpi[3]
        if yoy > 3.5 and rising:
            score -= 30
        elif yoy < 2.0 and falling:
            score += 20
        elif yoy > 3.5:
            score -= 15
        elif yoy < 2.0:
            score += 10

    if t5yie is not None:  # market-implied inflation cross-check
        if t5yie > 2.75:
            score -= 10
        elif t5yie < 2.0:
            score += 10

    if score <= -30:
        label = "INFLATION_HOT_RISING"
    elif score <= -15:
        label = "INFLATION_ELEVATED"
    elif score >= 20:
        label = "DISINFLATION"
    elif score >= 10:
        label = "INFLATION_LOW"
    else:
        label = "INFLATION_NEUTRAL"
    return {"label": label, "score": score, "source": "FRED",
            "cpi_yoy_pct": yoy,
            "cpi_yoy_rising": rising if yoy is not None else None,
            "t5yie_pct": t5yie}


def classify_labor_regime(fred: dict) -> dict:
    """Labor sub-regime from FRED UNRATE (Sahm-style deterioration rule).

    UNRATE up >= 0.5pp from its 12-month low -> -40 (deteriorating).
    Level check: < 4.0% tight (+10); > 5.0% soft (-15); +0.3pp rise -> -10.
    """
    h = _fred_history(fred, "UNRATE")
    if not h:
        return {"label": "INSUFFICIENT_DATA", "score": None}
    latest = h[0]
    low_12m = min(h[:13])  # ~12 months of monthly observations
    rise_pp = round(latest - low_12m, 2)
    if rise_pp >= 0.5:
        label, score = "LABOR_DETERIORATING", -40
    elif latest < 4.0:
        label, score = "LABOR_TIGHT", 10
    elif latest > 5.0:
        label, score = "LABOR_SOFT", -15
    elif rise_pp >= 0.3:
        label, score = "LABOR_COOLING", -10
    else:
        label, score = "LABOR_STABLE", 0
    return {"label": label, "score": score, "source": "FRED",
            "unrate_pct": latest,
            "rise_from_12m_low_pp": rise_pp,
            "low_12m_pct": round(low_12m, 2)}


def classify_liquidity_regime(fred: dict) -> dict:
    """Fed liquidity sub-regime from balance sheet (WALCL, weekly) + ON RRP.

    QT (WALCL shrinking ~3mo) + elevated RRP parked at the Fed -> -25.
    RRP draining back into the banking system -> +10 (liquidity returning).
    Balance-sheet expansion -> +15.
    """
    walcl = _fred_history(fred, "WALCL")
    rrp = _fred_history(fred, "RRPONTSYD")
    if not walcl and not rrp:
        return {"label": "INSUFFICIENT_DATA", "score": None}

    walcl_chg = (walcl[0] - walcl[min(13, len(walcl) - 1)]) if len(walcl) >= 2 else None  # ~3mo, $bn
    rrp_val = rrp[0] if rrp else None
    rrp_chg = (rrp[0] - rrp[-1]) if len(rrp) >= 2 else None  # over available window, $bn

    qt = walcl_chg is not None and walcl_chg < -50
    rrp_elevated = rrp_val is not None and rrp_val > 100

    if qt and rrp_elevated:
        label, score = "LIQUIDITY_QT_DRAIN", -25
    elif qt:
        label, score = "LIQUIDITY_QT", -10
    elif rrp_chg is not None and rrp_chg < -200:
        label, score = "LIQUIDITY_RRP_DRAINING", 10
    elif walcl_chg is not None and walcl_chg > 100:
        label, score = "LIQUIDITY_QE", 15
    else:
        label, score = "LIQUIDITY_NEUTRAL", 0
    return {"label": label, "score": score, "source": "FRED",
            "walcl_chg_3m_bn": round(walcl_chg, 1) if walcl_chg is not None else None,
            "rrp_bn": rrp_val,
            "rrp_chg_window_bn": round(rrp_chg, 1) if rrp_chg is not None else None}


# ═════════════════════════════════════════════════════════════════════
# SUB-REGIME CLASSIFIERS
# ═════════════════════════════════════════════════════════════════════
def classify_vix_regime(b: dict) -> dict:
    """VIX term structure via ETF proxies.

    With ETF proxies we use:
      - VIXY/VXX (front-month VIX exposure)
      - VIXM (mid-curve VIX exposure ~5 months)
      - UVXY for cross-check (1.5x levered short-term)
    The price RATIO of front-month (VIXY) to mid-curve (VIXM) is a direct
    proxy for the futures term structure. When front > mid (price ratio
    rises) = backwardation = stress.
    """
    short = b.get("vol_short")
    short_alt = b.get("vol_short_alt")  # VXX
    mid = b.get("vol_mid")              # VIXM
    if not short or not mid:
        return {"label": "INSUFFICIENT_DATA", "score": None}
    # Use 21d return spread as direction proxy
    short_ret = short.get("ret_21d_pct")
    mid_ret = mid.get("ret_21d_pct")
    if short_ret is None or mid_ret is None:
        return {"label": "INSUFFICIENT_DATA", "score": None}
    # When short_ret > mid_ret strongly = front-month vol bid harder = backwardation
    spread_21d = short_ret - mid_ret
    # 1d return for spot stress signal
    short_1d = short.get("ret_1d_pct") or 0
    if spread_21d > 8 or short_1d > 5:
        label, score = "VOL_BACKWARDATION_HIGH", -80
    elif spread_21d > 3:
        label, score = "VOL_BACKWARDATION", -40
    elif spread_21d < -8:
        label, score = "VOL_STEEP_CONTANGO", 40
    elif spread_21d < -3:
        label, score = "VOL_CONTANGO", 20
    else:
        label, score = "VOL_NEUTRAL", 0
    return {"label": label, "score": score,
            "short_21d_pct": short_ret, "mid_21d_pct": mid_ret,
            "spread_pct": round(spread_21d, 2), "short_1d_pct": short_1d}


def classify_curve_regime(b: dict, fred: dict = None) -> dict:
    """Treasury curve shape.

    Preferred: FRED T10Y2Y spread (actual 10Y-2Y in percentage points;
    negative = inverted).
      spread < 0     -> CURVE_INVERTED, -40 (recession signal)
      spread > 1.50  -> CURVE_STEEP, +25   (>150bp)
      spread < 0.50  -> CURVE_FLAT, -10
      else           -> CURVE_NORMAL, +5
    Fallback: SHY (2Y) / IEF (7-10Y) / TLT (20+Y) ETF price proxies, where
    bond ETF prices move INVERSELY to yields (unchanged legacy logic).
    """
    spread = _fred_value(fred or {}, "T10Y2Y")
    if spread is not None:
        entry = _fred_entry(fred, "T10Y2Y")
        if spread < 0:
            label, score = "CURVE_INVERTED", -40
        elif spread > 1.50:
            label, score = "CURVE_STEEP", 25
        elif spread < 0.50:
            label, score = "CURVE_FLAT", -10
        else:
            label, score = "CURVE_NORMAL", 5
        return {"label": label, "score": score, "source": "FRED_T10Y2Y",
                "spread_pp": round(spread, 2),
                "spread_date": entry.get("date"),
                "spread_chg_1m_pp": entry.get("chg_1m")}
    short = b.get("rates_short")  # SHY
    long = b.get("rates_long")    # TLT
    mid = b.get("rates_10y")      # IEF
    if not short or not long:
        return {"label": "INSUFFICIENT_DATA", "score": None}
    long_perf = long.get("ret_21d_pct")
    short_perf = short.get("ret_21d_pct")
    if long_perf is None or short_perf is None:
        return {"label": "INSUFFICIENT_DATA", "score": None}
    spread = long_perf - short_perf  # >0 = long outperforming = curve flattening
    if spread > 3.0:
        label, score = "CURVE_BULL_FLATTENER", -40   # recessionary signal
    elif spread > 1.0:
        label, score = "CURVE_FLATTENING", -15
    elif spread < -3.0:
        label, score = "CURVE_BEAR_STEEPENER", 40    # rates rising at long end
    elif spread < -1.0:
        label, score = "CURVE_STEEPENING", 15
    else:
        label, score = "CURVE_NEUTRAL", 0
    return {"label": label, "score": score, "source": "ETF_PROXY",
            "long_21d_pct": long_perf, "short_21d_pct": short_perf,
            "long_minus_short_21d": round(spread, 2)}


def classify_dollar_regime(b: dict) -> dict:
    """Dollar via UUP ETF + EURUSD/USDJPY direct cross-check."""
    uup = b.get("dxy_proxy")
    eur = b.get("eur")
    jpy = b.get("jpy_carry")
    score_components = []
    if uup and uup.get("ret_21d_pct") is not None:
        score_components.append(uup["ret_21d_pct"])
    if eur and eur.get("ret_21d_pct") is not None:
        score_components.append(-eur["ret_21d_pct"])  # EURUSD inverse
    if jpy and jpy.get("ret_21d_pct") is not None:
        score_components.append(jpy["ret_21d_pct"])
    if not score_components:
        return {"label": "INSUFFICIENT_DATA", "score": None}
    usd_proxy = sum(score_components) / len(score_components)
    if usd_proxy > 2:
        label, score = "USD_STRONG_RISING", 60
    elif usd_proxy > 0.5:
        label, score = "USD_STRONG", 30
    elif usd_proxy < -2:
        label, score = "USD_WEAK_FALLING", -60
    elif usd_proxy < -0.5:
        label, score = "USD_WEAK", -30
    else:
        label, score = "USD_NEUTRAL", 0
    return {"label": label, "score": score, "usd_proxy_21d_avg": round(usd_proxy, 2),
            "uup_21d_pct": uup and uup.get("ret_21d_pct"),
            "eurusd_21d_pct": eur and eur.get("ret_21d_pct"),
            "usdjpy_21d_pct": jpy and jpy.get("ret_21d_pct")}


def classify_carry_regime(b: dict) -> dict:
    """Carry trade via JPY + AUD."""
    jpy = b.get("jpy_carry")
    aud = b.get("commodity_fx")
    if not jpy:
        return {"label": "INSUFFICIENT_DATA", "score": None}
    jpy_21d = jpy.get("ret_21d_pct")
    aud_21d = aud.get("ret_21d_pct") if aud else None
    if jpy_21d is None:
        return {"label": "INSUFFICIENT_DATA", "score": None}
    carry_score = jpy_21d + (aud_21d if aud_21d else 0)
    if carry_score > 4:
        label, score = "CARRY_ON_STRONG", 70
    elif carry_score > 1:
        label, score = "CARRY_ON", 30
    elif carry_score < -4:
        label, score = "CARRY_UNWIND_STRONG", -70
    elif carry_score < -1:
        label, score = "CARRY_UNWIND", -30
    else:
        label, score = "CARRY_NEUTRAL", 0
    return {"label": label, "score": score, "carry_score_21d": round(carry_score, 2)}


def classify_commodity_regime(b: dict) -> dict:
    """Reflation via USO (oil) + CPER (copper) + GLD (gold)."""
    oil = b.get("energy")
    copper = b.get("growth")
    gold = b.get("safe_haven")
    silver = b.get("industrial_pm")
    if not all([oil, copper]):
        return {"label": "INSUFFICIENT_DATA", "score": None}
    oil_21d = oil.get("ret_21d_pct")
    cop_21d = copper.get("ret_21d_pct")
    gold_21d = gold.get("ret_21d_pct") if gold else 0
    silver_21d = silver.get("ret_21d_pct") if silver else None
    if oil_21d is None or cop_21d is None:
        return {"label": "INSUFFICIENT_DATA", "score": None}
    industrial = oil_21d + cop_21d
    safe = gold_21d or 0
    if industrial > 8 and safe < industrial / 2:
        label, score = "REFLATION_STRONG", 70
    elif industrial > 3:
        label, score = "REFLATION", 30
    elif industrial < -8 and safe > 3:
        label, score = "STAGFLATION_HEDGE", -70
    elif industrial < -3:
        label, score = "DEFLATIONARY", -30
    else:
        label, score = "COMMODITIES_NEUTRAL", 0
    return {"label": label, "score": score,
            "oil_21d_pct": oil_21d, "copper_21d_pct": cop_21d,
            "gold_21d_pct": gold_21d, "silver_21d_pct": silver_21d}


def classify_em_regime(b: dict) -> dict:
    """EM stress via USDMXN + USDCNH FX."""
    mxn = b.get("em_risk")
    cnh = b.get("china_stress")
    if not mxn and not cnh:
        return {"label": "INSUFFICIENT_DATA", "score": None}
    mxn_21d = mxn.get("ret_21d_pct") if mxn else None
    cnh_21d = cnh.get("ret_21d_pct") if cnh else None
    em_stress = (mxn_21d or 0) + (cnh_21d or 0)
    if em_stress > 4:
        label, score = "EM_STRESS_HIGH", -70
    elif em_stress > 1:
        label, score = "EM_PRESSURED", -30
    elif em_stress < -4:
        label, score = "EM_STRONG", 60
    elif em_stress < -1:
        label, score = "EM_BID", 30
    else:
        label, score = "EM_NEUTRAL", 0
    return {"label": label, "score": score, "em_stress_21d": round(em_stress, 2)}


def classify_credit_regime(b: dict, fred: dict = None) -> dict:
    """Credit appetite.

    Preferred: FRED HY option-adjusted spread (BAMLH0A0HYM2, in %).
      spread > 6.00 (>600bp)            -> CREDIT_STRESS, -50
      widening fast (1-mo chg > 100bp)  -> CREDIT_WIDENING, -15
      spread < 3.50                     -> CREDIT_TIGHT, +20
      spread < 4.50                     -> CREDIT_HEALTHY, +10
      else                              -> CREDIT_NEUTRAL, 0
    Fallback: HYG/LQD 21d performance ratio (unchanged legacy logic).
    """
    oas = _fred_value(fred or {}, "BAMLH0A0HYM2")
    if oas is not None:
        entry = _fred_entry(fred, "BAMLH0A0HYM2")
        chg_1m = entry.get("chg_1m")
        widening = isinstance(chg_1m, (int, float)) and chg_1m > 1.00
        if oas > 6.00:
            label, score = "CREDIT_STRESS", -50
        elif widening:
            label, score = "CREDIT_WIDENING", -15
        elif oas < 3.50:
            label, score = "CREDIT_TIGHT", 20
        elif oas < 4.50:
            label, score = "CREDIT_HEALTHY", 10
        else:
            label, score = "CREDIT_NEUTRAL", 0
        return {"label": label, "score": score, "source": "FRED_HY_OAS",
                "hy_oas_pct": round(oas, 2), "hy_oas_date": entry.get("date"),
                "hy_oas_chg_1m_pp": chg_1m}
    hy = b.get("credit_hy")
    ig = b.get("credit_ig")
    if not hy or not ig:
        return {"label": "INSUFFICIENT_DATA", "score": None}
    hy_21d = hy.get("ret_21d_pct")
    ig_21d = ig.get("ret_21d_pct")
    if hy_21d is None or ig_21d is None:
        return {"label": "INSUFFICIENT_DATA", "score": None}
    spread = hy_21d - ig_21d  # HY outperforming IG = risk appetite
    if spread > 1.5:
        label, score = "CREDIT_RISK_ON", 40
    elif spread > 0.3:
        label, score = "CREDIT_HEALTHY", 15
    elif spread < -1.5:
        label, score = "CREDIT_STRESS", -60
    elif spread < -0.3:
        label, score = "CREDIT_DETERIORATING", -25
    else:
        label, score = "CREDIT_NEUTRAL", 0
    return {"label": label, "score": score, "source": "ETF_PROXY",
            "hy_21d_pct": hy_21d, "ig_21d_pct": ig_21d,
            "hy_minus_ig_21d": round(spread, 2)}


# ═════════════════════════════════════════════════════════════════════
# TOP-LEVEL REGIME CLASSIFIER
# ═════════════════════════════════════════════════════════════════════
def classify_top_level(subs: dict) -> dict:
    """Combine 6 sub-regimes into a top-level macro tag."""
    scores = {k: v.get("score") for k, v in subs.items() if v.get("score") is not None}
    if len(scores) < 4:
        return {"regime": "INSUFFICIENT_DATA", "confidence": "LOW",
                "n_components_available": len(scores)}

    vol = scores.get("vix_regime", 0)
    curve = scores.get("curve_regime", 0)
    dollar = scores.get("dollar_regime", 0)
    carry = scores.get("carry_regime", 0)
    commod = scores.get("commodity_regime", 0)
    em = scores.get("em_regime", 0)
    credit = scores.get("credit_regime", 0)
    inflation = scores.get("inflation_regime", 0)
    labor = scores.get("labor_regime", 0)
    liquidity = scores.get("liquidity_regime", 0)

    # FRED macro-overlay rules — evaluated FIRST. Fundamentals take precedence
    # over market-price heuristics when both agree on stress.
    if inflation <= -30 and labor <= -30:
        return {"regime": "STAGFLATION_RISK", "confidence": "HIGH",
                "reasoning": "Hot/rising inflation + deteriorating labor market (FRED)"}
    if liquidity <= -25 and curve <= -40:
        return {"regime": "QT_TIGHTENING", "confidence": "MEDIUM",
                "reasoning": "QT liquidity drain + inverted/flat curve (FRED)"}

    # Heuristic top-level rules (priority order — first match wins)
    if credit <= -40 and vol <= -20:
        return {"regime": "CREDIT_STRESS", "confidence": "HIGH",
                "reasoning": "HY underperforming IG + vol bid = credit-led de-risking"}
    if vol <= -40 and (carry <= -30 or em <= -30):
        return {"regime": "FLIGHT_TO_QUALITY", "confidence": "HIGH",
                "reasoning": "Vol backwardation + carry unwind/EM stress"}
    if vol <= -40 and curve <= -10:
        return {"regime": "GLOBAL_RISK_OFF", "confidence": "HIGH",
                "reasoning": "Vol backwardation + curve flattening"}
    if commod >= 30 and carry >= 30 and em >= 0:
        return {"regime": "REFLATION", "confidence": "HIGH",
                "reasoning": "Commodities strong + carry on + EM not stressed"}
    if commod >= 30 and dollar <= -10:
        return {"regime": "REFLATION", "confidence": "MEDIUM",
                "reasoning": "Commodities strong + weak USD"}
    if commod <= -30 and vol <= -10:
        return {"regime": "DEFLATION", "confidence": "MEDIUM",
                "reasoning": "Commodities weak + vol elevated"}
    if dollar >= 30 and em <= -30:
        return {"regime": "USD_STRENGTH_EM_STRESS", "confidence": "MEDIUM",
                "reasoning": "Strong USD pressuring EM"}
    if vol >= 20 and carry >= 20 and commod >= 0 and credit >= 0:
        return {"regime": "GLOBAL_RISK_ON", "confidence": "MEDIUM",
                "reasoning": "Vol contango + carry on + credit healthy"}
    if abs(vol) < 20 and abs(curve) < 20 and abs(dollar) < 20:
        return {"regime": "NEUTRAL", "confidence": "MEDIUM",
                "reasoning": "All major sub-regimes near neutral"}
    return {"regime": "TRANSITION", "confidence": "LOW",
            "reasoning": "Mixed signals across sub-regimes"}


# ═════════════════════════════════════════════════════════════════════
# Handler
# ═════════════════════════════════════════════════════════════════════
def lambda_handler(event, context):
    t0 = time.time()
    print(f"[macro-regime] starting · universe size: {len(ALL_UNIVERSE)}")

    # 1. Parallel fetch
    snapshots = fetch_universe()
    n_ok = sum(1 for s in snapshots.values() if not s.get("error"))
    print(f"[macro-regime] fetched {n_ok}/{len(ALL_UNIVERSE)}")

    # 1b. FRED macro overlay (fail-soft — {} when the cache is unavailable)
    fred = fetch_fred_macro()
    fred_summary = _fred_summary(fred)

    # 2. Compute per-asset metrics
    metrics = [
        compute_asset_metrics(snapshots[t]) for t in ALL_UNIVERSE.keys()
    ]
    by_role_map = by_role(metrics)

    # 3. Sub-regime classifications
    subs = {
        "vix_regime":       classify_vix_regime(by_role_map),
        "curve_regime":     classify_curve_regime(by_role_map, fred),
        "dollar_regime":    classify_dollar_regime(by_role_map),
        "carry_regime":     classify_carry_regime(by_role_map),
        "commodity_regime": classify_commodity_regime(by_role_map),
        "em_regime":        classify_em_regime(by_role_map),
        "credit_regime":    classify_credit_regime(by_role_map, fred),
        "inflation_regime": classify_inflation_regime(fred),
        "labor_regime":     classify_labor_regime(fred),
        "liquidity_regime": classify_liquidity_regime(fred),
    }

    # 4. Top-level regime
    top_regime = classify_top_level(subs)

    elapsed = round(time.time() - t0, 1)
    print(f"[macro-regime] top-level regime: {top_regime.get('regime')}")

    # 5. Write outputs
    meta = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "universe_size": len(ALL_UNIVERSE),
        "n_ok": n_ok,
        "elapsed_s": elapsed,
        "schema_version": "1.1",
    }
    out = {
        **meta,
        "top_level_regime": top_regime,
        "sub_regimes": subs,
        "asset_metrics": metrics,
        "fred": fred_summary,
    }

    out = macro_regime_public(out)
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    s3.put_object(
        Bucket=S3_BUCKET,
        Key="macro/regime.json",
        Body=json.dumps(out, default=str).encode(),
        ContentType="application/json",
        CacheControl="public, max-age=600",
    )
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=f"macro/history/{today}.json",
        Body=json.dumps(out, default=str).encode(),
        ContentType="application/json",
        CacheControl="public, max-age=86400",
    )
    print(f"[macro-regime] wrote macro/regime.json + history/{today}.json")

    return {
        "statusCode": 200,
        "headers": {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"},
        "body": json.dumps(public_provider_diagnostics({
            "ok": True,
            "elapsed_s": elapsed,
            "n_ok": n_ok,
            "regime": top_regime.get("regime"),
            "confidence": top_regime.get("confidence"),
            "sub_regime_summary": {k: v.get("label") for k, v in subs.items()},
        })),
    }
