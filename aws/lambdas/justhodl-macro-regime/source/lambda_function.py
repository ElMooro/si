"""justhodl-macro-regime

PHASE 2: MULTI-ASSET MACRO REGIME ENGINE

Pulls from THREE Polygon subscriptions:
  - Indices Basic (free): VIX, SPX, NDX, RUT, DJX, VVIX
  - Futures Starter ($29/mo): VIX1M/3M/6M futures, ES, NQ, TY (10Y), TU (2Y),
    US (30Y), CL (oil), GC (gold), HG (copper)
  - Currencies Starter ($49/mo): DXY, EURUSD, USDJPY, USDCNH, USDMXN,
    AUDUSD, EURGBP

OUTPUTS:
  macro/regime.json         — current regime + 6 sub-regime signals
  macro/term-structure.json — VIX & Treasury curve shape
  macro/cross-asset.json    — rolling 60d correlations matrix
  macro/history/{date}.json — historical archive

REGIME CLASSIFIER:
  6 sub-regimes combined into top-level classification:
    1. VIX_REGIME: backwardation (stress) / contango (calm)
    2. CURVE_REGIME: inverted / steep / flat
    3. DOLLAR_REGIME: strong / weak / mixed
    4. CARRY_REGIME: risk-on / unwind / mixed (JPY signal)
    5. COMMODITY_REGIME: reflation / deflation / mixed
    6. EM_REGIME: bid / pressured / mixed

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

S3_BUCKET = "justhodl-dashboard-live"
POLYGON_KEY = os.environ.get("POLYGON_KEY", "")
POLYGON_BASE = "https://api.polygon.io"
FETCH_TIMEOUT = 15
MAX_WORKERS = 6

s3 = boto3.client("s3", region_name="us-east-1")


# ═════════════════════════════════════════════════════════════════════
# UNIVERSE — careful tagging by macro role
# ═════════════════════════════════════════════════════════════════════
# Each asset is tagged with what it tells us about the macro regime.

INDICES_UNIVERSE = {
    # Polygon prefix I: for indices
    "I:VIX":   {"name": "VIX",   "role": "vol_spot",  "feed": "indices"},
    "I:VIX9D": {"name": "VIX9D", "role": "vol_9d",    "feed": "indices"},
    "I:VVIX":  {"name": "VVIX",  "role": "vol_of_vol","feed": "indices"},
    "I:SPX":   {"name": "SPX",   "role": "equity_us", "feed": "indices"},
    "I:NDX":   {"name": "NDX",   "role": "equity_tech","feed": "indices"},
    "I:RUT":   {"name": "RUT",   "role": "equity_small","feed": "indices"},
}

# Futures use C: prefix for continuous contracts on Polygon
FUTURES_UNIVERSE = {
    # Vol term structure (futures)
    "VX1!":    {"name": "VIX 1M",    "role": "vol_1m",     "feed": "futures"},
    "VX3!":    {"name": "VIX 3M",    "role": "vol_3m",     "feed": "futures"},
    # Treasury futures (curve shape)
    "ZN1!":    {"name": "10Y T-Note",  "role": "rates_10y",  "feed": "futures"},
    "ZB1!":    {"name": "30Y T-Bond",  "role": "rates_30y",  "feed": "futures"},
    "ZT1!":    {"name": "2Y T-Note",   "role": "rates_2y",   "feed": "futures"},
    # Equity futures (sentiment / basis)
    "ES1!":    {"name": "E-Mini S&P", "role": "equity_fut", "feed": "futures"},
    "NQ1!":    {"name": "E-Mini NAS", "role": "tech_fut",   "feed": "futures"},
    # Commodities
    "CL1!":    {"name": "Crude Oil",  "role": "energy",     "feed": "futures"},
    "GC1!":    {"name": "Gold",       "role": "safe_haven", "feed": "futures"},
    "HG1!":    {"name": "Copper",     "role": "growth",     "feed": "futures"},
}

# FX pairs use C: prefix on Polygon (e.g., C:EURUSD)
FX_UNIVERSE = {
    "C:EURUSD": {"name": "EUR/USD",  "role": "eur",        "feed": "fx"},
    "C:USDJPY": {"name": "USD/JPY",  "role": "jpy_carry",  "feed": "fx"},
    "C:USDCNH": {"name": "USD/CNH",  "role": "china_stress","feed": "fx"},
    "C:USDMXN": {"name": "USD/MXN",  "role": "em_risk",    "feed": "fx"},
    "C:AUDUSD": {"name": "AUD/USD",  "role": "commodity_fx","feed": "fx"},
    "C:GBPUSD": {"name": "GBP/USD",  "role": "gbp",        "feed": "fx"},
    "C:USDCHF": {"name": "USD/CHF",  "role": "chf_safe",   "feed": "fx"},
}

ALL_UNIVERSE = {**INDICES_UNIVERSE, **FUTURES_UNIVERSE, **FX_UNIVERSE}


# ═════════════════════════════════════════════════════════════════════
# Polygon aggregates fetcher (works for indices / futures / fx)
# ═════════════════════════════════════════════════════════════════════
def fetch_daily_bars(ticker: str, days: int = 252) -> dict:
    """Fetch daily aggregates for one symbol. Returns latest + history."""
    if not POLYGON_KEY:
        return {"ticker": ticker, "error": "POLYGON_KEY not set"}
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
                return {"ticker": ticker, "error": "no_results",
                        "status": data.get("status"), "queryCount": data.get("queryCount")}
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
        body = ""
        try:
            body = e.read().decode("utf-8", errors="ignore")[:200]
        except Exception:
            pass
        return {"ticker": ticker, "error": f"http_{e.code}", "body": body}
    except Exception as e:
        return {"ticker": ticker, "error": str(e)[:200]}


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
                results[t] = {"ticker": t, "error": str(e)[:200]}
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
        return {**snap, "metric_status": "missing"}
    bars = snap.get("bars", []) or []
    closes = [b["close"] for b in bars if b.get("close") is not None]
    if not closes:
        return {**snap, "metric_status": "no_closes"}
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
# SUB-REGIME CLASSIFIERS
# ═════════════════════════════════════════════════════════════════════
def classify_vix_regime(b: dict) -> dict:
    """VIX term structure: backwardation = stress, contango = calm."""
    vix = b.get("vol_spot")
    vix1m = b.get("vol_1m")
    vix3m = b.get("vol_3m")
    if not vix or not vix1m or not vix3m:
        return {"label": "INSUFFICIENT_DATA", "score": None, "vix_spot": vix and vix.get("latest_close")}
    spot = vix.get("latest_close")
    front = vix1m.get("latest_close")
    back = vix3m.get("latest_close")
    # Front-back ratio. >1 = backwardation (stress); <0.95 = strong contango (calm)
    ratio_1m_3m = front / back if back else None
    if spot >= 30 and ratio_1m_3m and ratio_1m_3m > 1.02:
        label, score = "VOL_BACKWARDATION_HIGH", -80
    elif ratio_1m_3m and ratio_1m_3m > 1.0:
        label, score = "VOL_BACKWARDATION", -40
    elif ratio_1m_3m and ratio_1m_3m < 0.92:
        label, score = "VOL_STEEP_CONTANGO", 40
    elif ratio_1m_3m and ratio_1m_3m < 0.97:
        label, score = "VOL_CONTANGO", 20
    else:
        label, score = "VOL_NEUTRAL", 0
    return {"label": label, "score": score, "vix_spot": spot, "ratio_1m_3m": round(ratio_1m_3m, 3) if ratio_1m_3m else None}


def classify_curve_regime(b: dict) -> dict:
    """Treasury curve shape via futures prices.

    Note: Treasury futures prices move INVERSELY to yield. Spreads here are
    price spreads, not yield spreads. We use 21d returns as a proxy for
    rate direction (rising prices = falling yields).
    """
    ten = b.get("rates_10y")
    thirty = b.get("rates_30y")
    two = b.get("rates_2y")
    if not all([ten, thirty]):
        return {"label": "INSUFFICIENT_DATA", "score": None}
    # When long bonds outperform short bonds, curve is flattening / inverting
    # (yields falling more at long end)
    long_perf = thirty.get("ret_21d_pct")
    short_perf = two.get("ret_21d_pct") if two else None
    if long_perf is None or short_perf is None:
        return {"label": "INSUFFICIENT_DATA", "score": None}
    spread = long_perf - short_perf
    if spread > 2.0:
        label, score = "CURVE_BULL_FLATTENER", -30  # recessionary
    elif spread > 0.5:
        label, score = "CURVE_FLATTENING", -10
    elif spread < -2.0:
        label, score = "CURVE_BULL_STEEPENER", 30   # expansionary
    elif spread < -0.5:
        label, score = "CURVE_STEEPENING", 10
    else:
        label, score = "CURVE_NEUTRAL", 0
    return {"label": label, "score": score, "long_minus_short_21d_pct": round(spread, 2)}


def classify_dollar_regime(b: dict) -> dict:
    """Dollar strength via EURUSD + DXY proxies."""
    eur = b.get("eur")
    jpy = b.get("jpy_carry")
    if not eur:
        return {"label": "INSUFFICIENT_DATA", "score": None}
    # EURUSD falling = USD strong; USDJPY rising = USD strong
    eur_21d = eur.get("ret_21d_pct")
    jpy_21d = jpy.get("ret_21d_pct") if jpy else None
    if eur_21d is None:
        return {"label": "INSUFFICIENT_DATA", "score": None}
    # Composite USD score: negative EUR + positive JPY = strong USD
    usd_proxy = -eur_21d + (jpy_21d if jpy_21d else 0)
    if usd_proxy > 3:
        label, score = "USD_STRONG_RISING", 60
    elif usd_proxy > 1:
        label, score = "USD_STRONG", 30
    elif usd_proxy < -3:
        label, score = "USD_WEAK_FALLING", -60
    elif usd_proxy < -1:
        label, score = "USD_WEAK", -30
    else:
        label, score = "USD_NEUTRAL", 0
    return {"label": label, "score": score, "usd_proxy_21d": round(usd_proxy, 2),
            "eurusd_21d_pct": eur_21d, "usdjpy_21d_pct": jpy_21d}


def classify_carry_regime(b: dict) -> dict:
    """Carry trade direction via JPY + AUD."""
    jpy = b.get("jpy_carry")
    aud = b.get("commodity_fx")
    if not jpy:
        return {"label": "INSUFFICIENT_DATA", "score": None}
    # USDJPY rising = JPY weakening = carry on
    # AUDUSD rising = AUD strong = carry on
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
    """Reflation: oil up + copper up + gold up = inflationary growth."""
    oil = b.get("energy")
    copper = b.get("growth")
    gold = b.get("safe_haven")
    if not all([oil, copper]):
        return {"label": "INSUFFICIENT_DATA", "score": None}
    oil_21d = oil.get("ret_21d_pct")
    cop_21d = copper.get("ret_21d_pct")
    gold_21d = gold.get("ret_21d_pct") if gold else 0
    if oil_21d is None or cop_21d is None:
        return {"label": "INSUFFICIENT_DATA", "score": None}
    industrial = oil_21d + cop_21d  # both up = reflation
    safe = gold_21d or 0
    if industrial > 8 and safe < industrial / 2:
        label, score = "REFLATION_STRONG", 70
    elif industrial > 3:
        label, score = "REFLATION", 30
    elif industrial < -8 and safe > 3:
        label, score = "DEFLATION_STAGFLATION", -70  # commodities down, gold up = stagflation/recession hedging
    elif industrial < -3:
        label, score = "DEFLATIONARY", -30
    else:
        label, score = "COMMODITIES_NEUTRAL", 0
    return {"label": label, "score": score, "oil_21d_pct": oil_21d,
            "copper_21d_pct": cop_21d, "gold_21d_pct": gold_21d}


def classify_em_regime(b: dict) -> dict:
    """EM stress via USD vs MXN, CNH."""
    mxn = b.get("em_risk")
    cnh = b.get("china_stress")
    if not mxn and not cnh:
        return {"label": "INSUFFICIENT_DATA", "score": None}
    # USDMXN rising = MXN weakening = EM stress
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
    return {"label": label, "score": score, "em_stress_score": round(em_stress, 2)}


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

    # Heuristic top-level rules (priority order — first match wins)
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
    if vol >= 20 and carry >= 20 and commod >= 0:
        return {"regime": "GLOBAL_RISK_ON", "confidence": "MEDIUM",
                "reasoning": "Vol contango + carry on + commodities neutral+"}
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

    # 2. Compute per-asset metrics
    metrics = [
        compute_asset_metrics(snapshots[t]) for t in ALL_UNIVERSE.keys()
    ]
    by_role_map = by_role(metrics)

    # 3. Sub-regime classifications
    subs = {
        "vix_regime":       classify_vix_regime(by_role_map),
        "curve_regime":     classify_curve_regime(by_role_map),
        "dollar_regime":    classify_dollar_regime(by_role_map),
        "carry_regime":     classify_carry_regime(by_role_map),
        "commodity_regime": classify_commodity_regime(by_role_map),
        "em_regime":        classify_em_regime(by_role_map),
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
        "schema_version": "1.0",
    }
    out = {
        **meta,
        "top_level_regime": top_regime,
        "sub_regimes": subs,
        "asset_metrics": metrics,
    }

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
        "body": json.dumps({
            "ok": True,
            "elapsed_s": elapsed,
            "n_ok": n_ok,
            "regime": top_regime.get("regime"),
            "confidence": top_regime.get("confidence"),
            "sub_regime_summary": {k: v.get("label") for k, v in subs.items()},
        }),
    }
