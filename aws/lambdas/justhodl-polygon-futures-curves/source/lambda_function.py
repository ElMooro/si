"""justhodl-polygon-futures-curves

UTILIZES: Polygon Futures Starter ($29/mo) — currently silent.

Tracks futures CURVES (multiple contract months) to detect macro regime:
  - VIX futures (VX1, VX2, VX3): term structure — backwardation = stress
  - Crude oil (CL): contango/backwardation = supply signal
  - Gold (GC) + Silver (SI): risk-off proxy + industrial
  - Copper (HG): industrial growth
  - Natural Gas (NG): energy regime

REGIME SIGNALS:
  1. VIX_BACKWARDATION: VX1 > VX3 = acute stress, BUY signal historically
  2. VIX_STEEP_CONTANGO: VX3 - VX1 > 3 = complacency
  3. OIL_BACKWARDATION: CL1 > CL3 = tight supply, bullish energy
  4. GOLD_BREAKOUT: 20d return > +5% = risk-off
  5. COPPER_BREAKOUT: 20d return > +5% = industrial growth
"""
import json
import os
import time
import urllib.request
import urllib.error
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor
from typing import Optional, List, Dict

import boto3

S3_BUCKET = "justhodl-dashboard-live"
POLYGON_KEY = os.environ.get("POLYGON_KEY", "zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d")

# Continuous front-month + 2nd-month etc. tickers (Polygon syntax)
# Some plans require specific contract symbols (e.g. VXM4 vs VX)
FUTURES_PRODUCTS = {
    "VIX": ["VX", "VX1", "VX2", "VX3"],
    "CRUDE": ["CL", "CL1", "CL2", "CL3"],
    "GOLD": ["GC", "GC1"],
    "SILVER": ["SI", "SI1"],
    "COPPER": ["HG", "HG1"],
    "NATGAS": ["NG", "NG1"],
    "S&P": ["ES", "ES1"],
    "NDX": ["NQ", "NQ1"],
}

s3 = boto3.client("s3", region_name="us-east-1")


def fetch_futures_bars(ticker: str, days: int = 30) -> List[dict]:
    """Polygon futures aggregates."""
    to_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    from_date = (datetime.now(timezone.utc) - timedelta(days=days)).strftime("%Y-%m-%d")
    url = (f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/day/"
           f"{from_date}/{to_date}?adjusted=true&apiKey={POLYGON_KEY}")
    try:
        with urllib.request.urlopen(url, timeout=12) as r:
            data = json.loads(r.read().decode())
        return data.get("results") or []
    except urllib.error.HTTPError as e:
        if e.code in (403, 404):
            return []
        raise
    except Exception as e:
        print(f"[futures] {ticker}: {e}")
        return []


def fetch_futures_snapshot(ticker: str) -> Optional[dict]:
    """Polygon futures snapshot (latest tick)."""
    url = f"https://api.polygon.io/v3/snapshot/futures/{ticker}?apiKey={POLYGON_KEY}"
    try:
        with urllib.request.urlopen(url, timeout=10) as r:
            data = json.loads(r.read().decode())
        return data.get("results")
    except Exception:
        return None


def compute_returns(bars: List[dict]) -> dict:
    if len(bars) < 2:
        return {"error": "insufficient_data"}
    closes = [b.get("c") for b in bars if b.get("c") is not None]
    if len(closes) < 2:
        return {"error": "no_closes"}
    last = closes[-1]
    out = {"latest_price": round(last, 3)}

    def pct(idx):
        if abs(idx) >= len(closes):
            return None
        prior = closes[-1 - abs(idx)]
        if prior is None or prior == 0:
            return None
        return round((closes[-1] - prior) / prior * 100, 2)

    out["return_1d_pct"] = pct(1)
    out["return_5d_pct"] = pct(5)
    out["return_20d_pct"] = pct(20)
    out["n_bars"] = len(bars)
    return out


def detect_curve_signals(product_data: Dict[str, List]) -> List[str]:
    """Detect signals from curves."""
    signals = []

    # VIX backwardation/contango
    vix_curve = product_data.get("VIX", [])
    if len(vix_curve) >= 2:
        prices = [c.get("latest_price") for c in vix_curve if c.get("latest_price")]
        if len(prices) >= 2:
            vx1, vx2 = prices[0], prices[1]
            if vx1 > vx2 + 1:
                signals.append(f"VIX_BACKWARDATION (VX1={vx1:.1f} > VX2={vx2:.1f}) — buy signal historically")
            elif vx2 > vx1 + 3:
                signals.append(f"VIX_STEEP_CONTANGO (VX2-VX1=+{vx2-vx1:.1f}) — complacency")

    # Oil curve
    oil_curve = product_data.get("CRUDE", [])
    if len(oil_curve) >= 2:
        prices = [c.get("latest_price") for c in oil_curve if c.get("latest_price")]
        if len(prices) >= 2 and prices[0] > prices[1] + 0.5:
            signals.append(f"OIL_BACKWARDATION (CL1={prices[0]:.1f} > CL2={prices[1]:.1f}) — tight supply, bullish")

    # Single-product breakouts
    for product in ["GOLD", "COPPER", "SILVER", "NATGAS"]:
        data = product_data.get(product, [])
        if not data:
            continue
        front = data[0] if data else None
        if not front:
            continue
        r20 = front.get("return_20d_pct")
        if r20 is None:
            continue
        if r20 > 5:
            signals.append(f"{product}_BREAKOUT (+{r20:.1f}% 20d)")
        elif r20 < -5:
            signals.append(f"{product}_BREAKDOWN ({r20:.1f}% 20d)")

    return signals


def lambda_handler(event, context):
    t0 = time.time()
    print("[futures-curves] starting")

    product_data = {}
    for product, tickers in FUTURES_PRODUCTS.items():
        contract_data = []
        for t in tickers:
            bars = fetch_futures_bars(t, days=30)
            analysis = compute_returns(bars)
            analysis["ticker"] = t
            if not analysis.get("error"):
                contract_data.append(analysis)
        product_data[product] = contract_data

    signals = detect_curve_signals(product_data)
    elapsed = round(time.time() - t0, 1)
    n_products_with_data = sum(1 for d in product_data.values() if d)
    print(f"[futures-curves] DONE — {n_products_with_data} products with data, "
          f"{len(signals)} signals in {elapsed}s")
    for s in signals:
        print(f"  ⚡ {s}")

    output = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "elapsed_s": elapsed,
        "n_products": len(product_data),
        "n_products_with_data": n_products_with_data,
        "signals": signals,
        "product_data": product_data,
    }

    s3.put_object(
        Bucket=S3_BUCKET, Key="data/polygon-futures-curves.json",
        Body=json.dumps(output, default=str).encode(),
        ContentType="application/json", CacheControl="public, max-age=1800",
    )

    return {
        "statusCode": 200,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps({"ok": True, "elapsed_s": elapsed,
                              "n_products_with_data": n_products_with_data,
                              "n_signals": len(signals),
                              "signals": signals[:5]}),
    }
