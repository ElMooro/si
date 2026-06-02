"""justhodl-polygon-fx-regime

UTILIZES: Polygon Currencies Starter ($49/mo) — currently silent.

Tracks 9 major FX pairs to detect macro regime via currency action:
  - DXY (USD trade-weighted) — via synthetic from EUR/JPY/GBP basket
  - EURUSD — Euro
  - USDJPY — Yen (CARRY currency, weakness=risk-on)
  - GBPUSD — Pound
  - USDCNH — Yuan (CHINA stress)
  - USDBRL — Real (EM stress)
  - AUDUSD — Aussie (commodity proxy)
  - NZDUSD — Kiwi (carry)
  - USDCAD — Loonie (oil sensitive)

REGIME SIGNALS:
  1. USD_STRENGTHENING (>2% in 20d): risk-off, EM bearish
  2. JPY_STRENGTH: carry unwind warning
  3. EM_FX_STRESS (BRL/CNH break): EM equity warning
  4. COMMODITY_FX_DIVERGENCE (AUD vs oil): inflation regime shift
  5. CARRY_TRADE_FAVORABLE (low VIX + USD trending up + JPY weak)

OUTPUT: data/polygon-fx-regime.json
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

FX_PAIRS = {
    "C:EURUSD": "EUR_USD",
    "C:USDJPY": "USD_JPY",
    "C:GBPUSD": "GBP_USD",
    "C:USDCNH": "USD_CNH",
    "C:USDBRL": "USD_BRL",
    "C:AUDUSD": "AUD_USD",
    "C:NZDUSD": "NZD_USD",
    "C:USDCAD": "USD_CAD",
    "C:USDMXN": "USD_MXN",
}

s3 = boto3.client("s3", region_name="us-east-1")


def fetch_fx_bars(polygon_ticker: str, days: int = 30) -> List[dict]:
    """Polygon FX aggregates for last N days."""
    to_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    from_date = (datetime.now(timezone.utc) - timedelta(days=days)).strftime("%Y-%m-%d")
    url = (f"https://api.polygon.io/v2/aggs/ticker/{polygon_ticker}/range/1/day/"
           f"{from_date}/{to_date}?adjusted=true&apiKey={POLYGON_KEY}")
    try:
        with urllib.request.urlopen(url, timeout=12) as r:
            data = json.loads(r.read().decode())
        return data.get("results") or []
    except Exception as e:
        print(f"[fx] {polygon_ticker}: {e}")
        return []


def compute_returns(bars: List[dict]) -> dict:
    """Compute 1d/5d/20d returns and volatility."""
    if len(bars) < 2:
        return {"error": "insufficient_data"}
    closes = [b.get("c") for b in bars if b.get("c") is not None]
    if len(closes) < 2:
        return {"error": "no_closes"}

    last = closes[-1]
    out = {"latest_price": round(last, 5)}

    def pct(idx):
        if abs(idx) >= len(closes) or closes[-1 - abs(idx)] is None:
            return None
        return round((closes[-1] - closes[-1 - abs(idx)]) / closes[-1 - abs(idx)] * 100, 3)

    out["return_1d_pct"] = pct(1)
    out["return_5d_pct"] = pct(5)
    out["return_20d_pct"] = pct(20)

    # 20d realized vol (annualized)
    if len(closes) >= 21:
        rets = []
        for i in range(-20, 0):
            if i - 1 >= -len(closes):
                rets.append((closes[i] - closes[i - 1]) / closes[i - 1])
        if rets:
            mean = sum(rets) / len(rets)
            var = sum((r - mean) ** 2 for r in rets) / max(len(rets) - 1, 1)
            vol_annual = (var ** 0.5) * (252 ** 0.5) * 100
            out["realized_vol_20d_pct"] = round(vol_annual, 2)

    return out


def detect_regime_signals(pair_data: Dict[str, dict]) -> List[str]:
    """Cross-pair regime detection."""
    signals = []

    # USD strength via synthetic DXY (EUR 57.6%, JPY 13.6%, GBP 11.9%)
    eur = pair_data.get("EUR_USD", {})
    jpy = pair_data.get("USD_JPY", {})
    gbp = pair_data.get("GBP_USD", {})

    # Synthetic USD return: -EUR_USD return -GBP_USD return +USD_JPY return
    eur_r = eur.get("return_20d_pct", 0) or 0
    jpy_r = jpy.get("return_20d_pct", 0) or 0
    gbp_r = gbp.get("return_20d_pct", 0) or 0

    usd_synth_20d = (-eur_r * 0.576) + (jpy_r * 0.136) + (-gbp_r * 0.119)
    if usd_synth_20d > 2:
        signals.append(f"USD_STRENGTHENING_20D (+{usd_synth_20d:.2f}%)")
    elif usd_synth_20d < -2:
        signals.append(f"USD_WEAKENING_20D ({usd_synth_20d:.2f}%)")

    # JPY strengthening (carry unwind)
    if jpy_r is not None and jpy_r < -3:
        signals.append(f"JPY_STRENGTH_CARRY_RISK ({jpy_r:.1f}%)")

    # EM FX stress
    cnh = pair_data.get("USD_CNH", {})
    brl = pair_data.get("USD_BRL", {})
    mxn = pair_data.get("USD_MXN", {})
    cnh_r = cnh.get("return_20d_pct", 0) or 0
    brl_r = brl.get("return_20d_pct", 0) or 0
    mxn_r = mxn.get("return_20d_pct", 0) or 0
    em_stress = (cnh_r + brl_r + mxn_r) / 3
    if em_stress > 2:
        signals.append(f"EM_FX_STRESS (mean +{em_stress:.2f}%)")
    elif em_stress < -2:
        signals.append(f"EM_FX_STRENGTH ({em_stress:.2f}%)")

    # Commodity currencies (AUD, CAD vs USD)
    aud = pair_data.get("AUD_USD", {})
    cad = pair_data.get("USD_CAD", {})
    aud_r = aud.get("return_20d_pct", 0) or 0
    cad_r = cad.get("return_20d_pct", 0) or 0
    # AUD up, CAD down (USD/CAD up) = bullish commodities except oil — divergence
    if aud_r > 1 and cad_r > 1:
        signals.append("COMMODITY_FX_DIVERGENCE (AUD up vs CAD weak)")

    # Carry trade indicator
    if jpy_r and jpy_r > 1 and usd_synth_20d > 0:
        signals.append(f"CARRY_TRADE_FAVORABLE (USDJPY +{jpy_r:.1f}%)")

    return signals, {
        "usd_synthetic_20d_pct": round(usd_synth_20d, 3),
        "em_fx_mean_20d_pct": round(em_stress, 3),
    }


def lambda_handler(event, context):
    t0 = time.time()
    print("[fx-regime] starting")

    def _fetch(pair):
        polygon_ticker, internal = pair
        bars = fetch_fx_bars(polygon_ticker, days=30)
        analysis = compute_returns(bars)
        return internal, analysis

    pair_data = {}
    with ThreadPoolExecutor(max_workers=6) as ex:
        for internal, analysis in ex.map(_fetch, FX_PAIRS.items()):
            pair_data[internal] = analysis

    signals, regime_metrics = detect_regime_signals(pair_data)

    elapsed = round(time.time() - t0, 1)
    print(f"[fx-regime] DONE — {len(pair_data)} pairs, {len(signals)} signals in {elapsed}s")
    for s in signals:
        print(f"  ⚡ {s}")

    output = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "elapsed_s": elapsed,
        "n_pairs": len(pair_data),
        "regime_signals": signals,
        "regime_metrics": regime_metrics,
        "pair_data": pair_data,
    }

    s3.put_object(
        Bucket=S3_BUCKET, Key="data/polygon-fx-regime.json",
        Body=json.dumps(output, default=str).encode(),
        ContentType="application/json", CacheControl="public, max-age=1800",
    )

    return {
        "statusCode": 200,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps({"ok": True, "elapsed_s": elapsed,
                              "n_signals": len(signals),
                              "signals": signals[:5],
                              "usd_synth_20d": regime_metrics.get("usd_synthetic_20d_pct")}),
    }
