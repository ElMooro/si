"""justhodl-polygon-fx-regime — FX-anchored Risk-On/Risk-Off (RORO) engine.

Massive FX is fully entitled (probed ops 1939: all majors, JPY/CHF havens, carry
crosses, full EM basket, XAU/XAG — fresh daily). FX is the deepest, 24h, cleanest
real-time RORO read used by macro desks (HSBC RORO index, carry monitors).

Computes a single fx_roro_score [-100 risk-off .. +100 risk-on] from weighted
drivers, signed so + = risk-on:
  • AUD/JPY (the canonical RORO cross), EUR/JPY, NZD/JPY — carry crosses
  • JPY & CHF havens (USDJPY/USDCHF up = haven sold = risk-on)
  • AUD, NZD — commodity / risk currencies
  • Gold (XAUUSD) — haven (up = risk-off)
  • EM basket (MXN/ZAR/BRL/KRW/CNH) — EM weak vs USD = risk-off
  • Gold/Silver ratio — rising = fear

Backward-compatible: still emits regime_signals, usd_synthetic_20d_pct,
em_fx_mean_20d_pct, pair_data (consumed by capital-flow-radar dollar tide).
OUTPUT: data/polygon-fx-regime.json
"""
import json
import os
import time
import urllib.request
import urllib.error
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor
from typing import List, Dict

import boto3

S3_BUCKET = "justhodl-dashboard-live"
OLD_POLYGON_KEY = os.environ.get("POLYGON_KEY", "zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d")
try:
    from massive import get_massive_key, MASSIVE_BASE
    _MKEY = get_massive_key()
except Exception:
    _MKEY, MASSIVE_BASE = "", "https://api.massive.com"
KEY = _MKEY or OLD_POLYGON_KEY
BASE = MASSIVE_BASE if _MKEY else "https://api.polygon.io"

# internal_name -> massive ticker
FX_PAIRS = {
    # majors / USD legs
    "EUR_USD": "C:EURUSD", "USD_JPY": "C:USDJPY", "GBP_USD": "C:GBPUSD",
    "USD_CHF": "C:USDCHF", "USD_CAD": "C:USDCAD",
    # risk / commodity currencies
    "AUD_USD": "C:AUDUSD", "NZD_USD": "C:NZDUSD", "USD_NOK": "C:USDNOK",
    # carry crosses (RORO barometers)
    "AUD_JPY": "C:AUDJPY", "EUR_JPY": "C:EURJPY", "NZD_JPY": "C:NZDJPY",
    # EM basket
    "USD_CNH": "C:USDCNH", "USD_BRL": "C:USDBRL", "USD_MXN": "C:USDMXN",
    "USD_ZAR": "C:USDZAR", "USD_KRW": "C:USDKRW", "USD_TRY": "C:USDTRY",
    # metals (FX class)
    "XAU_USD": "C:XAUUSD", "XAG_USD": "C:XAGUSD",
}

s3 = boto3.client("s3", region_name="us-east-1")


def fetch_fx_bars(ticker: str, days: int = 30) -> List[dict]:
    to_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    from_date = (datetime.now(timezone.utc) - timedelta(days=days)).strftime("%Y-%m-%d")
    url = (f"{BASE}/v2/aggs/ticker/{ticker}/range/1/day/"
           f"{from_date}/{to_date}?adjusted=true&sort=asc&limit=60&apiKey={KEY}")
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "justhodl-fx/2.0"})
        with urllib.request.urlopen(req, timeout=12) as r:
            data = json.loads(r.read().decode())
        return data.get("results") or []
    except Exception as e:
        print(f"[fx] {ticker}: {e}")
        return []


def compute_returns(bars: List[dict]) -> dict:
    if len(bars) < 2:
        return {"error": "insufficient_data"}
    closes = [b.get("c") for b in bars if b.get("c") is not None]
    if len(closes) < 2:
        return {"error": "no_closes"}
    last = closes[-1]
    out = {"latest_price": round(last, 5)}

    def pct(n):
        if n >= len(closes) or closes[-1 - n] in (None, 0):
            return None
        return round((closes[-1] - closes[-1 - n]) / closes[-1 - n] * 100, 3)

    out["return_1d_pct"] = pct(1)
    out["return_5d_pct"] = pct(5)
    out["return_20d_pct"] = pct(20)
    if len(closes) >= 21:
        rets = [(closes[i] - closes[i - 1]) / closes[i - 1]
                for i in range(-20, 0) if closes[i - 1]]
        if rets:
            mean = sum(rets) / len(rets)
            var = sum((r - mean) ** 2 for r in rets) / max(len(rets) - 1, 1)
            out["realized_vol_20d_pct"] = round((var ** 0.5) * (252 ** 0.5) * 100, 2)
    return out


def _r5(pd, k):
    return (pd.get(k, {}) or {}).get("return_5d_pct")


def _clip(x, lo=-2.0, hi=2.0):
    return max(lo, min(hi, x))


def compute_fx_roro(pd: Dict[str, dict]) -> dict:
    """Weighted FX RORO score, + = risk-on. Each driver scaled by typical 5d move."""
    # (internal_key, sign(+1 means 'up = risk-on'), scale, weight, label)
    DRIVERS = [
        ("AUD_JPY", +1, 2.0, 0.18, "AUD/JPY carry cross"),
        ("USD_JPY", +1, 1.5, 0.15, "JPY haven (USDJPY)"),
        ("EM_BASKET", -1, 1.8, 0.15, "EM FX basket"),
        ("USD_CHF", +1, 1.2, 0.10, "CHF haven (USDCHF)"),
        ("AUD_USD", +1, 1.5, 0.10, "AUD risk currency"),
        ("XAU_USD", -1, 2.5, 0.10, "Gold haven"),
        ("NZD_USD", +1, 1.5, 0.07, "NZD risk currency"),
        ("EUR_JPY", +1, 2.0, 0.08, "EUR/JPY cross"),
        ("GSR", -1, 3.0, 0.07, "Gold/Silver ratio"),
    ]
    # EM basket mean 5d (USD vs EM; up = EM weak = risk-off). Exclude TRY (structural drift).
    em_parts = [_r5(pd, k) for k in ("USD_CNH", "USD_BRL", "USD_MXN", "USD_ZAR", "USD_KRW")]
    em_parts = [x for x in em_parts if x is not None]
    em_basket = round(sum(em_parts) / len(em_parts), 3) if em_parts else None
    # Gold/Silver ratio 5d change
    xau5, xag5 = _r5(pd, "XAU_USD"), _r5(pd, "XAG_USD")
    gsr = round(xau5 - xag5, 3) if (xau5 is not None and xag5 is not None) else None

    vals = {"EM_BASKET": em_basket, "GSR": gsr}
    contribs, tells = [], []
    total_w = 0.0
    acc = 0.0
    for key, sign, scale, w, label in DRIVERS:
        r5 = vals.get(key, _r5(pd, key))
        if r5 is None:
            continue
        c = _clip(sign * r5 / scale)
        acc += w * c
        total_w += w
        contribs.append({"driver": label, "ret_5d_pct": r5, "contribution": round(c, 2), "weight": w})
        if abs(c) >= 1.0:
            tells.append(f"{'RISK-ON' if c > 0 else 'RISK-OFF'}: {label} ({r5:+.2f}% 5d)")
    if total_w == 0:
        return {"fx_roro_score": None, "fx_roro_regime": "UNKNOWN", "drivers": [], "tells": []}
    weighted = acc / total_w  # ~[-2, 2]
    score = round(_clip(50 * weighted, -100, 100), 1)

    havens_bid = sum(1 for k in ("USD_JPY", "USD_CHF") if (_r5(pd, k) or 0) < -0.4) + \
                 (1 if (xau5 or 0) > 0.6 else 0)
    if score >= 35:
        regime = "RISK_ON"
    elif score >= 12:
        regime = "MILD_RISK_ON"
    elif score > -12:
        regime = "NEUTRAL"
    elif score > -35:
        regime = "MILD_RISK_OFF"
    else:
        regime = "FLIGHT_TO_QUALITY" if havens_bid >= 2 else "RISK_OFF"

    return {
        "fx_roro_score": score, "fx_roro_regime": regime,
        "em_basket_5d_pct": em_basket, "gold_silver_ratio_chg_5d": gsr,
        "havens_bid_count": havens_bid, "drivers": contribs, "tells": tells,
    }


def detect_regime_signals(pd: Dict[str, dict]):
    signals = []
    eur_r = (pd.get("EUR_USD", {}) or {}).get("return_20d_pct") or 0
    jpy_r = (pd.get("USD_JPY", {}) or {}).get("return_20d_pct") or 0
    gbp_r = (pd.get("GBP_USD", {}) or {}).get("return_20d_pct") or 0
    usd_synth_20d = (-eur_r * 0.576) + (jpy_r * 0.136) + (-gbp_r * 0.119)
    if usd_synth_20d > 2:
        signals.append(f"USD_STRENGTHENING_20D (+{usd_synth_20d:.2f}%)")
    elif usd_synth_20d < -2:
        signals.append(f"USD_WEAKENING_20D ({usd_synth_20d:.2f}%)")
    if jpy_r < -3:
        signals.append(f"JPY_STRENGTH_CARRY_RISK ({jpy_r:.1f}%)")
    cnh_r = (pd.get("USD_CNH", {}) or {}).get("return_20d_pct") or 0
    brl_r = (pd.get("USD_BRL", {}) or {}).get("return_20d_pct") or 0
    mxn_r = (pd.get("USD_MXN", {}) or {}).get("return_20d_pct") or 0
    em_stress = (cnh_r + brl_r + mxn_r) / 3
    if em_stress > 2:
        signals.append(f"EM_FX_STRESS (mean +{em_stress:.2f}%)")
    elif em_stress < -2:
        signals.append(f"EM_FX_STRENGTH ({em_stress:.2f}%)")
    aud_r = (pd.get("AUD_USD", {}) or {}).get("return_20d_pct") or 0
    if aud_r > 1 and jpy_r > 1:
        signals.append("COMMODITY_FX_DIVERGENCE")
    if jpy_r > 1 and usd_synth_20d > 0:
        signals.append(f"CARRY_TRADE_FAVORABLE (USDJPY +{jpy_r:.1f}%)")
    return signals, {
        "usd_synthetic_20d_pct": round(usd_synth_20d, 3),
        "em_fx_mean_20d_pct": round(em_stress, 3),
    }


def lambda_handler(event, context):
    t0 = time.time()
    print(f"[fx-regime] starting | base={BASE} key={'massive' if _MKEY else 'polygon-old'}")

    def _fetch(item):
        internal, ticker = item
        return internal, compute_returns(fetch_fx_bars(ticker, days=35))

    pair_data = {}
    with ThreadPoolExecutor(max_workers=10) as ex:
        for internal, analysis in ex.map(_fetch, FX_PAIRS.items()):
            pair_data[internal] = analysis

    signals, regime_metrics = detect_regime_signals(pair_data)
    roro = compute_fx_roro(pair_data)
    regime_metrics.update({
        "fx_roro_score": roro["fx_roro_score"],
        "fx_roro_regime": roro["fx_roro_regime"],
        "em_basket_5d_pct": roro.get("em_basket_5d_pct"),
        "gold_silver_ratio_chg_5d": roro.get("gold_silver_ratio_chg_5d"),
        "havens_bid_count": roro.get("havens_bid_count"),
    })

    elapsed = round(time.time() - t0, 1)
    print(f"[fx-regime] DONE — {len(pair_data)} pairs | RORO {roro['fx_roro_score']} "
          f"{roro['fx_roro_regime']} | {len(signals)} signals in {elapsed}s")
    for t in roro["tells"]:
        print("  •", t)

    output = {
        "engine": "polygon-fx-regime", "version": "2.0.0",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "elapsed_s": elapsed, "n_pairs": len(pair_data),
        "fx_roro": roro,
        "regime_signals": signals, "regime_metrics": regime_metrics,
        "pair_data": pair_data,
        "source": "Massive FX (entitled, daily)" if _MKEY else "polygon FX (old key)",
    }
    s3.put_object(
        Bucket=S3_BUCKET, Key="data/polygon-fx-regime.json",
        Body=json.dumps(output, default=str).encode(),
        ContentType="application/json", CacheControl="public, max-age=1800",
    )
    return {"statusCode": 200, "headers": {"Content-Type": "application/json"},
            "body": json.dumps({"ok": True, "elapsed_s": elapsed,
                                 "fx_roro_score": roro["fx_roro_score"],
                                 "fx_roro_regime": roro["fx_roro_regime"],
                                 "n_signals": len(signals), "tells": roro["tells"][:5]})}
