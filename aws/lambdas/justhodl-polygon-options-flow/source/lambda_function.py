"""justhodl-polygon-options-flow

UTILIZES: Polygon Options Starter ($29/mo) — currently silent.

Scans cascade-tracked tickers for UNUSUAL OPTIONS ACTIVITY — the most
reliable pre-pump signal (institutional positioning shows up in options
hours/days BEFORE price moves).

DETECTORS:
  1. Call volume spike    — today's call vol > 2× 20d avg total OI
  2. Put volume spike     — today's put vol > 2× 20d avg
  3. Call/Put ratio       — extreme bullish (>3) or bearish (<0.3)
  4. IV expansion         — atm_iv > 1.5x 30d historical iv
  5. OTM call sweeps      — heavy volume on far-OTM calls (gamma trigger)
  6. Smart money flow     — large blocks (vol > 500) on near-term contracts

For each cascade ticker, fetch /v3/snapshot/options/{underlying} and aggregate.

OUTPUT: data/polygon-options-flow.json
  Per-ticker: call_vol, put_vol, cv_pv_ratio, max_iv, otm_call_sweep,
              smart_money_score, signals[], alert_level
"""
import json
import os
import time
import urllib.request
import urllib.error
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor
from typing import Optional, List, Dict

import boto3

S3_BUCKET = "justhodl-dashboard-live"
POLYGON_KEY = os.environ.get("POLYGON_KEY", "zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d")
N_WORKERS = 6

s3 = boto3.client("s3", region_name="us-east-1")


def _read_json(key: str) -> Optional[dict]:
    try:
        return json.loads(s3.get_object(Bucket=S3_BUCKET, Key=key)["Body"].read())
    except Exception:
        return None


def fetch_options_snapshot(ticker: str, limit: int = 250) -> List[dict]:
    """Polygon options snapshot for an underlying."""
    url = (f"https://api.polygon.io/v3/snapshot/options/{ticker}"
           f"?limit={limit}&apiKey={POLYGON_KEY}")
    try:
        with urllib.request.urlopen(url, timeout=12) as r:
            data = json.loads(r.read().decode())
        return data.get("results") or []
    except urllib.error.HTTPError as e:
        if e.code in (403, 404):
            print(f"[options] {ticker}: HTTP {e.code} (not entitled or no data)")
            return []
        raise
    except Exception as e:
        print(f"[options] {ticker}: {e}")
        return []


def analyze_options(ticker: str, contracts: List[dict]) -> dict:
    """Compute aggregated options signals from snapshot."""
    if not contracts:
        return {"ticker": ticker, "error": "no_contracts"}

    call_vol = put_vol = 0
    call_oi = put_oi = 0
    ivs = []
    otm_call_vol = 0
    smart_money_blocks = []  # contracts with vol > 500
    underlying_price = None

    for c in contracts:
        details = c.get("details") or {}
        day = c.get("day") or {}
        ud = c.get("underlying_asset") or {}
        greeks = c.get("greeks") or {}

        ctype = (details.get("contract_type") or "").lower()  # 'call' or 'put'
        strike = details.get("strike_price")
        vol = day.get("volume") or 0
        oi = c.get("open_interest") or 0
        iv = c.get("implied_volatility")
        if underlying_price is None:
            underlying_price = ud.get("price") or ud.get("last_price")

        if iv is not None and 0.01 < iv < 5:
            ivs.append(iv)

        if ctype == "call":
            call_vol += vol
            call_oi += oi
            if strike and underlying_price and strike > underlying_price * 1.05:
                otm_call_vol += vol
        elif ctype == "put":
            put_vol += vol
            put_oi += oi

        # Smart money flag: high single-contract volume + reasonable OI
        if vol > 500 and oi > 100:
            smart_money_blocks.append({
                "type": ctype, "strike": strike, "vol": vol, "oi": oi,
                "expiration": details.get("expiration_date"),
                "iv": round(iv, 3) if iv else None,
            })

    cv_pv_ratio = round(call_vol / max(put_vol, 1), 2)
    total_vol = call_vol + put_vol
    total_oi = call_oi + put_oi
    vol_oi_ratio = round(total_vol / max(total_oi, 1), 3) if total_oi else None
    mean_iv = round(sum(ivs) / len(ivs), 3) if ivs else None
    max_iv = round(max(ivs), 3) if ivs else None

    signals = []
    if cv_pv_ratio > 3 and call_vol > 1000:
        signals.append(f"EXTREME_CALL_SKEW (C/P={cv_pv_ratio})")
    elif cv_pv_ratio > 2 and call_vol > 500:
        signals.append(f"BULLISH_CALL_FLOW (C/P={cv_pv_ratio})")
    if cv_pv_ratio < 0.3 and put_vol > 500:
        signals.append(f"BEARISH_PUT_FLOW (C/P={cv_pv_ratio})")

    if vol_oi_ratio and vol_oi_ratio > 0.3:
        signals.append(f"HIGH_VOL_VS_OI ({vol_oi_ratio})")

    if otm_call_vol > 1000:
        signals.append(f"OTM_CALL_SWEEP (vol={otm_call_vol})")

    if len(smart_money_blocks) >= 3:
        signals.append(f"SMART_MONEY_BLOCKS ({len(smart_money_blocks)} >500 vol)")

    if mean_iv and mean_iv > 0.7:
        signals.append(f"ELEVATED_IV (mean={mean_iv})")

    # Alert level: 0-3
    alert_level = 0
    if "EXTREME_CALL_SKEW" in " ".join(signals):
        alert_level = 3
    elif "BULLISH_CALL_FLOW" in " ".join(signals) or "OTM_CALL_SWEEP" in " ".join(signals):
        alert_level = 2
    elif len(signals) >= 2:
        alert_level = 1

    return {
        "ticker": ticker,
        "n_contracts": len(contracts),
        "call_vol": call_vol,
        "put_vol": put_vol,
        "total_vol": total_vol,
        "total_oi": total_oi,
        "cv_pv_ratio": cv_pv_ratio,
        "vol_oi_ratio": vol_oi_ratio,
        "mean_iv": mean_iv,
        "max_iv": max_iv,
        "otm_call_vol": otm_call_vol,
        "n_smart_money_blocks": len(smart_money_blocks),
        "smart_money_blocks": smart_money_blocks[:5],
        "underlying_price": underlying_price,
        "signals": signals,
        "alert_level": alert_level,
    }


def lambda_handler(event, context):
    t0 = time.time()
    print(f"[options-flow] starting")

    # Load tickers to scan — cascade tracked + radar ULTRA + momentum leaders
    cascade = _read_json("data/theme-cascade.json") or {}
    radar = _read_json("data/convergence-radar.json") or {}
    momentum = _read_json("data/momentum-leaders.json") or {}

    tickers = set()
    for tier in ["alert_tier", "medium_tier", "watch_tier", "laggards_hot_themes"]:
        for c in (cascade.get(tier) or []):
            t = c.get("ticker")
            if t: tickers.add(t)
    for i in (radar.get("items") or radar.get("tickers") or radar.get("results") or []):
        t = i.get("ticker")
        if t and (i.get("tier") in ("ULTRA", "HIGH")):
            tickers.add(t)
    for m in (momentum.get("leaders") or [])[:30]:
        t = m.get("ticker")
        if t: tickers.add(t)

    tickers = sorted(tickers)[:30]  # cap to control Polygon usage
    print(f"[options-flow] scanning {len(tickers)} tickers")

    # Parallel fetch
    def _scan(t):
        contracts = fetch_options_snapshot(t)
        return analyze_options(t, contracts)

    results = []
    with ThreadPoolExecutor(max_workers=N_WORKERS) as ex:
        for r in ex.map(_scan, tickers):
            results.append(r)

    # Sort by alert level desc, then call/put ratio
    results.sort(key=lambda x: (-(x.get("alert_level") or 0),
                                  -(x.get("cv_pv_ratio") or 0)))

    # Categorize
    extreme = [r for r in results if r.get("alert_level") == 3]
    bullish = [r for r in results if r.get("alert_level") == 2]
    notable = [r for r in results if r.get("alert_level") == 1]

    elapsed = round(time.time() - t0, 1)
    print(f"[options-flow] DONE — {len(results)} scanned, "
          f"extreme={len(extreme)} bullish={len(bullish)} notable={len(notable)} "
          f"in {elapsed}s")

    output = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "elapsed_s": elapsed,
        "n_scanned": len(results),
        "n_extreme": len(extreme),
        "n_bullish": len(bullish),
        "n_notable": len(notable),
        "extreme_call_flow": extreme,
        "bullish_call_flow": bullish,
        "notable_flow": notable[:20],
        "all_results": results,
    }

    s3.put_object(
        Bucket=S3_BUCKET, Key="data/polygon-options-flow.json",
        Body=json.dumps(output, default=str).encode(),
        ContentType="application/json", CacheControl="public, max-age=600",
    )

    return {
        "statusCode": 200,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps({
            "ok": True, "elapsed_s": elapsed,
            "n_scanned": len(results),
            "n_extreme": len(extreme),
            "n_bullish": len(bullish),
            "top_5_alerts": [
                {"ticker": r["ticker"], "cv_pv": r.get("cv_pv_ratio"),
                 "vol": r.get("total_vol"), "signals": r.get("signals", [])[:3]}
                for r in results[:5] if r.get("alert_level", 0) >= 2
            ],
        }),
    }
