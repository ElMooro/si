"""justhodl-polygon-options-flow

UTILIZES: Polygon Options Starter ($29/mo) — daily aggregates, Greeks, OI.
Does NOT include options trades or quotes. Daily volume is not a sweep and
not smart money. Relabeled F14.

DETECTORS (all from snapshot aggregates, inference_type=aggregate_anomaly):
  1. Call/put volume skew
  2. Volume vs open interest
  3. Far-OTM call volume (NOT a sweep)
  4. High single-contract volume (NOT a block print / not initiator)
  5. Elevated IV

OUTPUT: data/polygon-options-flow.json
"""
import json
import os
import time
import urllib.request
import urllib.error
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor
from typing import Optional, List

import boto3
from managed_secret import managed_secret

S3_BUCKET = "justhodl-dashboard-live"
POLYGON_KEY = managed_secret(('POLYGON_KEY', 'POLYGON_API_KEY', 'POLY_KEY'), ("/justhodl/polygon/api-key",))
N_WORKERS = 6
VERSION = "2.0.0"

s3 = boto3.client("s3", region_name="us-east-1")


def _read_json(key: str) -> Optional[dict]:
    try:
        return json.loads(s3.get_object(Bucket=S3_BUCKET, Key=key)["Body"].read())
    except Exception:
        return None


def fetch_options_snapshot(ticker: str, limit: int = 250) -> List[dict]:
    """First page of the options snapshot. Completeness is flagged, not implied."""
    url = (f"https://api.polygon.io/v3/snapshot/options/{ticker}"
           f"?limit={limit}&apiKey={POLYGON_KEY}")
    try:
        with urllib.request.urlopen(url, timeout=12) as r:
            data = json.loads(r.read().decode())
        rows = data.get("results") or []
        return rows, bool(data.get("next_url"))
    except urllib.error.HTTPError as e:
        if e.code in (403, 404):
            print(f"[options] {ticker}: HTTP {e.code} (not entitled or no data)")
            return [], False
        raise
    except Exception as e:
        print(f"[options] {ticker}: {e}")
        return [], False


def analyze_options(ticker: str, contracts: List[dict], truncated: bool) -> dict:
    if not contracts:
        return {"ticker": ticker, "error": "no_contracts", "inference_type": "aggregate_anomaly"}

    call_vol = put_vol = 0
    call_oi = put_oi = 0
    ivs = []
    otm_call_vol = 0
    high_vol_contracts = []
    underlying_price = None

    for c in contracts:
        details = c.get("details") or {}
        day = c.get("day") or {}
        ud = c.get("underlying_asset") or {}

        ctype = (details.get("contract_type") or "").lower()
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

        if vol > 500 and oi > 100:
            high_vol_contracts.append({
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
        signals.append(f"OTM_CALL_VOLUME (vol={otm_call_vol})")
    if len(high_vol_contracts) >= 3:
        signals.append(f"HIGH_CONTRACT_VOLUME ({len(high_vol_contracts)} >500 vol)")
    if mean_iv and mean_iv > 0.7:
        signals.append(f"ELEVATED_IV (mean={mean_iv})")

    alert_level = 0
    if "EXTREME_CALL_SKEW" in " ".join(signals):
        alert_level = 3
    elif "BULLISH_CALL_FLOW" in " ".join(signals) or "OTM_CALL_VOLUME" in " ".join(signals):
        alert_level = 2
    elif len(signals) >= 2:
        alert_level = 1

    return {
        "ticker": ticker,
        "n_contracts": len(contracts),
        "snapshot_truncated": truncated,
        "completeness": "partial_first_page" if truncated else "first_page",
        "inference_type": "aggregate_anomaly",
        "evidence_note": "Options Starter has no trades/quotes. Volume is a daily aggregate, not a sweep or block.",
        "call_vol": call_vol,
        "put_vol": put_vol,
        "total_vol": total_vol,
        "total_oi": total_oi,
        "cv_pv_ratio": cv_pv_ratio,
        "vol_oi_ratio": vol_oi_ratio,
        "mean_iv": mean_iv,
        "max_iv": max_iv,
        "otm_call_vol": otm_call_vol,
        "n_high_volume_contracts": len(high_vol_contracts),
        "high_volume_contracts": high_vol_contracts[:5],
        "underlying_price": underlying_price,
        "signals": signals,
        "alert_level": alert_level,
    }


def _pick_universe():
    """Held / alert names first. Alphabetical slice was F15."""
    cascade = _read_json("data/theme-cascade.json") or {}
    radar = _read_json("data/convergence-radar.json") or {}
    momentum = _read_json("data/momentum-leaders.json") or {}
    tickets = _read_json("data/trade-tickets.json") or {}
    ordered, seen = [], set()

    def add(t):
        t = (t or "").upper().strip()
        if t and t not in seen:
            seen.add(t)
            ordered.append(t)

    for t in (tickets.get("open") or tickets.get("tickets") or tickets.get("positions") or []):
        if isinstance(t, dict):
            add(t.get("ticker") or t.get("symbol"))
        elif isinstance(t, str):
            add(t)
    for c in (cascade.get("alert_tier") or []):
        add(c.get("ticker") if isinstance(c, dict) else c)
    for i in (radar.get("items") or radar.get("tickers") or []):
        if isinstance(i, dict) and i.get("tier") in ("ULTRA", "HIGH"):
            add(i.get("ticker"))
    for c in (cascade.get("medium_tier") or []):
        add(c.get("ticker") if isinstance(c, dict) else c)
    for m in (momentum.get("leaders") or [])[:30]:
        add(m.get("ticker") if isinstance(m, dict) else m)
    for c in (cascade.get("watch_tier") or []) + (cascade.get("laggards_hot_themes") or []):
        add(c.get("ticker") if isinstance(c, dict) else c)
    return ordered[:40]


def lambda_handler(event, context):
    t0 = time.time()
    tickers = _pick_universe()
    print(f"[options-flow] scanning {len(tickers)} tickers (priority, not alpha)")

    def _scan(t):
        contracts, truncated = fetch_options_snapshot(t)
        return analyze_options(t, contracts, truncated)

    results = []
    with ThreadPoolExecutor(max_workers=N_WORKERS) as ex:
        for r in ex.map(_scan, tickers):
            results.append(r)

    results.sort(key=lambda x: (-(x.get("alert_level") or 0),
                                  -(x.get("cv_pv_ratio") or 0)))
    extreme = [r for r in results if r.get("alert_level") == 3]
    bullish = [r for r in results if r.get("alert_level") == 2]
    notable = [r for r in results if r.get("alert_level") == 1]

    elapsed = round(time.time() - t0, 1)
    output = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "engine": "justhodl-polygon-options-flow",
        "version": VERSION,
        "inference_type": "aggregate_anomaly",
        "evidence_note": "No sweeps, blocks, or smart-money initiator. Options Starter has no trades/quotes.",
        "elapsed_s": elapsed,
        "n_scanned": len(results),
        "n_requested": len(tickers),
        "universe": tickers,
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
