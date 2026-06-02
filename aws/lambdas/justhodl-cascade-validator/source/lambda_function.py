"""justhodl-cascade-validator

Forward-looking validation tracker for theme-cascade predictions.

Runs daily after market close. For each prediction logged in
data/theme-cascade-history/{date}.json over the last 7 days:
  - Fetch the ticker's price at prediction time vs current price
  - Compute returns at 1d, 3d, 5d, 7d horizons
  - Classify as HIT (>=+5% in 3d), SLOW (1-5%), MISS (<0% or flat)
  - Track per-tier hit rates: alert_tier vs laggards vs medium

OUTPUT:
  data/cascade-validation-log.json    — per-prediction return records
  data/cascade-track-record.json      — aggregate hit rates + best/worst calls

This is the proof — does the cascade actually predict pumps?
"""
import json
import time
import urllib.request
import urllib.error
from datetime import datetime, timezone, timedelta
from typing import Optional, List, Dict
from concurrent.futures import ThreadPoolExecutor

import boto3

S3_BUCKET = "justhodl-dashboard-live"
FMP_KEY = "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb"
LOOKBACK_DAYS = 7  # how many days of cascade predictions to validate
N_WORKERS = 8

s3 = boto3.client("s3", region_name="us-east-1")


def _read_json(key: str) -> Optional[dict]:
    try:
        return json.loads(s3.get_object(Bucket=S3_BUCKET, Key=key)["Body"].read())
    except Exception:
        return None


def fetch_fmp_history(ticker: str, days: int = 14) -> List[dict]:
    """Fetch last N days of EOD prices via FMP /stable/historical-price-eod/light."""
    url = (f"https://financialmodelingprep.com/stable/historical-price-eod/light"
           f"?symbol={ticker}&apikey={FMP_KEY}")
    try:
        with urllib.request.urlopen(url, timeout=12) as r:
            data = json.loads(r.read().decode())
        # FMP returns list of {date, price, volume}
        if isinstance(data, list):
            rows = sorted(data, key=lambda x: x.get("date", ""))[-days:]
            return rows
        return []
    except Exception as e:
        print(f"[fmp] {ticker}: {e}")
        return []


def get_price_at_date(rows: List[dict], target_date: str) -> Optional[float]:
    """Return the price on target_date (or closest prior date if not trading day)."""
    if not rows:
        return None
    candidates = [r for r in rows if r.get("date", "") <= target_date]
    if not candidates:
        return rows[0].get("price")
    return candidates[-1].get("price")


def compute_returns(rows: List[dict], prediction_date: str) -> dict:
    """Returns dict with 1d/3d/5d/7d returns from prediction_date."""
    if not rows:
        return {"error": "no_price_data"}

    pred_price = get_price_at_date(rows, prediction_date)
    if not pred_price:
        return {"error": "no_price_at_pred_date"}

    pred_dt = datetime.fromisoformat(prediction_date.split("T")[0])

    horizons = {}
    for days in [1, 3, 5, 7]:
        target = (pred_dt + timedelta(days=days)).strftime("%Y-%m-%d")
        future_price = get_price_at_date(rows, target)
        if future_price and pred_price > 0:
            ret_pct = (future_price - pred_price) / pred_price * 100
            horizons[f"return_{days}d_pct"] = round(ret_pct, 2)
            horizons[f"price_{days}d"] = round(future_price, 2)

    # Max return in window
    max_ret = max(
        (horizons.get(f"return_{d}d_pct") for d in [1, 3, 5, 7]
         if horizons.get(f"return_{d}d_pct") is not None),
        default=None,
    )
    if max_ret is not None:
        horizons["max_return_pct"] = max_ret

    return {
        "prediction_price": round(pred_price, 2),
        "prediction_date": prediction_date.split("T")[0],
        **horizons,
    }


def classify_outcome(returns: dict) -> str:
    """Classify a prediction's outcome.

    HIT_BIG:  >= +10% within 5d
    HIT:      >= +5% within 5d
    SLOW:     +1% to +5% within 5d
    FLAT:     -1% to +1%
    MISS:     < -1%
    PENDING:  not enough time passed
    """
    if returns.get("error"):
        return "ERROR"
    max_ret = returns.get("max_return_pct")
    r3d = returns.get("return_3d_pct")
    r5d = returns.get("return_5d_pct")
    # Use max within window
    target = max_ret if max_ret is not None else r5d if r5d is not None else r3d
    if target is None:
        return "PENDING"
    if target >= 10:
        return "HIT_BIG"
    if target >= 5:
        return "HIT"
    if target >= 1:
        return "SLOW"
    if target >= -1:
        return "FLAT"
    return "MISS"


def validate_prediction(ticker: str, prediction_date: str, tier: str,
                         combined_score: float, predicted_size_pct: float) -> dict:
    """Validate one prediction against actual price action."""
    rows = fetch_fmp_history(ticker, days=14)
    returns = compute_returns(rows, prediction_date)
    outcome = classify_outcome(returns)
    return {
        "ticker": ticker,
        "prediction_date": prediction_date.split("T")[0],
        "tier": tier,
        "combined_score": combined_score,
        "predicted_size_pct": predicted_size_pct,
        "outcome": outcome,
        **returns,
    }


def lambda_handler(event, context):
    t0 = time.time()
    print(f"[validator] starting at {datetime.now(timezone.utc).isoformat()}")

    # Step 1: Find all theme-cascade-history files in the last LOOKBACK_DAYS
    pag = s3.get_paginator("list_objects_v2")
    history_keys = []
    for page in pag.paginate(Bucket=S3_BUCKET, Prefix="data/theme-cascade-history/"):
        for obj in (page.get("Contents") or []):
            history_keys.append(obj["Key"])
    history_keys.sort()

    # Take the last LOOKBACK_DAYS files
    history_keys = history_keys[-LOOKBACK_DAYS:]
    print(f"[validator] found {len(history_keys)} history files to validate")

    # Step 2: Collect all predictions
    predictions = []
    for key in history_keys:
        doc = _read_json(key)
        if not doc:
            continue
        pred_date = doc.get("generated_at", "")
        if not pred_date:
            # Try to parse from filename
            pred_date = key.split("/")[-1].replace(".json", "") + "T20:00:00+00:00"

        # Alert tier
        for c in (doc.get("alert_tier") or [])[:25]:
            predictions.append({
                "ticker": c.get("ticker"),
                "prediction_date": pred_date,
                "tier": "ALERT_TIER",
                "combined_score": c.get("combined_score"),
                "predicted_size_pct": (c.get("position_sizing") or {}).get("final_pct"),
            })

        # Medium tier (top 10)
        for c in (doc.get("medium_tier") or [])[:10]:
            predictions.append({
                "ticker": c.get("ticker"),
                "prediction_date": pred_date,
                "tier": "MEDIUM_TIER",
                "combined_score": c.get("combined_score"),
                "predicted_size_pct": (c.get("position_sizing") or {}).get("final_pct"),
            })

        # Laggards
        for c in (doc.get("laggards_hot_themes") or [])[:15]:
            predictions.append({
                "ticker": c.get("ticker"),
                "prediction_date": pred_date,
                "tier": "LAGGARD",
                "combined_score": c.get("combined_score"),
                "predicted_size_pct": (c.get("position_sizing") or {}).get("final_pct"),
            })

    # Dedupe: keep oldest prediction per (ticker, tier)
    seen = {}
    for p in predictions:
        key = (p["ticker"], p["tier"])
        if key not in seen or p["prediction_date"] < seen[key]["prediction_date"]:
            seen[key] = p
    predictions = list(seen.values())
    print(f"[validator] validating {len(predictions)} unique predictions")

    # Step 3: Parallel-fetch price returns for each
    def _validate(p):
        try:
            return validate_prediction(
                p["ticker"], p["prediction_date"], p["tier"],
                p.get("combined_score"), p.get("predicted_size_pct"),
            )
        except Exception as e:
            return {"ticker": p["ticker"], "error": str(e)[:150]}

    results = []
    if predictions:
        with ThreadPoolExecutor(max_workers=N_WORKERS) as ex:
            for r in ex.map(_validate, predictions):
                results.append(r)

    # Step 4: Aggregate stats by tier
    by_tier = {}
    outcome_counts = {}
    for r in results:
        tier = r.get("tier", "UNKNOWN")
        outcome = r.get("outcome", "UNKNOWN")
        by_tier.setdefault(tier, []).append(r)
        outcome_counts[outcome] = outcome_counts.get(outcome, 0) + 1

    def tier_stats(records):
        if not records:
            return {"n": 0}
        n = len(records)
        outcomes = [r.get("outcome") for r in records]
        hits = sum(1 for o in outcomes if o in ("HIT", "HIT_BIG"))
        big_hits = sum(1 for o in outcomes if o == "HIT_BIG")
        slow = sum(1 for o in outcomes if o == "SLOW")
        flat = sum(1 for o in outcomes if o == "FLAT")
        miss = sum(1 for o in outcomes if o == "MISS")
        max_rets = [r.get("max_return_pct") for r in records
                    if r.get("max_return_pct") is not None]
        return {
            "n": n,
            "n_hit": hits,
            "n_hit_big": big_hits,
            "n_slow": slow,
            "n_flat": flat,
            "n_miss": miss,
            "hit_rate_pct": round(100 * hits / n, 1) if n > 0 else 0,
            "big_hit_rate_pct": round(100 * big_hits / n, 1) if n > 0 else 0,
            "mean_max_return_pct": round(sum(max_rets) / len(max_rets), 2) if max_rets else None,
            "best_return": max(max_rets) if max_rets else None,
            "worst_return": min(max_rets) if max_rets else None,
        }

    aggregated = {tier: tier_stats(records) for tier, records in by_tier.items()}

    # Top 10 best calls + worst calls
    valid_results = [r for r in results
                     if r.get("max_return_pct") is not None and not r.get("error")]
    best_calls = sorted(valid_results, key=lambda x: -(x.get("max_return_pct") or 0))[:15]
    worst_calls = sorted(valid_results, key=lambda x: (x.get("max_return_pct") or 0))[:10]

    elapsed = round(time.time() - t0, 1)
    print(f"[validator] DONE — {len(results)} validated in {elapsed}s")
    print(f"[validator] outcomes: {outcome_counts}")
    for tier, stats in aggregated.items():
        if stats.get("n", 0) > 0:
            print(f"  {tier:12s}: n={stats['n']}  "
                  f"hit_rate={stats.get('hit_rate_pct')}%  "
                  f"mean_max={stats.get('mean_max_return_pct')}%")

    output = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "elapsed_s": elapsed,
        "lookback_days": LOOKBACK_DAYS,
        "n_predictions_validated": len(results),
        "outcome_counts": outcome_counts,
        "by_tier_stats": aggregated,
        "best_calls": best_calls,
        "worst_calls": worst_calls,
        "all_results": results,
    }

    s3.put_object(
        Bucket=S3_BUCKET, Key="data/cascade-validation-log.json",
        Body=json.dumps(output, default=str).encode(),
        ContentType="application/json", CacheControl="public, max-age=3600",
    )
    # Date-stamped archive
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    s3.put_object(
        Bucket=S3_BUCKET, Key=f"data/cascade-validation-history/{today}.json",
        Body=json.dumps(output, default=str).encode(),
        ContentType="application/json", CacheControl="public, max-age=86400",
    )

    return {
        "statusCode": 200,
        "headers": {"Content-Type": "application/json",
                     "Access-Control-Allow-Origin": "*"},
        "body": json.dumps({
            "ok": True, "elapsed_s": elapsed,
            "n_validated": len(results),
            "outcome_counts": outcome_counts,
            "alert_tier_hit_rate": aggregated.get("ALERT_TIER", {}).get("hit_rate_pct"),
            "laggard_hit_rate": aggregated.get("LAGGARD", {}).get("hit_rate_pct"),
            "best_3_calls": [
                {"ticker": c.get("ticker"), "max_return": c.get("max_return_pct"),
                 "tier": c.get("tier"), "outcome": c.get("outcome")}
                for c in best_calls[:3]
            ],
        }),
    }
