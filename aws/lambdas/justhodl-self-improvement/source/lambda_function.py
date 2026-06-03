"""justhodl-self-improvement

Closes the self-improvement loop. Runs daily morning before market open.

PIPELINE:
  1. Read yesterday's predictions snapshot (data/predictions-snapshots/{yesterday}.json)
  2. Fetch current prices for each ticker via Polygon
  3. Score each prediction: did it pump 1d / 3d / 5d later?
  4. Attribute outcomes to FEATURES — which features predict success?
  5. Compute calibrated weights: upweight winning features, downweight losers
  6. Write calibration recommendations to data/cascade-calibration.json
  7. Send weekly Telegram digest with stats + new weights

GOAL: cascade scoring auto-tunes itself based on what actually predicts pumps.
After 30+ days of data, the system should know which features matter and
adjust scoring weights to maximize hit rate.

OUTPUT:
  data/predictions-scored/{date}.json    — yesterday's outcomes
  data/cascade-calibration.json          — current calibrated weights
  data/calibration-history.json          — weight evolution over time
"""
import json
import os
import time
import urllib.request
import urllib.parse
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor
from typing import Optional, List, Dict, Tuple

import boto3

S3_BUCKET = "justhodl-dashboard-live"
POLYGON_KEY = os.environ.get("POLYGON_KEY", "zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d")
FMP_KEY = "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb"
TG_BOT_TOKEN = "8679881066:AAHTE6TAhDqs0FuUelTL6Ppt1x8ihis1aGs"
TG_CHAT_ID = "8678089260"

s3 = boto3.client("s3", region_name="us-east-1")
ssm = boto3.client("ssm", region_name="us-east-1")


def _read_json(key: str) -> Optional[dict]:
    try:
        return json.loads(s3.get_object(Bucket=S3_BUCKET, Key=key)["Body"].read())
    except Exception:
        return None


def _send_telegram(text: str) -> dict:
    try:
        token = ssm.get_parameter(Name="/justhodl/telegram/bot-token",
                                    WithDecryption=True)["Parameter"]["Value"]
        chat_id = ssm.get_parameter(Name="/justhodl/telegram/chat_id")["Parameter"]["Value"]
    except Exception:
        token, chat_id = TG_BOT_TOKEN, TG_CHAT_ID
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    data = urllib.parse.urlencode({
        "chat_id": chat_id, "text": text, "parse_mode": "HTML",
        "disable_web_page_preview": "true",
    }).encode()
    try:
        req = urllib.request.Request(url, data=data, method="POST")
        with urllib.request.urlopen(req, timeout=12) as r:
            return {"status": r.status}
    except Exception as e:
        return {"error": str(e)[:200]}


def fetch_fmp_history(ticker: str) -> List[dict]:
    """Fetch last 40 days of EOD prices via FMP (covers 30d horizon + buffer)."""
    url = (f"https://financialmodelingprep.com/stable/historical-price-eod/light"
           f"?symbol={ticker}&apikey={FMP_KEY}")
    try:
        with urllib.request.urlopen(url, timeout=10) as r:
            data = json.loads(r.read().decode())
        if isinstance(data, list):
            return sorted(data, key=lambda x: x.get("date", ""))[-40:]
        return []
    except Exception:
        return []


def get_price_on_or_after(rows: List[dict], target_date: str) -> Optional[float]:
    """Return price on target_date (or earliest date >= target)."""
    for r in rows:
        if r.get("date", "") >= target_date:
            return r.get("price")
    return None


def score_prediction(pred: dict, price_rows: List[dict]) -> dict:
    """Compute returns at multiple horizons + classify outcome.

    Horizons: 1, 3, 5, 7, 14, 21, 30 days.
    Different features predict different horizons:
      - Options gamma squeezes → 1-3 day moves
      - Theme acceleration → 5-14 day moves
      - Insider clusters → 14-30 day moves
    """
    snapshot_date = pred.get("snapshot_date")
    if not snapshot_date or not price_rows:
        return {"error": "insufficient_data"}

    entry_price = get_price_on_or_after(price_rows, snapshot_date)
    if not entry_price or entry_price <= 0:
        return {"error": "no_entry_price"}

    snapshot_dt = datetime.fromisoformat(snapshot_date)
    out = {"entry_price": round(entry_price, 2)}

    HORIZONS = [1, 3, 5, 7, 14, 21, 30]
    for days in HORIZONS:
        target = (snapshot_dt + timedelta(days=days)).strftime("%Y-%m-%d")
        future_price = get_price_on_or_after(price_rows, target)
        if future_price and entry_price > 0:
            ret = (future_price - entry_price) / entry_price * 100
            out[f"return_{days}d_pct"] = round(ret, 2)
            out[f"price_{days}d"] = round(future_price, 2)

    # Best return across all available horizons (primary outcome metric)
    returns = [out.get(f"return_{d}d_pct") for d in HORIZONS
               if out.get(f"return_{d}d_pct") is not None]
    if returns:
        out["max_return_pct"] = max(returns)

    # Classify by max return
    mr = out.get("max_return_pct")
    if mr is None:
        out["outcome"] = "PENDING"
    elif mr >= 10:
        out["outcome"] = "HIT_BIG"
    elif mr >= 5:
        out["outcome"] = "HIT"
    elif mr >= 1:
        out["outcome"] = "SLOW"
    elif mr >= -1:
        out["outcome"] = "FLAT"
    else:
        out["outcome"] = "MISS"

    # Did it pump WITHIN 1 DAY?
    r1 = out.get("return_1d_pct")
    out["pumped_within_1d"] = bool(r1 is not None and r1 >= 5)

    # Per-horizon hit flags (for horizon-specific attribution)
    out["hit_by_horizon"] = {}
    for days in HORIZONS:
        r = out.get(f"return_{days}d_pct")
        if r is not None:
            out["hit_by_horizon"][f"{days}d"] = r >= 5  # ≥+5% is a hit
        else:
            out["hit_by_horizon"][f"{days}d"] = None

    return out


def compute_feature_attribution(scored_preds: List[dict]) -> dict:
    """For each feature, compute its predictive power.

    Method: for each feature, split tickers into top-quartile vs bottom-quartile
    by feature value. Compare hit_rate in top vs bottom. The "lift" is how much
    the feature improves hit rate.
    """
    # Group all predictions with valid outcomes
    valid = [p for p in scored_preds if p.get("outcome") in ("HIT", "HIT_BIG", "SLOW", "FLAT", "MISS")]
    if len(valid) < 5:
        return {"insufficient_data": True, "n_valid": len(valid)}

    features_to_test = [
        "combined_score", "theme_acceleration", "n_etfs_in_top_10",
        "n_etfs_in_top_20", "aggregate_flow_5d_usd", "perf_20d_pct",
        "options_cv_pv_ratio", "options_call_vol", "options_mean_iv",
        "velocity_composite", "convergence_score", "n_engines",
        "early_score", "insider_n_buyers", "ticket_atr_pct",
    ]

    attribution = _compute_attribution_for_group(valid, features_to_test)
    return attribution


def _compute_attribution_for_group(preds: List[dict], features_to_test: List[str]) -> dict:
    """Helper: compute attribution for a specific group of predictions."""
    if len(preds) < 5:
        return {"insufficient_data": True, "n_valid": len(preds)}

    attribution = {}
    for feat in features_to_test:
        with_feat = []
        for p in preds:
            val = (p.get("features") or {}).get(feat)
            if val is not None and isinstance(val, (int, float)):
                with_feat.append({"val": val, "outcome": p["outcome"],
                                   "max_return": p.get("max_return_pct") or 0})
        if len(with_feat) < 5:
            continue

        with_feat.sort(key=lambda x: x["val"])
        n = len(with_feat)
        q_size = max(1, n // 4)
        bottom_q = with_feat[:q_size]
        top_q = with_feat[-q_size:]

        def hit_rate(group):
            hits = sum(1 for g in group if g["outcome"] in ("HIT", "HIT_BIG"))
            return hits / len(group) if group else 0

        def avg_return(group):
            return sum(g["max_return"] for g in group) / len(group) if group else 0

        top_hr = hit_rate(top_q)
        bot_hr = hit_rate(bottom_q)
        top_avg_ret = avg_return(top_q)
        bot_avg_ret = avg_return(bottom_q)

        attribution[feat] = {
            "n_with_feature": len(with_feat),
            "top_quartile_hit_rate": round(top_hr * 100, 1),
            "bottom_quartile_hit_rate": round(bot_hr * 100, 1),
            "hit_rate_lift_pp": round((top_hr - bot_hr) * 100, 1),
            "top_quartile_avg_return": round(top_avg_ret, 2),
            "bottom_quartile_avg_return": round(bot_avg_ret, 2),
            "return_lift_pct": round(top_avg_ret - bot_avg_ret, 2),
        }

    ranked = sorted(attribution.items(),
                     key=lambda x: -(x[1].get("hit_rate_lift_pp") or 0))

    return {
        "n_predictions_analyzed": len(preds),
        "features_analyzed": list(attribution.keys()),
        "by_feature": attribution,
        "ranked_by_hit_rate_lift": [
            {"feature": k, "hit_rate_lift_pp": v.get("hit_rate_lift_pp"),
             "return_lift_pct": v.get("return_lift_pct"),
             "top_q_hit_rate": v.get("top_quartile_hit_rate")}
            for k, v in ranked
        ],
    }


def compute_per_tier_attribution(scored_preds: List[dict]) -> dict:
    """Compute attribution SEPARATELY for each alert tier.

    Different tiers (ALERT, LAGGARD, FIRED, OPTIONS_EXTREME, INSIDER_CLUSTER,
    CONVERGENCE_ULTRA, RETAIL_VELOCITY) likely have different predictive features.
    This computes weights per group so the recalibrator can use tier-specific weights.
    """
    features_to_test = [
        "combined_score", "theme_acceleration", "n_etfs_in_top_10",
        "n_etfs_in_top_20", "aggregate_flow_5d_usd", "perf_20d_pct",
        "options_cv_pv_ratio", "options_call_vol", "options_mean_iv",
        "velocity_composite", "convergence_score", "n_engines",
        "early_score", "insider_n_buyers", "ticket_atr_pct",
        # Retail features
        "retail_velocity_pct", "retail_mentions", "retail_rank_climb",
        # News/earnings/GDELT features
        "news_score", "earnings_score", "gdelt_tone", "gdelt_articles",
        "politician_conviction", "politician_n_buyers", "politician_committee_relevant", "politician_cluster",
        "days_since_earnings",
    ]

    # Define tier classifiers
    def classify(alerts: List[str]) -> str:
        """Return the primary tier classification for a prediction."""
        alerts_set = set(alerts or [])
        # Priority order: most specific signal wins
        if "POLITICIAN_COMMITTEE" in alerts_set:
            return "POLITICIAN_COMMITTEE"
        if "POLITICIAN_BUY" in alerts_set:
            return "POLITICIAN_BUY"
        if "RETAIL_HOT" in alerts_set:
            return "RETAIL_HOT"
        if "RETAIL_VELOCITY" in alerts_set:
            return "RETAIL_VELOCITY"
        if "NEWS_SURGE_BULLISH" in alerts_set:
            return "NEWS_SURGE"
        if "EARNINGS_FRESH" in alerts_set:
            return "EARNINGS_FRESH"
        if "CASCADE_ALERT" in alerts_set:
            return "ALERT"
        if "CASCADE_LAGGARD" in alerts_set:
            return "LAGGARD"
        if any(a.startswith("VELOCITY_FIRED") for a in alerts_set):
            return "VELOCITY_FIRED"
        if "OPTIONS_EXTREME_CALL" in alerts_set:
            return "OPTIONS_EXTREME"
        if "OPTIONS_BULLISH_CALL" in alerts_set:
            return "OPTIONS_BULLISH"
        if "INSIDER_CLUSTER" in alerts_set:
            return "INSIDER_CLUSTER"
        if any(a.startswith("CONVERGENCE_") for a in alerts_set):
            return "CONVERGENCE"
        if "EARLY_MOVER_ALERT" in alerts_set:
            return "EARLY_MOVER"
        if "CASCADE_MEDIUM" in alerts_set:
            return "MEDIUM"
        return "OTHER"

    valid = [p for p in scored_preds if p.get("outcome") in ("HIT", "HIT_BIG", "SLOW", "FLAT", "MISS")]

    # Group by tier
    by_tier = {}
    for p in valid:
        tier = classify(p.get("alerts") or [])
        by_tier.setdefault(tier, []).append(p)

    # Compute attribution per tier
    per_tier = {}
    for tier, preds in by_tier.items():
        attribution = _compute_attribution_for_group(preds, features_to_test)
        per_tier[tier] = attribution

    # Also compute global
    global_attribution = _compute_attribution_for_group(valid, features_to_test)

    return {
        "global": global_attribution,
        "by_tier": per_tier,
        "tier_distribution": {tier: len(preds) for tier, preds in by_tier.items()},
        "n_valid_total": len(valid),
    }


def compute_multi_horizon_attribution(scored_preds: List[dict]) -> dict:
    """For each feature, compute predictive lift at EACH horizon separately.

    Reveals WHICH horizon each feature best predicts:
      - options_cv_pv_ratio might have +35pp lift at 1d, +10pp at 30d → 1d feature
      - insider_n_buyers might have +5pp at 1d, +28pp at 30d → 30d feature
      - theme_acceleration might peak at 5-7d → medium horizon

    This data drives horizon-aware position holding periods.
    """
    HORIZONS = [1, 3, 5, 7, 14, 21, 30]
    features_to_test = [
        "combined_score", "theme_acceleration", "n_etfs_in_top_10",
        "n_etfs_in_top_20", "aggregate_flow_5d_usd", "perf_20d_pct",
        "options_cv_pv_ratio", "options_call_vol", "options_mean_iv",
        "velocity_composite", "convergence_score", "n_engines",
        "early_score", "insider_n_buyers", "ticket_atr_pct",
        # Retail features
        "retail_velocity_pct", "retail_mentions", "retail_rank_climb",
        # NEW: News/earnings/GDELT features
        "news_score", "earnings_score", "gdelt_tone", "gdelt_articles",
        "politician_conviction", "politician_n_buyers", "politician_committee_relevant", "politician_cluster",
        "days_since_earnings",
    ]

    valid = [p for p in scored_preds if p.get("outcome") in ("HIT", "HIT_BIG", "SLOW", "FLAT", "MISS")]
    if len(valid) < 5:
        return {"insufficient_data": True, "n_valid": len(valid)}

    # For each horizon, compute attribution using horizon-specific hit flags
    by_horizon = {}
    for h in HORIZONS:
        h_key = f"{h}d"
        # Build a synthetic "outcome" per ticker based on this horizon's return
        preds_for_horizon = []
        for p in valid:
            r = p.get(f"return_{h}d_pct")
            if r is None:
                continue
            # Re-classify based on this horizon's return only
            if r >= 10:
                outcome = "HIT_BIG"
            elif r >= 5:
                outcome = "HIT"
            elif r >= 1:
                outcome = "SLOW"
            elif r >= -1:
                outcome = "FLAT"
            else:
                outcome = "MISS"
            p_copy = dict(p)
            p_copy["outcome"] = outcome
            p_copy["max_return_pct"] = r  # use horizon-specific return
            preds_for_horizon.append(p_copy)

        if len(preds_for_horizon) >= 5:
            attribution = _compute_attribution_for_group(preds_for_horizon, features_to_test)
            by_horizon[h_key] = attribution
        else:
            by_horizon[h_key] = {"insufficient_data": True, "n": len(preds_for_horizon)}

    # For each feature, find its BEST horizon (highest lift_pp)
    best_horizon_per_feature = {}
    for feat in features_to_test:
        best_lift = None
        best_h = None
        for h_key, attr in by_horizon.items():
            if attr.get("insufficient_data"):
                continue
            f_attr = (attr.get("by_feature") or {}).get(feat) or {}
            lift = f_attr.get("hit_rate_lift_pp")
            if lift is not None and (best_lift is None or lift > best_lift):
                best_lift = lift
                best_h = h_key
        if best_h:
            best_horizon_per_feature[feat] = {
                "best_horizon": best_h,
                "best_lift_pp": best_lift,
            }

    return {
        "by_horizon": by_horizon,
        "best_horizon_per_feature": best_horizon_per_feature,
        "n_valid_total": len(valid),
    }


def compute_calibrated_weights(attribution: dict) -> dict:
    """Compute calibrated scoring weights from feature attribution.

    Features with high lift get higher weights. Negative lift = downweight.
    """
    if attribution.get("insufficient_data"):
        return {"is_calibrated": False, "reason": "insufficient_data",
                "weights": {}, "n_data_points": attribution.get("n_valid", 0)}

    weights = {}
    for feat, stats in (attribution.get("by_feature") or {}).items():
        lift = stats.get("hit_rate_lift_pp", 0) or 0
        # Map lift to multiplier: 0pp lift = 1.0, +20pp = 1.5, +50pp = 2.0
        # Negative lift → multiplier < 1
        multiplier = 1.0 + (lift / 100)
        multiplier = max(0.3, min(2.5, multiplier))
        weights[feat] = round(multiplier, 3)

    return {
        "is_calibrated": True,
        "weights": weights,
        "n_data_points": attribution.get("n_predictions_analyzed", 0),
        "methodology": (
            "weight = 1.0 + (top_quartile_hit_rate - bottom_quartile_hit_rate). "
            "Clamped to [0.3, 2.5]."
        ),
    }


def build_telegram_digest(scored_preds: List[dict], attribution: dict,
                          weights: dict, yesterday: str) -> str:
    """Build daily/weekly Telegram digest message."""
    valid = [p for p in scored_preds if p.get("outcome") in ("HIT", "HIT_BIG", "SLOW", "FLAT", "MISS")]
    if not valid:
        return ""

    n = len(valid)
    n_hit_big = sum(1 for p in valid if p["outcome"] == "HIT_BIG")
    n_hit = sum(1 for p in valid if p["outcome"] == "HIT")
    n_pumped_1d = sum(1 for p in valid if p.get("pumped_within_1d"))
    hit_rate_overall = (n_hit + n_hit_big) / n * 100 if n > 0 else 0
    pump_1d_rate = n_pumped_1d / n * 100 if n > 0 else 0

    # Best calls
    best = sorted(valid, key=lambda x: -(x.get("max_return_pct") or 0))[:5]
    worst = sorted(valid, key=lambda x: x.get("max_return_pct") or 0)[:3]

    lines = [
        f"<b>🧠 SELF-IMPROVEMENT LOOP · {yesterday}</b>",
        f"<i>Yesterday's predictions scored against today's prices</i>",
        "",
        f"<b>📊 OUTCOMES ({n} predictions)</b>",
        f"  ✅ HIT_BIG (≥+10%): <b>{n_hit_big}</b>",
        f"  ✅ HIT (≥+5%): <b>{n_hit}</b>",
        f"  📈 SLOW (+1-5%): {sum(1 for p in valid if p['outcome'] == 'SLOW')}",
        f"  ➡️ FLAT: {sum(1 for p in valid if p['outcome'] == 'FLAT')}",
        f"  ❌ MISS: {sum(1 for p in valid if p['outcome'] == 'MISS')}",
        "",
        f"<b>🎯 HIT RATES</b>",
        f"  Overall hit rate: <b>{hit_rate_overall:.1f}%</b>",
        f"  Pumped within 1 day: <b>{pump_1d_rate:.1f}%</b>",
        "",
    ]

    if best:
        lines.append("<b>🏆 BEST CALLS</b>")
        for p in best[:5]:
            ticker = p.get("ticker", "?")
            mr = p.get("max_return_pct", 0)
            alerts = ", ".join(p.get("alerts", [])[:2])
            lines.append(f"  • <b>{ticker}</b>: <code>{mr:+.2f}%</code> · {alerts}")
        lines.append("")

    if worst:
        lines.append("<b>⚠️ WORST CALLS</b>")
        for p in worst[:3]:
            ticker = p.get("ticker", "?")
            mr = p.get("max_return_pct", 0)
            lines.append(f"  • <b>{ticker}</b>: <code>{mr:+.2f}%</code>")
        lines.append("")

    # Feature attribution
    if not attribution.get("insufficient_data"):
        ranked = attribution.get("ranked_by_hit_rate_lift", [])[:5]
        if ranked:
            lines.append("<b>🔬 TOP PREDICTIVE FEATURES</b>")
            for r in ranked:
                feat = r.get("feature", "?")
                lift = r.get("hit_rate_lift_pp", 0)
                if abs(lift) >= 5:  # only show meaningful lifts
                    arrow = "↑" if lift > 0 else "↓"
                    lines.append(f"  {arrow} <code>{feat}</code>: {lift:+.1f}pp hit-rate lift")
            lines.append("")

    if weights.get("is_calibrated"):
        n_dp = weights.get("n_data_points", 0)
        lines.append(f"<i>Calibration: {n_dp} data points · weights updated.</i>")
        lines.append(f"<i>Continue improving — accuracy compounds over time.</i>")
    else:
        lines.append(f"<i>Calibration: building dataset (need 5+ data points).</i>")

    return "\n".join(lines).strip()


def lambda_handler(event, context):
    t0 = time.time()
    yesterday = (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d")
    print(f"[self-improvement] checking yesterday's predictions ({yesterday})")

    # Load yesterday's snapshot
    snapshot = _read_json(f"data/predictions-snapshots/{yesterday}.json")
    if not snapshot:
        print(f"[self-improvement] no snapshot for {yesterday} — first run? Use latest")
        snapshot = _read_json("data/predictions-snapshots/latest.json")
        if not snapshot:
            return {"statusCode": 200,
                    "body": json.dumps({"ok": True, "msg": "no_snapshot_yet"})}

    predictions = snapshot.get("predictions") or []
    print(f"[self-improvement] scoring {len(predictions)} predictions")

    # Parallel fetch FMP prices for each ticker
    def _score(pred):
        rows = fetch_fmp_history(pred.get("ticker"))
        return {
            "ticker": pred.get("ticker"),
            "snapshot_date": pred.get("snapshot_date"),
            "alerts": pred.get("alerts"),
            "features": pred.get("features"),
            **score_prediction(pred, rows),
        }

    scored = []
    with ThreadPoolExecutor(max_workers=8) as ex:
        for r in ex.map(_score, predictions):
            scored.append(r)

    # Compute feature attribution + calibrated weights (BOTH global AND per-tier)
    attribution = compute_feature_attribution(scored)
    weights = compute_calibrated_weights(attribution)
    
    # Per-tier attribution
    per_tier_attribution = compute_per_tier_attribution(scored)
    per_tier_weights = {}
    for tier, tier_attr in (per_tier_attribution.get("by_tier") or {}).items():
        per_tier_weights[tier] = compute_calibrated_weights(tier_attr).get("weights") or {}

    # Multi-horizon attribution (which features predict which horizon)
    horizon_attribution = compute_multi_horizon_attribution(scored)

    # Save outputs
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    s3.put_object(
        Bucket=S3_BUCKET, Key=f"data/predictions-scored/{today}.json",
        Body=json.dumps({
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "snapshot_date_scored": yesterday,
            "n_predictions": len(scored),
            "scored": scored,
        }, default=str).encode(),
        ContentType="application/json", CacheControl="public, max-age=86400",
    )

    # Update cumulative calibration (with per-tier weights AND multi-horizon)
    cal_doc = _read_json("data/cascade-calibration.json") or {"history": []}
    cal_doc["last_updated"] = datetime.now(timezone.utc).isoformat()
    cal_doc["current_weights"] = weights.get("weights") or {}
    cal_doc["current_weights_by_tier"] = per_tier_weights
    cal_doc["feature_attribution"] = attribution
    cal_doc["feature_attribution_by_tier"] = per_tier_attribution
    cal_doc["horizon_attribution"] = horizon_attribution
    cal_doc["history"] = (cal_doc.get("history") or [])[-29:]  # keep last 30 entries
    cal_doc["history"].append({
        "date": today,
        "n_predictions_analyzed": weights.get("n_data_points", 0),
        "weights": weights.get("weights") or {},
        "weights_by_tier": per_tier_weights,
        "tier_distribution": per_tier_attribution.get("tier_distribution") or {},
        "best_horizon_per_feature": horizon_attribution.get("best_horizon_per_feature") or {},
    })
    s3.put_object(
        Bucket=S3_BUCKET, Key="data/cascade-calibration.json",
        Body=json.dumps(cal_doc, default=str).encode(),
        ContentType="application/json", CacheControl="public, max-age=3600",
    )

    # Send Telegram digest
    msg = build_telegram_digest(scored, attribution, weights, yesterday)
    tg = {}
    if msg:
        tg = _send_telegram(msg)

    elapsed = round(time.time() - t0, 1)
    valid_count = sum(1 for s in scored
                      if s.get("outcome") in ("HIT", "HIT_BIG", "SLOW", "FLAT", "MISS"))
    print(f"[self-improvement] DONE — {valid_count}/{len(scored)} valid outcomes in {elapsed}s")

    return {
        "statusCode": 200,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps({
            "ok": True, "elapsed_s": elapsed,
            "n_predictions_scored": len(scored),
            "n_valid_outcomes": valid_count,
            "n_features_attributed": len((attribution.get("by_feature") or {})),
            "calibrated": weights.get("is_calibrated", False),
            "telegram_status": tg.get("status"),
        }),
    }
