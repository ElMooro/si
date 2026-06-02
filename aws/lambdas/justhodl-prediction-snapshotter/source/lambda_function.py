"""justhodl-prediction-snapshotter

Daily snapshot of EVERY alerted ticker with FULL feature vector.
This is the dataset that powers self-improvement.

For each ticker alerted in cascade/velocity/options-flow/etc today, capture:
  ALL FEATURES used to predict, plus current prediction tier/score.

Tomorrow's self-improvement Lambda will check if these pumped, then
attribute outcomes back to features — discovering which features
actually predict price moves.

OUTPUT:
  data/predictions-snapshots/{date}.json
"""
import json
import time
from datetime import datetime, timezone
from typing import Optional, List, Dict

import boto3

S3_BUCKET = "justhodl-dashboard-live"
s3 = boto3.client("s3", region_name="us-east-1")


def _read_json(key: str) -> Optional[dict]:
    try:
        return json.loads(s3.get_object(Bucket=S3_BUCKET, Key=key)["Body"].read())
    except Exception:
        return None


def lambda_handler(event, context):
    t0 = time.time()
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    print(f"[snapshotter] starting for {today}")

    # Load all relevant signal sources
    cascade = _read_json("data/theme-cascade.json") or {}
    velocity = _read_json("data/velocity-acceleration.json") or {}
    radar = _read_json("data/convergence-radar.json") or {}
    early = _read_json("data/early-movers.json") or {}
    options = _read_json("data/polygon-options-flow.json") or {}
    fx = _read_json("data/polygon-fx-regime.json") or {}
    futures = _read_json("data/polygon-futures-curves.json") or {}
    insider = _read_json("data/insider-clusters.json") or {}
    activist = _read_json("data/activist-13d.json") or {}
    tickets = _read_json("data/trade-tickets.json") or {}

    # Build per-ticker feature vectors
    predictions = {}

    # Cascade tiers
    for tier_key, tier_label in [
        ("alert_tier", "CASCADE_ALERT"),
        ("medium_tier", "CASCADE_MEDIUM"),
        ("laggards_hot_themes", "CASCADE_LAGGARD"),
    ]:
        for c in (cascade.get(tier_key) or []):
            t = c.get("ticker")
            if not t:
                continue
            predictions.setdefault(t, {"ticker": t, "snapshot_date": today,
                                        "alerts": [], "features": {}})
            predictions[t]["alerts"].append(tier_label)
            predictions[t]["features"].update({
                "combined_score": c.get("combined_score"),
                "base_score": c.get("base_score"),
                "theme_multiplier": c.get("theme_multiplier"),
                "flow_multiplier": c.get("flow_multiplier"),
                "theme_acceleration": c.get("theme_acceleration") or c.get("max_rs_acceleration"),
                "theme_rs_rank": c.get("theme_rs_rank") or c.get("min_rs_rank"),
                "n_etfs_in_top_10": c.get("n_etfs_in_top_10"),
                "n_etfs_in_top_20": c.get("n_etfs_in_top_20"),
                "n_etfs_holding": c.get("n_etfs_holding"),
                "aggregate_flow_5d_usd": c.get("aggregate_flow_5d_usd"),
                "aggregate_flow_21d_usd": c.get("aggregate_flow_21d_usd"),
                "perf_5d_pct": c.get("perf_5d_pct"),
                "perf_20d_pct": c.get("perf_20d_pct"),
                "hot_etf": c.get("hot_etf"),
                "industry": c.get("industry_label") or c.get("industry"),
                "position_sizing_pct": (c.get("position_sizing") or {}).get("final_pct"),
                "is_laggard": c.get("is_laggard", False),
                "tier": c.get("tier"),
            })

    # Velocity tiers
    for tier_key, tier_label in [
        ("confirmed_today", "VELOCITY_FIRED_CONFIRMED"),
        ("fresh_fires", "VELOCITY_FIRED_FRESH"),
        ("emerging", "VELOCITY_EMERGING"),
        ("watch", "VELOCITY_WATCH"),
    ]:
        for v in (velocity.get(tier_key) or []):
            t = v.get("ticker")
            if not t:
                continue
            predictions.setdefault(t, {"ticker": t, "snapshot_date": today,
                                        "alerts": [], "features": {}})
            predictions[t]["alerts"].append(tier_label)
            predictions[t]["features"].update({
                "velocity_composite": v.get("composite_score") or v.get("current_score"),
                "velocity_slope": v.get("slope_score"),
                "velocity_accum": v.get("accum_score"),
                "velocity_floor": v.get("floor_score"),
                "current_vol_ratio": v.get("current_vol_ratio"),
                "velocity_theme": v.get("theme_label") or v.get("theme"),
            })

    # Convergence radar
    for r in (radar.get("items") or radar.get("tickers") or radar.get("results") or []):
        if not isinstance(r, dict):
            continue
        t = r.get("ticker")
        if not t:
            continue
        predictions.setdefault(t, {"ticker": t, "snapshot_date": today,
                                    "alerts": [], "features": {}})
        predictions[t]["alerts"].append(f"CONVERGENCE_{r.get('tier', '?')}")
        predictions[t]["features"].update({
            "convergence_score": r.get("convergence_score"),
            "n_engines": r.get("n_engines"),
            "is_ultra_new": r.get("is_ultra_new", False),
            "pump_category": r.get("pump_category"),
        })

    # Early movers
    for c in (early.get("alert_tier") or [])[:25]:
        t = c.get("ticker")
        if not t:
            continue
        predictions.setdefault(t, {"ticker": t, "snapshot_date": today,
                                    "alerts": [], "features": {}})
        predictions[t]["alerts"].append("EARLY_MOVER_ALERT")
        predictions[t]["features"].update({
            "early_score": c.get("early_score"),
            "early_factors": c.get("factors"),
        })

    # Options flow
    for c in ((options.get("extreme_call_flow") or []) +
              (options.get("bullish_call_flow") or []))[:30]:
        t = c.get("ticker")
        if not t:
            continue
        predictions.setdefault(t, {"ticker": t, "snapshot_date": today,
                                    "alerts": [], "features": {}})
        alert_lvl = c.get("alert_level", 0)
        predictions[t]["alerts"].append(
            f"OPTIONS_{'EXTREME' if alert_lvl == 3 else 'BULLISH'}_CALL")
        predictions[t]["features"].update({
            "options_call_vol": c.get("call_vol"),
            "options_put_vol": c.get("put_vol"),
            "options_cv_pv_ratio": c.get("cv_pv_ratio"),
            "options_vol_oi_ratio": c.get("vol_oi_ratio"),
            "options_mean_iv": c.get("mean_iv"),
            "options_otm_call_vol": c.get("otm_call_vol"),
            "options_smart_money_blocks": c.get("n_smart_money_blocks"),
            "options_alert_level": alert_lvl,
            "options_signals": c.get("signals"),
        })

    # Insider clusters
    for ic in (insider.get("clusters") or insider.get("items") or [])[:30]:
        t = ic.get("ticker") if isinstance(ic, dict) else None
        if not t:
            continue
        predictions.setdefault(t, {"ticker": t, "snapshot_date": today,
                                    "alerts": [], "features": {}})
        predictions[t]["alerts"].append("INSIDER_CLUSTER")
        predictions[t]["features"].update({
            "insider_n_buyers": ic.get("n_insiders") or ic.get("cluster_size"),
            "insider_total_value_usd": ic.get("total_value_usd"),
        })

    # Activist filings
    for f in (activist.get("filings") or activist.get("items") or [])[:30]:
        t = f.get("ticker") or f.get("symbol") or f.get("issuer_ticker")
        if not t:
            continue
        predictions.setdefault(t, {"ticker": t, "snapshot_date": today,
                                    "alerts": [], "features": {}})
        predictions[t]["alerts"].append("ACTIVIST_13D")
        predictions[t]["features"].update({
            "activist_filer": (f.get("filer") or f.get("activist") or "")[:50],
            "activist_stake_pct": f.get("ownership_pct") or f.get("stake_pct"),
        })

    # Trade ticket levels (entry/stop/TPs) for outcome scoring
    ticket_map = {t.get("ticker"): t for t in (tickets.get("tickets") or [])}
    for ticker, pred in predictions.items():
        tt = ticket_map.get(ticker)
        if tt and not tt.get("error"):
            pred["features"].update({
                "ticket_entry": tt.get("entry"),
                "ticket_stop": tt.get("stop_loss"),
                "ticket_tp1": tt.get("tp1"),
                "ticket_tp2": tt.get("tp2"),
                "ticket_tp3": tt.get("tp3"),
                "ticket_risk_pct": tt.get("risk_pct"),
                "ticket_atr_pct": tt.get("atr_pct"),
                "ticket_rr_tp3": tt.get("rr_tp3"),
            })

    # Macro context (shared across all tickers)
    macro_context = {
        "fx_signals": fx.get("regime_signals") or [],
        "fx_usd_synth_20d": (fx.get("regime_metrics") or {}).get("usd_synthetic_20d_pct"),
        "futures_signals": futures.get("signals") or [],
    }

    elapsed = round(time.time() - t0, 1)
    print(f"[snapshotter] DONE — {len(predictions)} tickers captured with features in {elapsed}s")

    # Print alert distribution
    alert_counts = {}
    for p in predictions.values():
        for a in p["alerts"]:
            alert_counts[a] = alert_counts.get(a, 0) + 1
    print(f"[snapshotter] alert distribution: {alert_counts}")

    output = {
        "schema_version": "1.0",
        "snapshot_date": today,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "elapsed_s": elapsed,
        "n_tickers": len(predictions),
        "alert_distribution": alert_counts,
        "macro_context": macro_context,
        "predictions": list(predictions.values()),
    }

    # Write daily snapshot (date-keyed)
    s3.put_object(
        Bucket=S3_BUCKET, Key=f"data/predictions-snapshots/{today}.json",
        Body=json.dumps(output, default=str).encode(),
        ContentType="application/json", CacheControl="public, max-age=86400",
    )
    # Also write latest pointer
    s3.put_object(
        Bucket=S3_BUCKET, Key="data/predictions-snapshots/latest.json",
        Body=json.dumps(output, default=str).encode(),
        ContentType="application/json", CacheControl="public, max-age=600",
    )

    return {
        "statusCode": 200,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps({
            "ok": True, "elapsed_s": elapsed,
            "n_tickers": len(predictions),
            "alert_distribution": alert_counts,
            "snapshot_date": today,
        }),
    }
