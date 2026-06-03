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
    retail = _read_json("data/retail-sentiment.json") or {}
    news = _read_json("sentiment/data.json") or {}
    political = _read_json("data/political-intel.json") or {}
    earnings = _read_json("screener/earnings-sentiment.json") or {}
    gdelt = _read_json("data/gdelt-financial-sentiment.json") or {}

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

    # Retail sentiment enrichment — also classify high-velocity tickers as
    # RETAIL_VELOCITY tier (new). These are the HPQ +4600% style signals.
    velocity_surges = retail.get("biggest_velocity_surges") or []
    rank_climbers = retail.get("biggest_rank_climbers") or []
    
    # Build retail signal maps
    retail_signal_map = {}
    for s in velocity_surges:
        if isinstance(s, dict):
            t = s.get("ticker") or s.get("symbol")
            if t:
                retail_signal_map.setdefault(t, {})["retail_velocity_pct"] = s.get("velocity_pct")
                retail_signal_map[t]["retail_mentions"] = s.get("mentions") or s.get("current_count")
    for s in rank_climbers:
        if isinstance(s, dict):
            t = s.get("ticker") or s.get("symbol")
            if t:
                retail_signal_map.setdefault(t, {})["retail_rank_climb"] = s.get("rank_climb") or s.get("delta")
                retail_signal_map[t]["retail_current_rank"] = s.get("rank")
    
    # Add retail features to existing predictions + create NEW RETAIL_VELOCITY tickers
    for ticker, ret_signal in retail_signal_map.items():
        if ticker not in predictions:
            # Top retail signals not yet in cascade — track as standalone RETAIL_VELOCITY tickers
            vel = ret_signal.get("retail_velocity_pct", 0) or 0
            if vel >= 200:  # only track significant surges
                predictions[ticker] = {"ticker": ticker, "snapshot_date": today,
                                        "alerts": ["RETAIL_VELOCITY"], "features": {}}
        else:
            # Already tracked - just add retail features
            pass
        
        if ticker in predictions:
            predictions[ticker]["features"].update({
                "retail_velocity_pct": ret_signal.get("retail_velocity_pct"),
                "retail_mentions": ret_signal.get("retail_mentions"),
                "retail_rank_climb": ret_signal.get("retail_rank_climb"),
                "retail_current_rank": ret_signal.get("retail_current_rank"),
            })
            # Mark high-velocity tickers (>500%) with extra label
            vel = ret_signal.get("retail_velocity_pct", 0) or 0
            if vel >= 500 and "RETAIL_HOT" not in predictions[ticker]["alerts"]:
                predictions[ticker]["alerts"].append("RETAIL_HOT")

    # ═══ NEWS / EARNINGS / GDELT INTEGRATION ═══
    # 
    # News sentiment: bullish/bearish scoring per ticker from FMP headlines
    # Earnings transcripts: forward-looking guidance sentiment per ticker
    # GDELT: geopolitical event impact on tickers/themes
    
    # News sentiment map
    news_map = {}
    for s in (news.get("sentiment") or []):
        if isinstance(s, dict):
            sym = s.get("symbol") or s.get("ticker")
            if sym:
                news_map[sym] = {
                    "news_score": s.get("score"),
                    "news_signal": s.get("signal"),  # bullish/bearish/neutral
                    "news_reason": (s.get("reason") or "")[:200],
                }
    
    # Earnings transcript map
    earnings_map = {}
    for t in (earnings.get("transcripts") or []):
        if isinstance(t, dict):
            sym = t.get("symbol")
            if sym:
                earnings_map[sym] = {
                    "earnings_score": t.get("score") or t.get("sentiment_score"),
                    "earnings_signal": t.get("signal"),
                    "earnings_date": t.get("transcript_date") or t.get("earnings_date"),
                    "earnings_summary": (t.get("summary") or "")[:200],
                }
    
    # GDELT asset sentiment map
    gdelt_map = {}
    for asset in (gdelt.get("asset_sentiment") or []):
        if isinstance(asset, dict):
            sym = asset.get("ticker") or asset.get("symbol") or asset.get("asset")
            if sym:
                gdelt_map[sym] = {
                    "gdelt_tone": asset.get("avg_tone") or asset.get("tone"),
                    "gdelt_articles": asset.get("article_count") or asset.get("mentions"),
                }
    
    # Apply enrichment + new alert tiers
    for ticker, pred in list(predictions.items()):
        # News sentiment
        n = news_map.get(ticker)
        if n:
            pred["features"].update({
                "news_score": n.get("news_score"),
                "news_signal": n.get("news_signal"),
            })
            score = n.get("news_score")
            signal = n.get("news_signal", "")
            if isinstance(score, (int, float)) and score >= 0.7 and signal == "bullish":
                if "NEWS_SURGE_BULLISH" not in pred["alerts"]:
                    pred["alerts"].append("NEWS_SURGE_BULLISH")
        
        # Earnings sentiment
        e = earnings_map.get(ticker)
        if e:
            pred["features"].update({
                "earnings_score": e.get("earnings_score"),
                "earnings_signal": e.get("earnings_signal"),
            })
            # Flag if earnings transcript is recent (within 14 days)
            try:
                if e.get("earnings_date"):
                    edt = datetime.fromisoformat(str(e["earnings_date"])[:10])
                    days_since = (datetime.now(timezone.utc).replace(tzinfo=None) - edt.replace(tzinfo=None)).days
                    if 0 <= days_since <= 14:
                        pred["features"]["days_since_earnings"] = days_since
                        if "EARNINGS_FRESH" not in pred["alerts"]:
                            pred["alerts"].append("EARNINGS_FRESH")
            except Exception:
                pass
        
        # GDELT sentiment
        g = gdelt_map.get(ticker)
        if g:
            pred["features"].update({
                "gdelt_tone": g.get("gdelt_tone"),
                "gdelt_articles": g.get("gdelt_articles"),
            })
    
    # Create NEW news-only predictions (tickers with extreme news but not in cascade)
    for ticker, n in news_map.items():
        if ticker in predictions:
            continue
        score = n.get("news_score")
        signal = n.get("news_signal", "")
        if isinstance(score, (int, float)) and score >= 0.8 and signal == "bullish":
            predictions[ticker] = {
                "ticker": ticker,
                "snapshot_date": today,
                "alerts": ["NEWS_SURGE_BULLISH"],
                "features": {
                    "news_score": score,
                    "news_signal": signal,
                },
            }

    # ═══ POLITICIAN / CONGRESSIONAL TRADE ENRICHMENT ═══
    # Politician buys (esp. committee-relevant) are a heavily-weighted signal.
    # We feed conviction + committee-match as features so the self-improvement
    # loop LEARNS their real predictive weight, and create POLITICIAN_BUY tickets
    # for high-conviction names not already in the cascade.
    pol_by_ticker = political.get("by_ticker") or {}
    for ticker, prec in pol_by_ticker.items():
        conviction = prec.get("conviction_score") or 0
        n_buyers = prec.get("n_buyers") or 0
        committee = bool(prec.get("committee_relevant"))
        net_buy = (prec.get("n_buys") or 0) > (prec.get("n_sells") or 0)
        if not net_buy:
            continue
        # Standalone POLITICIAN_BUY ticket for strong conviction not in cascade
        if ticker not in predictions and conviction >= 40:
            predictions[ticker] = {"ticker": ticker, "snapshot_date": today,
                                    "alerts": ["POLITICIAN_BUY"], "features": {}}
        if ticker in predictions:
            predictions[ticker]["features"].update({
                "politician_conviction": round(conviction, 1),
                "politician_n_buyers": n_buyers,
                "politician_committee_relevant": 1 if committee else 0,
                "politician_cluster": 1 if prec.get("cluster") else 0,
            })
            # Label tiers
            if committee and "POLITICIAN_COMMITTEE" not in predictions[ticker]["alerts"]:
                predictions[ticker]["alerts"].append("POLITICIAN_COMMITTEE")
            elif conviction >= 40 and "POLITICIAN_BUY" not in predictions[ticker]["alerts"]:
                predictions[ticker]["alerts"].append("POLITICIAN_BUY")

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
