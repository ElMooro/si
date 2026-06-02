"""justhodl-theme-cascade — THE SYNTHESIS LAYER (v2 — correct schema parsing)

User insight: "stocks within a theme/industry thats already pumping are most
likely to pump especially if capital and money has rotated to that theme."

SCHEMA REALITY (validated 2026-06-02 via ops 1206):

theme-rotation.json:
  - all_themes (LIST of 114 ETF themes): ticker, category, momentum_score,
    rs_5d/20d/60d, rs_acceleration, rs_rank_20d, rs_rank_delta,
    vol_ratio_20v60, money_flow_ratio
  - summary.top_10_momentum, rotators_in, convergent_breadth
  - breadth_details[ETF].constituents_perf

velocity-acceleration.json: tickers have theme = industry name

stock-exposure-lookup.json: ticker → top_etfs list with weights/flows
"""
import json
import time
from datetime import datetime, timezone
from typing import Optional

import boto3

S3_BUCKET = "justhodl-dashboard-live"
s3 = boto3.client("s3", region_name="us-east-1")


def _read_json(key: str) -> Optional[dict]:
    try:
        return json.loads(s3.get_object(Bucket=S3_BUCKET, Key=key)["Body"].read())
    except Exception as e:
        print(f"[read] {key}: {e}")
        return None


def build_theme_heat_index(theme_rotation: dict) -> dict:
    """For each ETF, compute heat: {etf: {multiplier, factors, momentum_score, ...}}"""
    if not theme_rotation:
        return {}

    all_themes = theme_rotation.get("all_themes") or []
    summary = theme_rotation.get("summary") or {}
    breadth_details = theme_rotation.get("breadth_details") or {}

    top_10_tickers = {t.get("ticker") for t in (summary.get("top_10_momentum") or [])}
    rotators_in_tickers = {t.get("ticker") for t in (summary.get("rotators_in") or [])}
    convergent_tickers = {t.get("ticker") for t in (summary.get("convergent_breadth") or [])}

    heat = {}
    for t in all_themes:
        if not isinstance(t, dict):
            continue
        etf = t.get("ticker")
        if not etf:
            continue

        mom = t.get("momentum_score") or 0
        rs_accel = t.get("rs_acceleration") or 0
        rs_rank_20d = t.get("rs_rank_20d") or 999
        rs_rank_delta = t.get("rs_rank_delta") or 0
        vol_ratio = t.get("vol_ratio_20v60") or 0
        money_flow = t.get("money_flow_ratio") or 0
        category = t.get("category") or "UNKNOWN"
        name = t.get("name") or etf

        bd = breadth_details.get(etf, {}).get("breadth", {})
        breadth_pct = bd.get("breadth_outperform_pct") or 0
        breadth_above_ma50 = bd.get("breadth_above_ma50_pct") or 0

        mult = 1.0
        factors = []

        if mom >= 90:
            mult *= 1.6
            factors.append(f"top_momentum (m={mom})")
        elif mom >= 75:
            mult *= 1.4
            factors.append(f"high_momentum (m={mom})")
        elif mom >= 60:
            mult *= 1.2
            factors.append(f"good_momentum (m={mom})")

        if rs_rank_20d <= 5:
            mult *= 1.35
            factors.append(f"top_5_rs (#{rs_rank_20d})")
        elif rs_rank_20d <= 10:
            mult *= 1.20
            factors.append(f"top_10_rs (#{rs_rank_20d})")
        elif rs_rank_20d <= 20:
            mult *= 1.10
            factors.append(f"top_20_rs (#{rs_rank_20d})")

        if rs_accel >= 80:
            mult *= 1.35
            factors.append(f"strong_accel (a={rs_accel:.1f})")
        elif rs_accel >= 50:
            mult *= 1.20
            factors.append(f"positive_accel (a={rs_accel:.1f})")
        elif rs_accel >= 20:
            mult *= 1.08
            factors.append(f"mild_accel (a={rs_accel:.1f})")

        if rs_rank_delta >= 10:
            mult *= 1.15
            factors.append(f"rank_jumping (+{rs_rank_delta})")

        if breadth_pct >= 75:
            mult *= 1.20
            factors.append(f"high_breadth ({breadth_pct:.0f}%)")
        elif breadth_pct >= 60:
            mult *= 1.10
            factors.append(f"good_breadth ({breadth_pct:.0f}%)")

        if etf in top_10_tickers:
            mult *= 1.15
            factors.append("top_10_momentum")
        if etf in rotators_in_tickers:
            mult *= 1.20
            factors.append("rotator_in")
        if etf in convergent_tickers:
            mult *= 1.20
            factors.append("convergent_breadth")

        if money_flow >= 2.0:
            mult *= 1.10
            factors.append(f"strong_money_flow ({money_flow:.1f})")

        mult = min(mult, 3.0)

        heat[etf] = {
            "etf": etf, "name": name, "category": category,
            "multiplier": round(mult, 3), "factors": factors,
            "momentum_score": mom, "rs_acceleration": rs_accel,
            "rs_rank_20d": rs_rank_20d, "rs_rank_delta": rs_rank_delta,
            "breadth_outperform_pct": breadth_pct,
            "breadth_above_ma50_pct": breadth_above_ma50,
        }
    return heat


def build_ticker_to_etfs_map(theme_rotation: dict, exposure_lookup: dict) -> dict:
    """For each stock, find ETFs that hold it. Combines breadth_details + stock-exposure."""
    ticker_to_etfs = {}
    breadth_details = theme_rotation.get("breadth_details") or {}

    # Source 1: theme-rotation breadth details
    for etf, info in breadth_details.items():
        for c in (info.get("constituents_perf") or []):
            sym = c.get("symbol")
            if sym:
                if etf not in ticker_to_etfs.setdefault(sym, []):
                    ticker_to_etfs[sym].append(etf)

    # Source 2: stock-exposure-lookup (much broader coverage)
    if isinstance(exposure_lookup, dict):
        for ticker, info in exposure_lookup.items():
            if not isinstance(info, dict):
                continue
            top_etfs = info.get("top_etfs") or []
            for etf_info in top_etfs:
                if isinstance(etf_info, dict):
                    etf = etf_info.get("etf")
                    if etf:
                        if etf not in ticker_to_etfs.setdefault(ticker, []):
                            ticker_to_etfs[ticker].append(etf)

    return ticker_to_etfs


def compute_flow_multiplier(ticker: str, exposure_lookup: dict) -> dict:
    info = exposure_lookup.get(ticker) if isinstance(exposure_lookup, dict) else None
    if not info:
        return {"multiplier": 1.0, "factors": [], "n_etfs_holding": 0,
                "cumulative_weight_pct": 0, "aggregate_flow_5d_usd": 0,
                "aggregate_flow_21d_usd": 0}

    n_etfs = info.get("n_etfs_holding") or 0
    cum_weight = info.get("cumulative_weight_pct") or 0
    agg_5d = info.get("total_aggregate_flow_5d_usd") or 0
    agg_21d = info.get("total_aggregate_flow_21d_usd") or 0

    mult = 1.0
    factors = []

    if agg_5d > 100e6 and cum_weight > 20:
        mult *= 1.4
        factors.append(f"strong_etf_inflow_+${agg_5d/1e6:.0f}M")
    elif agg_5d > 25e6 and cum_weight > 10:
        mult *= 1.25
        factors.append(f"etf_inflow_+${agg_5d/1e6:.0f}M")
    elif agg_5d > 0 and n_etfs >= 5:
        mult *= 1.10
        factors.append(f"broad_exposure_{n_etfs}_etfs")
    elif agg_5d < -50e6:
        mult *= 0.85
        factors.append(f"etf_outflow_${agg_5d/1e6:.0f}M")

    if agg_21d > 200e6 and agg_5d > 0:
        mult *= 1.10
        factors.append("sustained_21d_inflow")
    elif agg_21d < -100e6 and agg_5d < 0:
        mult *= 0.90
        factors.append("sustained_21d_outflow")

    return {
        "multiplier": round(mult, 3), "factors": factors,
        "n_etfs_holding": n_etfs, "cumulative_weight_pct": cum_weight,
        "aggregate_flow_5d_usd": agg_5d, "aggregate_flow_21d_usd": agg_21d,
    }


def lambda_handler(event, context):
    t0 = time.time()
    print(f"[theme-cascade] starting at {datetime.now(timezone.utc).isoformat()}")

    velocity = _read_json("data/velocity-acceleration.json") or {}
    theme_rotation = _read_json("data/theme-rotation.json") or {}
    exposure_lookup = _read_json("etf-flows/stock-exposure-lookup.json") or {}
    themes_doc = _read_json("data/themes.json") or {}
    macro = _read_json("macro/regime.json") or {}
    print(f"[theme-cascade] loaded: velocity={bool(velocity)} "
          f"theme_rot={bool(theme_rotation)} exposure={bool(exposure_lookup)} "
          f"themes={bool(themes_doc)}")

    theme_heat = build_theme_heat_index(theme_rotation)
    print(f"[theme-cascade] theme heat: {len(theme_heat)} ETFs scored")

    ticker_to_etfs = build_ticker_to_etfs_map(theme_rotation, exposure_lookup)
    print(f"[theme-cascade] ticker→etfs: {len(ticker_to_etfs)} stocks mapped")

    sorted_themes = sorted(theme_heat.items(), key=lambda x: -x[1]["multiplier"])
    top_hot_etfs = sorted_themes[:15]
    print(f"[theme-cascade] TOP 5 HOTTEST THEMES:")
    for etf, h in top_hot_etfs[:5]:
        print(f"  {etf:6s} ({h.get('category'):14s}) x{h['multiplier']:.2f}  "
              f"m={h.get('momentum_score')}  rs#{h.get('rs_rank_20d')}  "
              f"a={h.get('rs_acceleration'):.0f}  {h['factors'][:3]}")

    combined = []
    sources = [
        ("FIRED_CONFIRMED", velocity.get("confirmed_today") or [], 80),
        ("FIRED_FRESH",     velocity.get("fresh_fires") or [], 70),
        ("AGING",           velocity.get("aging") or [], 60),
        ("EMERGING",        velocity.get("emerging") or [], 50),
        ("WATCH",           velocity.get("watch") or [], 35),
    ]

    seen_tickers = set()
    for tier_name, items, default_score in sources:
        for item in items:
            t = item.get("ticker")
            if not t or t in seen_tickers:
                continue
            seen_tickers.add(t)

            base_score = (item.get("composite_score") or item.get("current_score")
                          or default_score)
            industry = item.get("theme")
            industry_label = item.get("theme_label") or industry

            etfs_holding = ticker_to_etfs.get(t, [])
            heat_entries = [theme_heat[e] for e in etfs_holding if e in theme_heat]

            if heat_entries:
                best_heat = max(heat_entries, key=lambda x: x["multiplier"])
                theme_mult = best_heat["multiplier"]
                hot_etf = best_heat["etf"]
                hot_etf_name = best_heat["name"]
                hot_etf_category = best_heat["category"]
                theme_factors = best_heat["factors"]
                theme_mom = best_heat["momentum_score"]
                theme_rs_rank = best_heat["rs_rank_20d"]
                theme_acceleration = best_heat["rs_acceleration"]
            else:
                theme_mult = 1.0
                hot_etf = None
                hot_etf_name = None
                hot_etf_category = None
                theme_factors = []
                theme_mom = None
                theme_rs_rank = None
                theme_acceleration = None

            flow_info = compute_flow_multiplier(t, exposure_lookup)
            flow_mult = flow_info["multiplier"]

            combined_score = base_score * theme_mult * flow_mult

            combined.append({
                "ticker": t, "tier": tier_name,
                "base_score": round(base_score, 1),
                "theme_multiplier": theme_mult,
                "flow_multiplier": flow_mult,
                "combined_score": round(combined_score, 1),
                "industry": industry, "industry_label": industry_label,
                "hot_etf": hot_etf, "hot_etf_name": hot_etf_name,
                "hot_etf_category": hot_etf_category,
                "n_etfs_in_heat_index": len(heat_entries),
                "theme_factors": theme_factors,
                "theme_momentum": theme_mom,
                "theme_rs_rank": theme_rs_rank,
                "theme_acceleration": theme_acceleration,
                "flow_factors": flow_info["factors"],
                "n_etfs_holding": flow_info["n_etfs_holding"],
                "aggregate_flow_5d_usd": flow_info["aggregate_flow_5d_usd"],
                "aggregate_flow_21d_usd": flow_info["aggregate_flow_21d_usd"],
                "slope_score": item.get("slope_score"),
                "accum_score": item.get("accum_score"),
                "floor_score": item.get("floor_score"),
                "current_vol_ratio": item.get("current_vol_ratio"),
                "momentum_score": item.get("momentum_score"),
            })

    combined.sort(key=lambda x: -x["combined_score"])

    alert_tier = [c for c in combined if c["combined_score"] >= 80]
    medium_tier = [c for c in combined if 50 <= c["combined_score"] < 80]
    watch_tier = [c for c in combined if c["combined_score"] < 50]

    elapsed = round(time.time() - t0, 1)
    print(f"[theme-cascade] DONE — {len(combined)} ranked, "
          f"alert={len(alert_tier)} medium={len(medium_tier)} watch={len(watch_tier)} "
          f"in {elapsed}s")

    top_hot_themes_out = [
        {
            "etf": h["etf"], "name": h["name"], "category": h["category"],
            "multiplier": h["multiplier"], "factors": h["factors"],
            "momentum_score": h["momentum_score"],
            "rs_rank_20d": h["rs_rank_20d"],
            "rs_acceleration": h["rs_acceleration"],
            "breadth_outperform_pct": h["breadth_outperform_pct"],
        }
        for _, h in top_hot_etfs
    ]

    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    output = {
        "schema_version": "2.0",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "elapsed_s": elapsed,
        "macro_regime": (macro.get("top_level_regime") or {}).get("regime"),
        "n_themes_tracked": len(theme_heat),
        "n_tickers_mapped": len(ticker_to_etfs),
        "n_total_ranked": len(combined),
        "n_alert_tier": len(alert_tier),
        "n_medium_tier": len(medium_tier),
        "n_watch_tier": len(watch_tier),
        "top_hot_themes": top_hot_themes_out,
        "alert_tier": alert_tier[:25],
        "medium_tier": medium_tier[:30],
        "watch_tier": watch_tier[:30],
        "all_ranked": combined[:80],
    }

    s3.put_object(Bucket=S3_BUCKET, Key="data/theme-cascade.json",
                  Body=json.dumps(output, default=str).encode(),
                  ContentType="application/json", CacheControl="public, max-age=600")
    s3.put_object(Bucket=S3_BUCKET, Key=f"data/theme-cascade-history/{today}.json",
                  Body=json.dumps(output, default=str).encode(),
                  ContentType="application/json", CacheControl="public, max-age=86400")

    return {
        "statusCode": 200,
        "headers": {"Content-Type": "application/json",
                     "Access-Control-Allow-Origin": "*"},
        "body": json.dumps({
            "ok": True, "elapsed_s": elapsed, "n_total": len(combined),
            "n_alert_tier": len(alert_tier),
            "n_medium_tier": len(medium_tier),
            "n_themes_tracked": len(theme_heat),
            "top_5_hot_themes": [
                {"etf": t["etf"], "name": t["name"], "category": t["category"],
                 "mult": t["multiplier"], "momentum": t["momentum_score"],
                 "rs_rank": t["rs_rank_20d"], "rs_accel": t["rs_acceleration"]}
                for t in top_hot_themes_out[:5]
            ],
            "top_10_combined": [
                {"ticker": c["ticker"], "tier": c["tier"],
                 "combined_score": c["combined_score"], "base": c["base_score"],
                 "theme_mult": c["theme_multiplier"],
                 "flow_mult": c["flow_multiplier"],
                 "hot_etf": c["hot_etf"], "industry": c["industry"]}
                for c in combined[:10]
            ],
        }),
    }
