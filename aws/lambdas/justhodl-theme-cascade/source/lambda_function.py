"""justhodl-theme-cascade — THE SYNTHESIS LAYER

User insight (2026-06-02): "stocks within a theme/industry that's already
pumping are most likely to pump next, especially if capital and money has
rotated to that theme."

This is the theme rotation thesis: when capital flows INTO a theme, the
LEADERS pump first (NVDA/AVGO/MRVL in AI semis), and then LAGGARDS pump
in sympathy. Catching laggards in hot themes = the best pre-pump bet.

This Lambda synthesizes 3 existing data sources:

  1. THEME HEAT — from theme-rotation.json (RS rank, breadth, velocity, money flow)
     → which themes are HOT (institutional rotation IN)

  2. ACCUMULATION SIGNAL — from velocity-acceleration.json (WATCH/EMERGING/FIRED)
     → which tickers are accumulating at the right moment

  3. CAPITAL FLOW — from etf-flows/constituent-pressure.json
     → which tickers have ETF channels pushing $$ in

COMBINED SCORE per ticker:
  base_score    = velocity composite_score (0-100)
  theme_mult    = function of theme RS rank, breadth, velocity (1.0-2.5x)
  flow_mult     = function of cross-ETF aggregate flow magnitude (1.0-1.5x)
  combined      = base_score × theme_mult × flow_mult

OUTPUT:
  data/theme-cascade.json — ranked list with theme + accumulation + flow

ALERT TIER: combined >= 80 = HIGH-CONVICTION PRE-PUMP CANDIDATE
  (a ticker with WATCH-tier accumulation in a TOP-3 theme with cross-ETF buying
  pressure satisfies this — that's the MRVL pattern caught EARLY)
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


# ═════════════════════════════════════════════════════════════════════
# THEME HEAT — extracts hot themes from theme-rotation.json
# ═════════════════════════════════════════════════════════════════════
def compute_theme_heat(theme_rotation: dict) -> dict:
    """For each theme, return a heat dict with multiplier and breakdown.

    The Lambda tries multiple possible field names because theme-rotation-engine
    has many output fields. Best-effort, defensive parsing.
    """
    if not theme_rotation:
        return {}

    # Try standard locations for themes
    themes_raw = (theme_rotation.get("themes") or theme_rotation.get("rotation")
                  or theme_rotation.get("rankings") or {})

    if isinstance(themes_raw, list):
        # list of {theme, rs_rank, breadth, velocity, ...}
        themes_dict = {t.get("theme") or t.get("name") or t.get("label"): t
                       for t in themes_raw if isinstance(t, dict)}
    elif isinstance(themes_raw, dict):
        themes_dict = themes_raw
    else:
        return {}

    # Build heat scores
    heat = {}
    for theme_name, t in themes_dict.items():
        if not isinstance(t, dict):
            continue
        # Try multiple possible field names
        rs_rank = (t.get("rs_rank") or t.get("rank") or t.get("rs_rank_today"))
        n_themes = len(themes_dict)
        breadth = (t.get("breadth") or t.get("breadth_pct") or t.get("pct_above_50dma")
                   or t.get("constituents_participating"))
        velocity = (t.get("velocity") or t.get("rs_velocity") or t.get("rs_acceleration")
                    or t.get("acceleration"))
        money_flow = (t.get("money_flow") or t.get("net_flow") or t.get("flow_score")
                      or t.get("volume_score"))
        # Status / regime
        regime = (t.get("status") or t.get("regime") or t.get("classification"))

        # Compute multiplier — start at 1.0, add for each hot signal
        mult = 1.0
        factors = []

        # Factor 1: RS rank — top 5 themes get strong boost
        if rs_rank is not None:
            try:
                r = int(rs_rank)
                if r <= 5:
                    mult *= 1.5
                    factors.append(f"top_5_rs (rank {r})")
                elif r <= 10:
                    mult *= 1.25
                    factors.append(f"top_10_rs (rank {r})")
                elif r <= 20:
                    mult *= 1.1
                    factors.append(f"top_20_rs (rank {r})")
            except Exception:
                pass

        # Factor 2: Breadth — 60%+ = institutional participation
        if breadth is not None:
            try:
                b = float(breadth)
                # Some implementations use 0-1, others use 0-100
                if b > 1.5:  # likely a percentage
                    if b > 60:
                        mult *= 1.2
                        factors.append(f"high_breadth ({b:.0f}%)")
                    elif b > 40:
                        mult *= 1.05
                        factors.append(f"medium_breadth ({b:.0f}%)")
                else:  # likely a fraction 0-1
                    if b > 0.6:
                        mult *= 1.2
                        factors.append(f"high_breadth ({b*100:.0f}%)")
                    elif b > 0.4:
                        mult *= 1.05
                        factors.append(f"medium_breadth ({b*100:.0f}%)")
            except Exception:
                pass

        # Factor 3: Velocity — accelerating themes (best signal for laggard pumps)
        if velocity is not None:
            try:
                v = float(velocity)
                if v > 0.5:
                    mult *= 1.3
                    factors.append(f"strong_velocity (v={v:.2f})")
                elif v > 0.1:
                    mult *= 1.1
                    factors.append(f"positive_velocity (v={v:.2f})")
            except Exception:
                pass

        # Factor 4: Regime / status — institutional convergence
        if regime in ("CONVERGENT", "ROTATING_IN", "STRONG", "ACCELERATING"):
            mult *= 1.15
            factors.append(f"regime_{regime}")

        # Cap at 2.5x
        mult = min(mult, 2.5)

        heat[theme_name] = {
            "multiplier": round(mult, 3),
            "factors": factors,
            "rs_rank": rs_rank,
            "breadth": breadth,
            "velocity": velocity,
            "money_flow": money_flow,
            "regime": regime,
            "raw": t,
        }
    return heat


# ═════════════════════════════════════════════════════════════════════
# FLOW MULTIPLIER — from constituent-pressure / stock-exposure-lookup
# ═════════════════════════════════════════════════════════════════════
def compute_flow_multiplier(ticker: str, exposure_lookup: dict) -> dict:
    """Returns multiplier 1.0-1.5x based on cross-ETF $$ flow exposure."""
    info = exposure_lookup.get(ticker) if exposure_lookup else None
    if not info:
        return {"multiplier": 1.0, "factors": []}

    n_etfs = info.get("n_etfs_holding") or 0
    cum_weight = info.get("cumulative_weight_pct") or 0
    agg_5d = info.get("total_aggregate_flow_5d_usd") or 0

    mult = 1.0
    factors = []

    # Positive cross-ETF flow + meaningful exposure
    if agg_5d > 100e6 and cum_weight > 20:
        mult *= 1.35
        factors.append(f"strong_etf_inflow_${agg_5d/1e6:.0f}M_{cum_weight:.0f}%wt")
    elif agg_5d > 25e6 and cum_weight > 10:
        mult *= 1.2
        factors.append(f"etf_inflow_${agg_5d/1e6:.0f}M_{cum_weight:.0f}%wt")
    elif agg_5d > 0 and n_etfs >= 5:
        mult *= 1.05
        factors.append(f"broad_etf_exposure_{n_etfs}etfs")
    # Negative flow penalty
    elif agg_5d < -50e6:
        mult *= 0.85
        factors.append(f"etf_outflow_${agg_5d/1e6:.0f}M")

    return {
        "multiplier": round(mult, 3),
        "factors": factors,
        "n_etfs_holding": n_etfs,
        "cumulative_weight_pct": cum_weight,
        "aggregate_flow_5d_usd": agg_5d,
    }


# ═════════════════════════════════════════════════════════════════════
# Main: combine velocity tiers × theme heat × flow multiplier
# ═════════════════════════════════════════════════════════════════════
def lambda_handler(event, context):
    t0 = time.time()
    print(f"[theme-cascade] starting at {datetime.now(timezone.utc).isoformat()}")

    # Load data sources
    velocity = _read_json("data/velocity-acceleration.json") or {}
    theme_rotation = _read_json("data/theme-rotation.json") or {}
    exposure_lookup = _read_json("etf-flows/stock-exposure-lookup.json") or {}
    themes_doc = _read_json("data/themes.json") or {}
    macro = _read_json("macro/regime.json") or {}

    # Compute per-theme heat
    theme_heat = compute_theme_heat(theme_rotation)
    print(f"[theme-cascade] computed heat for {len(theme_heat)} themes")

    # Show top 5 hottest themes
    sorted_themes = sorted(
        theme_heat.items(),
        key=lambda x: -x[1]["multiplier"],
    )[:10]
    print(f"[theme-cascade] TOP HOT THEMES:")
    for name, h in sorted_themes[:5]:
        print(f"  {name}: x{h['multiplier']} ({', '.join(h['factors'])})")

    # Build ticker → theme map (from themes.json or velocity-acceleration universe)
    ticker_to_theme = {}
    # First try themes.json
    themes_data = themes_doc.get("themes") or {}
    if isinstance(themes_data, dict):
        for theme_name, t_info in themes_data.items():
            if isinstance(t_info, dict):
                tickers = (t_info.get("tickers") or t_info.get("constituents") or [])
                label = t_info.get("label") or theme_name
                for tk in tickers:
                    ticker_to_theme[tk] = {"theme": theme_name, "label": label}

    # Also: velocity-acceleration may have theme info embedded
    for tier_name in ["fresh_fires", "confirmed_today", "aging",
                       "emerging", "watch"]:
        for item in (velocity.get(tier_name) or []):
            t = item.get("ticker")
            theme = item.get("theme") or item.get("theme_label")
            if t and theme and t not in ticker_to_theme:
                ticker_to_theme[t] = {"theme": theme, "label": theme}

    # ── BUILD COMBINED RANKED LIST ────────────────────────────────────
    combined = []

    # Collect all tickers across all tiers with their base scores
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
            theme_info = ticker_to_theme.get(t, {})
            theme_name = theme_info.get("theme") or item.get("theme")
            theme_label = theme_info.get("label") or item.get("theme_label")
            heat = theme_heat.get(theme_name) or theme_heat.get(theme_label) or {}
            theme_mult = heat.get("multiplier", 1.0)

            flow_info = compute_flow_multiplier(t, exposure_lookup)
            flow_mult = flow_info.get("multiplier", 1.0)

            combined_score = base_score * theme_mult * flow_mult

            combined.append({
                "ticker": t,
                "tier": tier_name,
                "base_score": round(base_score, 1),
                "theme_multiplier": theme_mult,
                "flow_multiplier": flow_mult,
                "combined_score": round(combined_score, 1),
                "theme": theme_name,
                "theme_label": theme_label,
                "theme_factors": heat.get("factors", []),
                "theme_rs_rank": heat.get("rs_rank"),
                "theme_breadth": heat.get("breadth"),
                "theme_velocity": heat.get("velocity"),
                "flow_factors": flow_info.get("factors", []),
                "n_etfs_holding": flow_info.get("n_etfs_holding"),
                "aggregate_flow_5d_usd": flow_info.get("aggregate_flow_5d_usd"),
                "slope_score": item.get("slope_score"),
                "accum_score": item.get("accum_score"),
                "floor_score": item.get("floor_score"),
                "current_vol_ratio": item.get("current_vol_ratio") or item.get("vol_ratio_now"),
                "momentum_score": item.get("momentum_score"),
            })

    # Sort by combined_score desc
    combined.sort(key=lambda x: -x["combined_score"])

    # High-conviction pre-pump tier: combined >= 80
    alert_tier = [c for c in combined if c["combined_score"] >= 80]
    medium_tier = [c for c in combined if 50 <= c["combined_score"] < 80]
    watch_tier = [c for c in combined if c["combined_score"] < 50]

    elapsed = round(time.time() - t0, 1)
    print(f"[theme-cascade] DONE — {len(combined)} ranked, "
          f"{len(alert_tier)} alert-tier (>=80), "
          f"{len(medium_tier)} medium (50-79), "
          f"{len(watch_tier)} watch (<50) in {elapsed}s")

    # Top hot themes for the dashboard
    top_hot_themes = [
        {
            "theme": name,
            "label": h.get("raw", {}).get("label") or name,
            "multiplier": h["multiplier"],
            "factors": h["factors"],
            "rs_rank": h.get("rs_rank"),
            "breadth": h.get("breadth"),
            "velocity": h.get("velocity"),
            "regime": h.get("regime"),
        }
        for name, h in sorted_themes
    ]

    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    output = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "elapsed_s": elapsed,
        "macro_regime": (macro.get("top_level_regime") or {}).get("regime"),
        "n_themes_tracked": len(theme_heat),
        "n_total_ranked": len(combined),
        "n_alert_tier": len(alert_tier),
        "n_medium_tier": len(medium_tier),
        "n_watch_tier": len(watch_tier),
        "top_hot_themes": top_hot_themes,
        "alert_tier": alert_tier[:25],
        "medium_tier": medium_tier[:30],
        "watch_tier": watch_tier[:30],
        "all_ranked": combined[:80],
    }

    s3.put_object(
        Bucket=S3_BUCKET, Key="data/theme-cascade.json",
        Body=json.dumps(output, default=str).encode(),
        ContentType="application/json", CacheControl="public, max-age=600",
    )
    s3.put_object(
        Bucket=S3_BUCKET, Key=f"data/theme-cascade-history/{today}.json",
        Body=json.dumps(output, default=str).encode(),
        ContentType="application/json", CacheControl="public, max-age=86400",
    )

    return {
        "statusCode": 200,
        "headers": {"Content-Type": "application/json",
                     "Access-Control-Allow-Origin": "*"},
        "body": json.dumps({
            "ok": True,
            "elapsed_s": elapsed,
            "n_total": len(combined),
            "n_alert_tier": len(alert_tier),
            "n_medium_tier": len(medium_tier),
            "top_5_hot_themes": [
                {"theme": t["theme"], "mult": t["multiplier"],
                 "rs_rank": t["rs_rank"]}
                for t in top_hot_themes[:5]
            ],
            "top_10_combined": [
                {
                    "ticker": c["ticker"],
                    "tier": c["tier"],
                    "combined_score": c["combined_score"],
                    "theme_mult": c["theme_multiplier"],
                    "flow_mult": c["flow_multiplier"],
                    "theme": c.get("theme_label"),
                }
                for c in combined[:10]
            ],
        }),
    }
