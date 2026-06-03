"""justhodl-trade-tickets

Converts every cascade alert into a COMPLETE TRADE TICKET:
  Entry / Stop / TP1 / TP2 / TP3 / R:R / Max loss

═══ ATR-BASED STOPS ═══
Stop = Entry - (multiplier × ATR_20)
  Alert tier:    1.5× ATR (high conviction → tighter stop)
  Laggards:      2.0× ATR (recent pullback → wider stop)
  Watch tier:    2.5× ATR (lower conviction → wider stop)

═══ R-MULTIPLE TAKE-PROFITS (theme-adjusted) ═══
Hot theme (rs_accel ≥ 100):     TP1=+1R · TP2=+2R · TP3=+5R
Strong theme (rs_accel 50-99):  TP1=+1R · TP2=+2R · TP3=+3R
Mild theme (rs_accel 20-49):    TP1=+1R · TP2=+1.5R · TP3=+2R
Weak theme (<20):                TP1=+0.5R · TP2=+1R · TP3=+1.5R

Where 1R = entry_price - stop_loss = risk per share

═══ POSITION SIZING (from cascade) ═══
Final_$ = portfolio_value × position_pct / 100
Shares = floor(Final_$ / entry)
Max_loss = shares × (entry - stop) = position_pct × (atr_mult × atr/entry × 100)

OUTPUT: data/trade-tickets.json
"""
import json
import os
import time
import urllib.request
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor
from typing import Optional, List

import boto3

S3_BUCKET = "justhodl-dashboard-live"
POLYGON_KEY = os.environ.get("POLYGON_KEY", "zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d")
DEFAULT_PORTFOLIO_USD = float(os.environ.get("PORTFOLIO_USD", "100000"))  # for ticket sizing display
N_WORKERS = 8

s3 = boto3.client("s3", region_name="us-east-1")


def _read_json(key: str) -> Optional[dict]:
    try:
        return json.loads(s3.get_object(Bucket=S3_BUCKET, Key=key)["Body"].read())
    except Exception:
        return None

def get_active_cascade() -> dict:
    """Return calibrated cascade if confidence >= MEDIUM, else original.

    Reads cascade-recalibration-audit.json to check calibration confidence.
    Once self-improvement has scored 20+ predictions, the system blends in
    learned weights. This consumer auto-switches without code changes.
    """
    try:
        audit = _read_json("data/cascade-recalibration-audit.json") or {}
        confidence = (audit.get("blend") or {}).get("confidence", "NONE")
        if confidence in ("MEDIUM", "HIGH"):
            cal = _read_json("data/theme-cascade-calibrated.json")
            if cal:
                print(f"[adaptive-cascade] using CALIBRATED cascade (confidence={confidence})")
                return cal
        print(f"[adaptive-cascade] using ORIGINAL cascade (confidence={confidence})")
    except Exception as e:
        print(f"[adaptive-cascade] err {e} — falling back to original")
    return _read_json("data/theme-cascade.json") or {}




def fetch_polygon_ohlc(ticker: str, days: int = 25) -> List[dict]:
    """Fetch daily OHLC via Polygon Stocks Starter."""
    to_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    from_date = (datetime.now(timezone.utc) - timedelta(days=days * 2)).strftime("%Y-%m-%d")
    url = (f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/day/"
           f"{from_date}/{to_date}?adjusted=true&apiKey={POLYGON_KEY}")
    try:
        with urllib.request.urlopen(url, timeout=12) as r:
            data = json.loads(r.read().decode())
        return data.get("results") or []
    except Exception as e:
        print(f"[polygon] {ticker}: {e}")
        return []


def compute_atr_20(bars: List[dict]) -> Optional[float]:
    """Compute 20-day Average True Range."""
    if len(bars) < 21:
        return None
    bars = sorted(bars, key=lambda b: b.get("t", 0))
    trs = []
    for i in range(-20, 0):
        if abs(i) >= len(bars):
            return None
        cur = bars[i]
        prev = bars[i - 1] if abs(i - 1) <= len(bars) else cur
        h = cur.get("h")
        l = cur.get("l")
        prev_close = prev.get("c")
        if h is None or l is None or prev_close is None:
            continue
        tr = max(h - l, abs(h - prev_close), abs(l - prev_close))
        trs.append(tr)
    if not trs:
        return None
    return sum(trs) / len(trs)


def determine_atr_multiplier(tier: str, is_laggard: bool) -> float:
    if is_laggard:
        return 2.0
    if tier in ("ALERT_TIER", "FIRED_CONFIRMED", "FIRED_FRESH"):
        return 1.5
    if tier in ("MEDIUM", "EMERGING"):
        return 1.8
    if tier in ("WATCH", "WATCH_TIER"):
        return 2.5
    return 2.0


def determine_tp_multiples(theme_accel: float) -> List[float]:
    """Returns [TP1_R, TP2_R, TP3_R]."""
    if theme_accel >= 100:
        return [1.0, 2.0, 5.0]
    if theme_accel >= 50:
        return [1.0, 2.0, 3.0]
    if theme_accel >= 20:
        return [1.0, 1.5, 2.0]
    return [0.5, 1.0, 1.5]


# ═══ HORIZON-AWARE TICKET LOGIC ════════════════════════════════════════════════

# Default holding period per cascade tier (used until calibration matures)
# These get OVERRIDDEN by empirical best_horizon_per_feature once data accumulates
DEFAULT_TIER_HORIZON = {
    "ALERT_TIER":       {"days": 10, "regime": "swing", "atr_mult_adj": 1.0},
    "FIRED_CONFIRMED":  {"days":  5, "regime": "fast_swing", "atr_mult_adj": 0.85},
    "FIRED_FRESH":      {"days":  5, "regime": "fast_swing", "atr_mult_adj": 0.85},
    "OPTIONS_EXTREME":  {"days":  2, "regime": "intraday_to_2d", "atr_mult_adj": 0.7},
    "OPTIONS_BULLISH":  {"days":  4, "regime": "fast_swing", "atr_mult_adj": 0.85},
    "VELOCITY_FIRED":   {"days":  5, "regime": "fast_swing", "atr_mult_adj": 0.9},
    "INSIDER_CLUSTER":  {"days": 25, "regime": "position", "atr_mult_adj": 1.5},
    "ACTIVIST":         {"days": 45, "regime": "investment", "atr_mult_adj": 2.0},
    "CONVERGENCE":      {"days": 12, "regime": "swing", "atr_mult_adj": 1.0},
    "EARLY_MOVER":      {"days":  8, "regime": "swing", "atr_mult_adj": 0.95},
    "MEDIUM":           {"days": 10, "regime": "swing", "atr_mult_adj": 1.0},
    "LAGGARD":          {"days": 18, "regime": "position", "atr_mult_adj": 1.3},
    "WATCH":            {"days": 15, "regime": "swing", "atr_mult_adj": 1.1},
}


def classify_candidate_setup(candidate: dict) -> str:
    """Classify a candidate into its primary setup type for horizon assignment.
    
    Reads the alerts array if present. Falls back to tier field.
    """
    alerts = candidate.get("alerts") or []
    alerts_set = set(alerts)

    # Priority order: most specific signal wins
    if "OPTIONS_EXTREME_CALL" in alerts_set:
        return "OPTIONS_EXTREME"
    if "OPTIONS_BULLISH_CALL" in alerts_set:
        return "OPTIONS_BULLISH"
    if any(a.startswith("VELOCITY_FIRED") for a in alerts_set):
        return "VELOCITY_FIRED"
    if "INSIDER_CLUSTER" in alerts_set:
        return "INSIDER_CLUSTER"
    if "ACTIVIST_13D" in alerts_set:
        return "ACTIVIST"
    if any(a.startswith("CONVERGENCE_") for a in alerts_set):
        return "CONVERGENCE"
    if "EARLY_MOVER_ALERT" in alerts_set:
        return "EARLY_MOVER"
    if candidate.get("is_laggard") or candidate.get("tier") == "LAGGARD":
        return "LAGGARD"

    # Fall back to tier field
    tier = candidate.get("tier") or candidate.get("entry_tier") or "ALERT_TIER"
    return tier.upper()


def get_horizon_attribution() -> dict:
    """Read multi-horizon calibration data — best_horizon_per_feature lookup."""
    try:
        cal = _read_json("data/cascade-calibration.json") or {}
        ha = cal.get("horizon_attribution") or {}
        if ha.get("insufficient_data"):
            return {}
        return ha.get("best_horizon_per_feature") or {}
    except Exception:
        return {}


def determine_horizon(candidate: dict, setup_type: str,
                       best_horizons: dict) -> dict:
    """Determine the optimal holding period for this candidate.

    Priority 1 (when calibration mature):
      Look at the candidate's strongest feature, use its empirical best_horizon
    
    Priority 2 (default):
      Use setup_type → DEFAULT_TIER_HORIZON mapping
    
    Returns dict with: days, regime, atr_mult_adj, source (default vs learned)
    """
    default = DEFAULT_TIER_HORIZON.get(setup_type) or DEFAULT_TIER_HORIZON.get("ALERT_TIER")
    
    # Try to find empirical best horizon if calibration has matured
    if best_horizons:
        # Look at this candidate's top features
        candidate_features = {
            "options_cv_pv_ratio": candidate.get("options_cv_pv_ratio") or 
                                    candidate.get("cv_pv_ratio"),
            "options_smart_money_blocks": candidate.get("options_smart_money_blocks") or
                                            candidate.get("n_smart_money_blocks"),
            "velocity_composite": candidate.get("velocity_composite") or
                                   candidate.get("composite_score"),
            "theme_acceleration": candidate.get("theme_acceleration") or
                                   candidate.get("max_rs_acceleration"),
            "insider_n_buyers": candidate.get("insider_n_buyers"),
            "aggregate_flow_5d_usd": candidate.get("aggregate_flow_5d_usd"),
            "n_etfs_in_top_10": candidate.get("n_etfs_in_top_10"),
            "convergence_score": candidate.get("convergence_score"),
        }
        # Find the feature with highest value AND known best_horizon
        active_feats = [(f, v) for f, v in candidate_features.items()
                        if v is not None and v > 0 and f in best_horizons]
        if active_feats:
            # Use the feature with strongest signal (highest value relative)
            # Take the best_horizon of the top-3 active features (weighted)
            horizons_seen = [best_horizons[f].get("best_horizon", "7d")
                              for f, v in active_feats[:3]]
            # Convert "1d" → 1, "30d" → 30
            day_vals = [int(h.replace("d", "")) for h in horizons_seen if h]
            if day_vals:
                avg_days = sum(day_vals) // len(day_vals)
                # Determine regime based on horizon
                if avg_days <= 2:
                    regime = "intraday_to_2d"
                elif avg_days <= 5:
                    regime = "fast_swing"
                elif avg_days <= 14:
                    regime = "swing"
                elif avg_days <= 30:
                    regime = "position"
                else:
                    regime = "investment"
                return {
                    "days": avg_days,
                    "regime": regime,
                    "atr_mult_adj": _atr_mult_for_horizon(avg_days),
                    "source": "learned",
                    "best_horizons_features": horizons_seen,
                }

    return {
        "days": default["days"],
        "regime": default["regime"],
        "atr_mult_adj": default["atr_mult_adj"],
        "source": "default",
    }


def _atr_mult_for_horizon(days: int) -> float:
    """Map holding period to ATR multiplier adjustment.
    
    Shorter horizons need tighter stops; longer horizons need wider stops.
    """
    if days <= 2:
        return 0.7
    if days <= 5:
        return 0.85
    if days <= 10:
        return 1.0
    if days <= 21:
        return 1.3
    return 1.6


def adjust_tp_for_horizon(base_multiples: List[float], horizon_days: int) -> List[float]:
    """Adjust TP multiples based on expected horizon.
    
    Shorter horizons → smaller R-multiples (tight, quick profits).
    Longer horizons → larger R-multiples (let it ride).
    """
    if horizon_days <= 2:
        # Intraday → 2d: smaller TPs, quick exits
        return [max(0.5, m * 0.7) for m in base_multiples]
    if horizon_days <= 5:
        # Fast swing: slightly smaller TPs
        return [m * 0.85 for m in base_multiples]
    if horizon_days <= 14:
        # Standard swing: use as-is
        return base_multiples
    if horizon_days <= 30:
        # Position: larger TPs
        return [m * 1.3 for m in base_multiples]
    # Investment: largest TPs
    return [m * 1.6 for m in base_multiples]


def build_ticket(candidate: dict, bars: List[dict],
                 portfolio_usd: float, best_horizons: dict = None) -> dict:
    """Build a complete trade ticket from cascade candidate + price bars.
    
    Now horizon-aware: uses learned best_horizon_per_feature (when calibration
    matures) to size stops/TPs to the expected holding period.
    """
    if best_horizons is None:
        best_horizons = {}
    ticker = candidate.get("ticker")
    if not bars:
        return {"ticker": ticker, "error": "no_polygon_data"}

    bars = sorted(bars, key=lambda b: b.get("t", 0))
    last_bar = bars[-1]
    entry = last_bar.get("c")
    if not entry or entry <= 0:
        return {"ticker": ticker, "error": "no_valid_price"}

    atr = compute_atr_20(bars)
    if not atr or atr <= 0:
        return {"ticker": ticker, "error": "no_atr"}

    # Determine setup type for horizon classification
    setup_type = classify_candidate_setup(candidate)
    horizon = determine_horizon(candidate, setup_type, best_horizons)

    # Determine tier & base multipliers (existing logic)
    tier = candidate.get("tier") or "UNKNOWN"
    is_laggard = candidate.get("is_laggard") or tier == "LAGGARD"
    base_atr_mult = determine_atr_multiplier(tier, is_laggard)
    theme_accel = (candidate.get("theme_acceleration") or
                   candidate.get("max_rs_acceleration") or 0)
    base_tp_multiples = determine_tp_multiples(theme_accel)

    # Apply horizon adjustment
    atr_mult = base_atr_mult * horizon["atr_mult_adj"]
    tp_multiples = adjust_tp_for_horizon(base_tp_multiples, horizon["days"])

    # Stop loss (horizon-adjusted)
    stop_loss = entry - (atr_mult * atr)
    risk_per_share = entry - stop_loss
    risk_pct = (risk_per_share / entry) * 100

    # Take profits (R-multiple, horizon-adjusted)
    tp1 = entry + (tp_multiples[0] * risk_per_share)
    tp2 = entry + (tp_multiples[1] * risk_per_share)
    tp3 = entry + (tp_multiples[2] * risk_per_share)

    # Position sizing (from cascade)
    position_pct = (candidate.get("position_sizing") or {}).get("final_pct") or 0
    position_usd = portfolio_usd * position_pct / 100
    shares = int(position_usd / entry) if entry > 0 else 0
    actual_position_usd = shares * entry

    # Max loss
    max_loss_usd = shares * risk_per_share
    max_loss_pct_of_portfolio = (max_loss_usd / portfolio_usd) * 100 if portfolio_usd else 0

    # R:R ratios
    rr_tp1 = tp_multiples[0]
    rr_tp2 = tp_multiples[1]
    rr_tp3 = tp_multiples[2]

    # Composite score: combined_score + R:R adjustment
    combined_score = candidate.get("combined_score") or 0
    rr_quality = rr_tp3 if rr_tp3 >= 3 else rr_tp3 * 0.7

    return {
        "ticker": ticker,
        "tier": tier,
        "is_laggard": is_laggard,
        "industry": candidate.get("industry_label") or candidate.get("industry"),
        "hot_etf": candidate.get("hot_etf"),
        "theme_acceleration": theme_accel,
        "combined_score": combined_score,

        # Horizon awareness (NEW)
        "setup_type": setup_type,
        "expected_horizon_days": horizon["days"],
        "horizon_regime": horizon["regime"],
        "horizon_source": horizon["source"],
        "atr_mult_horizon_adj": horizon["atr_mult_adj"],

        # Price + risk
        "entry": round(entry, 2),
        "atr_20": round(atr, 3),
        "atr_pct": round((atr / entry) * 100, 2),
        "atr_multiplier": atr_mult,
        "stop_loss": round(stop_loss, 2),
        "risk_per_share": round(risk_per_share, 3),
        "risk_pct": round(risk_pct, 2),

        # Take profits (R-multiples)
        "tp1": round(tp1, 2),
        "tp1_pct": round((tp1 - entry) / entry * 100, 2),
        "tp2": round(tp2, 2),
        "tp2_pct": round((tp2 - entry) / entry * 100, 2),
        "tp3": round(tp3, 2),
        "tp3_pct": round((tp3 - entry) / entry * 100, 2),
        "tp_multiples": tp_multiples,

        # R:R quality
        "rr_tp1": rr_tp1,
        "rr_tp2": rr_tp2,
        "rr_tp3": rr_tp3,
        "rr_quality_score": round(rr_quality, 1),

        # Position sizing (assumed portfolio_usd from env)
        "position_pct_of_portfolio": position_pct,
        "position_usd": round(actual_position_usd, 0),
        "shares": shares,
        "max_loss_usd": round(max_loss_usd, 0),
        "max_loss_pct_of_portfolio": round(max_loss_pct_of_portfolio, 2),

        # Scenarios
        "scenario_tp1": f"+${round((tp1-entry)*shares, 0):,.0f}",
        "scenario_tp2": f"+${round((tp2-entry)*shares, 0):,.0f}",
        "scenario_tp3": f"+${round((tp3-entry)*shares, 0):,.0f}",
    }


def lambda_handler(event, context):
    t0 = time.time()
    print("[trade-tickets] starting")

    cascade = get_active_cascade()
    portfolio_usd = float(event.get("portfolio_usd")) if event.get("portfolio_usd") else DEFAULT_PORTFOLIO_USD

    # Collect all cascade candidates (alert_tier + medium_tier + laggards)
    candidates = []
    seen = set()
    for tier_key in ["alert_tier", "medium_tier", "laggards_hot_themes"]:
        for c in (cascade.get(tier_key) or [])[:15]:
            t = c.get("ticker")
            if not t or t in seen:
                continue
            seen.add(t)
            candidates.append(c)

    print(f"[trade-tickets] generating tickets for {len(candidates)} candidates "
          f"(portfolio_usd=${portfolio_usd:,.0f})")

    # Load horizon attribution once (used by all tickets for setup-aware sizing)
    best_horizons = get_horizon_attribution()
    if best_horizons:
        print(f"[trade-tickets] horizon-aware mode: {len(best_horizons)} features have learned horizons")
    else:
        print(f"[trade-tickets] horizon-aware mode: using DEFAULT tier-based horizons (calibration not mature yet)")

    # Parallel fetch + ticket build
    def _build(c):
        bars = fetch_polygon_ohlc(c["ticker"], days=25)
        return build_ticket(c, bars, portfolio_usd, best_horizons)

    tickets = []
    with ThreadPoolExecutor(max_workers=N_WORKERS) as ex:
        for t in ex.map(_build, candidates):
            tickets.append(t)

    # Sort by rr_quality_score desc, then combined_score
    valid_tickets = [t for t in tickets if not t.get("error")]
    valid_tickets.sort(key=lambda x: (-(x.get("rr_quality_score") or 0),
                                       -(x.get("combined_score") or 0)))
    invalid_tickets = [t for t in tickets if t.get("error")]

    elapsed = round(time.time() - t0, 1)
    print(f"[trade-tickets] DONE — {len(valid_tickets)} valid tickets in {elapsed}s")

    output = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "elapsed_s": elapsed,
        "portfolio_usd": portfolio_usd,
        "n_tickets": len(valid_tickets),
        "n_errors": len(invalid_tickets),
        "tickets": valid_tickets,
        "errors": invalid_tickets[:5],
        "sizing_methodology": {
            "atr_multipliers": {"alert_tier": 1.5, "medium": 1.8,
                                 "laggard": 2.0, "watch": 2.5},
            "tp_multiples_by_theme_accel": {
                ">=100": [1.0, 2.0, 5.0], "50-99": [1.0, 2.0, 3.0],
                "20-49": [1.0, 1.5, 2.0], "<20": [0.5, 1.0, 1.5],
            },
        },
    }

    s3.put_object(
        Bucket=S3_BUCKET, Key="data/trade-tickets.json",
        Body=json.dumps(output, default=str).encode(),
        ContentType="application/json", CacheControl="public, max-age=600",
    )

    return {
        "statusCode": 200,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps({
            "ok": True, "elapsed_s": elapsed,
            "n_tickets": len(valid_tickets),
            "top_3_tickets": [
                {"ticker": t["ticker"], "tier": t["tier"], "entry": t["entry"],
                 "stop": t["stop_loss"], "tp3": t["tp3"], "rr": t["rr_tp3"],
                 "shares": t["shares"], "max_loss_usd": t["max_loss_usd"]}
                for t in valid_tickets[:3]
            ],
        }),
    }
