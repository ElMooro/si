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


def build_ticket(candidate: dict, bars: List[dict],
                 portfolio_usd: float) -> dict:
    """Build a complete trade ticket from cascade candidate + price bars."""
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

    # Determine tier & multipliers
    tier = candidate.get("tier") or "UNKNOWN"
    is_laggard = candidate.get("is_laggard") or tier == "LAGGARD"
    atr_mult = determine_atr_multiplier(tier, is_laggard)
    theme_accel = (candidate.get("theme_acceleration") or
                   candidate.get("max_rs_acceleration") or 0)
    tp_multiples = determine_tp_multiples(theme_accel)

    # Stop loss
    stop_loss = entry - (atr_mult * atr)
    risk_per_share = entry - stop_loss
    risk_pct = (risk_per_share / entry) * 100

    # Take profits (R-multiple)
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

    # Parallel fetch + ticket build
    def _build(c):
        bars = fetch_polygon_ohlc(c["ticker"], days=25)
        return build_ticket(c, bars, portfolio_usd)

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
