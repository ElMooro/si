"""justhodl-future-intelligence — composite "what's coming next" score.

Synthesizes the three forward-looking engines into one 0-100 per-ticker
score that feeds into bagger-engine + master-ranker.

INPUTS
══════
  data/forward-orders.json    — RPO + contracts (locked future revenue)
  data/rotation-chains.json   — value chain rotation (where capital is moving)
  data/buzz-velocity.json     — attention surge (where retail is looking)

OUTPUT (data/future-intelligence.json)
═════════════════════════════════════
  per-ticker composite score 0-100 with breakdown:
    {
      "ticker": "NVDA",
      "future_intel_score": 87.3,
      "forward_orders":     {score: 92, thesis: "..."},
      "rotation_chain":     {chain: "AI", tier: 1, role: "leader", lag_pct: 0},
      "buzz_velocity":      {score: 75, interp: "RISING", stealth: false},
      "thesis": "...",
    }

WEIGHTS
═══════
  Forward Orders:  45%  (most predictive — hard data)
  Rotation Chain:  30%  (market-confirmed momentum)
  Buzz Velocity:   25%  (sentiment signal — leading but noisier)

This composite then propagates to bagger-engine as the 8th pillar.
"""

import json
import os
from collections import defaultdict
from datetime import datetime, timezone

import boto3

from _sentry_lite import track_errors

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
OUTPUT_KEY = "data/future-intelligence.json"

# Input feeds
FEED_FWD     = "data/forward-orders.json"
FEED_ROT     = "data/rotation-chains.json"
FEED_BUZZ    = "data/buzz-velocity.json"
FEED_TRENDS  = "data/ticker-trends.json"

# Weights rebalanced 2026-05-31 to add 4th signal (Google search interest).
# Forward-orders still dominates (hard data) but each behavioral signal
# (Reddit/News buzz + Google search) gets independent weight.
WEIGHTS = {
    "forward_orders": 0.40,
    "rotation_chain": 0.25,
    "buzz_velocity":  0.20,
    "ticker_trends":  0.15,
}

# Threshold for future.signal.high_conviction events.
# Was 75, lowered to 65 after observing real conviction levels (top was
# GEV at 70.4). Tunable via env so we can dial it without redeploy.
HIGH_CONVICTION_THRESHOLD = int(os.environ.get("HIGH_CONVICTION_THRESHOLD", "65"))

# Threshold for ticker INCLUSION in the all_results list (anything weaker
# than this is filtered out as noise). Was implicit max < 15; explicit now.
MIN_INCLUSION_SCORE = int(os.environ.get("MIN_INCLUSION_SCORE", "15"))

s3 = boto3.client("s3", region_name=REGION)


def read_json(key, default=None):
    try:
        obj = s3.get_object(Bucket=BUCKET, Key=key)
        return json.loads(obj["Body"].read().decode("utf-8"))
    except Exception as e:
        print(f"[future-intel] read {key} failed: {e}")
        return default


def index_by_ticker(items, key="ticker"):
    """List of dicts → dict keyed by ticker."""
    return {item.get(key): item for item in (items or []) if item.get(key)}


def score_rotation_position(rot_data: dict, ticker: str) -> tuple:
    """Find this ticker in any rotation chain. Return (score, role_info)."""
    if not rot_data:
        return 0, None
    chains = rot_data.get("chains", {}) or {}
    
    best_score = 0
    best_role = None
    for chain_name, chain_info in chains.items():
        # Check if ticker is in next_up_tickers (highest value)
        for next_up in chain_info.get("next_up_tickers", []) or []:
            if next_up.get("ticker") == ticker:
                # next-up ticker score is 0-100 already
                s = next_up.get("score", 0)
                if s > best_score:
                    best_score = s
                    best_role = {
                        "chain":  chain_name,
                        "role":   f"next_up_tier_{chain_info.get('current_leader_tier', '?')+1}",
                        "lag_pct": next_up.get("lag_pct"),
                        "leader_30d": next_up.get("leader_30d_pct"),
                    }
        
        # Or in the leader tier (give moderate score — momentum confirmed)
        leader_tier = chain_info.get("current_leader_tier")
        if leader_tier is not None:
            # Need to find ticker in our chain definitions — not in output JSON though
            # Look at lead_lag_relations to infer
            pass
    
    return best_score, best_role


def score_buzz(buzz_data: dict, ticker: str) -> tuple:
    """Look up ticker in buzz results."""
    if not buzz_data:
        return 0, None
    by_ticker = index_by_ticker(buzz_data.get("all_results"))
    rec = by_ticker.get(ticker)
    if not rec:
        return 0, None
    return rec.get("score", 0), {
        "interpretation": rec.get("reddit_velocity", {}).get("interpretation"),
        "composite_velocity": rec.get("composite_velocity"),
        "stealth": rec.get("stealth_signal"),
        "price_7d_pct": rec.get("price_perf_7d_pct"),
    }


def score_forward(fwd_data: dict, ticker: str) -> tuple:
    """Look up ticker in forward-orders results."""
    if not fwd_data:
        return 0, None
    by_ticker = index_by_ticker(fwd_data.get("all_results"))
    rec = by_ticker.get(ticker)
    if not rec:
        return 0, None
    return rec.get("composite", 0), {
        "rpo_yield_pct":    rec.get("data", {}).get("rpo_yield_pct"),
        "rpo_growth_pct":   rec.get("data", {}).get("rpo_growth_yoy_pct"),
        "contracts_total":  (rec.get("contracts") or {}).get("total_usd"),
        "thesis":           rec.get("thesis"),
    }


def score_trends(trends_data: dict, ticker: str) -> tuple:
    """Look up ticker in ticker-trends (Google search) results."""
    if not trends_data:
        return 0, None
    by_ticker = index_by_ticker(trends_data.get("all_results"))
    rec = by_ticker.get(ticker)
    if not rec:
        return 0, None
    return rec.get("score", 0), {
        "velocity":     rec.get("velocity"),
        "current_level": rec.get("current_level"),
        "interp":       rec.get("interp"),
        "stealth":      rec.get("stealth"),
        "price_7d_pct": rec.get("price_7d_pct"),
    }


def composite_score(fwd_s, rot_s, buzz_s, trends_s) -> float:
    return round(
        fwd_s    * WEIGHTS["forward_orders"] +
        rot_s    * WEIGHTS["rotation_chain"] +
        buzz_s   * WEIGHTS["buzz_velocity"] +
        trends_s * WEIGHTS["ticker_trends"],
        1,
    )


def synthesize_thesis(fwd_role, rot_role, buzz_role, trends_role) -> str:
    bits = []
    if fwd_role and fwd_role.get("thesis"):
        bits.append(fwd_role["thesis"])
    if rot_role:
        bits.append(f"rotation: {rot_role.get('role','?')} in {rot_role.get('chain','?')} chain")
    if buzz_role and buzz_role.get("composite_velocity", 0) >= 2:
        sb = "stealth " if buzz_role.get("stealth") else ""
        bits.append(f"{sb}buzz {buzz_role.get('composite_velocity')}x")
    if trends_role and trends_role.get("velocity", 0) >= 2:
        st = "stealth " if trends_role.get("stealth") else ""
        bits.append(f"{st}google search {trends_role.get('velocity')}x")
    return " · ".join(bits) if bits else "—"


@track_errors
def handler(event, context):
    started = datetime.now(timezone.utc)
    
    fwd_data    = read_json(FEED_FWD)    or {}
    rot_data    = read_json(FEED_ROT)    or {}
    buzz_data   = read_json(FEED_BUZZ)   or {}
    trends_data = read_json(FEED_TRENDS) or {}
    
    print(f"[future-intel] fwd:    {len(fwd_data.get('all_results') or [])} tickers")
    print(f"[future-intel] rot:    {len(rot_data.get('chains') or {})} chains")
    print(f"[future-intel] buzz:   {len(buzz_data.get('all_results') or [])} tickers")
    print(f"[future-intel] trends: {len(trends_data.get('all_results') or [])} tickers")
    
    # Build union of all tickers across the four feeds
    all_tickers = set()
    for r in (fwd_data.get("all_results") or []):
        if r.get("ticker"):
            all_tickers.add(r["ticker"])
    for r in (buzz_data.get("all_results") or []):
        if r.get("ticker"):
            all_tickers.add(r["ticker"])
    for r in (trends_data.get("all_results") or []):
        if r.get("ticker"):
            all_tickers.add(r["ticker"])
    for chain in (rot_data.get("chains") or {}).values():
        for next_up in chain.get("next_up_tickers", []) or []:
            if next_up.get("ticker"):
                all_tickers.add(next_up["ticker"])
    
    print(f"[future-intel] union: {len(all_tickers)} tickers to score")
    
    # Score each ticker across 4 signals
    results = []
    for ticker in sorted(all_tickers):
        fwd_s, fwd_role       = score_forward(fwd_data, ticker)
        rot_s, rot_role       = score_rotation_position(rot_data, ticker)
        buzz_s, buzz_role     = score_buzz(buzz_data, ticker)
        trends_s, trends_role = score_trends(trends_data, ticker)
        
        # Skip tickers with no meaningful signal
        if max(fwd_s, rot_s, buzz_s, trends_s) < MIN_INCLUSION_SCORE:
            continue
        
        composite = composite_score(fwd_s, rot_s, buzz_s, trends_s)
        
        # Count independent signals (any score >= 30 counts)
        n_signals = sum(1 for s in (fwd_s, rot_s, buzz_s, trends_s) if s >= 30)
        # Bonus for multi-signal convergence
        if n_signals >= 2:
            composite = min(100, composite + 8)
        if n_signals >= 3:
            composite = min(100, composite + 6)
        if n_signals >= 4:
            composite = min(100, composite + 4)
        
        results.append({
            "ticker":          ticker,
            "future_intel_score": composite,
            "n_independent_signals": n_signals,
            "subscores": {
                "forward_orders":  round(fwd_s, 1),
                "rotation_chain":  round(rot_s, 1),
                "buzz_velocity":   round(buzz_s, 1),
                "ticker_trends":   round(trends_s, 1),
            },
            "forward_orders":  fwd_role,
            "rotation_chain":  rot_role,
            "buzz_velocity":   buzz_role,
            "ticker_trends":   trends_role,
            "thesis":          synthesize_thesis(fwd_role, rot_role, buzz_role, trends_role),
        })
    
    results.sort(key=lambda r: -r["future_intel_score"])
    
    out = {
        "schema_version":  "1.0",
        "method":          "future_intelligence_composite_v1",
        "generated_at":    datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "duration_s":      round((datetime.now(timezone.utc) - started).total_seconds(), 1),
        "weights":         WEIGHTS,
        "n_scored":        len(results),
        "feed_freshness": {
            "forward_orders":  fwd_data.get("generated_at"),
            "rotation_chain":  rot_data.get("generated_at"),
            "buzz_velocity":   buzz_data.get("generated_at"),
            "ticker_trends":   trends_data.get("generated_at"),
        },
        "top_25":          results[:25],
        "all_results":     results,
        "highlights": {
            "high_conviction":      [r for r in results if r["future_intel_score"] >= HIGH_CONVICTION_THRESHOLD][:10],
            "multi_signal":         [r for r in results if r["n_independent_signals"] >= 2][:15],
            "stealth_buzz":         [r for r in results if (r.get("buzz_velocity") or {}).get("stealth")][:10],
            "google_stealth":       [r for r in results if (r.get("ticker_trends") or {}).get("stealth")][:10],
            "next_up_rotation":     [r for r in results if (r.get("rotation_chain") or {}).get("role", "").startswith("next_up")][:10],
            "locked_future_value":  [r for r in results
                                       if (r.get("forward_orders") or {}).get("rpo_yield_pct", 0) >= 30][:10],
            "4_signal_alignment":   [r for r in results if r["n_independent_signals"] >= 3][:10],
        },
        "notes": "Composite of forward-orders (45%) + rotation-chain (30%) + buzz-velocity (25%) with bonus for multi-signal convergence.",
    }
    
    body = json.dumps(out, default=str, separators=(",", ":")).encode("utf-8")
    s3.put_object(Bucket=BUCKET, Key=OUTPUT_KEY, Body=body,
                  ContentType="application/json",
                  CacheControl="public, max-age=3600")
    print(f"[future-intel] wrote {len(body):,}B  top: {results[0]['ticker'] if results else 'none'}")
    
    # Emit event for the top high-conviction picks
    try:
        from system_events import publish_many
        events_to_pub = []
        for r in results[:8]:
            if r["future_intel_score"] >= HIGH_CONVICTION_THRESHOLD:
                events_to_pub.append(("future.signal.high_conviction", {
                    "ticker":              r["ticker"],
                    "score":               r["future_intel_score"],
                    "n_independent_signals": r["n_independent_signals"],
                    "subscores":           r["subscores"],
                    "thesis":              r["thesis"],
                    "threshold":           HIGH_CONVICTION_THRESHOLD,
                }))
        if events_to_pub:
            publish_many(events_to_pub)
    except Exception as e:
        print(f"[future-intel] event publish failed: {e}")
    
    return {"statusCode": 200, "body": json.dumps({
        "ok":         True,
        "n_scored":   len(results),
        "top_ticker": results[0]["ticker"] if results else None,
        "top_score":  results[0]["future_intel_score"] if results else None,
        "duration_s": out["duration_s"],
    })}


lambda_handler = handler
