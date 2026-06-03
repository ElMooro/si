"""justhodl-ticket-ai-rationale

Generates AI-powered 1-2 sentence rationales for fresh trade tickets.
Used by prepump-router + Telegram alerts to include the WHY.

Pipeline:
  1. Reads data/trade-tickets.json (current tickets)
  2. Reads cascade context for each ticker (theme, hot ETF, options data)
  3. For each ticker NOT yet in rationale cache (or stale), calls Claude
  4. Generates: setup summary + horizon + best features + R:R
  5. Writes data/trade-tickets-ai-rationale.json keyed by ticker

Schedule: hourly at :05 (after trade-tickets refresh at :00)
"""
import json
import os
import time
import urllib.request
import urllib.parse
from datetime import datetime, timezone, timedelta
from typing import Optional, List, Dict

import boto3

S3_BUCKET = "justhodl-dashboard-live"
ANTHROPIC_MODEL = "claude-haiku-4-5-20251001"
CACHE_HOURS = 6  # regenerate rationale if older than this

s3 = boto3.client("s3", region_name="us-east-1")
ssm = boto3.client("ssm", region_name="us-east-1")


def _read_json(key: str) -> Optional[dict]:
    try:
        return json.loads(s3.get_object(Bucket=S3_BUCKET, Key=key)["Body"].read())
    except Exception:
        return None


def _get_anthropic_key() -> str:
    if os.environ.get("ANTHROPIC_API_KEY"):
        return os.environ["ANTHROPIC_API_KEY"]
    for path in ["/justhodl/anthropic/api_key", "/anthropic/api_key"]:
        try:
            return ssm.get_parameter(Name=path, WithDecryption=True)["Parameter"]["Value"]
        except Exception:
            continue
    return ""


def call_claude(prompt: str, system: str = "", max_tokens: int = 350) -> str:
    api_key = _get_anthropic_key()
    if not api_key:
        return ""
    body = {"model": ANTHROPIC_MODEL, "max_tokens": max_tokens,
            "messages": [{"role": "user", "content": prompt}]}
    if system:
        body["system"] = system
    req = urllib.request.Request(
        "https://api.anthropic.com/v1/messages",
        data=json.dumps(body).encode(),
        headers={"Content-Type": "application/json",
                  "anthropic-version": "2023-06-01", "x-api-key": api_key},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            data = json.loads(r.read().decode())
        for block in data.get("content", []):
            if block.get("type") == "text":
                return block.get("text", "").strip()
        return ""
    except Exception as e:
        print(f"[claude] {e}")
        return ""


def find_cascade_context(ticker: str, cascade: dict) -> dict:
    """Find the cascade entry for this ticker, with hot_etf + theme info."""
    for tier_key in ["alert_tier", "medium_tier", "laggards_hot_themes", "watch_tier"]:
        for c in (cascade.get(tier_key) or []):
            if c.get("ticker") == ticker:
                return {
                    "tier_key": tier_key,
                    "industry": c.get("industry_label") or c.get("industry"),
                    "hot_etf": c.get("hot_etf"),
                    "theme_acceleration": c.get("theme_acceleration") or c.get("max_rs_acceleration"),
                    "n_etfs_top_10": c.get("n_etfs_in_top_10"),
                    "aggregate_flow_5d_usd": c.get("aggregate_flow_5d_usd"),
                    "combined_score": c.get("combined_score"),
                    "perf_5d_pct": c.get("perf_5d_pct"),
                    "is_laggard": c.get("is_laggard"),
                }
    return {}


def find_options_context(ticker: str, options_flow: dict) -> dict:
    """Find options data for this ticker if it's in extreme call flow."""
    for c in ((options_flow.get("extreme_call_flow") or []) +
              (options_flow.get("bullish_call_flow") or [])):
        if c.get("ticker") == ticker:
            return {
                "cv_pv_ratio": c.get("cv_pv_ratio"),
                "mean_iv": c.get("mean_iv"),
                "smart_money_blocks": c.get("n_smart_money_blocks"),
                "alert_level": c.get("alert_level"),
            }
    return {}


def find_insider_context(ticker: str, insider_clusters: dict) -> dict:
    """Find insider cluster data for this ticker."""
    for c in (insider_clusters.get("clusters") or insider_clusters.get("items") or []):
        if c.get("ticker") == ticker:
            return {
                "n_insiders": c.get("n_insiders") or c.get("cluster_size"),
                "total_value_usd": c.get("total_value_usd"),
            }
    return {}


def generate_rationale(ticker: str, ticket: dict, cascade_ctx: dict,
                        options_ctx: dict, insider_ctx: dict,
                        horizon_attribution: dict) -> str:
    """Have Claude generate a 1-2 sentence rationale for this trade ticket."""
    
    # Build feature context
    feature_lines = []
    if cascade_ctx.get("combined_score"):
        feature_lines.append(f"cascade combined_score: {cascade_ctx['combined_score']}")
    if cascade_ctx.get("theme_acceleration"):
        feature_lines.append(f"theme_acceleration: +{cascade_ctx['theme_acceleration']:.0f}%")
    if cascade_ctx.get("hot_etf"):
        feature_lines.append(f"hot ETF: {cascade_ctx['hot_etf']}")
    if cascade_ctx.get("aggregate_flow_5d_usd"):
        feature_lines.append(f"5d ETF flow: ${cascade_ctx['aggregate_flow_5d_usd']/1e6:.0f}M")
    if cascade_ctx.get("is_laggard"):
        feature_lines.append(f"LAGGARD (perf_5d: {cascade_ctx.get('perf_5d_pct',0):.1f}%)")
    if options_ctx.get("cv_pv_ratio"):
        feature_lines.append(f"options C/P: {options_ctx['cv_pv_ratio']:.1f}")
    if options_ctx.get("smart_money_blocks"):
        feature_lines.append(f"smart money blocks: {options_ctx['smart_money_blocks']}")
    if insider_ctx.get("n_insiders"):
        feature_lines.append(f"insider buys: {insider_ctx['n_insiders']}")
    
    # Find best-horizon features for this ticker's setup
    setup_type = ticket.get("setup_type", "?")
    horizon_days = ticket.get("expected_horizon_days", 10)
    regime = ticket.get("horizon_regime", "swing")
    
    relevant_horizons = []
    if horizon_attribution:
        relevant_features = ["options_cv_pv_ratio", "theme_acceleration", 
                              "insider_n_buyers", "velocity_composite",
                              "aggregate_flow_5d_usd", "n_etfs_in_top_10"]
        for feat in relevant_features:
            info = horizon_attribution.get(feat)
            if info:
                relevant_horizons.append(f"{feat}={info.get('best_horizon','?')}")
    
    system = """You are JustHodl.AI's trade alert writer. Generate a SINGLE
1-2 sentence rationale for a trade ticket. Be specific with numbers and concise.
Format like a hedge-fund analyst's one-liner.

Return ONLY the rationale text. No JSON. No preamble. No bullet points.
Max 280 characters total. Example:
'MU swing pick · Hold ~10d · cascade 167.7 + theme_accel +89% in CHAT ETF.
TP3 +66% to $1765 (R:R 3.9x). Best feature horizons: 14d theme + 1d options.'"""

    user_prompt = f"""Generate a 1-2 sentence trade alert rationale for:

TICKER: {ticker}
SETUP: {setup_type} · {regime} ({horizon_days}d hold)
INDUSTRY: {cascade_ctx.get('industry','?')}

LEVELS:
  Entry: ${ticket.get('entry','?')}
  Stop: ${ticket.get('stop_loss','?')} (-{ticket.get('risk_pct',0):.1f}%)
  TP3: ${ticket.get('tp3','?')} ({ticket.get('rr_tp3',0):.1f}× R:R)

FEATURES:
{chr(10).join('  ' + l for l in feature_lines)}

CONFIDENCE: position_size_mult={ticket.get('position_confidence_multiplier','?')}× ({ticket.get('position_confidence_source','default')})

LEARNED HORIZONS (when calibration mature):
{', '.join(relevant_horizons[:4]) if relevant_horizons else 'using default tier horizons'}

Write the rationale now (1-2 sentences max, ~250 chars):"""

    rationale = call_claude(user_prompt, system=system, max_tokens=250)
    if not rationale:
        # Fallback rationale (no AI available)
        return (f"{ticker} {setup_type.lower().replace('_',' ')} · Hold ~{horizon_days}d {regime} · "
                f"Entry ${ticket.get('entry','?')} · Stop ${ticket.get('stop_loss','?')} · "
                f"TP3 ${ticket.get('tp3','?')} (R:R {ticket.get('rr_tp3',0):.1f}x)")
    
    return rationale


def lambda_handler(event, context):
    t0 = time.time()
    print(f"[ticket-ai-rationale] starting at {datetime.now(timezone.utc).isoformat()}")
    
    # Load tickets
    tickets_doc = _read_json("data/trade-tickets.json") or {}
    tickets = (tickets_doc.get("tickets") or [])
    valid_tickets = [t for t in tickets if not t.get("error")]
    print(f"[ticket-ai-rationale] loaded {len(valid_tickets)} valid tickets")
    
    if not valid_tickets:
        return {"statusCode": 200, "body": json.dumps({"ok": True, "msg": "no_tickets"})}
    
    # Load context data
    cascade = _read_json("data/theme-cascade-calibrated.json") or _read_json("data/theme-cascade.json") or {}
    options_flow = _read_json("data/polygon-options-flow.json") or {}
    insider_clusters = _read_json("data/insider-clusters.json") or {}
    cal = _read_json("data/cascade-calibration.json") or {}
    horizon_attribution = (cal.get("horizon_attribution") or {}).get("best_horizon_per_feature") or {}
    
    # Load existing rationale cache
    cache = _read_json("data/trade-tickets-ai-rationale.json") or {"by_ticker": {}}
    by_ticker = cache.get("by_ticker") or {}
    
    cache_cutoff = datetime.now(timezone.utc) - timedelta(hours=CACHE_HOURS)
    
    # Generate rationale for tickets that need it
    n_generated = 0
    n_cached = 0
    n_errors = 0
    
    for t in valid_tickets[:15]:  # cap at 15 to control cost
        ticker = t.get("ticker")
        if not ticker:
            continue
        
        # Check cache
        cached = by_ticker.get(ticker)
        if cached:
            try:
                cached_at = datetime.fromisoformat(cached.get("generated_at", ""))
                if cached_at > cache_cutoff:
                    # Check if ticket has changed materially (entry diff > 2%)
                    cached_entry = cached.get("entry")
                    current_entry = t.get("entry")
                    if cached_entry and current_entry:
                        diff_pct = abs(current_entry - cached_entry) / cached_entry * 100
                        if diff_pct < 2:
                            n_cached += 1
                            continue
            except Exception:
                pass
        
        # Gather context
        cascade_ctx = find_cascade_context(ticker, cascade)
        options_ctx = find_options_context(ticker, options_flow)
        insider_ctx = find_insider_context(ticker, insider_clusters)
        
        try:
            rationale = generate_rationale(ticker, t, cascade_ctx, options_ctx,
                                             insider_ctx, horizon_attribution)
            by_ticker[ticker] = {
                "rationale": rationale,
                "generated_at": datetime.now(timezone.utc).isoformat(),
                "entry": t.get("entry"),
                "setup_type": t.get("setup_type"),
                "expected_horizon_days": t.get("expected_horizon_days"),
                "horizon_regime": t.get("horizon_regime"),
            }
            n_generated += 1
            print(f"  ✓ {ticker}: {rationale[:120]}")
        except Exception as e:
            print(f"  ✗ {ticker}: {e}")
            n_errors += 1
    
    # Save updated cache
    output = {
        "schema_version": "1.0",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "model": ANTHROPIC_MODEL,
        "n_tickets_total": len(valid_tickets),
        "n_generated": n_generated,
        "n_cached": n_cached,
        "n_errors": n_errors,
        "by_ticker": by_ticker,
    }
    s3.put_object(
        Bucket=S3_BUCKET, Key="data/trade-tickets-ai-rationale.json",
        Body=json.dumps(output, default=str).encode(),
        ContentType="application/json", CacheControl="public, max-age=300",
    )
    
    elapsed = round(time.time() - t0, 1)
    print(f"[ticket-ai-rationale] DONE in {elapsed}s — gen={n_generated} cached={n_cached} err={n_errors}")
    
    return {
        "statusCode": 200,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps({
            "ok": True, "elapsed_s": elapsed,
            "n_generated": n_generated,
            "n_cached": n_cached,
            "n_errors": n_errors,
        }),
    }
