"""
justhodl-pnl-tracker — Loop 2 hypothetical PnL tracker.

Runs daily at 22:00 UTC. Computes:
  - buy_and_hold portfolio value (starting allocation, drift-only)
  - khalid_strategy value (regime-adjusted allocation since inception)
  - delta_pct (system's value-add vs B&H)

Writes:
  - portfolio/pnl-daily.json   (today snapshot, full detail)
  - portfolio/pnl-history.json (rolling 365-day history)
"""
import json
import os
import time
import urllib.request
import ssl
from datetime import datetime, timezone, timedelta
import boto3

# Phase 2 KA rebrand — recursive khalid_* → ka_* alias helper.
try:
    from ka_aliases import add_ka_aliases
except Exception as _e:
    print(f"WARN: ka_aliases unavailable: {_e}")
    def add_ka_aliases(obj, **_kwargs):
        return obj

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
POLYGON_KEY = os.environ.get("POLYGON_KEY", "zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d")

s3 = boto3.client("s3", region_name=REGION)
ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE


def fetch_json(url, timeout=15):
    try:
        req = urllib.request.Request(url, headers={"Accept": "application/json"})
        with urllib.request.urlopen(req, timeout=timeout, context=ctx) as r:
            return json.loads(r.read().decode("utf-8"))
    except Exception as e:
        print(f"[FETCH] {url[:80]}: {e}")
        return None


def get_spot_price(ticker):
    """Get latest closing price from Polygon."""
    url = f"https://api.polygon.io/v2/aggs/ticker/{ticker}/prev?adjusted=true&apiKey={POLYGON_KEY}"
    data = fetch_json(url)
    if data and isinstance(data.get("results"), list) and data["results"]:
        return float(data["results"][0].get("c", 0))
    return None


def get_s3_json(key):
    try:
        obj = s3.get_object(Bucket=BUCKET, Key=key)
        return json.loads(obj["Body"].read().decode("utf-8"))
    except Exception as e:
        print(f"[S3] {key}: {e}")
        return None


def put_s3_json(key, body, cache="public, max-age=300"):
    # Phase 2 dual-write — duplicate khalid_* keys as ka_* in payload
    body = add_ka_aliases(body)
    s3.put_object(
        Bucket=BUCKET, Key=key,
        Body=json.dumps(body, default=str).encode("utf-8"),
        ContentType="application/json", CacheControl=cache,
    )


def regime_to_allocation(regime, action_required):
    """Map JustHodl regime + action to a target allocation."""
    r = (regime or "").upper()
    a = (action_required or "").upper()

    # CRISIS / strong bearish action → defensive
    if "CRISIS" in r or "REDUCE ALL RISK" in a or "RAISE CASH" in a:
        return {"SPY": 0.30, "TLT": 0.20, "GLD": 0.10, "CASH": 0.40}

    # BEAR / cautious
    if "BEAR" in r or "PRE-CRISIS" in r or "REDUCE" in a or "DEFENSIVE" in a:
        return {"SPY": 0.40, "TLT": 0.20, "GLD": 0.15, "CASH": 0.25}

    # NEUTRAL — match starting baseline
    if "NEUTRAL" in r or not r:
        return {"SPY": 0.60, "TLT": 0.20, "GLD": 0.10, "CASH": 0.10}

    # BULL / risk-on
    if "BULL" in r or "OPTIMISTIC" in r or "RISK_ON" in r:
        return {"SPY": 0.75, "TLT": 0.10, "GLD": 0.05, "CASH": 0.10}

    # EUPHORIA — still some restraint (don't chase)
    if "EUPHORIA" in r:
        return {"SPY": 0.80, "TLT": 0.05, "GLD": 0.05, "CASH": 0.10}

    # Unknown → fall back to baseline
    return {"SPY": 0.60, "TLT": 0.20, "GLD": 0.10, "CASH": 0.10}


def compute_portfolio_value(allocations, starting_value, current_prices, baseline_prices):
    """Given current allocation + price ratios from baseline, compute today's value."""
    total = 0.0
    breakdown = {}
    for ticker, weight in allocations.items():
        if ticker == "CASH":
            # Cash earns ~0% (could add a tiny SOFR yield in v2)
            value = starting_value * weight
        else:
            cur = current_prices.get(ticker)
            base = baseline_prices.get(ticker)
            if not cur or not base or base == 0:
                value = starting_value * weight  # treat unknown as flat
            else:
                ratio = cur / base
                value = starting_value * weight * ratio
        breakdown[ticker] = round(value, 2)
        total += value
    return total, breakdown


def lambda_handler(event, context):
    print("=== JUSTHODL PNL TRACKER v1 ===")
    now = datetime.now(timezone.utc)
    today_str = now.strftime("%Y-%m-%d")

    # 1. Read portfolio state (starting allocation + history)
    state = get_s3_json("portfolio/state.json")
    if not state:
        return {"statusCode": 500, "body": json.dumps({"error": "portfolio/state.json missing"})}

    starting = state.get("starting_value_usd", 100000)
    inception = state.get("as_of", today_str)
    baseline_alloc = state.get("allocations", {"SPY": 0.60, "TLT": 0.20, "GLD": 0.10, "CASH": 0.10})

    # 2. Read intelligence report → current regime
    intel = get_s3_json("intelligence-report.json") or {}
    phase = intel.get("phase", "UNKNOWN")
    regime = intel.get("regime", {}).get("khalid", "UNKNOWN") if isinstance(intel.get("regime"), dict) else "UNKNOWN"
    action = intel.get("action_required", "")
    print(f"  Current phase={phase}, khalid_regime={regime}, action={action[:80]}")

    # 3. Get baseline prices (what we paid at inception)
    # If state.json has baseline_prices, use them; else fetch + persist
    baseline_prices = state.get("baseline_prices", {})
    if not baseline_prices:
        print("  Baseline prices not set — capturing today's prices as baseline")
        for tk in ("SPY", "TLT", "GLD"):
            p = get_spot_price(tk)
            if p:
                baseline_prices[tk] = p
        # Persist the baseline back to state.json
        state["baseline_prices"] = baseline_prices
        state["as_of"] = today_str
        put_s3_json("portfolio/state.json", state, cache="no-cache")
        print(f"  Baseline captured: {baseline_prices}")

    # 4. Get current prices
    current_prices = {}
    for tk in ("SPY", "TLT", "GLD"):
        p = get_spot_price(tk)
        if p:
            current_prices[tk] = p
    print(f"  Current prices: {current_prices}")

    if not current_prices:
        return {"statusCode": 500, "body": json.dumps({"error": "could not fetch any current prices"})}

    # 5. Compute buy-and-hold value
    bh_value, bh_breakdown = compute_portfolio_value(
        baseline_alloc, starting, current_prices, baseline_prices,
    )

    # 6. Compute khalid_strategy current value
    # For v1 simplicity: apply CURRENT regime's allocation to TODAY's
    # price ratios from baseline. This is approximate (it doesn't model
    # historical regime changes mid-period — that requires regime history
    # which we'll add in v2). Conservative, but easy to reason about.
    khalid_alloc = regime_to_allocation(regime, action)
    ks_value, ks_breakdown = compute_portfolio_value(
        khalid_alloc, starting, current_prices, baseline_prices,
    )

    # 7. Compute deltas
    bh_return_pct = ((bh_value - starting) / starting) * 100
    ks_return_pct = ((ks_value - starting) / starting) * 100
    delta_pct = ks_return_pct - bh_return_pct

    snapshot = {
        "as_of": today_str,
        "generated_at": now.isoformat(),
        "inception": inception,
        "days_since_inception": max(0, (now.date() - datetime.fromisoformat(inception).date()).days)
                                if inception else 0,
        "starting_value_usd": starting,
        "current_phase": phase,
        "current_regime": regime,
        "current_action_required": action[:200],
        "buy_and_hold": {
            "allocation": baseline_alloc,
            "current_value_usd": round(bh_value, 2),
            "return_pct": round(bh_return_pct, 2),
            "breakdown": bh_breakdown,
        },
        "khalid_strategy": {
            "allocation": khalid_alloc,
            "current_value_usd": round(ks_value, 2),
            "return_pct": round(ks_return_pct, 2),
            "breakdown": ks_breakdown,
            "_note": "v1 approximation: current regime applied to current prices; doesn't model historical rebalances",
        },
        "delta_pct": round(delta_pct, 2),
        "system_alpha": round(delta_pct, 2),
        "prices": {
            "current": current_prices,
            "baseline": baseline_prices,
        },
        "v": "1.0",
        "DISCLAIMER": "HYPOTHETICAL — for tracking only. Not investment advice. Past hypothetical performance does not predict future returns.",
    }

    # 8. Write today's snapshot
    put_s3_json("portfolio/pnl-daily.json", snapshot, cache="public, max-age=300")
    print(f"  Wrote portfolio/pnl-daily.json ({bh_return_pct:+.2f}% B&H, {ks_return_pct:+.2f}% Khalid, Δ {delta_pct:+.2f}%)")

    # 9. Append to history (rolling 365 days)
    history = get_s3_json("portfolio/pnl-history.json") or {"v": "1.0", "snapshots": []}
    snapshots = history.get("snapshots", [])
    # Keep one snapshot per day — replace today's if it already exists
    snapshots = [s for s in snapshots if s.get("as_of") != today_str]
    snapshots.append({
        "as_of": today_str,
        "buy_and_hold_value_usd": round(bh_value, 2),
        "khalid_strategy_value_usd": round(ks_value, 2),
        "buy_and_hold_return_pct": round(bh_return_pct, 2),
        "khalid_return_pct": round(ks_return_pct, 2),
        "delta_pct": round(delta_pct, 2),
        "regime": regime,
        "phase": phase,
    })
    # Trim to last 365 days
    cutoff = (now - timedelta(days=365)).strftime("%Y-%m-%d")
    snapshots = [s for s in snapshots if s.get("as_of", "") >= cutoff]
    history["snapshots"] = sorted(snapshots, key=lambda s: s.get("as_of", ""))
    history["last_updated"] = now.isoformat()
    put_s3_json("portfolio/pnl-history.json", history, cache="public, max-age=600")
    print(f"  History updated: {len(snapshots)} daily snapshots in last 365 days")

    return {
        "statusCode": 200,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps({
            "as_of": today_str,
            "buy_and_hold_return_pct": round(bh_return_pct, 2),
            "khalid_return_pct": round(ks_return_pct, 2),
            "delta_pct": round(delta_pct, 2),
            "phase": phase,
            "regime": regime,
        }),
    }
