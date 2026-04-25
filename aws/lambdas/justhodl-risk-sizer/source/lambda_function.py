"""
justhodl-risk-sizer — Phase 3 risk management layer.

Reads outputs from Phases 1-2 and Loops 2,4. Produces sized position
recommendations with explicit reasoning for each, plus portfolio-level
constraints (max gross exposure, drawdown circuit breakers).

System NEVER trades. NEVER auto-rebalances. NEVER integrates with broker.
Pure recommendation layer — execute manually.
"""
import json
import os
import statistics
from datetime import datetime, timezone, timedelta
import boto3

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"

s3 = boto3.client("s3", region_name=REGION)


# ─── Risk-management constants ─────────────────────────────────────────
# These are CONSERVATIVE defaults. Real hedge funds use stress-tested
# values; we use sensible-defaults that prevent the worst mistakes.

# Max gross exposure by regime (% of portfolio in risk assets)
REGIME_MAX_EXPOSURE = {
    "RISK_OFF": 0.50,
    "NEUTRAL":  0.75,
    "RISK_ON":  1.00,
}

# Drawdown circuit breaker thresholds (% drawdown → size multiplier)
DRAWDOWN_TRIGGERS = [
    (-0.05, 0.75),  # 5% DD: scale to 75%
    (-0.10, 0.50),  # 10% DD: scale to 50%
    (-0.15, 0.00),  # 15% DD: STOP adding (existing positions stay)
]

# Per-position cap — no single name > 8% even with high conviction
MAX_SINGLE_POSITION_PCT = 0.08
# Per-cluster cap — no correlated cluster > 25% of portfolio
MAX_CLUSTER_PCT = 0.25
# Min cluster correlation to consider clustering
CLUSTER_CORRELATION_THRESHOLD = 0.65
# Fractional Kelly multiplier (1.0 = full Kelly, 0.25 = 1/4 Kelly)
KELLY_FRACTION = 0.25


def get_s3_json(key, default=None):
    try:
        obj = s3.get_object(Bucket=BUCKET, Key=key)
        return json.loads(obj["Body"].read().decode("utf-8"))
    except Exception as e:
        print(f"[S3] {key}: {e}")
        return default


def put_s3_json(key, body, cache="public, max-age=900"):
    s3.put_object(
        Bucket=BUCKET, Key=key,
        Body=json.dumps(body, default=str).encode("utf-8"),
        ContentType="application/json", CacheControl=cache,
    )


def safe_float(v, default=None):
    try:
        if v is None: return default
        f = float(v)
        return f if f == f else default
    except Exception:
        return default


def compute_returns(history):
    """Convert history list of {d, c} to a list of daily returns."""
    if not history or len(history) < 2:
        return []
    closes = [h.get("c") for h in history if isinstance(h, dict) and h.get("c")]
    closes = [c for c in closes if c and c > 0]
    if len(closes) < 2:
        return []
    # history is most-recent first; reverse for chronological
    closes_chron = list(reversed(closes))
    returns = []
    for i in range(1, len(closes_chron)):
        r = (closes_chron[i] - closes_chron[i-1]) / closes_chron[i-1]
        returns.append(r)
    return returns


def correlation(a, b):
    """Pearson correlation between two return series."""
    n = min(len(a), len(b))
    if n < 20:
        return None
    a = a[-n:]
    b = b[-n:]
    a_mean = statistics.mean(a)
    b_mean = statistics.mean(b)
    a_dev = [x - a_mean for x in a]
    b_dev = [x - b_mean for x in b]
    cov = sum(a_dev[i] * b_dev[i] for i in range(n)) / n
    a_var = sum(x * x for x in a_dev) / n
    b_var = sum(x * x for x in b_dev) / n
    if a_var == 0 or b_var == 0:
        return None
    return cov / ((a_var * b_var) ** 0.5)


def cluster_by_correlation(symbols, returns_by_symbol, threshold=CLUSTER_CORRELATION_THRESHOLD):
    """Greedy clustering: for each symbol, find others with corr > threshold."""
    clusters = []
    assigned = set()

    for sym in symbols:
        if sym in assigned:
            continue
        if sym not in returns_by_symbol:
            # Symbol with no return data goes alone (defensive)
            clusters.append({"id": f"isolated_{sym}", "members": [sym], "avg_correlation": 0})
            assigned.add(sym)
            continue
        cluster_members = [sym]
        cluster_corrs = []
        for other in symbols:
            if other == sym or other in assigned or other not in returns_by_symbol:
                continue
            corr = correlation(returns_by_symbol[sym], returns_by_symbol[other])
            if corr is not None and corr > threshold:
                cluster_members.append(other)
                cluster_corrs.append(corr)
        for m in cluster_members:
            assigned.add(m)
        clusters.append({
            "id": f"cluster_{cluster_members[0]}",
            "members": sorted(cluster_members),
            "avg_correlation": round(statistics.mean(cluster_corrs), 3) if cluster_corrs else 0,
            "size": len(cluster_members),
        })

    return clusters


def compute_drawdown(pnl_history_snapshots):
    """Compute current drawdown as % from peak in pnl-history.json."""
    if not pnl_history_snapshots:
        return 0.0, None
    # snapshot shape: {as_of, khalid_strategy_value_usd, ...}
    values = [(s.get("as_of"), s.get("khalid_strategy_value_usd"))
              for s in pnl_history_snapshots
              if s.get("khalid_strategy_value_usd")]
    if len(values) < 2:
        return 0.0, None
    # Sort by date
    values.sort()
    peak = values[0][1]
    peak_date = values[0][0]
    current_dd = 0
    for date, val in values:
        if val > peak:
            peak = val
            peak_date = date
    current_value = values[-1][1]
    if peak > 0:
        current_dd = (current_value - peak) / peak
    return round(current_dd, 4), peak_date


def drawdown_size_multiplier(current_dd):
    """Map current drawdown % to a size scaling factor."""
    for trigger_dd, multiplier in DRAWDOWN_TRIGGERS:
        if current_dd <= trigger_dd:
            continue  # we\'re past this trigger; check next
        # current_dd > trigger_dd, so we haven\'t hit it yet OR
        # we\'re between thresholds. Find the worst multiplier we\'ve hit.
    # Walk through triggers in order, applying the worst hit
    multiplier = 1.0
    for trigger_dd, mult in DRAWDOWN_TRIGGERS:
        if current_dd <= trigger_dd:
            multiplier = mult
    return multiplier


def kelly_size(conviction_pct, edge_pct=0.05):
    """Conservative Kelly sizing.
    conviction_pct: probability of being right (0.5-1.0)
    edge_pct: expected edge if right (default 5%)
    Returns recommended position size as fraction of capital.
    """
    if conviction_pct <= 0.50:
        return 0.0
    p = conviction_pct
    q = 1 - p
    # Kelly = (bp - q) / b where b = win/loss ratio
    # Simplified: assume symmetric outcomes, b = 1
    full_kelly = max(0, p - q)  # = 2p - 1
    # Apply fractional Kelly multiplier
    fractional = full_kelly * KELLY_FRACTION
    # Cap at 8%
    return round(min(MAX_SINGLE_POSITION_PCT, fractional), 4)


def lambda_handler(event, context):
    print("=== RISK SIZER v1 ===")
    now = datetime.now(timezone.utc)

    # ─── 1. Load all inputs ─────────────────────────────────────────────
    asym = get_s3_json("opportunities/asymmetric-equity.json", {})
    debate = get_s3_json("investor-debate/_index.json", {})
    regime = get_s3_json("regime/current.json", {})
    pnl_history = get_s3_json("portfolio/pnl-history.json", {})
    state = get_s3_json("portfolio/state.json", {})
    report = get_s3_json("data/report.json", {})

    print(f"  asymmetric setups: {len(asym.get('top_setups', []))}")
    print(f"  watchlist debate tickers: {debate.get('n_tickers', 0)}")
    print(f"  regime: {regime.get('regime', 'UNKNOWN')}")
    print(f"  pnl snapshots: {len(pnl_history.get('snapshots', []))}")

    # ─── 2. Compute drawdown status ─────────────────────────────────────
    snapshots = pnl_history.get("snapshots", [])
    current_dd, peak_date = compute_drawdown(snapshots)
    dd_multiplier = drawdown_size_multiplier(current_dd)
    print(f"  current_dd={current_dd:.2%}, multiplier={dd_multiplier:.2f}")

    # ─── 3. Determine regime-based gross exposure cap ──────────────────
    regime_str = regime.get("regime", "NEUTRAL")
    max_gross = REGIME_MAX_EXPOSURE.get(regime_str, 0.75)
    print(f"  regime={regime_str}, max_gross_exposure={max_gross:.0%}")

    # ─── 4. Build candidate idea list ───────────────────────────────────
    # Sources: Phase 2B setups (high-conviction filter passed) + Loop 4
    # debate tickers (independent multi-agent analysis).
    # Each idea gets a base conviction:
    #   - Phase 2B: dims_passed normalized + composite score
    #   - Loop 4: stage3 consensus_conviction
    # If a ticker appears in BOTH, take the higher conviction.

    ideas = {}
    for s in asym.get("top_setups", [])[:30]:
        sym = s.get("symbol")
        if not sym:
            continue
        # Base conviction: dims_passed + composite_score
        dims = s.get("dims_passed", 0)
        comp = s.get("composite_score", 50)
        # Map (dims=4, comp=90) → 0.85; (dims=3, comp=70) → 0.65
        conviction = 0.5 + (dims / 4) * 0.2 + (comp / 100 - 0.5) * 0.3
        conviction = max(0.5, min(0.92, conviction))
        ideas[sym] = {
            "symbol": sym,
            "name": s.get("name", sym),
            "sector": s.get("sector", "Unknown"),
            "price": s.get("price"),
            "source": "phase2b",
            "raw_conviction": round(conviction, 3),
            "phase2b_composite": comp,
            "phase2b_dims": dims,
        }

    # Loop 4 watchlist debate tickers
    debate_summary = debate.get("summary", {}) if isinstance(debate, dict) else {}
    for tk, info in debate_summary.items():
        if info.get("consensus_signal") in ("BUY", "STRONG BUY"):
            cv = info.get("consensus_conviction", 5)
            # Map 1-10 conviction → 0.50-0.92 probability
            conviction = 0.50 + (cv / 10) * 0.42
            existing = ideas.get(tk)
            if existing:
                # Update with higher conviction
                if conviction > existing.get("raw_conviction", 0):
                    existing["raw_conviction"] = round(conviction, 3)
                    existing["source"] = existing["source"] + "+loop4"
                    existing["loop4_conviction"] = cv
                    existing["loop4_signal"] = info.get("consensus_signal")
            else:
                ideas[tk] = {
                    "symbol": tk,
                    "name": tk,
                    "sector": "Unknown",
                    "source": "loop4",
                    "raw_conviction": round(conviction, 3),
                    "loop4_conviction": cv,
                    "loop4_signal": info.get("consensus_signal"),
                }

    print(f"  total candidate ideas: {len(ideas)}")

    if not ideas:
        return {"statusCode": 200, "body": json.dumps({
            "warning": "no_ideas_in_pipeline",
            "regime": regime_str,
            "drawdown": current_dd,
        })}

    # ─── 5. Cluster by correlation ──────────────────────────────────────
    stocks_data = report.get("stocks", {})
    returns_by_symbol = {}
    for sym in ideas:
        s = stocks_data.get(sym, {})
        history = s.get("history", [])
        rets = compute_returns(history)
        if rets and len(rets) >= 30:
            returns_by_symbol[sym] = rets

    print(f"  ideas with return data for clustering: {len(returns_by_symbol)}/{len(ideas)}")

    clusters = cluster_by_correlation(list(ideas.keys()), returns_by_symbol)
    # Sort clusters by size, large clusters first
    clusters.sort(key=lambda c: -c["size"])
    print(f"  clusters: {len(clusters)}")
    for c in clusters[:8]:
        print(f"    {c['id'][:25]:25} size={c['size']} avg_corr={c['avg_correlation']}")

    # Map symbol → cluster_id
    sym_to_cluster = {}
    for c in clusters:
        for m in c["members"]:
            sym_to_cluster[m] = c["id"]

    # ─── 6. Size each idea ──────────────────────────────────────────────
    sized = []
    for sym, idea in ideas.items():
        kelly = kelly_size(idea["raw_conviction"])
        # Apply drawdown multiplier
        adjusted = kelly * dd_multiplier
        # Apply regime exposure cap (proportional)
        # If sum of all ideas\' sizes would exceed max_gross, scale all down later
        idea["kelly_raw"] = kelly
        idea["dd_adjusted"] = round(adjusted, 4)
        idea["cluster"] = sym_to_cluster.get(sym, "isolated")
        sized.append(idea)

    # ─── 7. Apply per-cluster caps ──────────────────────────────────────
    # If a cluster\'s total size exceeds MAX_CLUSTER_PCT, scale it down
    cluster_totals = {}
    for idea in sized:
        cid = idea["cluster"]
        cluster_totals[cid] = cluster_totals.get(cid, 0) + idea["dd_adjusted"]

    cluster_scalings = {}
    for cid, total in cluster_totals.items():
        if total > MAX_CLUSTER_PCT:
            cluster_scalings[cid] = MAX_CLUSTER_PCT / total
        else:
            cluster_scalings[cid] = 1.0

    for idea in sized:
        cluster_scale = cluster_scalings.get(idea["cluster"], 1.0)
        idea["after_cluster_cap"] = round(idea["dd_adjusted"] * cluster_scale, 4)

    # ─── 8. Apply gross exposure cap ────────────────────────────────────
    total_post_cluster = sum(i["after_cluster_cap"] for i in sized)
    if total_post_cluster > max_gross:
        gross_scale = max_gross / total_post_cluster
    else:
        gross_scale = 1.0

    for idea in sized:
        idea["recommended_size_pct"] = round(idea["after_cluster_cap"] * gross_scale * 100, 2)

    # ─── 9. Sort by size descending and build reasoning ─────────────────
    sized.sort(key=lambda x: -x.get("recommended_size_pct", 0))

    for idea in sized:
        reasons = []
        if idea.get("phase2b_dims") == 4:
            reasons.append(f"Phase2B 4/4 dims (composite {idea['phase2b_composite']})")
        elif idea.get("phase2b_dims"):
            reasons.append(f"Phase2B {idea['phase2b_dims']}/4 dims (composite {idea['phase2b_composite']})")
        if idea.get("loop4_signal"):
            reasons.append(f"Loop4 debate: {idea['loop4_signal']} conv {idea['loop4_conviction']}/10")
        cluster_size = next((c["size"] for c in clusters if c["id"] == idea["cluster"]), 1)
        if cluster_size > 1:
            reasons.append(f"clustered with {cluster_size-1} others ({idea['cluster']})")
        if dd_multiplier < 1.0:
            reasons.append(f"DD circuit breaker: ×{dd_multiplier}")
        if gross_scale < 1.0:
            reasons.append(f"gross cap: ×{gross_scale:.2f}")
        idea["reasoning"] = " | ".join(reasons)

    # ─── 10. Build warnings list ────────────────────────────────────────
    warnings = []
    if current_dd <= -0.10:
        warnings.append({
            "level": "high",
            "message": f"Hypothetical drawdown {current_dd:.1%} — consider reducing all exposures",
        })
    if regime_str == "RISK_OFF":
        warnings.append({
            "level": "high",
            "message": "Bond market regime detector is RISK_OFF — max equity 50%",
        })
    if total_post_cluster > 1.5:
        warnings.append({
            "level": "medium",
            "message": f"Raw signal sum ({total_post_cluster:.0%}) exceeds 150% — over-signaled, scaled down",
        })
    if not snapshots:
        warnings.append({
            "level": "info",
            "message": "No PnL history yet — drawdown circuit breaker inactive (Loop 2 still warming up)",
        })

    final_total_size = sum(i["recommended_size_pct"] for i in sized)

    snapshot = {
        "as_of": now.isoformat(),
        "v": "1.0",
        "regime": regime_str,
        "regime_strength": regime.get("regime_strength"),
        "max_gross_exposure_pct": round(max_gross * 100, 1),
        "drawdown_status": {
            "current_dd_pct": round(current_dd * 100, 2),
            "peak_date": peak_date,
            "size_multiplier": dd_multiplier,
            "active_trigger": next(
                (f"DD<{t*100:.0f}% → ×{m}" for t, m in DRAWDOWN_TRIGGERS if current_dd <= t),
                "no trigger"
            ),
        },
        "summary": {
            "n_candidate_ideas": len(ideas),
            "n_clusters": len(clusters),
            "total_recommended_size_pct": round(final_total_size, 2),
            "total_pre_caps_pct": round(total_post_cluster * 100, 2),
        },
        "constraints_applied": {
            "max_single_position_pct": MAX_SINGLE_POSITION_PCT * 100,
            "max_cluster_pct": MAX_CLUSTER_PCT * 100,
            "max_gross_exposure_pct": max_gross * 100,
            "kelly_fraction": KELLY_FRACTION,
        },
        "clusters": clusters,
        "sized_recommendations": sized,
        "warnings": warnings,
    }

    put_s3_json("risk/recommendations.json", snapshot)

    print(f"  total recommended size: {final_total_size:.2f}%")
    print(f"  warnings: {len(warnings)}")

    return {
        "statusCode": 200,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps({
            "regime": regime_str,
            "max_gross_exposure_pct": round(max_gross * 100, 1),
            "current_drawdown_pct": round(current_dd * 100, 2),
            "drawdown_multiplier": dd_multiplier,
            "n_ideas": len(ideas),
            "n_clusters": len(clusters),
            "total_size_pct": round(final_total_size, 2),
            "n_warnings": len(warnings),
            "top_5_sized": [
                {"symbol": i["symbol"], "size_pct": i["recommended_size_pct"]}
                for i in sized[:5]
            ],
        }),
    }
