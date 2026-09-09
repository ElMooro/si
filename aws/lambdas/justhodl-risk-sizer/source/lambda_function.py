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
import math
from capital_contract import authority_view, capital_book_view, fresh_timestamp, finite
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
# audit 2026-09-08 FR-04: freshness SLAs for the binding authority (hourly engine) and the raw gate (daily)
AUTHORITY_SLA_H = 24.0
GATE_SLA_H = 36.0


def age_hours(ts):
    try:
        t = datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
        if t.tzinfo is None:
            t = t.replace(tzinfo=timezone.utc)
        return round((datetime.now(timezone.utc) - t).total_seconds() / 3600.0, 2)
    except Exception:
        return None


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


def cluster_by_correlation(symbols, returns_by_symbol, sector_by_symbol=None, threshold=CLUSTER_CORRELATION_THRESHOLD):
    """Cluster symbols by 60-day return correlation when data exists, else by sector.

    For symbols with sufficient return data we use the original 0.65 correlation
    threshold (precise but data-dependent). For symbols WITHOUT return data —
    most Phase 2B output, since data/report.json only carries ~80 major ETFs/
    names and the screener has 503 — we fall back to sector grouping. INCY +
    RMD = Healthcare → 1 cluster, etc. Less precise but vastly better than
    treating every name as its own cluster (which makes per-cluster caps useless).
    """
    if sector_by_symbol is None:
        sector_by_symbol = {}
    clusters = []
    assigned = set()

    # Pass 1: correlation-based clusters for symbols with return data
    for sym in symbols:
        if sym in assigned or sym not in returns_by_symbol:
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
            "id": f"corr_{cluster_members[0]}",
            "method": "correlation",
            "members": sorted(cluster_members),
            "avg_correlation": round(statistics.mean(cluster_corrs), 3) if cluster_corrs else 0,
            "size": len(cluster_members),
        })

    # Pass 2: sector-based clusters for the remainder
    by_sector = {}
    for sym in symbols:
        if sym in assigned:
            continue
        sector = sector_by_symbol.get(sym, "Unknown")
        by_sector.setdefault(sector, []).append(sym)

    for sector, members in by_sector.items():
        if len(members) == 1:
            # Single-member sector → still mark as sector cluster, not isolated
            clusters.append({
                "id": f"sector_{sector.lower().replace(' ', '_')}",
                "method": "sector_single",
                "members": members,
                "avg_correlation": 0,
                "size": 1,
                "sector": sector,
            })
        else:
            clusters.append({
                "id": f"sector_{sector.lower().replace(' ', '_')}",
                "method": "sector",
                "members": sorted(members),
                "avg_correlation": 0,  # sector grouping doesn\'t compute corr
                "size": len(members),
                "sector": sector,
            })
        for m in members:
            assigned.add(m)

    return clusters


def _finite_nonneg(v):
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    if f != f or f in (float("inf"), float("-inf")) or f < 0:
        return None
    return f


def compute_drawdown(pnl_history_snapshots):
    """Compute current drawdown as % from peak in pnl-history.json.

    audit 2026-09-08 FR-05: a NAV of 0 is a real observation (total loss), not a
    missing one -- `is not None` + finite/non-negative, never truthiness. When the
    history cannot support a drawdown read the result is (None, None) = UNKNOWN,
    and the caller must HOLD rather than assume 0% drawdown.
    """
    if not pnl_history_snapshots:
        return None, None
    # snapshot shape: {as_of, khalid_strategy_value_usd, ...}
    values = []
    for snap in pnl_history_snapshots:
        v = _finite_nonneg((snap or {}).get("khalid_strategy_value_usd"))
        if v is not None and (snap or {}).get("as_of"):
            values.append((snap.get("as_of"), v))
    if len(values) < 2:
        return None, None
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


def binding_trigger(current_dd):
    """The DEEPEST breached drawdown trigger (the one that actually binds), or None.
    audit 2026-09-08 FR-05: the page used to describe the FIRST breached rule (-5% / x0.75)
    even at -15% where the multiplier is zero."""
    if current_dd is None:
        return None
    hit = None
    for trigger_dd, mult in DRAWDOWN_TRIGGERS:
        if current_dd <= trigger_dd:
            hit = (trigger_dd, mult)
    return hit


def drawdown_size_multiplier(current_dd):
    """Map current drawdown % to a size scaling factor. UNKNOWN drawdown -> 0 (hold)."""
    if current_dd is None:
        return 0.0
    hit = binding_trigger(current_dd)
    return hit[1] if hit else 1.0


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
    # audit 2026-09-08 FR-04: portfolio/state.json was never written by any engine (dead read);
    # the reconciled book is portfolio/snapshot.json (justhodl-portfolio-snapshot).
    book = get_s3_json("portfolio/snapshot.json", {})
    report = get_s3_json("data/report.json", {})
    khalid_risk = get_s3_json("data/khalid-risk.json", {})       # the ONE capital authority
    risk_gate = get_s3_json("data/risk-gate.json", {})           # brain sizing multiplier

    print(f"  asymmetric setups: {len(asym.get('top_setups', []))}")
    print(f"  watchlist debate tickers: {debate.get('n_tickers', 0)}")
    print(f"  regime: {regime.get('regime', 'UNKNOWN')}")

    # ─── 2. Compute drawdown status ─────────────────────────────────────
    book_view = capital_book_view(book, now)
    snapshots = book_view["nav_history"]
    current_dd, peak_date = compute_drawdown(snapshots)
    dd_multiplier = drawdown_size_multiplier(current_dd)
    hold_reasons = list(book_view["errors"])
    if current_dd is None:
        hold_reasons.append("drawdown brake has no trusted NAV history (need >= 2 finite snapshots) -- HOLD")
        print("  current_dd=UNKNOWN -> multiplier 0 (hold)")
    else:
        print(f"  current_dd={current_dd:.2%}, multiplier={dd_multiplier:.2f}")

    # ─── 3. Gross exposure cap: regime x AUTHORITY x book ───────────────
    regime_str = regime.get("regime")
    if regime_str not in REGIME_MAX_EXPOSURE:
        hold_reasons.append("regime/current.json missing or unknown (%r) -- no default to NEUTRAL/75%%" % regime_str)
        regime_str = regime_str or "UNKNOWN"
    max_gross = REGIME_MAX_EXPOSURE.get(regime_str, 0.0)
    # audit 2026-09-08 FR-04: data/khalid-risk.json is the binding permission (same artifact the
    # homepage and Katlin obey). Missing/stale/invalid authority = HOLD, never a default.
    authority = authority_view(khalid_risk, now)
    if authority["status"] == "FRESH":
        max_gross = min(max_gross, authority["exposure_cap_pct"] / 100.0)
        if authority["allows_new_entries"] is False:
            hold_reasons.append("capital authority forbids new entries (%s)" % authority["mode"])
    else:
        hold_reasons.append("capital authority khalid-risk is %s%s -- HOLD" % (authority["status"], (" (age %sh > %sh)" % (authority["age_h"], AUTHORITY_SLA_H)) if authority["age_h"] is not None else ""))
    gate_mult = None
    try:
        gm = float(risk_gate.get("sizing_multiplier"))
        if 0.0 <= gm <= 1.5:
            gate_mult = min(gm, 1.0)          # a valid ZERO is a zero
    except (TypeError, ValueError):
        gate_mult = None
    gate_age = age_hours(risk_gate.get("generated_at"))
    if gate_mult is None or not fresh_timestamp(risk_gate.get("generated_at"), now, GATE_SLA_H) or risk_gate.get("engine") != "justhodl-risk-gate" or risk_gate.get("posture") not in {"RISK_ON", "NEUTRAL", "RISK_OFF", "SEVERE"}:
        hold_reasons.append("risk-gate sizing multiplier unusable (mult=%s age=%sh)" % (gate_mult, gate_age))
    # the existing book counts against the cap: sizing is for NEW entries only
    capital_book = book_view["contract"]
    book_positions = capital_book.get("positions") if isinstance(capital_book.get("positions"), list) else []
    book_value = book_view["equity_nav"]
    book_weights = book_view["gross_weights"]
    committed_weights = dict(book_weights)
    for symbol, weight in book_view["order_weights"].items():
        committed_weights[symbol] = committed_weights.get(symbol, 0.0) + weight
    current_gross = sum(committed_weights.values())
    available_gross = max(0.0, max_gross - current_gross)
    print(f"  regime={regime_str}, authority={authority['status']}/{authority['exposure_cap_pct']}, max_gross={max_gross:.0%}, book_gross={current_gross:.0%}, available={available_gross:.0%}")
    if available_gross <= 0:
        hold_reasons.append("existing positions and orders exhaust the gross exposure allowance")
    if dd_multiplier <= 0 or gate_mult == 0 or max_gross <= 0:
        hold_reasons.append("binding drawdown/gate/capital constraint permits no new risk")
    entries_allowed = not hold_reasons

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
        # audit 2026-09-08 FR-04: an empty pipeline must REPLACE the previous actionable book, not leave it in place
        empty = {"engine": "justhodl-risk-sizer", "schema_version": "3.0", "as_of": now.isoformat(), "expires_at": min(now + timedelta(hours=1), datetime.fromisoformat(authority["expires_at"]) if authority.get("expires_at") else now).isoformat(), "v": "3.0", "status": "NO_IDEAS", "regime": regime_str, "entries_allowed": entries_allowed,
                 "authority": authority, "hold_reasons": hold_reasons, "max_gross_exposure_pct": round(max_gross * 100, 1),
                 "drawdown_status": {"current_dd_pct": round(current_dd * 100, 2) if current_dd is not None else None, "status": "UNKNOWN" if current_dd is None else "OK",
                                     "peak_date": peak_date, "size_multiplier": dd_multiplier},
                 "sized_recommendations": [], "clusters": {}, "summary": {"n_candidate_ideas": 0, "n_clusters": 0, "total_recommended_size_pct": 0.0},
                 "warnings": [{"level": "info", "message": "no candidate ideas in the pipeline this run"}]}
        put_s3_json("risk/recommendations.json", empty)
        put_s3_json("data/risk-sizer.json", empty)
        return {"statusCode": 200, "body": json.dumps({"warning": "no_ideas_in_pipeline", "regime": regime_str, "drawdown": current_dd, "status": "NO_IDEAS"})}

    # ─── 5. Cluster by correlation ──────────────────────────────────────
    stocks_data = report.get("stocks", {})
    returns_by_symbol = {}
    for sym in set(ideas) | set(committed_weights):
        s = stocks_data.get(sym, {})
        history = s.get("history", [])
        rets = compute_returns(history)
        if rets and len(rets) >= 30:
            returns_by_symbol[sym] = rets

    print(f"  ideas with return data for clustering: {len(returns_by_symbol)}/{len(ideas)}")

    sector_by_symbol = {**book_view["sector_by_symbol"], **{sym: idea.get("sector") or "UNKNOWN" for sym, idea in ideas.items()}}
    clusters = cluster_by_correlation(sorted(set(ideas) | set(committed_weights)), returns_by_symbol, sector_by_symbol)
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
    # Step 151: composite-score-weighted sizing. Kelly produces base
    # sizes from raw_conviction (narrow range). We multiply by a weight
    # derived from composite_score (or raw_conviction as fallback) so
    # high-conviction names get larger allocations BEFORE caps bite.
    quality_signals = []
    for sym, idea in ideas.items():
        # Prefer phase2b_composite (0-100 scale, range usually 70-95).
        # Fallback: raw_conviction (0.5-0.92 scale → multiply by 100).
        qs = idea.get("phase2b_composite")
        if qs is None or qs <= 0:
            qs = idea.get("raw_conviction", 0.65) * 100
        quality_signals.append((sym, qs))
    avg_qs = (sum(qs for _, qs in quality_signals) / len(quality_signals)) if quality_signals else 1.0
    weight_by_sym = {sym: (qs / avg_qs) if avg_qs > 0 else 1.0 for sym, qs in quality_signals}
    # Cap weight range at [0.6x, 1.6x] to prevent extreme tilts
    weight_by_sym = {sym: max(0.6, min(1.6, w)) for sym, w in weight_by_sym.items()}

    sized = []
    for sym, idea in ideas.items():
        kelly = kelly_size(idea["raw_conviction"])
        weight = weight_by_sym.get(sym, 1.0)
        weighted_kelly = kelly * weight
        # audit 2026-09-08 FR-03: the published single-name limit binds AFTER the quality tilt
        # (it used to be applied inside kelly_size, before a x1.6 multiplier could breach it)
        capped = min(weighted_kelly, MAX_SINGLE_POSITION_PCT)
        # Apply drawdown multiplier and the brain risk-gate sizing multiplier (both <= 1)
        adjusted = capped * dd_multiplier * (gate_mult if gate_mult is not None else 0.0)
        # FR-04: sizing is for NEW entries -- an existing holding only gets the increment up to target
        held = committed_weights.get(str(sym).upper(), 0.0)
        adjusted = max(0.0, adjusted - held)
        if book_view["signed_weights"].get(str(sym).upper(), 0.0) < 0:
            adjusted = 0.0
            idea["blocked_reason"] = "Existing short requires an explicit cover/rebalance decision; this engine proposes new long risk only"
        idea["kelly_raw"] = round(kelly, 4)
        idea["quality_weight"] = round(weight, 3)
        idea["single_name_capped"] = round(capped, 4)
        idea["currently_held_pct"] = round(held * 100, 2)
        idea["dd_adjusted"] = round(adjusted, 4)
        idea["cluster"] = sym_to_cluster.get(sym, "isolated")
        sized.append(idea)

    # ─── 7. Apply per-cluster caps ──────────────────────────────────────
    # If a cluster\'s total size exceeds MAX_CLUSTER_PCT, scale it down
    cluster_totals = {}
    for idea in sized:
        cid = idea["cluster"]
        cluster_totals[cid] = cluster_totals.get(cid, 0) + idea["dd_adjusted"]

    cluster_held = {}
    for symbol, weight in committed_weights.items():
        cid = sym_to_cluster.get(symbol, "isolated")
        cluster_held[cid] = cluster_held.get(cid, 0.0) + weight
    cluster_scalings = {}
    for cid, total in cluster_totals.items():
        remaining = max(0.0, MAX_CLUSTER_PCT - cluster_held.get(cid, 0.0))
        if total > remaining:
            cluster_scalings[cid] = remaining / total
        else:
            cluster_scalings[cid] = 1.0

    for idea in sized:
        cluster_scale = cluster_scalings.get(idea["cluster"], 1.0)
        idea["after_cluster_cap"] = math.floor(idea["dd_adjusted"] * cluster_scale * 100000000 + 1e-8) / 100000000

    # ─── 8. Apply gross exposure cap (on the AVAILABLE gross after the existing book) ────
    total_post_cluster = sum(i["after_cluster_cap"] for i in sized)
    if total_post_cluster > available_gross:
        gross_scale = (available_gross / total_post_cluster) if total_post_cluster > 0 else 0.0
    else:
        gross_scale = 1.0

    for idea in sized:
        idea["recommended_size_pct"] = math.floor(idea["after_cluster_cap"] * gross_scale * 10000 + 1e-8) / 100
        if not entries_allowed:
            idea["recommended_size_pct"] = 0.0
            idea["blocked_reason"] = "; ".join(hold_reasons)

    # ─── 8b. FINAL constraint assertion after rounding (FR-03 acceptance) ──────────────
    final_check = {"single_name_ok": True, "cluster_ok": True, "gross_ok": True, "clamped": []}
    for idea in sized:
        if idea["recommended_size_pct"] > MAX_SINGLE_POSITION_PCT * 100 + 1e-9:
            final_check["single_name_ok"] = False
            final_check["clamped"].append(idea["symbol"])
            idea["recommended_size_pct"] = round(MAX_SINGLE_POSITION_PCT * 100, 2)
    cl_tot = {}
    for idea in sized:
        cl_tot[idea["cluster"]] = cl_tot.get(idea["cluster"], 0.0) + idea["recommended_size_pct"]
    if any(v > MAX_CLUSTER_PCT * 100 + 0.01 for v in cl_tot.values()):
        final_check["cluster_ok"] = False
    if sum(i["recommended_size_pct"] for i in sized) > available_gross * 100 + 0.01:
        final_check["gross_ok"] = False

    # Any post-rounding invariant failure invalidates the recommendation artifact.
    if not all(final_check[k] for k in ("single_name_ok", "cluster_ok", "gross_ok")):
        entries_allowed = False
        hold_reasons.append("post-rounding allocation constraint failed")
        for idea in sized:
            idea["recommended_size_pct"] = 0.0
            idea["blocked_reason"] = hold_reasons[-1]

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
    if current_dd is None:
        warnings.append({"level": "high", "message": "Drawdown UNKNOWN -- NAV history has fewer than 2 finite snapshots; sizing is held at zero until the brake can read"})
    elif current_dd <= -0.10:
        warnings.append({
            "level": "high",
            "message": f"Hypothetical drawdown {current_dd:.1%} — consider reducing all exposures",
        })
    for hr in hold_reasons:
        warnings.append({"level": "high", "message": hr})
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
        "engine": "justhodl-risk-sizer", "schema_version": "3.0",
        "as_of": now.isoformat(),
        "expires_at": min(now + timedelta(hours=1), datetime.fromisoformat(authority["expires_at"]) if authority.get("expires_at") else now).isoformat(),
        "v": "3.0",
        "model_estimates": {"method": "heuristic conviction with symmetric-payoff fractional Kelly", "calibration_status": "UNVALIDATED", "note": "Candidate conviction is not a calibrated win probability; use sized research only after independent review."},
        "status": "ENTRIES_BLOCKED" if not entries_allowed else "OK",
        "entries_allowed": entries_allowed,
        "hold_reasons": hold_reasons,
        "recommendation_semantics": "Incremental long exposure as percent of reconciled account equity NAV; absolute positions and remaining orders reserve name, cluster and gross capacity. Existing shorts require a separate cover/rebalance decision.",
        "authority": authority,
        "risk_gate": {"sizing_multiplier": gate_mult, "age_h": gate_age, "sla_h": GATE_SLA_H},
        "book": {"source": "portfolio/snapshot.json#capital_book", "schema_version": capital_book.get("schema_version"), "status": book_view["status"],
                 "as_of": capital_book.get("as_of"), "reconciled_at": capital_book.get("reconciled_at"), "book_id": capital_book.get("book_id"), "account_id": capital_book.get("account_id"),
                 "currency": capital_book.get("currency"), "n_positions": len(book_positions), "equity_nav": book_value, "cash": capital_book.get("cash"), "liabilities": capital_book.get("liabilities"),
                 "gross_exposure": book_view["gross_exposure"], "net_exposure": book_view["net_exposure"], "reserved_order_exposure": book_view["reserved_order_exposure"],
                 "gross_pct": round(current_gross * 100, 4), "available_gross_pct": round(available_gross * 100, 4), "errors": book_view["errors"]},
        "final_constraint_check": final_check,
        "regime": regime_str,
        "regime_strength": regime.get("regime_strength"),
        "max_gross_exposure_pct": round(max_gross * 100, 1),
        "drawdown_status": {
            "current_dd_pct": round(current_dd * 100, 2) if current_dd is not None else None,
            "status": "UNKNOWN" if current_dd is None else "OK",
            "peak_date": peak_date,
            "size_multiplier": dd_multiplier,
            "active_trigger": (f"DD<{binding_trigger(current_dd)[0]*100:.0f}% → ×{binding_trigger(current_dd)[1]}" if binding_trigger(current_dd) else ("unknown -- hold" if current_dd is None else "no trigger")),
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
            "max_gross_exposure_pct": round(max_gross * 100, 2),
            "available_gross_pct": round(available_gross * 100, 2),
            "kelly_fraction": KELLY_FRACTION,
            "single_name_cap_applied_after": "quality tilt, drawdown and risk-gate multipliers, cluster and gross scaling, rounding",
        },
        "clusters": clusters,
        "sized_recommendations": sized,
        "warnings": warnings,
    }

    put_s3_json("risk/recommendations.json", snapshot)
    # Mirror to canonical data/ path so consumers using either naming convention work.
    put_s3_json("data/risk-sizer.json", snapshot)

    print(f"  total recommended size: {final_total_size:.2f}%")
    print(f"  warnings: {len(warnings)}")

    return {
        "statusCode": 200,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps({
            "regime": regime_str,
            "max_gross_exposure_pct": round(max_gross * 100, 1),
            "current_drawdown_pct": round(current_dd * 100, 2) if current_dd is not None else None,
            "entries_allowed": entries_allowed,
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
