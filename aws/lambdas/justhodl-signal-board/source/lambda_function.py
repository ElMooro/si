"""
justhodl-signal-board — Unified Cross-Asset Signal Board

═══════════════════════════════════════════════════════════════════════
WHY THIS EXISTS
───────────────
The platform grew to ~268 engines but the newest ones — fundamentals,
construction-housing, crypto-narratives, short-pressure, mean-reversion,
pm-decision, cross-asset-rv — were not fused into any synthesis layer.
Data computed, never read.

A hedge fund solves this with a SIGNAL STORE: every model writes its
current read into one board, and the desk reads cross-asset posture
from a single place — instead of point-to-point spaghetti into every
scoring engine (which would destabilise scores the desk already trusts).

This engine reads each engine's sidecar, normalises its headline into a
5-state signal (-2 strong risk-off … +2 strong risk-on), aggregates a
composite posture + per-category sub-postures, and flags any stale feed.

OUTPUT: data/signal-board.json   SCHEDULE: every 3h
═══════════════════════════════════════════════════════════════════════
"""
import json
import os
import time
from datetime import datetime, timezone, timedelta

import boto3

S3_BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
OUT_KEY = "data/signal-board.json"
STALE_HOURS = 40

s3 = boto3.client("s3", region_name="us-east-1")

SIG_LABEL = {-2: "STRONG RISK-OFF", -1: "RISK-OFF", 0: "NEUTRAL",
             1: "RISK-ON", 2: "STRONG RISK-ON"}


def read_json(key):
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
        return json.loads(obj["Body"].read()), obj["LastModified"]
    except Exception:
        return None, None


def clamp(v):
    return max(-2, min(2, int(round(v))))


# ── per-engine normalisers — each returns (signal:-2..2, read:str) ──
def n_pm_decision(d):
    pw = (d.get("posture_word") or "").upper()
    m = {"AGGRESSIVE": 2, "CONSTRUCTIVE": 1, "NEUTRAL": 0,
         "CAUTIOUS": -1, "DEFENSIVE": -2}
    return m.get(pw, 0), f"Desk posture {pw or 'n/a'}"


def n_cross_asset_rv(d):
    st = (d.get("rv_state") or "").upper()
    m = {"ALIGNED": 1, "STRETCHED": 0, "DISLOCATION_PRESENT": -1}
    return m.get(st, 0), f"RV {st.replace('_', ' ').lower() or 'n/a'}"


def n_fundamentals(d):
    s = d.get("summary") or {}
    uv, ov = s.get("n_undervalued") or 0, s.get("n_overvalued") or 0
    sig = 1 if uv > ov * 1.5 else -1 if ov > uv * 1.5 else 0
    return sig, f"{uv} undervalued vs {ov} overvalued (DCF)"


def n_construction_housing(d):
    rg = (d.get("regime") or "").upper()
    m = {"EXPANSION": 2, "RECOVERY": 1, "SLOWING": -1, "CONTRACTION": -2}
    return m.get(rg, 0), f"Housing cycle {rg or 'n/a'}"


def n_crypto_narratives(d):
    st = (d.get("stance") or "").upper()
    m = {"RISK-ON ROTATION": 2, "SELECTIVE": 0, "RISK-OFF": -2}
    br = d.get("narrative_breadth_pct")
    return m.get(st, 0), f"Crypto {st or 'n/a'} ({br}% breadth)"


def n_short_pressure(d):
    b = d.get("n_pressure_building") or 0
    c = d.get("n_shorts_covering") or 0
    sig = 1 if c > b * 1.5 else -1 if b > c * 1.5 else 0
    return sig, f"{b} building short pressure, {c} covering"


def n_mean_reversion(d):
    ch = d.get("n_cheap_vs_history") or 0
    ri = d.get("n_rich_vs_history") or 0
    sig = 1 if ch > ri * 1.3 else -1 if ri > ch * 1.3 else 0
    return sig, f"{ch} cheap vs {ri} rich on own multiple history"


def n_canary_grid(d):
    band = (d.get("band") or "").upper()
    m = {"CALM": 1, "WATCH": 0, "ELEVATED": -1, "WARNING": -2, "CRITICAL": -2}
    lvl = d.get("early_warning_level")
    return m.get(band, 0), f"Global early-warning {band or 'n/a'} ({lvl}/100)"


def n_dollar_radar(d):
    # dollar_pressure -100 (DUMP) .. +100 (PUMP). A dollar PUMP (squeeze) is
    # risk-off; a dollar DUMP (a liquidity flood) is risk-on.
    p = d.get("dollar_pressure")
    if not isinstance(p, (int, float)):
        return 0, "Dollar pressure n/a"
    reg = d.get("regime") or "n/a"
    sig = (-2 if p >= 50 else -1 if p >= 20 else
           2 if p <= -50 else 1 if p <= -20 else 0)
    return sig, f"Dollar {reg} (pressure {p:+.0f})"


def n_eurodollar_plumbing(d):
    # plumbing_health 0-100 (FUNCTIONING=high). A seizing offshore-USD funding
    # system is acute, broad risk-off. verdict FUNCTIONING/MILD STRAIN/STRAINED/SEIZING.
    h = d.get("plumbing_health")
    v = d.get("verdict") or "n/a"
    if not isinstance(h, (int, float)):
        return 0, "Eurodollar plumbing n/a"
    sig = 1 if h >= 78 else 0 if h >= 60 else -1 if h >= 45 else -2
    return sig, f"Eurodollar funding {v} (health {h}/100)"


def n_global_stress(d):
    # global_stress_index 0-100; high = world equity/bond stress = risk-off.
    gsi = d.get("global_stress_index")
    lvl = d.get("global_stress_level") or "n/a"
    if not isinstance(gsi, (int, float)):
        return 0, "Global stress n/a"
    sig = -2 if gsi >= 75 else -1 if gsi >= 55 else 1 if gsi < 32 else 0
    return sig, f"Global market stress {lvl} ({gsi}/100)"


def n_auction_crisis(d):
    """Treasury auction crisis detector (Waves A-D, 2026-06).
    composite_score 0-100; regime CALM/WATCH/ELEVATED/ACUTE_STRESS.
    High composite = auction stress = bond market dysfunction = risk-off."""
    composite = d.get("composite_score")
    regime    = (d.get("regime") or "").upper()
    n_recent  = d.get("n_recent_auctions_14d") or 0
    if not isinstance(composite, (int, float)):
        return 0, "Auction crisis n/a"
    # Regime-anchored signal mapping (composite-aligned thresholds)
    regime_map = {"ACUTE_STRESS": -2, "ELEVATED": -2, "WATCH": -1, "CALM": 1}
    sig = regime_map.get(regime, 0)
    # Pull a representative bit of tail-risk context if present
    tail = d.get("tail_risk") or {}
    p_esc = ((tail.get("p_regime_escalation_14d") or {}).get("probability")) or 0
    tail_note = f", esc-risk {p_esc:.0f}%/14d" if p_esc >= 25 else ""
    return sig, f"Auctions {regime or 'n/a'} ({composite:.1f}/100, {n_recent} in 14d{tail_note})"


# ── 10-Edge institutional roadmap normalisers ──────────────────────────
def n_vix_backwardation(d):
    """Edge #1: VIX backwardation post-capitulation trigger."""
    state = (d.get("state") or "").upper()
    m = {"FIRED": 2, "ARMED": 1, "WARM": 0, "NULL": 0, "COOLDOWN": 0}
    return m.get(state, 0), f"VIX-back trigger {state or 'n/a'}"


def n_insider_buys(d):
    """Edge #2: Enriched insider open-market BUY clusters."""
    # Prefer canonical `state` field; fall back to count-based heuristic
    state = (d.get("state") or "").upper()
    state_map = {"FRESH_HIGH_CONVICTION": 2, "ELEVATED": 1,
                 "NORMAL": 0, "QUIET": 0}
    if state in state_map:
        sig = state_map[state]
        s = d.get("summary") or {}
        n_high = s.get("high_conviction") or 0
        n_enriched = s.get("enriched_returned") or 0
        return sig, f"Insider buys {state} ({n_high} high-conv / {n_enriched} enriched)"
    # Legacy fallback for older outputs
    s = d.get("summary") or {}
    n_clusters = (s.get("n_clusters_today") or s.get("n_clusters")
                  or s.get("enriched_returned")
                  or d.get("n_clusters_today") or d.get("n_clusters") or 0)
    avg_sig = (s.get("avg_cluster_signal") or d.get("avg_cluster_signal") or 0)
    sig = 2 if n_clusters >= 5 and avg_sig > 60 else (
        1 if n_clusters >= 2 else 0)
    return sig, f"{n_clusters} insider buy-clusters (avg signal {avg_sig})"


def n_breadth_thrust(d):
    """Edge #3: Zweig breadth-thrust capitulation reversal."""
    state = (d.get("state") or d.get("zweig_state") or "").upper()
    m = {"FIRED": 2, "ARMED": 1, "NULL": 0, "COOLDOWN": 0}
    return m.get(state, 0), f"Zweig breadth-thrust {state or 'n/a'}"


def n_vol_target_unwind(d):
    """Edge #4: Vol-target fund mechanical unwind detector."""
    state = (d.get("state") or "").upper()
    m = {"UNWIND_ACTIVE": -2, "ELEVATED_RISK": -1, "ARMED": -1,
         "WARM": 0, "QUIET": 0, "POST_UNWIND_REBOUND": 1, "NULL": 0}
    return m.get(state, 0), f"Vol-target {state or 'n/a'}"


def n_russell_recon(d):
    """Edge #5: Russell/S&P reconstitution front-run."""
    phase = (d.get("calendar_phase") or "").upper()
    # Event-driven not directional broad signal; only flag during active windows
    m = {"DORMANT": 0, "EARLY_MONITORING": 0, "POST_RANK_SNAPSHOT": 0,
         "PRE_ANNOUNCEMENT": 1, "ANNOUNCED_HIGH_CONVICTION": 1,
         "FINAL_WEEK": 1, "POST_REBAL_FADE": -1}
    return m.get(phase, 0), f"Russell-recon {phase or 'n/a'}"


def n_buyback_scanner(d):
    """Edge #6: Buyback authorization scanner."""
    state = (d.get("state") or "").upper()
    m = {"CROSS_CONFIRMED_HOT": 2, "MEGA_AUTH_WAVE": 2,
         "DRIFT_HUNTING": 1, "ELEVATED": 1, "NORMAL": 0,
         "QUIET": 0, "LOW_ACTIVITY": 0, "WAVE": 1,
         "MEGA_AUTH_DETECTED": 1}
    n_fresh = d.get("n_fresh_last_7d") or 0
    return m.get(state, 0), f"Buyback auth {state or 'n/a'} ({n_fresh} fresh 7d)"


def n_stablecoin_flow(d):
    """Edge #7: Stablecoin mint flow tracker."""
    state = (d.get("state") or "").upper()
    m = {"PARABOLIC_MINT": 2, "EXPLOSIVE_MINT": 2,
         "EXPANDING": 1, "FLAT": 0, "CONTRACTING": -2}
    delta = d.get("delta_30d_usd") or (d.get("aggregate") or {}).get("delta_30d_usd")
    delta_str = ""
    if isinstance(delta, (int, float)):
        delta_str = f"  ({delta/1e9:+.1f}B 30d)"
    return m.get(state, 0), f"Stablecoin {state or 'n/a'}{delta_str}"


def n_opex_calendar(d):
    """Edge #8: OPEX gamma-pinning calendar."""
    state = (d.get("state") or "").upper()
    m = {"POST_OPEX": 1, "OPEX_WEEK": 0, "OPEX_DAY": 0, "BUILDUP": 0,
         "QUIET": 0, "QUAD_WITCHING": -1, "NORMAL": 0}
    days = d.get("days_to_next_opex")
    days_str = f" ({days}d to next OPEX)" if days is not None else ""
    return m.get(state, 0), f"OPEX {state or 'n/a'}{days_str}"


def n_activist_13d(d):
    """Edge #9: Activist 13D fresh-filing alert."""
    state = (d.get("state") or "").upper()
    m = {"FRESH_TIER_A": 2, "TIER_A_HOT": 2, "MULTI_ACTIVIST": 2,
         "WAVE": 1, "NEW_FILING": 1, "ACTIVE": 1, "QUIET": 0}
    n_fresh = (d.get("current_readings") or {}).get("fresh_tier_a_count") or 0
    return m.get(state, 0), f"Activist 13D {state or 'n/a'} ({n_fresh} fresh)"


def n_rv_iv_scanner(d):
    """Edge #10: RV-IV variance risk premium + implied dispersion."""
    state = (d.get("state") or "").upper()
    # VRP_RICH = short-vol carry edge => mild risk-on
    # VRP_CHEAP = vol cheap relative to realized => realised vol coming => risk-off
    # DISPERSION_RICH = single-name dispersion vs index => stock-pickers' market
    # DISPERSION_CHEAP = inverse, factor correlation regime
    m = {"VRP_RICH": 1, "VRP_CHEAP": -1, "DISPERSION_RICH": 1,
         "DISPERSION_CHEAP": 0, "NORMAL": 0}
    summ = d.get("summary") or {}
    vrp = summ.get("vrp_spy_vol_pts")
    vrp_str = f" (SPY VRP {vrp:+.1f})" if isinstance(vrp, (int, float)) else ""
    return m.get(state, 0), f"VRP/Dispersion {state or 'n/a'}{vrp_str}"


def n_crypto_opportunities(d):
    """Crypto opportunity scanner: retail-actionable small-cap setups.

    OPPORTUNITY_RICH => +2 (cross-confirmed convergence regime, risk-on for crypto small caps)
    ACTIVE          => +1 (multiple actionable signals)
    NORMAL          =>  0 (modest setup density)
    QUIET           => -1 (no setups, often coincides with broader crypto risk-off)
    """
    state = (d.get("state") or "").upper()
    m = {"OPPORTUNITY_RICH": 2, "ACTIVE": 1, "NORMAL": 0, "QUIET": -1}
    summ = d.get("summary") or {}
    n_conv = summ.get("n_convergence") or 0
    n_total = (summ.get("n_volume_surge") or 0) + (summ.get("n_social_velocity") or 0) + (summ.get("n_stable_inflows") or 0)
    return m.get(state, 0), f"Crypto-opps {state or 'n/a'} (conv={n_conv}, picks={n_total})"


# ===== Retail-Edges Engines (7 new engines, 2026-05-20) =====

def n_earnings_iv_crush(d):
    """Pre-earnings IV vs realized. Both RICH and CHEAP regimes are edges (opposite directions)."""
    state = (d.get("state") or "").upper()
    m = {"RICH_REGIME": 1, "CHEAP_REGIME": 1, "MIXED": 0, "QUIET": 0}
    summ = d.get("summary") or {}
    n_rich = summ.get("n_rich") or 0
    n_cheap = summ.get("n_cheap") or 0
    return m.get(state, 0), f"IV-crush {state or 'n/a'} (rich={n_rich}, cheap={n_cheap})"


def n_stealth_accumulation(d):
    """4-signal smart-money convergence. RICH = high conviction = +2."""
    state = (d.get("state") or "").upper()
    m = {"STEALTH_RICH": 2, "ACTIVE": 1, "NORMAL": 0, "QUIET": 0}
    summ = d.get("summary") or {}
    n_conv = summ.get("n_convergence_2plus") or 0
    n_3 = summ.get("n_convergence_3plus") or 0
    return m.get(state, 0), f"Stealth {state or 'n/a'} (conv={n_conv}, 3+sig={n_3})"


def n_failed_pattern_reversal(d):
    """Failed-breakdown longs = bullish (+1), failed-breakouts shorts = bearish (-1)."""
    state = (d.get("state") or "").upper()
    m = {"BULLISH_REVERSAL_RICH": 1, "BEARISH_REVERSAL_RICH": -1,
         "ACTIVE": 0, "NORMAL": 0, "QUIET": 0}
    summ = d.get("summary") or {}
    n_long = summ.get("n_failed_breakdowns_long") or 0
    n_short = summ.get("n_failed_breakouts_short") or 0
    return m.get(state, 0), f"Failed-patterns {state or 'n/a'} (L={n_long}, S={n_short})"


def n_squeeze_pretrigger(d):
    """Short squeezes = bullish setups. RICH=+1, ACTIVE=+1, NORMAL/QUIET=0."""
    state = (d.get("state") or "").upper()
    m = {"SQUEEZE_RICH": 1, "ACTIVE": 1, "NORMAL": 0, "QUIET": 0}
    summ = d.get("summary") or {}
    n_imm = summ.get("n_imminent_5of5") or 0
    n_pre = summ.get("n_pretrigger_4of5") or 0
    return m.get(state, 0), f"Squeeze {state or 'n/a'} (5/5={n_imm}, 4/5={n_pre})"


def n_catalyst_skew_premove(d):
    """BULL_SKEW = bullish positioning ahead of catalysts. BEAR_SKEW = bearish."""
    state = (d.get("state") or "").upper()
    m = {"BULL_SKEW_RICH": 1, "BEAR_SKEW_RICH": -1, "ACTIVE": 0, "QUIET": 0}
    summ = d.get("summary") or {}
    n_bull = summ.get("n_bull_skew") or 0
    n_bear = summ.get("n_bear_skew") or 0
    return m.get(state, 0), f"Cat-skew {state or 'n/a'} (bull={n_bull}, bear={n_bear})"


def n_crypto_etf_arb(d):
    """ETF arb opportunities = tradeable edge. RICH/ACTIVE = +1, QUIET = 0."""
    state = (d.get("state") or "").upper()
    m = {"ARB_RICH": 1, "ACTIVE": 1, "QUIET": 0}
    summ = d.get("summary") or {}
    max_gap = summ.get("max_abs_gap_pct") or 0
    n_act = (summ.get("n_premium") or 0) + (summ.get("n_discount") or 0)
    return m.get(state, 0), f"ETF-arb {state or 'n/a'} (|gap|={max_gap:.2f}%, act={n_act})"


def n_lockup_expiration(d):
    """Lockup fades are bearish on specific names. RICH = -1, ACTIVE/NORMAL = 0."""
    state = (d.get("state") or "").upper()
    m = {"FADE_RICH": -1, "ACTIVE": 0, "NORMAL": 0, "QUIET": 0}
    summ = d.get("summary") or {}
    n_imm = summ.get("n_imminent_7d") or 0
    n_hc = summ.get("n_high_conviction") or 0
    return m.get(state, 0), f"Lockup {state or 'n/a'} (imm={n_imm}, hc={n_hc})"


# === Tier-2 Retail Edges normalizers (8 engines, 2026-05-20) ===

def n_precatalyst_vol_expansion(d):
    """Low IV + catalyst ahead = long-vol setup (bullish for vol, neutral for direction)."""
    state = (d.get("state") or "").upper()
    m = {"EXPANSION_RICH": 1, "ACTIVE": 1, "NORMAL": 0, "QUIET": 0}
    summ = d.get("summary") or {}
    n_picks = summ.get("picks") or 0
    n_deep = summ.get("deep_low_iv_picks") or 0
    return m.get(state, 0), f"PreCat-Vol {state or 'n/a'} (picks={n_picks}, deep={n_deep})"


def n_cef_discount(d):
    """CEF deep discounts = mean-rev long setups."""
    state = (d.get("state") or "").upper()
    m = {"DISCOUNT_RICH": 1, "ACTIVE": 1, "NORMAL": 0, "QUIET": 0}
    summ = d.get("summary") or {}
    n_disc = summ.get("discounted_n") or 0
    n_deep = summ.get("deep_discount_n") or 0
    return m.get(state, 0), f"CEF-disc {state or 'n/a'} (disc={n_disc}, deep={n_deep})"


def n_reit_nav_discount(d):
    """REIT NAV discounts = long mean-rev setups."""
    state = (d.get("state") or "").upper()
    m = {"DEEP_DISCOUNT_RICH": 1, "DISCOUNT_ACTIVE": 1, "NORMAL": 0, "QUIET": 0}
    summ = d.get("summary") or {}
    n = summ.get("fetched") or 0
    n_deep = summ.get("deep_discount_n") or 0
    return m.get(state, 0), f"REIT-NAV {state or 'n/a'} (picks={n}, deep={n_deep})"


def n_divcut_warning(d):
    """Dividend cut risk = AVOIDANCE signal. HIGH_RISK = -1 (bearish on income names)."""
    state = (d.get("state") or "").upper()
    m = {"HIGH_RISK_RICH": -1, "ELEVATED": -1, "NORMAL": 0, "QUIET": 0}
    summ = d.get("summary") or {}
    n_flag = summ.get("flagged_n") or 0
    n_high = summ.get("high_risk_n") or 0
    return m.get(state, 0), f"DivCut-warn {state or 'n/a'} (flag={n_flag}, hi={n_high})"


def n_rating_change_cluster(d):
    """3+ major-bank upgrades = bullish; 3+ downgrades = bearish."""
    state = (d.get("state") or "").upper()
    m = {"CLUSTER_BUY_RICH": 2, "CLUSTER_BUY_ACTIVE": 1,
         "CLUSTER_SELL_RICH": -2, "CLUSTER_SELL_ACTIVE": -1,
         "NORMAL": 0, "QUIET": 0}
    summ = d.get("summary") or {}
    n_buy = summ.get("buy_picks_n") or 0
    n_sell = summ.get("sell_picks_n") or 0
    return m.get(state, 0), f"Rating-cluster {state or 'n/a'} (B={n_buy}, S={n_sell})"


def n_multi_tf_convergence(d):
    """3-timeframe alignment: bull = +2, bear = -2."""
    state = (d.get("state") or "").upper()
    m = {"BULL_CONVERGENCE_RICH": 2, "BULL_CONVERGENCE_ACTIVE": 1,
         "BEAR_CONVERGENCE_RICH": -2, "BEAR_CONVERGENCE_ACTIVE": -1,
         "NORMAL": 0, "QUIET": 0}
    summ = d.get("summary") or {}
    n_b = summ.get("bull_n") or 0
    n_be = summ.get("bear_n") or 0
    return m.get(state, 0), f"3TF-conv {state or 'n/a'} (bull={n_b}, bear={n_be})"


def n_52wk_quality_breakout(d):
    """Quality 52w breakouts = bullish."""
    state = (d.get("state") or "").upper()
    m = {"QUALITY_BREAKOUT_RICH": 1, "QUALITY_BREAKOUT_ACTIVE": 1, "NORMAL": 0, "QUIET": 0}
    summ = d.get("summary") or {}
    n = summ.get("breakouts_found") or 0
    n_high = summ.get("high_quality_n_4_gates") or 0
    return m.get(state, 0), f"52w-QB {state or 'n/a'} (picks={n}, 4gates={n_high})"


def n_spac_floor_warrant(d):
    """SPAC asymmetric floor plays = +1 risk-free yield + warrant upside."""
    state = (d.get("state") or "").upper()
    m = {"ASYMMETRIC_RICH": 1, "ACTIVE": 1, "NORMAL": 0, "QUIET": 0}
    summ = d.get("summary") or {}
    n = summ.get("qualifying_picks_n") or 0
    return m.get(state, 0), f"SPAC-floor {state or 'n/a'} (picks={n})"


# === Tier-3 Retail Edges Cluster (6 engines, 2026-05-20) ===

def n_vvix_vov_regime(d):
    """Vol-of-vol regime: VEGA_RICH = -1 (sell vol = risk-off), VEGA_CHEAP = +1."""
    state = (d.get("state") or "").upper()
    m = {"VEGA_RICH": -1, "VEGA_ACTIVE": -1, "VEGA_CHEAP": 1, "VEGA_BUILDING": 1,
         "NEUTRAL": 0, "QUIET": 0}
    metrics = d.get("current_metrics") or {}
    ratio = metrics.get("vvix_vix_ratio")
    z = metrics.get("vvix_z")
    return m.get(state, 0), f"VVIX-VoV {state or 'n/a'} (ratio={ratio}, z={z})"


def n_sympathetic_momentum(d):
    """Peer-catchup setups are bullish (laggards expected to catch up)."""
    state = (d.get("state") or "").upper()
    m = {"CATCHUP_RICH": 1, "ACTIVE": 1, "NORMAL": 0, "QUIET": 0}
    n = d.get("n_setups") or 0
    return m.get(state, 0), f"Sympathetic-mom {state or 'n/a'} (setups={n})"


def n_insider_buyback_confluence(d):
    """Double-signal insider+buyback = strong bullish (long-hold)."""
    state = (d.get("state") or "").upper()
    m = {"CONFLUENCE_RICH": 2, "ACTIVE": 1, "NORMAL": 0, "QUIET": 0}
    n = d.get("n_high_conviction") or d.get("n_confluences") or 0
    return m.get(state, 0), f"Ins-byb-conf {state or 'n/a'} (high={n})"


def n_gap_fill_confirm(d):
    """Gap continuation setups = direction-agnostic event flag."""
    state = (d.get("state") or "").upper()
    m = {"CONTINUATION_RICH": 1, "ACTIVE": 1, "NORMAL": 0, "QUIET": 0}
    n = d.get("n_high_conviction") or d.get("n_setups") or 0
    return m.get(state, 0), f"Gap-fill {state or 'n/a'} (setups={n})"


def n_13f_price_divergence(d):
    """BULLISH divergences = +1, BEARISH = -1, mixed = net."""
    state = (d.get("state") or "").upper()
    n_bull = d.get("n_bullish") or 0
    n_bear = d.get("n_bearish") or 0
    if state in ("DIVERGENCE_RICH", "ACTIVE"):
        if n_bull > n_bear * 1.5:
            sig = 2 if state == "DIVERGENCE_RICH" else 1
        elif n_bear > n_bull * 1.5:
            sig = -2 if state == "DIVERGENCE_RICH" else -1
        else:
            sig = 0
    elif state == "NORMAL":
        sig = 1 if n_bull > n_bear else (-1 if n_bear > n_bull else 0)
    else:
        sig = 0
    return sig, f"13F-div {state or 'n/a'} (BULL={n_bull}, BEAR={n_bear})"


def n_credit_equity_divergence(d):
    """CREDIT_BULL_RICH = +2 (equity bullish), CREDIT_BEAR_RICH = -2 (equity risk-off)."""
    state = (d.get("state") or "").upper()
    m = {"CREDIT_BULL_RICH": 2, "CREDIT_BULL_ACTIVE": 1,
         "CREDIT_BEAR_RICH": -2, "CREDIT_BEAR_ACTIVE": -1,
         "NEUTRAL": 0, "QUIET": 0}
    metrics = d.get("current_metrics") or {}
    z = metrics.get("spread_zscore_252d")
    return m.get(state, 0), f"Credit-eq {state or 'n/a'} (z={z})"


# === Tier-4 Retail Edges Cluster (6 engines, 2026-05-20) ===

def n_post_earnings_mean_rev(d):
    """Counter-trend setups; LONG vs SHORT skew determines net direction."""
    state = (d.get("state") or "").upper()
    m = {"MEAN_REV_RICH": 1, "ACTIVE": 1, "NORMAL": 0, "QUIET": 0}
    n_long = d.get("n_long_setups") or 0
    n_short = d.get("n_short_setups") or 0
    base = m.get(state, 0)
    # Net direction skew
    if state in ("MEAN_REV_RICH", "ACTIVE") and n_long + n_short > 0:
        if n_long > n_short * 1.5:
            base = abs(base) * 1
        elif n_short > n_long * 1.5:
            base = -abs(base) * 1
        else:
            base = 0
    return base, f"PE-mean-rev {state or 'n/a'} (L={n_long}, S={n_short})"


def n_insider_sell_cluster(d):
    """Defensive engine: any HIGH/MEDIUM cluster = -1 (risk-off signal for portfolio)."""
    state = (d.get("state") or "").upper()
    m = {"RED_FLAG_RICH": -2, "ACTIVE": -1, "NORMAL": 0, "QUIET": 0}
    n_high = d.get("n_high_severity") or 0
    return m.get(state, 0), f"Ins-sell {state or 'n/a'} (HIGH={n_high})"


def n_vix9d_vix_inversion(d):
    """Statistical bottom signal during full inversion -> +1 (equity bullish 5-10d)."""
    state = (d.get("state") or "").upper()
    m = {"FULL_INVERSION_RICH": 1, "FULL_INVERSION_ACTIVE": 1,
         "PARTIAL_INVERSION": 0, "NORMAL": 0, "QUIET": 0}
    metrics = d.get("current_metrics") or {}
    persist = metrics.get("persistence_days_full_inversion")
    return m.get(state, 0), f"VIX9D-inv {state or 'n/a'} (persist={persist}d)"


def n_breadth_divergence(d):
    """BEARISH_DIVERGENCE = -2 (topping process), BULLISH = +2 (bottoming)."""
    state = (d.get("state") or "").upper()
    m = {"BEARISH_DIVERGENCE_RICH": -2, "BEARISH_DIVERGENCE_ACTIVE": -1,
         "BULLISH_DIVERGENCE_RICH": 2, "BULLISH_DIVERGENCE_ACTIVE": 1,
         "NEUTRAL": 0, "QUIET": 0}
    metrics = d.get("current_metrics") or {}
    n_pos = metrics.get("n_sectors_positive_20d")
    return m.get(state, 0), f"Breadth-div {state or 'n/a'} (sectors+={n_pos}/11)"


def n_skew_tail_hedging(d):
    """TAIL_HEDGE_RICH = -2 (risk-off institutional hedging surge);
    COMPLACENCY_RICH = -1 (cheap insurance signal, mildly bearish)."""
    state = (d.get("state") or "").upper()
    m = {"TAIL_HEDGE_RICH": -2, "TAIL_HEDGE_ACTIVE": -1, "TAIL_HEDGE_BUILDING": 0,
         "COMPLACENCY_RICH": -1, "COMPLACENCY_ACTIVE": 0, "NORMAL": 0, "QUIET": 0}
    metrics = d.get("current_metrics") or {}
    skew = metrics.get("skew")
    days_e = metrics.get("days_skew_above_145")
    return m.get(state, 0), f"SKEW {state or 'n/a'} (skew={skew}, elev={days_e}d)"


def n_dxy_equity_divergence(d):
    """DOLLAR_STRESS = -2 (equity risk-off via eurodollar stress);
    DOLLAR_TAILWIND = +2 (risk-on weak dollar)."""
    state = (d.get("state") or "").upper()
    m = {"DOLLAR_STRESS_RICH": -2, "DOLLAR_STRESS_ACTIVE": -1,
         "DOLLAR_TAILWIND_RICH": 2, "DOLLAR_TAILWIND_ACTIVE": 1,
         "ACTIVE": 0, "NEUTRAL": 0, "QUIET": 0}
    metrics = d.get("current_metrics") or {}
    z = metrics.get("spread_zscore_252d")
    return m.get(state, 0), f"DXY-eq {state or 'n/a'} (z={z})"


# === Tier-5 Retail Edges Cluster (6 engines, 2026-05-20) ===

def n_gold_equity_rotation(d):
    """GOLD_BREAKOUT = -1 (equity bearish, gold dominant);
    EQUITY_DOMINANT = +2 (risk-on, equity wins)."""
    state = (d.get("state") or "").upper()
    m = {"GOLD_BREAKOUT_RICH": -1, "GOLD_BREAKOUT_ACTIVE": 0,
         "EQUITY_DOMINANT_RICH": 2, "EQUITY_DOMINANT_ACTIVE": 1, "NEUTRAL": 0}
    metrics = d.get("current_metrics") or {}
    z = metrics.get("ratio_zscore_252d")
    return m.get(state, 0), f"Gold-eq {state or 'n/a'} (SPY/GLD z={z})"


def n_buyback_yield_ranking(d):
    """BUYBACK_RICH = +1 (n_strong >= 15 high quality buyback names available
    = positive equity quality regime). Not a directional macro signal."""
    state = (d.get("state") or "").upper()
    m = {"BUYBACK_RICH": 1, "ACTIVE": 1, "NORMAL": 0, "QUIET": 0}
    n_strong = d.get("n_strong")
    return m.get(state, 0), f"Buyback-yield {state or 'n/a'} (n_strong={n_strong})"


def n_put_call_extreme(d):
    """Sentiment Extreme Composite (rebuilt v2.0.0 2026-05-21).
    SENTIMENT_PANIC = +2 (capitulation, contrarian LONG signal);
    SENTIMENT_EUPHORIA = -2 (complacency, contrarian SHORT signal).
    Legacy state names BEARISH_EXTREME/BULLISH_EXTREME kept as aliases
    for backward compatibility with any historical readers."""
    state = (d.get("state") or "").upper()
    m = {
        # v2.0 names (Sentiment Extreme Composite)
        "SENTIMENT_PANIC_RICH": 2, "SENTIMENT_PANIC_ACTIVE": 1,
        "SENTIMENT_EUPHORIA_RICH": -2, "SENTIMENT_EUPHORIA_ACTIVE": -1,
        # v1.0 legacy aliases (CBOE P/C, dead but kept for safety)
        "BEARISH_EXTREME_RICH": 2, "BEARISH_EXTREME_ACTIVE": 1,
        "BULLISH_EXTREME_RICH": -2, "BULLISH_EXTREME_ACTIVE": -1,
        "NEUTRAL": 0, "DATA_UNAVAILABLE": 0,
    }
    cz = d.get("composite_z")
    return m.get(state, 0), f"SentExtreme {state or 'n/a'} (z={cz})"


def n_cta_trend_exhaust(d):
    """CTA_MAX_LONG = -2 (unwind risk, bearish);
    CTA_MAX_SHORT = +2 (forced cover rally, bullish)."""
    state = (d.get("state") or "").upper()
    m = {"CTA_MAX_LONG_RICH": -2, "CTA_MAX_LONG_ACTIVE": -1,
         "CTA_MAX_SHORT_RICH": 2, "CTA_MAX_SHORT_ACTIVE": 1, "NEUTRAL": 0}
    metrics = d.get("current_metrics") or {}
    p = metrics.get("avg_lev_pctile_156w")
    return m.get(state, 0), f"CTA-exhaust {state or 'n/a'} (pctile={p})"


def n_ndx_spx_spread(d):
    """NDX_EXTREME_LEAD = 0 (pair trade, balanced);
    SPX_EXTREME_LEAD = 0 (pair trade, balanced).
    Both are info-only since they're paired long/short."""
    state = (d.get("state") or "").upper()
    m = {"NDX_EXTREME_LEAD_RICH": 0, "NDX_LEAD_ACTIVE": 0,
         "SPX_EXTREME_LEAD_RICH": 0, "SPX_LEAD_ACTIVE": 0, "NEUTRAL": 0}
    metrics = d.get("current_metrics") or {}
    z = metrics.get("ratio_zscore_252d")
    return m.get(state, 0), f"NDX-SPX {state or 'n/a'} (QQQ/SPY z={z})"


def n_earnings_quality(d):
    """HIGH_QUALITY = +1 (equity quality regime favorable);
    LOW_QUALITY = -1 (manipulation/dilution risk elevated);
    BOTH_TAILS = 0 (mixed, dispersion)."""
    state = (d.get("state") or "").upper()
    m = {"HIGH_QUALITY_RICH": 1, "LOW_QUALITY_RICH": -1,
         "BOTH_TAILS_RICH": 0, "ACTIVE": 0, "QUIET": 0}
    n_high = d.get("n_high_quality")
    n_low = d.get("n_low_quality")
    return m.get(state, 0), f"Earn-Q {state or 'n/a'} (H={n_high} L={n_low})"


# (engine, category, s3_key, normaliser)


# === 2026-06 Alpha Stack (18-edge buildout + EU Dump Radar v3) ===

def n_ignition(d):
    """Pre-pump accumulation composite: hot top-decile = equity-bullish."""
    ranks = d.get("ranks") or []
    top = [r.get("ignition_score") for r in ranks[:8]
           if isinstance(r.get("ignition_score"), (int, float))]
    if not top:
        return 0, "Ignition n/a"
    avg = sum(top) / len(top)
    sig = 2 if avg >= 68 else 1 if avg >= 58 else 0
    tc = ", ".join((d.get("top_calls") or [])[:3])
    return sig, f"Ignition avg {avg:.1f} (top: {tc or 'n/a'})"


def n_bottleneck_boom(d):
    ranks = d.get("ranks") or d.get("top") or []
    vals = []
    for r in ranks[:8]:
        if isinstance(r, dict):
            v = next((r[k] for k in ("boom_score", "bottleneck_score", "score", "composite")
                      if isinstance(r.get(k), (int, float))), None)
            if v is not None:
                vals.append(v)
    if not vals:
        return 0, "Bottleneck n/a"
    avg = sum(vals) / len(vals)
    return (1 if avg >= 60 else 0), f"Bottleneck avg {avg:.1f}"


def n_crisis_canaries(d):
    v3 = d.get("composite_v3")
    sc = d.get("composite_score")
    if v3 is None and sc is None:
        return 0, "Canaries n/a"
    use = v3 if v3 is not None else sc
    lvl = d.get("level_v3") or d.get("level", "n/a")
    red = d.get("red_count")
    sig = -2 if use >= 70 else -1 if (use >= 45 or (red or 0) >= 4) else 0
    extra = f" · {red} red of {d.get('n_global')}" if red is not None else ""
    return sig, f"Crisis composite v3 {use} ({lvl}){extra} · plumbing {sc}"


def n_liquidity_inflection(d):
    u = d.get("usd") or {}
    z = u.get("impulse_z")
    if z is None:
        return 0, "Liq-inflect n/a"
    sig = 2 if z > 1 else 1 if z > 0.25 else -2 if z < -1 else -1 if z < -0.25 else 0
    return sig, f"USD liq impulse z={z} ({u.get('state', 'n/a')})"


def n_confluence_meta(d):
    nt = d.get("net_today") or {}
    net = nt.get("net")
    if net is None:
        return 0, "Confluence n/a"
    sig = 2 if net >= 4 else 1 if net >= 2 else -2 if net <= -4 else -1 if net <= -2 else 0
    return sig, (f"Net engine breadth {net:+d} "
                 f"({len(nt.get('up_engines') or [])}↑/{len(nt.get('down_engines') or [])}↓)")


def n_kb_match(d):
    top = (d.get("top_matches") or [{}])[0]
    fw, mp = top.get("framework") or "", top.get("match_pct")
    if not fw or mp is None:
        return 0, "KB-match n/a"
    bear = any(w in fw for w in ("Crisis", "Stress", "Inversion", "Default",
                                  "Stagflation", "Shortage", "Top", "Auction"))
    bull = any(w in fw for w in ("Pivot", "Bottom", "Buy"))
    sig = (-1 if mp >= 75 else 0) if bear else ((1 if mp >= 75 else 0) if bull else 0)
    return sig, f"KB closest: {fw} ({mp}%)"


def n_eu_dump(d):
    ds = d.get("dump_score") or {}
    sc = ds.get("score_0_100")
    if sc is None:
        return 0, "EU-dump n/a"
    sig = -2 if sc >= 75 else -1 if sc >= 60 else 0
    return sig, f"EU dump score {sc} ({ds.get('level', 'n/a')})"


def n_index_inclusion(d):
    n = d.get("n_eligible")
    if n is None:
        return 0, "Inclusion n/a"
    return 0, f"S&P inclusion-eligible names: {n} (context, non-directional)"




def n_us_cycle(d):
    cs = d.get("cycle_score") or {}
    sc = cs.get("score_0_100")
    if sc is None:
        return 0, "US-cycle n/a"
    sig = -2 if sc >= 75 else -1 if sc >= 60 else 0
    drv = sorted((cs.get("components") or []), key=lambda c: -abs(c.get("z", 0)))[:2]
    return sig, f"US cycle {sc} ({cs.get('level')}; drivers: " +                 ", ".join(f"{c['id']} z{c['z']:+.1f}" for c in drv) + ")"


def n_market_internals(d):
    mc = d.get("mcclellan") or {}
    osc = mc.get("oscillator")
    if osc is None:
        return 0, "Internals n/a"
    if (d.get("zweig_thrust") or {}).get("fired"):
        return 2, f"ZWEIG BREADTH THRUST {d['zweig_thrust'].get('date')}"
    sig = 2 if osc <= -100 else 1 if osc <= -70 else -1 if osc >= 100 else 0
    return sig, f"McClellan {osc} ({mc.get('state')}), summation {mc.get('summation')}"


def n_us_money(d):
    um = d.get("us_money") or {}
    z = um.get("z")
    if z is None:
        return 0, "Real-M2 n/a"
    v = um.get("real_m2_yoy_pct")
    sig = -1 if (v or 0) < 0 else 1 if z > 1 else 0
    return sig, f"US real M2 {v:+.1f}% YoY (z {z})"




def n_ma_reversion(d):
    sp = (d.get("spx") or {}).get("current") or {}
    ns = sp.get("nearest_shelf")
    n_set = d.get("n_setups") or 0
    if not sp:
        return 0, "MA-reversion n/a"
    xx = d.get("crossings") or {}
    u2 = sum(1 for c in (xx.get("stocks_up") or []) if c.get("ma") == 200)
    d2 = sum(1 for c in (xx.get("stocks_down") or []) if c.get("ma") == 200)
    if u2 + d2 >= 3 or abs(u2 - d2) >= 2:
        sig = 1 if u2 > d2 else -1 if d2 > u2 else 0
        return sig, f"200DMA breaks: {u2}▲/{d2}▼ in 3 sessions · {n_set} setups at shelves"
    if ns and ns.get("below_pct", 99) <= 1.0:
        return 1, f"SPX at the {ns['ma']}DMA shelf ({ns['below_pct']}% above) · {n_set} stock setups"
    return 0, (f"Nearest shelf {ns['ma']}DMA −{ns['below_pct']}%" if ns else "Below all MAs") +                f" · {n_set} setups at MAs"




def n_regime(d):
    c = d.get("current") or {}
    q = c.get("quadrant")
    if not q:
        return 0, "Regime n/a"
    pb = ((d.get("playbook") or {}).get("spx") or {}).get("3m", {}).get(q) or {}
    sig = {"GOLDILOCKS": 1, "REFLATION": 0, "STAGFLATION": -1, "DEFLATION-BUST": -1}.get(q, 0)
    if pb.get("pos_pct") is not None and 45 <= pb["pos_pct"] <= 60:
        sig = min(sig, 0) if sig < 0 else sig  # keep mild
    return sig, (f"{q} ({c.get('months_in_regime')}m, liq {c.get('liquidity_state')}) · "
                  f"SPX+3m playbook: {pb.get('median', '—')}% med, {pb.get('pos_pct', '—')}%+ "
                  f"n={pb.get('n', '—')}")




def n_episode_compass(d):
    cs = d.get("class_scores") or {}
    rd = d.get("reading") or {}
    t, b, sw = cs.get("TOP"), cs.get("BOTTOM"), cs.get("BLACK_SWAN")
    if t is None:
        return 0, "Compass n/a"
    spread = rd.get("top_minus_bottom")
    if spread is None and b is not None:
        spread = round(t - b, 1)
    tails = rd.get("tails_n") or 0
    # calm states score high on every class; the signal lives in the SPREAD
    # and in tail presence (active stress) vs none (calm-before-accident)
    if (sw or 0) >= 80 and tails >= 2:
        sig = -2
    elif (spread or 0) >= 10:
        sig = -1
    elif (spread or 0) <= -10:
        sig = 1
    else:
        sig = 0
    profile = ("calm-before-accident profile" if tails == 0 and (sw or 0) >= 80
                else "active-stress profile" if tails >= 2 else "mid-cycle")
    return sig, (f"Tops {t} vs bottoms {b} (spread {spread:+.1f}) · swans {sw} · "
                  f"{tails} tails — {profile}")




def n_upside_radar(d):
    sc = d.get("scans") or {}
    st = d.get("state") or {}
    nb = len(sc.get("breakout") or [])
    if not st:
        return 0, "Upside-radar n/a"
    apass = sum(1 for a in (d.get("anatomy") or []) if (a.get("anatomy_score") or 0) >= 40)
    sig = 1 if (nb >= 5 and apass >= 2) else 0
    warm = "" if st.get("warm") else f" (warming {st.get('sessions_seen')}/252)"
    return sig, (f"{nb} breakouts · {len(sc.get('rs_leaders') or [])} RS leaders · "
                  f"{len(sc.get('coiled') or [])} coiled · anatomy-pass {apass}{warm}")


def n_rotation_radar(d):
    sc = d.get("scores") or {}
    ca, er = sc.get("crypto_altseason"), sc.get("equity_rotation")
    if ca is None and er is None:
        return 0, "Rotation n/a"
    armed = []
    if ((d.get("crypto") or {}).get("live") or {}).get("ethbtc", {}).get("thrust_live"):
        armed.append("ETH/BTC")
    for k, v in ((d.get("equity") or {}).get("ratios") or {}).items():
        if (v.get("live") or {}).get("thrust_live"):
            armed.append(k)
    sig = 2 if len(armed) >= 2 else 1 if armed else 0
    return sig, (f"Altseason {ca} · equity rotation {er}"
                  + (f" · ARMED: {chr(44).join(armed)}" if armed else " · no thrusts live"))


def n_altseason(d):
    c = d.get("composite") or {}
    ph, sc = c.get("phase"), c.get("score")
    if ph is None:
        return 0, "Altseason n/a"
    rej = c.get("rejected_overlay")
    sig = 2 if (ph == "CONFIRMED" and not rej) else 1 if ph == "IGNITION" and not rej else \
          -1 if rej else 0
    return sig, (f"{ph} {sc}/100 · {len(c.get(chr(99)+chr(111)+chr(110)+chr(102)+chr(105)+chr(114)+chr(109)+chr(115)) or [])} confirm"
                  f" / {len(c.get(chr(114)+chr(101)+chr(106)+chr(101)+chr(99)+chr(116)+chr(115)) or [])} reject"
                  + (" · MACRO-REJECTED" if rej else ""))


def n_sizing(d):
    recs = d.get("recommendations") or []
    if not d.get("engine_table"):
        return 0, "Sizing n/a"
    top = recs[0] if recs else None
    rd = (f"{len(recs)} sized · gross {d.get(chr(103)+chr(114)+chr(111)+chr(115)+chr(115)+chr(95)+chr(114)+chr(101)+chr(99)+chr(111)+chr(109)+chr(109)+chr(101)+chr(110)+chr(100)+chr(101)+chr(100)+chr(95)+chr(119)+chr(95)+chr(112)+chr(99)+chr(116))}%"
          + (f" · top {top[chr(116)+chr(105)+chr(99)+chr(107)+chr(101)+chr(114)]} {top[chr(102)+chr(105)+chr(110)+chr(97)+chr(108)+chr(95)+chr(119)+chr(95)+chr(112)+chr(99)+chr(116)]}%" if top else "")
          + f" · fade {len(d.get(chr(102)+chr(97)+chr(100)+chr(101)+chr(95)+chr(108)+chr(105)+chr(115)+chr(116)) or [])}")
    return 0, rd


def n_market_map(d):
    b = d.get("breadth") or {}
    if b.get("advancers") is None:
        return 0, "Map n/a"
    a, dec = b["advancers"], b["decliners"]
    chg = b.get("mcap_weighted_chg") or 0
    sig = 1 if (a >= dec * 2 and chg >= 0.8) else -1 if (dec >= a * 2 and chg <= -0.8) else 0
    return sig, (f"S&P breadth {a}/{dec} · cap-wtd {chg:+.2f}% · "
                  f"{d.get(chr(110)+chr(95)+chr(116)+chr(105)+chr(108)+chr(101)+chr(115))} tiles ({d.get(chr(115)+chr(105)+chr(122)+chr(101)+chr(95)+chr(109)+chr(111)+chr(100)+chr(101))})")


def n_sector_groups(d):
    l = d.get("leadership") or {}
    top, bot = l.get("top_1m"), l.get("bottom_1m")
    if not top:
        return 0, "Groups n/a"
    DEF = ("Utilities", "Consumer Staples", "Health Care")
    CYC = ("Technology", "Consumer Discretionary", "Communication Services")
    sig = -1 if (top in DEF and bot in CYC) else 1 if (top in CYC and bot in DEF) else 0
    return sig, f"1M leadership: {top} → … → {bot}" + (" (defensive rotation)" if sig < 0 else " (risk-on rotation)" if sig > 0 else "")


def n_insider_radar(d):
    if d.get("source_used") == "unavailable":
        return 0, "Insider source gated (see diagnostics)"
    nb = d.get("n_buys") or 0
    nc = len(d.get("clusters") or [])
    nd = len(d.get("decline_clusters") or [])
    sig = 1 if nd >= 2 else 0
    return sig, f"{nb} buys 30d \u00b7 {nc} clusters \u00b7 {nd} after-decline"


def n_stock_valuations(d):
    sp = d.get("sp_coverage")
    if sp is None:
        return 0, "Valuations n/a"
    hp = d.get("hp") or []
    top = hp[0] if hp else {}
    ns = d.get("n_serious") or 0
    sig = 1 if ns >= 1 else 0
    return sig, (f"S&P {sp}/{d.get(chr(115)+chr(112)+chr(95)+chr(117)+chr(110)+chr(105)+chr(118)+chr(101)+chr(114)+chr(115)+chr(101))} valued · "
                  f"top HP {top.get(chr(116))} {top.get(chr(115)+chr(99)+chr(111)+chr(114)+chr(101))} · {ns} ≥75 clean")


def n_research_papers(d):
    n = d.get("n_papers")
    if not n:
        return 0, "Research papers n/a"
    p0 = (d.get("papers") or [{}])[0]
    return 0, f"{n} AI papers · latest {p0.get(chr(116))} conviction {p0.get(chr(99)+chr(111)+chr(110)+chr(118)+chr(105)+chr(99)+chr(116)+chr(105)+chr(111)+chr(110))}/10"


def n_backtest_harness(d):
    np_, nr = d.get("n_pass"), len(d.get("rules") or [])
    if np_ is None:
        return 0, "Backtest harness n/a"
    return 0, f"{np_}/{nr} archetypes pass deflated-Sharpe OOS gate"


def n_meta_labeler(d):
    m = d.get("model") or {}
    if d.get("status") == "warming_up":
        return 0, (f"gatekeeper WARMING UP - {d.get('n_training_rows')} graded rows "
                    f"(activates at {d.get('min_rows_to_activate')}); "
                    f"{d.get('n_pending_gated')} pending aging in")
    if m.get("uplift_pp") is None:
        return 0, "Meta-labeler n/a"
    up = m["uplift_pp"]
    tr = m.get("test_take_rate")
    return (1 if up > 3 else 0), (f"gatekeeper uplift {up:+}pp at {tr}% take-rate; "
                                     f"{d.get('n_take')}/{d.get('n_pending_gated')} pending TAKE")


def n_intraday_pulse(d):
    n = d.get("n_events_today")
    if n is None:
        return 0, "Intraday pulse n/a"
    tm = (d.get("top_movers") or [{}])[0]
    return (1 if n >= 3 else 0), (f"{d.get('armed_n')} armed · {n} events today · "
                                     f"top mover {tm.get('t')} {tm.get('chg')}%")


def n_estimate_revisions(d):
    b = d.get("breadth") or {}
    if d.get("coverage") is None:
        return 0, "Estimate revisions n/a"
    net = (b.get("up", 0) - b.get("down", 0))
    sig = 1 if net >= 8 else (-1 if net <= -8 else 0)
    return sig, (f"{d.get('coverage')} covered · revisions {b.get('up',0)}up/"
                  f"{b.get('down',0)}dn (aging in) · fwd-growth live")


def n_risk_regime(d):
    """Authoritative cross-asset Risk-On/Risk-Off synthesizer (Massive FX+options
    + FRED VIX/credit). + = risk-on, - = risk-off."""
    s = d.get("risk_regime_score")
    if not isinstance(s, (int, float)):
        return 0, "RORO n/a"
    sig = 2 if s >= 35 else 1 if s >= 12 else 0 if s > -12 else -1 if s > -35 else -2
    return sig, f"RORO {d.get('risk_regime', '?')} ({s:+.0f})"


def n_naaim(d):
    """NAAIM active-manager exposure — contrarian at extremes. + = washed-out
    (bullish forward), - = levered euphoria (crowded). Provisional history damps."""
    sig = d.get("signal")
    if not isinstance(sig, int):
        return 0, "NAAIM n/a"
    if d.get("provisional"):
        sig = max(-1, min(1, sig))
    v = (d.get("latest") or {}).get("value")
    return sig, "NAAIM %.0f %s%s" % (v if v is not None else -1, d.get("state", "?"),
                                     " (prov)" if d.get("provisional") else "")


def n_leverage(d):
    """System-wide leverage cycle (FINRA margin + NFCI leverage + lev-ETF + crypto).
    Excess-rolling / forced-delev = fragility; healthy rebuild = supportive."""
    lm = d.get("leverage_monitor") or {}
    ph = lm.get("phase")
    if not ph:
        return 0, "Leverage n/a"
    sig = {"FORCED_DELEVERAGING": -2, "EXCESSIVE_ROLLING": -2, "EXCESSIVE_BUILDING": -1,
           "COOLING": -1, "REBUILDING": 1, "LOW": 1}.get(ph, 0)
    fy = ((lm.get("layers") or {}).get("retail_finra") or {}).get("yoy_pct")
    return sig, "LEV %.0f %s%s" % (lm.get("cycle_score") or 0, ph,
                                   " (FINRA %+.0f%% yoy)" % fy if fy is not None else "")


def n_ici(d):
    """ICI slow-money flows: dry-powder hoard = forward fuel (+); MMF drain
    into equity chasing = late deployment (-). Provisional clamps to +/-1."""
    sig = d.get("signal")
    if not isinstance(sig, int):
        return 0, "ICI n/a"
    m = (d.get("mmf") or {})
    eq = ((d.get("long_term") or {}).get("equity_sum_4w_m"))
    return sig, "ICI %s · MMF $%.1fT%s · eq4w %s" % (
        d.get("regime", "?"), (m.get("total_b") or 0) / 1000,
        (" z%+.1f" % m["z_13w"]) if isinstance(m.get("z_13w"), (int, float)) else "",
        ("$%+.0fB" % (eq / 1000)) if isinstance(eq, (int, float)) else "?")


def n_aaii(d):
    """AAII individual-investor survey — contrarian at extremes (bull-bear spread)."""
    L = d.get("latest") or {}
    sp = L.get("bull_bear_spread")
    if not isinstance(sp, (int, float)):
        return 0, "AAII n/a"
    bull, bear = L.get("bullish"), L.get("bearish")
    sig = 2 if (sp <= -0.30 or (bear or 0) >= 0.55) else 1 if sp <= -0.15 \
        else -2 if (sp >= 0.30 or (bull or 0) >= 0.58) else -1 if sp >= 0.18 else 0
    return sig, "AAII %.0f/%.0f spread %+.0fpp" % ((bull or 0) * 100, (bear or 0) * 100, sp * 100)


def n_blackout(d):
    """Buyback-blackout share of S&P mktcap — the corporate bid switch. High
    blackout = buyback support absent into event risk; low = window open."""
    now = d.get("now") or {}
    p = now.get("blackout_mktcap_pct")
    if not isinstance(p, (int, float)):
        return 0, "Blackout n/a"
    sig = -1 if p >= 60 else 1 if p <= 20 else 0
    return sig, "Blackout %.0f%% of SPX cap · next-14d reporting %.0f%%" % (
        p, (d.get("next_14d") or {}).get("reporting_mktcap_pct") or 0)


def n_termprem(d):
    """ACM 10y term premium — TP shocks (not expectations) drive bond-vigilante
    risk-off; TP collapse = duration bid, discount-rate relief."""
    L = d.get("latest") or {}
    d21 = (d.get("deltas_bps") or {}).get("d21")
    if not isinstance(L.get("tp10"), (int, float)):
        return 0, "ACM n/a"
    sig = -2 if (d21 or 0) >= 45 else -1 if (d21 or 0) >= 25 \
        else 2 if (d21 or 0) <= -45 else 1 if (d21 or 0) <= -25 else 0
    return sig, "TP10 %.2f%% Δ21d %+.0fbps p%.0f" % (L["tp10"], d21 or 0,
                                                     d.get("pctile_full_history") or 0)


def n_bonddesk(d):
    """GLOBAL fixed-income anxiety — regional blend of what bond money is pricing."""
    a=d.get("world_anxiety") or d.get("anxiety_score"); r=d.get("regime")
    if not isinstance(a,(int,float)): return 0,"Bond desk n/a"
    sig={"STRESS":-2,"ANXIOUS":-1,"CALM":1}.get(r,0)
    h=d.get("hottest_region") or {}
    return sig,"GLOBAL FI %.0f %s (hot: %s %.0f)"%(a,r,(h.get("region") or "?").replace("_"," "),h.get("score") or 0)


def n_rebalance(d):
    """Quarter-end rebalance window + leadership->crypto rotation-risk flag."""
    c=d.get("calendar") or {}; rr=d.get("rotation_risk") or {}
    if not c: return 0,"Rebalance n/a"
    if rr.get("flag"):
        return (-2 if rr.get("severity")=="HIGH" else -1), "ROTATION RISK %s — leadership de-risking into crypto (Q-end window)"%rr.get("severity")
    if c.get("in_rebalance_window"):
        return 0,"Rebalance window OPEN (T%+d vs %s) — no rotation signature"%(-c.get("bdays_to_qtr_end",0) or c.get("bdays_since_prev_qtr_end",0),c.get("window_anchor"))
    return 0,"Next quarter-end %s (%d bdays)"%(c.get("quarter_end"),c.get("bdays_to_qtr_end",0))


def n_darkpool(d):
    """Own-DIX buying pressure + accumulation breadth from FINRA ATS/regsho fusion."""
    dx=(d.get("dix") or {}); v=dx.get("own_dix_pct")
    dist=(d.get("distribution") or {})
    if not isinstance(v,(int,float)): return 0,"Dark pool n/a"
    sig=1 if v>=57 else -1 if v<52 else 0
    return sig,"DARK POOL own-DIX %.1f%% %s (accum %d / dist %d)"%(v,dx.get("read",""),dist.get("accumulation",0),dist.get("distribution",0))


def n_factors(d):
    """Which equity style is being paid — daily L/S factor returns + rotation flags."""
    rg=d.get("regime") or {}; f=d.get("factors") or {}
    if not rg.get("leader"): return 0,"Factors n/a"
    sig=-1 if "MOMENTUM_CRASH" in (rg.get("flags") or []) else (1 if rg.get("leader")=="MOMENTUM" else 0)
    mom=(f.get("MOMENTUM") or {}).get("ls_ret_1d_pct")
    return sig,"FACTORS %s (MOM L/S %+.2f%%/1d)"%(rg.get("read","")[:60],mom if mom is not None else 0)


def n_xray(d):
    """Per-name umbrella coverage + derived-board pulse."""
    b=d.get("boards") or {}
    if not d.get("n_cards"): return 0,"X-Ray n/a"
    return 0,"X-RAY %d names (turnersto-profit %d, multibag %d, DIS %d, laggards %d)"%(
        d["n_cards"],len(b.get("turning_profitable") or []),len(b.get("multibagger_candidates") or []),
        len(b.get("dis_warnings") or []),len(b.get("laggards_watch") or []))


def n_globalflows(d):
    """Where money is going: class ladder + inst/retail divergence + hot-money map."""
    ir=(d.get("inst_vs_retail") or {}); hot=(d.get("hot_money") or {})
    inst,ret=ir.get("institutional"),ir.get("retail")
    if inst is None and not hot.get("n_scored"): return 0,"Global flows n/a"
    sig=1 if (inst or 0)>30 and (ret or 0)>0 else -1 if (inst or 0)<-30 else 0
    return sig,"GLOBAL FLOWS inst %s / retail %s — %s; hot in %s out %s"%(
        inst,ret,(ir.get("divergence") or "")[:34],
        ",".join((hot.get("top_inflows") or [])[:2]),",".join((hot.get("top_outflows") or [])[:2]))


def n_capex(d):
    """Corporate capex impulse: hyperscaler AI-buildout spend + market breadth."""
    hs=(d.get("hyperscalers") or {}); mk=(d.get("market") or {})
    hy=hs.get("yoy_pct")
    if hy is None and mk.get("yoy_pct") is None: return 0,"CapEx n/a"
    sig=1 if (hy or 0)>15 else -1 if (hy or 0)<-5 else 0
    return sig,"CAPEX hyperscalers $%.0fB %+.1f%% yoy | market $%.0fB %+.1f%%"%(
        hs.get("total_ttm_b") or 0,hy or 0,mk.get("capex_ttm_b") or 0,mk.get("yoy_pct") or 0)


def n_footprint(d):
    """Institutional posture: what they're doing vs what they're positioning for."""
    p=d.get("posture") or {}
    rn,rf=p.get("risk_now"),p.get("risk_forward")
    if rn is None and rf is None: return 0,"Footprint n/a"
    sig=1 if (rn or 0)>18 and (rf or 0)>-10 else -1 if (rf or 0)<-15 or (rn or 0)<-18 else 0
    return sig,"INSTITUTIONS %s (%s) -> %s (%s) | feeds %s"%(p.get("now_label"),rn,p.get("forward_label"),rf,d.get("feeds_alive"))


def n_canary_warroom(d):
    b = d.get("barometer") or {}
    sc = b.get("score")
    if sc is None:
        return 0, "War-room barometer n/a"
    sig = 1 if sc < 25 else 0 if sc < 40 else -1 if sc < 55 else -2
    m = d.get("master") or {}
    return sig, (f"War-room barometer {sc}/100 ({b.get('band')}), "
                 f"{m.get('n_firing')}/{m.get('n_canaries')} canaries firing")


FEEDS = [
    ("Early-Warning War Room", "macro", "data/canary-warroom.json", n_canary_warroom),
    ("PM Decision",        "positioning",      "data/pm-decision.json",        n_pm_decision),
    ("Cross-Asset RV",     "relative value",   "data/cross-asset-rv.json",     n_cross_asset_rv),
    ("Fundamentals X-Ray", "equity valuation", "data/fundamentals.json",       n_fundamentals),
    ("Housing Cycle",      "macro",            "data/construction-housing.json", n_construction_housing),
    ("Crypto Narratives",  "crypto",           "data/crypto-narratives.json",  n_crypto_narratives),
    ("Short Pressure",     "positioning",      "data/short-pressure.json",     n_short_pressure),
    ("Mean Reversion",     "equity valuation", "screener/mean-reversion.json", n_mean_reversion),
    ("Canary Grid",        "macro",            "data/canary-grid.json",        n_canary_grid),
    ("Dollar Radar",       "macro",            "data/dollar-radar.json",       n_dollar_radar),
    ("Global Stress",      "macro",            "data/global-stress.json",      n_global_stress),
    ("Eurodollar Plumbing","macro",            "data/eurodollar-plumbing.json", n_eurodollar_plumbing),
    ("Auction Crisis",     "macro",            "data/auction-crisis.json",     n_auction_crisis),
    # 10-Edge institutional roadmap
    ("Edge#1 VIX Backwardation",  "volatility",  "data/vix-backwardation-trigger.json", n_vix_backwardation),
    ("Edge#2 Insider Buys",       "smart money", "data/insider-buys-enriched.json",     n_insider_buys),
    ("Edge#3 Breadth Thrust",     "volatility",  "data/breadth-thrust.json",            n_breadth_thrust),
    ("Edge#4 Vol-Target Unwind",  "volatility",  "data/vol-target-unwind.json",         n_vol_target_unwind),
    ("Edge#5 Russell Recon",      "events",      "data/russell-recon-frontrun.json",    n_russell_recon),
    ("Edge#6 Buyback Scanner",    "events",      "data/buyback-scanner.json",           n_buyback_scanner),
    ("Edge#7 Stablecoin Flow",    "crypto",      "data/stablecoin-flow.json",           n_stablecoin_flow),
    ("Edge#8 OPEX Calendar",      "volatility",  "data/opex-calendar.json",             n_opex_calendar),
    ("Edge#9 Activist 13D",       "smart money", "data/activist-13d.json",              n_activist_13d),
    ("Edge#10 RV-IV / Dispersion","volatility",  "data/rv-iv-scanner.json",             n_rv_iv_scanner),
    # Retail opportunity engine
    ("Crypto Opportunities",      "crypto",      "data/crypto-opportunities.json",      n_crypto_opportunities),
    # === Retail-Edges Cluster (7 engines, 2026-05-20) ===
    ("Earnings IV Crush",         "volatility",       "data/earnings-iv-crush.json",        n_earnings_iv_crush),
    ("Stealth Accumulation",      "smart money",      "data/stealth-accumulation.json",     n_stealth_accumulation),
    ("Failed Pattern Reversal",   "equity tactical",  "data/failed-pattern-reversal.json",  n_failed_pattern_reversal),
    ("Squeeze Pre-Trigger",       "positioning",      "data/squeeze-pretrigger.json",       n_squeeze_pretrigger),
    ("Catalyst+Skew Premove",     "positioning",      "data/catalyst-skew-premove.json",    n_catalyst_skew_premove),
    ("Crypto ETF Arb",            "crypto",           "data/crypto-etf-arb.json",           n_crypto_etf_arb),
    ("Lockup Expiration",         "events",           "data/lockup-expiration.json",        n_lockup_expiration),
    # === Tier-2 Retail Edges Cluster (8 engines, 2026-05-20) ===
    ("Pre-Catalyst Vol Expansion","volatility",       "data/precatalyst-vol-expansion.json", n_precatalyst_vol_expansion),
    ("CEF Discount",              "equity tactical",  "data/cef-discount.json",              n_cef_discount),
    ("REIT NAV Discount",         "equity tactical",  "data/reit-nav-discount.json",         n_reit_nav_discount),
    ("Dividend Cut Warning",      "risk avoidance",   "data/divcut-warning.json",            n_divcut_warning),
    ("Rating Change Cluster",     "smart money",      "data/rating-change-cluster.json",     n_rating_change_cluster),
    ("Multi-TF Convergence",      "equity tactical",  "data/multi-tf-convergence.json",      n_multi_tf_convergence),
    ("52W Quality Breakout",      "equity tactical",  "data/52wk-quality-breakout.json",     n_52wk_quality_breakout),
    ("SPAC Floor + Warrant",      "asymmetric",       "data/spac-floor-warrant.json",        n_spac_floor_warrant),
    # === Tier-3 Retail Edges Cluster (6 engines, 2026-05-20) ===
    ("VVIX VoV Regime",           "volatility",       "data/vvix-vov-regime.json",           n_vvix_vov_regime),
    ("Sympathetic Momentum",      "equity tactical",  "data/sympathetic-momentum.json",      n_sympathetic_momentum),
    ("Insider+Buyback Confluence","smart money",      "data/insider-buyback-confluence.json",n_insider_buyback_confluence),
    ("Gap-Fill Continuation",     "equity tactical",  "data/gap-fill-confirm.json",          n_gap_fill_confirm),
    ("13F Price Divergence",      "smart money",      "data/13f-price-divergence.json",      n_13f_price_divergence),
    ("Credit-Equity Divergence",  "macro",            "data/credit-equity-divergence.json",  n_credit_equity_divergence),
    # === Tier-4 Retail Edges Cluster (6 engines, 2026-05-20) ===
    ("Post-Earnings Mean-Rev",    "equity tactical",  "data/post-earnings-mean-rev.json",    n_post_earnings_mean_rev),
    ("Insider Sell Cluster",      "risk avoidance",   "data/insider-sell-cluster.json",      n_insider_sell_cluster),
    ("VIX9D-VIX Inversion",       "volatility",       "data/vix9d-vix-inversion.json",       n_vix9d_vix_inversion),
    ("Breadth Divergence",        "macro",            "data/breadth-divergence.json",        n_breadth_divergence),
    ("SKEW Tail-Hedging",         "volatility",       "data/skew-tail-hedging.json",         n_skew_tail_hedging),
    ("DXY-Equity Divergence",     "macro",            "data/dxy-equity-divergence.json",     n_dxy_equity_divergence),
    # === Tier-5 Retail Edges Cluster (6 engines, 2026-05-20) ===
    ("Gold-Equity Rotation",      "macro",            "data/gold-equity-rotation.json",      n_gold_equity_rotation),
    ("Buyback Yield Ranking",     "smart money",      "data/buyback-yield-ranking.json",     n_buyback_yield_ranking),
    ("Sentiment Extreme",         "sentiment",        "data/put-call-extreme.json",          n_put_call_extreme),
    ("CTA Trend Exhaust",         "positioning",      "data/cta-trend-exhaust.json",         n_cta_trend_exhaust),
    ("NDX-SPX Spread",            "equity tactical",  "data/ndx-spx-spread.json",            n_ndx_spx_spread),
    ("Earnings Quality",          "equity valuation", "data/earnings-quality.json",          n_earnings_quality),
    # === 2026-06 Alpha Stack (18-edge buildout + EU Dump Radar v3) ===
    ("Ignition Pre-Pump",      "smart money",      "data/ignition.json",             n_ignition),
    ("Bottleneck Boom",        "equity tactical",  "data/bottleneck-boom.json",      n_bottleneck_boom),
    ("Crisis Canaries",        "macro",            "data/crisis-canaries.json",      n_crisis_canaries),
    ("Liquidity Inflection",   "macro",            "data/liquidity-inflection.json", n_liquidity_inflection),
    ("Confluence Net-Breadth", "sentiment",        "data/confluence-meta.json",      n_confluence_meta),
    ("Crisis-KB Match",        "macro",            "data/kb-match.json",             n_kb_match),
    ("EU Dump Radar",          "macro",            "data/ecb-derived.json",          n_eu_dump),
    ("S&P Inclusion Watch",    "events",           "data/index-inclusion.json",      n_index_inclusion),
    # === Brain-gap wave (ops-1580 audit) ===
    ("US Macro Cycle",         "macro",            "data/us-cycle.json",             n_us_cycle),
    ("Market Internals",       "sentiment",        "data/market-internals.json",     n_market_internals),
    ("US Real M2",             "macro",            "data/liquidity-inflection.json", n_us_money),
    ("MA Reversion Shelves",   "equity tactical",  "data/ma-reversion.json",         n_ma_reversion),
    ("Macro Regime (Conductor)","macro",           "data/regime.json",               n_regime),
    ("Episode Compass",        "macro",            "data/episode-compass.json",      n_episode_compass),
    ("Upside Radar",           "equity tactical",  "data/upside-radar.json",         n_upside_radar),
    ("Rotation Radar",         "sentiment",        "data/rotation-radar.json",       n_rotation_radar),
    ("Altseason Tribunal",     "sentiment",        "data/altseason.json",            n_altseason),
    ("Sizing Engine",          "meta",             "data/sizing.json",               n_sizing),
    ("Market Map (S&P)",       "equity tactical",  "data/market-map.json",           n_market_map),
    ("Sector Groups",          "equity tactical",  "data/sector-groups.json",        n_sector_groups),
    ("Insider Radar",          "equity tactical",  "data/insider-radar.json",        n_insider_radar),
    ("Stock Valuations",       "equity tactical",  "data/stock-valuations.json",     n_stock_valuations),
    ("Research Papers",        "equity tactical",  "data/research-papers.json",      n_research_papers),
    ("Backtest Harness",       "meta",             "data/backtest-harness.json",     n_backtest_harness),
    ("Meta-Labeler",           "meta",             "data/meta-labeler.json",         n_meta_labeler),
    ("Intraday Pulse",         "tape",             "data/intraday-pulse.json",       n_intraday_pulse),
    ("Estimate Revisions",     "equity tactical",  "data/estimate-revisions.json",   n_estimate_revisions),
    ("Risk Regime (RORO)",     "cross-asset",      "data/risk-regime.json",          n_risk_regime),
    ("NAAIM Positioning",      "sentiment",        "data/naaim.json",                n_naaim),
    ("Leverage Cycle",         "leverage",         "data/margin-lending.json",       n_leverage),
    ("AAII Retail Sentiment",  "sentiment",        "data/aaii-sentiment.json",       n_aaii),
    ("Buyback Blackout",       "flow",             "data/earnings-blackout.json",    n_blackout),
    ("Term Premium (ACM)",     "rates",            "data/term-premium.json",         n_termprem),
    ("Bond Desk",              "rates",            "data/bond-desk.json",            n_bonddesk),
    ("Rebalance Window",       "flow",             "data/rebalance-radar.json",      n_rebalance),
    ("Dark Pool",              "flow",             "data/dark-pool.json",            n_darkpool),
    ("Factor Returns",         "equity",           "data/factor-returns.json",       n_factors),
    ("Stock X-Ray",            "equity",           "data/stock-xray.json",           n_xray),
    ("Global Flows",           "flow",             "data/global-flow-desk.json",     n_globalflows),
    ("Institutional Footprint","flow",             "data/institutional-footprint.json", n_footprint),
    ("CapEx Pulse",            "equity",           "data/capex-pulse.json",          n_capex),
    ("Fund Flows (ICI)",       "flows",            "data/ici-flows.json",            n_ici),
]


def lambda_handler(event, context):
    t0 = time.time()
    now = datetime.now(timezone.utc)
    engines, stale = [], 0

    for name, cat, key, fn in FEEDS:
        data, last_mod = read_json(key)
        if data is None:
            engines.append({"engine": name, "category": cat, "signal": None,
                            "signal_label": "NO DATA", "read": "sidecar missing",
                            "as_of": None, "stale": True})
            stale += 1
            continue
        try:
            sig, read = fn(data)
            sig = clamp(sig)
        except Exception as e:
            sig, read = None, f"parse error: {str(e)[:80]}"
        as_of = data.get("generated_at") or (
            last_mod.isoformat() if last_mod else None)
        is_stale = False
        if last_mod and (now - last_mod) > timedelta(hours=STALE_HOURS):
            is_stale = True
            stale += 1
        engines.append({
            "engine": name, "category": cat, "signal": sig,
            "signal_label": SIG_LABEL.get(sig, "—") if sig is not None else "—",
            "read": read, "as_of": as_of, "stale": is_stale})

    live = [e for e in engines if e["signal"] is not None and not e["stale"]]
    composite = round(sum(e["signal"] for e in live) / len(live), 2) if live else None

    # per-category sub-posture
    cats = {}
    for e in live:
        cats.setdefault(e["category"], []).append(e["signal"])
    categories = {c: {"signal": round(sum(v) / len(v), 2), "n": len(v)}
                  for c, v in cats.items()}

    if composite is None:
        posture = "NO SIGNAL"
    elif composite >= 1.0:
        posture = "RISK-ON"
    elif composite >= 0.25:
        posture = "MILDLY RISK-ON"
    elif composite > -0.25:
        posture = "NEUTRAL / MIXED"
    elif composite > -1.0:
        posture = "MILDLY RISK-OFF"
    else:
        posture = "RISK-OFF"

    # ── Upgrade A: tiered-depth escalation ──────────────────────────────
    # On a genuinely conflicted cross-asset tape, fire a GLM-5.1 deep read.
    # Cheap by default: only runs when signals are dispersed across strong
    # both-ways OR the composite sits in the NEUTRAL no-man's-land — exactly
    # when a desk PM wants deeper analysis. Never breaks the board.
    deep_read = None
    try:
        sig_vals = [e["signal"] for e in live]
        dispersed = bool(sig_vals) and (max(sig_vals) >= 1 and min(sig_vals) <= -1)
        no_mans_land = posture == "NEUTRAL / MIXED"
        if len(live) >= 5 and (dispersed or no_mans_land):
            from llm_router import complete
            ctx = "\n".join(
                f"- [{e['category']}] {e['engine']}: {e['signal']} "
                f"({e['signal_label']}) — {e['read']}" for e in live)
            sys_p = (
                "You are a senior global-macro PM resolving a conflicted cross-asset "
                "tape. Signals are normalised -2 (strong risk-off) .. +2 (strong "
                "risk-on) from public-market models. Cut through decisively. Respond "
                "ONLY with a JSON object with keys: dominant_driver (string), "
                "conflict_resolution (string), hidden_risk (string), resolve_triggers "
                "(array of 2-4 observable level/threshold strings that would tip the "
                "posture), lean (one of RISK-ON_TILT, RISK-OFF_TILT, GENUINELY_FLAT), "
                "confidence (LOW/MEDIUM/HIGH). Be decisive; do not hedge.")
            usr_p = (f"Composite {composite} ({posture}); {len(live)} live signals:\n\n"
                     f"{ctx}\n\nResolve the cross-currents into one actionable lean.")
            raw = complete(usr_p, tier="reason", max_tokens=1600, system=sys_p)
            import re as _re
            j = _re.sub(r"^```(?:json)?|```$", "", (raw or "").strip(),
                        flags=_re.MULTILINE).strip()
            deep_read = json.loads(j)
            deep_read["_model"] = "glm-5.1"
            deep_read["_trigger"] = "dispersion" if dispersed else "neutral_mixed"
            print(f"[signal-board] deep_read fired lean={deep_read.get('lean')} "
                  f"trigger={deep_read['_trigger']}")
    except Exception as _de:
        print(f"[signal-board] deep_read skipped/failed: {str(_de)[:140]}")

    out = {
        "schema_version": "1.0",
        "method": "cross_asset_signal_aggregation",
        "generated_at": now.isoformat(),
        "elapsed_s": round(time.time() - t0, 2),
        "composite_signal": composite,
        "composite_posture": posture,
        "n_engines": len(engines),
        "n_live": len(live),
        "n_stale": stale,
        "categories": categories,
        "deep_read": deep_read,
        "engines": engines,
        "note": ("Unified signal store — each engine's headline read "
                 "normalised to a 5-state signal and aggregated into one "
                 "cross-asset posture. Stale feeds (sidecar older than "
                 f"{STALE_HOURS}h) are flagged and excluded from the "
                 "composite. A synthesis view, not advice."),
    }
    s3.put_object(Bucket=S3_BUCKET, Key=OUT_KEY,
                  Body=json.dumps(out, default=str).encode("utf-8"),
                  ContentType="application/json",
                  CacheControl="public, max-age=1800")
    print(f"[signal-board] posture={posture} composite={composite} "
          f"{len(live)}/{len(engines)} live, {stale} stale, {out['elapsed_s']}s")
    
    # ─── Emit posture.changed event if posture flipped vs previous run ──
    # Posture transitions across the RISK_ON / NEUTRAL / RISK_OFF axis are
    # institutional-level events — they move portfolio exposure decisions.
    # Best-effort, never blocks the engine.
    try:
        prev_posture = None
        try:
            prev_obj = s3.get_object(Bucket=S3_BUCKET, Key=OUT_KEY + ".prev")
            prev_data = json.loads(prev_obj["Body"].read().decode("utf-8"))
            prev_posture = prev_data.get("composite_posture")
        except Exception:
            pass
        
        if prev_posture and prev_posture != posture:
            from system_events import publish
            publish("posture.changed", {
                "previous":         prev_posture,
                "current":          posture,
                "composite_signal": composite,
                "n_live":           len(live),
                "n_stale":          stale,
                "n_engines_total":  len(engines),
            }, source_engine="signal-board")
        
        # Save current state for next run's comparison
        s3.put_object(Bucket=S3_BUCKET, Key=OUT_KEY + ".prev",
                       Body=json.dumps({
                           "composite_posture": posture,
                           "composite_signal":  composite,
                           "generated_at":      now.isoformat(),
                       }).encode("utf-8"),
                       ContentType="application/json")
    except Exception as e:
        print(f"[signal-board] event publish failed: {e}")
    
    return {"statusCode": 200, "body": json.dumps({
        "ok": True, "composite_posture": posture,
        "composite_signal": composite, "n_live": len(live),
        "n_stale": stale})}
