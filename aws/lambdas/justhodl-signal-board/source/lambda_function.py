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


def n_global_stress(d):
    # global_stress_index 0-100; high = world equity/bond stress = risk-off.
    gsi = d.get("global_stress_index")
    lvl = d.get("global_stress_level") or "n/a"
    if not isinstance(gsi, (int, float)):
        return 0, "Global stress n/a"
    sig = -2 if gsi >= 75 else -1 if gsi >= 55 else 1 if gsi < 32 else 0
    return sig, f"Global market stress {lvl} ({gsi}/100)"


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


# (engine, category, s3_key, normaliser)
FEEDS = [
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
    return {"statusCode": 200, "body": json.dumps({
        "ok": True, "composite_posture": posture,
        "composite_signal": composite, "n_live": len(live),
        "n_stale": stale})}
