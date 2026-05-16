"""
justhodl-allocator — Cross-asset relative-value allocator.

Reads ALL regime detectors and synthesizes them into a single asset allocation matrix:

INPUTS (all already in S3):
  - data/macro-surprise.json         → macro composite z + regime
  - data/yield-curve.json            → curve regime + spreads
  - data/correlation-surface.json    → correlation regime
  - data/sector-rotation.json        → market breadth
  - data/eurodollar-stress.json      → dollar/funding stress
  - data/auction-crisis.json         → treasury auction stress
  - data/historical-analogs.json     → directional call from analogs
  - data/event-study.json            → expected return from active events
  - data/calibration-snapshot.json   → which regime detectors are most accurate
  - data/sector-rotation.json        → sector momentum quintiles

ASSET CLASSES SCORED (-100 to +100):
  - SPY        US large-cap equities
  - QQQ        US tech / NASDAQ
  - IWM        US small-cap (Russell 2000)
  - EFA        Developed ex-US
  - EEM        Emerging markets
  - TLT        Long Treasury (20+ year)
  - IEF        Intermediate Treasury (7-10 year)
  - HYG        High yield credit
  - GLD        Gold
  - DBC        Broad commodities
  - UUP        US Dollar
  - VXX        Volatility
  - BTC        Bitcoin (proxied via existing crypto-intel data)

ALGORITHM (regime → asset score):
  Each regime detector has a "tilt" matrix that says "in regime X,
  asset Y gets +N points". We then sum tilts weighted by detector accuracy
  (from calibration-snapshot) and normalize.

  Final: each asset gets (score, conviction, recommended_weight).

  Conviction = abs(score) * detector_count_agreed / max_count
  Recommended weight = max(0, score) / sum(positive_scores) * (100% - cash%)

OUTPUT: data/allocator.json
SCHEDULE: every 4 hours (after sector rotation refreshes)
"""
import json
import os
import time
from datetime import datetime, timezone
from collections import defaultdict
import boto3

S3 = boto3.client("s3", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"
KEY = "data/allocator.json"


# ─────────────────────────────────────────────────────────────────
# Regime → asset tilt rules (each rule contributes ±points to asset score)

# Tilt magnitudes:  HARD = ±15, STRONG = ±10, MEDIUM = ±5, MILD = ±2
HARD, STRONG, MEDIUM, MILD = 15, 10, 5, 2

ASSETS = ["SPY", "QQQ", "IWM", "EFA", "EEM", "TLT", "IEF", "HYG", "GLD", "DBC", "UUP", "VXX", "BTC"]


def empty_scores():
    return {a: 0.0 for a in ASSETS}


def empty_evidence():
    return {a: [] for a in ASSETS}


def fs3(key, default=None):
    try:
        obj = S3.get_object(Bucket=BUCKET, Key=key)
        return json.loads(obj["Body"].read())
    except Exception as e:
        print(f"[s3] {key}: {e}")
        return default if default is not None else {}


def add(scores, evidence, asset, points, reason):
    """Apply a tilt to one asset and record evidence."""
    scores[asset] += points
    evidence[asset].append({"points": points, "reason": reason})


# ─────────────────────────────────────────────────────────────────
# Rule library — each takes scores, evidence, and a data dict


def rule_macro_surprise(scores, evidence):
    d = fs3("data/macro-surprise.json")
    z = d.get("composite") or d.get("composite_z")
    regime = d.get("regime") or "UNKNOWN"
    if z is None:
        return
    z = float(z)
    desc = f"macro_z={z:+.2f} ({regime})"
    if regime == "GROWTH_SURPRISE_POSITIVE" and z >= 0.5:
        add(scores, evidence, "SPY", STRONG, desc)
        add(scores, evidence, "QQQ", STRONG, desc)
        add(scores, evidence, "IWM", MEDIUM, desc)
        add(scores, evidence, "EEM", MEDIUM, desc)
        add(scores, evidence, "DBC", MEDIUM, desc)
        add(scores, evidence, "TLT", -STRONG, desc)
        add(scores, evidence, "IEF", -MEDIUM, desc)
        add(scores, evidence, "GLD", -MEDIUM, desc)
        add(scores, evidence, "VXX", -STRONG, desc)
    elif regime == "GROWTH_SURPRISE_NEGATIVE" and z <= -0.5:
        add(scores, evidence, "SPY", -STRONG, desc)
        add(scores, evidence, "QQQ", -MEDIUM, desc)
        add(scores, evidence, "IWM", -STRONG, desc)
        add(scores, evidence, "TLT", STRONG, desc)
        add(scores, evidence, "IEF", STRONG, desc)
        add(scores, evidence, "GLD", MEDIUM, desc)
        add(scores, evidence, "VXX", STRONG, desc)
    elif "INFLATION_SURPRISE" in regime:
        if z > 0:  # hot inflation
            add(scores, evidence, "GLD", STRONG, desc)
            add(scores, evidence, "DBC", STRONG, desc)
            add(scores, evidence, "TLT", -HARD, desc)
            add(scores, evidence, "IEF", -STRONG, desc)
            add(scores, evidence, "QQQ", -MEDIUM, desc)
        else:  # cool inflation
            add(scores, evidence, "TLT", STRONG, desc)
            add(scores, evidence, "IEF", STRONG, desc)
            add(scores, evidence, "QQQ", MEDIUM, desc)


def rule_yield_curve(scores, evidence):
    d = fs3("data/yield-curve.json")
    spreads = d.get("spreads_bps") or {}
    regime = d.get("regime") or d.get("yc_regime") or "UNKNOWN"
    s2s10s = spreads.get("2s10s")
    if s2s10s is None:
        return
    desc = f"YC {regime} (2s10s={s2s10s}bp)"
    if regime == "BEAR_STEEPENER":
        # Long-end selling more than front — inflation/term premium concern
        add(scores, evidence, "TLT", -STRONG, desc)
        add(scores, evidence, "IEF", -MEDIUM, desc)
        add(scores, evidence, "GLD", MEDIUM, desc)
        add(scores, evidence, "DBC", MEDIUM, desc)
        add(scores, evidence, "QQQ", -MILD, desc)
    elif regime == "BULL_STEEPENER":
        # Front-end rallying — Fed cut expectations
        add(scores, evidence, "SPY", STRONG, desc)
        add(scores, evidence, "QQQ", STRONG, desc)
        add(scores, evidence, "IWM", STRONG, desc)
        add(scores, evidence, "HYG", MEDIUM, desc)
        add(scores, evidence, "GLD", MEDIUM, desc)
    elif regime == "BEAR_FLATTENER":
        # Front-end selling — Fed hike fear
        add(scores, evidence, "SPY", -MEDIUM, desc)
        add(scores, evidence, "QQQ", -STRONG, desc)
        add(scores, evidence, "IWM", -STRONG, desc)
        add(scores, evidence, "UUP", STRONG, desc)
        add(scores, evidence, "HYG", -STRONG, desc)
    elif regime == "BULL_FLATTENER":
        # Long-end rallying more — recession positioning
        add(scores, evidence, "TLT", STRONG, desc)
        add(scores, evidence, "IEF", STRONG, desc)
        add(scores, evidence, "GLD", MEDIUM, desc)
        add(scores, evidence, "SPY", -MEDIUM, desc)
        add(scores, evidence, "VXX", MEDIUM, desc)
    if s2s10s < 0:
        # Inverted curve historically signals recession 6-18m forward
        add(scores, evidence, "TLT", MILD, "2s10s inverted")
        add(scores, evidence, "GLD", MILD, "2s10s inverted")


def rule_sector_breadth(scores, evidence):
    d = fs3("data/sector-rotation.json")
    breadth = d.get("market_breadth")
    if not breadth:
        return
    desc = f"sector breadth: {breadth}"
    if breadth == "BROAD_LEADERSHIP":
        add(scores, evidence, "SPY", STRONG, desc)
        add(scores, evidence, "IWM", STRONG, desc)
        add(scores, evidence, "VXX", -MEDIUM, desc)
    elif breadth == "NARROW_LEADERSHIP":
        # Historically precedes corrections — late-cycle
        add(scores, evidence, "SPY", -MEDIUM, desc)
        add(scores, evidence, "IWM", -STRONG, desc)
        add(scores, evidence, "QQQ", -MILD, desc)
        add(scores, evidence, "VXX", MEDIUM, desc)
        add(scores, evidence, "GLD", MILD, desc)


def rule_correlation_regime(scores, evidence):
    d = fs3("data/correlation-surface.json")
    regime = d.get("regime") or d.get("correlation_regime")
    breaks = d.get("regime_breaks") or []
    if not regime:
        return
    desc = f"correlation regime: {regime}"
    if regime == "MACRO_ALL_ON" or regime == "RISK_ON":
        # Everything correlated up — late cycle, risk-on
        add(scores, evidence, "VXX", -MEDIUM, desc)
        add(scores, evidence, "GLD", -MILD, desc)
    elif regime == "RISK_OFF":
        add(scores, evidence, "TLT", STRONG, desc)
        add(scores, evidence, "GLD", STRONG, desc)
        add(scores, evidence, "VXX", STRONG, desc)
        add(scores, evidence, "SPY", -MEDIUM, desc)
    elif regime == "DECORRELATION" or regime == "MACRO_NORMAL":
        # Healthy — diversification working
        add(scores, evidence, "SPY", MEDIUM, desc)
    # If many regime breaks → instability premium
    n_breaks = len([b for b in breaks if abs(b.get("delta_30d_vs_90d") or 0) >= 0.30])
    if n_breaks >= 3:
        add(scores, evidence, "VXX", MEDIUM, f"{n_breaks} corr regime breaks")
        add(scores, evidence, "GLD", MILD, f"{n_breaks} corr regime breaks")


def rule_eurodollar_stress(scores, evidence):
    d = fs3("data/eurodollar-stress.json")
    score = d.get("composite_stress_score") or d.get("composite_score")
    if score is None:
        return
    score = float(score)
    desc = f"eurodollar stress: {score}/100"
    if score >= 70:
        add(scores, evidence, "UUP", HARD, desc)
        add(scores, evidence, "TLT", STRONG, desc)
        add(scores, evidence, "GLD", STRONG, desc)
        add(scores, evidence, "SPY", -STRONG, desc)
        add(scores, evidence, "EEM", -HARD, desc)
        add(scores, evidence, "BTC", -STRONG, desc)
        add(scores, evidence, "HYG", -STRONG, desc)
    elif score >= 50:
        add(scores, evidence, "UUP", MEDIUM, desc)
        add(scores, evidence, "GLD", MEDIUM, desc)
        add(scores, evidence, "EEM", -MEDIUM, desc)
    elif score <= 25:
        # Calm dollar / abundant funding
        add(scores, evidence, "EEM", MEDIUM, desc)
        add(scores, evidence, "BTC", MEDIUM, desc)
        add(scores, evidence, "HYG", MEDIUM, desc)
        add(scores, evidence, "UUP", -MEDIUM, desc)


def rule_auction_crisis(scores, evidence):
    d = fs3("data/auction-crisis.json")
    score = d.get("composite_score") or d.get("crisis_score")
    if score is None:
        return
    score = float(score)
    desc = f"auction crisis: {score}/100"
    if score >= 60:
        add(scores, evidence, "TLT", -HARD, desc)
        add(scores, evidence, "IEF", -STRONG, desc)
        add(scores, evidence, "SPY", -MEDIUM, desc)
        add(scores, evidence, "GLD", STRONG, desc)
        add(scores, evidence, "BTC", STRONG, desc)
        add(scores, evidence, "VXX", MEDIUM, desc)


def rule_historical_analogs(scores, evidence):
    d = fs3("data/historical-analogs.json")
    call = (d.get("directional_call") or "").upper()
    fwd = d.get("forward_distribution", {})
    fwd21 = fwd.get("21d", {}) if isinstance(fwd, dict) else {}
    hr = fwd21.get("hit_rate_pct") if isinstance(fwd21, dict) else None
    mean_pct = fwd21.get("mean_pct") if isinstance(fwd21, dict) else None
    if not call:
        return
    desc = f"analogs {call}, hr={hr}% mean={mean_pct}%"
    if "BULL" in call:
        # weight by hit rate
        boost = STRONG if (hr and hr >= 80) else MEDIUM
        add(scores, evidence, "SPY", boost, desc)
        add(scores, evidence, "QQQ", boost, desc)
    elif "BEAR" in call:
        boost = -STRONG if (hr and hr >= 80) else -MEDIUM
        add(scores, evidence, "SPY", boost, desc)
        add(scores, evidence, "QQQ", boost, desc)
        add(scores, evidence, "VXX", -boost, desc)


def rule_event_study(scores, evidence):
    d = fs3("data/event-study.json")
    expected = d.get("expected_21d_return_from_active_pct")
    themes = d.get("active_themes") or []
    if expected is None or not themes:
        return
    expected = float(expected)
    desc = f"event-study: 21d expected {expected:+.2f}% from {len(themes)} themes"
    if abs(expected) < 0.5:
        return
    if expected > 0:
        boost = STRONG if expected >= 2 else MEDIUM
        add(scores, evidence, "SPY", boost, desc)
    else:
        boost = -STRONG if expected <= -2 else -MEDIUM
        add(scores, evidence, "SPY", boost, desc)
        add(scores, evidence, "VXX", -boost, desc)


def rule_btc_signals(scores, evidence):
    """Pull crypto signals from data/report.json or crypto-intel."""
    d = fs3("crypto-intel.json") or fs3("data/crypto-intel.json")
    if not d:
        return
    fg = (d.get("fear_greed") or {}).get("value") if isinstance(d.get("fear_greed"), dict) else d.get("fear_greed")
    risk = d.get("risk_score") or (d.get("crypto_risk_score") if isinstance(d.get("crypto_risk_score"), (int, float)) else None)
    try:
        if fg is not None:
            fg = float(fg)
            if fg <= 25:
                add(scores, evidence, "BTC", STRONG, f"crypto FG={fg} (extreme fear)")
            elif fg >= 75:
                add(scores, evidence, "BTC", -MEDIUM, f"crypto FG={fg} (extreme greed)")
    except Exception:
        pass


def rule_sector_momentum(scores, evidence):
    """Use sector momentum quintile to tilt SPY/QQQ/IWM proxies."""
    d = fs3("data/sector-rotation.json")
    sectors = d.get("sectors") or []
    # If XLK (top tech) is LEADER + momentum quintile 4, boost QQQ
    xlk = next((s for s in sectors if s.get("ticker") == "XLK"), None)
    if xlk and xlk.get("regime") == "LEADER" and xlk.get("momentum_quintile", 0) >= 3:
        add(scores, evidence, "QQQ", MEDIUM, "XLK is leader q4+")
    # If financials (XLF) lagging → bad sign for cycle
    xlf = next((s for s in sectors if s.get("ticker") == "XLF"), None)
    if xlf and xlf.get("regime") == "LAGGING":
        add(scores, evidence, "SPY", -MILD, "XLF lagging — banks weak")


def rule_liquidity_credit_engine(scores, evidence):
    """Position sizing based on Liquidity & Credit Engine state.

    Reads data/liquidity-credit-engine.json — Khalid-spec FRED + ICE BofA series.
    When liquidity is draining or credit spreads are widening, tilt away from
    risk assets toward duration, dollar, gold. When LCE is calm and reserves are
    growing, tilt into risk assets.
    """
    d = fs3("data/liquidity-credit-engine.json")
    if not d:
        return
    regime = d.get("regime")
    composite = (d.get("composite") or {}).get("score") or 0
    series = d.get("series") or {}

    def state(sid): return (series.get(sid) or {}).get("signal", "NORMAL")
    def value(sid): return (series.get(sid) or {}).get("latest_value")

    # ── Regime-level position adjustment ──
    desc = f"LCE regime: {regime} composite={composite}/100"
    if regime == "CRISIS":
        add(scores, evidence, "UUP", HARD, desc)
        add(scores, evidence, "TLT", STRONG, desc)
        add(scores, evidence, "GLD", STRONG, desc)
        add(scores, evidence, "SPY", -HARD, desc)
        add(scores, evidence, "QQQ", -HARD, desc)
        add(scores, evidence, "EEM", -HARD, desc)
        add(scores, evidence, "BTC", -HARD, desc)
        add(scores, evidence, "HYG", -HARD, desc)
    elif regime == "ACUTE_STRESS":
        add(scores, evidence, "UUP", STRONG, desc)
        add(scores, evidence, "TLT", STRONG, desc)
        add(scores, evidence, "GLD", STRONG, desc)
        add(scores, evidence, "SPY", -STRONG, desc)
        add(scores, evidence, "EEM", -STRONG, desc)
        add(scores, evidence, "BTC", -STRONG, desc)
        add(scores, evidence, "HYG", -STRONG, desc)
    elif regime == "ELEVATED":
        add(scores, evidence, "UUP", MEDIUM, desc)
        add(scores, evidence, "GLD", MEDIUM, desc)
        add(scores, evidence, "TLT", MEDIUM, desc)
        add(scores, evidence, "BTC", -MEDIUM, desc)
        add(scores, evidence, "EEM", -MEDIUM, desc)
        add(scores, evidence, "HYG", -MEDIUM, desc)
    elif regime == "CALM":
        # Liquidity abundant → risk-on tilt
        add(scores, evidence, "BTC", MILD, desc)
        add(scores, evidence, "EEM", MILD, desc)
        add(scores, evidence, "HYG", MILD, desc)
        add(scores, evidence, "UUP", -MILD, desc)

    # ── Specific sub-signal: CCC HY OAS widening (Khalid-spec) ──
    ccc_state = state("BAMLH0A3HYC")
    ccc_val = value("BAMLH0A3HYC")
    if ccc_state in ("ELEVATED", "CRISIS"):
        d2 = f"CCC HY OAS: {ccc_val}% [{ccc_state}]"
        add(scores, evidence, "HYG", -STRONG, d2)
        add(scores, evidence, "BTC", -MEDIUM, d2)
        add(scores, evidence, "GLD", MEDIUM, d2)

    # ── Primary credit usage spike (financial-crisis signal) ──
    pc_state = state("OTHL1690")
    pc_val = value("OTHL1690")
    if pc_state in ("ELEVATED", "CRISIS"):
        d3 = f"Primary credit (OTHL1690): ${pc_val}B [{pc_state}]"
        add(scores, evidence, "TLT", STRONG, d3)
        add(scores, evidence, "GLD", STRONG, d3)
        add(scores, evidence, "UUP", MEDIUM, d3)
        add(scores, evidence, "HYG", -STRONG, d3)
        add(scores, evidence, "SPY", -MEDIUM, d3)

    # ── Central-bank swap line activation (FX dollar shortage) ──
    swap_state_total = state("SWPT")
    swap_val_total = value("SWPT")
    swap_state_1690 = state("SWP1690")
    if swap_state_total in ("ELEVATED", "CRISIS") or swap_state_1690 in ("ELEVATED", "CRISIS"):
        d4 = f"CB liquidity swaps active: SWPT=${swap_val_total}B [{swap_state_total}]"
        add(scores, evidence, "UUP", STRONG, d4)
        add(scores, evidence, "GLD", MEDIUM, d4)
        add(scores, evidence, "EEM", -STRONG, d4)
        add(scores, evidence, "EFA", -MEDIUM, d4)

    # ── SLOOS bank lending tightening (recession leading indicator) ──
    # When banks tighten standards on C&I large or small firms above 25%, that
    # historically presages a credit-driven slowdown 6-12 months out.
    sloos_ci_large_state = state("DRTSCILM")
    sloos_ci_large_val = value("DRTSCILM")
    sloos_ci_small_state = state("DRTSCIS")
    sloos_cre_state = state("SUBLPDCRENQ")
    sloos_cc_state = state("DRTSCLCC")

    if sloos_ci_large_state in ("ELEVATED", "CRISIS") or sloos_ci_small_state in ("ELEVATED", "CRISIS"):
        d5 = (f"SLOOS C&I tightening: large={sloos_ci_large_val}% [{sloos_ci_large_state}]"
              f" small=[{sloos_ci_small_state}] — recession-prone bank tightening")
        add(scores, evidence, "TLT", STRONG, d5)
        add(scores, evidence, "GLD", MEDIUM, d5)
        add(scores, evidence, "SPY", -STRONG, d5)
        add(scores, evidence, "QQQ", -MEDIUM, d5)
        add(scores, evidence, "IWM", -HARD, d5)        # small caps most exposed
        add(scores, evidence, "HYG", -HARD, d5)        # HY most exposed to bank-tightening cycles
        add(scores, evidence, "EEM", -STRONG, d5)

    if sloos_cre_state in ("ELEVATED", "CRISIS"):
        d6 = f"SLOOS CRE tightening [{sloos_cre_state}] — office/retail credit crunch"
        add(scores, evidence, "GLD", MEDIUM, d6)
        # Regional banks + REITs are the direct hit
        add(scores, evidence, "IWM", -STRONG, d6)
        add(scores, evidence, "HYG", -MEDIUM, d6)

    if sloos_cc_state in ("ELEVATED", "CRISIS"):
        d7 = f"SLOOS Credit-Card tightening [{sloos_cc_state}] — consumer credit squeeze"
        add(scores, evidence, "QQQ", -MEDIUM, d7)
        add(scores, evidence, "HYG", -STRONG, d7)


def rule_tenor_signals(scores, evidence):
    """Position sizing based on Treasury tenor-signal interpreter.

    Reads data/auction-tenor-signals.json. fed_path firing CUTS_PRICED →
    risk-on tilt; HIKES_PRICED → risk-off tilt. eurodollar firing → dollar
    long. qe_imminence firing → long duration + gold + BTC.
    """
    d = fs3("data/auction-tenor-signals.json")
    if not d:
        return
    sigs = d.get("signals") or {}
    fp = sigs.get("fed_path") or {}
    ed = sigs.get("eurodollar") or {}
    qe = sigs.get("qe_imminence") or {}

    fp_state = fp.get("state")
    fp_dir = fp.get("direction")
    if fp_state in ("FIRING", "EXTREME"):
        desc = f"Fed-path firing: 2y → {fp_dir}"
        if fp_dir == "CUTS_PRICED":
            add(scores, evidence, "TLT", STRONG, desc)
            add(scores, evidence, "GLD", MEDIUM, desc)
            add(scores, evidence, "BTC", MEDIUM, desc)
            add(scores, evidence, "UUP", -MEDIUM, desc)
        elif fp_dir == "HIKES_PRICED":
            add(scores, evidence, "UUP", STRONG, desc)
            add(scores, evidence, "TLT", -STRONG, desc)
            add(scores, evidence, "GLD", -MEDIUM, desc)
            add(scores, evidence, "BTC", -STRONG, desc)
            add(scores, evidence, "QQQ", -MEDIUM, desc)

    if ed.get("state") in ("FIRING", "EXTREME"):
        desc = f"Eurodollar shortage firing: {ed.get('state')}"
        add(scores, evidence, "UUP", HARD, desc)
        add(scores, evidence, "TLT", STRONG, desc)
        add(scores, evidence, "GLD", STRONG, desc)
        add(scores, evidence, "EEM", -HARD, desc)
        add(scores, evidence, "EFA", -STRONG, desc)
        add(scores, evidence, "BTC", -STRONG, desc)

    if qe.get("state") in ("FIRING", "EXTREME"):
        desc = f"QE imminence firing: {qe.get('state')}"
        add(scores, evidence, "TLT", HARD, desc)
        add(scores, evidence, "GLD", HARD, desc)
        add(scores, evidence, "BTC", HARD, desc)
        add(scores, evidence, "UUP", -STRONG, desc)


def rule_global_business_cycle(scores, evidence):
    """Position sizing based on OECD Composite Leading Indicator phase mix.

    Reads data/global-business-cycle.json. Translates global phase + key-country
    phase into cross-asset and country-level tilts.
    """
    d = fs3("data/global-business-cycle.json")
    if not d:
        return
    agg = d.get("aggregate") or {}
    interp = d.get("interpretation") or {}
    by_country = d.get("by_country") or {}
    global_phase = agg.get("global_phase")
    avg_cli = agg.get("global_avg_cli")
    cont_pct = agg.get("contraction_breadth_pct") or 0
    exp_pct = agg.get("expansion_breadth_pct") or 0

    base_desc = f"global cycle: {global_phase} (avg CLI {avg_cli}, contraction {cont_pct}%, expansion {exp_pct}%)"

    # ── Global phase regime adjustment ──
    if global_phase == "GLOBAL_CONTRACTION":
        add(scores, evidence, "SPY", -HARD, base_desc)
        add(scores, evidence, "QQQ", -STRONG, base_desc)
        add(scores, evidence, "IWM", -HARD, base_desc)
        add(scores, evidence, "EEM", -HARD, base_desc)
        add(scores, evidence, "EFA", -STRONG, base_desc)
        add(scores, evidence, "HYG", -HARD, base_desc)
        add(scores, evidence, "TLT", STRONG, base_desc)
        add(scores, evidence, "GLD", STRONG, base_desc)
        add(scores, evidence, "UUP", MEDIUM, base_desc)
    elif global_phase == "GLOBAL_PEAKING":
        add(scores, evidence, "IWM", -STRONG, base_desc + " — small caps roll over first")
        add(scores, evidence, "EEM", -MEDIUM, base_desc + " — EM peaks with DM")
        add(scores, evidence, "HYG", -MEDIUM, base_desc + " — late-cycle spread widening")
        add(scores, evidence, "TLT", MEDIUM, base_desc + " — defensive bid building")
        add(scores, evidence, "GLD", MEDIUM, base_desc)
        add(scores, evidence, "UUP", MILD, base_desc)
    elif global_phase == "GLOBAL_RECOVERY":
        # Strongest equity returns historically
        add(scores, evidence, "SPY", STRONG, base_desc)
        add(scores, evidence, "IWM", HARD, base_desc + " — small caps explode out of recovery")
        add(scores, evidence, "EEM", HARD, base_desc + " — EM disproportionately benefits")
        add(scores, evidence, "EFA", MEDIUM, base_desc)
        add(scores, evidence, "HYG", STRONG, base_desc + " — HY snapback")
        add(scores, evidence, "BTC", STRONG, base_desc)
        add(scores, evidence, "TLT", -MEDIUM, base_desc + " — rates normalize")
        add(scores, evidence, "UUP", -MEDIUM, base_desc + " — USD weakens on global risk-on")
    elif global_phase == "GLOBAL_EXPANSION":
        add(scores, evidence, "SPY", MEDIUM, base_desc)
        add(scores, evidence, "QQQ", MEDIUM, base_desc)
        add(scores, evidence, "IWM", STRONG, base_desc + " — cyclical exposure benefits")
        add(scores, evidence, "EEM", STRONG, base_desc + " — EM beta to global growth")
        add(scores, evidence, "EFA", MEDIUM, base_desc + " — DM expansion broad")
        add(scores, evidence, "HYG", MEDIUM, base_desc + " — risk-on supports HY")
        add(scores, evidence, "BTC", MEDIUM, base_desc)
        add(scores, evidence, "TLT", -MILD, base_desc + " — rising-rate pressure")
        add(scores, evidence, "UUP", -MILD, base_desc)

    # ── USA-specific override ──
    usa = by_country.get("USA") or {}
    usa_phase = usa.get("phase")
    if usa_phase == "RECESSION":
        d2 = f"USA in RECESSION (CLI {usa.get('cli_level')})"
        add(scores, evidence, "SPY", -STRONG, d2)
        add(scores, evidence, "IWM", -HARD, d2)
        add(scores, evidence, "TLT", STRONG, d2)
        add(scores, evidence, "GLD", STRONG, d2)
    elif usa_phase == "AT_RISK":
        d2 = f"USA AT RISK (CLI {usa.get('cli_level')})"
        add(scores, evidence, "IWM", -MEDIUM, d2)
        add(scores, evidence, "TLT", MEDIUM, d2)

    # ── China-specific ──
    chn = by_country.get("CHN") or {}
    chn_phase = chn.get("phase")
    if chn_phase == "RECESSION":
        d3 = f"China in RECESSION (CLI {chn.get('cli_level')})"
        add(scores, evidence, "EEM", -HARD, d3)
        add(scores, evidence, "FXI", -HARD, d3)
    elif chn_phase == "RECOVERY":
        d3 = f"China in RECOVERY (CLI {chn.get('cli_level')})"
        add(scores, evidence, "EEM", STRONG, d3)
        add(scores, evidence, "FXI", STRONG, d3)

    # ── Europe (Germany proxy) ──
    deu = by_country.get("DEU") or {}
    deu_phase = deu.get("phase")
    if deu_phase == "RECESSION":
        d4 = f"Germany in RECESSION (CLI {deu.get('cli_level')})"
        add(scores, evidence, "EFA", -STRONG, d4)
        add(scores, evidence, "EWG", -HARD, d4)
    elif deu_phase == "RECOVERY":
        d4 = f"Germany in RECOVERY (CLI {deu.get('cli_level')})"
        add(scores, evidence, "EFA", MEDIUM, d4)
        add(scores, evidence, "EWG", STRONG, d4)


# ─────────────────────────────────────────────────────────────────


def rule_crisis_composite(scores, evidence):
    """Master Crisis Composite (DEFCON) — the platform's headline risk read.
    High DEFCON tilts hard to defensives; all-clear tilts to risk."""
    d = fs3("data/crisis-composite.json")
    lvl = d.get("defcon_level")
    if lvl is None:
        return
    desc = f"DEFCON {lvl} ({d.get('defcon_name','')})"
    if lvl <= 2:            # crisis / high stress
        for a in ("SPY", "QQQ", "IWM", "EEM", "HYG", "BTC"):
            add(scores, evidence, a, -HARD, desc)
        for a in ("TLT", "IEF", "GLD", "UUP", "VXX"):
            add(scores, evidence, a, STRONG, desc)
    elif lvl == 3:          # elevated
        for a in ("SPY", "QQQ", "IWM", "EEM", "HYG"):
            add(scores, evidence, a, -MEDIUM, desc)
        for a in ("TLT", "IEF", "GLD"):
            add(scores, evidence, a, MEDIUM, desc)
    elif lvl == 5:          # all-clear / risk-on
        for a in ("SPY", "QQQ", "IWM", "EEM", "HYG"):
            add(scores, evidence, a, MEDIUM, desc)
        add(scores, evidence, "VXX", -STRONG, desc)


def rule_capitulation(scores, evidence):
    """Capitulation engine — a GENERATIONAL/STRONG buy is the rare washout
    entry; tilt aggressively to risk when it fires with stabilisation."""
    d = fs3("data/capitulation.json")
    sig = d.get("signal")
    if sig not in ("GENERATIONAL_BUY", "STRONG_BUY"):
        return
    desc = f"capitulation {sig}"
    mag = HARD if sig == "GENERATIONAL_BUY" else STRONG
    for a in ("SPY", "QQQ", "IWM", "EEM", "HYG"):
        add(scores, evidence, a, mag, desc)
    add(scores, evidence, "VXX", -STRONG, desc)
    add(scores, evidence, "GLD", MEDIUM, desc)  # quality hedge still warranted


def rule_leading_markets(scores, evidence):
    """Leading/canary markets — the turning-point signal and which bucket is
    flashing reshape the cyclical vs defensive tilt."""
    d = fs3("data/leading-markets.json")
    sig = d.get("turning_point_signal")
    if not sig:
        return
    desc = f"canary {sig}"
    if sig == "TOP_WARNING":
        for a in ("SPY", "QQQ", "IWM", "EEM"):
            add(scores, evidence, a, -MEDIUM, desc)
    elif sig == "BROAD_CONTRACTION":
        for a in ("SPY", "QQQ", "IWM", "EEM", "HYG"):
            add(scores, evidence, a, -STRONG, desc)
        add(scores, evidence, "TLT", MEDIUM, desc)
    elif sig in ("EXPANSION_CONFIRMED", "BOTTOM_SIGNAL"):
        for a in ("SPY", "IWM", "EEM"):
            add(scores, evidence, a, MEDIUM, desc)
    # bucket-specific: commodity-cycle flashing -> fade DBC/EEM
    flashing = d.get("flashing_buckets") or []
    if "commodity_cycle" in flashing:
        add(scores, evidence, "DBC", -STRONG, "commodity-cycle canaries flashing")
        add(scores, evidence, "EEM", -MEDIUM, "commodity-cycle canaries flashing")
    if "credit_stress" in flashing:
        add(scores, evidence, "HYG", -STRONG, "credit-stress canaries flashing")


def rule_global_liquidity(scores, evidence):
    """Global central-bank liquidity tide — expanding lifts risk, contracting
    fades it. China credit impulse reinforces the commodity complex."""
    d = fs3("data/global-liquidity.json")
    reg = d.get("regime")
    if reg:
        desc = f"global liquidity {reg}"
        if reg in ("EXPANDING", "EASING"):
            for a in ("SPY", "QQQ", "EEM", "GLD", "BTC"):
                add(scores, evidence, a, MEDIUM, desc)
        elif reg in ("CONTRACTING", "TIGHTENING"):
            for a in ("SPY", "QQQ", "EEM", "HYG", "BTC"):
                add(scores, evidence, a, -MEDIUM, desc)
    ch = fs3("data/china-liquidity.json")
    creg = ch.get("regime")
    if creg == "EASING":
        add(scores, evidence, "DBC", STRONG, "China liquidity easing")
        add(scores, evidence, "EEM", MEDIUM, "China liquidity easing")
    elif creg == "TIGHTENING":
        add(scores, evidence, "DBC", -MEDIUM, "China liquidity tightening")


RULES = [
    ("macro_surprise", rule_macro_surprise),
    ("yield_curve", rule_yield_curve),
    ("sector_breadth", rule_sector_breadth),
    ("correlation_regime", rule_correlation_regime),
    ("eurodollar_stress", rule_eurodollar_stress),
    ("auction_crisis", rule_auction_crisis),
    ("liquidity_credit_engine", rule_liquidity_credit_engine),
    ("tenor_signals", rule_tenor_signals),
    ("global_business_cycle", rule_global_business_cycle),
    ("crisis_composite", rule_crisis_composite),
    ("capitulation", rule_capitulation),
    ("leading_markets", rule_leading_markets),
    ("global_liquidity", rule_global_liquidity),
    ("historical_analogs", rule_historical_analogs),
    ("event_study", rule_event_study),
    ("btc_signals", rule_btc_signals),
    ("sector_momentum", rule_sector_momentum),
]


def conviction_label(score):
    a = abs(score)
    if a >= 30:
        return "HIGH"
    elif a >= 15:
        return "MEDIUM"
    elif a >= 5:
        return "LOW"
    return "FLAT"


def call_label(score):
    if score >= 20:
        return "OVERWEIGHT"
    elif score >= 5:
        return "TILT_LONG"
    elif score <= -20:
        return "UNDERWEIGHT"
    elif score <= -5:
        return "TILT_SHORT"
    return "NEUTRAL"


def asset_metadata():
    return {
        "SPY": {"name": "S&P 500", "class": "EQUITY_US_LARGE", "emoji": "📈"},
        "QQQ": {"name": "Nasdaq 100", "class": "EQUITY_US_TECH", "emoji": "💻"},
        "IWM": {"name": "Russell 2000", "class": "EQUITY_US_SMALL", "emoji": "🏢"},
        "EFA": {"name": "EAFE Developed", "class": "EQUITY_INTL_DM", "emoji": "🌐"},
        "EEM": {"name": "Emerging Markets", "class": "EQUITY_INTL_EM", "emoji": "🌍"},
        "TLT": {"name": "20+ Year Treasury", "class": "BOND_LONG", "emoji": "🏛️"},
        "IEF": {"name": "7-10 Year Treasury", "class": "BOND_INTERMEDIATE", "emoji": "📜"},
        "HYG": {"name": "High Yield Credit", "class": "CREDIT_HY", "emoji": "💳"},
        "GLD": {"name": "Gold", "class": "COMMODITY_GOLD", "emoji": "🥇"},
        "DBC": {"name": "Broad Commodities", "class": "COMMODITY_BROAD", "emoji": "🌾"},
        "UUP": {"name": "US Dollar", "class": "FX_USD", "emoji": "💵"},
        "VXX": {"name": "VIX Futures", "class": "VOLATILITY", "emoji": "📊"},
        "BTC": {"name": "Bitcoin", "class": "CRYPTO", "emoji": "₿"},
    }


def lambda_handler(event=None, context=None):
    started = time.time()
    print(f"[allocator] start")

    scores = empty_scores()
    evidence = empty_evidence()
    rule_results = {}

    for name, fn in RULES:
        try:
            before = sum(abs(v) for v in scores.values())
            fn(scores, evidence)
            after = sum(abs(v) for v in scores.values())
            rule_results[name] = {"applied": True, "tilt_added": round(after - before, 2)}
        except Exception as e:
            rule_results[name] = {"applied": False, "error": str(e)}
            print(f"[allocator] {name} ERROR: {e}")

    # Build output
    meta = asset_metadata()
    asset_views = []
    for ticker in ASSETS:
        s = scores[ticker]
        ev = evidence[ticker]
        m = meta.get(ticker, {})
        asset_views.append({
            "ticker": ticker,
            "name": m.get("name"),
            "class": m.get("class"),
            "emoji": m.get("emoji"),
            "score": round(s, 2),
            "call": call_label(s),
            "conviction": conviction_label(s),
            "n_signals": len(ev),
            "evidence": ev,
        })

    asset_views.sort(key=lambda x: -x["score"])

    # Recommended weights — only positive scores get weight
    positives = [a for a in asset_views if a["score"] > 0]
    total_pos = sum(a["score"] for a in positives)
    # Cash buffer: based on max negative score (more bearish → more cash)
    max_neg = max([abs(a["score"]) for a in asset_views if a["score"] < 0] + [0])
    cash_pct = min(50, max(5, max_neg))  # 5-50% cash range
    deployable = 100 - cash_pct

    weights = {}
    if total_pos > 0:
        for a in positives:
            weights[a["ticker"]] = round(a["score"] / total_pos * deployable, 1)
    weights["CASH"] = round(cash_pct, 1)

    # Top regime synthesis: pick most-supported call across all assets
    overweights = [a for a in asset_views if a["call"] == "OVERWEIGHT"]
    underweights = [a for a in asset_views if a["call"] == "UNDERWEIGHT"]

    # Headline regime synthesis
    if any(a["ticker"] == "SPY" and a["score"] >= 15 for a in asset_views):
        regime_headline = "RISK_ON"
    elif any(a["ticker"] == "SPY" and a["score"] <= -15 for a in asset_views):
        if any(a["ticker"] == "TLT" and a["score"] >= 10 for a in asset_views):
            regime_headline = "RISK_OFF_FLIGHT_TO_QUALITY"
        elif any(a["ticker"] == "GLD" and a["score"] >= 10 for a in asset_views):
            regime_headline = "RISK_OFF_GOLD_HEDGE"
        else:
            regime_headline = "RISK_OFF"
    else:
        regime_headline = "BALANCED_NEUTRAL"

    out = {
        "version": "1.0",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "duration_s": round(time.time() - started, 2),
        "regime_headline": regime_headline,
        "n_rules_applied": sum(1 for r in rule_results.values() if r.get("applied")),
        "n_rules_total": len(RULES),
        "asset_scores": asset_views,
        "recommended_weights_pct": weights,
        "cash_buffer_pct": cash_pct,
        "overweights": [a["ticker"] for a in overweights],
        "underweights": [a["ticker"] for a in underweights],
        "rule_results": rule_results,
    }

    body = json.dumps(out, default=str).encode("utf-8")
    S3.put_object(Bucket=BUCKET, Key=KEY, Body=body, ContentType="application/json", CacheControl="public, max-age=900")
    print(f"[allocator] wrote {len(body):,}b — regime={regime_headline} OW={overweights[:3]}... UW={underweights[:3]}...")

    return {
        "statusCode": 200,
        "body": json.dumps({
            "regime": regime_headline,
            "n_overweights": len(overweights),
            "n_underweights": len(underweights),
            "cash_pct": cash_pct,
            "duration_s": out["duration_s"],
        }),
    }


if __name__ == "__main__":
    print(json.dumps(lambda_handler({}, None), indent=2))
