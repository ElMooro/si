"""
justhodl-alpha-score — The Spine

═══════════════════════════════════════════════════════════════════════
WHY THIS EXISTS
───────────────
The screener has 138 fields per stock. Each one is a partial truth.
This Lambda combines them into ONE actionable alpha score per stock,
ranked S/A/B/C/D, so you don't have to mentally weigh 40 columns when
deciding "is this stock worth attention right now?"

═══════════════════════════════════════════════════════════════════════
THE 8-FACTOR MODEL
──────────────────
Each factor scaled 0-100. Weighted composite = final alpha score 0-100.

  QUALITY       (16%) — Piotroski, Altman Z, ROIC, margins, sustainability
  GROWTH        (17%) — Revenue/EPS/FCF growth, beat streak
  MOMENTUM      (14%) — Price returns 1m/3m/6m, MA200 relationship
  SMART MONEY   (16%) — Famous-fund concentration (from smart-money sidecar)
  SENTIMENT     (10%) — AI news sentiment (from sentiment sidecar)
  ANALYSTS      ( 8%) — Grades consensus, recent upgrades, PT upside, DCF upside
  INSIDERS      (11%) — Insider+political buying, cluster signals
  OPTIONS FLOW  ( 8%) — Institutional options + short-interest squeeze setups

Total: 100%. Weights are configurable for future calibration.

═══════════════════════════════════════════════════════════════════════
OUTPUTS (S3: screener/alpha-score.json)
───────────────────────────────────────
  Top-level: generated_at, model_version, count, tier distribution, weights
  Per stock:
    - alpha_score          (0-100)
    - tier                 (S=90+ · A=80+ · B=70+ · C=50+ · D<50)
    - rank                 (1=best of 503)
    - components           (the 7 factor scores)
    - components_coverage  (% of factors with real data, transparency)
    - top_signals          (2-4 human-readable bullets: WHY this stock scores high)
    - risk_flags           (any red flags: distress, fraud, leverage, etc.)

═══════════════════════════════════════════════════════════════════════
RUNTIME
───────
Pure math. No external API calls. Reads 3 sidecars from S3, computes,
writes 1 sidecar back. <5 seconds for 503 stocks. <\$0.01 per run.
Schedule: hourly. Trigger: EventBridge cron(7 * * * ? *) (7 past each hour
to land after sentiment/smart-money/screener refreshes).
"""
import json
import os
import time
from datetime import datetime, timezone

import boto3

S3_BUCKET = "justhodl-dashboard-live"
SCREENER_KEY = "screener/data.json"
SENTIMENT_KEY = "sentiment/data.json"
SMART_MONEY_KEY = "screener/smart-money-holdings.json"
OPTIONS_FLOW_KEY = "data/options-flow.json"
ALPHA_WEIGHTS_KEY = "screener/alpha-weights.json"   # optional override from calibrator (#1)
OUTPUT_KEY = "screener/alpha-score.json"

MODEL_VERSION = "1.2.0"  # adds optional dynamic weight loading from calibrator

# Factor weights — must sum to 1.0
# Rebalanced 2026-05-12 to incorporate options-flow as the 8th factor.
# Trimmed 0.02 from quality+smart_money (both well-covered), 0.01 from
# growth+momentum+analysts+insiders. Options flow is a smart-trader signal
# (similar in spirit to smart_money) so 0.08 is the natural weight.
WEIGHTS = {
    "quality":      0.16,   # -0.02
    "growth":       0.17,   # -0.01
    "momentum":     0.14,   # -0.01
    "smart_money":  0.16,   # -0.02
    "sentiment":    0.10,   # unchanged
    "analysts":     0.08,   # -0.01
    "insiders":     0.11,   # -0.01
    "options_flow": 0.08,   # NEW — institutional options + short-interest signal
}
assert abs(sum(WEIGHTS.values()) - 1.0) < 1e-6, "Weights must sum to 1.0"

s3 = boto3.client("s3", region_name="us-east-1")


# ═══════════════════════════════════════════════════════════════════════
# HELPERS — bound scores to 0-100, handle missing data
# ═══════════════════════════════════════════════════════════════════════

def clamp(v, lo=0, hi=100):
    if v is None: return None
    return max(lo, min(hi, v))


def linear_score(value, low, high, low_score=0, high_score=100):
    """Map value linearly: at <=low score is low_score, at >=high score is high_score.
    Returns None if value is None."""
    if value is None: return None
    if value <= low: return low_score
    if value >= high: return high_score
    return low_score + (high_score - low_score) * (value - low) / (high - low)


def safe_mean(scores):
    """Mean of populated scores (ignores None). Returns None if all None."""
    valid = [s for s in scores if s is not None]
    if not valid: return None
    return sum(valid) / len(valid)


# ═══════════════════════════════════════════════════════════════════════
# FACTOR SCORERS — each returns (score 0-100 or None, coverage 0-1)
# ═══════════════════════════════════════════════════════════════════════

def score_quality(s):
    """How financially healthy is this business?"""
    components = []

    # Piotroski F-Score: 0-9, higher = better
    piotroski = s.get("piotroski")
    if piotroski is not None:
        components.append(linear_score(piotroski, 0, 9))

    # Altman Z-Score: <1.8 distress, >3 safe; cap at 30
    altman = s.get("altmanZ")
    if altman is not None:
        components.append(linear_score(min(altman, 30), 0, 10))

    # ROIC: ≥30% best-in-class, ≤0% bad
    roic = s.get("roic")
    if roic is not None:
        components.append(linear_score(roic, -10, 30))

    # Operating margin: ≥30% great, ≤0% bad
    op_mar = s.get("operatingMargin")
    if op_mar is not None:
        components.append(linear_score(op_mar, -10, 35))

    # Net margin
    net_mar = s.get("netMargin")
    if net_mar is not None:
        components.append(linear_score(net_mar, -5, 25))

    # Sustainability bonus
    sus_quality = s.get("sustainableQuality")
    sus_3y = s.get("sustainable3y")
    if sus_quality is True or sus_3y is True:
        components.append(85)
    elif sus_quality is False and sus_3y is False:
        components.append(35)

    # Interest coverage: how easily can they pay debt interest? >5 good
    ic = s.get("interestCoverage")
    if ic is not None:
        components.append(linear_score(ic, 0, 15))

    score = safe_mean(components)
    coverage = len(components) / 7
    return score, coverage


def score_growth(s):
    """How fast is the business growing?"""
    components = []

    # Revenue growth (annual): 30% great, 0% neutral, negative bad
    rev_g = s.get("revenueGrowth")
    if rev_g is not None:
        components.append(linear_score(rev_g, -10, 30))

    # EPS growth
    eps_g = s.get("epsGrowth")
    if eps_g is not None:
        components.append(linear_score(eps_g, -20, 40))

    # Free cash flow growth
    fcf_g = s.get("fcfGrowth")
    if fcf_g is not None:
        components.append(linear_score(fcf_g, -20, 40))

    # 3-year revenue CAGR — more stable than 1y
    cagr = s.get("rev3yCAGR")
    if cagr is not None:
        components.append(linear_score(cagr, -5, 25))

    # Beat streak: consecutive earnings beats
    beats = s.get("beatStreak")
    if beats is not None:
        components.append(linear_score(beats, 0, 8))

    # Forward revenue growth (analyst projection)
    fwd_g = s.get("forwardRevenueGrowth")
    if fwd_g is not None:
        components.append(linear_score(fwd_g, -5, 25))

    score = safe_mean(components)
    coverage = len(components) / 6
    return score, coverage


def score_value(s):
    """Is this stock cheap relative to its fundamentals?
    Note: NOT a top-level factor — bundled into the absolute Alpha Score is
    debatable. Some growth stocks are 'expensive' but earning the multiple.
    We include valuation indirectly via DCF upside in analysts."""
    components = []
    pe = s.get("peRatio")
    if pe is not None and pe > 0:
        # Lower P/E = higher value score. <10 great, >50 bad
        components.append(linear_score(pe, 50, 10))  # inverted
    ev = s.get("evEbitda")
    if ev is not None and ev > 0:
        components.append(linear_score(ev, 30, 8))  # inverted

    return safe_mean(components), len(components) / 2


def score_momentum(s):
    """How strong is the recent price trend?"""
    components = []

    # 1-month change: ±15% range
    c1m = s.get("chg1m")
    if c1m is not None:
        components.append(linear_score(c1m, -15, 15))

    # 3-month change: ±30%
    c3m = s.get("chg3m")
    if c3m is not None:
        components.append(linear_score(c3m, -30, 30))

    # 6-month change: ±50%
    c6m = s.get("chg6m")
    if c6m is not None:
        components.append(linear_score(c6m, -50, 50))

    # 1-year change: ±60%
    c1y = s.get("chg1y")
    if c1y is not None:
        components.append(linear_score(c1y, -60, 80))

    # Price vs SMA200: above = bullish trend
    price = s.get("price")
    sma200 = s.get("sma200")
    if price and sma200 and sma200 > 0:
        # +20% above SMA = 100, -20% below = 0
        pct = (price / sma200 - 1) * 100
        components.append(linear_score(pct, -20, 20))

    # Recent cross signal bonus
    cross = s.get("crossSignal")
    cross_days = s.get("crossDaysAgo")
    if cross == "golden" and cross_days is not None and cross_days <= 30:
        components.append(95)  # recent golden cross
    elif cross == "death" and cross_days is not None and cross_days <= 30:
        components.append(15)  # recent death cross — bearish

    score = safe_mean(components)
    coverage = len(components) / 6
    return score, coverage


def score_smart_money(s, sm_entry):
    """How concentrated is famous-fund ownership in this stock?
    Combines our smart-money sidecar with the screener's native stealScore
    and institutional signal."""
    components = []

    # From smart-money sidecar (Stage 16): max concentration of any one fund
    if sm_entry and isinstance(sm_entry, dict):
        max_pct = sm_entry.get("max_pct_of_fund")
        if max_pct is not None:
            # Concentration tiering: 1%=50, 5%=80, 10%=95, 20%+=100
            if max_pct >= 20: components.append(100)
            elif max_pct >= 10: components.append(85 + (max_pct - 10))
            elif max_pct >= 5: components.append(70 + (max_pct - 5) * 3)
            elif max_pct >= 1: components.append(50 + (max_pct - 1) * 5)
            else: components.append(30 + max_pct * 20)

        # How many funds hold it as ≥5%? More = more conviction
        n_high = sm_entry.get("n_high_conviction") or 0
        if n_high >= 5: components.append(95)
        elif n_high >= 3: components.append(80)
        elif n_high >= 1: components.append(65)
        else:
            holders = sm_entry.get("holders") or []
            if len(holders) >= 10: components.append(55)  # widely held
            elif len(holders) >= 3: components.append(45)
            elif holders: components.append(35)

    # Native stealScore (composite already in screener — uses CFTC, options, dark pool, etc.)
    steal = s.get("stealScore")
    if steal is not None:
        components.append(linear_score(steal, 0, 100))

    # Institutional signal
    inst_signal = s.get("instSignal")
    if inst_signal == "buying": components.append(85)
    elif inst_signal == "selling": components.append(30)
    elif inst_signal == "neutral": components.append(55)

    # Institutional ownership change
    inst_chg = s.get("instSharesChangePct")
    if inst_chg is not None:
        # >+5% accumulation great, <-5% distribution bad
        components.append(linear_score(inst_chg, -10, 10))

    score = safe_mean(components)
    coverage = len(components) / 5
    return score, coverage


def score_sentiment(s, sent_entry):
    """AI-scored news sentiment + news flow."""
    components = []

    # AI sentiment from sentiment sidecar (Stage 17) — primary signal
    if sent_entry and isinstance(sent_entry, dict):
        ai_score = sent_entry.get("sentimentScore")
        if ai_score is not None:
            # Map -1.0..+1.0 to 0..100, more aggressive at extremes
            components.append(linear_score(ai_score, -1, 1))

    # News flow: more articles = more attention. Saturates at 20/week
    n7 = s.get("newsCount7d")
    if n7 is not None:
        # Some flow good, ~5-15/week ideal, 0 = invisible, 25+ = saturated
        if n7 == 0: components.append(40)
        else: components.append(linear_score(n7, 0, 15))

    # Heuristic sentiment fallback (already on screener; less reliable than AI)
    if not sent_entry:
        ns30 = s.get("newsSentiment30d")
        if ns30 is not None:
            # Already on -100..+100 scale
            components.append(linear_score(ns30, -50, 50))

    score = safe_mean(components)
    coverage = len(components) / 2 if sent_entry else len(components) / 1
    return score, min(coverage, 1.0)


def score_analysts(s):
    """Analyst consensus, recent upgrades/downgrades, price target upside, DCF upside."""
    components = []

    # Grades score (composite already on screener)
    grades_score = s.get("gradesScore")
    if grades_score is not None:
        components.append(linear_score(grades_score, 0, 5))

    # Net upgrades over 30d (recent revisions matter most)
    up_net = s.get("upgradeNet30d")
    if up_net is not None:
        components.append(linear_score(up_net, -3, 5))

    # Net upgrades 90d
    up90 = s.get("upgradeNet90d")
    if up90 is not None:
        components.append(linear_score(up90, -5, 8))

    # Price target upside %
    pt_up = s.get("priceTargetUpsidePct")
    if pt_up is not None:
        # +30% upside = 95, 0% = 50, -20% = 20
        components.append(linear_score(pt_up, -30, 50))

    # DCF fair value upside
    dcf_up = s.get("dcfUpsidePct")
    if dcf_up is not None:
        components.append(linear_score(dcf_up, -30, 50))

    score = safe_mean(components)
    coverage = len(components) / 5
    return score, coverage


def score_insiders(s):
    """Insider buying, political buying, cluster signals.
    Strong cluster signals = "people with non-public information are buying"."""
    components = []

    # Insider signal
    iss = s.get("insiderSignal")
    if iss == "buying": components.append(85)
    elif iss == "selling": components.append(30)
    elif iss == "neutral": components.append(55)

    # Cluster buy: multiple insiders buying simultaneously = strong signal
    if s.get("insiderClusterBuy") is True:
        components.append(95)

    # Net insider $ over 90d
    net = s.get("insiderNet90dUsd")
    if net is not None:
        # +$1M = good, -$1M = ignore (insiders sell for many reasons),
        # but +$5M = great
        if net >= 5_000_000: components.append(95)
        elif net >= 1_000_000: components.append(80)
        elif net >= 0: components.append(60)
        elif net >= -10_000_000: components.append(50)  # neutral — sells are noisy
        else: components.append(40)

    # Political signal (politicians sometimes have info)
    ps = s.get("politicalSignal")
    if ps == "buying": components.append(75)
    elif ps == "selling": components.append(40)

    # Political cluster buy
    if s.get("politicalClusterBuy") is True:
        components.append(85)

    # Senate buys recent
    sb = s.get("senateBuysN90d")
    if sb is not None and sb >= 2:
        components.append(75)

    score = safe_mean(components)
    coverage = min(1.0, len(components) / 4)
    return score, coverage


def score_options_flow(s, of_entry):
    """Options + short-interest flow signal from options-flow-scanner sidecar.

    The scanner already produces a 0-100 score per ticker combining:
      - Call/put ratio surge (CPR change vs 20d avg)
      - Heavy call volume (today's call vol > N× ATM avg)
      - Falling short interest (bears giving up — squeeze setup)
      - High IV percentile (premiums elevated, big move expected)

    Universe is currently ~150 tickers (limited Polygon options coverage),
    so most S&P 500 stocks have no data here. When missing, returns
    (None, 0) and alpha-score's existing re-weighting kicks in.
    """
    if not of_entry: return None, 0
    raw_score = of_entry.get("score")
    if raw_score is None: return None, 0

    # Coverage scales with tier strength so missing data isn't punished
    # but high-tier signals get full weight
    tier = of_entry.get("tier", "NEUTRAL")
    if tier == "TIER_A_BULLISH_FLOW":   coverage = 1.0
    elif tier == "TIER_B_FLOW_BUILDING": coverage = 0.95
    elif tier == "WATCH":                coverage = 0.85
    else:                                coverage = 0.75  # NEUTRAL still informative (low score)

    return float(raw_score), coverage


# ═══════════════════════════════════════════════════════════════════════
# SIGNAL DESCRIPTIONS — generate human-readable bullets
# ═══════════════════════════════════════════════════════════════════════

def generate_top_signals(s, sm_entry, sent_entry, of_entry, components):
    """Pick 2-4 bullet points that best explain why this stock scored where it did."""
    signals = []

    # Smart money: name the fund if concentration is real
    if components["smart_money"] is not None and components["smart_money"] >= 75 and sm_entry:
        holders = sm_entry.get("holders") or []
        if holders:
            top = sorted(holders, key=lambda h: -(h.get("pct_of_fund") or 0))[0]
            pct = top.get("pct_of_fund") or 0
            if pct >= 5:
                signals.append(f"★ Smart money: {top.get('name','')[:32]} holds at {pct:.1f}% of fund")
            elif sm_entry.get("n_high_conviction", 0) >= 2:
                signals.append(f"★ {sm_entry['n_high_conviction']} famous funds hold this as ≥5% of portfolio")
            else:
                n = len(holders)
                signals.append(f"🐳 {n} tracked institutional holders")

    # News sentiment
    if sent_entry and isinstance(sent_entry, dict):
        sig = sent_entry.get("sentimentSignal")
        reason = (sent_entry.get("sentimentReason") or "").strip()
        ai_score = sent_entry.get("sentimentScore") or 0
        if sig == "bullish" and ai_score >= 0.5:
            signals.append(f"🐂 News +{ai_score:.2f}: {reason[:80]}")
        elif sig == "bearish" and ai_score <= -0.4:
            signals.append(f"🐻 News {ai_score:.2f}: {reason[:80]}")

    # Quality
    pio = s.get("piotroski")
    if pio is not None and pio >= 8:
        signals.append(f"💎 Piotroski {pio}/9 — pristine financial quality")

    # Momentum
    c3m = s.get("chg3m")
    c1y = s.get("chg1y")
    if c3m is not None and c3m >= 25:
        signals.append(f"📈 Momentum: +{c3m:.1f}% over 90 days")
    elif c1y is not None and c1y >= 50 and (components.get("momentum") or 0) >= 70:
        signals.append(f"📈 Momentum: +{c1y:.1f}% over 1 year")

    # Insider cluster
    if s.get("insiderClusterBuy") is True:
        n = s.get("insiderBuyersN90d") or 0
        signals.append(f"🤝 Insider CLUSTER BUY: {n} insiders bought last 90d")

    # Analyst upside
    dcf_up = s.get("dcfUpsidePct")
    pt_up = s.get("priceTargetUpsidePct")
    if pt_up is not None and pt_up >= 20:
        signals.append(f"🎯 Analyst PT upside: +{pt_up:.0f}%")
    elif dcf_up is not None and dcf_up >= 20:
        signals.append(f"💰 DCF fair value upside: +{dcf_up:.0f}%")

    # Recent golden cross
    if s.get("crossSignal") == "golden" and (s.get("crossDaysAgo") or 999) <= 30:
        days = s.get("crossDaysAgo") or 0
        signals.append(f"🌟 Golden cross {days}d ago")

    # Growth
    rev_g = s.get("revenueGrowth")
    if rev_g is not None and rev_g >= 25:
        signals.append(f"🚀 Revenue +{rev_g:.0f}% YoY")

    # Beat streak
    beats = s.get("beatStreak")
    if beats is not None and beats >= 4:
        signals.append(f"✓ Beat earnings {beats} quarters in a row")

    # Options flow (TIER A bullish flow only — meaningful signal)
    if components.get("options_flow") is not None and components["options_flow"] >= 65 and of_entry:
        flags_list = of_entry.get("flags") or []
        of_metrics = of_entry.get("metrics") or {}
        cpr_chg = of_metrics.get("cpr_change_pct")
        call_surge = of_metrics.get("call_vol_surge")
        # Pick the most descriptive flag
        priority_order = ["HIGH_SHORT_SQUEEZE_SETUP", "CPR_SURGING", "CALL_VOL_3X",
                            "SHORTS_COVERING", "ABS_CPR_3X", "ABS_CPR_2X"]
        chosen_flag = next((f for f in priority_order if f in flags_list), None)
        flag_label = {
            "HIGH_SHORT_SQUEEZE_SETUP": "Squeeze setup",
            "CPR_SURGING": "Call/put surge",
            "CALL_VOL_3X": "Call volume 3×",
            "SHORTS_COVERING": "Shorts covering",
            "ABS_CPR_3X": "Heavy call skew",
            "ABS_CPR_2X": "Call skew",
        }.get(chosen_flag, "Bullish options flow")
        extras = []
        if cpr_chg is not None and cpr_chg > 100:
            extras.append(f"CPR +{cpr_chg:.0f}%")
        if call_surge is not None and call_surge > 3:
            extras.append(f"vol {call_surge:.1f}×")
        extra_str = f" ({', '.join(extras)})" if extras else ""
        signals.append(f"⚡ {flag_label}{extra_str}")

    return signals[:5]


def generate_risk_flags(s, sent_entry):
    """Red flags to surface alongside the score — ALWAYS show these."""
    flags = []

    # Financial distress
    altman = s.get("altmanZ")
    if altman is not None and altman < 1.8:
        flags.append(f"⚠ Altman Z {altman:.1f} — distress zone")

    # Negative earnings
    if (s.get("netIncome") or 0) < 0 and (s.get("revenue") or 0) > 0:
        flags.append("⚠ Currently unprofitable")

    # Fraud / lawsuit news
    if sent_entry and isinstance(sent_entry, dict):
        reason = (sent_entry.get("sentimentReason") or "").lower()
        if any(t in reason for t in ["fraud", "lawsuit", "sec investigation", "class action",
                                       "accounting", "delisted"]):
            flags.append("🚨 Fraud/lawsuit/SEC news flagged")

    # Earnings miss streak
    last_surprise = s.get("lastSurprisePct")
    if last_surprise is not None and last_surprise < -10:
        flags.append(f"❌ Last earnings missed by {last_surprise:.0f}%")

    # Heavy insider selling
    net = s.get("insiderNet90dUsd")
    if net is not None and net < -50_000_000:
        flags.append(f"📉 Insider net sell ${abs(net)/1e6:.0f}M last 90d")

    # Strong analyst downgrades
    down_net = s.get("upgradeNet30d")
    if down_net is not None and down_net <= -3:
        flags.append(f"📉 {abs(down_net)} net analyst downgrades last 30d")

    return flags[:4]


# ═══════════════════════════════════════════════════════════════════════
# TIER CLASSIFICATION
# ═══════════════════════════════════════════════════════════════════════

def classify_tier(score):
    if score is None: return "—"
    if score >= 90: return "S"
    if score >= 80: return "A"
    if score >= 70: return "B"
    if score >= 50: return "C"
    return "D"


# ═══════════════════════════════════════════════════════════════════════
# MAIN HANDLER
# ═══════════════════════════════════════════════════════════════════════

def lambda_handler(event, context):
    started = time.time()
    print(f"=== ALPHA SCORE ENGINE v{MODEL_VERSION} · {datetime.now(timezone.utc).isoformat()} ===")

    # 0. Optional dynamic weights override from calibrator (#1)
    #    Only applied if alpha-weights.json has auto_apply_calibrations=true AND
    #    active_weights sums to ~1.00 AND all 8 components present.
    #    Otherwise falls back to hardcoded WEIGHTS.
    active_weights = WEIGHTS
    weights_source = "hardcoded_default"
    weights_calibration_version = None
    weights_active_since = None
    try:
        aw = json.loads(s3.get_object(Bucket=S3_BUCKET, Key=ALPHA_WEIGHTS_KEY)["Body"].read())
        if aw.get("auto_apply_calibrations") is True:
            candidate = aw.get("active_weights") or {}
            # Safety gates
            if (isinstance(candidate, dict)
                    and set(candidate.keys()) == set(WEIGHTS.keys())
                    and abs(sum(candidate.values()) - 1.0) < 0.01
                    and all(isinstance(v, (int, float)) and 0.01 < v < 0.30 for v in candidate.values())):
                active_weights = {k: float(candidate[k]) for k in WEIGHTS}
                weights_source = "calibrator_active"
                weights_calibration_version = aw.get("last_calibration_version")
                weights_active_since = aw.get("active_since")
                print(f"  ✓ using calibrator weights (v{weights_calibration_version}, since {weights_active_since})")
            else:
                print(f"  ⚠ alpha-weights.json failed safety gates, using hardcoded defaults")
    except Exception as e:
        print(f"  alpha-weights sidecar unavailable (using defaults): {str(e)[:120]}")

    # 1. Load screener
    try:
        screener = json.loads(s3.get_object(Bucket=S3_BUCKET, Key=SCREENER_KEY)["Body"].read())
        stocks = screener.get("stocks") or []
    except Exception as e:
        return {"statusCode": 500, "body": json.dumps({"err": f"screener: {e}"})}

    # 2. Load sentiment (optional — fall back to heuristic if missing)
    sentiment_by_sym = {}
    try:
        sent = json.loads(s3.get_object(Bucket=S3_BUCKET, Key=SENTIMENT_KEY)["Body"].read())
        for s in (sent.get("sentiment") or []):
            sentiment_by_sym[s["symbol"]] = s
        sent_generated = sent.get("generated_at")
    except Exception as e:
        print(f"  sentiment unavailable: {e}")
        sent_generated = None

    # 3. Load smart-money (optional)
    sm_by_sym = {}
    try:
        sm = json.loads(s3.get_object(Bucket=S3_BUCKET, Key=SMART_MONEY_KEY)["Body"].read())
        sm_by_sym = sm.get("holdings") or {}
        sm_generated = sm.get("generated_at")
    except Exception as e:
        print(f"  smart-money unavailable: {e}")
        sm_generated = None

    # 4. Load options-flow (optional — daily refresh @21:30 UTC, ~150 ticker coverage)
    of_by_sym = {}
    of_generated = None
    try:
        of = json.loads(s3.get_object(Bucket=S3_BUCKET, Key=OPTIONS_FLOW_KEY)["Body"].read())
        for row in (of.get("all_qualifying") or []):
            sym = row.get("symbol")
            if sym: of_by_sym[sym] = row
        of_generated = of.get("generated_at")
    except Exception as e:
        print(f"  options-flow unavailable: {e}")

    print(f"  inputs: {len(stocks)} stocks · {len(sentiment_by_sym)} sentiment · "
          f"{len(sm_by_sym)} smart-money · {len(of_by_sym)} options-flow")

    # 5. Score each stock
    scored = []
    for s in stocks:
        sym = s.get("symbol")
        if not sym: continue

        sm_entry = sm_by_sym.get(sym)
        sent_entry = sentiment_by_sym.get(sym)
        of_entry = of_by_sym.get(sym)

        # 8 factor scores
        q_score, q_cov = score_quality(s)
        g_score, g_cov = score_growth(s)
        m_score, m_cov = score_momentum(s)
        sm_score, sm_cov = score_smart_money(s, sm_entry)
        sent_score, sent_cov = score_sentiment(s, sent_entry)
        an_score, an_cov = score_analysts(s)
        i_score, i_cov = score_insiders(s)
        of_score, of_cov = score_options_flow(s, of_entry)

        components = {
            "quality":      round(q_score) if q_score is not None else None,
            "growth":       round(g_score) if g_score is not None else None,
            "momentum":     round(m_score) if m_score is not None else None,
            "smart_money":  round(sm_score) if sm_score is not None else None,
            "sentiment":    round(sent_score) if sent_score is not None else None,
            "analysts":     round(an_score) if an_score is not None else None,
            "insiders":     round(i_score) if i_score is not None else None,
            "options_flow": round(of_score) if of_score is not None else None,
        }

        # Weighted alpha. Missing components are skipped + re-weight the rest.
        active_weight = sum(active_weights[k] for k, v in components.items() if v is not None)
        if active_weight == 0:
            alpha = None
        else:
            alpha = sum(components[k] * active_weights[k] for k in components if components[k] is not None) / active_weight
            alpha = round(alpha)

        # Coverage = how many of the 7 factors had data
        coverage = (q_cov + g_cov + m_cov + sm_cov + sent_cov + an_cov + i_cov) / 7

        top_signals = generate_top_signals(s, sm_entry, sent_entry, of_entry, components)
        risk_flags = generate_risk_flags(s, sent_entry)

        scored.append({
            "symbol": sym,
            "name": s.get("name", sym),
            "sector": s.get("sector"),
            "price": s.get("price"),
            "alpha_score": alpha,
            "tier": classify_tier(alpha),
            "components": components,
            "components_coverage": round(coverage, 2),
            "top_signals": top_signals,
            "risk_flags": risk_flags,
        })

    # 5. Rank
    ranked = sorted([s for s in scored if s["alpha_score"] is not None],
                    key=lambda s: -s["alpha_score"])
    for i, s in enumerate(ranked):
        s["rank"] = i + 1
    no_score = [s for s in scored if s["alpha_score"] is None]
    for s in no_score:
        s["rank"] = None
    all_stocks = ranked + no_score

    # 6. Tier counts
    tiers = {"S": 0, "A": 0, "B": 0, "C": 0, "D": 0}
    for s in all_stocks:
        if s["tier"] in tiers:
            tiers[s["tier"]] += 1

    elapsed = time.time() - started
    print(f"  scored {len(ranked)} of {len(stocks)} stocks · {elapsed:.1f}s")
    print(f"  tiers: S={tiers['S']} A={tiers['A']} B={tiers['B']} C={tiers['C']} D={tiers['D']}")

    # 7. Write sidecar
    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "generated_at_unix": int(time.time()),
        "model_version": MODEL_VERSION,
        "elapsed_seconds": round(elapsed, 2),
        "count": len(all_stocks),
        "scored_count": len(ranked),
        "tier_distribution": tiers,
        "weights": active_weights,
        "weights_source": weights_source,
        "weights_calibration_version": weights_calibration_version,
        "weights_active_since": weights_active_since,
        "weights_default_fallback": WEIGHTS,
        "inputs": {
            "screener_generated_at": screener.get("generated_at"),
            "sentiment_generated_at": sent_generated,
            "smart_money_generated_at": sm_generated,
            "options_flow_generated_at": of_generated,
            "stocks_with_sentiment": len(sentiment_by_sym),
            "stocks_with_smart_money": len(sm_by_sym),
            "stocks_with_options_flow": len(of_by_sym),
        },
        "stocks": all_stocks,
    }

    try:
        s3.put_object(
            Bucket=S3_BUCKET, Key=OUTPUT_KEY,
            Body=json.dumps(payload, separators=(",", ":")).encode("utf-8"),
            ContentType="application/json",
            CacheControl="public, max-age=1800")
        print(f"  wrote {round(len(json.dumps(payload))/1024,1)} KB to s3://{S3_BUCKET}/{OUTPUT_KEY}")
    except Exception as e:
        return {"statusCode": 500, "body": json.dumps({"err": f"s3 put: {e}"})}

    return {"statusCode": 200, "body": json.dumps({
        "success": True,
        "scored": len(ranked),
        "tier_distribution": tiers,
        "elapsed_seconds": round(elapsed, 2),
    })}
