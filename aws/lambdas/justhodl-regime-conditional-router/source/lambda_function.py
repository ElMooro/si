"""
justhodl-regime-conditional-router -- Crisis KB framework -> sleeve allocator.

═══════════════════════════════════════════════════════════════════════════════
INSTITUTIONAL THESIS
────────────────────
Most portfolio managers operate with binary risk-on/risk-off. The Crisis KB
v2.0 (16 codified frameworks from 1,091 expert rules) is a far more nuanced
regime taxonomy — each framework has a mathematically optimal sleeve. The
existing allocator stack (desk-allocator + master-allocator + pm-decision)
weighs CAPITAL across desks but does NOT translate Crisis KB framework
elevation into specific TRADE STRUCTURE.

This router closes that loop: when crisis-composite or eurodollar-stress or
auction-crisis-detector elevates a specific framework, the router outputs the
exact sleeve mapping (longs / shorts / hedges / sizing) that the framework
mandates.

Bridgewater has an internal version of this ("All-Weather adjusted to
regime"). PIMCO has cruder regime models. No commercial product exposes this
at retail or boutique.

THE 8 PRIMARY FRAMEWORK -> SLEEVE MAPPINGS
───────────────────────────────────────────
Each framework, when elevated, mandates:

  1. EURODOLLAR_STRESS (offshore USD funding pressure)
     LONG: USD broadly (UUP), defensives (XLU, XLP), short-duration UST (BIL)
     SHORT: EM credit (EMB), copper (CPER), EM equities (EEM)
     HEDGE: long VXX 1-month, gold (GLD) tail
     SIZE: 60% of max equity allocation, 40% cash + UST

  2. TREASURY_AUCTION_CRISIS (long-end refinancing stress)
     LONG: gold (GLD), short-duration UST (BIL), financials (XLF — steepener),
           USD (UUP)
     SHORT: long-duration UST (TLT), small caps (IWM — refinancing risk),
            utilities (XLU — duration sensitive)
     HEDGE: long 30Y rate puts via TBT, long volatility (VXX)
     SIZE: 50% equity max, 50% cash; emphasize quality balance sheets

  3. DOLLAR_SHORTAGE_COLLATERAL (collateral chain unwind)
     LONG: USD (UUP) heavily, gold (GLD), short-duration UST (BIL)
     SHORT: copper (CPER), EM equities (EEM), KWEB, oil
     HEDGE: SPY puts 5% OTM 30d, long VXX
     SIZE: 40% equity max; emphasize US large-cap quality only

  4. DOLLAR_SMILE_LEFT (USD strength during global crisis)
     LONG: USD (UUP), US defensives (XLU, XLP, XLV), long-duration UST (TLT)
     SHORT: EM equities (EEM), commodities (GSG)
     HEDGE: long gold (GLD) as systemic tail
     SIZE: 70% defensive equity, 30% UST; reduce growth/tech

  5. DOLLAR_SMILE_RIGHT (USD strength during US outperformance)
     LONG: US growth/tech (QQQ, XLK), USD (UUP), small caps (IWM)
     SHORT: EM (EEM), EAFE (EFA)
     HEDGE: minimal — risk-on regime
     SIZE: 100% equity, US-tilted; can lever in conviction

  6. PLANT_AND_HARVEST (early-cycle accumulation phase)
     LONG: small caps (IWM), cyclicals (XLI, XLB), value (IWD), banks (XLF)
     SHORT: defensives (XLU — relative underperform)
     HEDGE: minimal
     SIZE: 100% equity; tilt to cyclicals; consider modest leverage

  7. US10Y_AT_5PCT (rate shock regime)
     LONG: short-duration UST (BIL), value (IWD), financials (XLF), USD (UUP)
     SHORT: long-duration UST (TLT), growth/tech (QQQ — multiple compression),
            utilities (XLU), homebuilders (XHB)
     HEDGE: long volatility, long gold tail
     SIZE: 50-60% equity, value-tilted; 40-50% cash + BIL

  8. PERMANENT_PORTFOLIO (uncertainty / no clear regime)
     LONG: 25% equity (VTI), 25% LT UST (TLT), 25% gold (GLD), 25% cash (BIL)
     SHORT: none
     HEDGE: built into the structure (Harry Browne 1981)
     SIZE: 100% deployed across 4 sleeves

CROSS-ENGINE INPUTS (already running)
──────────────────────────────────────
  data/crisis-composite.json       — top-level crisis score + framework heat
  data/eurodollar-stress.json      — offshore USD funding stress 0-100
  data/auction-crisis.json         — Treasury auction stress
  data/dollar-radar.json           — USD trend + stance
  data/global-stress.json          — global stress composite
  data/canary-grid.json            — leading ex-US early warning
  data/signal-board.json           — composite posture across 7 engines
  data/master-allocation.json      — capital allocation context (informs not
                                     overrides this router's mapping)

OUTPUT
──────
  s3://justhodl-dashboard-live/data/regime-conditional-router.json
  Schedule: every 2h during US trading hours, 4h overnight

DISTINCTION FROM EXISTING ALLOCATORS
─────────────────────────────────────
  justhodl-desk-allocator   — sizes 7 strategy desks by tail-aware inverse-vol
  justhodl-master-allocator — capital allocation across signal-board postures
  justhodl-pm-decision      — fuses macro + allocator + portfolio risk
  
  THIS engine — Framework-specific TRADE STRUCTURE (longs/shorts/hedges/sizing).
  It does NOT replace the allocators; it provides the GRANULAR sleeve recipe
  that allocators use to compose actual positions.

ACADEMIC BASIS
──────────────
- Browne, Harry (1981). Fail-Safe Investing. Origin of Permanent Portfolio.
- Bridgewater Research (2015-2020). All-Weather adjusted for inflation/growth.
- Rey, Helene (2013). Dilemma not Trilemma: USD as global cycle driver.
- Pozsar, Zoltan (2014-2024). Money market plumbing + collateral chains.
═══════════════════════════════════════════════════════════════════════════════
"""
import json
import os
import time
from datetime import datetime, timezone

import boto3

VERSION = "1.0.0"
S3_BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
S3_KEY = "data/regime-conditional-router.json"

s3 = boto3.client("s3", region_name="us-east-1")

# ---------- Framework -> Sleeve Mappings ----------
FRAMEWORK_SLEEVES = {
    "EURODOLLAR_STRESS": {
        "name": "Eurodollar Stress (offshore USD funding)",
        "regime": "DEFENSIVE_USD_LONG",
        "long": ["UUP", "XLU", "XLP", "BIL"],
        "short": ["EMB", "CPER", "EEM"],
        "hedge": ["VXX", "GLD"],
        "max_equity_pct": 60,
        "cash_pct": 40,
        "horizon_days": 30,
        "thesis": ("Offshore USD funding stress. Reduce risk; rotate to US "
                    "defensives + USD. Short EM credit and EM equity. "
                    "Tail-hedge with VIX longs."),
    },
    "TREASURY_AUCTION_CRISIS": {
        "name": "Treasury Auction Crisis (long-end refinancing stress)",
        "regime": "DEFENSIVE_QUALITY_BS",
        "long": ["GLD", "BIL", "XLF", "UUP"],
        "short": ["TLT", "IWM", "XLU"],
        "hedge": ["TBT", "VXX"],
        "max_equity_pct": 50,
        "cash_pct": 50,
        "horizon_days": 60,
        "thesis": ("Long-end UST under stress. Steepener trade: short TLT "
                    "long XLF. Avoid small caps (refi risk) and rate-sensitive "
                    "utilities. Gold as monetary debasement hedge."),
    },
    "DOLLAR_SHORTAGE_COLLATERAL": {
        "name": "Dollar Shortage Collateral (collateral chain unwind)",
        "regime": "EXTREME_DEFENSIVE",
        "long": ["UUP", "GLD", "BIL"],
        "short": ["CPER", "EEM", "KWEB"],
        "hedge": ["SPY_PUTS_5PCT_OTM_30D", "VXX"],
        "max_equity_pct": 40,
        "cash_pct": 60,
        "horizon_days": 30,
        "thesis": ("Collateral chain unwind — broad dollar strength + flight "
                    "to safety. US large-cap quality only on equity sleeve. "
                    "Tail-hedge mandatory."),
    },
    "DOLLAR_SMILE_LEFT": {
        "name": "Dollar Smile Left (USD strong on global stress)",
        "regime": "DEFENSIVE_DURATION_LONG",
        "long": ["UUP", "XLU", "XLP", "XLV", "TLT"],
        "short": ["EEM", "GSG"],
        "hedge": ["GLD"],
        "max_equity_pct": 70,
        "cash_pct": 30,
        "horizon_days": 60,
        "thesis": ("USD strong during global risk-off. Long-duration UST "
                    "rally; defensive equity outperforms. Reduce growth/tech."),
    },
    "DOLLAR_SMILE_RIGHT": {
        "name": "Dollar Smile Right (USD strong on US outperformance)",
        "regime": "RISK_ON_US_TILT",
        "long": ["QQQ", "XLK", "UUP", "IWM"],
        "short": ["EEM", "EFA"],
        "hedge": [],
        "max_equity_pct": 100,
        "cash_pct": 0,
        "horizon_days": 90,
        "thesis": ("USD strong because US economy outperforming. Lean into "
                    "growth/tech, US small caps. Avoid international EM/EAFE."),
    },
    "PLANT_AND_HARVEST": {
        "name": "Plant and Harvest (early-cycle accumulation)",
        "regime": "RISK_ON_VALUE_CYCLICAL",
        "long": ["IWM", "XLI", "XLB", "IWD", "XLF"],
        "short": ["XLU"],
        "hedge": [],
        "max_equity_pct": 100,
        "cash_pct": 0,
        "horizon_days": 180,
        "thesis": ("Early-cycle accumulation. Small caps, cyclicals, value, "
                    "banks lead. Sell defensives. Consider modest leverage."),
    },
    "US10Y_AT_5PCT": {
        "name": "US 10Y at 5% (rate shock regime)",
        "regime": "DEFENSIVE_VALUE_SHORT_DURATION",
        "long": ["BIL", "IWD", "XLF", "UUP"],
        "short": ["TLT", "QQQ", "XLU", "XHB"],
        "hedge": ["VXX", "GLD"],
        "max_equity_pct": 55,
        "cash_pct": 45,
        "horizon_days": 60,
        "thesis": ("10Y rate shock. Multiple compression hits growth/tech "
                    "hardest. Rotate to value, financials. Short long-duration "
                    "UST and homebuilders. Cash + short-duration UST in size."),
    },
    "PERMANENT_PORTFOLIO": {
        "name": "Permanent Portfolio (no clear regime / max uncertainty)",
        "regime": "BALANCED_UNCERTAIN",
        "long": ["VTI", "TLT", "GLD", "BIL"],
        "short": [],
        "hedge": [],
        "long_weights_pct": {"VTI": 25, "TLT": 25, "GLD": 25, "BIL": 25},
        "max_equity_pct": 25,
        "cash_pct": 25,
        "horizon_days": 365,
        "thesis": ("Max uncertainty; no framework dominant. Browne 1981 "
                    "Permanent Portfolio: 25% each across stocks, long bonds, "
                    "gold, cash. Designed for any future economic state."),
    },
}


def fetch_s3_json(key):
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
        return json.loads(obj["Body"].read().decode("utf-8"))
    except Exception as e:
        print(f"[fetch] {key} failed: {e}")
        return None


def safe_get(d, *keys, default=None):
    if not isinstance(d, dict):
        return default
    cur = d
    for k in keys:
        if not isinstance(cur, dict):
            return default
        cur = cur.get(k)
        if cur is None:
            return default
    return cur


# ---------- Framework elevation detectors ----------
def detect_eurodollar_stress(eds, gs, ds):
    """Return (elevation_score 0-100, evidence_dict)."""
    score = safe_get(eds, "score") or safe_get(eds, "stress_score") or 0
    evidence = {
        "eurodollar_stress_score": score,
        "global_stress_score": safe_get(gs, "global_stress_index"),
        "dollar_stance": safe_get(ds, "stance"),
    }
    return (int(score) if isinstance(score, (int, float)) else 0), evidence


def detect_treasury_auction_crisis(ac, signal_board):
    score = safe_get(ac, "score") or safe_get(ac, "crisis_score") or 0
    state = safe_get(ac, "state")
    evidence = {
        "auction_crisis_score": score,
        "auction_state": state,
        "signal_board_posture": safe_get(signal_board, "posture"),
    }
    if isinstance(state, str) and "CRISIS" in state.upper():
        score = max(score, 75)
    return int(score) if isinstance(score, (int, float)) else 0, evidence


def detect_dollar_shortage(eds, ds, gs):
    """Combines eurodollar stress + dollar surge + global stress."""
    eds_score = safe_get(eds, "score") or 0
    dollar_score = safe_get(ds, "score") or safe_get(ds, "composite") or 0
    gs_score = safe_get(gs, "global_stress_index") or 0
    # Dollar shortage = simultaneously high eurodollar stress AND dollar surge
    combined = 0
    if isinstance(eds_score, (int, float)) and eds_score >= 60:
        if isinstance(dollar_score, (int, float)) and dollar_score >= 60:
            combined = min(95, (eds_score + dollar_score) / 2)
    evidence = {
        "eurodollar_score": eds_score,
        "dollar_score": dollar_score,
        "global_stress": gs_score,
        "combined_shortage_score": combined,
    }
    return int(combined), evidence


def detect_dollar_smile_left(ds, gs, crisis):
    """USD strong + global stress + crisis elevated."""
    dollar = safe_get(ds, "score") or 0
    gs_score = safe_get(gs, "global_stress_index") or 0
    crisis_score = safe_get(crisis, "score") or 0
    combined = 0
    if isinstance(dollar, (int, float)) and dollar >= 60:
        if (isinstance(gs_score, (int, float)) and gs_score >= 60) or \
           (isinstance(crisis_score, (int, float)) and crisis_score >= 50):
            combined = min(90, (dollar + max(gs_score, crisis_score)) / 2)
    evidence = {"dollar": dollar, "global_stress": gs_score,
                "crisis": crisis_score, "combined": combined}
    return int(combined), evidence


def detect_dollar_smile_right(ds, gs, crisis, signal_board):
    """USD strong + low global stress + risk-on posture."""
    dollar = safe_get(ds, "score") or 0
    gs_score = safe_get(gs, "global_stress_index") or 100
    crisis_score = safe_get(crisis, "score") or 100
    posture = safe_get(signal_board, "posture")
    combined = 0
    if isinstance(dollar, (int, float)) and dollar >= 55:
        if (isinstance(gs_score, (int, float)) and gs_score < 40) and \
           (isinstance(crisis_score, (int, float)) and crisis_score < 35):
            combined = min(90, dollar + 20)
    evidence = {"dollar": dollar, "global_stress": gs_score,
                "crisis": crisis_score, "posture": posture,
                "combined": combined}
    return int(combined), evidence


def detect_plant_and_harvest(signal_board, crisis, vol_radar):
    """Risk-on posture + low crisis + low vol."""
    posture = safe_get(signal_board, "posture")
    crisis_score = safe_get(crisis, "score") or 100
    vol = safe_get(vol_radar, "spike_risk_score") or 100
    combined = 0
    if isinstance(posture, str) and posture.upper() in (
            "RISK_ON", "EXPANSION", "BULLISH", "OFFENSIVE"):
        if isinstance(crisis_score, (int, float)) and crisis_score < 35:
            if isinstance(vol, (int, float)) and vol < 50:
                combined = 70
                if crisis_score < 25:
                    combined = 85
    evidence = {"posture": posture, "crisis": crisis_score,
                "vol_spike_risk": vol, "combined": combined}
    return int(combined), evidence


def detect_us10y_5pct(canary, signal_board, vol_radar):
    """Rate shock — DGS10 elevated + vol spiking + risk-off."""
    # canary-grid has signals about rates; canary_yield_signal could indicate
    yield_signal = safe_get(canary, "rates_signal") or safe_get(
        canary, "us10y_signal") or safe_get(canary, "yield_curve_signal")
    posture = safe_get(signal_board, "posture")
    vol = safe_get(vol_radar, "spike_risk_score") or 0
    combined = 0
    if isinstance(yield_signal, str) and yield_signal.upper() in (
            "SHOCK", "ELEVATED", "STRESSED", "5PCT_PLUS"):
        combined = 65
    if isinstance(vol, (int, float)) and vol >= 60:
        combined += 15
    if isinstance(posture, str) and posture.upper() in (
            "DEFENSIVE", "RISK_OFF", "BEARISH"):
        combined += 10
    combined = min(95, combined)
    evidence = {"yield_signal": yield_signal, "posture": posture,
                "vol_spike": vol, "combined": combined}
    return int(combined), evidence


def detect_permanent_portfolio(scores_so_far):
    """Permanent Portfolio is the DEFAULT when no other framework dominates.
    Fires when all other frameworks are < 50."""
    max_other = max(scores_so_far) if scores_so_far else 0
    if max_other < 50:
        return 80, {"reason": (
            f"No primary framework elevated (max other = {max_other}); "
            "default to balanced Permanent Portfolio.")}
    return 0, {"reason": f"Active framework at {max_other} — PP not needed"}


# ---------- Main ----------
def lambda_handler(event=None, context=None):
    started = time.time()
    print(f"[regime-router] start v{VERSION}")

    # Fetch all signal feeds
    eds = fetch_s3_json("data/eurodollar-stress.json")
    ac = fetch_s3_json("data/auction-crisis.json")
    ds = fetch_s3_json("data/dollar-radar.json")
    gs = fetch_s3_json("data/global-stress.json")
    crisis = fetch_s3_json("data/crisis-composite.json")
    canary = fetch_s3_json("data/canary-grid.json")
    signal_board = fetch_s3_json("data/signal-board.json")
    vol_radar = fetch_s3_json("data/vol-radar.json")
    master_alloc = fetch_s3_json("data/master-allocation.json")

    feeds_available = {
        "eurodollar_stress": eds is not None,
        "auction_crisis": ac is not None,
        "dollar_radar": ds is not None,
        "global_stress": gs is not None,
        "crisis_composite": crisis is not None,
        "canary_grid": canary is not None,
        "signal_board": signal_board is not None,
        "vol_radar": vol_radar is not None,
    }

    # Score each framework
    scores = {}
    evidence_map = {}
    for fwk, scorer in [
        ("EURODOLLAR_STRESS",
         lambda: detect_eurodollar_stress(eds, gs, ds)),
        ("TREASURY_AUCTION_CRISIS",
         lambda: detect_treasury_auction_crisis(ac, signal_board)),
        ("DOLLAR_SHORTAGE_COLLATERAL",
         lambda: detect_dollar_shortage(eds, ds, gs)),
        ("DOLLAR_SMILE_LEFT",
         lambda: detect_dollar_smile_left(ds, gs, crisis)),
        ("DOLLAR_SMILE_RIGHT",
         lambda: detect_dollar_smile_right(ds, gs, crisis, signal_board)),
        ("PLANT_AND_HARVEST",
         lambda: detect_plant_and_harvest(signal_board, crisis, vol_radar)),
        ("US10Y_AT_5PCT",
         lambda: detect_us10y_5pct(canary, signal_board, vol_radar)),
    ]:
        try:
            score, evidence = scorer()
        except Exception as e:
            score, evidence = 0, {"error": str(e)[:120]}
        scores[fwk] = score
        evidence_map[fwk] = evidence

    # Permanent Portfolio as default when nothing else dominates
    pp_score, pp_evidence = detect_permanent_portfolio(list(scores.values()))
    scores["PERMANENT_PORTFOLIO"] = pp_score
    evidence_map["PERMANENT_PORTFOLIO"] = pp_evidence

    # Determine primary framework (highest score, must be >=50 to be "active")
    sorted_fwks = sorted(scores.items(), key=lambda x: -x[1])
    primary = sorted_fwks[0]
    secondary = sorted_fwks[1] if len(sorted_fwks) > 1 else None

    primary_framework, primary_score = primary
    primary_active = primary_score >= 50
    if not primary_active:
        primary_framework = "PERMANENT_PORTFOLIO"
        primary_score = pp_score

    primary_sleeve = FRAMEWORK_SLEEVES.get(primary_framework, {})
    secondary_sleeve = (FRAMEWORK_SLEEVES.get(secondary[0])
                         if (secondary and secondary[1] >= 40) else None)

    # Construct actionable output
    rec_long = primary_sleeve.get("long", [])
    rec_short = primary_sleeve.get("short", [])
    rec_hedge = primary_sleeve.get("hedge", [])

    output = {
        "engine": "regime-conditional-router",
        "version": VERSION,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "primary_framework": primary_framework,
        "primary_framework_score": primary_score,
        "primary_framework_active": primary_active,
        "primary_regime": primary_sleeve.get("regime"),
        "primary_thesis": primary_sleeve.get("thesis"),
        "primary_sleeve": {
            "long": rec_long,
            "short": rec_short,
            "hedge": rec_hedge,
            "long_weights_pct": primary_sleeve.get("long_weights_pct"),
            "max_equity_pct": primary_sleeve.get("max_equity_pct"),
            "cash_pct": primary_sleeve.get("cash_pct"),
            "horizon_days": primary_sleeve.get("horizon_days"),
        },
        "secondary_framework": (secondary[0]
                                  if secondary and secondary[1] >= 40
                                  else None),
        "secondary_framework_score": (secondary[1]
                                       if secondary and secondary[1] >= 40
                                       else None),
        "secondary_sleeve": secondary_sleeve and {
            "regime": secondary_sleeve.get("regime"),
            "long": secondary_sleeve.get("long"),
            "short": secondary_sleeve.get("short"),
            "thesis": secondary_sleeve.get("thesis"),
        },
        "all_framework_scores": scores,
        "evidence_map": evidence_map,
        "feeds_available": feeds_available,
        "framework_universe": list(FRAMEWORK_SLEEVES.keys()),
        "methodology": {
            "framework": "Crisis KB framework -> sleeve mapping",
            "philosophy": (
                "Risk-on/risk-off is binary; the Crisis KB taxonomy has "
                "16 distinct frameworks each with optimal sleeve. This "
                "router maps elevated frameworks to specific trade "
                "structure (longs/shorts/hedges/sizing). Bridgewater + "
                "PIMCO run versions internally; not sold."),
            "scoring": ("Each framework scored 0-100 from underlying signal "
                          "engines. Score >= 50 = active. Highest active "
                          "framework = primary; 2nd-highest if >= 40 = "
                          "secondary. Permanent Portfolio is default when "
                          "no framework dominates."),
            "integration": (
                "This router OUTPUTS the sleeve recipe; the existing "
                "allocator stack (desk-allocator + master-allocator + "
                "pm-decision) consumes it to compose actual positions in "
                "the portfolio book."),
        },
        "academic_basis": [
            "Browne, Harry (1981). Fail-Safe Investing. Permanent Portfolio "
            "design.",
            "Bridgewater Research (2015-2020). All-Weather adjusted for "
            "inflation and growth regimes.",
            "Rey, Helene (2013). Dilemma not Trilemma — USD as global "
            "financial cycle driver.",
            "Pozsar, Zoltan (2014-2024). Money market plumbing, eurodollar "
            "collateral chains, dollar shortage mechanics.",
        ],
        "duration_seconds": round(time.time() - started, 1),
    }

    s3.put_object(
        Bucket=S3_BUCKET, Key=S3_KEY,
        Body=json.dumps(output, default=str).encode("utf-8"),
        ContentType="application/json",
        CacheControl="public, max-age=900")

    print(f"[regime-router] primary={primary_framework} "
          f"score={primary_score} active={primary_active}")

    return {
        "statusCode": 200,
        "body": json.dumps({
            "ok": True,
            "version": VERSION,
            "primary_framework": primary_framework,
            "primary_score": primary_score,
            "regime": primary_sleeve.get("regime"),
            "long_universe": rec_long,
            "short_universe": rec_short,
            "hedge_universe": rec_hedge,
        }),
    }


if __name__ == "__main__":
    print(json.dumps(lambda_handler(), indent=2))
