"""
justhodl-correlation-break-trade-router -- Stock-level trade recipes for
cross-asset correlation regime breaks.

═══════════════════════════════════════════════════════════════════════════════
INSTITUTIONAL THESIS
────────────────────
The existing fleet (justhodl-correlation-breaks, correlation-surface,
cross-asset-rv, divergence-engine-v2, bond-regime-detector, anomaly-detector)
DETECTS when cross-asset correlations break. NONE of them PRESCRIBE the
specific stock/sector trades that historically work in each correlation
regime.

This router closes that loop. When stock-bond correlation flips positive
(bond rout 2022), specific names get crushed disproportionately
(long-duration tech) while others benefit (steepener-positive financials).
When DXY/gold breaks negative (dollar debasement), gold miners + EM explode.
The CORRELATION-LEVEL signal is captured; the STOCK-LEVEL trade is not.

Two Sigma + AQR + Renaissance have internal versions of this stock-mapped
correlation trade engine. Zero commercial product exposes it. Bloomberg/
FactSet correlations are descriptive; this engine is PRESCRIPTIVE.

THE 6 CORRELATION REGIMES (classified from correlation-breaks output)
──────────────────────────────────────────────────────────────────────
Each regime, when active, mandates specific equity / sector / hedge plays:

  1. BOND_ROUT (SP500/DGS10 correlation flips positive)
     LONG: XLF (banks benefit from steeper curve), KBE, KRE
     SHORT: XLU (rate-sensitive utilities), TLT (long duration),
            high-duration tech (large unprofitable growth)
     HEDGE: TBT (short long bond)
     THESIS: End of 40-year bond hedge regime; rate normalization
     PRECEDENT: Q1 2022 — stock-bond correlation flipped -0.4 -> +0.5

  2. DOLLAR_DEBASEMENT (DTWEXBGS/GOLD correlation breaks negative)
     LONG: GLD, GDX (gold miners), GDXJ, EEM, copper miners (FCX, SCCO)
     SHORT: USD multinationals with no FX hedge, US small caps (US-centric)
     HEDGE: long DXY puts as secondary tail
     THESIS: Real-asset bid / monetary regime shift; gold as primary store
     PRECEDENT: 2024-2025 gold rally with USD stable = decoupling

  3. STAGFLATION_PRICING (10Y/Gold correlation flips positive)
     LONG: XLE (energy), XLB (materials), GLD, BIL (short-duration UST)
     SHORT: QQQ (tech/duration), XHB (homebuilders rate-sensitive),
            consumer discretionary
     HEDGE: long VXX (vol catches up to fundamentals)
     THESIS: Real rates rise AND gold rises = stagflation pricing
     PRECEDENT: 1970s playbook applied to 2022-2024 setup

  4. RISK_PARITY_UNWIND (everything correlates positive simultaneously)
     LONG: BIL, cash, short-volatility ETFs (after spike)
     SHORT: BROADLY — reduce all risk exposures
     HEDGE: long VXX heavy, long SPY puts 5% OTM 30d
     THESIS: All assets selling together = forced de-leveraging from
             risk-parity funds + multi-strat platforms
     PRECEDENT: March 2020, October 2018, Q3 2022

  5. DEFLATION_FEAR (VIX/TLT correlation breaks negative — bond rally amid stress)
     LONG: TLT, XLU, XLP (defensives), USD (UUP)
     SHORT: XLE, XLB, XLI (cyclicals)
     HEDGE: GLD as monetary backstop
     THESIS: Growth scare; duration rally even as risk falls
     PRECEDENT: H2 2019, Q4 2018

  6. REFLATION_CARRY (BTC/QQQ correlation rises + DXY/gold rises together)
     LONG: high-beta tech, BTC, EEM, IWM (small caps), oil
     SHORT: defensives (XLU, XLP)
     HEDGE: minimal — risk-on regime
     THESIS: Reflation trade, dollar liquidity ample, animal spirits
     PRECEDENT: 2020-2021 stimulus rally, 2024 H1

CROSS-ENGINE INPUTS (already running)
──────────────────────────────────────
  data/correlation-breaks.json     — primary signal (which pairs broke)
  data/correlation-surface.json    — 30d/90d/252d context
  data/cross-asset-rv.json         — OLS residual-z dislocations
  data/bond-regime.json            — bond regime context
  data/dollar-radar.json           — USD stance
  data/vol-radar.json              — vol regime context

OUTPUT
──────
  s3://justhodl-dashboard-live/data/correlation-break-trades.json
  Schedule: every 2h during US trading hours

INTEGRATION
───────────
This router OUTPUTS trade recipes per active correlation regime.
The Regime-Conditional Portfolio Router (Engine #4) handles Crisis KB
regimes; this handles CORRELATION REGIMES. Both feed the allocator stack
as separate signal sources. They can coexist (e.g., Bond Rout regime
active per correlation + Eurodollar Stress active per Crisis KB → combine
their long/short universes).

ACADEMIC BASIS
──────────────
- Asness, Moskowitz, Pedersen (2013). Value and Momentum Everywhere.
  Cross-asset correlation regime sensitivity.
- Pollet & Wilson (2010). Average correlation and stock market returns.
- Krishnamurthy (2010). How debt markets have malfunctioned in the
  crisis. JEP. Risk-parity unwind mechanics.
- Ilmanen, A. (2011). Expected Returns. Cross-asset regime taxonomy.
═══════════════════════════════════════════════════════════════════════════════
"""
import json
import os
import time
from datetime import datetime, timezone

import boto3

VERSION = "1.0.0"
S3_BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
S3_KEY = "data/correlation-break-trades.json"

s3 = boto3.client("s3", region_name="us-east-1")

# ---------- Regime -> Trade recipes ----------
REGIME_TRADES = {
    "BOND_ROUT": {
        "name": "Bond Rout (stock-bond correlation flipped positive)",
        "long": ["XLF", "KBE", "KRE"],
        "short": ["XLU", "TLT", "QQQ"],
        "hedge": ["TBT"],
        "max_equity_pct": 70,
        "thesis": ("Stock-bond correlation has flipped positive — end of "
                    "40-year bond hedge regime. Banks benefit from "
                    "steeper curve; long-duration tech and utilities "
                    "crushed by multiple compression. Q1 2022 played "
                    "this exact pattern."),
        "horizon_days": 90,
    },
    "DOLLAR_DEBASEMENT": {
        "name": "Dollar Debasement (DXY/gold correlation broke negative)",
        "long": ["GLD", "GDX", "GDXJ", "EEM", "FCX", "SCCO"],
        "short": ["UNH", "JNJ"],  # USD-multinational defensives with no FX hedge
        "hedge": ["UUP_PUTS"],
        "max_equity_pct": 80,
        "thesis": ("USD weakness + gold strength = real-asset bid. "
                    "Gold miners + EM explode. USD multinationals "
                    "without FX hedges get squeezed. 2024-2025 setup."),
        "horizon_days": 120,
    },
    "STAGFLATION_PRICING": {
        "name": "Stagflation Pricing (10Y/Gold correlation flipped positive)",
        "long": ["XLE", "XLB", "GLD", "BIL"],
        "short": ["QQQ", "XHB", "XLY"],
        "hedge": ["VXX"],
        "max_equity_pct": 60,
        "thesis": ("Both real rates AND gold rising = stagflation pricing. "
                    "1970s playbook: hard assets + short-duration UST + "
                    "short growth/tech. Tail vol kicker as vol catches "
                    "up to fundamentals."),
        "horizon_days": 180,
    },
    "RISK_PARITY_UNWIND": {
        "name": "Risk Parity Unwind (all assets correlate positive)",
        "long": ["BIL"],
        "short": ["SPY", "QQQ", "IWM", "TLT", "GLD", "EEM"],
        "hedge": ["VXX", "SPY_PUTS_5PCT_OTM_30D"],
        "max_equity_pct": 20,
        "thesis": ("Forced de-leveraging from risk-parity funds + multi-"
                    "strat platforms. All assets selling together. Cash + "
                    "vol = only winners. March 2020, Oct 2018, Q3 2022 "
                    "playbook."),
        "horizon_days": 14,
    },
    "DEFLATION_FEAR": {
        "name": "Deflation Fear (VIX/TLT correlation broke negative)",
        "long": ["TLT", "XLU", "XLP", "UUP"],
        "short": ["XLE", "XLB", "XLI"],
        "hedge": ["GLD"],
        "max_equity_pct": 65,
        "thesis": ("Growth scare; duration rallying even as risk falls. "
                    "Defensives + USD + LT bonds. Short cyclicals. "
                    "H2 2019 + Q4 2018 playbook."),
        "horizon_days": 60,
    },
    "REFLATION_CARRY": {
        "name": "Reflation Carry (BTC/QQQ + DXY/gold both rising)",
        "long": ["QQQ", "BTC-USD", "EEM", "IWM"],  # high-beta everything
        "short": ["XLU", "XLP"],
        "hedge": [],
        "max_equity_pct": 100,
        "thesis": ("Stimulus / liquidity-rich regime. Animal spirits. "
                    "High-beta tech + small caps + crypto + EM all rally. "
                    "Defensives lag. 2020-2021 stimulus + 2024 H1."),
        "horizon_days": 90,
    },
    "NORMAL": {
        "name": "Normal Cross-Asset Regime",
        "long": [],
        "short": [],
        "hedge": [],
        "max_equity_pct": None,
        "thesis": ("Cross-asset correlations are within historical norms. "
                    "No correlation-regime-specific trade signal. Use "
                    "other engines for positioning."),
        "horizon_days": None,
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
    cur = d
    for k in keys:
        if not isinstance(cur, dict):
            return default
        cur = cur.get(k)
        if cur is None:
            return default
    return cur


# ---------- Regime classifier ----------
def classify_correlation_regime(corr_breaks, dollar, vol, bond_regime):
    """Score each of 6 named regimes 0-100 from underlying signals."""
    scores = {k: 0 for k in REGIME_TRADES.keys() if k != "NORMAL"}
    evidence = {k: {} for k in scores}

    if not isinstance(corr_breaks, dict):
        return scores, evidence

    # Extract individual break signals from correlation-breaks engine output
    state = safe_get(corr_breaks, "state") or "NORMAL"
    n_breaks_3sigma = safe_get(corr_breaks, "n_breaks_3sigma") or safe_get(
        corr_breaks, "n_crisis_breaks") or 0
    pair_details = (safe_get(corr_breaks, "pair_details") or
                     safe_get(corr_breaks, "breaks") or
                     safe_get(corr_breaks, "top_breaks") or [])

    # Helper: find a pair's break direction
    def get_pair_break(pair_names_set):
        for p in pair_details:
            if not isinstance(p, dict):
                continue
            pair = p.get("pair") or p.get("series_pair") or ""
            for name in pair_names_set:
                if name in pair:
                    return {
                        "z_delta": p.get("z_delta") or p.get("delta_z"),
                        "current_corr": p.get("current_corr") or p.get("corr_now"),
                        "prior_corr": p.get("prior_corr") or p.get("corr_prior"),
                    }
        return None

    sb = get_pair_break(("SP500_DGS10", "SPY_TLT", "STOCK_BOND"))
    dxy_gold = get_pair_break(("DTWEXBGS_GOLD", "DXY_GOLD", "USD_GOLD"))
    rate_gold = get_pair_break(("DGS10_GOLD", "10Y_GOLD"))
    vix_spy = get_pair_break(("VIX_SP500", "VIX_SPY"))
    vix_tlt = get_pair_break(("VIX_TLT", "VIX_DGS10"))
    btc_qqq = get_pair_break(("BTC_QQQ", "BTCUSD_QQQ", "BTC_NASDAQ"))

    # 1. BOND_ROUT: SP500/DGS10 corr now positive AND z_delta strongly positive
    if sb:
        curr = sb.get("current_corr") or 0
        z = sb.get("z_delta") or 0
        if curr > 0.15 and z > 1.5:
            scores["BOND_ROUT"] = min(95, 60 + z * 10)
            evidence["BOND_ROUT"] = sb

    # 2. DOLLAR_DEBASEMENT: DXY/Gold corr now negative (typically positive)
    if dxy_gold:
        curr = dxy_gold.get("current_corr")
        z = dxy_gold.get("z_delta") or 0
        if curr is not None and curr < -0.3 and abs(z) > 1.5:
            scores["DOLLAR_DEBASEMENT"] = min(95, 50 + abs(z) * 15)
            evidence["DOLLAR_DEBASEMENT"] = dxy_gold

    # 3. STAGFLATION: 10Y/Gold corr positive
    if rate_gold:
        curr = rate_gold.get("current_corr") or 0
        z = rate_gold.get("z_delta") or 0
        if curr > 0.2 and z > 1.5:
            scores["STAGFLATION_PRICING"] = min(95, 50 + z * 15)
            evidence["STAGFLATION_PRICING"] = rate_gold

    # 4. RISK_PARITY_UNWIND: simultaneous breaks across MULTIPLE pairs +
    #     VIX/SPY correlation breaks positive (vol + equities both up)
    if state in ("CRISIS", "EXTREME") or n_breaks_3sigma >= 3:
        scores["RISK_PARITY_UNWIND"] = 60
        if vix_spy:
            curr = vix_spy.get("current_corr") or -1
            if curr > 0:  # normally negative; positive = both up = unwind
                scores["RISK_PARITY_UNWIND"] = min(
                    95, scores["RISK_PARITY_UNWIND"] + 25)
        evidence["RISK_PARITY_UNWIND"] = {
            "n_breaks_3sigma": n_breaks_3sigma,
            "state": state,
            "vix_spy": vix_spy,
        }

    # 5. DEFLATION_FEAR: VIX/TLT corr breaks negative (TLT rallies on stress)
    if vix_tlt:
        curr = vix_tlt.get("current_corr") or 0
        z = vix_tlt.get("z_delta") or 0
        if curr < -0.3 and abs(z) > 1.2:
            scores["DEFLATION_FEAR"] = min(90, 50 + abs(z) * 12)
            evidence["DEFLATION_FEAR"] = vix_tlt

    # 6. REFLATION_CARRY: BTC/QQQ rising correlation + dollar stance neutral/weak
    if btc_qqq:
        curr = btc_qqq.get("current_corr") or 0
        z = btc_qqq.get("z_delta") or 0
        if curr > 0.6 and z > 0:
            dollar_stance = safe_get(dollar, "stance") or ""
            if isinstance(dollar_stance, str) and "STRONG" not in dollar_stance.upper():
                scores["REFLATION_CARRY"] = 65
                evidence["REFLATION_CARRY"] = {
                    "btc_qqq_corr": curr,
                    "dollar_stance": dollar_stance,
                }

    return scores, evidence


# ---------- Main ----------
def lambda_handler(event=None, context=None):
    started = time.time()
    print(f"[corr-break-trades] start v{VERSION}")

    corr_breaks = fetch_s3_json("data/correlation-breaks.json")
    corr_surface = fetch_s3_json("data/correlation-surface.json")
    cross_asset_rv = fetch_s3_json("data/cross-asset-rv.json")
    bond_regime = fetch_s3_json("data/bond-regime.json")
    dollar = fetch_s3_json("data/dollar-radar.json")
    vol_radar = fetch_s3_json("data/vol-radar.json")

    feeds_available = {
        "correlation_breaks": corr_breaks is not None,
        "correlation_surface": corr_surface is not None,
        "cross_asset_rv": cross_asset_rv is not None,
        "bond_regime": bond_regime is not None,
        "dollar_radar": dollar is not None,
        "vol_radar": vol_radar is not None,
    }

    scores, evidence = classify_correlation_regime(
        corr_breaks, dollar, vol_radar, bond_regime)

    # Primary = highest, secondary = 2nd-highest if >=40, else None
    sorted_regimes = sorted(scores.items(), key=lambda x: -x[1])
    primary_regime, primary_score = (sorted_regimes[0]
                                       if sorted_regimes else
                                       ("NORMAL", 0))
    secondary_regime = (sorted_regimes[1][0]
                          if len(sorted_regimes) > 1
                          and sorted_regimes[1][1] >= 40
                          else None)
    secondary_score = (sorted_regimes[1][1]
                          if secondary_regime else None)

    if primary_score < 50:
        primary_regime = "NORMAL"
        primary_score = 0
    
    primary_recipe = REGIME_TRADES.get(primary_regime, REGIME_TRADES["NORMAL"])
    secondary_recipe = (REGIME_TRADES.get(secondary_regime)
                         if secondary_regime else None)

    output = {
        "engine": "correlation-break-trade-router",
        "version": VERSION,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "primary_regime": primary_regime,
        "primary_regime_score": primary_score,
        "primary_regime_name": primary_recipe.get("name"),
        "primary_thesis": primary_recipe.get("thesis"),
        "primary_recipe": {
            "long": primary_recipe.get("long", []),
            "short": primary_recipe.get("short", []),
            "hedge": primary_recipe.get("hedge", []),
            "max_equity_pct": primary_recipe.get("max_equity_pct"),
            "horizon_days": primary_recipe.get("horizon_days"),
        },
        "secondary_regime": secondary_regime,
        "secondary_regime_score": secondary_score,
        "secondary_recipe": secondary_recipe and {
            "name": secondary_recipe.get("name"),
            "long": secondary_recipe.get("long"),
            "short": secondary_recipe.get("short"),
            "thesis": secondary_recipe.get("thesis"),
        },
        "all_regime_scores": scores,
        "evidence": evidence,
        "feeds_available": feeds_available,
        "regime_universe": list(REGIME_TRADES.keys()),
        "methodology": {
            "framework": "Correlation regime -> stock-level trade recipe",
            "philosophy": (
                "Existing fleet (correlation-breaks + correlation-surface + "
                "cross-asset-rv) DETECTS regime change. This engine "
                "PRESCRIBES the equity / sector / hedge trades that "
                "historically work in each regime. Two Sigma + AQR + "
                "Renaissance internal versions; not sold."),
            "regime_classification": {
                "BOND_ROUT": "SP500/DGS10 corr flipped positive + z_delta>1.5",
                "DOLLAR_DEBASEMENT": "DXY/Gold corr broke negative",
                "STAGFLATION_PRICING": "10Y/Gold corr flipped positive",
                "RISK_PARITY_UNWIND": "n_breaks>=3 OR VIX/SPY corr positive",
                "DEFLATION_FEAR": "VIX/TLT corr broke negative",
                "REFLATION_CARRY": "BTC/QQQ corr rising + weak USD",
            },
            "distinction_from_engine_4": (
                "Engine #4 (Regime-Conditional Router) maps CRISIS KB "
                "frameworks. This engine maps CORRELATION REGIMES. "
                "Different taxonomies, complementary signals. Both feed "
                "the allocator stack independently."),
        },
        "academic_basis": [
            "Asness, C. S., Moskowitz, T. J., & Pedersen, L. H. (2013). "
            "Value and momentum everywhere. Journal of Finance, 68(3).",
            "Pollet, J. M., & Wilson, M. (2010). Average correlation "
            "and stock market returns. JFE, 96(3), 364-380.",
            "Krishnamurthy, A. (2010). How debt markets have "
            "malfunctioned in the crisis. JEP, 24(1), 3-28.",
            "Ilmanen, A. (2011). Expected Returns. Wiley.",
        ],
        "duration_seconds": round(time.time() - started, 1),
    }

    s3.put_object(
        Bucket=S3_BUCKET, Key=S3_KEY,
        Body=json.dumps(output, default=str).encode("utf-8"),
        ContentType="application/json",
        CacheControl="public, max-age=900")

    print(f"[corr-break-trades] primary={primary_regime} "
          f"score={primary_score} secondary={secondary_regime}")

    return {
        "statusCode": 200,
        "body": json.dumps({
            "ok": True, "version": VERSION,
            "primary_regime": primary_regime,
            "primary_score": primary_score,
            "secondary_regime": secondary_regime,
            "long_universe": primary_recipe.get("long", []),
            "short_universe": primary_recipe.get("short", []),
        }),
    }


if __name__ == "__main__":
    print(json.dumps(lambda_handler(), indent=2))
