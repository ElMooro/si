"""
justhodl-master-ranker — Today's Top Action Items (cross-system signal ranking).

WHY THIS EXISTS
───────────────
The platform has 138 Lambdas producing 12+ distinct signal feeds. Each feed
ranks its own results, but there's no SINGLE list answering:
  "Across EVERYTHING you compute, what should I act on TODAY?"

compound-aggregator already does this for 5 ticker-level systems
(nobrainers, insider-clusters, smart-money, deep-value, eps-revision-velocity).
This Lambda extends that concept by:

  1. Reading compound-signals.json as the per-ticker spine (49 names)
  2. Adding 7 MORE ticker-level systems:
     - sector-tilt        (regime-conditional sector OW/UW)
     - options-flow       (unusual options activity)
     - momentum-breakout  (technical breakouts)
     - volatility-squeeze (BB squeeze candidates)
     - PEAD signals       (earnings drift active windows)
     - theme-tiers        (tier-1/2 asymmetric setups)
     - asymmetric-scorer  (QARP top setups)
  3. Adding non-ticker MARKET signals (rendered as separate "macro" rank):
     - divergence-v2 EXTREME pairs
     - pairs-scanner EXTREME setups
     - sector-tilt MISALIGNED BUY OPPORTUNITY
     - macro-nowcast regime stance
     - cross-asset-regime regime
  4. Computes a Conviction Score per item using:
     - Signal strength (z-score / score / drift_pct)
     - Multi-system convergence (n_systems present)
     - Calibration weight (from /justhodl/calibration/weights SSM if available)
  5. Ranks across BOTH ticker and macro signals into ONE master list

OUTPUT
──────
  s3://justhodl-dashboard-live/data/master-ranker.json
  {
    "as_of": ISO-8601,
    "regime_context": {regime, composite_z, fwd_returns},
    "top_tickers":     [{ticker, score, n_systems, systems[], rationale, ...}, x25],
    "top_macro":       [{name, type, z, rationale, action_hint}, x10],
    "alerts":          {n_tier_3+: ..., n_extreme_macro: ..., regime_change: bool},
    "duration_s":      ...
  }

SCHEDULE
────────
  rate(1 hour) — same cadence as compound-aggregator. Runs 5min after the
  hourly compound-aggregator to ensure fresh data.

ZERO DETERIORATION
  * Pure consumer of existing S3 data — no other Lambda touched
  * compound-aggregator unchanged (provides primary spine via S3)
  * No Telegram alerts here — the existing alert systems (compound,
    divergence-interpreter, redflag-alerter) cover that
"""
import json
import math
import os
import time
from datetime import datetime, timezone

import boto3

REGION = "us-east-1"
BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
S3_KEY_OUT = os.environ.get("S3_KEY_OUT", "data/master-ranker.json")
CALIBRATION_SSM = "/justhodl/calibration/weights"

S3 = boto3.client("s3", region_name=REGION)
SSM = boto3.client("ssm", region_name=REGION)


# ─────────────────────────────────────────────────────────────────────────────
# DATA LOADING
# ─────────────────────────────────────────────────────────────────────────────
def fetch_json(key, default=None):
    try:
        obj = S3.get_object(Bucket=BUCKET, Key=key)
        return json.loads(obj["Body"].read())
    except Exception as e:
        print(f"[master-ranker] fetch_json({key}) failed: {e}")
        return default


def load_calibration_weights():
    """Read calibration weights from SSM. Returns dict {signal_name: weight 0-1}."""
    try:
        params = SSM.get_parameters_by_path(
            Path=CALIBRATION_SSM, Recursive=True, WithDecryption=False
        )
        weights = {}
        for p in params.get("Parameters", []):
            name = p["Name"].split("/")[-1]
            try:
                weights[name] = float(p["Value"])
            except (ValueError, TypeError):
                pass
        return weights
    except Exception as e:
        print(f"[master-ranker] calibration load failed: {e} — using flat weights")
        return {}


# ─────────────────────────────────────────────────────────────────────────────
# TICKER-LEVEL ENRICHMENT
# ─────────────────────────────────────────────────────────────────────────────
def build_ticker_index():
    """Build a master ticker → systems index by joining all per-ticker feeds."""
    feeds = {
        "compound":         fetch_json("data/compound-signals.json"),
        "asymmetric":       fetch_json("data/asymmetric-scorer.json"),
        "theme_tiers":      fetch_json("data/theme-tiers.json"),
        "eps_velocity":     fetch_json("data/eps-revision-velocity.json"),
        "insider":          fetch_json("data/insider-clusters.json"),
        "smart_money":      fetch_json("data/smart-money-clusters.json"),
        "deep_value":       fetch_json("data/deep-value.json"),
        "pead":             fetch_json("data/pead-signals.json"),
        "nobrainers":       fetch_json("data/nobrainers.json"),
        "options_flow":     fetch_json("data/options-flow.json"),
        "momentum_breakout": fetch_json("data/momentum-breakout.json"),
        "volatility_squeeze": fetch_json("data/volatility-squeeze.json"),
        "supply_inflection": fetch_json("data/supply-inflection.json"),
        "pre_pump":         fetch_json("data/pre-pump-signals.json"),
        "revenue_accel":    fetch_json("data/revenue-acceleration.json"),
    }

    # Index: ticker → {system_name: {score, details}}
    idx = {}

    # 1. compound — primary spine
    if feeds["compound"]:
        for c in (feeds["compound"].get("compound") or []):
            sym = c.get("symbol")
            if not sym:
                continue
            idx.setdefault(sym, {})["compound"] = {
                "score": c.get("compound_score"),
                "n_systems": c.get("n_systems"),
                "systems": c.get("systems", []),
                "details": c.get("details", {}),
            }

    # 2. theme tiers (tier 1/2 leaderboards)
    if feeds["theme_tiers"]:
        summary = feeds["theme_tiers"].get("summary", {})
        for k in ("top_asymmetric_leaderboard", "tier1_leaderboard", "tier2_leaderboard"):
            for r in (summary.get(k) or []):
                sym = r.get("ticker")
                if not sym:
                    continue
                if "theme_tiers" not in idx.setdefault(sym, {}):
                    idx[sym]["theme_tiers"] = {
                        "score": r.get("asymmetry_score"),
                        "tier": r.get("tier"),
                        "theme": r.get("theme_name"),
                        "phase": r.get("theme_phase"),
                    }

    # 3. asymmetric scorer top setups
    if feeds["asymmetric"]:
        for r in (feeds["asymmetric"].get("top_setups") or []):
            sym = r.get("symbol")
            if not sym:
                continue
            idx.setdefault(sym, {})["asymmetric"] = {
                "score": r.get("score") or r.get("asymmetry_score"),
                "sector": r.get("sector"),
                "marketCap": r.get("marketCap"),
                "pe": r.get("peRatio"),
            }

    # 4. EPS revision velocity
    if feeds["eps_velocity"]:
        for r in (feeds["eps_velocity"].get("all_qualifying") or []):
            sym = r.get("symbol")
            if not sym:
                continue
            idx.setdefault(sym, {})["eps_velocity"] = {
                "score": r.get("score"),
                "flag": r.get("flag"),
            }

    # 5. insider clusters
    if feeds["insider"]:
        for r in (feeds["insider"].get("clusters") or []):
            sym = r.get("ticker")
            if not sym:
                continue
            idx.setdefault(sym, {})["insider"] = {
                "n_insiders": r.get("n_insiders"),
                "total_value": r.get("total_value"),
                "pct_from_high": r.get("pct_from_52w_high"),
                "has_ceo": r.get("has_ceo"),
                "has_cfo": r.get("has_cfo"),
            }

    # 6. smart money 13F
    if feeds["smart_money"]:
        for r in (feeds["smart_money"].get("clusters") or []):
            sym = r.get("ticker")
            if not sym:
                continue
            idx.setdefault(sym, {})["smart_money"] = {
                "score": r.get("score"),
                "flag": r.get("flag"),
                "n_funds": r.get("n_funds_holding"),
                "legend_buyers": r.get("legend_buyers"),
            }

    # 7. deep value
    if feeds["deep_value"]:
        for r in (feeds["deep_value"].get("all_qualifying") or []):
            sym = r.get("symbol")
            if not sym:
                continue
            idx.setdefault(sym, {})["deep_value"] = {
                "score": r.get("score"),
                "flag": r.get("flag"),
            }

    # 8. PEAD
    if feeds["pead"]:
        for r in (feeds["pead"].get("summary", {}).get("top_30_overall") or []):
            sym = r.get("symbol")
            if not sym:
                continue
            idx.setdefault(sym, {})["pead"] = {
                "score": r.get("score"),
                "tier": r.get("tier"),
                "drift_pct": r.get("drift_pct"),
                "days_to_next": r.get("days_to_next"),
            }

    # 9. nobrainers
    if feeds["nobrainers"]:
        for r in (feeds["nobrainers"].get("top_setups") or feeds["nobrainers"].get("setups") or []):
            sym = r.get("ticker") or r.get("symbol")
            if not sym:
                continue
            idx.setdefault(sym, {})["nobrainers"] = {
                "score": r.get("score") or r.get("asymmetry_score"),
                "tier": r.get("tier"),
                "flag": r.get("flag"),
                "theme": r.get("theme"),
            }

    # 10. options flow
    if feeds["options_flow"]:
        for r in (feeds["options_flow"].get("unusual") or feeds["options_flow"].get("top_flow") or []):
            sym = r.get("ticker") or r.get("symbol")
            if not sym:
                continue
            idx.setdefault(sym, {})["options_flow"] = {
                "score": r.get("score") or r.get("unusual_score"),
                "flag": r.get("flag") or r.get("classification"),
                "premium": r.get("premium"),
            }

    # 11. momentum breakout
    if feeds["momentum_breakout"]:
        for r in (feeds["momentum_breakout"].get("breakouts") or feeds["momentum_breakout"].get("top") or []):
            sym = r.get("ticker") or r.get("symbol")
            if not sym:
                continue
            idx.setdefault(sym, {})["momentum"] = {
                "score": r.get("score"),
                "ret_20d": r.get("ret_20d"),
            }

    # 12. volatility squeeze
    if feeds["volatility_squeeze"]:
        for r in (feeds["volatility_squeeze"].get("squeezes") or feeds["volatility_squeeze"].get("top") or []):
            sym = r.get("ticker") or r.get("symbol")
            if not sym:
                continue
            idx.setdefault(sym, {})["vol_squeeze"] = {
                "score": r.get("score"),
                "compression": r.get("compression"),
            }

    # 13. pre-pump (technical breakout candidates)
    if feeds["pre_pump"]:
        for r in (feeds["pre_pump"].get("setups") or feeds["pre_pump"].get("top") or []):
            sym = r.get("ticker") or r.get("symbol")
            if not sym:
                continue
            idx.setdefault(sym, {})["pre_pump"] = {
                "score": r.get("score"),
                "obv_z": r.get("obv_z"),
            }

    # 14. revenue acceleration (fundamental coiled spring)
    if feeds["revenue_accel"]:
        for r in (feeds["revenue_accel"].get("all_qualifying") or feeds["revenue_accel"].get("top") or []):
            sym = r.get("symbol") or r.get("ticker")
            if not sym:
                continue
            idx.setdefault(sym, {})["revenue_accel"] = {
                "score": r.get("score"),
                "tier": r.get("tier"),
            }

    return idx, feeds


# ─────────────────────────────────────────────────────────────────────────────
# SCORING
# ─────────────────────────────────────────────────────────────────────────────
def normalize_signal_score(system_name, system_data):
    """Map system-specific score to a 0-100 normalized scale."""
    if not system_data or not isinstance(system_data, dict):
        return 0
    raw = system_data.get("score") or 0
    try:
        raw = float(raw)
    except (ValueError, TypeError):
        raw = 0
    # Most of our systems already use 0-100 conventions
    if raw >= 100:
        return 100
    if raw < 0:
        return 0
    # eps_velocity, smart_money, deep_value, asymmetric are 0-100
    # compound_score can go to 1000+, normalize differently
    if system_name == "compound":
        n_sys = system_data.get("n_systems", 1)
        # log-scale compound, multiplicative bonus for systems
        return min(100, math.log10(max(raw, 1)) * 25 + n_sys * 10)
    return raw


def compute_conviction(systems_dict, calibration_weights):
    """Master conviction score combining all systems for a single ticker."""
    if not systems_dict:
        return 0, []
    contributions = []
    total = 0
    for sys_name, data in systems_dict.items():
        normalized = normalize_signal_score(sys_name, data)
        weight = calibration_weights.get(sys_name, 1.0)  # default 1.0 if uncalibrated
        contribution = normalized * weight
        total += contribution
        if normalized > 0:
            contributions.append({
                "system": sys_name,
                "raw_score": data.get("score"),
                "normalized": round(normalized, 1),
                "weight": round(weight, 2),
                "contribution": round(contribution, 1),
            })
    # Convergence multiplier: log-linear bonus for multi-system agreement
    n_active = len(contributions)
    convergence_mult = 1.0 + 0.4 * math.log(max(n_active, 1))  # 1 sys=1.0, 5 sys=1.64, 10 sys=1.92
    final = total * convergence_mult / max(n_active, 1)  # weighted average × convergence
    return round(final, 1), contributions


def synthesize_rationale(ticker, systems_dict, score):
    """One-sentence rationale string for the master rank list."""
    n = len(systems_dict)
    sys_names = sorted(systems_dict.keys())
    sys_str = ", ".join(sys_names[:5]) + ("…" if len(sys_names) > 5 else "")

    # Highlight notable signals
    highlights = []
    if "insider" in systems_dict and systems_dict["insider"].get("has_ceo"):
        highlights.append("CEO buying")
    if "insider" in systems_dict and (systems_dict["insider"].get("pct_from_high") or 0) <= -20:
        highlights.append(f"-{abs(systems_dict['insider']['pct_from_high']):.0f}% drawdown")
    if "smart_money" in systems_dict and systems_dict["smart_money"].get("legend_buyers"):
        legends = systems_dict["smart_money"]["legend_buyers"]
        if legends:
            highlights.append(f"legends: {', '.join(legends[:2])}")
    if "pead" in systems_dict:
        drift = systems_dict["pead"].get("drift_pct")
        if drift and drift > 20:
            highlights.append(f"PEAD drift +{drift:.0f}%")
    if "theme_tiers" in systems_dict:
        tier = systems_dict["theme_tiers"].get("tier")
        theme = systems_dict["theme_tiers"].get("theme")
        if tier in (1, 2):
            highlights.append(f"theme T-{tier} {theme}")

    parts = [f"{n} systems agree (compound={score})"]
    if sys_str:
        parts.append(f"[{sys_str}]")
    if highlights:
        parts.append("· " + " · ".join(highlights))
    return " ".join(parts)


# ─────────────────────────────────────────────────────────────────────────────
# MACRO SIGNALS (non-ticker)
# ─────────────────────────────────────────────────────────────────────────────
def collect_macro_signals():
    """Collect non-ticker macro signals into a unified list."""
    out = []

    # Divergence v2 EXTREME pairs
    div = fetch_json("data/divergence-v2.json")
    if div:
        all_rels = div.get("all_relationships", [])
        extremes = [r for r in all_rels if r.get("status") == "extreme"]
        extremes.sort(key=lambda r: abs(r.get("divergence_z") or 0), reverse=True)
        for r in extremes[:5]:
            out.append({
                "type": "divergence",
                "name": r.get("name"),
                "category": r.get("category"),
                "z": r.get("divergence_z"),
                "abs_z": abs(r.get("divergence_z") or 0),
                "rationale": r.get("description", "")[:120],
                "action_hint": "Cross-asset divergence flagging — read divergence-v2.html",
                "score": min(100, abs(r.get("divergence_z") or 0) * 20),
            })

    # Pairs scanner EXTREME
    pairs = fetch_json("data/pairs-scanner.json")
    if pairs:
        for r in (pairs.get("pairs") or []):
            if r.get("state") in ("EXTREME", "EXTENDED"):
                out.append({
                    "type": "pair",
                    "name": r.get("name"),
                    "trade": r.get("trade"),
                    "z": r.get("spread_z"),
                    "abs_z": abs(r.get("spread_z") or 0),
                    "rr": r.get("rr_estimate"),
                    "half_life_days": r.get("half_life_days"),
                    "rationale": (r.get("rationale") or "")[:140],
                    "action_hint": r.get("trade") or "",
                    "score": min(100, abs(r.get("spread_z") or 0) * 25),
                })

    # Sector-tilt MISALIGNED BUY
    tilt = fetch_json("data/sector-tilt.json")
    if tilt:
        misaligned = [t for t in (tilt.get("tilts") or [])
                       if t.get("alignment") == "MISALIGNED"
                       and t.get("implication") == "BUY_OPPORTUNITY"
                       and t.get("urgency") == "HIGH"]
        for t in misaligned:
            out.append({
                "type": "sector_tilt",
                "name": f"{t.get('ticker','?')} {t.get('name','?')}",
                "ticker": t.get("ticker"),
                "regime": t.get("regime"),
                "rs_20d": t.get("rs_20d"),
                "rationale": (t.get("rationale") or "")[:160],
                "action_hint": f"BUY {t.get('ticker','')} — regime {t.get('regime','?')} calls for OW",
                "score": min(100, abs(t.get("regime_tilt_score") or 0) * 30 + abs(t.get("rs_20d") or 0)),
            })

    # Cross-asset regime
    car = fetch_json("data/cross-asset-regime.json")
    if car:
        regime = car.get("regime") or car.get("dominant_regime")
        if regime:
            out.append({
                "type": "cross_asset_regime",
                "name": f"Cross-Asset Regime: {regime}",
                "regime": regime,
                "rationale": car.get("rationale") or car.get("interpretation") or "",
                "action_hint": f"Position for {regime} regime",
                "score": 70,  # always show in top 10
            })

    # Regime anomaly
    ra = fetch_json("data/regime-anomaly.json")
    if ra:
        anom = ra.get("anomaly", {})
        comp = anom.get("composite_anomaly_score") or 0
        if comp >= 50:
            out.append({
                "type": "regime_anomaly",
                "name": "HMM regime anomaly detected",
                "score": comp,
                "rationale": f"composite anomaly {comp}/100, HMM state={ra.get('hmm', {}).get('state_label', '?')}",
                "action_hint": "Reduce risk — regime appears unstable",
            })

    # Sort by score descending
    out.sort(key=lambda x: x.get("score", 0), reverse=True)
    return out[:10]


# ─────────────────────────────────────────────────────────────────────────────
# REGIME CONTEXT
# ─────────────────────────────────────────────────────────────────────────────
REGIME_FORWARDS = {
    "STRONG EXPANSION": {"6m": "+10.9%", "12m": "+12.7%"},
    "EXPANSION":        {"6m": "-2.7%",  "12m": "-5.2%"},
    "MUDDLE":           {"6m": "+5.3%",  "12m": "+12.7%"},
    "SLOWING":          {"6m": "+8.8%",  "12m": "+17.5%"},
    "CONTRACTION":      {"6m": "+18.5%", "12m": "+37.6%"},
}


def get_regime_context():
    nowcast = fetch_json("data/macro-nowcast.json") or {}
    regime = nowcast.get("regime") or "UNKNOWN"
    fwds = REGIME_FORWARDS.get(regime, {})
    return {
        "regime": regime,
        "regime_color": nowcast.get("regime_color"),
        "composite_z": nowcast.get("composite_z") or nowcast.get("normalized_score"),
        "coverage_pct": nowcast.get("coverage_pct"),
        "spy_fwd_6m": fwds.get("6m"),
        "spy_fwd_12m": fwds.get("12m"),
    }


# ─────────────────────────────────────────────────────────────────────────────
# MAIN HANDLER
# ─────────────────────────────────────────────────────────────────────────────
def lambda_handler(event, context):
    started = time.time()

    print("[master-ranker] Loading regime context…")
    regime_ctx = get_regime_context()

    print("[master-ranker] Loading calibration weights…")
    calibration = load_calibration_weights()

    print("[master-ranker] Building ticker index across 14 systems…")
    ticker_idx, feeds = build_ticker_index()
    print(f"[master-ranker] {len(ticker_idx)} unique tickers indexed")

    # Compute conviction for every ticker
    ticker_ranks = []
    for ticker, systems_dict in ticker_idx.items():
        score, contributions = compute_conviction(systems_dict, calibration)
        n_sys = len(systems_dict)
        ticker_ranks.append({
            "ticker": ticker,
            "score": score,
            "n_systems": n_sys,
            "systems": sorted(systems_dict.keys()),
            "contributions": contributions,
            "rationale": synthesize_rationale(ticker, systems_dict, score),
            "details": systems_dict,
        })
    ticker_ranks.sort(key=lambda r: r["score"], reverse=True)
    top_tickers = ticker_ranks[:25]

    print("[master-ranker] Collecting macro signals…")
    macro_signals = collect_macro_signals()

    # Aggregate stats / alerts
    n_tier_3_plus = sum(1 for t in ticker_ranks if t["n_systems"] >= 3)
    n_tier_5_plus = sum(1 for t in ticker_ranks if t["n_systems"] >= 5)
    n_extreme_macro = sum(1 for m in macro_signals
                           if m.get("type") in ("divergence", "pair")
                           and m.get("abs_z", 0) >= 3)

    payload = {
        "schema_version": "1.0",
        "method": "master_signal_ranker_v1",
        "as_of": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "regime_context": regime_ctx,
        "n_tickers_total": len(ticker_idx),
        "n_macro_signals": len(macro_signals),
        "alerts": {
            "n_tier_3_plus_systems": n_tier_3_plus,
            "n_tier_5_plus_systems": n_tier_5_plus,
            "n_extreme_macro": n_extreme_macro,
            "calibration_active": bool(calibration),
        },
        "top_tickers": top_tickers,
        "top_macro": macro_signals,
        "feed_health": {
            name: {
                "loaded": data is not None,
                "size_hint": (len(data) if isinstance(data, dict) else
                              len(data) if isinstance(data, list) else 0),
            }
            for name, data in feeds.items()
        },
        "calibration_weights": calibration,
        "duration_s": round(time.time() - started, 2),
    }

    body_bytes = json.dumps(payload, indent=2, default=str).encode("utf-8")
    S3.put_object(
        Bucket=BUCKET, Key=S3_KEY_OUT, Body=body_bytes,
        ContentType="application/json", CacheControl="max-age=300",
    )

    print(f"[master-ranker] DONE in {payload['duration_s']}s · "
          f"{len(top_tickers)} tickers · {len(macro_signals)} macro · "
          f"{n_tier_3_plus} tier-3+, {n_tier_5_plus} tier-5+")

    return {
        "statusCode": 200,
        "body": json.dumps({
            "ok": True,
            "n_tickers": len(top_tickers),
            "n_macro": len(macro_signals),
            "n_tier_3_plus": n_tier_3_plus,
            "n_tier_5_plus": n_tier_5_plus,
            "regime": regime_ctx.get("regime"),
            "duration_s": payload["duration_s"],
        }),
    }
