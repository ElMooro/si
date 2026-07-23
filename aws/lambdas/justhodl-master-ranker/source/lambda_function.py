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

try:
    import engine_trust
except Exception:
    engine_trust = None

REGION = "us-east-1"
BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")


# ── CENSUS OVERLAY (ops 3550 · MR-CENSUS): conviction / combo / risk / patterns /
# whale-flow per ticker from the Fundamental Census matrix ──
_CENSUS_CACHE = None


def census_idx(s3_client, bucket):
    global _CENSUS_CACHE
    if _CENSUS_CACHE is not None:
        return _CENSUS_CACHE
    import json as _cj
    out = {}
    try:
        mx = _cj.loads(s3_client.get_object(
            Bucket=bucket,
            Key="data/fundamental-census-matrix.json")["Body"].read())
        C = mx.get("cols") or {}
        rk = C.get("risk_score") or []
        xs = sorted(v for v in rk if isinstance(v, (int, float)))
        lo = xs[len(xs)//3] if len(xs) >= 3 else None
        hi = xs[2*len(xs)//3] if len(xs) >= 3 else None
        col = lambda k: C.get(k) or [None] * len(mx.get("tickers") or [])
        for i, t in enumerate(mx.get("tickers") or []):
            pats = [lbl for lbl, k in
                    (("double_bottom", "double_bottom"),
                     ("double_top", "double_top"),
                     ("golden_cross", "golden_cross_10_40w"),
                     ("breakout_20w", "breakout_20w"))
                    if col(k)[i] == 1]
            rv = col("risk_score")[i]
            tier = (None if not isinstance(rv, (int, float)) or lo is None
                    else "LOW" if rv <= lo else "HIGH" if rv >= hi
                    else "MED")
            out[t] = {"conviction": col("conviction_score")[i],
                      "combo": col("combo_score")[i],
                      "risk": rv, "risk_tier": tier,
                      "turn": (mx.get("turn") or [None]*(i+1))[i]
                      if i < len(mx.get("turn") or []) else None,
                      "patterns": pats,
                      "whale_usd_m": col("whale_net_usd_m")[i]}
    except Exception as _e:  # noqa: BLE001
        print("[census-overlay]", str(_e)[:80])
    _CENSUS_CACHE = out
    return out

S3_KEY_OUT = os.environ.get("S3_KEY_OUT", "data/master-ranker.json")
CALIBRATION_SSM = "/justhodl/calibration/weights"

S3 = boto3.client("s3", region_name=REGION)
SSM = boto3.client("ssm", region_name=REGION)


# ─────────────────────────────────────────────────────────────────────────────
# DATA LOADING
# ─────────────────────────────────────────────────────────────────────────────
_FEED_HEALTH = []   # [{key, age_h, stale, used}] — transparency for every load

def _doc_age_hours(doc):
    """Hours since a feed was generated, read from its timestamp; None if absent."""
    if not isinstance(doc, dict):
        return None
    ts = (doc.get("generated_at") or doc.get("updated_at") or doc.get("as_of")
          or doc.get("generated") or doc.get("timestamp") or doc.get("last_updated"))
    if not ts:
        return None
    try:
        t = datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
        if t.tzinfo is None:
            t = t.replace(tzinfo=timezone.utc)
        return (datetime.now(timezone.utc) - t).total_seconds() / 3600.0
    except Exception:
        return None


def fetch_json(key, default=None, max_age_h=None):
    """Load a feed. If max_age_h is set and the feed is older, treat it as ABSENT
    (return default) so stale data never silently contaminates a decision. Every
    load is recorded in _FEED_HEALTH for transparency. Feeds with no max_age_h are
    age-tracked but never auto-excluded (cadence may legitimately be slow)."""
    try:
        obj = S3.get_object(Bucket=BUCKET, Key=key)
        doc = json.loads(obj["Body"].read())
    except Exception as e:
        print(f"[master-ranker] fetch_json({key}) failed: {e}")
        _FEED_HEALTH.append({"key": key, "age_h": None, "stale": None, "used": False, "missing": True})
        return default
    age = _doc_age_hours(doc)
    stale = (max_age_h is not None and age is not None and age > max_age_h)
    _FEED_HEALTH.append({"key": key, "age_h": round(age, 1) if age is not None else None,
                         "stale": bool(stale), "used": not stale})
    if stale:
        print(f"[master-ranker] STALE {key}: {age:.1f}h > {max_age_h}h -> excluded from decision")
        return default
    return doc


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
        "options_flow":     fetch_json("data/options-flow.json", max_age_h=72),
        "momentum_breakout": fetch_json("data/momentum-breakout.json"),
        "volatility_squeeze": fetch_json("data/volatility-squeeze.json"),
        # Forward-looking (added 2026-05-31)
        "future_intel":     fetch_json("data/future-intelligence.json"),
        "supply_inflection": fetch_json("data/supply-inflection.json"),
        "pre_pump":         fetch_json("data/pre-pump-signals.json"),
        "revenue_accel":    fetch_json("data/revenue-acceleration.json"),
        "massive":          fetch_json("data/massive-signals.json", max_age_h=72),
        "capital_flow":     fetch_json("data/capital-flow-radar.json", max_age_h=60),
        "risk_regime":      fetch_json("data/risk-regime.json", max_age_h=48),
        "finviz_tech":      fetch_json("data/finviz-signals.json", max_age_h=30),
        # fused confluence synthesizers (added — rank should weight multi-engine-confirmed names)
        "options_confluence":  fetch_json("data/options-confluence.json", max_age_h=72),
        "flow_confluence":     fetch_json("data/flow-confluence.json", max_age_h=72),
        "equity_confluence":   fetch_json("data/equity-confluence.json", max_age_h=72),
        "earnings_confluence": fetch_json("data/earnings-confluence.json", max_age_h=120),
        "scarcity_radar":      fetch_json("data/scarcity-radar.json", max_age_h=72),
        # corporate buybacks — net-of-dilution conviction (added 2026-06-30)
        "buyback":             fetch_json("data/buyback-engine.json", max_age_h=48),
        # institutional + fundamental-momentum layer (added 2026-07-01) — none of these
        # were previously read despite being fresh, live, and directly ticker-relevant
        "institutional_13f":   fetch_json("data/13f-positions.json", max_age_h=48),
        "estimate_revisions":  fetch_json("data/estimate-revisions.json", max_age_h=48),
        "forward_orders":      fetch_json("data/forward-orders.json", max_age_h=96),
        "squeeze_setup":       fetch_json("data/finra-short.json", max_age_h=48),
        "earnings_quality_hi": fetch_json("data/earnings-quality.json", max_age_h=200),
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

    # 6a. corporate buybacks — genuine net-of-dilution accumulation (justhodl-buyback-engine).
    #     Only the real classes contribute (DILUTION_OFFSET / net issuers are excluded), so
    #     the rank rewards companies actually shrinking their share count, and the convergence
    #     multiplier lifts names where a buyback agrees with insider / smart-money / value signals.
    if feeds["buyback"]:
        _BB_GOOD = {"🚀 FRESH_LARGE_AUTH", "💪 NET_SHRINKER", "🎯 CHEAP_REPURCHASER", "💰 HIGH_SHAREHOLDER_YIELD"}
        for sym, v in (feeds["buyback"].get("tickers") or {}).items():
            if not isinstance(v, dict):
                continue
            klass = v.get("class") or ""
            if not (v.get("high_conviction_pump") or klass in _BB_GOOD
                    or (float(v.get("buyback_score") or 0) >= 50 and not v.get("net_issuer"))):
                continue
            idx.setdefault(sym, {})["buyback"] = {
                "score": v.get("buyback_score"),
                "class": klass,
                "net_yield": v.get("net_buyback_yield"),
                "share_reduction": v.get("share_count_reduction_yoy"),
                "auth_pct": v.get("auth_pct_mcap"),
                "pump": v.get("high_conviction_pump"),
            }

    # 6b. fused confluence synthesizers — a name confirmed by a synthesizer (several
    #     independent engines stacked) is higher-quality than one raw-engine flag.
    for _key in ("options_confluence", "flow_confluence"):
        if feeds.get(_key):
            for r in (feeds[_key].get("multi_engine_confluence") or []):
                sym = r.get("ticker")
                if not sym:
                    continue
                idx.setdefault(sym, {})[_key] = {
                    "score": r.get("score"),
                    "n_engines": r.get("n_engines"),
                    "posture": r.get("posture"),
                }
    for _key in ("equity_confluence", "earnings_confluence"):
        if feeds.get(_key):
            for r in (feeds[_key].get("confluence_book") or []):
                sym = r.get("ticker")
                if not sym:
                    continue
                idx.setdefault(sym, {})[_key] = {
                    "score": r.get("composite"),
                    "n_families": r.get("n_super_families") or r.get("n_dimensions"),
                }

    # 6c. scarcity-radar — the next-shortage synthesizer. A PRIME/CANDIDATE name is a
    #     real supply tightening + capture + pricing-power + still-cheap setup; surface it
    #     in the master rank so the next shortage trade appears here, not only on its page.
    if feeds.get("scarcity_radar"):
        for r in (feeds["scarcity_radar"].get("stealth_shortage_board") or []):
            if r.get("tier") not in ("PRIME", "CANDIDATE"):
                continue
            sym = r.get("ticker")
            if not sym:
                continue
            idx.setdefault(sym, {})["scarcity_radar"] = {
                "score": r.get("composite"),
                "tier": r.get("tier"),
                "scarcity": r.get("scarcity"),
                "stealth": r.get("stealth"),
                "vertical": r.get("vertical"),
            }

    # 6d. institutional 13F accumulation — funds actively ADDING or opening NEW positions
    #     this quarter (not just "held by a fund"). Complements smart_money (which reads a
    #     different, pre-clustered feed) with the raw fund-flow signal, named buyers included.
    if feeds.get("institutional_13f"):
        for r in (feeds["institutional_13f"].get("most_bought") or []):
            sym = r.get("ticker")
            if not sym:
                continue
            adding = r.get("n_funds_adding") or 0
            new_pos = r.get("n_funds_new_position") or 0
            score = min(100, adding * 8 + new_pos * 12)
            if score < 24:  # require real conviction, not one fund nibbling
                continue
            fund_names = [fa.get("fund") for fa in (r.get("fund_actions") or [])[:3] if fa.get("fund")]
            idx.setdefault(sym, {})["institutional_13f"] = {
                "score": score, "n_funds_adding": adding, "n_funds_new": new_pos,
                "n_funds_holding": r.get("n_funds_holding"), "buyers": fund_names,
            }

    # 6e. estimate revisions — analyst EPS estimates moving before the print. A fundamental
    #     "smart money is repricing this" signal distinct from price-based momentum.
    if feeds.get("estimate_revisions"):
        for r in (feeds["estimate_revisions"].get("estimate_strength_leaders") or []):
            sym = r.get("ticker")
            if not sym:
                continue
            rev = r.get("eps_rev_pct")
            if rev is None:
                continue
            score = min(100, max(0, rev * 3))
            idx.setdefault(sym, {})["estimate_revisions"] = {
                "score": round(score, 1), "eps_rev_pct": rev,
                "days_to_earnings": r.get("days_to_earnings"), "fiscal_period": r.get("fiscal_period"),
            }

    # 6f. forward orders / RPO composite — remaining-performance-obligation yield, growth and
    #     acceleration. A genuine forward-fundamental signal (contracted future revenue), not
    #     backward-looking like most price/earnings-based systems here.
    if feeds.get("forward_orders"):
        for r in (feeds["forward_orders"].get("top_25_by_score") or []):
            sym = r.get("ticker")
            if not sym:
                continue
            idx.setdefault(sym, {})["forward_orders"] = {
                "score": r.get("composite"), "subscores": r.get("subscores"),
                "sector": r.get("sector"),
            }

    # 6g. squeeze setups (FINRA short-volume ratio + z-score + days-to-cover composite) — a
    #     distinct mechanical-pressure signal, complementary to price/fundamental conviction.
    if feeds.get("squeeze_setup"):
        for r in (feeds["squeeze_setup"].get("squeeze_candidates") or []):
            sym = r.get("symbol")  # this feed keys on "symbol", not "ticker"
            if not sym:
                continue
            idx.setdefault(sym, {})["squeeze_setup"] = {
                "score": r.get("squeeze_score"), "days_to_cover": r.get("days_to_cover"),
                "z_score": r.get("z_score"), "flags": r.get("squeeze_flags"),
            }

    # 6h. earnings quality — the POSITIVE side (high cash-conversion, low accrual manipulation
    #     risk). The negative side already demotes via the red-flag gate below; a name that's
    #     independently confirmed HIGH quality deserves a small first-class conviction credit,
    #     not just avoidance of a penalty.
    if feeds.get("earnings_quality_hi"):
        for i, r in enumerate(feeds["earnings_quality_hi"].get("top_20_high_quality") or []):
            sym = r.get("ticker")
            if not sym:
                continue
            idx.setdefault(sym, {})["earnings_quality_hi"] = {
                "score": max(55, 95 - i * 2), "rank": i + 1,
                "cash_conversion": r.get("cash_conversion"),
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

    # FinViz technical events (ops 2695): whole-market MA crosses / ATH breakouts /
    # base breaks from justhodl-finviz-signals v2 confluence. Registers a
    # "finviz_tech" system ({"score": 0-100, "tags": [...]}) so compute_conviction,
    # n_systems and the calibration loop all see the technical tape.
    if feeds.get("finviz_tech"):
        _fvc = feeds["finviz_tech"].get("confluence") or {}
        for _key, _sc, _tag in (("ath_momentum", 88, "ATH_BREAKOUT"),
                                ("base_breakout", 84, "BASE_BREAKOUT"),
                                ("bottom_squeeze_insider", 90, "BOTTOM_SQUEEZE_INSIDER"),
                                ("ma200_reclaim_vol", 78, "MA200_RECLAIM"),
                                ("trend_flip_up", 72, "TREND_FLIP_UP"),
                                ("breakout_52w_vol", 70, "52W_BREAKOUT_VOL")):
            for _sym in (_fvc.get(_key) or [])[:60]:
                _cur = idx.setdefault(_sym, {}).get("finviz_tech") or {}
                _tags = _cur.get("tags") or []
                if _tag not in _tags:
                    _tags.append(_tag)
                idx[_sym]["finviz_tech"] = {"score": max(_cur.get("score") or 0, _sc), "tags": _tags}

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

    # 15. future intelligence — composite of forward-orders, rotation-chain, buzz
    # This is the institutional alpha layer: what's about to happen, not what
    # has happened. RPO + value-chain rotation + buzz velocity composite.
    if feeds["future_intel"]:
        for r in (feeds["future_intel"].get("all_results") or []):
            sym = r.get("ticker")
            if not sym:
                continue
            idx.setdefault(sym, {})["future_intel"] = {
                "score":              r.get("future_intel_score"),
                "n_signals":          r.get("n_independent_signals"),
                "rpo_yield_pct":      (r.get("forward_orders") or {}).get("rpo_yield_pct"),
                "rpo_growth_pct":     (r.get("forward_orders") or {}).get("rpo_growth_pct"),
                "rotation_role":      (r.get("rotation_chain") or {}).get("role"),
                "rotation_chain":     (r.get("rotation_chain") or {}).get("chain"),
                "rotation_lag_pct":   (r.get("rotation_chain") or {}).get("lag_pct"),
                "buzz_velocity":      (r.get("buzz_velocity") or {}).get("composite_velocity"),
                "buzz_stealth":       (r.get("buzz_velocity") or {}).get("stealth"),
                "thesis":             r.get("thesis"),
            }

    # 16. massive-signals — unified Massive data (gamma squeeze + unusual options flow)
    if feeds["massive"]:
        for _sym, _t in (feeds["massive"].get("tickers") or {}).items():
            if not _sym or not _t.get("prepump_score"):
                continue
            idx.setdefault(_sym, {})["massive"] = {
                "score": _t.get("prepump_score"),
                "gamma_squeeze": _t.get("gamma_squeeze_score"),
                "bullish_flow": _t.get("bullish_flow"),
                "otm_sweep": _t.get("otm_call_sweep"),
                "why": _t.get("massive_why"),
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
        cal_weight = calibration_weights.get(sys_name, 1.0)  # default 1.0 if uncalibrated
        # EDGE-ACCURACY trust: down-weight signal families the fleet's own truth layer has
        # shown to be below their null benchmark (net-negative after cost), up-weight the
        # handful that are alpha-proven. Regime-conditioned, defaults to 1.0 (no-op) until
        # a family's ledger has matured enough to grade — see aws/shared/engine_trust.py.
        trust_mult = engine_trust.trust(sys_name, default=1.0) if engine_trust else 1.0
        weight = cal_weight * trust_mult
        contribution = normalized * weight
        total += contribution
        if normalized > 0:
            contributions.append({
                "system": sys_name,
                "raw_score": data.get("score"),
                "normalized": round(normalized, 1),
                "weight": round(weight, 2),
                "trust_mult": round(trust_mult, 2),
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
    if "buyback" in systems_dict:
        _bb = systems_dict["buyback"]
        if _bb.get("pump"):
            highlights.append(f"buyback pump (auth {_bb.get('auth_pct')}% mcap)")
        elif (_bb.get("share_reduction") or 0) >= 2:
            highlights.append(f"net shrinker (shares -{_bb.get('share_reduction')}% YoY)")
        elif (_bb.get("net_yield") or 0) > 0:
            highlights.append(f"buyback {_bb.get('net_yield')}% net")
    if "institutional_13f" in systems_dict:
        _f = systems_dict["institutional_13f"]
        buyers = _f.get("buyers") or []
        if buyers:
            highlights.append(f"13F buying: {', '.join(buyers[:2])}")
        elif (_f.get("n_funds_new") or 0) >= 2:
            highlights.append(f"{_f['n_funds_new']} funds opened new positions")
    if "estimate_revisions" in systems_dict:
        rev = systems_dict["estimate_revisions"].get("eps_rev_pct")
        if rev and rev >= 8:
            highlights.append(f"EPS estimates +{rev:.0f}% into print")
    if "forward_orders" in systems_dict:
        sub = systems_dict["forward_orders"].get("subscores") or {}
        if (sub.get("rpo_acceleration") or 0) >= 70:
            highlights.append("RPO/backlog accelerating")
    if "squeeze_setup" in systems_dict:
        _sq = systems_dict["squeeze_setup"]
        if (_sq.get("score") or 0) >= 75:
            highlights.append(f"squeeze setup (dtc {_sq.get('days_to_cover')}d)")
    if "earnings_quality_hi" in systems_dict:
        highlights.append("high earnings quality")
    if "pead" in systems_dict:
        drift = systems_dict["pead"].get("drift_pct")
        if drift and drift > 20:
            highlights.append(f"PEAD drift +{drift:.0f}%")
    if "theme_tiers" in systems_dict:
        tier = systems_dict["theme_tiers"].get("tier")
        theme = systems_dict["theme_tiers"].get("theme")
        if tier in (1, 2):
            highlights.append(f"theme T-{tier} {theme}")
    if "massive" in systems_dict and systems_dict["massive"].get("why"):
        highlights.append("options: " + systems_dict["massive"]["why"])
    if "scarcity_radar" in systems_dict:
        sr = systems_dict["scarcity_radar"]
        vert = sr.get("vertical") or "supply"
        highlights.append(f"shortage {sr.get('tier')}: {vert} (scarcity {sr.get('scarcity')}×stealth {sr.get('stealth')})")

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
    car = fetch_json("data/cross-asset-regime.json", max_age_h=72)
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

    # Liquidity inflection — composite 2nd-derivative regime (macro tide)
    liq = fetch_json("data/liquidity-inflection.json", max_age_h=48)
    lcomp = (liq or {}).get("composite") or {}
    if isinstance(lcomp.get("liquidity_score"), (int, float)):
        _lreg = lcomp.get("regime") or "NEUTRAL"
        _ltraj = ((liq or {}).get("trajectory") or {}).get("heading")
        _lds = ((liq or {}).get("dollar_shortage") or {}).get("status")
        _name = f"Liquidity Regime: {_lreg} ({lcomp.get('liquidity_score')}/100)"
        if _ltraj and _ltraj != "STABLE / MIXED":
            _name += f" · {_ltraj}"
        _rat = (lcomp.get("read") or "")[:200]
        if _ltraj:
            _rat += f" Trajectory: {_ltraj}."
        if _lds and _lds != "CALM":
            _rat += f" Dollar funding: {_lds}."
        _act = ("Liquidity tailwind — lean into beta/cyclicals" if _lreg == "EXPANDING"
                else "Liquidity headwind — favor quality, raise hedges" if _lreg == "CONTRACTING"
                else "Liquidity neutral now, but mechanics point to tightening — keep quality bias" if _ltraj == "TIGHTENING AHEAD"
                else "Liquidity neutral now, mechanics improving — add risk selectively" if _ltraj == "EASING AHEAD"
                else "Liquidity neutral — no strong 2nd-derivative push")
        if _lds == "SCRAMBLE":
            _act = "DOLLAR SHORTAGE — broad de-risk, raise USD/quality"
        out.append({
            "type": "liquidity_inflection", "name": _name, "regime": _lreg,
            "z": lcomp.get("composite_z"), "trajectory": _ltraj, "dollar_shortage": _lds,
            "rationale": _rat, "action_hint": _act,
            "score": (85 if _lds == "SCRAMBLE" else 72 if (_lreg != "NEUTRAL" or (_ltraj and _ltraj != "STABLE / MIXED")) else 55),
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

    # Master Crisis Composite — surface when stress is elevated (DEFCON <= 3)
    crisis = fetch_json("data/crisis-composite.json")
    if crisis and (crisis.get("defcon_level") or 5) <= 3:
        lvl = crisis.get("defcon_level")
        out.append({
            "type": "crisis_composite",
            "name": f"Crisis DEFCON {lvl} — {crisis.get('defcon_name','')}",
            "regime": crisis.get("defcon_name"),
            "score": min(100, (crisis.get("master_crisis_score") or 0) + 20),
            "rationale": (crisis.get("playbook") or "")[:160],
            "action_hint": f"DEFCON {lvl} — {(crisis.get('playbook') or '')[:80]}",
        })

    # Capitulation engine — surface when a buy signal fires
    capit = fetch_json("data/capitulation.json")
    if capit and capit.get("signal") in ("GENERATIONAL_BUY", "STRONG_BUY", "CAPITULATION_WAIT"):
        out.append({
            "type": "capitulation",
            "name": f"Capitulation: {capit.get('signal','').replace('_',' ')}",
            "score": min(100, (capit.get("capitulation_score") or 0) + 25),
            "rationale": (capit.get("action") or "")[:160],
            "action_hint": (capit.get("action") or "")[:90],
        })

    # Leading markets — surface canary turning-point warnings/signals
    leading = fetch_json("data/leading-markets.json")
    if leading and leading.get("turning_point_signal") in (
            "TOP_WARNING", "BOTTOM_SIGNAL", "BROAD_CONTRACTION"):
        sig = leading.get("turning_point_signal")
        out.append({
            "type": "leading_markets",
            "name": f"Canary markets: {sig.replace('_',' ')}",
            "regime": sig,
            "score": 75,
            "rationale": (leading.get("signal_read") or "")[:170],
            "action_hint": f"Flashing: {', '.join(leading.get('flashing_buckets') or []) or 'broad'}",
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
    nowcast = fetch_json("data/macro-nowcast.json", max_age_h=96) or {}
    regime = nowcast.get("regime") or "UNKNOWN"
    fwds = REGIME_FORWARDS.get(regime, {})
    _liqc = (fetch_json("data/liquidity-inflection.json", max_age_h=48) or {})
    _liqcomp = _liqc.get("composite") or {}

    # ── Risk Pack overlay — master crisis read + canary turning point ──
    crisis = fetch_json("data/crisis-composite.json") or {}
    leading = fetch_json("data/leading-markets.json") or {}
    capit = fetch_json("data/capitulation.json") or {}
    defcon = crisis.get("defcon_level")
    tp_signal = leading.get("turning_point_signal")
    cap_signal = capit.get("signal")

    # one-line decisive posture the whole platform can act on
    if cap_signal in ("GENERATIONAL_BUY", "STRONG_BUY"):
        posture = "AGGRESSIVE BUY — washout + stabilising; deploy into quality"
    elif defcon is not None and defcon <= 2:
        posture = "DEFENSIVE — cut beta, raise quality/cash, hedges on"
    elif tp_signal == "TOP_WARNING" or defcon == 3:
        posture = "CAUTIOUS — trim leverage; quality over speculative names"
    elif (defcon is not None and defcon >= 4
          and tp_signal in ("EXPANSION_CONFIRMED", "BOTTOM_SIGNAL")):
        posture = "CONSTRUCTIVE — risk-on; beta and cyclicals favoured"
    else:
        posture = "NEUTRAL — standard positioning"

    return {
        "regime": regime,
        "regime_color": nowcast.get("regime_color"),
        "composite_z": nowcast.get("composite_z") or nowcast.get("normalized_score"),
        "coverage_pct": nowcast.get("coverage_pct"),
        "spy_fwd_6m": fwds.get("6m"),
        "spy_fwd_12m": fwds.get("12m"),
        "defcon_level": defcon,
        "defcon_name": crisis.get("defcon_name"),
        "crisis_trend": crisis.get("trend"),
        "leading_markets_signal": tp_signal,
        "flashing_buckets": leading.get("flashing_buckets"),
        "capitulation_signal": cap_signal,
        "liquidity_score": _liqcomp.get("liquidity_score"),
        "liquidity_regime": _liqcomp.get("regime"),
        "liquidity_z": _liqcomp.get("composite_z"),
        "liquidity_trajectory": (_liqc.get("trajectory") or {}).get("heading"),
        "dollar_shortage": (_liqc.get("dollar_shortage") or {}).get("status"),
        "risk_posture": posture,
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

    # Capital-flow sector overlay (boost sectors with money accelerating in, penalize party-over /
    # price-vs-flow divergence). Kept OUT of systems_dict so it does not inflate n_systems.
    cf_map = {}
    if feeds.get("capital_flow"):
        for _c in (feeds["capital_flow"].get("complexes") or []):
            _pp = _c.get("pump_probability")
            for _s in (_c.get("ref_stocks") or []):
                _s = (_s or "").upper()
                if _s not in cf_map or (_pp or 0) > (cf_map[_s].get("pump_probability") or 0):
                    cf_map[_s] = {"pump_probability": _pp, "regime": _c.get("regime"),
                                  "complex": _c.get("complex"), "divergence": _c.get("flow_price_divergence")}

    # ── Risk-On/Risk-Off regime overlay (cross-asset RORO synthesizer) ──
    _rr = feeds.get("risk_regime") or {}
    _rr_score = _rr.get("risk_regime_score")
    _rr_regime = _rr.get("risk_regime") or "NEUTRAL"
    # ── Composite liquidity-inflection regime (slower macro tide) ──
    _liq = fetch_json("data/liquidity-inflection.json", max_age_h=48) or {}
    _liq_comp = _liq.get("composite") or {}
    _liq_regime = _liq_comp.get("regime") or "NEUTRAL"
    _liq_score = _liq_comp.get("liquidity_score")
    _liq_heading = (_liq.get("trajectory") or {}).get("heading")
    _liq_ds = (_liq.get("dollar_shortage") or {}).get("status")
    HIGH_BETA_SECTORS = {"Technology", "Consumer Cyclical", "Energy", "Financial Services",
                         "Basic Materials", "Communication Services", "Industrials"}
    DEFENSIVE_SECTORS = {"Utilities", "Consumer Defensive", "Healthcare", "Real Estate"}
    ENERGY_MATERIALS = {"Energy", "Basic Materials", "Materials"}
    # ── Nowcast-regime overlay (Fed growth×inflation nowcast: GDPNow + underlying inflation) ──
    _nc_desk = fetch_json("data/nowcast-desk.json", max_age_h=48) or {}
    _nc_quad = _nc_desk.get("nowcast_quadrant") or {}
    _nc_regime = _nc_quad.get("regime")

    def _nowcast_overlay(sector, base_score):
        # regime-conditional macro tilt from the Fed nowcast desk. GOLDILOCKS favors
        # high-beta; OVERHEAT/STAGFLATION favor energy/materials (real assets);
        # STAGFLATION/DISINFLATION lift defensives and haircut cyclicals.
        if not _nc_regime or not sector:
            return base_score, 1.0, None
        hb = sector in HIGH_BETA_SECTORS
        dfn = sector in DEFENSIVE_SECTORS
        em = sector in ENERGY_MATERIALS
        r = _nc_regime
        m = 1.0
        if r == "GOLDILOCKS":
            m = 1.06 if hb else (0.97 if dfn else 1.0)
        elif r == "OVERHEAT":
            m = 1.08 if em else (1.03 if hb else (0.98 if dfn else 1.0))
        elif r == "STAGFLATION":
            m = 1.08 if em else (1.05 if dfn else (0.90 if hb else 1.0))
        elif r == "SOFT LANDING":
            m = 1.03 if hb else 1.0
        elif r.startswith("DISINFLATION"):
            m = 0.90 if em else (1.06 if dfn else (0.95 if hb else 1.0))
        m = round(m, 3)
        if m == 1.0:
            return base_score, 1.0, None
        tag = "nowcast tailwind" if m > 1 else "nowcast headwind"
        return round(base_score * m, 1), m, f"nowcast {r} {tag} ({sector})"

    def _ticker_sector(systems_dict):
        for v in systems_dict.values():
            if isinstance(v, dict) and v.get("sector"):
                return v["sector"]
        return None

    # sector-flow-state overlay (fused RRG-quadrant + posture conviction feed)
    _sf = fetch_json("data/sector-flow-state.json") or {}
    sf_by_name = {s.get("name"): s for s in (_sf.get("sectors") or []) if s.get("name")}
    _SF_NORM = {"Consumer Cyclical": "Consumer Discretionary", "Financial Services": "Financials",
                "Basic Materials": "Materials", "Consumer Defensive": "Consumer Staples"}

    def _sectorflow_overlay(sector, base_score):
        if not sector:
            return base_score, 1.0, None
        nm = _SF_NORM.get(sector, sector)
        s = sf_by_name.get(nm)
        if not s:
            return base_score, 1.0, None
        post, quad, conv = s.get("posture"), s.get("quadrant"), s.get("conviction") or 0
        if post == "OVERWEIGHT" or (quad == "Leading" and conv >= 65):
            return round(base_score * 1.07, 1), 1.07, f"sector Leading/OW · flow-confirmed ({nm})"
        if quad == "Improving":
            return round(base_score * 1.04, 1), 1.04, f"sector rotating in (Improving · {nm})"
        if post == "UNDERWEIGHT" or quad == "Lagging":
            return round(base_score * 0.92, 1), 0.92, f"sector Lagging/UW headwind ({nm})"
        if quad == "Weakening":
            return round(base_score * 0.97, 1), 0.97, f"sector momentum fading ({nm})"
        return base_score, 1.0, None

    def _roro_overlay(sector, base_score):
        if not isinstance(_rr_score, (int, float)) or not sector:
            return base_score, 1.0, None
        hb, dfn = sector in HIGH_BETA_SECTORS, sector in DEFENSIVE_SECTORS
        if not (hb or dfn):
            return base_score, 1.0, None
        if _rr_score >= 35:
            m = 1.08 if hb else 0.96
        elif _rr_score >= 12:
            m = 1.04 if hb else 0.98
        elif _rr_score > -12:
            m = 1.0
        elif _rr_score > -35:
            m = 0.93 if hb else 1.04
        else:
            m = 0.85 if hb else 1.08
        if m == 1.0:
            return base_score, 1.0, None
        tag = "risk-on tilt" if m > 1 else "risk-off de-risk"
        return round(base_score * m, 1), m, f"RORO {_rr_regime} {tag} ({sector})"

    def _liq_overlay(sector, base_score):
        # composite liquidity regime tilt: expanding liquidity favors high-beta /
        # cyclicals, contracting favors defensives. Gentler than RORO — liquidity
        # is a slower-moving macro tide, so it nudges rather than dominates.
        # Also forward-aware: the trajectory nudges even when the regime reads NEUTRAL,
        # and an offshore dollar-shortage scramble broadly de-risks high-beta.
        if not isinstance(_liq_score, (int, float)) or not sector:
            return base_score, 1.0, None
        hb, dfn = sector in HIGH_BETA_SECTORS, sector in DEFENSIVE_SECTORS
        if not (hb or dfn):
            return base_score, 1.0, None
        if _liq_regime == "EXPANDING":
            m = 1.05 if hb else 0.98
        elif _liq_regime == "CONTRACTING":
            m = 0.95 if hb else 1.04
        else:
            m = 1.0
        if _liq_heading == "TIGHTENING AHEAD":
            m *= 0.97 if hb else 1.02
        elif _liq_heading == "EASING AHEAD":
            m *= 1.03 if hb else 0.99
        if _liq_ds == "SCRAMBLE" and hb:
            m *= 0.90
        m = round(m, 3)
        if m == 1.0:
            return base_score, 1.0, None
        tag = "liquidity tailwind" if m > 1 else "liquidity headwind"
        head = (_liq_heading or "flat").replace(" AHEAD", "").lower()
        return round(base_score * m, 1), m, f"liquidity {_liq_regime}/{head} {tag} ({sector})"

    def _cf_overlay(tk, base_score):
        cf = cf_map.get(tk)
        if not cf:
            return base_score, 1.0, None
        pp = cf.get("pump_probability")
        cx = cf.get("complex") or ""
        if cf.get("divergence"):
            return round(base_score * 0.82, 1), 0.82, "capital OUTFLOW vs price · distribution in " + cx
        if "PARTY OVER" in (cf.get("regime") or "") or "TOP WARNING" in (cf.get("regime") or ""):
            return round(base_score * 0.85, 1), 0.85, "sector party-over (" + cx + ")"
        if pp is not None and pp >= 80:
            return round(base_score * 1.12, 1), 1.12, "sector capital ACCELERATING IN (" + cx + ")"
        if pp is not None and pp >= 65:
            return round(base_score * 1.06, 1), 1.06, "sector inflow (" + cx + ")"
        if pp is not None and pp <= 25:
            return round(base_score * 0.90, 1), 0.90, "weak sector flow (" + cx + ")"
        return base_score, 1.0, None

    # cycle overlay map (accumulation-radar): distribution-at-top is a lower-quality rank
    _accum = fetch_json("data/accumulation-radar.json") or {}
    _cycle_map = {}
    for _bk in ("tops", "distributing", "bottoms", "accumulating"):
        _b = _accum.get(_bk) or {}
        for _r in (_b.get("stocks") or []) + (_b.get("etfs") or []):
            _cycle_map.setdefault(_r.get("ticker"), _r)
    # red-flag map: insider dumping / Beneish manipulation / low earnings quality
    _redflag = {}
    for _r in (fetch_json("data/beneish.json") or {}).get("red_flags") or []:
        _redflag.setdefault(_r.get("ticker"), []).append("fails Beneish manipulation test")
    for _r in (fetch_json("data/insider-sell-cluster.json") or {}).get("top_clusters") or []:
        _redflag.setdefault(_r.get("ticker"), []).append("cluster of insider selling")
    for _r in (fetch_json("data/earnings-quality.json") or {}).get("top_10_low_quality_avoid") or []:
        _redflag.setdefault(_r.get("ticker"), []).append("low earnings quality")
    _redflag.pop(None, None)

    # Compute conviction for every ticker
    ticker_ranks = []
    for ticker, systems_dict in ticker_idx.items():
        score, contributions = compute_conviction(systems_dict, calibration)
        n_sys = len(systems_dict)
        score, cf_mult, cf_note = _cf_overlay(ticker, score)
        _sector = _ticker_sector(systems_dict)
        score, roro_mult, roro_note = _roro_overlay(_sector, score)
        score, liq_mult, liq_note = _liq_overlay(_sector, score)
        score, sf_mult, sf_note = _sectorflow_overlay(_sector, score)
        score, nc_mult, nc_note = _nowcast_overlay(_sector, score)
        # cycle gate — tag phase; haircut only the strongest distribution-at-top tell
        _cyc = _cycle_map.get(ticker)
        _cyc_phase = (_cyc or {}).get("phase")
        _cyc_warn = None
        if _cyc:
            if _cyc.get("flag") == "LIKELY_TOP" and _cyc.get("divergence") == "bearish":
                score = round(score * 0.93, 2); _cyc_warn = "likely_top_bearish_divergence"
            elif _cyc.get("flag") == "LIKELY_TOP" or _cyc_phase == "DISTRIBUTION":
                _cyc_warn = "distribution_at_top"
        # red-flag gate: dumping / manipulation / poor quality => demote + tag
        _rf = _redflag.get(ticker)
        if _rf:
            score = round(score * 0.80, 2)
        rationale = synthesize_rationale(ticker, systems_dict, score)
        if cf_note:
            rationale += " · " + cf_note
        if roro_note:
            rationale += " · " + roro_note
        if liq_note:
            rationale += " · " + liq_note
        if sf_note:
            rationale += " · " + sf_note
        if nc_note:
            rationale += " · " + nc_note
        if _cyc_warn:
            rationale += " · cycle: " + _cyc_warn
        if _rf:
            rationale += " · ⚠️ red flag: " + "; ".join(_rf)
        ticker_ranks.append({
            "ticker": ticker,
            "score": score,
            "n_systems": n_sys,
            "systems": sorted(systems_dict.keys()),
            "contributions": contributions,
            "capital_flow_mult": cf_mult,
            "risk_regime_mult": roro_mult,
            "liquidity_regime_mult": liq_mult,
            "nowcast_regime_mult": nc_mult,
            "cycle_phase": _cyc_phase,
            "cycle_warning": _cyc_warn,
            "red_flags": _rf,
            "rationale": rationale,
            "details": systems_dict,
        })
    ticker_ranks.sort(key=lambda r: r["score"], reverse=True)
    top_tickers = ticker_ranks[:25]

    # ── Structural-chokepoint overlay: flag ranked names the chokepoint engine marks as
    #    structurally indispensable (curated / LLM-confirmed / supply-chain hub). Context for
    #    durability, not a score change — a top rank on a chokepoint is a higher-quality rank.
    # ── ops 3145 fusion overlays (additive context, no score change) ──
    # ops 3171: Khalid's own notes ride the ranking as context
    _notes_idx = (fetch_json("data/notes-index.json") or {}).get("index") or {}
    _kill = fetch_json("data/kill-theses.json") or {}
    _kill_idx = {}
    for _t in (_kill.get("theses") or []):
        if _t.get("error"):
            continue
        _tk = (_t.get("symbol") or _t.get("ticker") or "").upper()
        if not _tk:
            continue
        _c = _t.get("kill_conditions") or []
        _first = (_c[0].get("risk") or _c[0].get("condition") or ""
                  ) if _c and isinstance(_c[0], dict) else (
                  str(_c[0]) if _c else "")
        _kill_idx[_tk] = (_first or _t.get("risk") or "")[:160]
    _sqf = fetch_json("data/squeeze-fuel.json") or {}
    _sqf_idx = {(r.get("ticker") or "").upper():
                {"score": r.get("score"), "state": r.get("state")}
                for r in (_sqf.get("board") or []) if r.get("ticker")}
    _deal_idx = {}                                      # ops 3572: fresh deal-win overlay
    try:
        for _dd in (fetch_json("data/deal-scanner.json") or {}).get("deals", []) or []:
            _dt = str(_dd.get("symbol") or "").upper()
            if not _dt or (_dd.get("age_h") or 999) > 72:
                continue
            if _dt not in _deal_idx or (_dd.get("score") or 0) > (_deal_idx[_dt].get("score") or 0):
                _deal_idx[_dt] = {"value": _dd.get("deal_value_str"),
                                  "vs_mc_pct": _dd.get("vs_market_cap_pct"),
                                  "materiality_pct": _dd.get("materiality_pct"),
                                  "highlight": _dd.get("highlight"),
                                  "ai_megadeal": bool(_dd.get("ai_megadeal")),
                                  "age_h": _dd.get("age_h"), "score": _dd.get("score"),
                                  "title": (_dd.get("title") or "")[:120]}
    except Exception:
        _deal_idx = {}
    n_kill = n_sqf = n_notes = 0
    n_deal = 0
    for t in top_tickers:
        if t["ticker"] in _kill_idx:
            t["kill_risk"] = _kill_idx[t["ticker"]]
            n_kill += 1
        _kn = _notes_idx.get(str(t.get("ticker") or "").upper())
        if _kn:                              # ops 3171: his own research
            t["khalid_note"] = {"n": _kn["n_notes"], "stance": _kn["stance"],
                                "score": _kn["stance_score"],
                                "last": _kn["last_note_at"]}
            n_notes += 1
        _cc3 = census_idx(S3, "justhodl-dashboard-live").get(
            str(t.get("ticker") or "").upper())
        if _cc3:
            t["census"] = _cc3
        if t["ticker"] in _sqf_idx:
            t["squeeze_fuel"] = _sqf_idx[t["ticker"]]
            n_sqf += 1
        if t["ticker"] in _deal_idx:                    # ops 3572: deal-win overlay
            t["deal_win"] = _deal_idx[t["ticker"]]
            n_deal += 1
    print(f"[ranker] fusion overlays: kill_risk={n_kill} squeeze_fuel={n_sqf} khalid_notes={n_notes} deal_wins={n_deal}")

    _ck = fetch_json("data/chokepoint.json") or {}
    _structural = _ck.get("structural_names") or {}
    _hiconv = {r.get("ticker") for r in (_ck.get("highest_conviction_book") or [])}
    n_structural = 0
    for t in top_tickers:
        sn = _structural.get(t["ticker"])
        if not sn:
            continue
        n_structural += 1
        t["structural_chokepoint"] = True
        t["criticality"] = sn.get("criticality")
        note = "structural chokepoint (crit %s)" % sn.get("criticality")
        if t["ticker"] in _hiconv:
            t["highest_conviction"] = True
            note += " AT a trough/cheap — system's highest-quality setup"
        t["rationale"] = (t.get("rationale") or "") + " · " + note


    # ── Capture-gap overlay (ops 3780) ──
    # Value CREATION vs value CAPTURE: criticality percentile minus market-cap-share
    # percentile, within industry (plus the cross-industry variant). Attached as
    # CONTEXT ONLY — like the structural overlay above, this does NOT change score
    # or rank: capture gap is a slow valuation-structure fact, while this board's
    # conviction weights are calibrated on shorter-horizon evidence.
    _cap = (_ck.get("capture_gap") or {})
    _cap_rows = {r.get("ticker"): r for r in (_cap.get("all_rows") or [])}
    n_capture = 0
    for t in top_tickers:
        cr = _cap_rows.get(t.get("ticker"))
        if not cr:
            continue
        n_capture += 1
        t["capture_gap"] = cr.get("capture_gap")
        t["global_capture_gap"] = cr.get("global_capture_gap")
        t["capture_tier"] = cr.get("tier")
        t["mcap_share_pct"] = cr.get("mcap_share_pct")
        t["undervaluation_score"] = cr.get("undervaluation_score")
        if cr.get("catchup_pct") is not None:
            t["catchup_pct"] = cr.get("catchup_pct")
            t["catchup_basis"] = cr.get("catchup_basis")
        if cr.get("tier") == "STRUCTURALLY_UNDERVALUED":
            _n = "captures %.0fpp less of its industry than its criticality implies" % (
                cr.get("capture_gap") or 0)
            if cr.get("catchup_pct") is not None:
                _n += "; %.0f%% to industry-median multiple (%s — arithmetic, not a target)" % (
                    cr["catchup_pct"], cr.get("catchup_basis") or "-")
            t["rationale"] = (t.get("rationale") or "") + " · " + _n
    print("[master-ranker] capture_gap joined=%d" % n_capture)

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
        "risk_regime": {
            "score": _rr_score, "regime": _rr_regime,
            "posture": _rr.get("posture"),
            "tells": (_rr.get("tells") or [])[:5],
        },
        "nowcast_regime": {
            "regime": _nc_regime,
            "gdpnow": _nc_quad.get("gdpnow"),
            "underlying_inflation": _nc_quad.get("underlying_inflation"),
            "growth": _nc_quad.get("growth"),
            "inflation": _nc_quad.get("inflation"),
            "tilt": ("energy/materials + defensives" if _nc_regime == "STAGFLATION"
                     else "high-beta / cyclicals" if _nc_regime == "GOLDILOCKS"
                     else "energy/materials + cyclicals" if _nc_regime == "OVERHEAT"
                     else "defensives / duration" if (_nc_regime or "").startswith("DISINFLATION")
                     else "balanced" if _nc_regime == "SOFT LANDING" else None),
        },
        "n_tickers_total": len(ticker_idx),
        "n_macro_signals": len(macro_signals),
        "alerts": {
            "n_tier_3_plus_systems": n_tier_3_plus,
            "n_tier_5_plus_systems": n_tier_5_plus,
            "n_extreme_macro": n_extreme_macro,
            "calibration_active": bool(calibration),
            "n_structural_chokepoints": n_structural,
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
        "feed_freshness": sorted(_FEED_HEALTH, key=lambda f: -(f.get("age_h") or 0)),
        "stale_feeds_excluded": [f["key"] for f in _FEED_HEALTH if f.get("stale")],
        "missing_feeds": [f["key"] for f in _FEED_HEALTH if f.get("missing")],
        "calibration_weights": calibration,
        "duration_s": round(time.time() - started, 2),
    }

    payload["wl_research"] = __import__("wl_fusion").block(("CREDIT",))
    body_bytes = json.dumps(payload, indent=2, default=str).encode("utf-8")
    S3.put_object(
        Bucket=BUCKET, Key=S3_KEY_OUT, Body=body_bytes,
        ContentType="application/json", CacheControl="max-age=300",
    )

    print(f"[master-ranker] DONE in {payload['duration_s']}s · "
          f"{len(top_tickers)} tickers · {len(macro_signals)} macro · "
          f"{n_tier_3_plus} tier-3+, {n_tier_5_plus} tier-5+")

    # ─── Emit convergence.tier_up events for new tier-3+/tier-5+ tickers ──
    # Institutional value: when multiple independent systems converge on
    # the same ticker, that's a stronger signal than any one system alone.
    # Operators want to know IMMEDIATELY when a name reaches tier-5+ (5
    # independent systems agree).
    try:
        # Read previous run's ranked tickers to detect new tier crossings
        prev_tier = {}
        try:
            prev_obj = S3.get_object(Bucket=BUCKET, Key=S3_KEY_OUT + ".prev")
            prev_data = json.loads(prev_obj["Body"].read().decode("utf-8"))
            for t in prev_data.get("top_tickers", []) or []:
                prev_tier[t.get("ticker")] = t.get("n_systems", 0)
        except Exception:
            pass
        
        from system_events import publish_many
        tier_events = []
        for t in top_tickers:
            ticker = t.get("ticker")
            curr_n = t.get("n_systems", 0)
            prev_n = prev_tier.get(ticker, 0)
            
            # Tier-3 first appearance OR tier-5 first appearance
            if prev_n < 3 and curr_n >= 3:
                tier_events.append(("convergence.tier_up", {
                    "ticker":    ticker,
                    "new_tier":  3,
                    "n_systems": curr_n,
                    "systems":   list(t.get("systems") or [])[:10],
                    "score":     t.get("score"),
                }))
            if prev_n < 5 and curr_n >= 5:
                tier_events.append(("convergence.tier_up", {
                    "ticker":    ticker,
                    "new_tier":  5,
                    "n_systems": curr_n,
                    "systems":   list(t.get("systems") or [])[:10],
                    "score":     t.get("score"),
                }))
        
        # Publish in 10-entry batches per EventBridge limit
        for i in range(0, len(tier_events), 10):
            publish_many(tier_events[i:i+10])
        
        # Persist current state for next run's comparison
        S3.put_object(
            Bucket=BUCKET, Key=S3_KEY_OUT + ".prev",
            Body=json.dumps({
                "as_of":   payload["as_of"],
                "top_tickers": [
                    {"ticker": t.get("ticker"), "n_systems": t.get("n_systems")}
                    for t in top_tickers
                ],
            }, default=str).encode("utf-8"),
            ContentType="application/json",
        )
        if tier_events:
            print(f"[master-ranker] emitted {len(tier_events)} tier-up events")
    except Exception as e:
        print(f"[master-ranker] event publish failed: {e}")

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
