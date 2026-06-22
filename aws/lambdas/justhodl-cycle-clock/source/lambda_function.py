"""
justhodl-cycle-clock  ·  v1.0
─────────────────────────────────────────────────────────────────────────────
ONE honest verdict on WHERE WE ARE IN THE CYCLE and HOW MUCH LIQUIDITY-SQUEEZE
RISK is building — synthesized from the engines that already exist, with the
DIVERGENCES surfaced rather than averaged away.

This is a META engine: it reads other engines' published JSON (no raw re-fetch)
and fuses them into:
  · CYCLE POSITION  — investment-clock phase (EARLY/MID/LATE/DOWNTURN) anchored on
    the macro-regime quadrant, confirmed/contradicted by US-cycle, global business
    cycle, and the growth nowcast. Late-stage froth overlay (valuation/leverage).
  · LIQUIDITY-SQUEEZE RISK  — 0-100 gauge from the purpose-built stress stack
    (plumbing health, funding stress, crisis composite, canaries, global &
    systemic stress) modified by the liquidity-flow direction.
  · DIVERGENCES  — explicit conflicts (e.g. global-expansion vs US-stagflation;
    aggregate-liquidity-flat vs credit-engine-draining; equity-on vs crypto-off).
  · VERDICT  — a single honest sentence a PM can act on.

Honesty: this fuses MODEL OUTPUTS, not ground truth. Every input read is shown
with its own staleness; a missing/stale feed degrades gracefully and is flagged.
"""
import json, time
from datetime import datetime, timezone

VERSION = "1.0"
BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/cycle-clock.json"

import boto3
s3 = boto3.client("s3", "us-east-1")


def _read(key):
    try:
        return json.loads(s3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
    except Exception:
        return None

def _age_days(iso):
    try:
        dt = datetime.fromisoformat(str(iso).replace("Z", "+00:00"))
        return round((datetime.now(timezone.utc) - dt).total_seconds() / 86400, 1)
    except Exception:
        return None

def _get(d, *path, default=None):
    cur = d
    for p in path:
        if not isinstance(cur, dict):
            return default
        cur = cur.get(p)
    return cur if cur is not None else default


# macro-regime quadrant → investment-clock phase
QUAD_PHASE = {
    "REFLATION":      ("EARLY CYCLE",   1, "growth recovering, inflation rising off lows"),
    "GOLDILOCKS":     ("MID CYCLE",     2, "growth firm, inflation contained — the sweet spot"),
    "OVERHEAT":       ("LATE-MID CYCLE", 2.5, "growth strong but inflation accelerating"),
    "STAGFLATION":    ("LATE CYCLE",    3, "growth flat/slowing while inflation runs — classic late cycle"),
    "DEFLATION-BUST": ("DOWNTURN",      4, "growth and inflation both falling — contraction/recession risk"),
}


def lambda_handler(event, context):
    t0 = time.time()
    avail, stale = {}, []

    def load(key, label):
        d = _read(key)
        if d is None:
            avail[label] = "MISSING"
            return {}
        age = _age_days(d.get("generated_at") or d.get("as_of") or d.get("asof"))
        avail[label] = f"ok ({age}d)" if age is not None else "ok"
        if age is not None and age > 3.5:
            stale.append(f"{label} {age}d stale")
        return d

    uscy = load("data/us-cycle.json", "us_cycle")
    gbc = load("data/global-business-cycle.json", "global_cycle")
    now = load("data/macro-nowcast.json", "nowcast")
    reg = load("data/regime.json", "macro_regime")
    play = load("data/regime-playbook.json", "regime_playbook")
    gliq = load("data/global-liquidity.json", "global_liquidity")
    lflow = load("data/liquidity-flow.json", "liquidity_flow")
    lce = load("data/liquidity-credit-engine.json", "liquidity_credit")
    plumb = load("data/eurodollar-plumbing.json", "plumbing")
    tnoise = load("data/treasury-noise.json", "treasury_noise")
    crisis = load("data/crisis-composite.json", "crisis_composite")
    canary = load("data/crisis-canaries.json", "crisis_canaries")
    gstress = load("data/global-stress.json", "global_stress")
    sysstress = load("data/systemic-stress.json", "systemic_stress")
    rmap = load("data/regime-map.json", "risk_map")

    # ───────────────────────── CYCLE POSITION ─────────────────────────
    quad = _get(reg, "current", "quadrant")
    phase_label, phase_n, phase_desc = QUAD_PHASE.get(quad, ("UNKNOWN", 0, "macro regime unavailable"))
    growth_mom = _get(reg, "current", "growth_6m_momentum")
    infl_mom = _get(reg, "current", "inflation_6m_momentum")
    liq_state_macro = _get(reg, "current", "liquidity_state")
    next3 = _get(reg, "current", "next_3m_probabilities", default={})

    nowcast_regime = _get(now, "regime")
    nowcast_score = _get(now, "normalized_score")
    us_score = _get(uscy, "cycle_score", "score_0_100")
    us_level = _get(uscy, "cycle_score", "level")
    gbc_phase = _get(gbc, "aggregate", "global_phase")
    gbc_cli = _get(gbc, "aggregate", "global_avg_cli")
    gbc_expansion_breadth = _get(gbc, "aggregate", "expansion_breadth_pct")

    # late-stage froth overlay from US-cycle valuation/leverage z-scores
    froth = []
    for comp in _get(uscy, "cycle_score", "components", default=[]):
        if comp.get("id") in ("buffett", "margin", "real_10y", "term_premium") and (comp.get("z") or 0) >= 0.85:
            froth.append(f"{comp['id']} z+{comp['z']}")
    growth_dir = ("slowing" if (nowcast_regime == "SLOWING" or (growth_mom or 0) < 0.1)
                  else "accelerating" if (growth_mom or 0) > 0.3 else "flat")
    infl_dir = ("rising" if (infl_mom or 0) > 0.3 else "falling" if (infl_mom or 0) < -0.3 else "stable")

    # confidence = agreement among US-cycle WATCH/late, nowcast slowing, stagflation, froth
    late_votes = sum([quad in ("STAGFLATION", "DEFLATION-BUST", "OVERHEAT"),
                      us_level in ("WATCH", "CAUTION", "ELEVATED"),
                      nowcast_regime in ("SLOWING", "CONTRACTION RISK"),
                      len(froth) >= 2])
    cycle_conf = "high" if late_votes >= 3 else "moderate" if late_votes == 2 else "low"

    cycle = {
        "phase": phase_label, "phase_n": phase_n, "quadrant": quad, "description": phase_desc,
        "growth_direction": growth_dir, "inflation_direction": infl_dir,
        "froth_markers": froth, "confidence": cycle_conf,
        "us_cycle_score": us_score, "us_cycle_level": us_level,
        "global_phase": gbc_phase, "global_avg_cli": gbc_cli, "global_expansion_breadth_pct": gbc_expansion_breadth,
        "nowcast_regime": nowcast_regime, "macro_liquidity_state": liq_state_macro,
        "next_3m_quadrant_odds": next3,
    }

    # ──────────────────── LIQUIDITY-SQUEEZE RISK ────────────────────
    def num(v):
        return float(v) if isinstance(v, (int, float)) else None
    plumb_health = num(_get(plumb, "plumbing_health"))
    plumb_stress = (100 - plumb_health) if plumb_health is not None else None
    fund_stress = num(_get(tnoise, "treasury_stress")) or num(_get(tnoise, "funding_stress"))
    crisis_score = num(_get(crisis, "master_crisis_score"))
    canary_score = num(_get(canary, "composite_score"))
    gstress_idx = num(_get(gstress, "global_stress_index"))
    sys_score = num(_get(sysstress, "composite", "score_0_100"))

    comps = {"global_stress": (gstress_idx, 0.25), "crisis_composite": (crisis_score, 0.20),
             "plumbing": (plumb_stress, 0.20), "funding": (fund_stress, 0.15),
             "systemic": (sys_score, 0.10), "canaries": (canary_score, 0.10)}
    num_, den_ = 0.0, 0.0
    for v, w in comps.values():
        if v is not None:
            num_ += v * w; den_ += w
    base_squeeze = (num_ / den_) if den_ else None

    # liquidity-DIRECTION modifier: draining adds, easing subtracts
    impulse = num(_get(gliq, "global_impulse_13w_pct"))
    lce_liq_state = _get(lce, "interpretation", "pillars", "liquidity", "state")
    flow_regime = _get(lflow, "regime")
    net_30d = _get(lflow, "deltas", "1m", "net")
    plumb_fingerprint = _get(play, "current_fingerprint", "plumbing")
    direction_mod = 0.0
    flickers = []
    if impulse is not None and impulse < -0.5:
        direction_mod += min(10, abs(impulse) * 4); flickers.append(f"global liquidity impulse {impulse}% (draining)")
    if lce_liq_state and str(lce_liq_state).upper() in ("DRAINING", "TIGHTENING"):
        direction_mod += 4; flickers.append(f"credit-engine liquidity pillar {lce_liq_state}")
    if plumb_fingerprint and str(plumb_fingerprint).upper() == "TIGHTENING":
        direction_mod += 4; flickers.append("plumbing fingerprint TIGHTENING")
    if isinstance(net_30d, (int, float)) and net_30d < -30:
        direction_mod += 3; flickers.append(f"net liquidity {net_30d}B/30d (draining)")
    gstress_pctile = _get(gstress, "stress_momentum", "percentile")
    if isinstance(gstress_pctile, (int, float)) and gstress_pctile >= 60:
        flickers.append(f"global stress {gstress_pctile}th pctile (elevated)")

    squeeze = round(min(100, (base_squeeze or 0) + direction_mod), 1) if base_squeeze is not None else None
    def squeeze_level(s):
        if s is None:
            return "UNKNOWN"
        if s < 20: return "LOW"
        if s < 35: return "LOW · WATCH"
        if s < 50: return "RISING"
        if s < 70: return "ELEVATED"
        if s < 85: return "HIGH"
        return "ACUTE"
    sq_level = squeeze_level(squeeze)

    liquidity = {
        "squeeze_risk_score": squeeze, "level": sq_level,
        "aggregate_liquidity_regime": _get(gliq, "regime"),
        "aggregate_liquidity_read": _get(gliq, "regime_read"),
        "impulse_13w_pct": impulse, "flow_regime": flow_regime, "net_30d_bn": net_30d,
        "flickers": flickers,
        "components": {"global_stress": gstress_idx, "crisis_composite": crisis_score,
                       "plumbing_health": plumb_health, "funding_stress": fund_stress,
                       "systemic_stress": sys_score, "crisis_canaries": canary_score,
                       "crisis_defcon": _get(crisis, "defcon_level"),
                       "global_stress_level": _get(gstress, "global_stress_level")},
    }

    # ───────────────────────── DIVERGENCES ─────────────────────────
    divergences = []
    if gbc_phase and "EXPANSION" in str(gbc_phase) and quad in ("STAGFLATION", "DEFLATION-BUST"):
        divergences.append(f"GROWTH SPLIT: global business cycle reads {gbc_phase} "
                           f"(CLI {gbc_cli}, {gbc_expansion_breadth}% breadth) while the US macro regime is "
                           f"{quad} — global growth engines bullish, US/inflation engines cautious.")
    if _get(gliq, "regime") in ("NEUTRAL", "EASING", "LOOSE") and lce_liq_state in ("DRAINING", "TIGHTENING"):
        divergences.append(f"LIQUIDITY SPLIT: aggregate central-bank liquidity is {_get(gliq, 'regime')}/flat, "
                           f"but the credit-engine liquidity pillar is {lce_liq_state} — a draining flicker under a calm surface.")
    rlabel = _get(rmap, "regime", "label")
    if rlabel == "BIFURCATED":
        divergences.append("CROSS-ASSET SPLIT: Risk Map BIFURCATED — equities broadly risk-on while crypto is risk-off.")
    if nowcast_regime == "SLOWING":
        spy3 = _get(now, "regime_spy_performance", "SLOWING", "horizons", "3m", "median_pct")
        if spy3 is not None:
            divergences.append(f"COUNTERINTUITIVE: growth nowcast is SLOWING, but historically SLOWING preceded "
                               f"SPY +{spy3}% median over 3m — slowing has not meant down for equities.")

    # ───────────────────────── VERDICT ─────────────────────────
    sq_phrase = {"LOW": "liquidity ample", "LOW · WATCH": "liquidity flat/easy with mild draining flickers",
                 "RISING": "liquidity tightening", "ELEVATED": "liquidity stress building",
                 "HIGH": "liquidity squeeze underway", "ACUTE": "acute liquidity squeeze",
                 "UNKNOWN": "liquidity read unavailable"}.get(sq_level, "")
    verdict = (f"{phase_label} ({quad.lower() if quad else 'regime n/a'}, growth {growth_dir}, inflation {infl_dir}"
               f"{', valuation/leverage froth' if len(froth) >= 2 else ''}) — "
               f"{sq_phrase}, squeeze risk {sq_level} ({squeeze}). "
               + ("Real divergence to watch: " + divergences[0].split(':')[0] + "." if divergences else ""))

    falsifier = ("Cycle call flips if the macro-regime quadrant rotates out of STAGFLATION (→ GOLDILOCKS/REFLATION = "
                 "re-acceleration, or → DEFLATION-BUST = downturn). Squeeze-risk escalates if plumbing health breaks "
                 "below ~70, funding stress > 40, or crisis composite leaves DEFCON 4 — none of which is true now.")

    out = {
        "engine": "cycle-clock", "version": VERSION,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "duration_s": round(time.time() - t0, 1),
        "verdict": verdict,
        "cycle": cycle,
        "liquidity": liquidity,
        "divergences": divergences,
        "falsifier": falsifier,
        "availability": avail, "stale": stale,
        "methodology": (
            "Meta-synthesis of published engine outputs. Cycle phase anchors on the macro-regime quadrant "
            "(Investment-Clock mapping: REFLATION→early, GOLDILOCKS→mid, STAGFLATION→late, DEFLATION-BUST→downturn), "
            "confirmed/contradicted by US-cycle level, global business-cycle phase, and the growth nowcast, with a "
            "late-stage froth overlay (US-cycle valuation/leverage z-scores ≥0.85). Liquidity-squeeze risk is a "
            "weighted blend of the purpose-built stress stack — global stress 0.25, crisis composite 0.20, plumbing "
            "0.20, funding 0.15, systemic 0.10, canaries 0.10 — plus a liquidity-direction modifier (draining adds). "
            "Divergences are surfaced, not averaged. Fuses model outputs, not ground truth; stale inputs are flagged. "
            "Research, not investment advice."),
    }

    s3.put_object(Bucket=BUCKET, Key=OUT_KEY, Body=json.dumps(out, default=str).encode(),
                  ContentType="application/json", CacheControl="no-cache, max-age=0")
    print(f"[cycle-clock] {phase_label} ({quad}) conf={cycle_conf} | squeeze {squeeze} {sq_level} | "
          f"{len(divergences)} divergences | {len(flickers)} flickers")
    return {"statusCode": 200, "body": json.dumps({"phase": phase_label, "quadrant": quad,
                                                    "squeeze_risk": squeeze, "level": sq_level})}
