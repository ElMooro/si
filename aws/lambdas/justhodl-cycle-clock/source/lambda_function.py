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

VERSION = "2.1"
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
import urllib.request, urllib.parse
from statistics import mean, pstdev

FRED_KEY = "2f057499936072679d8843d7fce99989"


def _fred(series, start="2006-01-01"):
    try:
        u = "https://api.stlouisfed.org/fred/series/observations?" + urllib.parse.urlencode(
            {"series_id": series, "api_key": FRED_KEY, "file_type": "json", "observation_start": start})
        j = json.loads(urllib.request.urlopen(u, timeout=12).read())
        return [(o["date"], float(o["value"])) for o in j.get("observations", [])
                if o.get("value") not in (None, ".", "")]
    except Exception:
        return []


def _yoy(pts):
    d = dict(pts); ks = sorted(d); out = []
    for i, k in enumerate(ks):
        if i >= 12 and d[ks[i - 12]]:
            out.append((k, d[k] / d[ks[i - 12]] - 1.0))
    return out


def _z_series(pts, lb=120):
    vals = [v for _, v in pts]; dates = [d for d, _ in pts]; out = []
    for i in range(len(vals)):
        if i < 24:
            continue
        w = vals[max(0, i - lb + 1):i + 1]
        m = mean(w); sd = pstdev(w) if len(w) > 1 else 0
        out.append((dates[i], round((vals[i] - m) / sd, 2) if sd else 0.0))
    return out


def _coord_series():
    """Monthly growth_z & inflation_z from FRED momentum (industrial production + payrolls for
    growth; headline CPI + core PCE for inflation), aligned → last ~13 months for the trail + now."""
    def blend(a, b):
        da, db = dict(a), dict(b); ks = sorted(set(da) & set(db))
        return {k: round((da[k] + db[k]) / 2, 2) for k in ks}
    g = blend(_z_series(_yoy(_fred("INDPRO"))), _z_series(_yoy(_fred("PAYEMS"))))
    f = blend(_z_series(_yoy(_fred("CPIAUCSL"))), _z_series(_yoy(_fred("PCEPILFE"))))
    ks = sorted(set(g) & set(f))[-13:]
    return [{"m": k[:7], "g": g[k], "i": f[k]} for k in ks]


def _quad2d(g, i):
    if g is None or i is None:
        return None
    if g >= 0 and i < 0:  return "GOLDILOCKS"
    if g >= 0 and i >= 0: return "OVERHEAT"
    if g < 0 and i >= 0:  return "STAGFLATION"
    return "DOWNTURN"


# historical asset leadership by clock quadrant (Merrill investment-clock canon)
QUAD_ASSETS = {
    "GOLDILOCKS":  {"clock": "Recovery",    "lead": ["Equities — cyclicals & tech", "High-yield credit", "EM equities"],
                    "lag": ["Cash", "Long-duration Treasuries"]},
    "OVERHEAT":    {"clock": "Overheat",     "lead": ["Commodities — energy & materials", "Value / cyclical equities", "TIPS / inflation hedges"],
                    "lag": ["Long Treasuries", "Long-duration growth"]},
    "STAGFLATION": {"clock": "Stagflation",  "lead": ["Cash / T-bills", "Gold", "Energy & commodities", "Defensives — staples, utilities, healthcare"],
                    "lag": ["Long-duration equities", "High-yield credit"]},
    "DOWNTURN":    {"clock": "Reflation",    "lead": ["Long Treasuries — duration", "Gold", "Quality / defensive equities"],
                    "lag": ["Cyclicals", "High-yield credit", "Commodities"]},
}


COORD_PHASE = {"GOLDILOCKS": ("MID CYCLE", 2), "OVERHEAT": ("LATE-MID CYCLE", 2.5),
               "STAGFLATION": ("LATE CYCLE", 3), "DOWNTURN": ("DOWNTURN", 4)}


def _net_liquidity():
    """Fed net liquidity = balance sheet − reverse repo − Treasury General Account, with the
    13-week change. WALCL is $millions, RRP/TGA are $billions — normalize all to $trillions."""
    walcl = _fred("WALCL", start="2017-01-01")
    rrp = _fred("RRPONTSYD", start="2017-01-01")
    tga = _fred("WTREGEN", start="2017-01-01")
    if not walcl:
        return None
    def last(p): return p[-1][1] if p else 0.0
    def back(p, n): return p[-(n + 1)][1] if len(p) > n else (p[0][1] if p else 0.0)
    w, r, t = last(walcl) / 1e6, last(rrp) / 1e3, last(tga) / 1e3
    w13, r13, t13 = back(walcl, 13) / 1e6, back(rrp, 65) / 1e3, back(tga, 13) / 1e3
    net, net13 = w - r - t, w13 - r13 - t13
    return {"walcl_tn": round(w, 3), "rrp_tn": round(r, 3), "tga_tn": round(t, 3),
            "net_tn": round(net, 3), "net_13w_delta_bn": round((net - net13) * 1000, 1),
            "as_of": walcl[-1][0]}


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
    rrisk = load("data/risk-regime.json", "risk_regime")
    msurp = load("data/macro-surprise.json", "macro_surprise")
    ycurve = load("data/yield-curve.json", "yield_curve")
    credit = load("data/credit-stress.json", "credit_stress")
    secrot = load("data/sector-rotation.json", "sector_rotation")
    rcomp = load("data/regime-composite.json", "regime_composite")
    fomc = load("data/fomc-reaction.json", "fomc_reaction")
    sovf = load("data/sovereign-fiscal.json", "sovereign_fiscal")
    sfails = load("data/settlement-fails.json", "settlement_fails")
    analogs_d = load("data/historical-analogs.json", "historical_analogs")

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

    # ── 2D growth×inflation coordinates + 12-month trail (FRED momentum z) ──
    trail = _coord_series()
    coord = trail[-1] if trail else None
    g_now = coord["g"] if coord else None
    i_now = coord["i"] if coord else None
    quad2d = _quad2d(g_now, i_now)
    # data-surprise tilt (macro-surprise engine) — complementary near-term read
    surprise = {"growth_z": _get(msurp, "growth_z"), "inflation_z": _get(msurp, "inflation_z"),
                "composite_z": _get(msurp, "composite_z"), "regime": _get(msurp, "regime")}
    # asset leadership for the current quadrant (canonical clock) + system playbook leaders
    assets = None
    if quad2d in QUAD_ASSETS:
        a = QUAD_ASSETS[quad2d]
        leaders = _get(play, "proven_signal_leaders", default=[])
        assets = {"clock_phase": a["clock"], "lead": a["lead"], "lag": a["lag"],
                  "system_leaders": leaders[:5] if isinstance(leaders, list) else []}
    # recession-probability composite (yield curve + credit + nowcast + sector rotation)
    rp_votes, rp_max = 0.0, 0.0
    yc_regime = _get(ycurve, "regime"); yc_inv = _get(ycurve, "inversion_flags", default={})
    if yc_regime is not None:
        rp_max += 35
        if "INVERT" in str(yc_regime).upper() or (isinstance(yc_inv, dict) and any(yc_inv.values())):
            rp_votes += 35
        elif "BULL" in str(yc_regime).upper() and "STEEP" in str(yc_regime).upper():
            rp_votes += 20
    cr_regime = _get(credit, "composite_regime")
    if cr_regime is not None:
        rp_max += 25
        if str(cr_regime).upper() in ("STRESS", "WIDENING", "ELEVATED", "RISK-OFF"):
            rp_votes += 25
    if nowcast_regime is not None:
        rp_max += 25
        if nowcast_regime in ("SLOWING", "CONTRACTION RISK"):
            rp_votes += (25 if nowcast_regime == "CONTRACTION RISK" else 12)
    sr_app = _get(secrot, "risk_appetite")
    if sr_app is not None:
        rp_max += 15
        if str(sr_app).upper() in ("RISK-OFF", "DEFENSIVE", "DEFENSIVE LEADERSHIP"):
            rp_votes += 15
    recession_prob = round(rp_votes / rp_max * 100) if rp_max else None

    cycle = {
        "phase": phase_label, "phase_n": phase_n, "quadrant": quad, "description": phase_desc,
        "growth_direction": growth_dir, "inflation_direction": infl_dir,
        "froth_markers": froth, "confidence": cycle_conf,
        "us_cycle_score": us_score, "us_cycle_level": us_level,
        "global_phase": gbc_phase, "global_avg_cli": gbc_cli, "global_expansion_breadth_pct": gbc_expansion_breadth,
        "nowcast_regime": nowcast_regime, "macro_liquidity_state": liq_state_macro,
        "next_3m_quadrant_odds": next3,
        "coordinates": coord, "trail": trail, "quadrant_2d": quad2d,
        "headline_quadrant": quad2d or quad,
        "headline_phase": (COORD_PHASE.get(quad2d, (phase_label, phase_n))[0] if quad2d else phase_label),
        "macro_regime_quadrant": quad,
        "surprise_tilt": surprise, "asset_leadership": assets,
        "recession_prob_pct": recession_prob,
        "yield_curve_regime": yc_regime, "credit_regime": cr_regime,
        "sector_risk_appetite": sr_app, "regime_composite": _get(rcomp, "meta_regime"),
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
    # Fed net-liquidity decomposition + settlement-fails collateral flicker (Phase 2)
    netliq = _net_liquidity()
    if netliq and isinstance(netliq.get("net_13w_delta_bn"), (int, float)) and netliq["net_13w_delta_bn"] < -40:
        flickers.append(f"Fed net liquidity {netliq['net_13w_delta_bn']}B/13w (draining: TGA/RRP)")
    sf_sig = _get(sfails, "signal")
    if sf_sig and str(sf_sig).upper() in ("ELEVATED", "HIGH", "ACUTE", "STRESS", "STRAINED"):
        flickers.append(f"settlement fails {sf_sig} — {_get(sfails, 'headline', 'collateral scarcity')}")

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
        "net_liquidity": netliq,
        "flickers": flickers,
        "components": {"global_stress": gstress_idx, "crisis_composite": crisis_score,
                       "plumbing_health": plumb_health, "funding_stress": fund_stress,
                       "systemic_stress": sys_score, "crisis_canaries": canary_score,
                       "crisis_defcon": _get(crisis, "defcon_level"),
                       "global_stress_level": _get(gstress, "global_stress_level")},
    }

    # ──────────────────── CROSS-ASSET RISK (RORO) ────────────────────
    roro = num(_get(rrisk, "risk_regime_score"))
    risk = {
        "roro_score": roro,
        "regime": _get(rrisk, "risk_regime"),
        "posture": _get(rrisk, "posture"),
        "components": _get(rrisk, "components"),
        "tells": _get(rrisk, "tells"),
        "read": ("risk-on" if (roro or 0) > 15 else "risk-off" if (roro or 0) < -15 else "neutral"),
        "fomc_context": _get(fomc, "regime_context"),
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
    if roro is not None and roro > 15 and recession_prob is not None and recession_prob >= 50:
        divergences.append(f"RISK-vs-MACRO SPLIT: cross-asset RORO is risk-on ({roro:+.0f}) while the recession-"
                           f"probability composite is {recession_prob}% — markets price benign, the curve/credit/cycle stack does not.")
    if roro is not None and roro < -15 and quad2d == "GOLDILOCKS":
        divergences.append(f"RISK-vs-CYCLE SPLIT: the cycle reads goldilocks but cross-asset RORO is risk-off "
                           f"({roro:+.0f}) — price action disagrees with the macro read.")

    # ───────────────────────── VERDICT ─────────────────────────
    sq_phrase = {"LOW": "liquidity ample", "LOW · WATCH": "liquidity flat/easy with mild draining flickers",
                 "RISING": "liquidity tightening", "ELEVATED": "liquidity stress building",
                 "HIGH": "liquidity squeeze underway", "ACUTE": "acute liquidity squeeze",
                 "UNKNOWN": "liquidity read unavailable"}.get(sq_level, "")
    fav = (f" Coordinates sit in {quad2d.title()} — historically favours " + ", ".join(assets["lead"][:2]).lower() + ".") if (assets and quad2d) else ""
    roro_clause = f", RORO {risk['read']}" if roro is not None else ""
    verdict = (f"{phase_label} ({quad.lower() if quad else 'regime n/a'}, growth {growth_dir}, inflation {infl_dir}"
               f"{', valuation/leverage froth' if len(froth) >= 2 else ''}) — "
               f"{sq_phrase}, squeeze risk {sq_level} ({squeeze}){roro_clause}. "
               + ("Real divergence to watch: " + divergences[0].split(':')[0] + "." if divergences else "")
               + fav)

    falsifier = ("Cycle call flips if the macro-regime quadrant rotates out of STAGFLATION (→ GOLDILOCKS/REFLATION = "
                 "re-acceleration, or → DEFLATION-BUST = downturn). Squeeze-risk escalates if plumbing health breaks "
                 "below ~70, funding stress > 40, or crisis composite leaves DEFCON 4 — none of which is true now.")

    out = {
        "engine": "cycle-clock", "version": VERSION,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "duration_s": round(time.time() - t0, 1),
        "verdict": verdict,
        "cycle": cycle,
        "risk": risk,
        "analogs": {
            "nearest": (_get(analogs_d, "analogs", default=[]) or [])[:3],
            "forward_distribution": _get(analogs_d, "forward_distribution"),
            "directional_call": _get(analogs_d, "directional_call"),
            "directional_description": _get(analogs_d, "directional_description"),
            "unprecedentedness": _get(analogs_d, "unprecedentedness"),
        },
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
