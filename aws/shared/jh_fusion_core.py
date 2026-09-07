"""aws/shared/jh_fusion_core.py -- Fusion Engine v1 (transparent, deterministic, replayable).

Pure functions: (current-state snapshot, registry, universe, reliability,
prior result, now) -> fusion result. No AWS calls, so the same function
replays history (phase 49) and runs in tests.

Per signal (phase 6 / 14):
    effective = score x confidence x freshness x reliability x regime_fit
                x independence x evidence_quality
    weight    = confidence x freshness x reliability x independence x evidence_quality

Per (entity, horizon) (phases 14-16):
    raw_fusion   = sum(effective) / sum(weight)            in [-1, +1]
    conviction   = |raw_fusion| x 100, direction from the sign
    bullish / bearish evidence kept SEPARATELY (never averaged away)
    independent_evidence_count = evidence clusters with material weight
    contradiction (0-100) = 60% two-sided evidence balance
                          + 25% family disagreement + 15% horizon disagreement
    coverage     = weighted families present / expected (stale counts half)
    confidence   = f(independence, coverage, reliability, freshness,
                     regime certainty) x (1 - contradiction/200)

Vetoes (phase 17): signals may carry metadata.veto {type HARD|SOFT, severity,
reason}. HARD -> capital_decision BLOCKED; SOFT -> REDUCED with a size
modifier; CRITICAL registry engines missing/expired -> BLOCKED with the
CAPITAL_DECISION_BLOCKED reason (phase 30).

Regime fit (phase 18/19, v1 heuristic): the market subject's MACRO+RISK
net score is the regime axis; entity-level FLOW/MARKET/CATALYST/FUNDAMENTAL
signals that lean against the regime are discounted up to 40%. Release 4
replaces this table with conditional reliability from
justhodl-regime-conditional-trust (which already exists).
"""
from __future__ import annotations

import math
import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import jhsignal as J

MARKET_SUBJECT = "market:US_EQUITY"
CONTEXT_FAMILIES = ("MACRO", "RISK")          # inherited from the market subject by every entity
REGIME_SENSITIVE = ("FLOW", "MARKET", "CATALYST", "FUNDAMENTAL")
INHERITED_WEIGHT = 0.7                         # market context is real evidence, but not entity-specific
CLUSTER_DIMINISH = 0.6                         # k-th confirmation inside a cluster weighs 1/(1+0.6k)
MATERIAL_WEIGHT = 0.05
CONTRADICTION_CLASSES = ((25, "LOW"), (50, "MODERATE"), (75, "HIGH"), (101, "EXTREME"))
SIZE_FLOOR = 0.2


def _cls(x: float) -> str:
    for th, lab in CONTRADICTION_CLASSES:
        if x < th:
            return lab
    return "EXTREME"


def _mean(xs: List[float], default: float = 0.0) -> float:
    return sum(xs) / len(xs) if xs else default


def _quality(sig: Dict[str, Any]) -> float:
    q = sig.get("quality") or {}
    v = float(q.get("source_reliability", 1)) * float(q.get("data_completeness", 1)) * float(q.get("calculation_quality", 1))
    return v ** (1.0 / 3.0)


# ---------------------------------------------------------------------------
# Regime (phase 18 v1)
# ---------------------------------------------------------------------------
def regime_context(market_signals: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Regime axis from the market subject's MACRO + RISK signals (freshness-weighted mean of score).
    Returns score in [-1, 1], a label, certainty (mean confidence x freshness) and the raw legs."""
    legs, num, den, cert = [], 0.0, 0.0, []
    for s in market_signals:
        if s.get("family") not in CONTEXT_FAMILIES or s.get("freshness") == "EXPIRED":
            continue
        w = float(s["confidence"]) * float(s.get("freshness_weight", 1.0))
        num += float(s["score"]) * w
        den += w
        cert.append(w)
        legs.append({"engine_id": s["engine_id"], "signal_type": s["signal_type"], "score": s["score"], "confidence": s["confidence"], "freshness": s.get("freshness"), "label": (s.get("metadata") or {}).get("meta_regime") or (s.get("metadata") or {}).get("posture") or (s.get("metadata") or {}).get("global_phase")})
    score = (num / den) if den else 0.0
    if not legs:
        label = "UNKNOWN"
    elif score >= 0.4:
        label = "SUPPORTIVE"
    elif score >= 0.15:
        label = "MILDLY_SUPPORTIVE"
    elif score > -0.15:
        label = "MIXED"
    elif score > -0.4:
        label = "MILDLY_HOSTILE"
    else:
        label = "HOSTILE"
    return {"score": round(score, 4), "label": label, "certainty": round(_mean(cert), 4), "n_legs": len(legs), "legs": legs}


def regime_fit(sig_score: float, family: str, regime: Dict[str, Any], *, enabled: bool = True) -> float:
    if not enabled or family not in REGIME_SENSITIVE or regime.get("label") == "UNKNOWN":
        return 1.0
    r = float(regime["score"])
    if abs(r) < 0.2 or sig_score == 0 or (sig_score > 0) == (r > 0):
        return 1.0
    return round(1.0 - 0.4 * min(1.0, abs(r)), 4)


# ---------------------------------------------------------------------------
# Independence (phase 12/13)
# ---------------------------------------------------------------------------
def independence_weights(sigs: List[Dict[str, Any]], clusters: Dict[Tuple[str, str], str],
                         corr: Optional[Dict[str, Dict[str, float]]] = None, *, enabled: bool = True) -> Dict[str, float]:
    """Per signal_id independence weight. Within a cluster the strongest reading keeps weight 1; the k-th
    additional confirmation gets 1/(1+0.6k). When a pairwise engine correlation matrix is supplied, a signal
    whose engine correlates >= 0.5 with a stronger same-sign signal's engine is further multiplied by (1-corr)."""
    out = {s["signal_id"]: 1.0 for s in sigs}
    if not enabled:
        return out
    groups: Dict[str, List[Dict[str, Any]]] = {}
    for s in sigs:
        c = clusters.get((s["engine_id"], s["signal_type"]), "uncategorised")
        groups.setdefault(c, []).append(s)
    for members in groups.values():
        members.sort(key=lambda s: -abs(float(s["score"]) * float(s["confidence"]) * float(s.get("freshness_weight", 1.0))))
        for k, s in enumerate(members):
            out[s["signal_id"]] = 1.0 / (1.0 + CLUSTER_DIMINISH * k)
    if corr:
        ranked = sorted(sigs, key=lambda s: -abs(float(s["score"]) * float(s["confidence"]) * float(s.get("freshness_weight", 1.0))))
        for i, s in enumerate(ranked):
            for t in ranked[:i]:
                if (float(s["score"]) > 0) != (float(t["score"]) > 0):
                    continue
                c = (corr.get(s["engine_id"]) or {}).get(t["engine_id"])
                if c is not None and c >= 0.5:
                    out[s["signal_id"]] = round(out[s["signal_id"]] * (1.0 - min(0.95, float(c))), 4)
    return out


# ---------------------------------------------------------------------------
# Core per (entity, horizon)
# ---------------------------------------------------------------------------
def fuse_horizon(entity_id: str, horizon: str, sigs: List[Dict[str, Any]], *, expected: Dict[str, float],
                 clusters: Dict[Tuple[str, str], str], reliability: Dict[str, float], regime: Dict[str, Any],
                 flags: Dict[str, Any], corr: Optional[Dict[str, Dict[str, float]]] = None) -> Dict[str, Any]:
    fl = flags
    indep = independence_weights(sigs, clusters, corr if fl.get("FUSION_CORRELATION_ADJUST_ENABLED", True) else None,
                                 enabled=fl.get("FUSION_CORRELATION_ADJUST_ENABLED", True))
    rows: List[Dict[str, Any]] = []
    bull = bear = wsum = 0.0
    fam_num: Dict[str, float] = {}; fam_den: Dict[str, float] = {}; fam_n: Dict[str, int] = {}
    clu_num: Dict[str, float] = {}; clu_den: Dict[str, float] = {}
    for s in sigs:
        fam = s["family"]
        fw = float(s.get("freshness_weight", 1.0))
        rel = float(reliability.get(s["engine_id"], 1.0)) if fl.get("FUSION_RELIABILITY_WEIGHTING_ENABLED", True) else 1.0
        rf = regime_fit(float(s["score"]), fam, regime, enabled=fl.get("FUSION_REGIME_WEIGHTING_ENABLED", True))
        iw = indep[s["signal_id"]]
        eq = _quality(s)
        inh = INHERITED_WEIGHT if s.get("inherited") else 1.0
        weight = float(s["confidence"]) * fw * rel * iw * eq * inh
        eff = float(s["score"]) * weight * rf
        cl = clusters.get((s["engine_id"], s["signal_type"]), "uncategorised")
        rows.append({
            "signal_id": s["signal_id"], "engine_id": s["engine_id"], "signal_type": s["signal_type"], "family": fam, "cluster": cl,
            "direction": s["direction"], "score": s["score"], "confidence": s["confidence"], "freshness": s.get("freshness"),
            "freshness_weight": round(fw, 4), "reliability_weight": round(rel, 4), "regime_fit": rf, "independence_weight": round(iw, 4),
            "evidence_quality": round(eq, 4), "inherited": bool(s.get("inherited")), "weight": round(weight, 5), "effective": round(eff, 5),
            "data_asof": s["data_asof"], "engine_version": s.get("engine_version"), "evidence": (s.get("evidence") or [])[:4],
            "invalidation": s.get("invalidation"), "veto": (s.get("metadata") or {}).get("veto"),
        })
        wsum += weight
        if eff > 0:
            bull += eff
        elif eff < 0:
            bear += -eff
        fam_num[fam] = fam_num.get(fam, 0.0) + eff; fam_den[fam] = fam_den.get(fam, 0.0) + weight; fam_n[fam] = fam_n.get(fam, 0) + 1
        clu_num[cl] = clu_num.get(cl, 0.0) + eff; clu_den[cl] = clu_den.get(cl, 0.0) + weight
    raw = (bull - bear) / wsum if wsum > 0 else 0.0
    raw = max(-1.0, min(1.0, raw))
    family_scores = {f: {"score": round(fam_num[f] / fam_den[f], 4) if fam_den[f] else 0.0, "n": fam_n[f], "weight": round(fam_den[f], 4)} for f in fam_num}
    cluster_scores = {c: round(clu_num[c] / clu_den[c], 4) if clu_den[c] else 0.0 for c in clu_num}
    material = {r["cluster"] for r in rows if abs(r["effective"]) >= MATERIAL_WEIGHT}
    independent = len(material)

    # contradiction (phase 16)
    c1 = (2.0 * min(bull, bear) / (bull + bear)) if (bull + bear) > 0 else 0.0
    overall_sign = 1 if raw > 0 else -1 if raw < 0 else 0
    fams = [f for f in family_scores if abs(family_scores[f]["score"]) >= 0.05]
    c2 = (sum(1 for f in fams if overall_sign and (family_scores[f]["score"] > 0) != (overall_sign > 0)) / len(fams)) if fams and overall_sign else 0.0
    contradiction = 100.0 * (0.75 * c1 + 0.25 * c2)   # horizon disagreement is added at entity level

    # coverage (phase 29): stale counts half
    present: Dict[str, float] = {}
    for r in rows:
        v = 1.0 if r["freshness"] == "FRESH" else 0.5
        present[r["family"]] = max(present.get(r["family"], 0.0), v)
    exp_total = sum(expected.values()) or 1.0
    coverage = sum(expected.get(f, 0.0) * v for f, v in present.items()) / exp_total
    missing_families = sorted(f for f, w in expected.items() if w > 0 and f not in present)

    # confidence (phase 28)
    ie = 1.0 - math.exp(-independent / 3.0)
    rel_mean = _mean([r["reliability_weight"] for r in rows], 1.0)
    fresh_mean = _mean([r["freshness_weight"] for r in rows], 0.0)
    comp_mean = _mean([r["evidence_quality"] for r in rows], 0.0)
    reg_cert = float(regime.get("certainty") or 0.0)
    conf_base = 0.30 * ie + 0.25 * coverage + 0.15 * min(1.0, rel_mean) + 0.15 * fresh_mean + 0.10 * comp_mean + 0.05 * reg_cert
    confidence = conf_base * (1.0 - contradiction / 200.0)
    if not rows:
        confidence = 0.0

    # vetoes (phase 17)
    hard, soft = [], []
    if fl.get("FUSION_VETO_ENABLED", True):
        for r in rows:
            v = r.get("veto")
            if not isinstance(v, dict) or not v.get("type"):
                continue
            item = {"type": v["type"], "severity": float(v.get("severity") or 1.0), "reason": v.get("reason"), "engine_id": r["engine_id"],
                    "signal_type": r["signal_type"], "signal_id": r["signal_id"], "data_asof": r["data_asof"], "horizon": horizon,
                    "invalidation": r.get("invalidation"), "expires_at": None}
            (hard if v["type"] == "HARD" else soft).append(item)
    size_mod = 1.0
    for v in soft:
        size_mod *= max(SIZE_FLOOR, 1.0 - 0.5 * min(1.0, v["severity"]))
    capital = "BLOCKED" if hard else "REDUCED" if soft else "OPEN"

    rows.sort(key=lambda r: -abs(r["effective"]))
    supporting = [r for r in rows if r["effective"] > 0][:8]
    opposing = [r for r in rows if r["effective"] < 0][:8]
    attribution = {("%s#%s" % (r["engine_id"], r["signal_type"])): round(r["effective"] / wsum, 5) for r in rows} if wsum else {}
    return {
        "entity_id": entity_id, "horizon": horizon,
        "fusion_score": round(raw, 4), "conviction": int(round(abs(raw) * 100)), "direction": J.direction_from_numeric(raw),
        "confidence": round(max(0.0, min(1.0, confidence)), 4),
        "confidence_components": {"independence": round(ie, 4), "coverage": round(coverage, 4), "reliability": round(rel_mean, 4), "freshness": round(fresh_mean, 4), "completeness": round(comp_mean, 4), "regime_certainty": round(reg_cert, 4), "contradiction_penalty": round(1.0 - contradiction / 200.0, 4)},
        "fusion_coverage": round(coverage, 4), "missing_families": missing_families,
        "raw_signal_count": len(rows), "independent_evidence_count": independent,
        "bullish_evidence": round(bull, 4), "bearish_evidence": round(bear, 4), "evidence_mass": round(wsum, 4),
        "contradiction_score": int(round(contradiction)), "contradiction_class": _cls(contradiction),
        "contradiction_components": {"two_sided": round(c1, 4), "family_disagreement": round(c2, 4)},
        "regime_fit": round(_mean([r["regime_fit"] for r in rows], 1.0), 4),
        "family_scores": family_scores, "cluster_scores": cluster_scores,
        "top_supporting_evidence": supporting, "top_opposing_evidence": opposing,
        "hard_vetoes": hard, "soft_vetoes": soft, "capital_decision": capital, "size_modifier": round(size_mod, 4),
        "attribution": attribution, "signals": rows,
    }


# ---------------------------------------------------------------------------
# Entity + run level
# ---------------------------------------------------------------------------
def fuse_entity(entity_id: str, entity_type: str, own: List[Dict[str, Any]], context: List[Dict[str, Any]], *,
                expected: Dict[str, float], clusters, reliability, regime, flags, corr=None,
                prior: Optional[Dict[str, Any]] = None, now: Optional[datetime] = None) -> Dict[str, Any]:
    now = now or J.utcnow()
    sigs = list(own)
    if entity_id != MARKET_SUBJECT:
        for s in context:
            c = dict(s); c["inherited"] = True
            sigs.append(c)
    horizons: Dict[str, Dict[str, Any]] = {}
    for h in J.HORIZONS:
        hs = [s for s in sigs if s["horizon"] == h]
        if not hs:
            continue
        horizons[h] = fuse_horizon(entity_id, h, hs, expected=expected, clusters=clusters, reliability=reliability, regime=regime, flags=flags, corr=corr)
    # horizon disagreement (the 15% leg of contradiction) applied across horizons with evidence
    signs = [1 if r["fusion_score"] > 0.05 else -1 if r["fusion_score"] < -0.05 else 0 for r in horizons.values()]
    nz = [s for s in signs if s]
    c3 = 0.0 if len(nz) < 2 else (1.0 - abs(sum(nz)) / len(nz))
    for r in horizons.values():
        r["contradiction_components"]["horizon_disagreement"] = round(c3, 4)
        total = min(100.0, r["contradiction_score"] * 0.85 + 15.0 * c3)
        r["contradiction_score"] = int(round(total)); r["contradiction_class"] = _cls(total)
        r["confidence"] = round(max(0.0, min(1.0, r["confidence"] * (1.0 - 0.15 * c3))), 4)
    best = None
    if horizons:
        best = max(horizons.values(), key=lambda r: (r["evidence_mass"] * (0.5 + 0.5 * r["fusion_coverage"]), r["conviction"]))["horizon"]

    # what changed (phase 27) + velocity from the prior document's history (phase 26)
    prior_h = ((prior or {}).get("horizons") or {})
    history = list((prior or {}).get("history") or [])
    for h, r in horizons.items():
        p = prior_h.get(h) or {}
        pa = p.get("attribution") or {}
        deltas = []
        for k in set(list(r["attribution"].keys()) + list(pa.keys())):
            d = r["attribution"].get(k, 0.0) - pa.get(k, 0.0)
            if abs(d) >= 0.005:
                deltas.append({"contributor": k, "delta": round(d, 4), "now": r["attribution"].get(k), "prev": pa.get(k)})
        deltas.sort(key=lambda x: -abs(x["delta"]))
        r["what_changed"] = {"prev_fusion_score": p.get("fusion_score"), "prev_conviction": p.get("conviction"), "prev_generated_at": (prior or {}).get("generated_at"),
                             "delta_fusion_score": (round(r["fusion_score"] - p["fusion_score"], 4) if p.get("fusion_score") is not None else None),
                             "new_engines": sorted(set(r["attribution"]) - set(pa)), "gone_engines": sorted(set(pa) - set(r["attribution"])),
                             "contributions": deltas[:10]}
    history.append({"t": J.iso(now), "h": {h: r["fusion_score"] for h, r in horizons.items()}})
    history = history[-60:]
    velocity = _velocity(history, now)
    return {
        "entity_id": entity_id, "entity_type": entity_type, "ticker": entity_id.split(":", 1)[1] if entity_type in ("equity", "etf", "crypto", "index") else None,
        "best_horizon": best, "horizons": horizons, "history": history, "velocity": velocity,
        "raw_signal_count": len(sigs), "own_signal_count": len(own), "inherited_signal_count": len(sigs) - len(own),
        "updated_at": J.iso(now),
    }


def _velocity(history: List[Dict[str, Any]], now: datetime) -> Dict[str, Any]:
    """1d / 5d / 20d deltas of the best-populated horizon's fusion score, from the entity's own history."""
    out: Dict[str, Any] = {"n_points": len(history), "classification": "INSUFFICIENT_HISTORY"}
    if len(history) < 2:
        return out
    last = history[-1]
    def val(entry, h):
        return (entry.get("h") or {}).get(h)
    hz = max((last.get("h") or {}).keys(), key=lambda h: 1, default=None)
    if not hz:
        return out
    # pick the horizon present in the most history points
    counts = {}
    for e in history:
        for h in (e.get("h") or {}):
            counts[h] = counts.get(h, 0) + 1
    hz = max(counts, key=counts.get)
    cur = val(last, hz)
    def at(days):
        target = now.timestamp() - days * 86400
        cand = [e for e in history[:-1] if val(e, hz) is not None and (J.parse_ts(e["t"]) or now).timestamp() <= target]
        return val(cand[-1], hz) if cand else None
    d1, d5, d20 = at(1), at(5), at(20)
    out.update({"horizon": hz, "current": cur, "delta_1d": (round(cur - d1, 4) if d1 is not None else None),
                "delta_5d": (round(cur - d5, 4) if d5 is not None else None), "delta_20d": (round(cur - d20, 4) if d20 is not None else None)})
    v = out["delta_5d"] if out["delta_5d"] is not None else out["delta_1d"]
    if v is None:
        return out
    a = None
    if out["delta_1d"] is not None and out["delta_5d"] is not None:
        a = out["delta_1d"] - out["delta_5d"] / 5.0
    out["velocity"] = round(v, 4); out["acceleration"] = (round(a, 4) if a is not None else None)
    out["classification"] = ("RAPID_CONVICTION_BUILD" if v >= 0.25 else "CONVICTION_BUILD" if v >= 0.08 else "RAPID_CONVICTION_DECAY" if v <= -0.25 else "CONVICTION_DECAY" if v <= -0.08 else "STABLE")
    return out


def run_fusion(snapshot: Dict[str, Any], *, registry_doc: Dict[str, Any], universe_doc: Dict[str, Any], flags: Dict[str, Any],
               reliability: Optional[Dict[str, float]] = None, corr: Optional[Dict[str, Dict[str, float]]] = None,
               prior: Optional[Dict[str, Any]] = None, now: Optional[datetime] = None, run_id: Optional[str] = None,
               trigger: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """The whole run. Deterministic given its inputs (ids aside)."""
    now = now or J.utcnow()
    run_id = run_id or str(uuid.uuid4())
    reliability = reliability or {}
    clusters = {(e["engine_id"], st): spec["cluster"] for e in registry_doc["engines"] for st, spec in e["signal_types"].items()}
    families = {e["engine_id"]: e["engine_family"] for e in registry_doc["engines"]}
    expected_by_type = universe_doc.get("expected_families") or {}
    entities = snapshot.get("entities") or {}
    market = [s for s in entities.get(MARKET_SUBJECT, []) if s.get("freshness") != "EXPIRED"]
    regime = regime_context(market)
    context = [s for s in market if families.get(s["engine_id"]) in CONTEXT_FAMILIES]

    # critical dependencies (phase 30)
    critical = {e["engine_id"]: e for e in registry_doc["engines"] if e["criticality"] == "CRITICAL" and e["status"] != "disabled"}
    present = {s["engine_id"]: s for s in market}
    dep_failures = []
    for eid, e in critical.items():
        s = present.get(eid)
        if s is None:
            dep_failures.append({"engine_id": eid, "reason": "no live signal from CRITICAL engine %s (%s)" % (eid, e["artifact"]), "last_valid": None})
        elif s.get("freshness") == "STALE":
            dep_failures.append({"engine_id": eid, "reason": "CRITICAL engine %s is STALE" % eid, "last_valid": s.get("data_asof"), "severity": "IMPORTANT"})
    blocked_by_dependency = [d for d in dep_failures if d.get("severity") != "IMPORTANT"]

    disabled_entities = set(flags.get("FUSION_DISABLED_ENTITIES") or [])
    pilot = [e["entity_id"] for e in universe_doc["entities"]]
    scope = [eid for eid in pilot if eid not in disabled_entities]
    if flags.get("FUSION_EMERGENT_ENTITIES_ENABLED"):
        extra = [eid for eid, lst in entities.items() if eid not in scope and len({s["family"] for s in lst}) >= 2 and len(lst) >= 3]
        scope += sorted(extra)[: max(0, int(flags.get("FUSION_MAX_ENTITIES", 400)) - len(scope))]
    prior_entities = ((prior or {}).get("entities") or {})
    results: Dict[str, Any] = {}
    for eid in scope:
        et = eid.split(":", 1)[0]
        own = [s for s in entities.get(eid, []) if s.get("freshness") != "EXPIRED"]
        expected = expected_by_type.get(et) or expected_by_type.get("equity") or {f: 1.0 for f in J.ENGINE_FAMILIES}
        r = fuse_entity(eid, et, own, context, expected=expected, clusters=clusters, reliability=reliability, regime=regime,
                        flags=flags, corr=corr, prior=prior_entities.get(eid), now=now)
        if blocked_by_dependency:
            for h in r["horizons"].values():
                h["capital_decision"] = "BLOCKED"
                h["hard_vetoes"] = h["hard_vetoes"] + [{"type": "HARD", "severity": 1.0, "reason": "CAPITAL_DECISION_BLOCKED: " + d["reason"], "engine_id": d["engine_id"], "signal_type": None, "signal_id": None, "data_asof": d.get("last_valid"), "horizon": h["horizon"], "invalidation": None, "expires_at": None} for d in blocked_by_dependency]
        results[eid] = r

    hard_n = sum(len(h["hard_vetoes"]) for r in results.values() for h in r["horizons"].values())
    soft_n = sum(len(h["soft_vetoes"]) for r in results.values() for h in r["horizons"].values())
    covs = [h["fusion_coverage"] for r in results.values() for h in r["horizons"].values()]
    confs = [h["confidence"] for r in results.values() for h in r["horizons"].values()]
    return {
        "schema_version": "JH-FUSION-1.0", "run_id": run_id, "generated_at": J.iso(now), "shadow_mode": bool(flags.get("FUSION_SHADOW_MODE", True)),
        "snapshot_run_id": snapshot.get("run_id"), "snapshot_generated_at": snapshot.get("generated_at"), "trigger": trigger,
        "regime": regime, "critical_dependencies": {"failures": dep_failures, "capital_blocked": bool(blocked_by_dependency)},
        "engine_versions": {e["engine_id"]: e["version"] for e in registry_doc["engines"]},
        "reliability_basis": {"n_engines_with_reliability": len(reliability), "source": "data/engine-trust.json + signal-scorecard multipliers when present, else 1.0"},
        "flags": {k: v for k, v in flags.items() if k.startswith("FUSION_")},
        "stats": {"n_entities": len(results), "n_horizon_results": len(covs), "hard_vetoes": hard_n, "soft_vetoes": soft_n,
                  "coverage_mean": round(_mean(covs), 4), "confidence_mean": round(_mean(confs), 4),
                  "n_market_context_signals": len(context)},
        "entities": results,
        "methodology": METHODOLOGY,
    }


METHODOLOGY = {
    "effective_strength": "score x confidence x freshness x reliability x regime_fit x independence x evidence_quality (every factor published per signal)",
    "freshness": "exp(-ln2 x age_days / half_life_days); FRESH within the engine TTL, STALE to 2x TTL (counts half toward coverage), EXPIRED after (dropped)",
    "independence": "evidence clusters from the registry; k-th confirmation inside a cluster weighs 1/(1+0.6k); pairwise engine correlation >= 0.5 (signal-orthogonality) discounts the weaker same-sign signal by (1-corr)",
    "fusion_score": "sum(effective)/sum(weight) in [-1,1]; conviction = |fusion| x 100; bullish and bearish evidence reported separately, never averaged away",
    "contradiction": "60% two-sided balance 2min(bull,bear)/(bull+bear) + 25% family disagreement + 15% horizon disagreement -> 0-100, LOW/MODERATE/HIGH/EXTREME",
    "coverage": "weighted families present / expected families for the entity type (config/jh-fusion-universe.json)",
    "confidence": "0.30 independence(1-e^(-clusters/3)) + 0.25 coverage + 0.15 reliability + 0.15 freshness + 0.10 completeness + 0.05 regime certainty, x (1 - contradiction/200)",
    "regime_fit_v1": "market subject MACRO+RISK net score is the regime axis; FLOW/MARKET/CATALYST/FUNDAMENTAL signals leaning against it are discounted up to 40%; Release 4 replaces with regime-conditional trust",
    "vetoes": "HARD (crisis composite >= 80 / DEFCON <= 2, risk-gate SEVERE) blocks capital; SOFT reduces the size modifier by half its severity, floor 0.2; CRITICAL engines missing -> CAPITAL_DECISION_BLOCKED",
    "shadow_mode": "fusion never feeds the existing sizing engine until FUSION_SHADOW_MODE is switched off after the phase-51 comparison window",
}
