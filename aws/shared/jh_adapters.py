"""aws/shared/jh_adapters.py -- signal adapters (phase 3).

    CURRENT ENGINE OUTPUT  ->  SignalAdapter  ->  JHSIGNAL-1.0

Engines keep writing exactly what they write today; an adapter reads that
artifact (already fetched from S3 by the bridge) and emits standardised
signals next to it. Adapters are the only place that knows an engine's
private shape, so a shape change breaks ONE adapter loudly (diagnostics +
metric) rather than silently poisoning fusion.

Rules every adapter obeys:
  * never fabricate: a missing input field -> no signal for that row, and a
    diagnostic that says so (data_completeness < 1 when partial)
  * data_asof is the ENGINE's own timestamp (generated_at / as_of); the S3
    LastModified is the fallback and is flagged as such in metadata
  * scores are normalised to [-1, +1] with the mapping documented in the
    adapter docstring; the raw numbers ride along in `evidence`
  * confidence is derived from measured breadth/coverage in the artifact,
    never a constant pretending to be knowledge; where an engine gives no
    breadth measure the adapter says "prior" in metadata.confidence_basis
  * entity ids come from the pilot universe index when the symbol is known,
    else from what the engine itself declares (asset_class / ETF board),
    else `equity` (the fleet's default asset class)
"""
from __future__ import annotations

import math
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional, Tuple

import jhsignal as J

VETO_HARD = "HARD"
VETO_SOFT = "SOFT"


def _f(x: Any) -> Optional[float]:
    """float or None -- never a silent 0."""
    if x is None or isinstance(x, bool):
        return None
    try:
        v = float(x)
    except (TypeError, ValueError):
        return None
    if math.isnan(v) or math.isinf(v):
        return None
    return v


def _clip(x: float, lo: float = -1.0, hi: float = 1.0) -> float:
    return max(lo, min(hi, x))


def _path(doc: Any, path: str) -> Any:
    cur = doc
    for part in path.split("."):
        if isinstance(cur, dict):
            cur = cur.get(part)
        else:
            return None
    return cur


class AdapterResult:
    def __init__(self, engine_id: str):
        self.engine_id = engine_id
        self.signals: List[Dict[str, Any]] = []
        self.rejected: List[Dict[str, Any]] = []
        self.skipped: int = 0
        self.skip_reasons: Dict[str, int] = {}
        self.diagnostics: List[str] = []
        self.source_status: str = "OK"      # OK | STALE | MISSING | INVALID
        self.data_asof: Optional[str] = None
        self.asof_basis: str = "engine"     # engine | s3_last_modified

    def as_dict(self) -> Dict[str, Any]:
        return {
            "engine_id": self.engine_id, "n_signals": len(self.signals), "n_rejected": len(self.rejected),
            "n_skipped": self.skipped, "source_status": self.source_status, "data_asof": self.data_asof,
            "asof_basis": self.asof_basis, "skip_reasons": dict(sorted(self.skip_reasons.items(), key=lambda kv: -kv[1])[:8]),
            "diagnostics": self.diagnostics[:20],
            "rejected_sample": self.rejected[:5],
        }


class SignalAdapter:
    """Base adapter. Subclasses implement ``rows`` (yield per-entity readings)
    and the normalisation hooks; ``emit_signal`` assembles + validates.
    """
    engine_id = ""
    signal_type = ""
    category = ""
    entity_default = "equity"

    def __init__(self, spec: Dict[str, Any], universe: Dict[str, Dict[str, Any]], *, now: Optional[datetime] = None):
        self.spec = spec
        self.universe = universe
        self.now = now or J.utcnow()
        self.engine_id = spec["engine_id"]

    # -- interface (spec phase 3) -------------------------------------------
    def parse_existing_output(self, doc: Any, meta: Dict[str, Any]) -> AdapterResult:
        res = AdapterResult(self.engine_id)
        if not isinstance(doc, dict) or not doc:
            res.source_status = "MISSING"
            res.diagnostics.append("artifact missing or not an object")
            return res
        asof, basis = self.calculate_freshness(doc, meta)
        if asof is None:
            res.source_status = "INVALID"
            res.diagnostics.append("no usable timestamp (engine paths %s, s3 last_modified %s)" % (self.spec.get("timestamp_paths"), meta.get("last_modified")))
            return res
        res.data_asof, res.asof_basis = J.iso(asof), basis
        age = (self.now - asof).total_seconds()
        if age > float(self.spec["freshness_ttl_seconds"]):
            res.source_status = "STALE"
        try:
            ok = self.validate_source(doc)
        except Exception as exc:
            ok = False
            res.diagnostics.append("validate_source raised %s: %s" % (type(exc).__name__, str(exc)[:120]))
        if not ok:
            res.source_status = "INVALID" if res.source_status == "OK" else res.source_status
            res.diagnostics.append("validate_source failed -- artifact shape is not what the adapter expects")
            return res
        for reading in self.rows(doc):
            if reading is None or "skip" in reading:
                res.skipped += 1
                reason = (reading or {}).get("skip", "no reading")
                res.skip_reasons[reason] = res.skip_reasons.get(reason, 0) + 1
                continue
            try:
                sig = self.emit_signal(reading, asof, basis)
            except J.JHSignalError as exc:
                res.rejected.append({"reading": {k: reading.get(k) for k in ("symbol", "entity_type", "score", "confidence")}, "problems": exc.problems[:4]})
                continue
            except Exception as exc:  # adapter bug -> visible, never silent
                res.rejected.append({"reading": str(reading)[:160], "problems": ["%s: %s" % (type(exc).__name__, str(exc)[:120])]})
                continue
            if sig is None:
                res.skipped += 1
                continue
            res.signals.append(sig)
        return res

    def validate_source(self, doc: Dict[str, Any]) -> bool:
        return True

    def rows(self, doc: Dict[str, Any]) -> Iterable[Dict[str, Any]]:
        raise NotImplementedError

    def normalize_entity(self, symbol: str, hint: Optional[str] = None) -> Tuple[str, str]:
        sym = J.canonical_symbol(symbol)
        spec = self.universe.get(sym) or self.universe.get(str(symbol).upper())
        if spec:
            return spec["entity_type"], spec["symbol"]
        if hint in J.ENTITY_TYPES:
            return hint, sym
        return self.entity_default, sym

    def normalize_direction(self, reading: Dict[str, Any]) -> Optional[float]:
        return _f(reading.get("score"))

    def calculate_confidence(self, reading: Dict[str, Any]) -> Optional[float]:
        return _f(reading.get("confidence"))

    def calculate_freshness(self, doc: Dict[str, Any], meta: Dict[str, Any]) -> Tuple[Optional[datetime], str]:
        for p in self.spec.get("timestamp_paths") or ["generated_at"]:
            dt = J.parse_ts(_path(doc, p))
            if dt is not None:
                return dt, "engine"
        dt = J.parse_ts(meta.get("last_modified"))
        return (dt, "s3_last_modified") if dt is not None else (None, "none")

    def calculate_horizon(self, reading: Dict[str, Any]) -> Tuple[str, float]:
        return reading.get("horizon") or self.spec["default_horizon"], float(reading.get("half_life_days") or self.spec["half_life_days"])

    def emit_signal(self, reading: Dict[str, Any], asof: datetime, basis: str) -> Optional[Dict[str, Any]]:
        score = self.normalize_direction(reading)
        conf = self.calculate_confidence(reading)
        if score is None or conf is None:
            return None
        et, sym = self.normalize_entity(reading["symbol"], reading.get("entity_type"))
        horizon, hl = self.calculate_horizon(reading)
        md = dict(reading.get("metadata") or {})
        md.setdefault("producer", self.spec["producer"])
        md.setdefault("artifact", self.spec["artifact"])
        md["asof_basis"] = basis
        md.setdefault("confidence_basis", reading.get("confidence_basis", "measured"))
        uni = self.universe.get(sym)
        affects = list(reading.get("affects") or ((uni or {}).get("affects") or []))
        return J.make_signal(
            engine_id=self.engine_id, engine_version=str(self.spec["version"]), entity_type=et, symbol=sym,
            category=reading.get("category") or self.category, signal_type=reading.get("signal_type") or self.signal_type,
            score=score, confidence=conf, data_asof=asof, horizon=horizon, half_life_days=hl,
            freshness_ttl_seconds=int(self.spec["freshness_ttl_seconds"]),
            magnitude=reading.get("magnitude"), percentile=reading.get("percentile"), affects=affects,
            dependencies=list(self.spec.get("dependencies") or []),
            invalidation=reading.get("invalidation"), evidence=reading.get("evidence") or [],
            quality=reading.get("quality") or {"source_reliability": 0.9, "data_completeness": 1.0, "calculation_quality": 0.9},
            metadata=md, now=self.now,
        )


def _ev(**kw) -> List[Dict[str, Any]]:
    return [{"field": k, "value": v} for k, v in kw.items() if v is not None]


# ===========================================================================
# MACRO
# ===========================================================================
class RegimeCompositeAdapter(SignalAdapter):
    """composite_score in [-100, 100] -> score/100; confidence = modules with data / total."""
    signal_type, category = "meta_regime", "macro"

    def validate_source(self, doc):
        return _f(doc.get("composite_score")) is not None and isinstance(doc.get("meta_regime"), str)

    def rows(self, doc):
        n_with = _f(doc.get("n_modules_with_data")); n_tot = _f(doc.get("n_modules_total"))
        conf = (n_with / n_tot) if (n_with is not None and n_tot) else None
        yield {
            "symbol": "US_EQUITY", "entity_type": "market", "score": _clip(_f(doc["composite_score"]) / 100.0),
            "confidence": conf, "confidence_basis": "n_modules_with_data/n_modules_total",
            "evidence": _ev(composite_score=doc["composite_score"], meta_regime=doc["meta_regime"], meta_class=doc.get("meta_class"),
                            dimensions=doc.get("dimensions")),
            "metadata": {"meta_regime": doc["meta_regime"], "meta_class": doc.get("meta_class"), "prior_regime": doc.get("prior_regime"),
                         "regime_changed_from_prior": doc.get("regime_changed_from_prior")},
            "invalidation": {"type": "state", "description": "meta_regime flips class (regime_changed_from_prior)"},
        }


class LiquidityCreditAdapter(SignalAdapter):
    """LCE composite is a STRESS score 0-100 (NORMAL 0 / WATCH 25 / ELEVATED 60 / CRISIS 90 ranks,
    worst-of 70% + mean 30%). score = -(stress - 20)/80 -> +0.25 when nothing fires, -1 at 100."""
    signal_type, category = "liquidity_credit_composite", "liquidity"

    def validate_source(self, doc):
        return _f(_path(doc, "composite.score")) is not None

    def rows(self, doc):
        comp = doc["composite"]; stress = _f(comp.get("score"))
        series = doc.get("series") or {}
        n_avail = sum(1 for v in series.values() if isinstance(v, dict) and v.get("available")) if isinstance(series, dict) else None
        n_tot = len(series) if isinstance(series, dict) else None
        conf = (n_avail / n_tot) if (n_avail is not None and n_tot) else None
        yield {
            "symbol": "US_EQUITY", "entity_type": "market", "score": _clip(-(stress - 20.0) / 80.0), "confidence": conf,
            "confidence_basis": "available series / tracked series",
            "evidence": _ev(stress_score=stress, n_firing=comp.get("n_firing"), regime=doc.get("regime"), by_category=comp.get("by_category")),
            "metadata": {"regime": doc.get("regime"), "veto": ({"type": VETO_SOFT, "severity": round(min(1.0, (stress - 60) / 40 + 0.5), 2), "reason": "liquidity/credit stress %.0f (ELEVATED+)" % stress} if stress >= 60 else None)},
            "invalidation": {"type": "level", "description": "composite stress falls back below 25 (WATCH)"},
        }


class GlobalCycleAdapter(SignalAdapter):
    """global_phase base (RECOVERY +0.7, EXPANSION +0.5, PEAKING -0.3, CONTRACTION -0.7) shifted by the
    6-month downturn probability: score = base - 0.8*(p - 0.25). Confidence = fresh countries / mapped."""
    signal_type, category = "global_cycle_phase", "cycle"
    BASE = {"GLOBAL_RECOVERY": 0.7, "GLOBAL_EXPANSION": 0.5, "GLOBAL_PEAKING": -0.3, "GLOBAL_CONTRACTION": -0.7}

    def validate_source(self, doc):
        return isinstance(_path(doc, "aggregate.global_phase"), str)

    def rows(self, doc):
        agg = doc["aggregate"]; phase = agg["global_phase"]
        base = self.BASE.get(phase)
        if base is None:
            return
        p = _f(doc.get("downturn_probability_6m"))
        if p is None:
            p = _f(_path(doc, "composite.downturn_probability_6m.probability_now"))
        score = base if p is None else _clip(base - 0.8 * (p - 0.25))
        fresh = _f(doc.get("fresh_count")); n = _f(doc.get("n_countries"))
        conf = (fresh / n) if (fresh is not None and n) else None
        yield {
            "symbol": "US_EQUITY", "entity_type": "market", "score": score, "confidence": conf, "confidence_basis": "fresh_count/n_countries",
            "evidence": _ev(global_phase=phase, downturn_probability_6m=p, global_avg_cli=agg.get("global_avg_cli"), cli_level=agg.get("cli_level")),
            "metadata": {"global_phase": phase},
            "invalidation": {"type": "state", "description": "global phase changes or downturn probability crosses 0.5"},
        }


# ===========================================================================
# RISK
# ===========================================================================
class RiskGateAdapter(SignalAdapter):
    """posture RISK_ON/NEUTRAL/RISK_OFF/SEVERE (sizing 1.0/.75/.45/.20) and composite in [-10, 10]:
    score = 0.5*posture_score + 0.5*composite/10. SEVERE = HARD veto (never touch stocks when plumbing is
    shaky -- brain rule), RISK_OFF = SOFT veto carrying the sizing multiplier. Confidence = legs with data."""
    signal_type, category = "risk_posture", "risk"
    POSTURE = {"RISK_ON": 0.6, "NEUTRAL": 0.1, "RISK_OFF": -0.55, "SEVERE": -0.95}

    def validate_source(self, doc):
        return isinstance(doc.get("posture"), str) and _f(doc.get("sizing_multiplier")) is not None

    def rows(self, doc):
        post = doc["posture"]; comp = _f(doc.get("composite"))
        ps = self.POSTURE.get(post)
        if ps is None:
            return
        score = _clip(0.5 * ps + (0.5 * comp / 10.0 if comp is not None else 0.0))
        legs = doc.get("legs")
        if isinstance(legs, dict):
            n_tot = len(legs); n_ok = sum(1 for v in legs.values() if isinstance(v, dict) and v.get("value") is not None or (not isinstance(v, dict) and v is not None))
        elif isinstance(legs, list):
            n_tot = len(legs); n_ok = sum(1 for v in legs if isinstance(v, dict) and (v.get("value") is not None or v.get("score") is not None))
        else:
            n_tot = n_ok = 0
        conf = (n_ok / n_tot) if n_tot else None
        veto = None
        if post == "SEVERE":
            veto = {"type": VETO_HARD, "severity": 1.0, "reason": "brain risk-gate posture SEVERE (sizing 0.20)"}
        elif post == "RISK_OFF":
            veto = {"type": VETO_SOFT, "severity": 0.55, "reason": "brain risk-gate posture RISK_OFF (sizing 0.45)"}
        yield {
            "symbol": "US_EQUITY", "entity_type": "market", "score": score, "confidence": conf, "confidence_basis": "legs with a value / legs",
            "evidence": _ev(posture=post, composite=comp, sizing_multiplier=doc.get("sizing_multiplier")),
            "metadata": {"posture": post, "sizing_multiplier": doc.get("sizing_multiplier"), "veto": veto},
            "invalidation": {"type": "state", "description": "posture flips (recent_timeline)"},
        }


class CrisisCompositeAdapter(SignalAdapter):
    """master_crisis_score 0-100: score = -(m - 30)/70 (0 at 30, -1 at 100, +0.43 at 0). HARD veto at >= 80
    (existing fusion-policy hard_veto threshold) or DEFCON <= 2; SOFT veto at >= 65 (risk_off threshold)."""
    signal_type, category = "systemic_stress", "risk"

    def validate_source(self, doc):
        return _f(doc.get("master_crisis_score")) is not None

    def rows(self, doc):
        m = _f(doc["master_crisis_score"]); lvl = doc.get("defcon_level")
        comps = doc.get("components") or []
        n_tot = len(comps) if isinstance(comps, list) else 0
        n_ok = sum(1 for c in comps if isinstance(c, dict) and c.get("available", c.get("score") is not None)) if n_tot else 0
        conf = (n_ok / n_tot) if n_tot else _f(doc.get("components_available"))
        if conf is not None and conf > 1:  # components_available may be a count
            conf = conf / n_tot if n_tot else None
        veto = None
        if m >= 80 or (isinstance(lvl, (int, float)) and lvl <= 2):
            veto = {"type": VETO_HARD, "severity": 1.0, "reason": "crisis composite %.0f / DEFCON %s" % (m, lvl)}
        elif m >= 65:
            veto = {"type": VETO_SOFT, "severity": round(0.5 + (m - 65) / 30, 2), "reason": "crisis composite %.0f elevated" % m}
        yield {
            "symbol": "US_EQUITY", "entity_type": "market", "score": _clip(-(m - 30.0) / 70.0), "confidence": conf,
            "confidence_basis": "components available / components", "horizon": "TACTICAL",
            "evidence": _ev(master_crisis_score=m, defcon_level=lvl, defcon_name=doc.get("defcon_name"), trend=doc.get("trend")),
            "metadata": {"defcon_level": lvl, "veto": veto},
            "invalidation": {"type": "level", "description": "master crisis score back under 50 and DEFCON >= 4"},
        }


class TailRiskAdapter(SignalAdapter):
    """indices[] rows (SPY/QQQ/IWM): tail_stress 0-100 -> score = -(ts - 30)/70; SOFT veto at >= 70.
    Confidence from how many of the four tail components were measured (p_drop_10, skew, RR, skew index)."""
    signal_type, category = "crash_probability", "risk"

    def validate_source(self, doc):
        return isinstance(doc.get("indices"), list)

    def rows(self, doc):
        for r in doc["indices"]:
            if not isinstance(r, dict):
                yield {"skip": "not isinstance(r, dict)"}
                continue
            ts = _f(r.get("tail_stress")); sym = r.get("ticker")
            if ts is None or not sym:
                yield {"skip": "ts is None or not sym"}
                continue
            parts = [r.get("p_drop_10"), r.get("put_skew_slope"), r.get("risk_reversal_25"), r.get("skew_index")]
            n_ok = sum(1 for x in parts if x is not None)
            yield {
                "symbol": sym, "entity_type": "etf", "score": _clip(-(ts - 30.0) / 70.0), "confidence": n_ok / 4.0,
                "confidence_basis": "measured tail components / 4", "horizon": "TACTICAL", "magnitude": _f(r.get("p_drop_10")),
                "evidence": _ev(tail_stress=ts, p_drop_10=r.get("p_drop_10"), p_drop_20=r.get("p_drop_20"), skew_index=r.get("skew_index"), spot=r.get("spot")),
                "metadata": {"veto": ({"type": VETO_SOFT, "severity": round(0.4 + (ts - 70) / 60, 2), "reason": "%s tail stress %.0f" % (sym, ts)} if ts >= 70 else None),
                             "tail_regime": doc.get("tail_regime")},
                "invalidation": {"type": "level", "description": "tail stress falls back under 50"},
            }


# ===========================================================================
# FLOW
# ===========================================================================
class InsiderRadarAdapter(SignalAdapter):
    """clusters[] (>=2 insiders buying): score = 0.45 + 0.1*(n_insiders-2) + value term (+0.15 >= $1M,
    +0.25 >= $5M), capped 1.0. Confidence = 0.55 + 0.1 per extra buy (cap 0.9)."""
    signal_type, category = "insider_cluster_buy", "smart_money"

    def validate_source(self, doc):
        return isinstance(doc.get("clusters"), list)

    def rows(self, doc):
        for c in doc["clusters"]:
            if not isinstance(c, dict):
                yield {"skip": "not isinstance(c, dict)"}
                continue
            sym = c.get("ticker"); n_ins = _f(c.get("n_insiders")); tv = _f(c.get("total_value"))
            if not sym or n_ins is None or tv is None:
                yield {"skip": "not sym or n_ins is None or tv is None"}
                continue
            score = 0.45 + 0.1 * max(0.0, n_ins - 2) + (0.25 if tv >= 5e6 else 0.15 if tv >= 1e6 else 0.0)
            n_buys = _f(c.get("n_buys")) or n_ins
            conf = min(0.9, 0.55 + 0.1 * max(0.0, n_buys - 2))
            yield {
                "symbol": sym, "score": _clip(score), "confidence": conf, "confidence_basis": "0.55 + 0.1 per buy beyond two",
                "magnitude": tv, "evidence": _ev(n_insiders=n_ins, n_buys=n_buys, total_value=tv, span_days=c.get("span_days"), ret_60d_pct=c.get("ret_60d_pct"), names=c.get("names")),
                "metadata": {"cluster_first": c.get("first"), "cluster_last": c.get("last")},
                "invalidation": {"type": "text", "description": "insider selling cluster or a material negative filing"},
            }


class Institutional13FAdapter(SignalAdapter):
    """t[ticker] {wn: whale net USD, na: net action score, fb/fs: funds buying/selling}: score = sign(wn) x
    value tier (>= $500M 1.0, $100M 0.7, $20M 0.45, $5M 0.25, else 0.1). Confidence = 0.4 + 0.1 per fund on
    the winning side (cap 0.8). 13F is ~45 days lagged -> INTERMEDIATE horizon, 45d half-life."""
    signal_type, category = "whale_net_flow", "institutional_flow"

    def validate_source(self, doc):
        return isinstance(doc.get("t"), dict)

    def rows(self, doc):
        for sym, r in doc["t"].items():
            if not isinstance(r, dict):
                yield {"skip": "not isinstance(r, dict)"}
                continue
            wn = _f(r.get("wn"))
            if wn is None or wn == 0:
                yield {"skip": "wn is None or wn == 0"}
                continue
            a = abs(wn)
            tier = 1.0 if a >= 5e8 else 0.7 if a >= 1e8 else 0.45 if a >= 2e7 else 0.25 if a >= 5e6 else 0.1
            side = r.get("fb") if wn > 0 else r.get("fs")
            n_side = len(side) if isinstance(side, list) else 0
            yield {
                "symbol": sym, "score": _clip(math.copysign(tier, wn)), "confidence": min(0.8, 0.4 + 0.1 * n_side),
                "confidence_basis": "0.4 + 0.1 per whale fund on the net side", "magnitude": wn,
                "evidence": _ev(whale_net_usd=wn, whale_bought_usd=r.get("wb"), whale_sold_usd=r.get("ws"), n_funds_holding=r.get("nf"), net_action_score=r.get("na"), funds_buying=r.get("fb"), funds_selling=r.get("fs")),
                "invalidation": {"type": "text", "description": "next 13F delta flips whale net flow"},
            }


class EtfFlowsAdapter(SignalAdapter):
    """by_etf[ticker] {dvol_z_score, return_1d_pct, flow_signal}: score = clip(z/3) with sign from the flow
    classification (HEAVY_INFLOW/ROTATION_IN +, HEAVY_OUTFLOW/ROTATION_OUT -, else sign of 1d return).
    Confidence = 0.45 + 0.15*min(|z|, 3)/3 + 0.2 if classified."""
    signal_type, category = "etf_flow", "institutional_flow"
    entity_default = "etf"

    def validate_source(self, doc):
        return isinstance(doc.get("by_etf"), dict)

    def rows(self, doc):
        for sym, r in doc["by_etf"].items():
            if not isinstance(r, dict):
                yield {"skip": "not isinstance(r, dict)"}
                continue
            z = _f(r.get("dvol_z_score"))
            if z is None:
                yield {"skip": "z is None"}
                continue
            fs = str(r.get("flow_signal") or "")
            r1 = _f(r.get("return_1d_pct"))
            if fs in ("HEAVY_INFLOW", "ROTATION_IN"):
                sgn = 1.0
            elif fs in ("HEAVY_OUTFLOW", "ROTATION_OUT"):
                sgn = -1.0
            elif r1 is not None:
                sgn = 1.0 if r1 > 0 else -1.0 if r1 < 0 else 0.0
            else:
                continue
            classified = fs not in ("", "NORMAL", "NONE", "UNUSUAL_VOL")
            yield {
                "symbol": sym, "entity_type": "etf", "score": _clip(sgn * min(1.0, abs(z) / 3.0)),
                "confidence": min(0.9, 0.45 + 0.15 * min(abs(z), 3.0) / 3.0 + (0.2 if classified else 0.0)),
                "confidence_basis": "z-score size + classification", "magnitude": _f(r.get("today_dollar_vol_b")), "percentile": None,
                "evidence": _ev(dvol_z_score=z, flow_signal=fs or None, return_1d_pct=r1, aum_b=r.get("aum_b")),
                "invalidation": {"type": "state", "description": "flow_signal reverses"},
            }


class DarkPoolAdapter(SignalAdapter):
    """board[] rows: state ACCUMULATION (+) / DISTRIBUTION (-), score 0-100 -> +/- score/100; NEUTRAL skipped.
    Confidence = 0.5 + 0.3*(dark_accel measured) + 0.1 (weekly FINRA data, one print)."""
    signal_type, category = "dark_pool_accumulation", "institutional_flow"

    def validate_source(self, doc):
        return isinstance(doc.get("board"), list)

    def rows(self, doc):
        for r in doc["board"]:
            if not isinstance(r, dict):
                yield {"skip": "not isinstance(r, dict)"}
                continue
            st = r.get("state"); sc = _f(r.get("score")); sym = r.get("ticker")
            if not sym or sc is None or st not in ("ACCUMULATION", "DISTRIBUTION"):
                yield {"skip": "not sym or sc is None or st not in ('ACCUMULATION', 'DISTRIBUTION')"}
                continue
            sgn = 1.0 if st == "ACCUMULATION" else -1.0
            yield {
                "symbol": sym, "score": _clip(sgn * sc / 100.0), "confidence": 0.5 + (0.3 if r.get("dark_accel") is not None else 0.0) + 0.1,
                "confidence_basis": "0.6 + 0.3 when acceleration is measured", "magnitude": _f(r.get("dark_pool_pct")),
                "evidence": _ev(state=st, score=sc, dark_pool_pct=r.get("dark_pool_pct"), offex_pct=r.get("offex_pct"), dark_accel=r.get("dark_accel"), week_return_pct=r.get("week_return_pct"), venue=r.get("venue_fingerprint")),
                "invalidation": {"type": "state", "description": "ATS state flips to %s" % ("DISTRIBUTION" if sgn > 0 else "ACCUMULATION")},
            }


class ShortInterestAdapter(SignalAdapter):
    """by_ticker[sym] {signal, score, latest_short_pct, days_to_cover, si_change_pct}: COVERING and
    SI_COLLAPSE_FLAT_PRICE (+), SQUEEZE_RISK (+, smaller), DISTRIBUTION / CROWDED_SHORT (-); NEUTRAL skipped.
    Magnitude scaled by the engine's own 0-100 score. Confidence = 0.5 + 0.2 per extra measured input (cap 0.85)."""
    signal_type, category = "short_positioning", "positioning"
    SIGN = {"SI_COLLAPSE_FLAT_PRICE": 1.0, "COVERING": 0.75, "SQUEEZE_RISK": 0.5, "DISTRIBUTION": -0.75, "CROWDED_SHORT": -0.5}

    def validate_source(self, doc):
        return isinstance(doc.get("by_ticker"), dict)

    def rows(self, doc):
        for sym, r in doc["by_ticker"].items():
            if not isinstance(r, dict):
                yield {"skip": "not isinstance(r, dict)"}
                continue
            s = self.SIGN.get(str(r.get("signal") or ""))
            if s is None:
                yield {"skip": "s is None"}
                continue
            sc = _f(r.get("score"))
            strength = (sc / 100.0) if sc is not None else 0.5
            n_meas = sum(1 for k in ("latest_short_pct", "days_to_cover", "si_change_pct", "trend_pct") if r.get(k) is not None)
            yield {
                "symbol": sym, "score": _clip(s * max(0.2, strength)), "confidence": min(0.85, 0.45 + 0.1 * n_meas),
                "confidence_basis": "0.45 + 0.1 per measured input", "magnitude": _f(r.get("latest_short_pct")),
                "evidence": _ev(signal=r.get("signal"), score=sc, latest_short_pct=r.get("latest_short_pct"), days_to_cover=r.get("days_to_cover"), si_change_pct=r.get("si_change_pct"), trend_pct=r.get("trend_pct")),
                "invalidation": {"type": "state", "description": "short positioning signal flips sign"},
            }


# ===========================================================================
# FUNDAMENTAL
# ===========================================================================
class EstimateRevisionsAdapter(SignalAdapter):
    """rows in estimate_strength_leaders + upward/downward_revisions: score = 0.5*clip(eps_rev_pct/10)
    + 0.5*(estimate_strength-50)/50. Confidence = 0.4 + 0.05*n_analysts + 0.15 when revenue confirms (cap 0.95)."""
    signal_type, category = "eps_revision", "fundamental_growth"

    def validate_source(self, doc):
        return any(isinstance(doc.get(k), list) for k in ("estimate_strength_leaders", "upward_revisions", "downward_revisions"))

    def rows(self, doc):
        seen = set()
        for k in ("estimate_strength_leaders", "upward_revisions", "downward_revisions"):
            for r in doc.get(k) or []:
                if not isinstance(r, dict):
                    yield {"skip": "not isinstance(r, dict)"}
                    continue
                sym = r.get("ticker")
                if not sym or sym in seen:
                    yield {"skip": "not sym or sym in seen"}
                    continue
                rev = _f(r.get("eps_rev_pct")); strength = _f(r.get("estimate_strength"))
                if rev is None and strength is None:
                    yield {"skip": "rev is None and strength is None"}
                    continue
                seen.add(sym)
                parts = []
                if rev is not None:
                    parts.append(_clip(rev / 10.0))
                if strength is not None:
                    parts.append(_clip((strength - 50.0) / 50.0))
                score = sum(parts) / len(parts)
                n_an = _f(r.get("n_analysts")) or 0.0
                conf = min(0.95, 0.4 + 0.05 * n_an + (0.15 if r.get("revenue_confirms") else 0.0))
                d2e = _f(r.get("days_to_earnings"))
                yield {
                    "symbol": sym, "score": score, "confidence": conf, "confidence_basis": "0.4 + 0.05/analyst + 0.15 revenue confirmation",
                    "magnitude": rev, "percentile": strength,
                    "evidence": _ev(eps_rev_pct=rev, estimate_strength=strength, fwd_eps_growth_pct=r.get("fwd_eps_growth_pct"), rev_rev_pct=r.get("rev_rev_pct"), revenue_confirms=r.get("revenue_confirms"), n_analysts=n_an, dispersion_pct=r.get("dispersion_pct"), earnings_date=r.get("earnings_date"), days_to_earnings=d2e),
                    "metadata": {"earnings_date": r.get("earnings_date"), "days_to_earnings": d2e, "binary_event_soon": bool(d2e is not None and 0 <= d2e <= 10)},
                    "invalidation": {"type": "text", "description": "EPS revisions turn %s" % ("negative" if score > 0 else "positive")},
                }


# ===========================================================================
# MARKET
# ===========================================================================
class MomentumLeadersAdapter(SignalAdapter):
    """all_scored[] dossiers: momentum_score 0-100 (universe percentile composite) -> score=(m-50)/50,
    percentile = m. Confidence = 0.6 + 0.1 per measured leg beyond three (20d, 60d, RS, 52w, volume; cap 0.85)."""
    signal_type, category = "momentum_composite", "price_confirmation"

    def validate_source(self, doc):
        return isinstance(doc.get("all_scored"), list)

    def rows(self, doc):
        for r in doc["all_scored"]:
            if not isinstance(r, dict):
                yield {"skip": "not isinstance(r, dict)"}
                continue
            m = _f(r.get("momentum_score")); sym = r.get("ticker")
            if m is None or not sym:
                yield {"skip": "m is None or not sym"}
                continue
            legs = sum(1 for k in ("perf_20d_pct", "perf_60d_pct", "rs_spy_20d_pct", "wk52_proximity", "volume_surge") if r.get(k) is not None)
            yield {
                "symbol": sym, "score": _clip((m - 50.0) / 50.0), "confidence": min(0.85, 0.6 + 0.1 * max(0, legs - 3)),
                "confidence_basis": "0.6 + 0.1 per measured leg beyond three", "percentile": m, "magnitude": _f(r.get("rs_spy_20d_pct")),
                "evidence": _ev(momentum_score=m, rank=r.get("rank"), perf_20d_pct=r.get("perf_20d_pct"), perf_60d_pct=r.get("perf_60d_pct"), rs_spy_20d_pct=r.get("rs_spy_20d_pct"), wk52_proximity=r.get("wk52_proximity"), volume_surge=r.get("volume_surge"), tags=r.get("tags")),
                "invalidation": {"type": "level", "description": "momentum composite drops under 50 or RS vs SPY turns negative"},
            }


class FortressAdapter(SignalAdapter):
    """board[] (stocks) + etfs[]: tier base (FORTRESS_COIL 0.6, COILED 0.4, ACCUMULATING 0.2, WATCH 0.05,
    else 0) + 0.3*(composite-50)/50. Confidence = the engine's own conviction/100 (geometric mean of the six
    gate pillars) shrunk by resilience_confidence when published."""
    signal_type, category = "fortress_accumulation", "price_confirmation"
    TIER = {"FORTRESS_COIL": 0.6, "COILED": 0.4, "ACCUMULATING": 0.2, "WATCH": 0.05}

    def validate_source(self, doc):
        return isinstance(doc.get("board"), list)

    def rows(self, doc):
        for kind, key in (("equity", "board"), ("etf", "etfs")):
            for r in doc.get(key) or []:
                if not isinstance(r, dict):
                    yield {"skip": "not isinstance(r, dict)"}
                    continue
                sym = r.get("ticker"); tier = r.get("tier"); comp = _f(r.get("composite"))
                if not sym or comp is None or tier not in self.TIER:
                    yield {"skip": "not sym or comp is None or tier not in self.TIER"}
                    continue
                conv = _f(r.get("conviction"))
                if conv is None:
                    yield {"skip": "conv is None"}
                    continue
                rc = _f(r.get("resilience_confidence"))
                conf = _clip(conv / 100.0 * (0.5 + 0.5 * rc if rc is not None else 1.0), 0.0, 1.0)
                yield {
                    "symbol": sym, "entity_type": kind, "score": _clip(self.TIER[tier] + 0.3 * (comp - 50.0) / 50.0), "confidence": conf,
                    "confidence_basis": "fortress conviction (geometric mean of six gate pillars) x resilience_confidence", "percentile": comp,
                    "magnitude": _f(r.get("asymmetry")),
                    "evidence": _ev(tier=tier, composite=comp, conviction=conv, asymmetry=r.get("asymmetry"), gates_passed=r.get("gates_passed"), coil_state=r.get("coil_state"), sector=r.get("sector"), industry=r.get("industry"), cap_bucket=r.get("cap_bucket")),
                    "metadata": {"tier": tier, "sector": r.get("sector"), "industry": r.get("industry"), "watch_trigger": r.get("watch_trigger")},
                    "invalidation": {"type": "text", "description": "knife guard trips (-40% 3m) or the coil releases downward (tier drops below WATCH)"},
                }


class KatlinAdapter(SignalAdapter):
    """picks[]: tier base (KATLIN_PRIME 0.8, READY 0.6, BASING 0.35, CRASH_BARBELL 0.3) + 0.2*(composite-50)/50
    -> katlin_bottom_setup; a second CATALYST-family signal (katlin_catalyst) when the pick carries named
    catalysts. Confidence = conviction/100 (else composite-based, flagged). asset_class drives the entity type."""
    signal_type, category = "katlin_bottom_setup", "price_confirmation"
    TIER = {"KATLIN_PRIME": 0.8, "READY": 0.6, "BASING": 0.35, "CRASH_BARBELL": 0.3}
    CLASS = {"stock": "equity", "equity": "equity", "etf": "etf", "crypto": "crypto"}

    def validate_source(self, doc):
        return isinstance(doc.get("picks"), list)

    def rows(self, doc):
        for r in doc["picks"]:
            if not isinstance(r, dict):
                yield {"skip": "not isinstance(r, dict)"}
                continue
            sym = r.get("ticker"); tier = r.get("tier"); comp = _f(r.get("composite"))
            if not sym or comp is None or tier not in self.TIER:
                yield {"skip": "not sym or comp is None or tier not in self.TIER"}
                continue
            conv = _f(r.get("conviction"))
            conf, basis = (conv / 100.0, "katlin conviction/100") if conv is not None else (_clip(comp / 100.0, 0.0, 1.0) * 0.8, "composite/100 x 0.8 (no conviction field)")
            et = self.CLASS.get(str(r.get("asset_class") or "").lower())
            plan = r.get("plan") or {}
            base_md = {"tier": tier, "asset_class": r.get("asset_class"), "sector": r.get("sector"), "industry": r.get("industry"),
                       "stop": plan.get("stop"), "target_1": plan.get("target_1"), "sniper": (r.get("sniper") or {}).get("state") if isinstance(r.get("sniper"), dict) else r.get("sniper")}
            yield {
                "symbol": sym, "entity_type": et, "score": _clip(self.TIER[tier] + 0.2 * (comp - 50.0) / 50.0), "confidence": _clip(conf, 0.0, 1.0),
                "confidence_basis": basis, "percentile": comp, "magnitude": _f(r.get("learned_excess_126s_pct")),
                "evidence": _ev(tier=tier, composite=comp, conviction=conv, structure_state=r.get("structure_state"), dist_sma200_pct=r.get("dist_sma200_pct"), rsi_w=r.get("rsi_w"), pillars=r.get("pillars")),
                "metadata": base_md,
                "invalidation": {"type": "level", "description": "close below the structural stop %s" % plan.get("stop") if plan.get("stop") else "structure breaks (lower low under the confirmed bottom)"},
            }
            cats = r.get("catalysts")
            named = [c for c in cats if isinstance(c, dict) and c.get("name") and str(c.get("kind") or c.get("class") or "").lower() != "calendar"] if isinstance(cats, list) else []
            if named:
                yield {
                    "symbol": sym, "entity_type": et, "signal_type": "katlin_catalyst", "category": "catalyst",
                    "score": _clip(0.4 + 0.15 * len(named)), "confidence": min(0.8, 0.5 + 0.1 * len(named)), "confidence_basis": "0.5 + 0.1 per named non-calendar catalyst",
                    "evidence": [{"field": "catalyst", "value": (c.get("name") or "")[:140], "source": c.get("source")} for c in named[:6]],
                    "metadata": {"tier": tier, "n_named_catalysts": len(named)},
                    "invalidation": {"type": "text", "description": "catalyst resolves or is withdrawn"},
                }


# ===========================================================================
# CATALYST
# ===========================================================================
class CatalystAdapter(SignalAdapter):
    """by_ticker[t] {catalysts[], score}: score = clip(weight sum / 5), confidence = 0.45 + 0.1 per catalyst (cap 0.8)."""
    signal_type, category = "named_catalyst", "catalyst"

    def validate_source(self, doc):
        return isinstance(doc.get("by_ticker"), dict)

    def rows(self, doc):
        for sym, e in doc["by_ticker"].items():
            if not isinstance(e, dict):
                yield {"skip": "not isinstance(e, dict)"}
                continue
            sc = _f(e.get("score")); cats = e.get("catalysts")
            if sc is None or not isinstance(cats, list) or not cats:
                yield {"skip": "sc is None or not isinstance(cats, list) or not cats"}
                continue
            yield {
                "symbol": sym, "score": _clip(sc / 5.0), "confidence": min(0.8, 0.45 + 0.1 * len(cats)), "confidence_basis": "0.45 + 0.1 per classified catalyst",
                "magnitude": sc,
                "evidence": [{"field": "catalyst", "value": "%s: %s" % (c.get("class"), (c.get("evidence") or "")[:120]), "source": c.get("src")} for c in cats[:6] if isinstance(c, dict)],
                "metadata": {"classes": sorted({str(c.get("class")) for c in cats if isinstance(c, dict)})},
                "invalidation": {"type": "time", "description": "catalyst window lapses (half-life 10d)"},
            }


class DealerGexAdapter(SignalAdapter):
    """underlyings[sym] {regime, total_dealer_gex_billions, pcr_oi, spot_above_flip}: STRONG_POSITIVE +0.35,
    POSITIVE +0.2, NEAR_FLIP 0, NEGATIVE -0.35, STRONG_NEGATIVE -0.6 (+0.1 contrarian when pcr_oi > 1.2).
    Negative gamma = SOFT veto (short gamma amplifies moves). Confidence = 0.55 + 0.3*min(1, contracts/5000)."""
    signal_type, category = "gamma_regime", "options_positioning"
    REG = {"STRONG_POSITIVE_GAMMA": 0.35, "POSITIVE_GAMMA": 0.2, "NEAR_FLIP": 0.0, "NEGATIVE_GAMMA": -0.35, "STRONG_NEGATIVE_GAMMA": -0.6}

    def validate_source(self, doc):
        return isinstance(doc.get("underlyings"), dict)

    def rows(self, doc):
        for sym, r in doc["underlyings"].items():
            if not isinstance(r, dict) or r.get("err"):
                yield {"skip": "not isinstance(r, dict) or r.get('err')"}
                continue
            base = self.REG.get(str(r.get("regime") or ""))
            gex = _f(r.get("total_dealer_gex_billions"))
            if base is None or gex is None:
                yield {"skip": "base is None or gex is None"}
                continue
            pcr = _f(r.get("pcr_oi"))
            score = base + (0.1 if (pcr is not None and pcr > 1.2) else 0.0)
            n = _f(r.get("n_contracts_modeled")) or 0.0
            neg = base < 0
            yield {
                "symbol": sym, "score": _clip(score), "confidence": 0.55 + 0.3 * min(1.0, n / 5000.0), "confidence_basis": "0.55 + 0.3 x contracts modelled / 5000",
                "horizon": "TACTICAL", "magnitude": gex,
                "evidence": _ev(regime=r.get("regime"), total_dealer_gex_billions=gex, pcr_oi=pcr, zero_gamma_flip_level=r.get("zero_gamma_flip_level"), spot=r.get("spot"), spot_above_flip=r.get("spot_above_flip"), trading_bias=r.get("trading_bias")),
                "metadata": {"veto": ({"type": VETO_SOFT, "severity": 0.35 if base > -0.5 else 0.5, "reason": "%s dealers short gamma (%s)" % (sym, r.get("regime"))} if neg else None)},
                "invalidation": {"type": "level", "description": "spot crosses the zero-gamma flip level %s" % r.get("zero_gamma_flip_level")},
            }


# ---------------------------------------------------------------------------
ADAPTERS = {
    "regime_composite": RegimeCompositeAdapter,
    "liquidity_credit": LiquidityCreditAdapter,
    "global_cycle": GlobalCycleAdapter,
    "risk_gate": RiskGateAdapter,
    "crisis_composite": CrisisCompositeAdapter,
    "tail_risk": TailRiskAdapter,
    "insider_radar": InsiderRadarAdapter,
    "institutional_13f": Institutional13FAdapter,
    "etf_flows": EtfFlowsAdapter,
    "dark_pool": DarkPoolAdapter,
    "short_interest": ShortInterestAdapter,
    "estimate_revisions": EstimateRevisionsAdapter,
    "momentum_leaders": MomentumLeadersAdapter,
    "fortress": FortressAdapter,
    "katlin": KatlinAdapter,
    "catalyst": CatalystAdapter,
    "dealer_gex": DealerGexAdapter,
}


def adapter_for(spec: Dict[str, Any], universe: Dict[str, Dict[str, Any]], *, now: Optional[datetime] = None) -> SignalAdapter:
    name = spec.get("adapter")
    cls = ADAPTERS.get(name)
    if cls is None:
        raise J.JHSignalError([f"no adapter named {name!r} for engine {spec.get('engine_id')}"])
    a = cls(spec, universe, now=now)
    # the registry is the authority for signal_type -> category; the adapter's declared type must be registered
    declared = spec.get("signal_types") or {}
    if cls.signal_type not in declared:
        raise J.JHSignalError([f"adapter {name} emits {cls.signal_type} which engine {spec['engine_id']} does not declare"])
    return a
