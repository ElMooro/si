"""Brief-domain adapters. Register in BRIEF_ADAPTERS only. jh_adapters.adapter_for falls through.
LIVE briefs only. market:US_EQUITY INTERMEDIATE.
"""
from jh_adapters import SignalAdapter, _f, _clip, _ev


class PlumbingBriefAdapter(SignalAdapter):
    signal_type, category = "plumbing_stress", "risk"

    def validate_source(self, doc):
        if doc.get("schema") != "brief-1.0" or doc.get("mode") != "plumbing":
            return False
        if doc.get("status") != "LIVE":
            return False
        fields = doc.get("fields") or {}
        return _f(fields.get("composite_score")) is not None

    def rows(self, doc):
        fields = doc.get("fields") or {}
        stress = _f(fields.get("composite_score"))
        n_ok = _f(fields.get("n_with_data"))
        n_tot = _f(fields.get("n_indicators"))
        conf = (n_ok / n_tot) if (n_ok is not None and n_tot) else None
        yield {
            "symbol": "US_EQUITY",
            "entity_type": "market",
            "score": _clip(-(stress - 50.0) / 50.0),
            "confidence": conf,
            "confidence_basis": "n_with_data/n_indicators",
            "horizon": "INTERMEDIATE",
            "evidence": _ev(
                composite_score=stress,
                composite_label=fields.get("composite_label"),
                n_indicators=fields.get("n_indicators"),
                n_with_data=fields.get("n_with_data"),
                layer_scores=fields.get("layer_scores"),
            ),
            "metadata": {
                "label": fields.get("composite_label"),
                "brief_source": doc.get("source"),
            },
            "invalidation": {
                "type": "level",
                "description": "plumbing composite back under 35",
            },
        }


class PositioningBriefAdapter(SignalAdapter):
    """Inst buy vs sell breadth. score=(accum-dist)/(accum+dist). No signal if both missing."""
    signal_type, category = "positioning_flow", "institutional_flow"

    def validate_source(self, doc):
        if doc.get("schema") != "brief-1.0" or doc.get("mode") != "positioning":
            return False
        if doc.get("status") != "LIVE":
            return False
        fields = doc.get("fields") or {}
        return _f(fields.get("accumulating")) is not None or _f(fields.get("funds_parsed")) is not None

    def rows(self, doc):
        fields = doc.get("fields") or {}
        acc = _f(fields.get("accumulating"))
        dist = _f(fields.get("distributing"))
        parsed = _f(fields.get("funds_parsed"))
        total = _f(fields.get("funds_total"))
        if acc is None or dist is None:
            yield {"skip": "no accum/dist breadth"}
            return
        denom = acc + dist
        if denom <= 0:
            yield {"skip": "zero accum+dist"}
            return
        # ops 5417 truncated both lists at 100 and published len() as breadth -> 100/100 -> 0.0 forever.
        # Equal counts sitting exactly on a round cap are a truncation artifact; never score them.
        if acc == dist and acc >= 100 and acc % 50 == 0 and (fields.get("breadth_basis") != "uncapped"):
            yield {"skip": "breadth counts equal at a round cap (%d/%d) -- capped lists, not breadth" % (int(acc), int(dist))}
            return
        conf = (parsed / total) if (parsed is not None and total) else None
        yield {
            "symbol": "US_EQUITY",
            "entity_type": "market",
            "score": _clip((acc - dist) / denom),
            "confidence": conf,
            "confidence_basis": "funds_parsed/funds_total",
            "horizon": "INTERMEDIATE",
            "evidence": _ev(
                accumulating=acc,
                distributing=dist,
                funds_parsed=parsed,
                funds_total=total,
                as_of_quarter=fields.get("as_of_quarter"),
                stale_funds=fields.get("stale_funds"),
            ),
            "metadata": {"brief_source": doc.get("source"), "quarter": fields.get("as_of_quarter")},
            "invalidation": {"type": "state", "description": "next 13F or inst-flow flip buy/sell breadth"},
        }


BRIEF_ADAPTERS = {
    "plumbing_brief": PlumbingBriefAdapter,
    "positioning_brief": PositioningBriefAdapter,
}
