"""Brief-domain adapters. Register in BRIEF_ADAPTERS only."""
from jh_adapters import SignalAdapter, _f, _clip, _ev


class PlumbingBriefAdapter(SignalAdapter):
    signal_type, category = "plumbing_stress", "risk"

    def validate_source(self, doc):
        if doc.get("schema") != "brief-1.0" or doc.get("mode") != "plumbing":
            return False
        if doc.get("status") != "LIVE":
            return False
        return _f((doc.get("fields") or {}).get("composite_score")) is not None

    def rows(self, doc):
        fields = doc.get("fields") or {}
        stress = _f(fields.get("composite_score"))
        n_ok = _f(fields.get("n_with_data"))
        n_tot = _f(fields.get("n_indicators"))
        conf = (n_ok / n_tot) if (n_ok is not None and n_tot) else None
        yield {
            "symbol": "US_EQUITY", "entity_type": "market",
            "score": _clip(-(stress - 50.0) / 50.0), "confidence": conf,
            "confidence_basis": "n_with_data/n_indicators", "horizon": "INTERMEDIATE",
            "evidence": _ev(composite_score=stress, composite_label=fields.get("composite_label"),
                            n_indicators=fields.get("n_indicators"), n_with_data=fields.get("n_with_data")),
            "metadata": {"label": fields.get("composite_label"), "brief_source": doc.get("source")},
            "invalidation": {"type": "level", "description": "plumbing composite back under 35"},
        }


class PositioningBriefAdapter(SignalAdapter):
    signal_type, category = "positioning_flow", "institutional_flow"

    def validate_source(self, doc):
        if doc.get("schema") != "brief-1.0" or doc.get("mode") != "positioning" or doc.get("status") != "LIVE":
            return False
        fields = doc.get("fields") or {}
        return _f(fields.get("accumulating")) is not None or _f(fields.get("funds_parsed")) is not None

    def rows(self, doc):
        fields = doc.get("fields") or {}
        acc = _f(fields.get("accumulating"))
        dist = _f(fields.get("distributing"))
        parsed = _f(fields.get("funds_parsed"))
        total = _f(fields.get("funds_total"))
        stale = fields.get("stale_funds") or []
        n_stale = float(len(stale)) if isinstance(stale, list) else 0.0
        if acc is None or dist is None:
            yield {"skip": "no accum/dist breadth"}
            return
        denom = acc + dist
        if denom <= 0:
            yield {"skip": "zero accum+dist"}
            return
        if acc == dist and acc >= 100 and acc % 50 == 0 and (fields.get("breadth_basis") != "uncapped"):
            yield {"skip": "capped equal lists"}
            return
        conf = max(0.0, (parsed - n_stale) / total) if (parsed is not None and total) else None
        yield {
            "symbol": "US_EQUITY", "entity_type": "market",
            "score": _clip((acc - dist) / denom), "confidence": conf,
            "confidence_basis": "(funds_parsed - stale_funds)/funds_total", "horizon": "INTERMEDIATE",
            "evidence": _ev(accumulating=acc, distributing=dist, funds_parsed=parsed, funds_total=total,
                            n_stale=n_stale, breadth_basis=fields.get("breadth_basis")),
            "metadata": {"brief_source": doc.get("source")},
            "invalidation": {"type": "state", "description": "inst breadth flip"},
        }


class MarketTapeBriefAdapter(SignalAdapter):
    """ETF heavy in vs out, uncapped only. score=(in-out)/(in+out). conf=(in+out)/n_etfs."""
    signal_type, category = "market_tape_flow", "price_confirmation"

    def validate_source(self, doc):
        if doc.get("schema") != "brief-1.0" or doc.get("mode") != "market_tape" or doc.get("status") != "LIVE":
            return False
        fields = doc.get("fields") or {}
        return _f(fields.get("heavy_inflow_n")) is not None and _f(fields.get("heavy_outflow_n")) is not None

    def rows(self, doc):
        fields = doc.get("fields") or {}
        n_in = _f(fields.get("heavy_inflow_n"))
        n_out = _f(fields.get("heavy_outflow_n"))
        n_etfs = _f(fields.get("n_etfs"))
        if fields.get("breadth_basis") != "uncapped":
            yield {"skip": "market-tape breadth_basis is not uncapped"}
            return
        denom = n_in + n_out
        if denom <= 0:
            yield {"skip": "zero heavy in+out -- not a tape vote"}
            return
        conf = (denom / n_etfs) if n_etfs else None
        yield {
            "symbol": "US_EQUITY", "entity_type": "market",
            "score": _clip((n_in - n_out) / denom), "confidence": conf,
            "confidence_basis": "(heavy_in+heavy_out)/n_etfs", "horizon": "INTERMEDIATE",
            "evidence": _ev(heavy_inflow_n=n_in, heavy_outflow_n=n_out, other_flow_n=fields.get("other_flow_n"),
                            n_etfs=n_etfs, session=fields.get("session"), breadth_pct=fields.get("breadth_pct")),
            "metadata": {"brief_source": doc.get("source"), "session": fields.get("session")},
            "invalidation": {"type": "state", "description": "next ETF heavy-flow session flips in/out"},
        }


BRIEF_ADAPTERS = {
    "plumbing_brief": PlumbingBriefAdapter,
    "positioning_brief": PositioningBriefAdapter,
    "market_tape_brief": MarketTapeBriefAdapter,
}
