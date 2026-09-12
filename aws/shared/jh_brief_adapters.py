"""Brief-domain adapters. Import PlumbingBriefAdapter into jh_adapters.ADAPTERS as plumbing_brief.

score = -(composite_score-50)/50. LIVE briefs only. market:US_EQUITY INTERMEDIATE.
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
