"""Deterministic market thought. No LLM. Missing critical input = NO_READ, never NEUTRAL."""
from __future__ import annotations

STOCKS = {"RISK_ON", "SELECTIVE", "DEFENSIVE", "AVOID", "NO_READ"}
BONDS = {"LONG_DURATION", "NEUTRAL", "SHORT_DURATION", "AVOID", "NO_READ"}
METALS = {"ACCUMULATE", "HOLD", "TRIM", "AVOID", "NO_READ"}
CRYPTO = {"ACCUMULATE", "HOLD", "REDUCE", "AVOID", "NO_READ"}

# gate × constitution enum → arms
TABLE = {
    ("SEVERE", None): ("AVOID", "LONG_DURATION", "HOLD", "AVOID"),
    ("SEVERE", "defensive"): ("AVOID", "LONG_DURATION", "HOLD", "AVOID"),
    ("RISK_OFF", None): ("DEFENSIVE", "LONG_DURATION", "HOLD", "REDUCE"),
    ("RISK_OFF", "defensive"): ("DEFENSIVE", "LONG_DURATION", "HOLD", "REDUCE"),
    ("RISK_OFF", "balanced"): ("DEFENSIVE", "NEUTRAL", "HOLD", "REDUCE"),
    ("NEUTRAL", "aggressive"): ("SELECTIVE", "NEUTRAL", "HOLD", "HOLD"),
    ("NEUTRAL", "balanced"): ("SELECTIVE", "NEUTRAL", "HOLD", "HOLD"),
    ("NEUTRAL", "defensive"): ("DEFENSIVE", "LONG_DURATION", "HOLD", "REDUCE"),
    ("RISK_ON", "aggressive"): ("RISK_ON", "SHORT_DURATION", "HOLD", "HOLD"),
    ("RISK_ON", "balanced"): ("SELECTIVE", "NEUTRAL", "HOLD", "HOLD"),
    ("RISK_ON", "defensive"): ("SELECTIVE", "NEUTRAL", "HOLD", "REDUCE"),
}


def _gate(raw):
    s = str(raw or "").upper()
    if "SEVERE" in s:
        return "SEVERE"
    if "OFF" in s or "DEFENS" in s:
        return "RISK_OFF"
    if "RISK_ON" in s or s.endswith("_ON") or s == "ON":
        return "RISK_ON"
    if "NEUTRAL" in s or "BALANC" in s:
        return "NEUTRAL"
    return None


def _const(raw):
    s = str(raw or "").lower()
    if "aggress" in s:
        return "aggressive"
    if "defens" in s:
        return "defensive"
    if "balance" in s:
        return "balanced"
    return None


def desk_read(board):
    rg = (board or {}).get("regime") or {}
    gate = _gate(rg.get("risk_gate_posture"))
    const = _const((board or {}).get("constitution_posture") or rg.get("constitution_posture"))
    gaps = []
    if not gate:
        gaps.append("risk_gate_posture")
    arms = TABLE.get((gate, const)) or TABLE.get((gate, None))
    if not gate or not arms:
        def dead(why):
            return {"stance": "NO_READ", "read": why}
        why = "NO_READ: critical risk-gate missing or unmapped (%r)" % (rg.get("risk_gate_posture"),)
        return {
            "voice": "deterministic",
            "teacher": "grok-curve",
            "overall": why,
            "macro": why,
            "stocks": dead(why), "bonds": dead(why), "metals": dead(why), "crypto": dead(why),
            "what_would_change_my_mind": ["Fresh risk-gate posture in {SEVERE,RISK_OFF,NEUTRAL,RISK_ON}"],
            "data_gaps": gaps or ["risk_gate"],
            "best_opportunities": [], "calls": [],
            "fallback": True, "parse_error": False,
        }
    st, bd, mt, cr = arms
    overall = (
        "Deterministic desk (no LLM). Gate=%s constitution=%s sizing=%s. "
        "Teachers are off the invoke path."
        % (gate, const or "none", rg.get("risk_gate_sizing"))
    )
    def arm(s, note):
        return {"stance": s, "read": note}
    return {
        "voice": "deterministic",
        "teacher": "grok-curve",
        "overall": overall,
        "macro": "Risk-gate is governor. Constitution enum tilts; missing enum does not invent balanced.",
        "stocks": arm(st, "table gate=%s const=%s" % (gate, const)),
        "bonds": arm(bd, "duration from same table"),
        "metals": arm(mt, "metals last"),
        "crypto": arm(cr, "crypto cut first on risk-off"),
        "what_would_change_my_mind": ["Gate flip", "Constitution enum flip after brain-sync"],
        "data_gaps": gaps,
        "best_opportunities": [], "calls": [],
        "fallback": True, "parse_error": False,
        "table_row": [gate, const],
    }
