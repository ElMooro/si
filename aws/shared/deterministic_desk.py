"""Deterministic market thought. No LLM. Missing critical input = NO_READ.
Plumbing can veto a calm gate (Brain: never touch stocks when plumbing is shaky).
"""
from __future__ import annotations

TABLE = {
    ("SEVERE", None): ("AVOID", "LONG_DURATION", "HOLD", "AVOID"),
    ("SEVERE", "defensive"): ("AVOID", "LONG_DURATION", "HOLD", "AVOID"),
    ("RISK_OFF", None): ("DEFENSIVE", "LONG_DURATION", "HOLD", "REDUCE"),
    ("RISK_OFF", "defensive"): ("DEFENSIVE", "LONG_DURATION", "HOLD", "REDUCE"),
    ("RISK_OFF", "balanced"): ("DEFENSIVE", "NEUTRAL", "HOLD", "REDUCE"),
    ("NEUTRAL", "aggressive"): ("SELECTIVE", "NEUTRAL", "HOLD", "HOLD"),
    ("NEUTRAL", "balanced"): ("SELECTIVE", "NEUTRAL", "HOLD", "HOLD"),
    ("NEUTRAL", "defensive"): ("DEFENSIVE", "LONG_DURATION", "HOLD", "REDUCE"),
    ("NEUTRAL", None): ("SELECTIVE", "NEUTRAL", "HOLD", "HOLD"),
    ("RISK_ON", "aggressive"): ("RISK_ON", "SHORT_DURATION", "HOLD", "HOLD"),
    ("RISK_ON", "balanced"): ("SELECTIVE", "NEUTRAL", "HOLD", "HOLD"),
    ("RISK_ON", "defensive"): ("SELECTIVE", "NEUTRAL", "HOLD", "REDUCE"),
    ("RISK_ON", None): ("SELECTIVE", "NEUTRAL", "HOLD", "HOLD"),
}

RANK = {"AVOID": 0, "REDUCE": 1, "DEFENSIVE": 2, "TRIM": 2, "HOLD": 3,
        "SELECTIVE": 3, "LONG_DURATION": 3, "NEUTRAL": 3, "SHORT_DURATION": 3,
        "ACCUMULATE": 4, "RISK_ON": 4, "NO_READ": -1}


def _gate(raw):
    s = str(raw or "").upper()
    if "SEVERE" in s:
        return "SEVERE"
    if "OFF" in s:
        return "RISK_OFF"
    if "RISK_ON" in s or s == "ON":
        return "RISK_ON"
    if "NEUTRAL" in s:
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


def _num(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _funding_score(board):
    rg = (board or {}).get("regime") or {}
    n = _num(rg.get("funding_score"))
    if n is not None:
        return n
    legs = rg.get("risk_gate_legs") or (board or {}).get("legs") or {}
    if isinstance(legs, dict):
        block = legs.get("funding") or {}
        if isinstance(block, dict):
            return _num(block.get("score"))
    return None


def _tighten(stance, floor):
    if RANK.get(stance, 3) <= RANK.get(floor, 3):
        return stance
    return floor


def desk_read(board):
    rg = (board or {}).get("regime") or {}
    gate = _gate(rg.get("risk_gate_posture") or (board or {}).get("posture"))
    const = _const((board or {}).get("constitution_posture") or rg.get("constitution_posture"))
    fund = _funding_score(board)
    gaps = []
    if not gate:
        gaps.append("risk_gate_posture")
    arms = TABLE.get((gate, const)) or TABLE.get((gate, None))
    if not gate or not arms:
        why = "NO_READ: risk-gate posture missing or unmapped (%r)" % (
            rg.get("risk_gate_posture") or (board or {}).get("posture"),)
        dead = {"stance": "NO_READ", "read": why}
        return {
            "voice": "deterministic", "teacher": "grok-curve",
            "overall": why, "macro": why,
            "stocks": dead, "bonds": dead, "metals": dead, "crypto": dead,
            "what_would_change_my_mind": ["Publish SEVERE|RISK_OFF|NEUTRAL|RISK_ON"],
            "data_gaps": gaps or ["risk_gate"],
            "best_opportunities": [], "calls": [],
            "fallback": True, "parse_error": False,
        }
    st, bd, mt, cr = arms
    vetoes = []
    if fund is not None and fund <= -1.5:
        st = _tighten(st, "DEFENSIVE")
        cr = _tighten(cr, "REDUCE")
        vetoes.append("funding_score<=-1.5 plumbing veto (RRP/reserves)")
    overall = (
        "Deterministic desk. Gate=%s const=%s sizing=%s funding=%s. Vetoes=%s. "
        "Teachers off invoke."
        % (gate, const or "none", rg.get("risk_gate_sizing"), fund, vetoes or "none")
    )
    def arm(s, note):
        return {"stance": s, "read": note}
    return {
        "voice": "deterministic", "teacher": "grok-curve",
        "overall": overall,
        "macro": "Gate first. Constitution tilts only if present. Funding stress caps risk-on.",
        "stocks": arm(st, "gate=%s fund=%s veto=%s" % (gate, fund, bool(vetoes))),
        "bonds": arm(bd, "duration from table"),
        "metals": arm(mt, "metals last"),
        "crypto": arm(cr, "cut first when plumbing fails"),
        "what_would_change_my_mind": ["Gate flip", "Funding score back above -1.0"],
        "data_gaps": gaps,
        "best_opportunities": [], "calls": [],
        "fallback": True, "parse_error": False,
        "table_row": [gate, const], "funding_score": fund, "vetoes": vetoes,
    }
