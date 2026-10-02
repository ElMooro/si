"""Desk that asks questions in Grok order. Still not an LLM. Trace is the thought.
Order: missing? authority? plumbing? credit? dollar? vol? then table tilt.
"""
from __future__ import annotations

import math
from decimal import Decimal

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
    if not isinstance(raw, str): return None
    return {"SEVERE": "SEVERE", "RISK_OFF": "RISK_OFF", "OFF": "RISK_OFF",
            "RISK_ON": "RISK_ON", "ON": "RISK_ON", "NEUTRAL": "NEUTRAL"}.get(raw.strip().upper())


def _const(raw):
    if not isinstance(raw, str): return None
    value = raw.strip().lower()
    return value if value in ("aggressive", "defensive", "balanced") else None


def _num(v):
    if type(v) not in (int, float, Decimal): return None
    try:
        value = float(v)
        return value if math.isfinite(value) and not (value == 0 and v != 0) else None
    except (ValueError, OverflowError):
        return None


def _first(*pairs):
    for row, key in pairs:
        if isinstance(row, dict) and key in row:
            return row[key]
    return None


def _qualification():
    return {"contract": "deterministic-desk-inputs.v1", "status": "unqualified_rule_table",
            "source_qualified": False, "forecast_qualified": False, "sizing_eligible": False,
            "execution_eligible": False, "reason": "Reported constraints and historical rules are not validated investment permission."}


def _leg(board, name):
    rg = board.get("regime") if isinstance(board, dict) else None
    legs = _first((rg, "risk_gate_legs"), (board, "legs"))
    if isinstance(legs, dict) and isinstance(legs.get(name), dict): return legs[name]
    return {}


def _tighten(stance, floor):
    if RANK.get(stance, 3) <= RANK.get(floor, 3):
        return stance
    return floor


def think(board):
    """Return (arms or None, trace). arms is (st,bd,mt,cr) or None for NO_READ."""
    rg = board.get("regime") if isinstance(board, dict) else None
    rg = rg if isinstance(rg, dict) else {}
    trace = []
    gate = _gate(_first((rg, "risk_gate_posture"), (board, "posture")))
    const = _const(_first((board, "constitution_posture"), (rg, "constitution_posture")))
    sizing = _num(_first((rg, "risk_gate_sizing"), (board, "sizing_multiplier")))
    fund = _num(_first((rg, "funding_score"), (_leg(board, "funding"), "score")))
    credit = _num(_leg(board, "credit").get("score"))
    ccc = _num(_leg(board, "credit").get("ccc_21d_pct"))
    dxy = _num(_first((rg, "dxy_63d_pct"), (_leg(board, "dollar"), "dxy_63d_pct")))
    vix = _num(_first((rg, "vix"), (_leg(board, "structure"), "vix")))
    auth = _first((board, "authority"), (rg, "authority"))
    if not isinstance(auth, dict):
        auth = {}

    def note(step, ok, why):
        trace.append({"step": step, "ok": ok, "why": why})

    if not gate:
        note("gate", None, "typed complete posture unavailable; no assumed NEUTRAL")
        return None, trace
    note("gate", True, gate)

    allows = _first((auth, "allows_new_entries"), (rg, "authority_allows_new_entries"))
    caps = [_num(auth.get("cap")), _num(auth.get("sizing_multiplier")), _num(rg.get("authority_cap_pct"))]
    if allows is False or any(value is not None and value <= 0 for value in caps):
        note("authority", False, "reported no-entry constraint or nonpositive cap")
        return None, trace
    declared_caps = [row[key] for row, key in ((auth, "cap"), (auth, "sizing_multiplier"), (rg, "authority_cap_pct")) if key in row]
    caps_valid = bool(declared_caps) and all(_num(value) is not None for value in declared_caps)
    note("authority", True if allows is True and caps_valid else None,
         "reported permission only" if allows is True and caps_valid else "typed permission or cap unavailable")

    if sizing is not None and sizing <= 0:
        note("sizing", False, "nonpositive multiplier %s" % sizing)
        return None, trace
    note("sizing", True if sizing is not None else None, sizing)

    arms = TABLE.get((gate, const)) or TABLE.get((gate, None))
    st, bd, mt, cr = arms
    note("table", True, "%s x %s -> %s" % (gate, const or "none", st))

    if fund is not None and fund <= -1.5:
        st, cr = _tighten(st, "DEFENSIVE"), _tighten(cr, "REDUCE")
        note("plumbing", False, "funding %s — do not add equity risk" % fund)
    else:
        note("plumbing", True if fund is not None else None, fund)

    stressed_credit = (credit is not None and credit <= -1.0) or (ccc is not None and ccc >= 8)
    if stressed_credit:
        st, cr = _tighten(st, "DEFENSIVE"), _tighten(cr, "REDUCE")
        note("credit", False, "credit score=%s ccc21=%s — credit IS visible liquidity" % (credit, ccc))
    else:
        note("credit", True if credit is not None and ccc is not None else None, "score=%s ccc21=%s" % (credit, ccc))

    if dxy is not None and dxy >= 3:
        st, cr = _tighten(st, "SELECTIVE"), _tighten(cr, "REDUCE")
        note("dollar", False, "DXY 63d %+0.1f — dollar first" % dxy)
    else:
        note("dollar", True if dxy is not None else None, dxy)

    if vix is not None and vix >= 25:
        st, cr = _tighten(st, "DEFENSIVE"), _tighten(cr, "REDUCE")
        note("vol", False, "VIX %s" % vix)
    else:
        note("vol", True if vix is not None else None, vix)

    if const == "defensive" and st in ("RISK_ON",):
        st = "SELECTIVE"
        note("constitution", False, "defensive Brain caps RISK_ON")
    else:
        note("constitution", True if const is not None else None, const or "unavailable")

    return (st, bd, mt, cr), trace


def desk_read(board):
    arms, trace = think(board)
    if arms is None:
        why = "NO_READ: " + (trace[-1]["why"] if trace else "no gate")
        dead = {"stance": "NO_READ", "read": why}
        return {
            "voice": "deterministic", "teacher": "grok-curve", "ok": False, "qualification": _qualification(),
            "overall": why, "macro": why, "reasoning": trace,
            "stocks": dict(dead), "bonds": dict(dead), "metals": dict(dead), "crypto": dict(dead),
            "what_would_change_my_mind": ["Resolve the reported blocking constraint and qualify source inputs"],
            "data_gaps": [t["step"] for t in trace if t["ok"] is None], "best_opportunities": [], "calls": [],
            "fallback": True, "parse_error": False, "vetoes": [t["why"] for t in trace if t["ok"] is False],
        }
    st, bd, mt, cr = arms
    vetoes = [t["why"] for t in trace if t["ok"] is False]
    overall = " ".join("%s:%s" % (t["step"], "reported_clear" if t["ok"] is True else "unknown" if t["ok"] is None else t["why"]) for t in trace)
    def arm(s):
        return {"stance": s, "read": overall[:240]}
    return {
        "voice": "deterministic", "teacher": "grok-curve", "ok": True, "qualification": _qualification(),
        "overall": overall[:500],
        "macro": "Ask in order: gate, authority, plumbing, credit, dollar, vol. Never skip to a stock pick.",
        "stocks": arm(st), "bonds": arm(bd), "metals": arm(mt), "crypto": arm(cr),
        "what_would_change_my_mind": [t["step"] + " flip" for t in trace if t["ok"] is False] or ["Gate flip"],
        "data_gaps": [t["step"] for t in trace if t["ok"] is None], "best_opportunities": [], "calls": [],
        "fallback": True, "parse_error": False, "reasoning": trace, "vetoes": vetoes,
    }


def execute_task(task, payload=None):
    kind = str(task or "stance").lower().replace(" ", "_")
    board = payload if isinstance(payload, dict) else {}
    if kind in ("stance", "market_read", "think", "desk"):
        return desk_read(board)
    if kind in ("veto-check", "veto_check", "can_add_risk"):
        out = desk_read(board)
        st = (out.get("stocks") or {}).get("stance")
        allowed = bool(out.get("ok")) and st in ("RISK_ON", "SELECTIVE") and not out.get("vetoes")
        return {"ok": True, "task": "veto-check", "allowed": False, "stance": st,
                "reported_rule_result": allowed if not out.get("data_gaps") else None,
                "qualification": _qualification(), "data_gaps": out.get("data_gaps") or [],
                "vetoes": out.get("vetoes") or [], "reasoning": out.get("reasoning")}
    return {"ok": False, "task": kind, "error": "unknown_task", "allowed": ["stance", "veto-check"]}
