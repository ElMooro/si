"""Autonomous deterministic desk. No LLM. Tasks: stance | veto-check.
Missing critical input = NO_READ. Plumbing and vol can cap a calm gate.
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


def _leg(board, name):
    rg = (board or {}).get("regime") or {}
    legs = rg.get("risk_gate_legs") or (board or {}).get("legs") or {}
    if isinstance(legs, dict):
        block = legs.get(name) or {}
        if isinstance(block, dict):
            return block
    return {}


def _tighten(stance, floor):
    if RANK.get(stance, 3) <= RANK.get(floor, 3):
        return stance
    return floor


def _authority_hold(board):
    auth = (board or {}).get("authority") or ((board or {}).get("regime") or {}).get("authority") or {}
    if not isinstance(auth, dict):
        return False, None
    if auth.get("allows_new_entries") is False:
        return True, "authority_blocks_entries"
    sm = _num(auth.get("sizing_multiplier") or auth.get("cap"))
    if sm is not None and sm <= 0:
        return True, "authority_cap_zero"
    return False, None


def desk_read(board):
    rg = (board or {}).get("regime") or {}
    gate = _gate(rg.get("risk_gate_posture") or (board or {}).get("posture"))
    const = _const((board or {}).get("constitution_posture") or rg.get("constitution_posture"))
    fund = _num(rg.get("funding_score"))
    if fund is None:
        fund = _num(_leg(board, "funding").get("score"))
    vix = _num(rg.get("vix") or _leg(board, "structure").get("vix"))
    dxy = _num(rg.get("dxy_63d_pct") or _leg(board, "dollar").get("dxy_63d_pct"))
    sizing = _num(rg.get("risk_gate_sizing") or (board or {}).get("sizing_multiplier"))
    hold, hold_why = _authority_hold(board)
    gaps = []
    if not gate:
        gaps.append("risk_gate_posture")
    arms = TABLE.get((gate, const)) or TABLE.get((gate, None))
    if not gate or not arms:
        why = "NO_READ: risk-gate posture missing (%r)" % (
            rg.get("risk_gate_posture") or (board or {}).get("posture"),)
        dead = {"stance": "NO_READ", "read": why}
        return {
            "voice": "deterministic", "teacher": "grok-curve", "ok": False,
            "overall": why, "macro": why,
            "stocks": dead, "bonds": dead, "metals": dead, "crypto": dead,
            "what_would_change_my_mind": ["Publish a gate enum"],
            "data_gaps": gaps or ["risk_gate"],
            "best_opportunities": [], "calls": [],
            "fallback": True, "parse_error": False, "task": "stance",
        }
    st, bd, mt, cr = arms
    vetoes = []
    if hold:
        st, cr = "AVOID", "AVOID"
        vetoes.append(hold_why)
    if sizing is not None and sizing <= 0:
        st, cr = "AVOID", "AVOID"
        vetoes.append("sizing_zero")
    if fund is not None and fund <= -1.5:
        st = _tighten(st, "DEFENSIVE")
        cr = _tighten(cr, "REDUCE")
        vetoes.append("funding<=-1.5")
    if vix is not None and vix >= 25:
        st = _tighten(st, "DEFENSIVE")
        cr = _tighten(cr, "REDUCE")
        vetoes.append("vix>=25")
    if dxy is not None and dxy >= 3.0:
        st = _tighten(st, "SELECTIVE")
        cr = _tighten(cr, "REDUCE")
        vetoes.append("dollar_63d>=3")
    overall = (
        "Autonomous desk. gate=%s const=%s sizing=%s fund=%s vix=%s dxy63=%s vetoes=%s"
        % (gate, const or "none", sizing, fund, vix, dxy, vetoes or "none")
    )
    def arm(s, note):
        return {"stance": s, "read": note}
    return {
        "voice": "deterministic", "teacher": "grok-curve", "ok": True,
        "overall": overall,
        "macro": "Gate, then authority/sizing, then funding/vol/dollar caps.",
        "stocks": arm(st, "vetoes=%s" % (vetoes or "none")),
        "bonds": arm(bd, "duration table"),
        "metals": arm(mt, "metals last"),
        "crypto": arm(cr, "first cut"),
        "what_would_change_my_mind": ["Gate flip", "Funding > -1", "VIX < 20"],
        "data_gaps": gaps,
        "best_opportunities": [], "calls": [],
        "fallback": True, "parse_error": False,
        "table_row": [gate, const], "funding_score": fund, "vix": vix,
        "dxy_63d_pct": dxy, "vetoes": vetoes, "task": "stance",
    }


def execute_task(task, payload=None):
    """Student-safe task runner. Only stance and veto-check. No IAM, no HTTP."""
    kind = str(task or "stance").lower().replace(" ", "_")
    board = payload if isinstance(payload, dict) else {}
    if kind in ("stance", "market_read", "think", "desk"):
        return desk_read(board)
    if kind in ("veto-check", "veto_check", "can_add_risk"):
        out = desk_read(board)
        st = (out.get("stocks") or {}).get("stance")
        allowed = st in ("RISK_ON", "SELECTIVE") and not out.get("vetoes")
        return {"ok": True, "task": "veto-check", "allowed": allowed,
                "stance": st, "vetoes": out.get("vetoes") or []}
    return {"ok": False, "task": kind, "error": "unknown_task",
            "allowed": ["stance", "veto-check"]}
