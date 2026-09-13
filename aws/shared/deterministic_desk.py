"""Desk that asks questions in Grok order. Still not an LLM. Trace is the thought.
Order: missing? authority? plumbing? credit? dollar? vol? then table tilt.
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
    if isinstance(legs, dict) and isinstance(legs.get(name), dict):
        return legs[name]
    return {}


def _tighten(stance, floor):
    if RANK.get(stance, 3) <= RANK.get(floor, 3):
        return stance
    return floor


def think(board):
    """Return (arms or None, trace). arms is (st,bd,mt,cr) or None for NO_READ."""
    rg = (board or {}).get("regime") or {}
    trace = []
    gate = _gate(rg.get("risk_gate_posture") or (board or {}).get("posture"))
    const = _const((board or {}).get("constitution_posture") or rg.get("constitution_posture"))
    sizing = _num(rg.get("risk_gate_sizing") or (board or {}).get("sizing_multiplier"))
    fund = _num(rg.get("funding_score") or _leg(board, "funding").get("score"))
    credit = _num(_leg(board, "credit").get("score"))
    ccc = _num(_leg(board, "credit").get("ccc_21d_pct"))
    dxy = _num(rg.get("dxy_63d_pct") or _leg(board, "dollar").get("dxy_63d_pct"))
    vix = _num(rg.get("vix") or _leg(board, "structure").get("vix"))
    auth = (board or {}).get("authority") or rg.get("authority") or {}
    if not isinstance(auth, dict):
        auth = {}

    def note(step, ok, why):
        trace.append({"step": step, "ok": ok, "why": why})

    if not gate:
        note("gate", False, "missing posture — refuse to invent NEUTRAL")
        return None, trace
    note("gate", True, gate)

    if auth.get("allows_new_entries") is False or _num(auth.get("cap") or auth.get("sizing_multiplier")) == 0:
        note("authority", False, "DATA_HOLD / no new entries")
        return ("AVOID", "LONG_DURATION", "HOLD", "AVOID"), trace
    note("authority", True, "not blocking")

    if sizing is not None and sizing <= 0:
        note("sizing", False, "multiplier 0")
        return ("AVOID", "LONG_DURATION", "HOLD", "AVOID"), trace
    note("sizing", True, sizing)

    arms = TABLE.get((gate, const)) or TABLE.get((gate, None))
    st, bd, mt, cr = arms
    note("table", True, "%s x %s -> %s" % (gate, const or "none", st))

    if fund is not None and fund <= -1.5:
        st, cr = _tighten(st, "DEFENSIVE"), _tighten(cr, "REDUCE")
        note("plumbing", False, "funding %s — do not add equity risk" % fund)
    else:
        note("plumbing", True, fund)

    stressed_credit = (credit is not None and credit <= -1.0) or (ccc is not None and ccc >= 8)
    if stressed_credit:
        st, cr = _tighten(st, "DEFENSIVE"), _tighten(cr, "REDUCE")
        note("credit", False, "credit score=%s ccc21=%s — credit IS visible liquidity" % (credit, ccc))
    else:
        note("credit", True, "score=%s ccc21=%s" % (credit, ccc))

    if dxy is not None and dxy >= 3:
        st, cr = _tighten(st, "SELECTIVE"), _tighten(cr, "REDUCE")
        note("dollar", False, "DXY 63d %+0.1f — dollar first" % dxy)
    else:
        note("dollar", True, dxy)

    if vix is not None and vix >= 25:
        st, cr = _tighten(st, "DEFENSIVE"), _tighten(cr, "REDUCE")
        note("vol", False, "VIX %s" % vix)
    else:
        note("vol", True, vix)

    if const == "defensive" and st in ("RISK_ON",):
        st = "SELECTIVE"
        note("constitution", False, "defensive Brain caps RISK_ON")
    else:
        note("constitution", True, const or "absent (not assumed balanced)")

    return (st, bd, mt, cr), trace


def desk_read(board):
    arms, trace = think(board)
    if arms is None:
        why = "NO_READ: " + (trace[-1]["why"] if trace else "no gate")
        dead = {"stance": "NO_READ", "read": why}
        return {
            "voice": "deterministic", "teacher": "grok-curve", "ok": False,
            "overall": why, "macro": why, "reasoning": trace,
            "stocks": dead, "bonds": dead, "metals": dead, "crypto": dead,
            "what_would_change_my_mind": ["A real risk-gate posture"],
            "data_gaps": ["risk_gate_posture"], "best_opportunities": [], "calls": [],
            "fallback": True, "parse_error": False, "vetoes": [t["why"] for t in trace if not t["ok"]],
        }
    st, bd, mt, cr = arms
    vetoes = [t["why"] for t in trace if not t["ok"]]
    overall = " ".join("%s:%s" % (t["step"], "ok" if t["ok"] else t["why"]) for t in trace)
    def arm(s):
        return {"stance": s, "read": overall[:240]}
    return {
        "voice": "deterministic", "teacher": "grok-curve", "ok": True,
        "overall": overall[:500],
        "macro": "Ask in order: gate, authority, plumbing, credit, dollar, vol. Never skip to a stock pick.",
        "stocks": arm(st), "bonds": arm(bd), "metals": arm(mt), "crypto": arm(cr),
        "what_would_change_my_mind": [t["step"] + " flip" for t in trace if not t["ok"]] or ["Gate flip"],
        "data_gaps": [], "best_opportunities": [], "calls": [],
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
        return {"ok": True, "task": "veto-check", "allowed": allowed, "stance": st,
                "vetoes": out.get("vetoes") or [], "reasoning": out.get("reasoning")}
    return {"ok": False, "task": kind, "error": "unknown_task", "allowed": ["stance", "veto-check"]}
