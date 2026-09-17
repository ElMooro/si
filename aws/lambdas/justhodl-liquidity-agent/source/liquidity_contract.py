"""Required FRED legs and observation freshness for the balance-sheet proxy."""
import copy
import math
from datetime import datetime, timezone

CORE = {"WALCL": ("fed_balance_sheet", 16), "WTREGEN": ("tga", 16), "RRPONTSYD": ("rrp", 7)}
FORMULA_NOTE = ("WALCL - WTREGEN - RRPONTSYD is a balance-sheet proxy, not cash available to buy equities. "
                "WALCL is a Wednesday level, WTREGEN a weekly average, and RRP a daily observation; dates are explicit.")


def observation_status(day, max_days, now=None):
    if not day:
        return "unavailable"
    try:
        today = (now or datetime.now(timezone.utc)).date()
        age = (today - datetime.fromisoformat(day[:10]).date()).days
        return "invalid" if age < 0 else "stale" if age > max_days else "fresh"
    except (TypeError, ValueError):
        return "invalid"


def core_quality(values, dates, publication_date, now=None):
    states, missing = {}, []
    for sid, (_, days) in CORE.items():
        value = values.get(sid)
        if value is None:
            state = "unavailable"
        elif isinstance(value, bool) or not isinstance(value, (float, int)) or not math.isfinite(value) or value < 0:
            state = "invalid"
        else:
            state = observation_status(dates.get(sid), days, now)
        states[sid] = state
        if state != "fresh":
            missing.append(sid)
    status = next((s for s in ("invalid", "unavailable", "stale") if s in states.values()), "fresh")
    return {"observation_date": min((d for d in dates.values() if d), default=None),
            "publication_date": publication_date, "frequency": "weekly", "freshness_basis": "observation",
            "status": status, "missing": sorted(missing), "input_dates": dates, "input_status": states}


def unavailable_payload(values, dates, quality, previous=None):
    """Keep consumer keys while expiring every current score or directional call."""
    out = copy.deepcopy(previous) if isinstance(previous, dict) else {}
    for key in ("meta", "catalog", "part4", "components", "soma", "reserves", "money_supply", "dollar", "yields", "funding", "chart_data"):
        out.setdefault(key, {})
    out.update(generated_at=quality["publication_date"], quality=quality, call=None, ok=False,
               formula="WALCL - WTREGEN - RRPONTSYD", formula_note=FORMULA_NOTE)
    out["meta"].update(generated_at=quality["publication_date"], agent_version="2.1.0", data_sources=["FRED"])
    out["core"] = {"net_liquidity": {"value_bn": None, "label": quality["status"].upper(), "score": None, "color": "#888888"}}
    for sid, (name, _) in CORE.items():
        value = values.get(sid)
        out["core"][name] = {"value_bn": value if isinstance(value, (int, float)) and math.isfinite(value) else None,
                             "date": dates.get(sid), "unit": "Billions USD"}
    out["regime"] = {"trend": "UNKNOWN", "structure": "UNKNOWN", "spy_signal": None, "signal_strength": None,
                     "delta_4w_bn": None, "delta_13w_bn": None}
    out["spy_signal"] = {"direction": None, "strength": None, "lead_days": None, "confidence": None,
                         "validated": False, "basis": "Required liquidity observations unavailable or stale.", "components": {}}
    out["signal_logger"] = {**(out.get("signal_logger") or {}), "direction": None, "score": None,
                            "label": "UNAVAILABLE", "lead_days": None, "status": "expired"}
    out["components"] = {"tga_analysis": {"signal": "UNKNOWN", "score": None}, "rrp_analysis": {"signal": "UNKNOWN"}}
    out["part4"] = {"quality": quality, "status": "unavailable"}
    return out
