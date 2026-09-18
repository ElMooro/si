"""Pure observation and diagnostic rules. Signals remain research-only.

Definitions for diagnostic inputs follow https://fred.stlouisfed.org/series/<id>.
Age ceilings are conservative observation-age bounds, not release calendars.
Unknown definitions do not acquire a unit, freshness status or voting permission.
"""
import csv
import io
import math
from datetime import date, datetime, timezone

# unit, frequency, age ceiling (days), seasonal adjustment
DEFINITIONS = {
    **{sid: ("Number", "W", 21, "SA") for sid in ("ICSA", "IC4WSA", "CCSA", "CC4WSA")},
    "IURSA": ("Percent", "W", 21, "SA"),
    **{sid: ("Percentage points", "D", 7, "NSA") for sid in ("T10Y3M", "T10Y2Y", "T10YFF", "T5YFF")},
    **{sid: ("Percent", "D", 7, "NSA") for sid in ("SOFR", "IORB", "EFFR", "DFII10", "DGS2", "DGS10", "T5YIE", "T10YIE", "BAMLH0A0HYM2", "BAMLC0A0CM")},
    **{sid: ("Percentage points", "M", 75, "SA") for sid in ("SAHMREALTIME", "SAHMCURRENT")},
    **{sid: ("Millions of U.S. dollars", "W", 14, "NSA") for sid in ("WALCL", "WTREGEN", "WRESBAL")},
    "RRPONTSYD": ("Billions of U.S. dollars", "D", 7, "NSA"),
    "GDP": ("Billions of U.S. dollars", "Q", 200, "SAAR"),
    "DTWEXBGS": ("Index January 2006=100", "D", 11, "NSA"),
    "NEWORDER": ("Millions of U.S. dollars", "M", 100, "SA"),
    "INDPRO": ("Index 2017=100", "M", 75, "SA"),
    "DRTSCILM": ("Percent", "Q", 200, "NSA"),
    "NFCI": ("Index", "W", 21, "NSA"),
    "RECPROUSM156N": ("Percent", "M", 120, "NSA"),
    "GDPNOW": ("Percent change at annual rate", "D", 21, "SAAR"),
}


def finite(value):
    return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value)


def csv_observations(raw, sid, today=None):
    """Exact series column, chronological ordering, unique observation dates."""
    today = today or datetime.now(timezone.utc).date()
    rows = csv.DictReader(io.StringIO(raw.decode("utf-8-sig")))
    if not rows.fieldnames or sid not in rows.fieldnames:
        raise ValueError("exact series column absent: " + sid)
    date_key = rows.fieldnames[0]
    observations = {}
    for row in rows:
        stamp = str(row.get(date_key, "")).strip()
        if not stamp: continue
        period = date.fromisoformat(stamp)
        if period > today: raise ValueError("future observation date")
        text = str(row.get(sid, "")).strip()
        if text in ("", "."): continue
        number = float(text)
        if not math.isfinite(number): raise ValueError("nonfinite provider observation")
        if stamp in observations and observations[stamp] != number:
            raise ValueError("conflicting duplicate observation date")
        observations[stamp] = number
    return sorted(observations.items(), reverse=True)


def observation_quality(sid, period, now=None):
    now = now or datetime.now(timezone.utc)
    definition = DEFINITIONS.get(sid)
    try: age = (now.date() - date.fromisoformat(str(period))).days
    except (TypeError, ValueError): age = None
    ceiling = definition[2] if definition else None
    status = ("unknown" if ceiling is None or age is None else
              "invalid" if age < 0 else "stale" if age > ceiling else "within_age_ceiling")
    return {"status": status, "observation_age_days": age, "maximum_age_days": ceiling,
            "policy": "conservative_observation_age_not_release_calendar"}


def usable(hot, sid, now=None):
    row = hot.get(sid) or {}
    definition = DEFINITIONS.get(sid)
    if not isinstance(row, dict) or not definition or row.get("data_unavailable"):
        return None
    if row.get("unit") != definition[0] or row.get("source", {}).get("series_id") != sid:
        return None
    if observation_quality(sid, row.get("as_of"), now)["status"] != "within_age_ceiling":
        return None
    return row.get("value") if finite(row.get("value")) else None


def curve_movement(hot, now=None):
    short, long = hot.get("DGS2") or {}, hot.get("DGS10") or {}
    a, b = usable(hot, "DGS2", now), usable(hot, "DGS10", now)
    if a is None or b is None or not finite(short.get("prev")) or not finite(long.get("prev")):
        return None
    if short.get("as_of") != long.get("as_of") or not short.get("prev_date") or short.get("prev_date") != long.get("prev_date"):
        return None
    try:
        if not 0 < (date.fromisoformat(short["as_of"]) - date.fromisoformat(short["prev_date"])).days <= 7:
            return None
    except (TypeError, ValueError): return None
    ds, dl = a - short["prev"], b - long["prev"]
    change = dl - ds
    if abs(change) < 1e-9: return "parallel_or_unchanged"
    shape = "steepener" if change > 0 else "flattener"
    direction = "bull" if ds <= 0 and dl <= 0 else "bear" if ds >= 0 and dl >= 0 else "twist"
    return direction + "_" + shape


def diagnostics(hot, now=None):
    """Recomputable heuristics; distinct from validated forecasts or trade calls."""
    v = lambda sid: usable(hot, sid, now)
    flags = {"_inputs": {}, "input_evidence": {}, "contract_version": "canary-diagnostics.v2", "sizing_eligible": False,
             "authority": "research_only", "notes": {
                 "reserve_share_of_fed_liabilities": "Legacy key: reserves / (assets - TGA - RRP), not a share of total Fed liabilities; mixed weekly-average and point-in-time observations.",
                 "reserve_regime": "Unvalidated heuristic thresholds on reserves / annualized GDP, not a test of reserve adequacy.",
                 "windows": "Trailing 60 differences and 252 observations, not necessarily calendar or trading days.",
                 "curve_regime": "Change in matched-date 2y/10y yields and slope; inversion is reported separately."}}
    def put(name, value, ids):
        flags[name] = value
        flags["_inputs"][name] = ids
        flags["input_evidence"][name] = [{"series_id": sid, "trace_id": (hot.get(sid) or {}).get("trace_id"),
                                    "observation_date": (hot.get(sid) or {}).get("as_of"),
                                    "unit": (hot.get(sid) or {}).get("unit")} for sid in ids]
    def z(sid):
        row = hot.get(sid) or {}
        value, mean, sd = v(sid), row.get("mean252"), row.get("std252")
        return round((value - mean) / sd, 2) if value is not None and finite(mean) and finite(sd) and sd > 0 else None
    sahm, slope, claims = v("SAHMREALTIME"), v("T10Y3M"), v("IC4WSA")
    previous = (hot.get("IC4WSA") or {}).get("prev")
    try:
        claims_paired = (date.fromisoformat(hot["IC4WSA"]["as_of"]) - date.fromisoformat(hot["IC4WSA"]["prev_date"])).days == 7
    except (KeyError, TypeError, ValueError): claims_paired = False
    put("sahm_triggered", sahm >= .5 if sahm is not None else None, ["SAHMREALTIME"])
    flags["sahm_value"] = sahm
    put("curve_10y3m_inverted", slope < 0 if slope is not None else None, ["T10Y3M"])
    put("claims_4wk_rising", claims > previous if claims is not None and finite(previous) and claims_paired else None, ["IC4WSA"])
    sofr, iorb = v("SOFR"), v("IORB")
    aligned = (hot.get("SOFR") or {}).get("as_of") == (hot.get("IORB") or {}).get("as_of")
    put("floor_breach_bp", round((sofr - iorb) * 100, 1) if sofr is not None and iorb is not None and aligned else None, ["SOFR", "IORB"])
    wres, wal, tga, rrp, gdp = [v(s) for s in ("WRESBAL", "WALCL", "WTREGEN", "RRPONTSYD", "GDP")]
    denominator = wal - tga - rrp * 1000 if None not in (wal, tga, rrp) else None
    ratio = round(100 * wres / denominator, 1) if wres is not None and denominator is not None and denominator > 0 else None
    put("reserve_share_of_fed_liabilities", ratio, ["WRESBAL", "WALCL", "WTREGEN", "RRPONTSYD"])
    flags["reserve_composition_proxy_pct"] = ratio
    scarcity = round(100 * (wres / 1000) / gdp, 2) if wres is not None and gdp is not None and gdp > 0 else None
    put("reserve_scarcity_pct", scarcity, ["WRESBAL", "GDP"])
    put("reserve_regime", None if scarcity is None else "ABUNDANT" if scarcity > 12 else "AMPLE" if scarcity >= 10 else "TIGHT" if scarcity >= 8 else "SCARCE", ["WRESBAL", "GDP"])
    real = hot.get("DFII10") or {}
    d1, dm, ds = real.get("d1"), real.get("dmean60"), real.get("dstd60")
    put("real_rate_shock_z", round((d1 - dm) / ds, 2) if v("DFII10") is not None and all(finite(x) for x in (d1, dm, ds)) and ds > 0 else None, ["DFII10"])
    flags["real_rate_level_z"] = z("DFII10")
    put("curve_regime", curve_movement(hot, now), ["DGS2", "DGS10"])
    survey, nfci = v("DRTSCILM"), v("NFCI")
    phase = (None if survey is None or nfci is None else "NEUTRAL_survey_flat" if abs(survey) < .5 else
             "LATE_tightening" if survey > 10 else "MID_complacent" if survey > 0 and nfci < 0 else "EARLY_easing" if survey < -10 else "NEUTRAL")
    put("credit_cycle_phase", phase, ["DRTSCILM", "NFCI"])
    orders, production = z("NEWORDER"), z("INDPRO")
    put("orders_production_gap", round(orders - production, 2) if orders is not None and production is not None else None, ["NEWORDER", "INDPRO"])
    flags["per_flag_status"] = {name: "UNKNOWN" if flags[name] is None else "COMPUTED" for name in flags["_inputs"]}
    flags["status"] = "UNKNOWN" if any(flags[k] is None for k in ("sahm_triggered", "curve_10y3m_inverted", "claims_4wk_rising")) else "COMPUTED"
    return flags
