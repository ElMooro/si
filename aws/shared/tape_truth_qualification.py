"""Qualification only for tape-truth and its industry-case display projection.

No feed I/O, calendar, SLA, directional inference or measurement arithmetic.
Dates identify the existing ledger/file keys, not verified exchange timestamps.
"""
from copy import deepcopy
from datetime import date, datetime, timezone
import re

CONTRACT = "tape-truth-observations.v2"
QUALIFICATION = "tape-truth-qualification.v1"
PROJECTION = "tape-truth-projection.v1"
REASON = "Bar proxies, short-sale volume and assumed dealer positions do not establish aggressor direction or participant intent."


def clock(value, kind, now):
    result = {"value": deepcopy(value), "kind": kind, "status": "MISSING"}
    if value is None:
        return result
    result["status"] = "INVALID"
    if not isinstance(value, str):
        return result
    try:
        if kind == "date":
            if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", value):
                return result
            parsed = date.fromisoformat(value)
            future = parsed > now.astimezone(timezone.utc).date()
        else:
            if not re.fullmatch(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?(?:Z|[+-]\d{2}:\d{2})", value):
                return result
            if value[-1] != "Z" and (int(value[-5:-3]) > 23 or int(value[-2:]) > 59):
                return result
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
            future = parsed > now
        result["status"] = "FUTURE" if future else "VALID"
    except (ValueError, OverflowError):
        pass
    return result


def withheld():
    return {"call": None, "conviction": None, "status": "WITHHELD", "why": REASON}


def qualification():
    return {"contract": QUALIFICATION, "status": "WITHHELD", "calls_eligible": False,
            "sizing_eligible": False, "freshness": "UNKNOWN",
            "freshness_reason": "NO_AUTHORITATIVE_OBSERVATION_SLA", "why": REASON}


def observation_clocks(symbol, now):
    def leg(key):
        value = symbol.get(key)
        return value if isinstance(value, dict) else {}
    return {
        "cvd": {**clock(leg("cvd").get("last_day"), "date", now),
                "basis": "requested session ledger key; underlying bar times not verified"},
        "short_vol": {**clock(leg("short_vol").get("observation_date"), "date", now),
                      "basis": "requested FINRA filename date retained in ledger"},
        "gex": {**clock(leg("gex").get("source_timestamp"), "timestamp", now),
                "basis": "CBOE response timestamp; individual option/OI observation times and feed delay unknown"},
    }


def qualify_symbol(symbol, now):
    result = deepcopy(symbol)
    # Calculation availability was historically labelled LIVE. It is not freshness.
    for key in ("cvd", "gex"):
        if isinstance(result.get(key), dict) and result[key].get("status") == "LIVE":
            result[key]["status"] = "OBSERVATIONS"
    result["verdict"] = withheld()
    result["qualification"] = qualification()
    result["observation_clocks"] = observation_clocks(result, now)
    return result


def qualified(value):
    return (isinstance(value, dict) and value.get("contract") == QUALIFICATION
            and value.get("status") == "WITHHELD" and value.get("calls_eligible") is False
            and value.get("sizing_eligible") is False and value.get("freshness") == "UNKNOWN")


def observation_shape(value):
    """Null/missing legs are explicit gaps; scalar/array legs are not records."""
    return isinstance(value, dict) and all(
        value.get(key) is None or isinstance(value.get(key), dict)
        for key in ("cvd", "short_vol", "gex"))


def project_tape(packet, ticker, now):
    """Retain source observations; a new projection cannot refresh their clocks."""
    packet = packet if isinstance(packet, dict) else {}
    source_time = packet.get("generated_at")
    publication = clock(source_time, "timestamp", now)
    out = {"contract": PROJECTION, "availability": "UNAVAILABLE",
           "source_key": "data/tape-truth.json", "source_generated_at": deepcopy(source_time),
           "source_publication_clock": publication, "projected_at": now.isoformat(),
           "qualification": qualification(), "call": None, "conviction": None,
           "observations": None, "why": "Missing or unknown observation contract/publication clock."}
    symbols = packet.get("symbols")
    symbol = symbols.get(ticker) if isinstance(symbols, dict) else None
    if (packet.get("measurement_contract") != CONTRACT or packet.get("status") != "RESEARCH"
            or not qualified(packet.get("qualification"))
            or publication["status"] != "VALID" or not isinstance(symbol, dict)
            or not qualified(symbol.get("qualification")) or not observation_shape(symbol)):
        return out
    out.update({"availability": "AVAILABLE", "observations": {
        key: deepcopy(symbol.get(key)) for key in ("cvd", "short_vol", "gex")},
        "observation_clocks": observation_clocks(symbol, now), "why": REASON})
    return out
