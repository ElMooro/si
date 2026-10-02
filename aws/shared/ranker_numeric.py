"""Typed research-score arithmetic. Availability is not investment qualification.

The valid legacy normalization, convergence formula and rounding are retained.
In particular, positive normalized components with zero weight still enter the
legacy convergence denominator. This contract does not assert independence.
"""
import json
import math
import re
from decimal import Decimal

CONTRACT = "ranker-numeric.v1"


class InvalidNumber(ValueError):
    pass


def number(value, *, nonnegative=False):
    if type(value) not in (int, float, str) or isinstance(value, str) and not value.strip():
        raise InvalidNumber("finite_number_required")
    try:
        result = float(value)
    except (ValueError, OverflowError):
        raise InvalidNumber("finite_number_required") from None
    if not math.isfinite(result) or nonnegative and result < 0:
        raise InvalidNumber("nonnegative_finite_required" if nonnegative else "finite_number_required")
    if isinstance(value, str) and result == 0 and Decimal(value) != 0:
        raise InvalidNumber("numeric_underflow")
    return result


def product(left, right):
    result = number(left * right, nonnegative=True)
    if left != 0 and right != 0 and result == 0:
        raise InvalidNumber("numeric_underflow")
    return result


def normalized(system, data):
    if not isinstance(data, dict):
        raise InvalidNumber("component_object_required")
    raw = number(data.get("score"))
    if system == "compound":
        count = data.get("n_systems", 1)
        if type(count) is not int or count < 0:
            raise InvalidNumber("compound_count_nonnegative_integer_required")
    if raw >= 100:
        return 100
    if raw < 0:
        return 0
    if system == "compound":
        return min(100, math.log10(max(raw, 1)) * 25 + count * 10)
    return raw


class Calibration(dict):
    def __init__(self, weights, *, available=True, reasons=(), pages=0):
        super().__init__(weights)
        self.diagnostics = {"available": available, "reasons": list(reasons),
                            "pages_read": pages, "default_weight": 1.0,
                            "default_basis": "uncalibrated_legacy_assumption",
                            "investment_qualified": False}


def load_weights(client, path, eligible):
    """Read every page; never turn an invalid present weight into a flat default.

    A failed/incomplete census is unavailable as a whole. Duplicate basenames
    and invalid individual weights are null, excluding their affected rows.
    Diagnostic reasons contain no service responses, parameter values or errors.
    """
    weights, reasons, seen = {}, [], set()
    token = None
    pages = 0
    try:
        while True:
            args = {"Path": path, "Recursive": True, "WithDecryption": False}
            if token is not None:
                args["NextToken"] = token
            packet = client.get_parameters_by_path(**args)
            pages += 1
            if not isinstance(packet, dict) or not isinstance(packet.get("Parameters"), list):
                raise InvalidNumber("calibration_response_invalid")
            for row in packet["Parameters"]:
                name = row.get("Name") if isinstance(row, dict) else None
                if not isinstance(name, str) or not name.startswith(path.rstrip("/") + "/"):
                    raise InvalidNumber("calibration_name_invalid")
                name = name.rsplit("/", 1)[-1]
                if not re.fullmatch(r"[A-Za-z0-9_-]+", name):
                    raise InvalidNumber("calibration_name_invalid")
                if name in weights:
                    weights[name] = None
                    reasons.append({"system": name, "reason": "duplicate_calibration_name"})
                    continue
                try:
                    weights[name] = number(row.get("Value"), nonnegative=True)
                except InvalidNumber:
                    weights[name] = None
                    reasons.append({"system": name, "reason": "invalid_calibration_weight"})
            token = packet.get("NextToken")
            if token is None:
                break
            if not isinstance(token, str) or not token or token in seen or pages >= 1000:
                raise InvalidNumber("calibration_pagination_incomplete")
            seen.add(token)
        return Calibration(eligible(weights), reasons=reasons, pages=pages)
    except Exception as exc:
        reason = str(exc) if isinstance(exc, InvalidNumber) else "calibration_read_failed"
        return Calibration({}, available=False, reasons=[{"reason": reason}], pages=pages)


def evidence(systems, weights, trust=None):
    out = {"contract": CONTRACT, "status": "unavailable", "score": None,
           "components": [], "contributions": [], "reasons": [],
           "independent_evidence_qualified": False, "portfolio_qualified": False}
    if not isinstance(systems, dict) or not systems:
        out["reasons"].append({"reason": "no_components"})
        return out
    if not isinstance(weights, dict) or getattr(weights, "diagnostics", {}).get("available") is False:
        out["reasons"].append({"reason": "calibration_unavailable"})
        return out
    total = 0.0
    for system, data in systems.items():
        component = {"system": system, "status": "unavailable"}
        out["components"].append(component)
        field = "score"
        try:
            # The actual index builder includes these two descriptive records
            # without a score by design. Keep them visible without manufacturing
            # a zero contribution or an independent numerical vote.
            if system in ("insider", "fmp_ratios") and isinstance(data, dict) and "score" not in data:
                field = "details"
                try:
                    json.dumps(data, allow_nan=False)
                except (ValueError, TypeError, OverflowError):
                    raise InvalidNumber("component_details_not_json_safe") from None
                component.update(status="context_only", reason="no_score_definition",
                                 normalized=None, contribution=None)
                continue
            value = normalized(system, data)
            # Prevent invalid numbers hidden in otherwise valid component details
            # from reaching the published row. No lossy null substitution.
            field = "details"
            try:
                json.dumps(data, allow_nan=False)
            except (ValueError, TypeError, OverflowError):
                raise InvalidNumber("component_details_not_json_safe") from None
            field = "calibration_weight"
            cal = number(weights.get(system, 1.0), nonnegative=True)
            field = "trust_multiplier"
            try:
                tm = number(trust(system) if trust else 1.0, nonnegative=True)
            except Exception:
                raise InvalidNumber("trust_unavailable") from None
            field = "effective_weight"
            weight = product(cal, tm)
            field = "contribution"
            contribution = product(value, weight)
            field = "total"
            total = number(total + contribution, nonnegative=True)
            component.update(status="usable", raw_score=data["score"], normalized=value,
                             calibration_weight=cal, trust_mult=tm, weight=weight,
                             calibration_origin="configured" if system in weights else "flat_default",
                             trust_origin="resolver" if trust else "flat_default", contribution=contribution)
            if system == "compound":
                component.update(compound_count=data.get("n_systems", 1),
                                 compound_count_origin="reported" if "n_systems" in data else "legacy_default")
            if value > 0:
                out["contributions"].append({"system": system, "raw_score": data["score"],
                    "normalized": round(value, 1), "weight": round(weight, 2),
                    "trust_mult": round(tm, 2), "contribution": round(contribution, 1)})
        except (InvalidNumber, OverflowError) as exc:
            reason = str(exc) if isinstance(exc, InvalidNumber) else "arithmetic_overflow"
            component["reason"] = reason
            component["invalid_field"] = field
            out["reasons"].append({"system": system, "field": field, "reason": reason})
    if not any(row["status"] == "usable" for row in out["components"]):
        out["reasons"].append({"reason": "no_scored_components"})
    if out["reasons"]:
        return out
    active = len(out["contributions"])
    multiplier = 1.0 + 0.4 * math.log(max(active, 1))
    try:
        final = number(total * multiplier / max(active, 1), nonnegative=True)
    except InvalidNumber:
        out["reasons"].append({"reason": "arithmetic_overflow"})
        return out
    out.update(status="usable", score=round(final, 1), sum_contributions=total,
               legacy_active_components=active, convergence_multiplier=multiplier,
               denominator=max(active, 1), unrounded_score=final, rounding_digits=1)
    return out


def adjustment(trace, name, result):
    score, multiplier, note = result
    if type(score) not in (int, float) or type(multiplier) not in (int, float):
        raise InvalidNumber("numeric_adjustment_required")
    number(score, nonnegative=True)
    number(multiplier, nonnegative=True)
    previous = trace["adjustments"][-1]["after"] if trace["adjustments"] else trace["base"]["score"]
    trace["adjustments"].append({"stage": name, "before": previous,
                                 "after": score, "multiplier": multiplier})
    return score, multiplier, note


def unavailable(ticker, base, *, reason=None):
    reasons = list(base.get("reasons", []))
    if reason:
        reasons.append({"reason": reason})
    return {"ticker": ticker, "score": None, "status": "unavailable",
            "reasons": reasons, "score_calculation": base}
