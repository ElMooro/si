"""Descriptive BTC market-cap extension; never Market Value / Realized Value.

The complete parsed provider document is retained. This is not a claim to
original HTTP bytes, first-release vintages, source timing or investment edge.
No network, account, model or storage operations occur in this module.
"""
from copy import deepcopy
from datetime import datetime, timezone
import hashlib
import json
import math

CONTRACT = "btc-market-cap-to-returned-mean.v1"
PERMISSIONS = dict(calls_eligible=False, sizing_eligible=False,
                   execution_eligible=False, forecast_qualified=False,
                   independent_investment_votes=0)


def finite_number(value, minimum=None):
    if type(value) not in (int, float):
        return None
    try:
        if not math.isfinite(value) or minimum is not None and value < minimum:
            return None
    except (OverflowError, ValueError):
        return None
    return value


def build_market_cap_extension(document):
    # A string preserves even a permissively parsed NaN token without emitting
    # invalid numbers in the public JSON packet. It is explicitly not HTTP bytes.
    try:
        source_text = json.dumps(document, ensure_ascii=True, allow_nan=True,
                                 separators=(",", ":"))
    except (TypeError, ValueError, OverflowError):
        source_text = None
    out = {
        "status": "unavailable", "mvrv_approx": None, "signal": "UNAVAILABLE",
        "market_cap": None, "market_cap_fmt": None, "momentum_30d": None,
        "mvrv_status": "unavailable_no_realized_capitalization",
        "legacy_momentum_status": "unavailable_observation_window_is_not_30_calendar_days",
        "source_response_json": source_text,
        "source_response_kind": "complete_python_json_serialization_of_parsed_response_not_original_http_bytes",
        "source_response_sha256": hashlib.sha256(source_text.encode()).hexdigest() if source_text is not None else None,
        "market_cap_extension": {
            "contract": CONTRACT, "status": "unavailable", "value": None,
            "unit": "ratio", "series_id": "blockchain.info:market-cap",
            "source_url": "https://api.blockchain.info/charts/market-cap?timespan=365days&format=json",
            "requested_timespan": "365days", "expected_source_unit": "usd", "source_unit": None,
            "numerator": None, "denominator": None,
            "observation_window": None, "returned_rows": None,
            "reason": "unvalidated_source_response",
            "observation_freshness_verified": False,
            "source_definition_verified": False,
            "first_release_availability_verified": False,
            **PERMISSIONS,
        },
        **PERMISSIONS,
    }
    measurement = out["market_cap_extension"]

    def unavailable(reason):
        measurement["reason"] = reason
        return out

    if source_text is None:
        return unavailable("unserializable_parsed_response")
    if not isinstance(document, dict) or not isinstance(document.get("values"), list):
        return unavailable("missing_complete_values_array")
    if document.get("unit") != "USD":
        return unavailable("source_unit_missing_or_not_usd")
    measurement["source_unit"] = "usd"
    rows = document["values"]
    measurement["returned_rows"] = len(rows)
    if len(rows) < 30:
        return unavailable("fewer_than_30_returned_observations")
    values, dates = [], []
    previous = None
    for row in rows:
        if not isinstance(row, dict):
            return unavailable("invalid_observation_object")
        clock, value = row.get("x"), finite_number(row.get("y"), 0)
        if type(clock) is not int or clock < 0 or value is None:
            return unavailable("invalid_observation_clock_or_value")
        if previous is not None and clock <= previous:
            return unavailable("non_increasing_or_duplicate_observation_clock")
        try:
            date = datetime.fromtimestamp(clock, timezone.utc).isoformat()
        except (OverflowError, OSError, ValueError):
            return unavailable("unsupported_observation_clock")
        values.append(value); dates.append(date); previous = clock
    try:
        mean = math.fsum(values) / len(values)
        if finite_number(mean, 0) is None or mean == 0:
            return unavailable("unavailable_nonzero_denominator")
        ratio = values[-1] / mean
        if finite_number(ratio, 0) is None or values[-1] > 0 and ratio == 0:
            return unavailable("unrepresentable_ratio")
    except (OverflowError, ValueError):
        return unavailable("unrepresentable_arithmetic")
    measurement.update(
        status="descriptive", value=ratio, reason="not_mvrv_no_realized_capitalization",
        numerator={"source_row": "/values/" + str(len(rows)-1),
                   "observation_time": dates[-1], "value_usd": values[-1]},
        denominator={"method": "arithmetic_mean_of_all_returned_observations",
                     "source_rows": "/values", "count": len(rows), "value_usd": mean},
        observation_window={"first": dates[0], "last": dates[-1],
                            "regular_daily_sampling_verified": False},
        arithmetic="fsum(values.y) / returned_count; latest_y / mean; binary64 display projection",
    )
    out.update(status="descriptive", market_cap=values[-1])
    return out


def reported_extension(ratios):
    """Only project the explicit descriptive contract; no legacy MVRV fallback."""
    if not isinstance(ratios, dict):
        return None
    measurement = ratios.get("market_cap_extension")
    if not isinstance(measurement, dict) or measurement.get("contract") != CONTRACT:
        return None
    if measurement.get("status") != "descriptive" or measurement.get("unit") != "ratio":
        return None
    if any(measurement.get(key) != value or type(measurement.get(key)) is not type(value)
           for key, value in PERMISSIONS.items()):
        return None
    return finite_number(measurement.get("value"), 0)


def apply_crypto_proxy_boundary(output, ratios):
    """A historical mean is not a qualified MVRV vote or a neutral substitute."""
    out = deepcopy(output)
    previous_quality = out.get("quality")
    factor = (out.get("factors") or {}).get("mvrv_extension")
    if isinstance(factor, dict):
        out["unqualified_legacy_mvrv_factor"] = deepcopy(factor)
        factor.update(risk=None, mvrv=None, status="unqualified_historical_mean_proxy",
                      note="Realized capitalization is absent. No MVRV valuation vote is available.")
    out["unqualified_legacy_methodology"] = out.get("methodology")
    out["unqualified_legacy_honesty_note"] = out.get("honesty_note")
    out.update(
        dump_risk_score=None, risk_level="UNAVAILABLE",
        action="WAIT — MVRV requires realized capitalization; the historical-mean proxy cannot vote.",
        top_drivers=[], decision={"verb": "WAIT", "meaning": "abstain"},
        quality={"status": "unqualified_required_input", "source": "crypto-mvrv",
                 "preceding_quality": previous_quality},
        methodology="Descriptive factor context only. No composite or investment action is qualified.",
        honesty_note="Historical co-occurrence does not establish causal or predictive validity. Source definitions, observation timing, independent evidence and out-of-sample validation remain unverified.",
        reported_market_cap_to_mean_ratio=reported_extension(ratios),
        **PERMISSIONS,
    )
    return out
