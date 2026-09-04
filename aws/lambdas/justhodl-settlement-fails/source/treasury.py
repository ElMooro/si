"""Pure normalization for weekly US Treasury settlement fails.

The normalized scope is deliberately Treasury-only: ex-TIPS plus TIPS on
common dates.  It never uses the producer's all-asset totals.
"""
from __future__ import annotations

import json
import math
from typing import Any


def _number(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        parsed = float(value)
        return parsed if math.isfinite(parsed) else None
    except (TypeError, ValueError):
        return None


def _clean(series: Any) -> dict[str, float]:
    out = {}
    if not isinstance(series, list):
        return out
    for point in series:
        if not isinstance(point, list) or len(point) != 2:
            continue
        value = _number(point[1])
        if not isinstance(point[0], str) or value is None or value < 0:
            continue
        out[point[0]] = value
    return out


def _safe_sum(values: Any) -> float | None:
    try:
        total = math.fsum(values)
    except (OverflowError, TypeError, ValueError):
        return None
    return total if math.isfinite(total) else None


def strict_json_dumps(payload: Any, **kwargs: Any) -> str:
    """Serialize standards-compliant JSON and reject non-finite numbers."""
    return json.dumps(payload, allow_nan=False, **kwargs)


def sum_on_common_dates(left: Any, right: Any) -> list[list[Any]]:
    """Sum two clean date/value series only where both components exist."""
    a, b = _clean(left), _clean(right)
    out = []
    for day in sorted(set(a) & set(b)):
        total = _safe_sum((a[day], b[day]))
        if total is not None:
            out.append([day, round(total, 2)])
    return out


def combine_on_common_dates(ftd: Any, ftr: Any) -> list[list[Any]]:
    """Gross fails = FTD + FTR only where both normalized sides exist."""
    return sum_on_common_dates(ftd, ftr)


def series_stats(points: Any) -> dict:
    clean = [[day, value] for day, value in _clean(points).items()]
    clean.sort(key=lambda point: point[0])
    if not clean:
        return {}
    values = [point[1] for point in clean]
    n = len(values)
    latest = values[-1]
    scale = max(values)
    scaled_mean = min(1.0, math.fsum(value / scale for value in values) / n) if scale else 0.0
    mean = scale * scaled_mean
    normalized_variance = (
        math.fsum(((value - mean) / scale) ** 2 for value in values) / n
        if scale else 0.0
    )
    stddev = scale * min(1.0, normalized_variance) ** 0.5
    z_score = (latest - mean) / stddev if stddev else 0.0
    percentile = sum(value <= latest for value in values) / n * 100.0
    recent = values[-52:]
    recent_scale = max(recent)
    recent_scaled_mean = (
        min(1.0, math.fsum(value / recent_scale for value in recent) / len(recent))
        if recent_scale else 0.0
    )
    recent_mean = recent_scale * recent_scaled_mean
    return {
        "latest": round(latest, 2),
        "mean": round(mean, 2),
        "max": round(max(values), 2),
        "min": round(min(values), 2),
        "z": round(z_score, 2),
        "pctile": round(percentile, 1),
        "avg_52w": round(recent_mean, 2),
        "n_obs": n,
        "start": clean[0][0],
        "as_of": clean[-1][0],
        "spike": bool(z_score >= 2 or percentile >= 95),
    }


def _regime(stats: dict) -> tuple[str, int | None]:
    percentile = _number(stats.get("pctile"))
    z_score = _number(stats.get("z"))
    if percentile is None or z_score is None:
        return "UNKNOWN", None
    if percentile >= 97 or z_score >= 2.5:
        return "CRISIS", round(min(100, max(80, 80 + (percentile - 97) * 6)))
    if percentile >= 90 or z_score >= 1.5:
        return "STRESS", round(min(79, max(60, 60 + (percentile - 90) * 2)))
    if percentile >= 70 or z_score >= 0.7:
        return "ELEVATED", round(min(59, max(40, 40 + (percentile - 70))))
    return "CALM", round(max(0, percentile * 0.5))


def normalized_treasury(classes: Any) -> dict:
    """Build US Treasury including-TIPS metrics from exactly two components."""
    class_rows = {
        row.get("key"): row
        for row in classes
        if isinstance(row, dict) and row.get("key") in {"ust_ex_tips", "tips"}
    } if isinstance(classes, list) else {}
    ex_tips = class_rows.get("ust_ex_tips") or {}
    tips = class_rows.get("tips") or {}

    ftd = sum_on_common_dates(ex_tips.get("ftd"), tips.get("ftd"))
    ftr = sum_on_common_dates(ex_tips.get("ftr"), tips.get("ftr"))
    gross = combine_on_common_dates(ftd, ftr)
    gross_stats = series_stats(gross)
    as_of = gross_stats.get("as_of")
    ftd_by_date, ftr_by_date = _clean(ftd), _clean(ftr)
    component_flags = {
        "ust_ex_tips_ftd": bool(_clean(ex_tips.get("ftd"))),
        "ust_ex_tips_ftr": bool(_clean(ex_tips.get("ftr"))),
        "tips_ftd": bool(_clean(tips.get("ftd"))),
        "tips_ftr": bool(_clean(tips.get("ftr"))),
        "common_ftd_dates": bool(ftd),
        "common_ftr_dates": bool(ftr),
        "common_gross_dates": bool(gross),
    }
    complete = all(component_flags.values()) and as_of is not None
    regime, score = _regime(gross_stats)

    def component(row: dict) -> dict:
        ftd_values, ftr_values = _clean(row.get("ftd")), _clean(row.get("ftr"))
        ftd_value = ftd_values.get(as_of)
        ftr_value = ftr_values.get(as_of)
        gross_value = (
            _safe_sum((ftd_value, ftr_value))
            if ftd_value is not None and ftr_value is not None else None
        )
        return {
            "key": row.get("key"),
            "label": row.get("label"),
            "as_of": as_of,
            "ftd_bn": ftd_value,
            "ftr_bn": ftr_value,
            "gross_bn": round(gross_value, 2) if gross_value is not None else None,
            "complete": gross_value is not None,
        }

    return {
        "scope": "US_TREASURY_INCLUDING_TIPS",
        "scope_note": "ust_ex_tips + tips on common dates; excludes corporate, agency, MBS, and all-asset totals",
        "as_of": as_of,
        "unit": "USD_bn_par",
        "ftd_bn": ftd_by_date.get(as_of),
        "ftr_bn": ftr_by_date.get(as_of),
        "gross_bn": gross_stats.get("latest"),
        "ftd": ftd,
        "ftr": ftr,
        "gross": gross,
        "stats": {
            "ftd": series_stats(ftd),
            "ftr": series_stats(ftr),
            "gross": gross_stats,
        },
        "regime": regime,
        "score": score,
        "components": [component(ex_tips), component(tips)],
        "completeness": component_flags,
        "complete": complete,
    }
