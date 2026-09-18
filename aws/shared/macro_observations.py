"""Explicit macro transformations, calendar alignment, clocks and source meaning.

No row-count inference of frequency; no stale series relabeled as current; no
provider retrieval time presented as the date an economic observation occurred.
"""
import math
import re
from datetime import date, datetime, timezone

YOY_CONTRACT = "fred-calendar-yoy.v1"
CURATED_YOY = {
    "USIRYY": "CPIAUCSL", "JPIRYY": "JPNCPIALLMINMEI", "USGDPYY": "GDPC1",
    "CNIRYY": "CHNCPIALLMINMEI", "DEIRYY": "DEUCPIALLMINMEI", "EUIRYY": "CP0000EZ19M086NEST",
    "USMPRYY": "PPIACO", "USHPIYY": "CSUSHPINSA", "CNGDPYY": "CHNGDPNQDSMEI",
    "JPGDPYY": "JPNRGDPEXP", "EUGDPYY": "CLVMNACSCAB1GQEA19", "FIGDPYY": "CLVMNACSCAB1GQFI",
    "CLGDPYY": "CLVMNACSAB1GQCL", "JPPPIYY": "PITGCG01JPM661N",
    "FIIPYY": "FINPROINDMISMEI", "ESIPYY": "ESPPROINDMISMEI", "ITIPYY": "ITAPROINDMISMEI",
    "CHIPYY": "CHEPROINDMISMEI", "KRIPYY": "KORPROINDMISMEI", "BRIPYY": "BRAPROINDMISMEI",
    "JPEXPYY": "XTEXVA01JPM667N",
}
# Conservative maximum observation age; not a forecast of publication dates.
MAX_OBSERVATION_DAYS = {"M": 100, "Q": 200, "A": 550}


def finite(value):
    if value is None or isinstance(value, bool): return None
    try:
        number = float(value)
        return number if math.isfinite(number) else None
    except (ValueError, TypeError):
        return None


def period_key(day, frequency):
    if frequency == "M": return day.year, day.month
    if frequency == "Q": return day.year, (day.month - 1) // 3 + 1
    if frequency == "A": return day.year, 1
    raise ValueError("unsupported YoY observation frequency")


def calendar_yoy(observations, series, now=None):
    """Compute from one consistent FRED response vintage and matching periods."""
    current = now or datetime.now(timezone.utc)
    if current.tzinfo is None: raise ValueError("calculation time must include timezone")
    sid, frequency = series.get("id"), series.get("frequency_short")
    if not sid or frequency not in MAX_OBSERVATION_DAYS or not series.get("units") or not series.get("seasonal_adjustment"):
        raise ValueError("complete series definition and supported frequency required")
    by_period, dated = {}, []
    for row in observations:
        day = date.fromisoformat(row["date"])
        key, value = period_key(day, frequency), finite(row.get("value"))
        if key in by_period and by_period[key][1] != value:
            raise ValueError("conflicting observations in one calendar period")
        by_period[key] = (day, value, row)
        dated.append((day, key))
    if not dated: raise ValueError("no observations")
    day, latest_key = max(dated)
    age = (current.date() - day).days
    if age < 0: raise ValueError("future observation")
    latest = by_period[latest_key]
    base_key = (latest_key[0] - 1, latest_key[1])
    baseline = by_period.get(base_key)
    if latest[1] is None: raise ValueError("latest observation is missing")
    if baseline is None or baseline[1] is None or baseline[1] <= 0:
        raise ValueError("same period one year earlier is missing or nonpositive")
    value = (latest[1] / baseline[1] - 1) * 100
    if not math.isfinite(value): raise ValueError("nonfinite YoY calculation")
    period_count = {"M": 12, "Q": 4, "A": 1}[frequency]
    previous_key = ((latest_key[0], latest_key[1] - 1) if latest_key[1] > 1
                    else (latest_key[0] - 1, period_count))
    prior, prior_base = by_period.get(previous_key), by_period.get((previous_key[0] - 1, previous_key[1]))
    previous = None
    if prior and prior_base and prior[1] is not None and prior_base[1] is not None and prior_base[1] > 0:
        previous = (prior[1] / prior_base[1] - 1) * 100
    stale = age > MAX_OBSERVATION_DAYS[frequency]
    return {
        "value": round(value, 6), "prev": round(previous, 6) if previous is not None else None,
        "chg_pct": None, "change_pp": round(value - previous, 6) if previous is not None else None,
        "unit": "% YoY", "source_unit": series["units"], "frequency": frequency,
        "seasonal_adjustment": series["seasonal_adjustment"], "series_id": sid,
        "definition": series.get("title") or sid,
        "source_url": "https://fred.stlouisfed.org/series/" + sid,
        "observation_date": day.isoformat(), "comparison_date": baseline[0].isoformat(),
        "asof": day.isoformat() + " YoY", "published_at": None,
        "provider_updated_at": series.get("last_updated"),
        "vintage": {"realtime_start": latest[2].get("realtime_start"),
                    "realtime_end": latest[2].get("realtime_end"),
                    "basis": "provider_response_vintage; not original-publication history"},
        "calculated_at": current.isoformat(), "contract_version": YOY_CONTRACT,
        "calculation": {"formula": "100 * (current / year_earlier - 1)",
                        "current": latest[1], "year_earlier": baseline[1],
                        "calendar_join": frequency, "annualized": False},
        "quality": {"status": "stale" if stale else "fresh", "observation_age_days": age,
                    "max_observation_age_days": MAX_OBSERVATION_DAYS[frequency],
                    "freshness_basis": "observation_age_ceiling; release-calendar validation pending"},
        "status": "STALE" if stale else "LIVE", "sizing_eligible": False,
    }


def valid_yoy_row(row, symbol, now=None, require_evidence=True):
    """Consumers recheck age; an old fresh flag cannot renew an observation."""
    if not isinstance(row, dict): return False
    if (row.get("contract_version") != YOY_CONTRACT or row.get("series_id") != CURATED_YOY.get(symbol)
            or row.get("unit") != "% YoY" or row.get("frequency") not in MAX_OBSERVATION_DAYS
            or finite(row.get("value", row.get("v"))) is None): return False
    try:
        current = now or datetime.now(timezone.utc)
        day = date.fromisoformat(row["observation_date"])
        baseline = date.fromisoformat(row["comparison_date"])
        key, base_key = period_key(day, row["frequency"]), period_key(baseline, row["frequency"])
        if base_key != (key[0] - 1, key[1]): return False
        if not 0 <= (current.date() - day).days <= MAX_OBSERVATION_DAYS[row["frequency"]]: return False
        quality = row.get("quality")
        if not isinstance(quality, dict) or quality.get("status") != "fresh": return False
        value = finite(row.get("value", row.get("v")))
        if "v" in row and finite(row["v"]) != value: return False
        calculation = row.get("calculation")
        if not isinstance(calculation, dict): return False
        numerator, denominator = finite(calculation.get("current")), finite(calculation.get("year_earlier"))
        if numerator is None or denominator is None or denominator <= 0: return False
        if abs(value - 100 * (numerator / denominator - 1)) > 0.000001: return False
        if require_evidence:
            proofs = row["evidence"]
            if not isinstance(proofs, dict) or set(proofs) != {"observations", "definition"}: return False
            for proof in proofs.values():
                if (not isinstance(proof, dict) or proof.get("contract") != "source-evidence.v1" or proof.get("captured") is not True
                        or not re.fullmatch(r"[a-f0-9]{64}", str(proof.get("sha256", ""))) or not proof.get("key")): return False
        return True
    except (ValueError, TypeError, KeyError):
        return False
