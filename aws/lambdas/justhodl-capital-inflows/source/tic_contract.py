"""Monthly TIC transaction alignment; dollar levels are never holdings deltas."""
import math
from datetime import date


def monthly_map(obs):
    result = {}
    for day, value in obs:
        try:
            parsed = date.fromisoformat(day)
        except (ValueError, TypeError):
            return {}
        if parsed.day != 1 or isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or day in result:
            return {}
        result[day] = value
    return result


def align(obs, asof):
    data = monthly_map(obs)
    if asof not in data:
        return []
    return sorted(((d,v) for d,v in data.items() if d <= asof), reverse=True)


def roll(obs, n):
    data = monthly_map(obs)
    days = sorted(data, reverse=True)[:n]
    if len(days) != n:
        return None
    months = [int(d[:4])*12 + int(d[5:7]) for d in days]
    if any(a-b != 1 for a,b in zip(months,months[1:])):
        return None
    return round(sum(data[d] for d in days)/1000.0, 1)


def monthly_quality(series, asof, published, now):
    missing, states = [], {}
    for name, obs in series.items():
        aligned = align(obs, asof)
        state = "fresh" if aligned and roll(aligned,12) is not None else "incomplete"
        if obs and not monthly_map(obs): state = "invalid"
        if not obs: state = "unavailable"
        states[name] = state
        if state != "fresh": missing.append(name)
    age = (now.date()-date.fromisoformat(asof)).days if asof else None
    status = "unavailable" if asof is None else "invalid" if age < 0 else "stale" if age > 95 else "incomplete" if missing else "fresh"
    if "invalid" in states.values(): status = "invalid"
    return {"observation_date": asof, "publication_date": published, "frequency": "monthly",
            "freshness_basis": "monthly_observation_95_day_SLA", "status": status,
            "missing": sorted(missing), "input_status": states,
            "input_latest_dates": {name:max((d for d,_ in obs),default=None) for name,obs in series.items()},
            "vintage_date": now.date().isoformat()}
