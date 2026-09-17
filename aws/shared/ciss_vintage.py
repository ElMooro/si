"""Canonical ECB CISS warehouse selection for all stress desks.

Discontinued observations remain historical context. They cannot supply a
current rank, regime or score. Daily and monthly series have separate SLAs.
"""
import math
from datetime import datetime, timedelta, timezone

KEY = "data/ciss-stress.json"


def observation_quality(day, frequency, publication, now=None):
    clock = now or datetime.now(timezone.utc)
    status = "unavailable"
    if day:
        try:
            date = datetime.fromisoformat(day[:10] if len(day) >= 10 else day + "-01").date()
            age = (clock.date() - date).days
            limit = {"D": 14, "W": 21, "M": 95, "Q": 180}.get(frequency, 14)
            status = "invalid" if age < 0 else "stale" if age > limit else "fresh"
        except (ValueError, TypeError):
            status = "invalid"
    return {"observation_date": day, "publication_date": publication,
            "frequency": {"D": "daily", "W": "weekly", "M": "monthly", "Q": "quarterly"}.get(frequency, "daily"),
            "freshness_basis": "observation_with_frequency_SLA", "status": status,
            "missing": [] if status == "fresh" else ["current_observation"]}


def finite(value):
    return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value)


def select_series(doc, area, sovereign=False, now=None):
    """Return current points (newest first), quality and source row.

    Select maintained N variants, with GDP-weighted EA SovCISS. The warehouse
    supplies every desk's values; consumers never fall back to frozen SDMX keys.
    """
    doc = doc if isinstance(doc, dict) else {}
    clock = now or datetime.now(timezone.utc)
    prefix = "SOV_GDPW" if sovereign and area == "U2" else "SOV_CI" if sovereign else "SS_CI"
    candidates = []
    for row in doc.get("series") or []:
        parts = str(row.get("key", "")).split(".")
        if row.get("area") == area and len(parts) >= 3 and parts[-2] in (prefix, prefix + "N"):
            candidates.append(row)
    row = max(candidates, key=lambda r: (r.get("latest_date") or "", str(r.get("key", "")).endswith(prefix + "N.IDX")), default={})
    q = observation_quality(row.get("latest_date"), row.get("freq", "D"), doc.get("generated_at"), clock)
    try:
        published = datetime.fromisoformat(doc["generated_at"].replace("Z", "+00:00"))
        age = (clock - published).total_seconds()
        if not -300 <= age <= 72 * 3600:
            q.update(status="stale" if age > 0 else "invalid", missing=["current_warehouse_publication"])
    except (ValueError, TypeError, KeyError, AttributeError):
        q.update(status="unavailable", missing=["warehouse_publication"])
    if row.get("discontinued"):
        q.update(status="stale", missing=["maintained_series"])
    if row and (not finite(row.get("latest")) or not 0 <= row["latest"] <= 1):
        q.update(status="invalid", missing=["valid_index_value"])
    q.update(warehouse_key=KEY, source_key=row.get("key"))
    points = []
    if q["status"] == "fresh":
        for day, value in row.get("points") or []:
            if finite(value) and day <= row["latest_date"]:
                points.append((day, value))
        # Downsampling must never lose or replace the exact latest observation.
        points = [(d, v) for d, v in points if d != row["latest_date"]]
        points.append((row["latest_date"], row["latest"]))
    return sorted(points, reverse=True), q, row


def window_percentile(points, years, now=None):
    if not points:
        return None
    cutoff = ((now or datetime.now(timezone.utc)) - timedelta(days=365.25 * years)).date().isoformat()
    vals = [v for d, v in points if d >= cutoff and finite(v)]
    return round(100 * sum(v <= points[0][1] for v in vals) / len(vals), 1) if vals else None
