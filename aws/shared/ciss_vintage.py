"""Canonical ECB CISS warehouse selection for all stress desks.

Discontinued observations remain historical context. They cannot supply a
current rank, regime or score. Daily and monthly series have separate SLAs.
"""
import calendar
import math
from datetime import date, datetime, timedelta, timezone
from ciss_source_model import CONTRACT, digest, clock as source_clock, row_is_current

KEY = "data/ciss-stress.json"


def observation_quality(day, frequency, publication, now=None):
    clock = now or datetime.now(timezone.utc)
    status = "unavailable"
    if day:
        try:
            observed = date.fromisoformat(day[:10] if len(day) >= 10 else day + "-01")
            if frequency == 'M':observed=observed.replace(day=calendar.monthrange(observed.year,observed.month)[1])
            age = (clock.date() - observed).days
            limit = {"D": 14, "W": 21, "M": 95, "Q": 180}.get(frequency, 14)
            status = "invalid" if age < 0 else "stale" if age > limit else "fresh"
        except (ValueError, TypeError):
            status = "invalid"
    return {"observation_date": day, "publication_date": None, "warehouse_generated_at": publication,
            "period_end":observed.isoformat() if day and status!='invalid' else None,
            "frequency": {"D": "daily", "W": "weekly", "M": "monthly", "Q": "quarterly"}.get(frequency, "daily"),
            "freshness_basis": "completed_observation_period_with_frequency_ceiling; exact release calendar unverified", "status": status,
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
    # The maintained N methodology is explicit. A missing N observation must not
    # cause a fallback to a different, obsolete methodology with a numeric value.
    row = max(candidates, key=lambda r: (str(r.get("key", "")).endswith(prefix + "N.IDX"), r.get("latest_date") or ""), default={})
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
    try:
        ref=doc['replay']
        if doc.get('contract')!=CONTRACT or digest({k:v for k,v in doc.items() if k!='replay'})!=ref['output_sha256']:
            raise ValueError('canonical packet differs')
        if not str(ref['manifest_key']).startswith('data/ciss-research/runs/') or row.get('unit')!='dimensionless_index':
            raise ValueError('source identity differs')
        rq=row.get('quality',{})
        if rq.get('status')!='fresh':q.update(status=rq.get('status','unavailable'),missing=rq.get('missing') or ['qualified_source_row'])
        elif not row_is_current(row,clock.isoformat()):q.update(status='stale',missing=['current_source_observation_and_acquisition'])
        if q['status']=='fresh' and not 0 <= (clock-source_clock(doc['generated_at'])).total_seconds() <= 72*3600:
            q.update(status='invalid',missing=['current_warehouse_clock'])
        q.update(source_replay=ref,source_acquired_at=row.get('acquired_at'),source_evidence=row.get('evidence'),
                 source_row=row.get('source_row'),unit=row.get('unit'),observation_status=row.get('observation_status'),
                 maximum_observation_age_days=rq.get('maximum_observation_age_days'),
                 maximum_acquisition_age_seconds=rq.get('maximum_acquisition_age_seconds'),
                 source_definition=row.get('source_metadata'),calls_eligible=False,sizing_eligible=False,
                 distribution_scope='within this exact series; not a cross-country crisis probability')
    except (KeyError,TypeError,ValueError):
        q.update(status='unverified',missing=['canonical_source_packet_and_replay'])
    if q['status']=='fresh' and row and (not finite(row.get("latest")) or not 0 <= row["latest"] <= 1):
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


def calendar_comparison(row, months=0, days=0):
    """Use comparisons computed from full source rows, never usable-only points."""
    out={'value':None,'baseline_value':None,'baseline_date':None,'target_date':None,'unit':'index_points'}
    if not row or not finite(row.get('latest')):return out
    name={1:'1m',3:'3m',12:'12m'}.get(months) if months else '1w' if days==7 else None
    comparison=(row.get('comparisons') or {}).get(name)
    if comparison is None and months==12:comparison=row.get('annual_comparison')
    if comparison is None:return out
    return {**out,**comparison,'baseline_date':comparison.get('baseline_period')}


def window_percentile(points, years, now=None):
    if not points:
        return None
    end=(now or datetime.now(timezone.utc)).date()
    cutoff = date(end.year-years,end.month,min(end.day,calendar.monthrange(end.year-years,end.month)[1])).isoformat()
    vals = [v for d, v in points if d >= cutoff and finite(v)]
    return round(100 * sum(v <= points[0][1] for v in vals) / len(vals), 1) if vals else None
