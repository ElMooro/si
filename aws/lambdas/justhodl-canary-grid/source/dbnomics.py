"""
dbnomics.py — DBnomics fetcher (free, no API key).

Gives the platform programmatic access to global statistics that FRED does
not carry — BOJ, SNB, Eurostat, IMF, OECD and national statistics offices.

Phase 1 of the Canary Grid runs entirely on FRED (the platform's own
source). This module is bundled and reserved for Phase 3, where it supplies
the genuine FRED-gaps: Taiwan export orders, the Swiss KOF barometer, and
Chile / Peru copper production.

API: https://api.db.nomics.world  — series addressed PROVIDER/DATASET/SERIES.
"""
import json
import urllib.parse
import urllib.request

API = "https://api.db.nomics.world/v22"


def fetch_series(series_id, timeout=25):
    """series_id = 'PROVIDER/DATASET/SERIES'.
    Returns [(period, value|None), ...] oldest-first, or [] on any failure."""
    try:
        url = f"{API}/series/{urllib.parse.quote(series_id)}?observations=1"
        with urllib.request.urlopen(url, timeout=timeout) as r:
            data = json.loads(r.read())
        docs = ((data.get("series") or {}).get("docs")) or []
        if not docs:
            return []
        doc = docs[0]
        periods = doc.get("period") or []
        values = doc.get("value") or []
        out = []
        for p, v in zip(periods, values):
            try:
                fv = float(v)
            except (TypeError, ValueError):
                fv = None
            out.append((p, fv))
        return out
    except Exception as e:  # noqa: BLE001
        print(f"[dbnomics] {series_id}: {e}")
        return []


def latest(series_id):
    """Return (period, value) of the most recent non-null observation."""
    pts = [x for x in fetch_series(series_id) if x[1] is not None]
    return pts[-1] if pts else (None, None)


def yoy(series_id, periods=12):
    """Year-over-year % change of the latest observation, or None."""
    pts = [x for x in fetch_series(series_id) if x[1] is not None]
    if len(pts) <= periods:
        return None
    cur = pts[-1][1]
    prior = pts[-1 - periods][1]
    if prior in (0, None):
        return None
    return cur / prior - 1.0
