"""Benzinga data via Massive (api.polygon.io/benzinga/v1/...).

Reuses the metered Massive key (same key entitles the Benzinga feeds).
Key resolution mirrors massive.py:
  1. MASSIVE_API_KEY env var (CI ops runner)
  2. SSM SecureString /justhodl/massive-api-key (Lambdas at runtime)

Benzinga Earnings schema (REPORTED rows carry actuals + surprises):
  ticker, date, time, date_status, importance(1-5), fiscal_period, fiscal_year,
  actual_eps, estimated_eps, previous_eps, eps_surprise, eps_surprise_percent,
  actual_revenue, estimated_revenue, previous_revenue,
  revenue_surprise, revenue_surprise_percent
NOTE: *_surprise_percent are FRACTIONS (0.0361 == +3.61%).
"""
import os
import json
import urllib.request
import urllib.error
from datetime import date

import boto3

_BASE = "https://api.polygon.io/benzinga/v1"
_CACHE = {}


def _key():
    if _CACHE.get("k") is not None:
        return _CACHE["k"]
    k = os.environ.get("MASSIVE_API_KEY")
    if not k:
        try:
            k = boto3.client("ssm", "us-east-1").get_parameter(
                Name="/justhodl/massive-api-key", WithDecryption=True
            )["Parameter"]["Value"]
        except Exception:
            k = ""
    _CACHE["k"] = k
    return k


def _get(path, params, timeout=15):
    k = _key()
    if not k:
        return None
    qs = "&".join(f"{a}={b}" for a, b in params.items())
    url = f"{_BASE}/{path}?{qs}&apiKey={k}"
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "justhodl-benzinga/1.0"})
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return json.loads(r.read())
    except Exception:
        return None


def _pct(v):
    """Benzinga surprise_percent is a fraction; return percent (3.61) or None."""
    try:
        return round(float(v) * 100.0, 4)
    except (TypeError, ValueError):
        return None


def fetch_recent_reported(ticker, limit=4):
    """Most recent REPORTED quarters (actual+estimate+surprise), newest first."""
    j = _get("earnings", {
        "ticker": ticker,
        "date.lte": date.today().isoformat(),
        "order": "desc",
        "sort": "date",
        "limit": str(limit),
    })
    out = []
    for r in (j or {}).get("results", []) or []:
        # keep only rows that actually reported (have an actual or a surprise)
        if r.get("actual_eps") is None and r.get("eps_surprise_percent") is None:
            continue
        out.append({
            "ticker": r.get("ticker"),
            "date": r.get("date"),
            "time": r.get("time"),
            "date_status": r.get("date_status"),
            "actual_eps": r.get("actual_eps"),
            "estimated_eps": r.get("estimated_eps"),
            "previous_eps": r.get("previous_eps"),
            "eps_surprise_pct": _pct(r.get("eps_surprise_percent")),
            "actual_revenue": r.get("actual_revenue"),
            "estimated_revenue": r.get("estimated_revenue"),
            "revenue_surprise_pct": _pct(r.get("revenue_surprise_percent")),
            "importance": r.get("importance"),
            "fiscal_period": r.get("fiscal_period"),
            "fiscal_year": r.get("fiscal_year"),
        })
    return out


def fetch_upcoming(ticker, limit=2):
    """Next projected/confirmed earnings with estimate + importance, soonest first."""
    j = _get("earnings", {
        "ticker": ticker,
        "date.gte": date.today().isoformat(),
        "order": "asc",
        "sort": "date",
        "limit": str(limit),
    })
    out = []
    for r in (j or {}).get("results", []) or []:
        out.append({
            "ticker": r.get("ticker"),
            "date": r.get("date"),
            "time": r.get("time"),
            "date_status": r.get("date_status"),
            "estimated_eps": r.get("estimated_eps"),
            "previous_eps": r.get("previous_eps"),
            "estimated_revenue": r.get("estimated_revenue"),
            "importance": r.get("importance"),
            "fiscal_period": r.get("fiscal_period"),
            "fiscal_year": r.get("fiscal_year"),
        })
    return out


def fetch_calendar(days_ahead=14, min_importance=0, limit=1000):
    """Market-wide forward earnings calendar (no ticker filter): every company
    reporting in the next N days with consensus estimate + importance + AMC/BMO
    timing. This is the untapped breadth of the Benzinga Earnings feed."""
    from datetime import timedelta
    today = date.today()
    j = _get("earnings", {
        "date.gte": today.isoformat(),
        "date.lte": (today + timedelta(days=days_ahead)).isoformat(),
        "order": "asc",
        "sort": "date",
        "limit": str(limit),
    })
    out = []
    for r in (j or {}).get("results", []) or []:
        imp = r.get("importance") or 0
        if imp < min_importance:
            continue
        t = (r.get("time") or "")
        # Benzinga time HH:MM:SS -> session bucket
        session = "—"
        try:
            hh = int(t.split(":")[0])
            session = "BMO" if hh < 12 else "AMC"
        except Exception:
            pass
        out.append({
            "ticker": r.get("ticker"),
            "company": r.get("company_name"),
            "date": r.get("date"),
            "session": session,
            "date_status": r.get("date_status"),
            "estimated_eps": r.get("estimated_eps"),
            "previous_eps": r.get("previous_eps"),
            "estimated_revenue": r.get("estimated_revenue"),
            "importance": imp,
            "fiscal_period": r.get("fiscal_period"),
            "fiscal_year": r.get("fiscal_year"),
        })
    return out
