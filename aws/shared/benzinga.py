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


# ---------------------------------------------------------------------------
# Benzinga ratings / guidance / analyst-insights (entitled via Massive add-on).
# ---------------------------------------------------------------------------
_RATING_RANK = {
    "strong sell": 0, "sell": 1, "underperform": 1, "underweight": 1, "reduce": 1,
    "negative": 1, "neutral": 2, "hold": 2, "market perform": 2, "sector perform": 2,
    "equal-weight": 2, "equal weight": 2, "in-line": 2, "peer perform": 2,
    "buy": 3, "outperform": 3, "overweight": 3, "accumulate": 3, "positive": 3,
    "sector outperform": 3, "add": 3, "strong buy": 4, "conviction buy": 4,
}


def _rank(rating):
    return _RATING_RANK.get((rating or "").strip().lower())


def _back_window(days_back):
    from datetime import timedelta
    return (date.today() - timedelta(days=days_back)).isoformat()


def fetch_ratings(days_back=7, min_importance=0, limit=1000):
    """Recent analyst rating actions with price-target changes. Returns normalized
    rows: rating transition direction + PT direction + % PT change."""
    j = _get("ratings", {"date.gte": _back_window(days_back), "order": "desc",
                         "sort": "date", "limit": str(limit)})
    out = []
    for r in (j or {}).get("results", []) or []:
        imp = r.get("importance") or 0
        if imp < min_importance:
            continue
        rnew, rprev = _rank(r.get("rating")), _rank(r.get("previous_rating"))
        if rnew is not None and rprev is not None and rnew != rprev:
            rdir = "UPGRADE" if rnew > rprev else "DOWNGRADE"
        elif (r.get("rating_action") or "").lower() in ("upgrades",):
            rdir = "UPGRADE"
        elif (r.get("rating_action") or "").lower() in ("downgrades",):
            rdir = "DOWNGRADE"
        else:
            rdir = "REITERATE"
        pta = (r.get("price_target_action") or "").lower()
        pt_dir = ("RAISE" if pta == "raises" else "CUT" if pta == "lowers"
                  else "INIT" if pta in ("announces", "initiates") else "AFFIRM")
        out.append({
            "ticker": r.get("ticker"), "company": r.get("company_name"),
            "firm": r.get("firm"), "analyst": r.get("analyst"),
            "date": r.get("date"), "time": r.get("time"), "importance": imp,
            "rating": r.get("rating"), "previous_rating": r.get("previous_rating"),
            "rating_dir": rdir, "rating_action": r.get("rating_action"),
            "pt": r.get("price_target"), "pt_prev": r.get("previous_price_target"),
            "pt_pct": r.get("price_percent_change"), "pt_dir": pt_dir,
        })
    return out


def _mid(lo, hi):
    vals = [v for v in (lo, hi) if isinstance(v, (int, float))]
    return sum(vals) / len(vals) if vals else None


def fetch_guidance(days_back=21, min_importance=0, limit=1000):
    """Recent company guidance with raise/cut detection vs previous guidance,
    for both EPS and revenue (midpoint comparison)."""
    j = _get("guidance", {"date.gte": _back_window(days_back), "order": "desc",
                         "sort": "date", "limit": str(limit)})
    out = []
    for r in (j or {}).get("results", []) or []:
        imp = r.get("importance") or 0
        if imp < min_importance:
            continue
        eps_now = _mid(r.get("min_eps_guidance"), r.get("max_eps_guidance"))
        eps_prev = _mid(r.get("previous_min_eps_guidance"), r.get("previous_max_eps_guidance"))
        rev_now = _mid(r.get("min_revenue_guidance"), r.get("max_revenue_guidance"))
        rev_prev = _mid(r.get("previous_min_revenue_guidance"), r.get("previous_max_revenue_guidance"))

        def _dir(now, prev):
            if now is None:
                return None
            if prev is None:
                return "INIT"
            if now > prev * 1.001:
                return "RAISE"
            if now < prev * 0.999:
                return "CUT"
            return "AFFIRM"

        eps_dir, rev_dir = _dir(eps_now, eps_prev), _dir(rev_now, rev_prev)
        eps_pct = ((eps_now / eps_prev - 1) * 100) if (eps_now and eps_prev) else None
        rev_pct = ((rev_now / rev_prev - 1) * 100) if (rev_now and rev_prev) else None
        # overall: a raise on either line (no cut on the other) is bullish
        dirs = [d for d in (eps_dir, rev_dir) if d in ("RAISE", "CUT")]
        overall = ("RAISE" if "RAISE" in dirs and "CUT" not in dirs else
                   "CUT" if "CUT" in dirs and "RAISE" not in dirs else
                   "MIXED" if dirs else (eps_dir or rev_dir or "AFFIRM"))
        out.append({
            "ticker": r.get("ticker"), "company": r.get("company_name"),
            "fiscal_period": r.get("fiscal_period"), "fiscal_year": r.get("fiscal_year"),
            "date": r.get("date"), "importance": imp, "release_type": r.get("release_type"),
            "eps_mid": eps_now, "eps_prev": eps_prev, "eps_dir": eps_dir, "eps_pct": eps_pct,
            "rev_mid": rev_now, "rev_prev": rev_prev, "rev_dir": rev_dir, "rev_pct": rev_pct,
            "overall_dir": overall,
        })
    return out


def fetch_analyst_insights(days_back=7, limit=500):
    """Analyst commentary rows carrying numeric price target + rating."""
    j = _get("analyst-insights", {"date.gte": _back_window(days_back), "order": "desc",
                                  "sort": "date", "limit": str(limit)})
    out = []
    for r in (j or {}).get("results", []) or []:
        out.append({
            "ticker": r.get("ticker"), "company": r.get("company_name"),
            "firm": r.get("firm"), "date": r.get("date"),
            "rating": r.get("rating"), "rating_action": r.get("rating_action"),
            "price_target": r.get("price_target"), "insight": r.get("insight"),
        })
    return out
