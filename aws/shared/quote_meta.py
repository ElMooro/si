"""quote_meta - shared quote freshness / latency metadata contract (8/10).

Zero third-party dependencies. Pure functions so every consumer (Lambdas,
unit tests, frontend mirrors) classifies badges identically.

Badge vocabulary:
  LIVE     - realtime-entitled quote, < 60s old, market open
  DELAYED  - < 15 min old intraday (or a delayed feed)
  SESSION  - market closed; quote is the last session close (< 1 trading day)
  STALE    - older than the above, unparseable timestamp, or future-dated
  OFFLINE  - no quote at all (as_of is None)

The per-ticker record built by quote_record() is the additive v2.0 schema
consumed by price-redundancy, market-tape and the frontend quote badges.
"""
from __future__ import annotations

import time
from datetime import date, datetime, timedelta
from datetime import timezone

# Badge boundaries (seconds). DELAYED_MAX_AGE_S preserves the historical
# 900s boundary previously hardcoded in justhodl-market-tape.
LIVE_MAX_AGE_S = 60
DELAYED_MAX_AGE_S = 900
SESSION_MAX_AGE_S = 86400
CLOCK_SKEW_S = 300

# Source kinds entitled to the LIVE badge. Anything else (delayed feeds,
# daily bars, unasserted entitlements) tops out at DELAYED intraday.
REALTIME_SOURCE_KINDS = ("polygon-realtime", "fmp-realtime")

BADGES = ("LIVE", "DELAYED", "SESSION", "STALE", "OFFLINE")

# Advisory quote expiry used for the record's stale_after field.
STALE_AFTER_S = 900


def _parse_iso(value):
    """Parse an ISO-8601 timestamp (or epoch seconds) to aware UTC datetime.

    Returns None when the value is missing or unparseable. Naive datetimes
    are assumed UTC. Never raises.
    """
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        try:
            return datetime.fromtimestamp(value, timezone.utc)
        except (OSError, OverflowError, ValueError):
            return None
    if not isinstance(value, str):
        return None
    s = value.strip()
    if not s:
        return None
    try:
        if s.endswith(("Z", "z")):
            s = s[:-1] + "+00:00"
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except ValueError:
        return None


def _nth_weekday_utc(year, month, weekday, n, hour):
    """Return the nth `weekday` (Mon=0..Sun=6) of month at hour:00 UTC (aware)."""
    first = date(year, month, 1)
    delta = (weekday - first.weekday()) % 7
    return datetime(year, month, 1 + delta + 7 * (n - 1), hour, 0,
                    tzinfo=timezone.utc)


def _et_parts(now_utc):
    """Return (isoweekday, minutes_since_midnight) in America/New_York.

    Manual DST math - no tzdata dependency: EDT (UTC-4) from the second
    Sunday of March 07:00 UTC through the first Sunday of November 06:00 UTC,
    otherwise EST (UTC-5).
    """
    dst_start = _nth_weekday_utc(now_utc.year, 3, 6, 2, 7)
    dst_end = _nth_weekday_utc(now_utc.year, 11, 6, 1, 6)
    offset_h = -4 if dst_start <= now_utc < dst_end else -5
    et = now_utc + timedelta(hours=offset_h)
    return et.isoweekday(), et.hour * 60 + et.minute


def _market_open(isoweekday, minutes):
    """Heuristic session clock: Mon-Fri 09:30-16:00 ET. Holidays ignored."""
    return 1 <= isoweekday <= 5 and 9 * 60 + 30 <= minutes < 16 * 60


def market_is_open(now=None):
    """Public helper: is the US equity session open right now (heuristic)?"""
    now_utc = now or datetime.now(timezone.utc)
    if now_utc.tzinfo is None:
        now_utc = now_utc.replace(tzinfo=timezone.utc)
    else:
        now_utc = now_utc.astimezone(timezone.utc)
    dow, mins = _et_parts(now_utc)
    return _market_open(dow, mins)


def classify_badge(as_of_iso, source_kind=None, now=None):
    """Classify a quote's freshness badge.

    Args:
        as_of_iso: ISO-8601 timestamp of the quote (None -> OFFLINE).
        source_kind: feed identifier; only "polygon-realtime"/"fmp-realtime"
            are entitled to LIVE.
        now: reference datetime (aware or naive UTC); defaults to now.

    Returns one of LIVE, DELAYED, SESSION, STALE, OFFLINE.
    """
    if as_of_iso is None:
        return "OFFLINE"
    now_utc = now or datetime.now(timezone.utc)
    if now_utc.tzinfo is None:
        now_utc = now_utc.replace(tzinfo=timezone.utc)
    else:
        now_utc = now_utc.astimezone(timezone.utc)
    as_of = _parse_iso(as_of_iso)
    if as_of is None:
        return "STALE"
    age_s = (now_utc - as_of).total_seconds()
    if age_s < -CLOCK_SKEW_S:
        return "STALE"
    age_s = max(age_s, 0.0)
    dow, mins = _et_parts(now_utc)
    kind = (source_kind or "").strip().lower()
    if _market_open(dow, mins):
        if kind in REALTIME_SOURCE_KINDS and age_s <= LIVE_MAX_AGE_S:
            return "LIVE"
        if age_s <= DELAYED_MAX_AGE_S:
            return "DELAYED"
        return "STALE"
    if age_s <= SESSION_MAX_AGE_S:
        return "SESSION"
    return "STALE"


def time_fetch(fn, *args, **kwargs):
    """Time a callable with perf_counter. Returns (result, latency_ms).

    Exceptions from fn propagate unchanged (latency is discarded on error);
    fetchers in this fleet return {"ok": False} dicts instead of raising.
    """
    start = time.perf_counter()
    result = fn(*args, **kwargs)
    latency_ms = round((time.perf_counter() - start) * 1000, 1)
    return result, latency_ms


def quote_record(price, as_of, observed_at, received_at, source, served_by,
                 served_by_chain, latency_ms, badge, stale_after, cache_hit):
    """Build the canonical v2.0 per-ticker quote metadata record.

    All fields are plain JSON scalars/lists. Callers merge this over their
    legacy price fields (price/change_7d/change_30d/sources stay untouched).
    """
    return {
        "price": price,
        "as_of": as_of,
        "observed_at": observed_at,
        "received_at": received_at,
        "source": source,
        "served_by": served_by,
        "served_by_chain": served_by_chain,
        "latency_ms": latency_ms,
        "badge": badge,
        "stale_after": stale_after,
        "cache_hit": bool(cache_hit),
    }


def fresh_enough(record, max_age_s):
    """Consumer gate: True if the record's as_of is within max_age_s seconds."""
    if not isinstance(record, dict):
        return False
    as_of = _parse_iso(record.get("as_of"))
    if as_of is None:
        return False
    age_s = (datetime.now(timezone.utc) - as_of).total_seconds()
    return 0 <= age_s <= max_age_s


def chain(hops):
    """Normalize a list of fetch-hop dicts into a served_by_chain list.

    Each hop keeps source, latency_ms, ok and as_of; unknown shapes are
    stringified rather than dropped.
    """
    out = []
    for hop in hops or []:
        if isinstance(hop, dict):
            out.append({
                "source": hop.get("source"),
                "latency_ms": hop.get("latency_ms"),
                "ok": bool(hop.get("ok")),
                "as_of": hop.get("as_of"),
            })
        else:
            out.append({"source": str(hop), "latency_ms": None,
                        "ok": False, "as_of": None})
    return out


def stale_after_for(as_of_iso, window_s=STALE_AFTER_S):
    """Advisory expiry timestamp (ISO) for a quote, or None if unparseable."""
    as_of = _parse_iso(as_of_iso)
    if as_of is None:
        return None
    return (as_of + timedelta(seconds=window_s)).isoformat()
