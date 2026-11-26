"""Catalyst calendar 2.0 - events taken from documents the warehouse already keeps.

Khalid 2026-11-26 (choosing the calendar for a chart tab): "audit their engine ... and see if you can expand it to
cover more data". The audit (ops 6971) found a calendar of 125 events of which 113 were earnings already reported;
the forward side was eight earnings dates, one FOMC meeting, one quad witching and one index rebalance. Everything
below was already in the warehouse and simply not on the calendar:

  ECON           the economic calendar (data/econ-calendar.json): releases and central-bank events, every country
  EARNINGS       the wider earnings calendar (data/finviz-earnings-calendar.json), sized by market value
  IPO            the IPO pipeline (data/ipo-pipeline.json)
  LOCKUP         lock-up expirations (data/lockup-expiration.json)
  QUARTER_END    the quarter-end rebalance window (data/rebalance-radar.json)
  REBALANCE      the Russell reconstitution Friday (data/russell-recon-frontrun.json)
  MARKET_HOLIDAY exchange closures and early closes (the Polygon bank's market-holidays file)
  OPEX           every monthly options expiration, VIX_EXPIRY the VIX settlement Wednesday (calendar rules, no data)

Each function takes the kept document and returns events in the calendar's own shape; none of them fetches anything.
A document that is missing, late or shaped differently yields no events and a line in the source report - never a
guessed date.
"""
from datetime import date, datetime, timedelta

IMPACTS = ("HIGH", "MEDIUM", "LOW")


def _day(text):
    try:
        return datetime.strptime(str(text)[:10], "%Y-%m-%d").date()
    except (ValueError, TypeError):
        return None


def _num(v):
    return v if isinstance(v, (int, float)) and not isinstance(v, bool) and v == v else None


def third_friday(year, month):
    d = date(year, month, 1)
    while d.weekday() != 4:
        d += timedelta(days=1)
    return d + timedelta(days=14)


def _fmt(v, unit):
    v = _num(v)
    if v is None:
        return None
    text = ("%.2f" % v).rstrip("0").rstrip(".")
    return text + (unit if unit in ("%",) else "")


def econ_events(doc, today, window_days):
    """Economic releases and central-bank events from the kept economic calendar, every country it lists."""
    cutoff = today + timedelta(days=window_days)
    seen, out = set(), []
    for row in list((doc or {}).get("upcoming") or []) + list((doc or {}).get("this_week") or []):
        if not isinstance(row, dict) or row.get("released"):
            continue
        d = _day(row.get("date"))
        name = (row.get("event") or "").strip()
        if not d or not name or d < today or d > cutoff:
            continue
        key = (d, row.get("time_utc"), row.get("country"), name)
        if key in seen:
            continue
        seen.add(key)
        impact = str(row.get("impact") or "").upper()
        impact = "HIGH" if row.get("tier1") else impact if impact in IMPACTS else "LOW"
        unit = row.get("unit")
        parts = [row.get("country") or ""]
        cons, prev = _fmt(row.get("consensus"), unit), _fmt(row.get("previous"), unit)
        if cons is not None:
            parts.append("consensus " + cons)
        if prev is not None:
            parts.append("previous " + prev)
        out.append({
            "date": d.isoformat(), "time": row.get("time_utc"), "type": "ECON", "title": name[:120],
            "subtitle": " · ".join(p for p in parts if p), "impact": impact, "source": "Economic calendar (kept document)",
            "url": None, "size_billions": None, "country": row.get("country"), "family": row.get("family"),
            "tier1": bool(row.get("tier1")), "consensus": _num(row.get("consensus")), "previous": _num(row.get("previous")), "unit": unit,
        })
    return out


def earnings_wide_events(doc, today, window_days, have=()):
    """Every company on the kept earnings calendar that the tracker's own list does not already carry."""
    cutoff = today + timedelta(days=window_days)
    have = {str(t).upper() for t in have}
    out = []
    for day in (doc or {}).get("calendar") or []:
        d = _day((day or {}).get("date"))
        if not d or d < today or d > cutoff:
            continue
        for r in day.get("reporters") or []:
            t = str((r or {}).get("ticker") or "").upper()
            if not t or t in have:
                continue
            have.add(t)
            cap = _num(r.get("mktcap_m"))
            impact = "HIGH" if cap is not None and cap >= 100000 else "MEDIUM" if cap is not None and cap >= 10000 else "LOW"
            when = {"BMO": "before the open", "AMC": "after the close"}.get(str(r.get("time") or "").upper())
            parts = [(r.get("company") or "")[:70], when, ("$%.1fB" % (cap / 1000.0)) if cap is not None else None]
            out.append({
                "date": d.isoformat(), "time": None, "type": "EARNINGS", "title": t + " earnings",
                "subtitle": " · ".join(p for p in parts if p), "impact": impact, "source": "Finviz earnings calendar (kept document)",
                "url": None, "size_billions": None, "ticker": t, "sector": r.get("sector"), "session": r.get("time"),
                "market_cap_usd": cap * 1e6 if cap is not None else None,
            })
    return out


def ipo_events(doc, today, window_days):
    cutoff = today + timedelta(days=window_days)
    out = []
    for r in (doc or {}).get("upcoming") or []:
        d = _day((r or {}).get("date"))
        sym = str(r.get("symbol") or "").upper() if isinstance(r, dict) else ""
        if not d or not sym or d < today or d > cutoff:
            continue
        lo, hi, shares = _num(r.get("price_low")), _num(r.get("price_high")), _num(r.get("shares_offered"))
        mid = _num(r.get("price_mid"))
        size = mid * shares if mid is not None and shares is not None else None
        parts = [(r.get("company") or "")[:70], r.get("exchange"), ("$%s–$%s" % (_fmt(lo, None), _fmt(hi, None))) if lo is not None and hi is not None else None]
        out.append({
            "date": d.isoformat(), "time": None, "type": "IPO", "title": sym + " IPO",
            "subtitle": " · ".join(p for p in parts if p), "impact": "MEDIUM" if size is not None and size >= 5e8 else "LOW",
            "source": "IPO pipeline (kept document)", "url": None, "size_billions": round(size / 1e9, 3) if size is not None else None,
            "ticker": sym, "exchange": r.get("exchange"),
        })
    return out


def lockup_events(doc, today, window_days):
    cutoff = today + timedelta(days=window_days)
    seen, out = set(), []
    for name in ("imminent_lockups_7d", "approaching_lockups_21d", "upcoming_lockups_35d"):
        for r in (doc or {}).get(name) or []:
            d = _day((r or {}).get("lockup_expiration_date")) if isinstance(r, dict) else None
            sym = str(r.get("symbol") or "").upper() if isinstance(r, dict) else ""
            if not d or not sym or sym in seen or d < today or d > cutoff:
                continue
            seen.add(sym)
            out.append({
                "date": d.isoformat(), "time": None, "type": "LOCKUP", "title": sym + " lock-up expires",
                "subtitle": " · ".join(p for p in [(r.get("company_name") or "")[:70], ("IPO " + str(r.get("ipo_date"))) if r.get("ipo_date") else None] if p),
                "impact": "LOW", "source": "Lock-up expirations (kept document)", "url": None, "size_billions": None, "ticker": sym,
            })
    return out


def quarter_end_events(doc, today, window_days):
    cal = (doc or {}).get("calendar") or {}
    d = _day(cal.get("quarter_end"))
    if not d or d < today or d > today + timedelta(days=window_days):
        return []
    return [{
        "date": d.isoformat(), "time": None, "type": "QUARTER_END", "title": "Quarter-end",
        "subtitle": "Rebalance window " + str(cal.get("window_def") or ""), "impact": "MEDIUM", "source": "Rebalance radar (kept document)",
        "url": None, "size_billions": None, "business_days_to": _num(cal.get("bdays_to_qtr_end")),
    }]


def russell_events(doc, today, window_days):
    d = _day((doc or {}).get("rebal_friday"))
    if not d or d < today or d > today + timedelta(days=window_days):
        return []
    return [{
        "date": d.isoformat(), "time": "20:00", "type": "REBALANCE", "title": "Russell reconstitution",
        "subtitle": "Phase " + str(doc.get("calendar_phase") or "unknown"), "impact": "HIGH", "source": "Russell reconstitution engine (kept document)",
        "url": None, "size_billions": _num(doc.get("aum_benchmarked_usd_bn")),
    }]


def holiday_events(rows, today, window_days):
    """Exchange closures and early closes. rows: the banked Polygon upcoming-holidays list (one row per exchange)."""
    cutoff = today + timedelta(days=window_days)
    by_day = {}
    for r in rows or []:
        if not isinstance(r, dict):
            continue
        d = _day(r.get("date"))
        if not d or d < today or d > cutoff or not r.get("name"):
            continue
        slot = by_day.setdefault((d, r.get("name")), {"status": set(), "exchanges": set(), "close": None})
        slot["status"].add(str(r.get("status") or "").lower())
        if r.get("exchange"):
            slot["exchanges"].add(str(r.get("exchange")))
        if r.get("close"):
            slot["close"] = str(r.get("close"))
    out = []
    for (d, name), slot in sorted(by_day.items()):
        early = "early-close" in slot["status"] and "closed" not in slot["status"]
        out.append({
            "date": d.isoformat(), "time": (slot["close"] or "")[11:16] or None if early else None, "type": "MARKET_HOLIDAY",
            "title": name + (" (early close)" if early else " (market closed)"),
            "subtitle": ", ".join(sorted(slot["exchanges"])), "impact": "MEDIUM", "source": "Polygon market holidays (kept document)",
            "url": None, "size_billions": None, "status": "early-close" if early else "closed",
        })
    return out


def opex_events(today, window_days):
    """Every monthly options expiration (third Friday). The quarterly ones are the calendar's own quad-witching rows."""
    cutoff = today + timedelta(days=window_days)
    out = []
    y, m = today.year, today.month
    for _ in range(window_days // 28 + 3):
        d = third_friday(y, m)
        if today <= d <= cutoff and m not in (3, 6, 9, 12):
            out.append({
                "date": d.isoformat(), "time": None, "type": "OPEX", "title": "Monthly options expiration",
                "subtitle": "Stock and index options expire", "impact": "MEDIUM", "source": "Calendar rule (third Friday)",
                "url": None, "size_billions": None,
            })
        y, m = (y + 1, 1) if m == 12 else (y, m + 1)
    return out


def vix_expiry_events(today, window_days):
    """VIX futures and options settle on the Wednesday thirty days before the third Friday of the following month."""
    cutoff = today + timedelta(days=window_days)
    out = []
    y, m = today.year, today.month
    for _ in range(window_days // 28 + 3):
        ny, nm = (y + 1, 1) if m == 12 else (y, m + 1)
        d = third_friday(ny, nm) - timedelta(days=30)
        if today <= d <= cutoff:
            out.append({
                "date": d.isoformat(), "time": None, "type": "VIX_EXPIRY", "title": "VIX expiration",
                "subtitle": "VIX futures and options settle at the open", "impact": "LOW", "source": "Calendar rule (30 days before next third Friday)",
                "url": None, "size_billions": None,
            })
        y, m = ny, nm
    return out


def dedupe(events):
    """One row per (date, type, title, ticker); the first seen is kept, so a source listed earlier wins."""
    seen, out = set(), []
    for e in events:
        key = (e.get("date"), e.get("type"), e.get("title"), e.get("ticker"))
        if key in seen:
            continue
        seen.add(key)
        out.append(e)
    return out
