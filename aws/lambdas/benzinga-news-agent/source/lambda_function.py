"""benzinga-news-agent — analyst ratings, earnings + economic calendars, and
market news for benzinga.html.

RE-SOURCED onto FMP /stable (2026-07-15): the Benzinga direct API key
(bzMJ62…) is expired (401) and Massive stopped serving Benzinga to this
account (403 NOT_AUTHORIZED on all keys). FMP is already entitled and serves
every section this page needs. The page's JS reads these keys with field
fallbacks, so we emit both the legacy Benzinga-style names and FMP names:

  analyst_ratings[] : date, ticker/symbol, analyst/analyst_name,
                      action_company, rating_current, pt_current
  earnings_calendar[]: date, ticker/symbol, name/company, eps_est/epsEstimated, time
  economic_events[] : date, event_name/event, actual, consensus, prior
  market_news[]     : title, author, created/published
  dividends[]       : bonus (date, ticker, dividend, yield)

Served via Lambda Function URL (CORS *). Response envelope: {agent, ts, ...}.
"""
import json
import os
import urllib.request
import urllib.error
from datetime import datetime, timedelta, timezone

FMP_KEY = os.environ.get("FMP_KEY") or os.environ.get("FMP_API_KEY", "")
BASE = "https://financialmodelingprep.com/stable"

# NOTE: CORS is owned SOLELY by the Lambda Function URL's native CORS config
# (AllowOrigins ['*']). The handler must NOT also emit Access-Control-Allow-*
# headers, or the two stack into an invalid doubled header
# ("*, https://justhodl.ai") that browsers reject. So we return ONLY
# Content-Type here.
CORS = {
    "Content-Type": "application/json",
}


def fmp(path, params=None, timeout=20):
    if not FMP_KEY:
        return []
    p = {**(params or {}), "apikey": FMP_KEY}
    qs = "&".join(f"{a}={b}" for a, b in p.items())
    url = f"{BASE}/{path}?{qs}"
    try:
        req = urllib.request.Request(url, headers={
            "User-Agent": "justhodl-benzinga-agent/2.0",
            "Accept": "application/json"})
        with urllib.request.urlopen(req, timeout=timeout) as r:
            d = json.loads(r.read())
            return d if isinstance(d, list) else []
    except Exception as e:
        print(f"[bz-agent] {path}: {e}")
        return []


def _dir_from_grades(prev, new, action):
    a = (action or "").lower()
    if "upgrade" in a:
        return "Upgrade"
    if "downgrade" in a:
        return "Downgrade"
    return "Maintains"


def build_ratings():
    """Merge grade changes + PT revisions into one analyst-action list."""
    grades = fmp("grades-latest-news", {"limit": 60})
    pts = fmp("price-target-latest-news", {"limit": 60})
    # index latest PT per symbol
    pt_by = {}
    for p in pts:
        s = p.get("symbol")
        if s and (s not in pt_by or (p.get("publishedDate", "") >
                                     pt_by[s].get("publishedDate", ""))):
            pt_by[s] = p
    out = []
    seen = set()
    for g in grades:
        s = g.get("symbol")
        if not s:
            continue
        seen.add(s)
        p = pt_by.get(s)
        pt_cur = (p.get("adjPriceTarget") or p.get("priceTarget")) if p else None
        out.append({
            "date": (g.get("publishedDate") or "")[:10],
            "ticker": s, "symbol": s,
            "analyst": g.get("gradingCompany") or "",
            "analyst_name": g.get("gradingCompany") or "",
            "action_company": _dir_from_grades(
                g.get("previousGrade"), g.get("newGrade"), g.get("action")),
            "rating_current": g.get("newGrade") or "",
            "pt_current": pt_cur if pt_cur is not None else "-",
            "adjusted_pt_current": pt_cur if pt_cur is not None else "-",
        })
    # PT-only actions (no grade change this window)
    for s, p in pt_by.items():
        if s in seen:
            continue
        pt_cur = p.get("adjPriceTarget") or p.get("priceTarget")
        out.append({
            "date": (p.get("publishedDate") or "")[:10],
            "ticker": s, "symbol": s,
            "analyst": p.get("analystCompany") or "",
            "analyst_name": p.get("analystCompany") or "",
            "action_company": "PT Update",
            "rating_current": "",
            "pt_current": pt_cur if pt_cur is not None else "-",
            "adjusted_pt_current": pt_cur if pt_cur is not None else "-",
        })
    out.sort(key=lambda r: r.get("date", ""), reverse=True)
    return out[:40]


def build_earnings():
    today = datetime.now(timezone.utc).date()
    end = today + timedelta(days=14)
    rows = fmp("earnings-calendar", {"from": str(today), "to": str(end)})
    # US-relevant, upcoming, with an estimate; keep it readable
    out = []
    for e in rows:
        sym = e.get("symbol", "")
        # skip obvious foreign tickers (with a dot suffix exchange) to keep US focus
        if "." in sym:
            continue
        out.append({
            "date": e.get("date", ""),
            "ticker": sym, "symbol": sym,
            "name": sym, "company": sym,
            "eps_est": e.get("epsEstimated"),
            "epsEstimated": e.get("epsEstimated"),
            "time": "",
        })
    out.sort(key=lambda r: r.get("date", ""))
    return out[:30]


def build_economics():
    today = datetime.now(timezone.utc).date()
    end = today + timedelta(days=10)
    rows = fmp("economic-calendar", {"from": str(today), "to": str(end)})
    out = []
    for e in rows:
        if (e.get("country") or "").upper() not in ("US", "USD"):
            continue
        out.append({
            "date": e.get("date", ""),
            "event_name": e.get("event", ""), "event": e.get("event", ""),
            "name": e.get("event", ""),
            "actual": e.get("actual") if e.get("actual") is not None else "-",
            "consensus": e.get("estimate") if e.get("estimate") is not None else "-",
            "prior": e.get("previous") if e.get("previous") is not None else "-",
            "impact": e.get("impact", ""),
        })
    out.sort(key=lambda r: r.get("date", ""))
    return out[:20]


def build_news():
    rows = fmp("news/general-latest", {"page": 0, "limit": 20})
    if not rows:
        rows = fmp("news/stock-latest", {"page": 0, "limit": 20})
    out = []
    for n in rows:
        out.append({
            "title": n.get("title", ""),
            "author": n.get("publisher") or n.get("site") or "",
            "created": (n.get("publishedDate") or "")[:16],
            "published": (n.get("publishedDate") or "")[:16],
            "url": n.get("url", ""),
        })
    return out[:15]


def build_dividends():
    today = datetime.now(timezone.utc).date()
    end = today + timedelta(days=14)
    rows = fmp("dividends-calendar", {"from": str(today), "to": str(end)})
    out = []
    for d in rows:
        sym = d.get("symbol", "")
        if "." in sym:
            continue
        out.append({
            "date": d.get("date", ""),
            "ticker": sym, "symbol": sym,
            "dividend": d.get("dividend"),
            "yield": round(d.get("yield"), 2) if d.get("yield") else None,
            "frequency": d.get("frequency", ""),
        })
    out.sort(key=lambda r: r.get("date", ""))
    return out[:20]


def lambda_handler(event, context):
    if isinstance(event, dict) and event.get("requestContext", {}).get(
            "http", {}).get("method") == "OPTIONS":
        return {"statusCode": 200, "headers": CORS, "body": "{}"}
    path = event.get("rawPath", "") if isinstance(event, dict) else ""
    if "/health" in path:
        return {"statusCode": 200, "headers": CORS, "body": json.dumps(
            {"status": "healthy", "agent": "benzinga-news-agent",
             "source": "FMP /stable", "has_key": bool(FMP_KEY)})}
    try:
        ratings = build_ratings()
        earnings = build_earnings()
        economics = build_economics()
        news = build_news()
        dividends = build_dividends()
        body = {
            "agent": "benzinga-news-agent",
            "source": "FMP /stable (Benzinga retired 2026-07-15)",
            "ts": datetime.now(timezone.utc).isoformat(),
            "analyst_ratings": ratings,
            "earnings_calendar": earnings,
            "economic_events": economics,
            "dividends": dividends,
            "market_news": news,
            "counts": {"ratings": len(ratings), "earnings": len(earnings),
                       "economics": len(economics), "news": len(news),
                       "dividends": len(dividends)},
        }
        return {"statusCode": 200, "headers": CORS,
                "body": json.dumps(body, default=str)}
    except Exception as e:
        import traceback
        return {"statusCode": 500, "headers": CORS, "body": json.dumps(
            {"error": str(e), "trace": traceback.format_exc()})}
