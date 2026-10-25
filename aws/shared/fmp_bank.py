"""FMP Ultimate warehouse reader -- S3 first, the provider never, for anything justhodl-fmp-bank has banked.

The bank (aws/lambdas/justhodl-fmp-bank) stores FMP's bulk CSVs verbatim (gzip) under data/warm/fmp-bank/:

    <endpoint>/latest.csv.gz                       snapshot bulks (rating-bulk, dcf-bulk, scores-bulk, ratios-ttm-bulk,
    <endpoint>/part-N/latest.csv.gz                key-metrics-ttm-bulk, price-target-summary-bulk,
                                                   upgrades-downgrades-consensus-bulk, peers-bulk, profile-bulk, etf-holder-bulk)
    <endpoint>/<year>/<period>.csv.gz              statements / growth / ratios / key-metrics / financial-growth bulks
                                                   (period annual | Q1..Q4), earnings-surprises-bulk/<year>/year.csv.gz
    eod-bulk/<year>/<date>.csv.gz                  every listed symbol's close for one session

This module gives consumers the same shapes they got from the per-symbol /stable endpoints (a list of dict rows per
symbol, newest first), read from the bank with a small per-container cache, so the 233 FMP engines can stop paying
~7,000 per-symbol calls a day and all read ONE dated copy of the same number (warehouse-first doctrine, 2026-09-02).
Misses return None so a caller can still fall through to fmp_stable.fmp_get.  Never raises on S3 errors.

Bundle via deploy-lambdas (aws/shared).  No key, no network except S3.
"""
from __future__ import annotations

import csv
import gzip
import io
import json
import time

import boto3

_S3 = boto3.client("s3", region_name="us-east-1")
_BUCKET = "justhodl-dashboard-live"
ROOT = "data/warm/fmp-bank/"
_TTL = 900
_cache: dict = {}
_MAX_CACHED = 24

STATEMENTS = {
    "income-statement": "income-statement-bulk", "balance-sheet-statement": "balance-sheet-statement-bulk",
    "cash-flow-statement": "cash-flow-statement-bulk", "income-statement-growth": "income-statement-growth-bulk",
    "balance-sheet-statement-growth": "balance-sheet-statement-growth-bulk",
    "cash-flow-statement-growth": "cash-flow-statement-growth-bulk", "key-metrics": "key-metrics-bulk",
    "ratios": "ratios-bulk", "financial-growth": "financial-growth-bulk",
}
SNAPSHOTS = {
    "ratios-ttm": "ratios-ttm-bulk", "key-metrics-ttm": "key-metrics-ttm-bulk", "scores": "scores-bulk",
    "financial-scores": "scores-bulk", "rating": "rating-bulk", "ratings-snapshot": "rating-bulk",
    "discounted-cash-flow": "dcf-bulk", "price-target-summary": "price-target-summary-bulk",
    "grades-consensus": "upgrades-downgrades-consensus-bulk", "upgrades-downgrades-consensus": "upgrades-downgrades-consensus-bulk",
    "stock-peers": "peers-bulk", "profile": "profile-bulk",
}


_LAST = {"key": None, "meta": None}          # the S3 metadata of the last object read (fmp_bankfirst freshness guard)


def reset_last_answer():
    _LAST["key"], _LAST["meta"] = None, None


def last_answer_age_days():
    """Age in days of the last object read, from its pulled_at / as_of metadata (None when unknown)."""
    m = _LAST.get("meta") or {}
    stamp = m.get("pulled_at") or m.get("as_of") or m.get("date")
    if not stamp:
        return None
    try:
        from datetime import datetime, timezone
        t = datetime.fromisoformat(str(stamp)[:19].replace("Z", ""))
        return (datetime.now(timezone.utc).replace(tzinfo=None) - t).total_seconds() / 86400.0
    except Exception:  # noqa: BLE001
        return None


def _get(key):
    now = time.time()
    hit = _cache.get(key)
    if hit and now - hit[0] < _TTL:
        _LAST["key"], _LAST["meta"] = key, (hit[2] if len(hit) > 2 else None)
        return hit[1]
    meta = None
    try:
        obj = _S3.get_object(Bucket=_BUCKET, Key=key)
        raw = obj["Body"].read()
        meta = obj.get("Metadata") or {}
        if key.endswith(".gz"):
            raw = gzip.decompress(raw)
        val = raw
    except Exception:  # noqa: BLE001
        val = None
    if len(_cache) >= _MAX_CACHED:
        _cache.pop(next(iter(_cache)))
    _cache[key] = (now, val, meta)
    if val is not None:
        _LAST["key"], _LAST["meta"] = key, meta
    return val


def _rows(key):
    raw = _get(key)
    if not raw:
        return None
    try:
        return list(csv.DictReader(io.StringIO(raw.decode("utf-8", "replace"))))
    except Exception:  # noqa: BLE001
        return None


def _num(v):
    if v is None or v == "":
        return None
    try:
        f = float(v)
        return int(f) if f.is_integer() and "." not in str(v) else f
    except (TypeError, ValueError):
        return v


def _typed(row):
    return {k: _num(v) for k, v in row.items()}


def _norm(tk):
    return str(tk or "").upper().strip()


def manifest():
    raw = _get(ROOT + "manifest.json")
    try:
        return json.loads(raw) if raw else {}
    except Exception:  # noqa: BLE001
        return {}


def as_of():
    return manifest().get("updated_at")


def served(endpoint_bulk):
    return endpoint_bulk in (manifest().get("endpoints_served") or [])


def _by_symbol(rows, symbol):
    sym = _norm(symbol)
    alt = sym.replace("-", ".")
    return [_typed(r) for r in rows if _norm(r.get("symbol") or r.get("Symbol")) in (sym, alt)]


def snapshot(path, symbol=None):
    """Latest banked snapshot rows for an endpoint family (ratios-ttm, key-metrics-ttm, scores, rating, dcf,
    price-target-summary, grades-consensus, stock-peers, profile); the whole market when symbol is None."""
    bulk = SNAPSHOTS.get(path)
    if not bulk:
        return None
    rows = _rows(ROOT + bulk + "/latest.csv.gz")
    if rows is None:                                   # parted bulks
        rows = []
        part = 0
        while True:
            r = _rows(ROOT + "%s/part-%d/latest.csv.gz" % (bulk, part))
            if r is None:
                break
            rows.extend(r)
            part += 1
            if part > 40:
                break
        if not rows:
            return None
    if symbol is None:
        return [_typed(r) for r in rows]
    out = _by_symbol(rows, symbol)
    return out or None


def statements(path, symbol, period="annual", limit=None, years=None):
    """History rows for one symbol from the dated bulks, newest first (what /stable/<path>?symbol=&period=&limit= gave).

    period: 'annual' | 'quarter' (Q1..Q4 merged) | 'Q1'..'Q4'.  years: iterable of fiscal years to scan (default: every
    year the bank holds for that endpoint, newest first).  Returns None when the bank holds nothing for the request.
    """
    bulk = STATEMENTS.get(path)
    if not bulk:
        return None
    man = manifest()
    held = sorted({int(k.split("/")[1]) for k in (man.get("dated_keys") or []) if k.startswith(bulk + "/")}, reverse=True)
    if not held:
        held = list(range(int(time.strftime("%Y")), 1984, -1))
    if years is not None:
        held = [y for y in held if y in set(years)]
    periods = ["annual"] if period == "annual" else (["Q1", "Q2", "Q3", "Q4"] if period == "quarter" else [period])
    out = []
    misses = 0
    for y in held:
        got_any = False
        for p in periods:
            rows = _rows(ROOT + "%s/%d/%s.csv.gz" % (bulk, y, p))
            if rows is None:
                continue
            got_any = True
            out.extend(_by_symbol(rows, symbol))
        misses = 0 if got_any else misses + 1
        if misses >= 3 and years is None:           # three consecutive unbanked years: the bank ends here
            break
        if limit and len(out) >= limit:
            break
    if not out:
        return None
    out.sort(key=lambda r: str(r.get("date") or r.get("fiscalYear") or ""), reverse=True)
    return out[:limit] if limit else out


def eod(date_iso, symbol=None):
    """Every listed symbol's close for one session (or one symbol's row)."""
    rows = _rows(ROOT + "eod-bulk/%s/%s.csv.gz" % (date_iso[:4], date_iso))
    if rows is None:
        return None
    if symbol is None:
        return [_typed(r) for r in rows]
    out = _by_symbol(rows, symbol)
    return out[0] if out else None


def try_bank(path, qs=""):
    """Drop-in for fmp_book.try_harvest's contract: answer a /stable path+querystring from the bank or return None.

    Understands symbol=, period=, limit= (the shapes equity_enrich / fmp_stable callers use).  Anything else -> None.
    """
    from urllib.parse import parse_qs
    q = {k: v[0] for k, v in parse_qs(qs or "").items()}
    path = (path or "").strip("/")
    if "/" in path and "symbol" not in q:            # legacy path-style 'ratios/AAPL'
        path, _, q["symbol"] = path.partition("/")
    sym = q.get("symbol")
    if path in STATEMENTS and sym:
        period = q.get("period", "annual")
        if period in ("FY",):
            period = "annual"
        lim = int(q["limit"]) if str(q.get("limit", "")).isdigit() else None
        return statements(path, sym, period=period, limit=lim)
    if path in SNAPSHOTS and sym:
        return snapshot(path, sym)
    return None


# ---------------------------------------------------------------- the walker bank (non-bulk families) ----------------
WALKER_ROOT = "data/warm/fmp/"


def _period_split(base_annual, base_quarter):
    def pick(q):
        p = str(q.get("period", "annual")).lower()
        return base_quarter if p in ("quarter", "quarterly", "q", "q1", "q2", "q3", "q4") else base_annual
    return pick


# /stable path -> walker dataset id (str) or chooser(query) -> dataset id.  Entity = symbol unless noted.
WALKER_PATHS = {
    "analyst-estimates": _period_split("analyst-estimates-annual", "analyst-estimates-quarter"),
    "enterprise-values": _period_split("enterprise-values-annual", "enterprise-values-quarter"),
    "key-metrics": _period_split("key-metrics-annual", "key-metrics-quarter"),
    "ratios": _period_split("ratios-annual", "ratios-quarter"),
    "financial-growth": _period_split("financial-growth-annual", "financial-growth-quarter"),
    "income-statement-as-reported": _period_split("income-statement-as-reported-annual", "income-statement-as-reported-quarter"),
    "balance-sheet-statement-as-reported": _period_split("balance-sheet-statement-as-reported-annual", "balance-sheet-statement-as-reported-quarter"),
    "cash-flow-statement-as-reported": _period_split("cash-flow-statement-as-reported-annual", "cash-flow-statement-as-reported-quarter"),
    "financial-statement-full-as-reported": "financial-statement-full-as-reported-annual",
    "revenue-product-segmentation": _period_split("revenue-product-segmentation-annual", "revenue-product-segmentation-quarter"),
    "revenue-geographic-segmentation": _period_split("revenue-geographic-segmentation-annual", "revenue-geographic-segmentation-quarter"),
    "price-target": "price-target", "price-target-consensus": "price-target-consensus", "price-target-news": "price-target-news",
    "grades": "grades", "grades-historical": "grades-historical", "grades-news": "grades-news",
    "ratings-historical": "ratings-historical", "historical-rating": "historical-rating",
    "analyst-stock-recommendations": "analyst-stock-recommendations",
    "levered-discounted-cash-flow": "levered-discounted-cash-flow", "owner-earnings": "owner-earnings",
    "financial-reports-dates": "financial-reports-dates",
    "company-core-information": "company-core-information", "company-notes": "company-notes", "key-executives": "key-executives",
    "governance-executive-compensation": "governance-executive-compensation", "employee-count": "employee-count",
    "historical/employee-count": "historical-employee-count", "historical/shares-float": "historical-shares-float",
    "market-capitalization": "market-capitalization", "historical-market-capitalization": "historical-market-capitalization",
    "dividends": "dividends", "splits": "splits", "earnings": "earnings", "earnings-surprises": "earnings-surprises",
    "sec-profile": "sec-profile", "sec-filings-search/symbol": "sec-filings-search-symbol",
    "acquisition-of-beneficial-ownership": "acquisition-of-beneficial-ownership", "fail_to_deliver": "fail-to-deliver",
    "insider-trading/search": "insider-trading-search", "insider-trading/statistics": "insider-trading-statistics",
    "senate-trades": "senate-trades", "house-trades": "house-trades",
    "esg-ratings": "esg-ratings", "esg-disclosures": "esg-disclosures",
    "news/stock": "news-stock", "news/press-releases": "news-press-releases", "historical/social-sentiment": "historical-social-sentiment",
    "stock-peers": "stock-peers", "industry-classification-search": "industry-classification-search",
    "etf/info": "etf-info", "etf/holdings": "etf-holdings", "etf/portfolio-dates": "etf-portfolio-dates",
    "etf/sector-weightings": "etf-sector-weightings", "etf/country-weightings": "etf-country-weightings",
    "etf/asset-exposure": "etf-asset-exposure", "etf/holder": "etf-holder",
    "funds/disclosure-dates": "funds-disclosure-dates", "funds/disclosure-holders-latest": "funds-disclosure-holders-latest",
    "historical-price-eod/full": "historical-price-eod-full", "historical-price-eod/dividend-adjusted": "historical-price-eod-dividend-adjusted",
    "historical-price-eod/non-split-adjusted": "historical-price-eod-non-split-adjusted",
    "earning-call-transcript-dates": "earning-call-transcript-dates",
    "economic-indicators": ("economic-indicators", "name"),
    "commitment-of-traders-report": ("commitment-of-traders-report", "symbol"),
    "commitment-of-traders-analysis": ("commitment-of-traders-analysis", "symbol"),
    "institutional-ownership/dates": ("institutional-ownership-dates", "cik"),
    "institutional-ownership/holder-performance-summary": ("institutional-ownership-holder-performance-summary", "cik"),
    "exchange-market-hours": ("exchange-market-hours", "exchange"), "holidays-by-exchange": ("holidays-by-exchange", "exchange"),
}
WALKER_PERIOD_PATHS = {
    "institutional-ownership/symbol-positions-summary": ("institutional-ownership-symbol-positions-summary", "symbol"),
    "institutional-ownership/extract-analytics/holder": ("institutional-ownership-extract-analytics-holder", "symbol"),
    "institutional-ownership/holder-industry-breakdown": ("institutional-ownership-holder-industry-breakdown", "cik"),
    "institutional-ownership/extract": ("institutional-ownership-extract", "cik"),
    "earning-call-transcript": ("earning-call-transcript", "symbol"),
    "funds/disclosure": ("funds-disclosure", "symbol"),
}
WALKER_LISTS = {
    "stock-list", "etf-list", "actively-trading-list", "financial-statement-symbol-list", "tradable-list", "available-exchanges",
    "available-sectors", "available-industries", "available-countries", "index-list", "commodities-list", "forex-list",
    "cryptocurrency-list", "earnings-transcript-list", "commitment-of-traders-list", "insider-trading-transaction-type",
    "standard-industrial-classification-list", "symbol-change", "all-exchange-market-hours", "market-risk-premium", "treasury-rates",
    "sp500-constituent", "nasdaq-constituent", "dowjones-constituent", "historical-sp500-constituent", "historical-nasdaq-constituent",
    "historical-dowjones-constituent",
}
_SAFE = None


def _safe(s):
    import re
    global _SAFE
    _SAFE = _SAFE or re.compile(r"[^A-Za-z0-9._=^-]")
    return _SAFE.sub("_", str(s))[:120] or "_"


def _wjson(key):
    raw = _get(key)
    if not raw:
        return None
    try:
        return json.loads(raw.decode("utf-8", "replace"))
    except Exception:  # noqa: BLE001
        return None


def walker_manifest():
    raw = _get(WALKER_ROOT + "manifest.json")
    try:
        return json.loads(raw) if raw else {}
    except Exception:  # noqa: BLE001
        return {}


def walker(dataset, entity=None, period=None, day=None, exchange=None):
    """Rows banked by justhodl-fmp-walker: lists (entity None -> latest), per-entity files, per-entity-period files
    (period=(year, quarter)), dated feeds (day='YYYY-MM-DD') and month windows (day='YYYY-MM').  None when not banked."""
    key = WALKER_ROOT + dataset
    if entity is None and period is None and day is None:
        return _wjson(key + "/latest.json.gz")
    if entity is not None:
        key += "/" + _safe(entity)
    if exchange:
        key += "/" + _safe(exchange)
    if period:
        key += "/%dQ%d" % (int(period[0]), int(period[1]))
    if day:
        key += ("/%s/%s" % (day[:4], day)) if (entity is None and len(day) == 10) else ("/" + day)
    return _wjson(key + ".json.gz")


def try_walker(path, q):
    """Answer a /stable path + parsed query from the walker bank (same shapes the live endpoint returns) or None."""
    head = (path or "").strip("/")
    if head in WALKER_LISTS and not q.get("symbol"):
        return walker(head.replace("/", "-"))
    spec = WALKER_PATHS.get(head)
    if spec is not None:
        if callable(spec):
            ds, ent_key = spec(q), "symbol"
        elif isinstance(spec, tuple):
            ds, ent_key = spec
        else:
            ds, ent_key = spec, "symbol"
        ent = q.get(ent_key)
        if not ent:
            return None
        rows = walker(ds, ent, exchange=q.get("exchange") if ds.startswith("historical-sector") or ds.startswith("historical-industry") else None)
        if rows is None:
            return None
        lim = q.get("limit")
        if isinstance(rows, list) and str(lim or "").isdigit():
            rows = rows[: int(lim)]
        return rows
    pspec = WALKER_PERIOD_PATHS.get(head)
    if pspec and q.get(pspec[1]) and q.get("year") and q.get("quarter"):
        try:
            return walker(pspec[0], q.get(pspec[1]), period=(int(q["year"]), int(str(q["quarter"]).replace("Q", ""))))
        except (TypeError, ValueError):
            return None
    return None


_try_bank_bulk = try_bank


def try_bank(path, qs=""):  # noqa: F811 - the public entry point now covers both banks
    """Bulk bank first (fmp-bank), then the walker bank (fmp-walker); None -> the caller goes live."""
    hit = _try_bank_bulk(path, qs)
    if hit:
        return hit
    from urllib.parse import parse_qs
    q = {k: v[0] for k, v in parse_qs(qs or "").items()}
    p = (path or "").strip("/")
    if "/" in p and "symbol" not in q and p.split("/")[0] in WALKER_PATHS and p.count("/") == 1 and not p.startswith(("historical/", "insider-trading/", "etf/", "funds/", "news/", "institutional-ownership/", "sec-filings-search/", "historical-price-eod/")):
        p, _, q["symbol"] = p.partition("/")                # legacy path-style 'dividends/AAPL'
    try:
        return try_walker(p, q)
    except Exception:  # noqa: BLE001
        return None
