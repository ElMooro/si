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
import os
import threading
import time

import boto3

_S3 = boto3.client("s3", region_name="us-east-1")
_BUCKET = "justhodl-dashboard-live"
ROOT = "data/warm/fmp-bank/"
_TTL = 900
_cache: dict = {}                      # small objects (JSON, manifests): key -> (fetched_at, bytes|None, metadata)
_MAX_CACHED = 24


def _text_budget():
    """Chars of decoded CSV kept per container: 30% of the Lambda's memory, clamped to [48 MB, 640 MB]; local 256 MB.
    (First light 2026-10-25: at 20% the 512 MB engines thrashed on two statement year-files; the engines themselves use
    ~100-250 MB, and a table costs ~1.3x its text while it is being built.)"""
    try:
        mb = int(os.environ.get("FMP_BANK_TEXT_BUDGET_MB") or 0)
        if mb:
            return mb << 20
        mem = int(os.environ.get("AWS_LAMBDA_FUNCTION_MEMORY_SIZE") or 0)
        return max(48 << 20, min(640 << 20, (mem << 20) * 3 // 10)) if mem else 256 << 20
    except (TypeError, ValueError):
        return 256 << 20


class _Table:
    """One banked CSV, decoded once, with a lazily built symbol -> row-span index.

    The bulks are whole-market files (profile-bulk ~94k rows, a statements year-file ~30k rows); parsing one into dicts
    per lookup cost seconds and ~10x its size in memory, so a per-symbol consumer looping 1,000 names would have timed
    out or run the container out of memory (2026-10-25 review before the consumer lane widened).  Here the text is
    parsed ONCE with csv.reader to record each row's character span, and a symbol lookup re-parses only its own rows.
    """
    __slots__ = ("key", "text", "header", "spans", "symcol", "touched", "meta")

    def __init__(self, key, text, meta=None):
        self.key, self.text, self.touched, self.meta = key, text, time.time(), meta or {}
        self.header, self.spans, self.symcol = None, None, None

    def _build(self):
        lines = _Lines(self.text)
        reader = csv.reader(lines)
        try:
            self.header = next(reader)
        except StopIteration:
            self.header, self.spans = [], {}
            return
        low = [h.strip().lower() for h in self.header]
        self.symcol = low.index("symbol") if "symbol" in low else None
        spans = {}
        if self.symcol is not None:
            col = self.symcol
            while True:
                start = lines.pos
                try:
                    row = next(reader)
                except StopIteration:
                    break
                if col < len(row):
                    spans.setdefault(_norm(row[col]), []).append((start, lines.pos))
        self.spans = spans

    def rows_for(self, symbol):
        """Typed dict rows whose symbol column equals `symbol` (dash/dot variants merged), in file order."""
        if self.spans is None:
            self._build()
        if not self.spans:
            return []
        sym = _norm(symbol)
        keys = [sym] if "-" not in sym else [sym, sym.replace("-", ".")]
        spans = []
        for k in keys:
            spans.extend(self.spans.get(k) or [])
        if not spans:
            return []
        spans.sort()
        buf = io.StringIO("".join(self.text[a:b] for a, b in spans))
        return [_typed(dict(zip(self.header, r))) for r in csv.reader(buf) if r]

    def all_rows(self):
        if self.spans is None:
            self._build()
        return [_typed(r) for r in csv.DictReader(io.StringIO(self.text))]


class _Lines:
    """Line iterator that reports how far csv.reader has consumed (quoted fields may span physical lines)."""
    __slots__ = ("text", "pos")

    def __init__(self, text):
        self.text, self.pos = text, 0

    def __iter__(self):
        return self

    def __next__(self):
        t = self.text
        if self.pos >= len(t):
            raise StopIteration
        nl = t.find("\n", self.pos)
        end = len(t) if nl < 0 else nl + 1
        line = t[self.pos:end]
        self.pos = end
        return line


_TABLES: dict = {}                     # key -> _Table (LRU by touched; total chars <= _text_budget())
_LOCK = threading.Lock()               # eviction only; concurrent builds of one table are benign (last writer wins)
_MISSING: dict = {}                    # key -> time the bank answered "no such object" (negative cache, _TTL)
STATS = {"s3_gets": 0, "s3_misses": 0, "table_builds": 0, "table_evictions": 0, "table_refetches": 0}
_EVICTED: set = set()

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


class _ThreadLast:
    """The key + S3 metadata of the last object read, PER THREAD (engines read FMP from thread pools; the freshness guard
    in fmp_bankfirst must see its own thread's answer, never a neighbour's)."""

    def __init__(self):
        self._tl = threading.local()

    def _d(self):
        d = getattr(self._tl, "d", None)
        if d is None:
            d = self._tl.d = {"key": None, "meta": None}
        return d

    def get(self, k, default=None):
        return self._d().get(k, default)

    def __getitem__(self, k):
        return self._d()[k]

    def __setitem__(self, k, v):
        self._d()[k] = v


_LAST = _ThreadLast()                 # fmp_bankfirst freshness guard reads _LAST["key"] / _LAST["meta"]


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


def _fetch(key):
    """(decoded bytes | None, S3 metadata) for one banked object; a missing object is remembered for _TTL seconds."""
    now = time.time()
    if now - _MISSING.get(key, 0) < _TTL:
        return None, None
    STATS["s3_gets"] += 1
    try:
        obj = _S3.get_object(Bucket=_BUCKET, Key=key)
        raw = obj["Body"].read()
        meta = obj.get("Metadata") or {}
        if key.endswith(".gz"):
            raw = gzip.decompress(raw)
        return raw, meta
    except Exception:  # noqa: BLE001
        STATS["s3_misses"] += 1
        if len(_MISSING) > 4096:
            _MISSING.clear()
        _MISSING[key] = now
        return None, None


def _get(key):
    """Small objects (JSON, manifests) through the bytes cache."""
    now = time.time()
    hit = _cache.get(key)
    if hit and now - hit[0] < _TTL:
        _LAST["key"], _LAST["meta"] = key, (hit[2] if len(hit) > 2 else None)
        return hit[1]
    val, meta = _fetch(key)
    if len(_cache) >= _MAX_CACHED:
        _cache.pop(next(iter(_cache)))
    _cache[key] = (now, val, meta)
    if val is not None:
        _LAST["key"], _LAST["meta"] = key, meta
    return val


def _table(key):
    """The decoded, indexable CSV at `key` (None when not banked), kept per container within _text_budget()."""
    t = _TABLES.get(key)
    if t is not None:
        t.touched = time.time()
        _LAST["key"], _LAST["meta"] = key, t.meta
        return t
    raw, meta = _fetch(key)
    if not raw:
        return None
    if key in _EVICTED:
        STATS["table_refetches"] += 1
    text = raw.decode("utf-8", "replace")
    del raw
    budget = _text_budget()
    if len(text) > budget:                            # one file larger than the whole budget: serve it uncached
        STATS["table_builds"] += 1
        _LAST["key"], _LAST["meta"] = key, meta
        return _Table(key, text, meta)
    with _LOCK:
        while _TABLES and sum(len(x.text) for x in _TABLES.values()) + len(text) > budget:
            victim = min(_TABLES.values(), key=lambda x: x.touched)
            _TABLES.pop(victim.key, None)
            _EVICTED.add(victim.key)
            STATS["table_evictions"] += 1
    t = _TABLES[key] = _Table(key, text, meta)
    STATS["table_builds"] += 1
    _LAST["key"], _LAST["meta"] = key, meta
    return t


def _rows(key):
    """Every row of a banked CSV as typed dicts (whole-market consumers); None when not banked."""
    t = _table(key)
    if t is None:
        return None
    try:
        return t.all_rows()
    except Exception:  # noqa: BLE001
        return None


def _rows_for(key, symbol):
    """One symbol's rows from a banked CSV via the span index; None when the file is not banked, [] when absent."""
    t = _table(key)
    if t is None:
        return None
    try:
        return t.rows_for(symbol)
    except Exception:  # noqa: BLE001
        return None


def thrashing():
    """True when tables keep being evicted and re-read: the working set does not fit this container's budget and the
    bank is now slower than the provider - fmp_bankfirst stops consulting it for the rest of the invocation."""
    return STATS["table_refetches"] >= 6


# Fields FMP serves as JSON strings even when they look numeric (identifiers, dates, codes): a bank answer must carry
# the same types as the live JSON or a consumer comparing fiscalYear == "2025" or building an SEC URL from a zero-padded
# cik would break.  Everything else numeric-looking becomes a number; "true"/"false" become booleans (FMP's JSON booleans
# arrive as those words in the CSV bulks).
STRING_FIELDS = {"symbol", "cik", "date", "fiscalyear", "calendaryear", "period", "reportedcurrency", "filingdate", "accepteddate", "link", "finallink",
                 "companyname", "currency", "exchange", "exchangeshortname", "exchangefullname", "industry", "sector", "country", "description", "ceo", "website",
                 "ipodate", "isin", "cusip", "address", "city", "state", "zip", "phone", "image", "name", "type", "ticker", "lei", "sic", "sicdescription",
                 "stateoflocation", "stateofincorporation", "fiscalyearend", "formtype", "fillingdate", "ratingrecommendation", "rating", "gradingcompany",
                 "newgrade", "previousgrade", "action", "publisheddate", "title", "text", "url", "site", "publisher", "label", "peerslist", "transactiontype",
                 "reportingname", "typeofowner", "securityname", "acquisitionordisposition", "formtype", "securitiesowned", "lastupdated", "updatedat", "createdat"}


def _num(v, key=None):
    if v is None or v == "":
        return None
    if key is not None and str(key).lower() in STRING_FIELDS:
        return v
    s = str(v)
    low = s.lower()
    if low in ("true", "false"):
        return low == "true"
    if len(s) > 1 and s[0] == "0" and s[1].isdigit():              # zero-padded code (cik, fips, zip): a string in FMP's JSON
        return v
    try:
        f = float(s)
        return int(f) if f.is_integer() and "." not in s and "e" not in low else f
    except (TypeError, ValueError):
        return v


def _typed(row):
    return {k: _num(v, k) for k, v in row.items()}


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


def _snapshot_keys(bulk):
    """The banked file(s) of a snapshot bulk: one latest.csv.gz, or its part-N files."""
    whole = ROOT + bulk + "/latest.csv.gz"
    if _table(whole) is not None:
        return [whole]
    keys = []
    for part in range(0, 41):
        k = ROOT + "%s/part-%d/latest.csv.gz" % (bulk, part)
        if _table(k) is None:
            break
        keys.append(k)
    return keys


def snapshot(path, symbol=None):
    """Latest banked snapshot rows for an endpoint family (ratios-ttm, key-metrics-ttm, scores, rating, dcf,
    price-target-summary, grades-consensus, stock-peers, profile); the whole market when symbol is None."""
    bulk = SNAPSHOTS.get(path)
    if not bulk:
        return None
    keys = _snapshot_keys(bulk)
    if not keys:
        return None
    if symbol is None:
        rows = []
        for k in keys:
            rows.extend(_rows(k) or [])
        return rows or None
    out = []
    for k in keys:
        out.extend(_rows_for(k, symbol) or [])
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
    if limit and years is None:
        # a bounded ask scans only the years that can hold it (+1 for a company with a gap); every year-file is a
        # whole-market table (~10-20 MB decoded), so depth is what decides whether the working set fits the budget
        held = held[: -(-int(limit) // len(periods)) + 1]
    out = []
    misses = 0
    for y in held:
        got_any = False
        for p in periods:
            rows = _rows_for(ROOT + "%s/%d/%s.csv.gz" % (bulk, y, p), symbol)
            if rows is None:
                continue
            got_any = True
            out.extend(rows)
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
    key = ROOT + "eod-bulk/%s/%s.csv.gz" % (date_iso[:4], date_iso)
    if symbol is None:
        return _rows(key)
    out = _rows_for(key, symbol)
    if out is None:
        return None
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
