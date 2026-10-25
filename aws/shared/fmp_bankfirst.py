"""fmp_bankfirst -- answer FMP /stable requests from the warehouse banks before the provider, for engines that build
their own urllib requests (the consumer-conversion lane, Khalid 2026-10-25: "100% pulled AND USED").

    from fmp_bankfirst import install
    install()                      # once, at import time; idempotent

After install(), urllib.request.urlopen(url_or_Request) for a URL on financialmodelingprep.com/stable/ is answered from
the banks when they hold a FRESH answer for that path + query (same JSON shape FMP returns, verbatim banked rows); every
other URL - and every FMP request the banks cannot answer freshly - goes to the real urlopen untouched.  requests users
are not intercepted (urllib only).  Any failure inside the shim falls through to the network.

Freshness guards (a bank must never masquerade as today's print):
  * price/market-cap families (historical-price-eod/*, historical-market-capitalization): the banked rows' newest
    `date` must be within PRICE_MAX_AGE_DAYS (5) of today, else live.
  * fast families (analyst estimates, grades, targets, ratings, insiders, Senate/House, news, press releases, earnings,
    calendars): the banked object's pulled_at (S3 metadata) within FAST_MAX_AGE_DAYS (14), else live.
  * slow families (statements, as-reported, segmentation, executives, employees, ESG, profile-class data): within
    SLOW_MAX_AGE_DAYS (95).
  * never answered from the bank: quote, quote-short, batch quotes, aftermarket, search-*, screeners, anything with a
    date window that the bank stores by month (callers get those live - the walker banks them separately).
Counters: fmp_bankfirst.STATS = {"hits": n, "misses": n, "stale": n, "passthrough": n, "errors": n}; print_stats()
logs one line (engines that already log a summary can call it).
"""
from __future__ import annotations

import io
import json
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from http.client import HTTPMessage

PRICE_MAX_AGE_DAYS = 5
FAST_MAX_AGE_DAYS = 14
SLOW_MAX_AGE_DAYS = 95
STATS = {"hits": 0, "misses": 0, "stale": 0, "passthrough": 0, "errors": 0}
_installed = {"done": False, "real": None}

PRICE_HEADS = {"historical-price-eod/full", "historical-price-eod/light", "historical-price-eod/dividend-adjusted", "historical-price-eod/non-split-adjusted", "historical-market-capitalization"}
FAST_HEADS = {"analyst-estimates", "price-target", "price-target-consensus", "price-target-news", "grades", "grades-historical", "grades-news", "ratings-historical", "historical-rating",
              "analyst-stock-recommendations", "insider-trading/search", "insider-trading/statistics", "senate-trades", "house-trades", "news/stock", "news/press-releases",
              "earnings", "earnings-surprises", "dividends", "splits", "acquisition-of-beneficial-ownership", "fail_to_deliver", "historical/social-sentiment", "market-capitalization",
              "etf/holdings", "etf/info", "etf/sector-weightings", "etf/country-weightings", "etf/asset-exposure", "etf/holder", "institutional-ownership/symbol-positions-summary"}
NEVER_HEADS = {"quote", "quote-short", "batch-quote", "batch-quote-short", "aftermarket-quote", "aftermarket-trade", "batch-aftermarket-quote", "batch-aftermarket-trade",
               "company-screener", "stock-screener", "search-symbol", "search-name", "search-cik", "search-cusip", "search-isin", "search-exchange-variants",
               "earnings-calendar", "dividends-calendar", "splits-calendar", "ipos-calendar", "economic-calendar", "earning-calendar-confirmed", "sec-filings-8k", "sec-filings-financials",
               "stock-price-change", "biggest-gainers", "biggest-losers", "most-actives", "sector-performance-snapshot", "industry-performance-snapshot", "sector-pe-snapshot", "industry-pe-snapshot",
               "treasury-rates", "market-risk-premium", "pre-post-market", "quotes/commodity", "quotes/forex", "quotes/crypto", "batch-index-quotes", "batch-etf-quotes"}


def _head(path):
    p = path.strip("/")
    parts = p.split("/")
    for n in (3, 2, 1):
        cand = "/".join(parts[:n])
        if cand in PRICE_HEADS or cand in FAST_HEADS or cand in NEVER_HEADS:
            return cand
    return parts[0] if parts else p


def _age_days(iso):
    try:
        t = datetime.fromisoformat(str(iso).replace("Z", "+00:00"))
        if t.tzinfo is None:
            t = t.replace(tzinfo=timezone.utc)
        return (datetime.now(timezone.utc) - t).total_seconds() / 86400.0
    except Exception:  # noqa: BLE001
        return None


def _fresh_enough(head, rows, q):
    """True when the banked answer may stand in for a live call."""
    import fmp_bank
    if head in NEVER_HEADS:
        return False
    if head in PRICE_HEADS:
        newest = None
        for r in rows if isinstance(rows, list) else []:
            d = str((r or {}).get("date") or "")[:10] if isinstance(r, dict) else ""
            if d and (newest is None or d > newest):
                newest = d
        if not newest:
            return False
        want = str(q.get("to") or datetime.now(timezone.utc).date().isoformat())[:10]
        try:
            gap = (datetime.fromisoformat(want) - datetime.fromisoformat(newest)).days
        except ValueError:
            return False
        return gap <= PRICE_MAX_AGE_DAYS
    limit = FAST_MAX_AGE_DAYS if head in FAST_HEADS else SLOW_MAX_AGE_DAYS
    age = fmp_bank.last_answer_age_days()
    if age is None:
        return head not in FAST_HEADS            # bulk-bank answers carry no per-object stamp; they refresh daily
    return age <= limit


def answer(url):
    """Bank answer (bytes) for an FMP /stable URL, or None."""
    import fmp_bank
    u = urllib.parse.urlparse(url)
    if "financialmodelingprep.com" not in u.netloc or not u.path.startswith("/stable/"):
        return None
    path = u.path[len("/stable/"):]
    q = {k: v[0] for k, v in urllib.parse.parse_qs(u.query).items()}
    q.pop("apikey", None)
    head = _head(path)
    if head in NEVER_HEADS:
        return None
    qs = urllib.parse.urlencode(q)
    fmp_bank.reset_last_answer()
    rows = fmp_bank.try_bank(path, qs)
    if rows is None:
        STATS["misses"] += 1
        return None
    if not _fresh_enough(head, rows, q):
        STATS["stale"] += 1
        return None
    STATS["hits"] += 1
    return json.dumps(rows, separators=(",", ":"), default=str).encode("utf-8")


def _headers(n):
    """A real http.client.HTTPMessage (email.message.Message): .get / .get_all / .get_content_charset /
    .get_content_type / .items / ['X'] / `in` all behave as they do on a live response."""
    h = HTTPMessage()
    h["Content-Type"] = "application/json; charset=utf-8"
    h["Content-Length"] = str(n)
    h["X-JH-Source"] = "fmp-bank"
    return h


class _Resp(io.BytesIO):
    """Enough of an http.client.HTTPResponse for the fleet's call sites (surveyed 2026-10-25 across aws/lambdas): read()
    / read1 / readline / json.load(resp), status / code / reason / msg, getcode(), geturl(), headers.get(...),
    headers.get_all(...), headers.get_content_charset(), info(), length, context-manager.  No Content-Encoding header:
    the body is plain JSON bytes, so gzip branches at call sites stay cold."""

    status = 200
    code = 200
    reason = "OK"
    version = 11
    chunked = False
    debuglevel = 0

    def __init__(self, body, url):
        super().__init__(body)
        self.url = url
        self.length = len(body)
        self.headers = _headers(len(body))
        self.msg = self.headers
        self.fp = self

    def getcode(self):
        return 200

    def getheader(self, name, default=None):
        return self.headers.get(name, default)

    def getheaders(self):
        return list(self.headers.items())

    def geturl(self):
        return self.url

    def info(self):
        return self.headers

    def isclosed(self):
        return self.closed

    def __enter__(self):
        return self

    def __exit__(self, *a):
        self.close()
        return False


def _urlopen(url, data=None, timeout=None, *args, **kwargs):
    real = _installed["real"]
    try:
        target = url.full_url if isinstance(url, urllib.request.Request) else str(url)
        if data is None and "financialmodelingprep.com/stable/" in target:
            body = answer(target)
            if body is not None:
                return _Resp(body, target)
        else:
            STATS["passthrough"] += 1
    except Exception:  # noqa: BLE001
        STATS["errors"] += 1
    if timeout is None:
        return real(url, data, *args, **kwargs)
    return real(url, data, timeout, *args, **kwargs)


def install():
    if _installed["done"]:
        return
    _installed["real"] = urllib.request.urlopen
    urllib.request.urlopen = _urlopen
    _installed["done"] = True


def uninstall():
    if _installed["done"] and _installed["real"]:
        urllib.request.urlopen = _installed["real"]
    _installed["done"] = False


def print_stats(prefix="[fmp-bankfirst]"):
    print("%s hits=%d stale=%d misses=%d passthrough=%d errors=%d" % (prefix, STATS["hits"], STATS["stale"], STATS["misses"], STATS["passthrough"], STATS["errors"]))
