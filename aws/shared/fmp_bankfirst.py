"""fmp_bankfirst -- answer FMP /stable requests from the warehouse banks before the provider, for engines that build
their own urllib requests (the consumer-conversion lane, Khalid 2026-10-25: "100% pulled AND USED").

    from fmp_bankfirst import install
    install()                      # once, at import time; idempotent; a no-op inside the banks themselves

After install(), urllib.request.urlopen(url_or_Request) for a URL on financialmodelingprep.com/stable/ is answered from
the banks when they hold a FRESH answer with the SAME meaning as the live call (FMP's own JSON shape, verbatim banked
rows); every other URL - and every FMP request the banks cannot answer exactly - goes to the real urlopen untouched.
requests users are not intercepted (urllib only).  Any failure inside the shim falls through to the network.

Meaning guards (the bank may only answer what the live call would have returned):
  * query whitelist per request: symbol/limit/period/from/to (+ cik/name/exchange/year/quarter where the dataset keys
    on them).  Anything else - page, datatype=csv, structure, symbols=, ... - is a shape the bank cannot reproduce: live.
  * from/to windows are applied to the banked rows' `date`; rows without a date under a window: live.
  * statements (income/balance/cash-flow, growth, ratios, key-metrics) only with an explicit limit <= 12 annual /
    16 quarterly: FMP's default depth without a limit is the provider's to define, not the bank's.
Freshness guards (a bank must never masquerade as today's print):
  * price / market-cap histories: the banked file's newest `date` must BE the last completed US session (and during the
    13:30-21:30 UTC session window with no `to` in the past the request goes live - the partial bar is the provider's).
  * fast families (estimates, grades, targets, insiders, Senate/House, news, press releases, earnings, dividends, splits,
    ETF holdings...): the banked object's pulled_at within FAST_MAX_AGE_DAYS (1) - older is live.
  * slow families from the walker (as-reported, segmentation, executives, employees, ESG...): within SLOW_MAX_AGE_DAYS (30).
  * bulk-bank answers (profile-class snapshots, statements): the bank manifest's updated_at within BULK_MAX_AGE_DAYS (2).
  * never: quotes, searches, screeners, calendars, snapshots, treasury/market-risk - always live.
Cost guards (the bank must be cheaper than the provider or it steps aside for the rest of the invocation):
  * MISS_STORM (25) consecutive misses on one dataset -> that dataset goes live for the rest of the process;
  * fmp_bank.thrashing() (tables evicted and re-read) or mean bank time > 0.3 s after 20 attempts, or 90 s of bank time
    in total -> everything goes live for the rest of the process (one log line).
Counters: STATS = {"hits", "misses", "stale", "unshaped", "passthrough", "errors", "bypassed"}; print_stats() logs one line.
"""
from __future__ import annotations

import io
import json
import os
import time
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone
from http.client import HTTPMessage

FAST_MAX_AGE_DAYS = 1.0
SLOW_MAX_AGE_DAYS = 30.0
BULK_MAX_AGE_DAYS = 2.0
MISS_STORM = 25
BANK_TIME_CAP_S = float(os.environ.get("FMP_BANKFIRST_TIME_CAP_S") or 90)
BANK_MEAN_CAP_S = 0.3
STATEMENT_LIMIT_MAX = {"annual": 12, "quarter": 16}
STATS = {"hits": 0, "misses": 0, "stale": 0, "unshaped": 0, "passthrough": 0, "errors": 0, "bypassed": 0}
_installed = {"done": False, "real": None}
_ctl = {"bank_time": 0.0, "attempts": 0, "off": None, "miss_run": {}, "off_heads": set(), "since": 0.0}
CTL_WINDOW_S = 900.0                   # a warm container serves many invocations: the cost guards reset every 15 minutes


def _reset_ctl(now=None):
    _ctl.update(bank_time=0.0, attempts=0, off=None, miss_run={}, off_heads=set(), since=(now if now is not None else time.time()))
STATEMENT_HEADS = {"income-statement", "balance-sheet-statement", "cash-flow-statement", "income-statement-growth", "balance-sheet-statement-growth",
                   "cash-flow-statement-growth", "key-metrics", "ratios", "financial-growth"}
COMMON_PARAMS = {"symbol", "limit", "period", "from", "to"}
EXTRA_PARAMS = {"economic-indicators": {"name"}, "commitment-of-traders-report": set(), "commitment-of-traders-analysis": set(),
                "institutional-ownership/dates": {"cik"}, "institutional-ownership/holder-performance-summary": {"cik"},
                "institutional-ownership/holder-industry-breakdown": {"cik", "year", "quarter"}, "institutional-ownership/extract": {"cik", "year", "quarter"},
                "institutional-ownership/symbol-positions-summary": {"year", "quarter"}, "institutional-ownership/extract-analytics/holder": {"year", "quarter"},
                "earning-call-transcript": {"year", "quarter"}, "funds/disclosure": {"year", "quarter"},
                "exchange-market-hours": {"exchange"}, "holidays-by-exchange": {"exchange"},
                "historical-sector-performance": {"sector", "exchange"}, "historical-industry-performance": {"industry", "exchange"}}
# The warehouse banks and the provider probes must always reach the live provider; a bank answered from itself would
# freeze the warehouse at whatever it last held.  install() is a no-op inside these functions whatever the source says.
NEVER_INSTALL_IN = {"justhodl-fmp-bank", "justhodl-fmp-walker", "justhodl-fmp-ratios", "justhodl-provider-catalog", "justhodl-provider-coverage",
                    "justhodl-provider-bank", "justhodl-polygon-bank", "justhodl-eia-bank"}

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


def _last_completed_session(now=None):
    """The most recent US equity session whose close is final: today once 21:30 UTC has passed (16:00 ET + settle, DST
    or not), else the previous weekday.  Exchange holidays are not modelled: on a holiday the bank's newest date is one
    session older than this and the request simply goes live."""
    now = now or datetime.now(timezone.utc)
    d = now.date()
    if now.hour * 60 + now.minute < 21 * 60 + 30:
        d -= timedelta(days=1)
    while d.weekday() >= 5:
        d -= timedelta(days=1)
    return d


def _in_session_window(now=None):
    now = now or datetime.now(timezone.utc)
    m = now.hour * 60 + now.minute
    return now.weekday() < 5 and 13 * 60 + 30 <= m < 21 * 60 + 30


def _newest_date(rows):
    newest = None
    for r in rows if isinstance(rows, list) else []:
        d = str((r or {}).get("date") or "")[:10] if isinstance(r, dict) else ""
        if d and (newest is None or d > newest):
            newest = d
    return newest


def _window(rows, q):
    """Apply from/to to rows carrying a `date`; None when the rows cannot be windowed."""
    lo, hi = str(q.get("from") or "")[:10], str(q.get("to") or "")[:10]
    if not lo and not hi:
        return rows
    if not isinstance(rows, list) or not all(isinstance(r, dict) and r.get("date") for r in rows):
        return None
    out = [r for r in rows if (not lo or str(r["date"])[:10] >= lo) and (not hi or str(r["date"])[:10] <= hi)]
    return out


def _shaped(head, path, q):
    """True when the request's parameters are ones the bank reproduces exactly."""
    allowed = COMMON_PARAMS | EXTRA_PARAMS.get(head, set()) | EXTRA_PARAMS.get(path, set())
    if set(q) - allowed:
        return False
    if head in STATEMENT_HEADS or path in STATEMENT_HEADS:
        lim = str(q.get("limit", ""))
        if not lim.isdigit():
            return False
        period = str(q.get("period", "annual")).lower()
        if period in ("fy", "annual"):
            return int(lim) <= STATEMENT_LIMIT_MAX["annual"]
        if period in ("quarter", "quarterly", "q1", "q2", "q3", "q4"):
            return int(lim) <= STATEMENT_LIMIT_MAX["quarter"]
        return False
    return True


def _fresh_enough(head, rows, q, now=None):
    """True when the banked answer may stand in for a live call."""
    import fmp_bank
    if head in NEVER_HEADS:
        return False
    now = now or datetime.now(timezone.utc)
    if head in PRICE_HEADS:
        newest = _newest_date(rows)
        if not newest:
            return False
        to = str(q.get("to") or "")[:10]
        today = now.date().isoformat()
        if not to or to >= today:
            if _in_session_window(now):
                return False                         # the live answer would carry today's partial bar
            return newest == _last_completed_session(now).isoformat()
        try:
            want = datetime.fromisoformat(to).date()
        except ValueError:
            return False
        while want.weekday() >= 5:
            want -= timedelta(days=1)
        return newest >= want.isoformat()            # the bank must reach the window's end; being newer is fine
    key = fmp_bank._LAST.get("key") or ""
    if key.startswith(fmp_bank.ROOT):                 # bulk bank: the manifest's updated_at is the bank's clock
        age = _age_days(fmp_bank.as_of())
        return age is not None and age <= BULK_MAX_AGE_DAYS
    meta = fmp_bank._LAST.get("meta") or {}
    age = _age_days(meta.get("pulled_at") or meta.get("as_of") or meta.get("date"))
    if age is None:
        return False
    return age <= (FAST_MAX_AGE_DAYS if head in FAST_HEADS else SLOW_MAX_AGE_DAYS)


def _bypass(reason):
    if _ctl["off"] is None:
        _ctl["off"] = reason
        print("[fmp-bankfirst] bank stepped aside for the rest of this invocation: %s (attempts=%d bank_time=%.1fs)" % (reason, _ctl["attempts"], _ctl["bank_time"]))


def answer(url):
    """Bank answer (bytes) for an FMP /stable URL, or None."""
    import fmp_bank
    if time.time() - _ctl["since"] > CTL_WINDOW_S:
        _reset_ctl()
    if _ctl["off"]:
        STATS["bypassed"] += 1
        return None
    u = urllib.parse.urlparse(url)
    if "financialmodelingprep.com" not in u.netloc or not u.path.startswith("/stable/"):
        return None
    path = u.path[len("/stable/"):].strip("/")
    q = {k: v[0] for k, v in urllib.parse.parse_qs(u.query).items()}
    q.pop("apikey", None)
    head = _head(path)
    if head in NEVER_HEADS or head in _ctl["off_heads"]:
        STATS["bypassed" if head in _ctl["off_heads"] else "passthrough"] += 1
        return None
    if not _shaped(head, path, q):
        STATS["unshaped"] += 1
        return None
    t0 = time.time()
    _ctl["attempts"] += 1
    fmp_bank.reset_last_answer()
    rows = fmp_bank.try_bank(path, urllib.parse.urlencode({k: v for k, v in q.items() if k not in ("from", "to")}))
    spent = time.time() - t0
    _ctl["bank_time"] += spent
    if _ctl["bank_time"] > BANK_TIME_CAP_S:
        _bypass("bank time cap %.0fs" % BANK_TIME_CAP_S)
    elif _ctl["attempts"] >= 20 and _ctl["bank_time"] / _ctl["attempts"] > BANK_MEAN_CAP_S:
        _bypass("mean bank time %.2fs > %.2fs" % (_ctl["bank_time"] / _ctl["attempts"], BANK_MEAN_CAP_S))
    elif fmp_bank.thrashing():
        _bypass("table cache thrashing (working set exceeds this container's text budget)")
    if rows is None:
        STATS["misses"] += 1
        n = _ctl["miss_run"][head] = _ctl["miss_run"].get(head, 0) + 1
        if n >= MISS_STORM:
            _ctl["off_heads"].add(head)
            print("[fmp-bankfirst] %s: %d consecutive misses - live for the rest of this invocation" % (head, n))
        return None
    _ctl["miss_run"][head] = 0
    if not _fresh_enough(head, rows, q):
        STATS["stale"] += 1
        return None
    rows = _window(rows, q)
    if rows is None:
        STATS["unshaped"] += 1
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
    if os.environ.get("AWS_LAMBDA_FUNCTION_NAME", "") in NEVER_INSTALL_IN or os.environ.get("FMP_BANKFIRST_DISABLE") == "1":
        return
    _installed["real"] = urllib.request.urlopen
    urllib.request.urlopen = _urlopen
    _installed["done"] = True


def uninstall():
    if _installed["done"] and _installed["real"]:
        urllib.request.urlopen = _installed["real"]
    _installed["done"] = False


def print_stats(prefix="[fmp-bankfirst]"):
    print("%s hits=%d stale=%d misses=%d unshaped=%d passthrough=%d bypassed=%d errors=%d bank_time=%.1fs%s" % (
        prefix, STATS["hits"], STATS["stale"], STATS["misses"], STATS["unshaped"], STATS["passthrough"], STATS["bypassed"], STATS["errors"],
        _ctl["bank_time"], (" off=%s" % _ctl["off"]) if _ctl["off"] else ""))
