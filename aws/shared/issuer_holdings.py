"""Issuer-published ETF holdings (Bloomberg parity 10/10).

Fetches holdings directly from ETF issuers -- iShares, Vanguard, SPDR/State
Street, Invesco and Schwab -- for the Phase 1 universe.

Hard honesty rules:
* ``as_of`` is always the date stated inside the issuer file. iShares US
  domestic-equity holdings can lag ~30 days; "today" is never substituted.
* HTML responses (login walls, bot checks) are refused, never parsed as
  holdings.
* One ETF failing never fails the run: every public entry point is
  fail-soft and returns ``None`` (or ``{}``) on failure.
* No secrets: every endpoint used here is public.

Standard library only (urllib, csv, json). S3 access is injected by the
caller through ``read_state``/``write_state`` callables so this module stays
import-safe outside Lambda.
"""

import csv
import io
import json
import os
import re
import socket
import ssl
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation

UA = "JustHodl-IssuerHoldings/1.0"
MAX_BODY = 64 * 1024 * 1024
PACE_SECONDS = 2.0
RETRY_STATUSES = (429, 503)
MAX_ATTEMPTS = 3

# URL patterns grounded in aws/shared/sector_issuer_native.py and the
# issuer-published download endpoints.
ISHARES_SCREENER_URL = (
    "https://www.ishares.com/us/product-screener/product-screener-v3.1.jsn"
    "?dcrPath=/templatedata/config/product-screener-v3/data/en/us-ishares/"
    "ishares-product-screener-backend-config&siteEntryPassthrough=true"
)
ISHARES_DOWNLOAD_URL = (
    "https://www.blackrock.com/varnish-api/blk-one01-product-data/product-data/"
    "api/v1/get-fund-document?appSubType=ISHARES&appType=PRODUCT_PAGE&"
    "component=fundDownload&locale=en_US&portfolioId={pid}&targetSite=us-ishares"
    "&userType=individual"
)
VANGUARD_URL = (
    "https://investor.vanguard.com/investment-products/etfs/profile/api/"
    "{ticker}/portfolio-holdings"
)
SSGA_URL = (
    "https://www.ssga.com/us/en/individual/etfs/fund-data/download-holdings"
    "?symbol={ticker}"
)
INVESCO_URL = (
    "https://www.invesco.com/us/financial-products/etfs/holdings/main/holdings/0/"
    "?audienceType=Investor&ticker={ticker}"
)
SCHWAB_URL = "https://www.schwabassetmanagement.com/products/{ticker}/holdings"

ISSUERS = ("ishares", "vanguard", "ssga", "invesco", "schwab")

# Phase 1 universe: top-by-AUM names across the five issuers. All are members
# of provider_flow_catalog.ETF_UNIVERSE except the six Schwab flagships, which
# are included on AUM despite being absent from that catalog.
PHASE1_UNIVERSE = (
    # iShares (BlackRock)
    ("IVV", "ishares"), ("IWM", "ishares"), ("EFA", "ishares"),
    ("EEM", "ishares"), ("AGG", "ishares"), ("IAU", "ishares"),
    ("SLV", "ishares"), ("IBB", "ishares"), ("ICLN", "ishares"),
    ("IEF", "ishares"), ("IGV", "ishares"), ("IJH", "ishares"),
    ("INDA", "ishares"), ("ITA", "ishares"), ("IWD", "ishares"),
    ("IWF", "ishares"), ("IYT", "ishares"), ("LQD", "ishares"),
    ("SHY", "ishares"), ("SOXX", "ishares"), ("TIP", "ishares"),
    ("TLT", "ishares"), ("HYG", "ishares"), ("MTUM", "ishares"),
    ("QUAL", "ishares"), ("USMV", "ishares"), ("EWA", "ishares"),
    ("EWG", "ishares"), ("EWJ", "ishares"), ("EWZ", "ishares"),
    ("FXI", "ishares"), ("MCHI", "ishares"), ("EMB", "ishares"),
    # Vanguard
    ("VOO", "vanguard"), ("VTI", "vanguard"), ("VEA", "vanguard"),
    ("VWO", "vanguard"), ("VTV", "vanguard"), ("VUG", "vanguard"),
    ("VGT", "vanguard"),
    # SPDR (State Street)
    ("SPY", "ssga"), ("DIA", "ssga"),
    ("XLF", "ssga"), ("XLK", "ssga"), ("XLE", "ssga"),
    ("XLV", "ssga"), ("XLI", "ssga"), ("XLP", "ssga"),
    ("XLU", "ssga"), ("XLY", "ssga"), ("XLB", "ssga"),
    ("XLC", "ssga"), ("XLG", "ssga"), ("XLRE", "ssga"),
    ("XME", "ssga"), ("XOP", "ssga"), ("XBI", "ssga"),
    ("XRT", "ssga"), ("XHB", "ssga"), ("XSD", "ssga"),
    ("GLD", "ssga"), ("JNK", "ssga"), ("KBE", "ssga"),
    ("KRE", "ssga"),
    # Invesco
    ("QQQ", "invesco"), ("TAN", "invesco"), ("DBA", "invesco"),
    ("DBC", "invesco"), ("PHO", "invesco"), ("PPA", "invesco"),
    # Schwab
    ("SCHX", "schwab"), ("SCHD", "schwab"), ("SCHB", "schwab"),
    ("SCHF", "schwab"), ("SCHG", "schwab"), ("SCHA", "schwab"),
)

# Header synonym map: normalized header cell -> canonical field.
COLUMN_SYNONYMS = {
    "ticker": ("ticker", "holding ticker", "holding_ticker", "symbol",
               "holding symbol", "security ticker", "underlying ticker"),
    "name": ("name", "holding name", "security name", "company name",
             "description", "holding description", "security description"),
    "cusip": ("cusip", "holding cusip"),
    "isin": ("isin", "holding isin"),
    "figi": ("figi", "holding figi", "composite figi", "figi code"),
    "weight": ("weight (%)", "weight %", "weight", "% of net assets",
               "% net assets", "percent of net assets", "pct of net assets",
               "allocation", "allocation %", "portfolio weight", "weighting"),
    "shares": ("shares", "shares held", "quantity", "units", "holding shares",
               "par value", "principal amount", "nominal", "number of shares",
               "share count"),
    "market_value": ("market value", "market value ($)", "market value usd",
                     "value", "holding value", "position value",
                     "notional value", "market_value"),
    "asset_class": ("asset class", "assetclass", "class", "type",
                    "security type", "instrument type", "category"),
    "as_of": ("as of", "as of date", "date", "holding date", "effective date",
              "valuation date"),
}

MISSING_TOKENS = ("", "--", "-", "n/a", "na", "#n/a", ".", "null", "none")

_DATE_FORMATS = (
    "%Y-%m-%d", "%m/%d/%Y", "%m/%d/%y", "%b %d, %Y", "%B %d, %Y",
    "%b %d %Y", "%B %d %Y", "%d-%b-%Y", "%d %b %Y", "%Y%m%d",
)

_AS_OF_PATTERNS = (
    r"as\s+of[^\w]{0,10}([A-Z][a-z]{2,9}\s+\d{1,2},?\s+\d{4})",
    r"as\s+of[^\w]{0,10}(\d{1,2}/\d{1,2}/\d{2,4})",
    r"as\s+of[^\w]{0,10}(\d{4}-\d{2}-\d{2})",
    r"as\s+of[^\w]{0,10}(\d{1,2}-[A-Za-z]{3}-\d{4})",
    r"holdings[^\w]{0,16}(\d{1,2}/\d{1,2}/\d{2,4})",
    r"date[^\w]{0,6}(\d{4}-\d{2}-\d{2})",
    r"date[^\w]{0,6}(\d{1,2}/\d{1,2}/\d{2,4})",
)

# Last-resort: a bare ISO date in the preamble is overwhelmingly the
# holdings date in these issuer files.
_BARE_ISO_PATTERN = r"(\d{4}-\d{2}-\d{2})"

_AS_OF_KEYS = ("asofdate", "asof", "as_of", "holdingdate", "effectivedate",
               "valuationdate", "date")


class IssuerHoldingsError(Exception):
    """Base for all issuer-holdings failures."""


class IssuerHTTPError(IssuerHoldingsError):
    """Non-200 or transport-level failure."""


class IssuerHTMLRefused(IssuerHoldingsError):
    """Response looked like HTML (login wall / bot check); refused to parse."""


class IssuerParseError(IssuerHoldingsError):
    """Response could not be interpreted as holdings."""


def _norm(text):
    """Normalize a header/key cell for synonym matching."""
    return re.sub(r"[^a-z0-9]", "", str(text).lower())


_COLUMN_LOOKUP = {
    _norm(syn): field
    for field, syns in COLUMN_SYNONYMS.items()
    for syn in syns
}

_last_request_ts = 0.0


def _pace():
    """Enforce polite pacing between requests (skippable in tests)."""
    global _last_request_ts
    if os.environ.get("ISSUER_HOLDINGS_NO_PACE") == "1":
        return
    wait = PACE_SECONDS - (time.monotonic() - _last_request_ts)
    if wait > 0:
        time.sleep(wait)
    _last_request_ts = time.monotonic()


def _sleep_retry(retry_after):
    """Sleep for a 429/503 Retry-After hint, bounded; skipped in tests."""
    if os.environ.get("ISSUER_HOLDINGS_NO_PACE") == "1":
        return
    try:
        delay = min(max(int(str(retry_after).split(",")[0].strip()), 1), 60)
    except (TypeError, ValueError):
        delay = 5
    time.sleep(delay)


def _http_get(url, timeout=30):
    """GET with pacing, UA, bounded body and 429/503 retry.

    Returns ``(status, body)``. Content sniffing is by bytes, so headers
    are not returned. Raises on failure.
    """
    _pace()
    request = urllib.request.Request(
        url,
        headers={
            "User-Agent": UA,
            "Accept": "text/csv,application/json,text/plain,*/*",
        },
    )
    attempts = 0
    while True:
        attempts += 1
        try:
            with urllib.request.urlopen(request, timeout=timeout) as resp:
                status = resp.status
                body = resp.read(MAX_BODY + 1)
        except urllib.error.HTTPError as exc:
            if exc.code in RETRY_STATUSES and attempts < MAX_ATTEMPTS:
                _sleep_retry(exc.headers.get("Retry-After"))
                continue
            raise IssuerHTTPError(f"HTTP {exc.code} for {url}") from exc
        except (urllib.error.URLError, socket.timeout, TimeoutError,
                ssl.SSLError, OSError) as exc:
            raise IssuerHTTPError(f"transport failure for {url}: {exc}") from exc
        if len(body) > MAX_BODY:
            raise IssuerHoldingsError(f"response too large for {url}")
        return status, body


def _looks_like_html(body):
    """True when the response is an HTML page, not holdings data."""
    head = bytes(body[:4096]).lstrip().lower()
    return (
        head.startswith(b"<!doctype html")
        or head.startswith(b"<html")
        or head.startswith(b"<head")
    )


def _check_body(url, body):
    """Refuse HTML/empty/zip responses; return the body when usable."""
    if not body:
        raise IssuerParseError(f"empty response for {url}")
    if _looks_like_html(body):
        raise IssuerHTMLRefused(f"HTML response refused for {url}")
    if body[:2] == b"PK":
        raise IssuerParseError(f"archive (xlsx/zip) response unsupported for {url}")
    return body


def _get_body(url):
    """Fetch a URL and return validated raw bytes."""
    _status, body = _http_get(url)
    return _check_body(url, body)


def _parse_date(text):
    """Parse a date string to ISO YYYY-MM-DD; None when unparseable."""
    if text is None:
        return None
    cleaned = str(text).strip().strip('"').strip("'")
    for fmt in _DATE_FORMATS:
        try:
            return datetime.strptime(cleaned, fmt).date().isoformat()
        except ValueError:
            continue
    return None


def _extract_preamble_as_of(rows):
    """Find the issuer-stated holdings date in CSV preamble rows."""
    text = " ".join(" ".join(str(c) for c in row) for row in rows)
    for pattern in _AS_OF_PATTERNS:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            iso = _parse_date(match.group(1))
            if iso:
                return iso
    match = re.search(_BARE_ISO_PATTERN, text)
    if match:
        return _parse_date(match.group(1))
    return None


def _parse_decimal(text):
    """Source-native decimal as a plain string; None for missing/invalid.

    Commas, currency symbols, and a single trailing percent sign (with
    optional whitespace, e.g. "7.12 %") are stripped; parenthesised
    negatives are honoured. No rescaling: the value keeps the source's
    own units, so "7.12%" parses to "7.12", not "0.0712".
    """
    if text is None:
        return None
    cleaned = str(text).strip().replace(",", "").replace("$", "")
    if cleaned.lower() in MISSING_TOKENS:
        return None
    negative = cleaned.startswith("(") and cleaned.endswith(")")
    if negative:
        cleaned = cleaned[1:-1]
    cleaned = cleaned.rstrip()
    if cleaned.endswith("%"):
        cleaned = cleaned[:-1].rstrip()
    if not re.fullmatch(r"[+-]?\d+(?:\.\d+)?(?:[eE][+-]?\d+)?", cleaned):
        return None
    try:
        value = Decimal(cleaned)
    except InvalidOperation:
        return None
    if negative:
        value = -value
    return format(value, "f")


def _clean_str(text):
    """Trimmed string or None for missing tokens."""
    if text is None:
        return None
    cleaned = str(text).strip()
    if cleaned.lower() in MISSING_TOKENS:
        return None
    return cleaned


def _find_header(rows):
    """Locate the holdings header row; return (index, {field: col})."""
    for i, row in enumerate(rows[:25]):
        mapping = {}
        for j, cell in enumerate(row):
            field = _COLUMN_LOOKUP.get(_norm(cell))
            if field and field not in mapping:
                mapping[field] = j
        if len(mapping) >= 2 and ("ticker" in mapping or "name" in mapping):
            return i, mapping
    return None, {}


def _csv_record(cells, colmap):
    """Build one normalized 10-field record from a CSV row; None to skip."""
    def get(field):
        """Cell value for a canonical field, or None."""
        j = colmap.get(field)
        if j is None or j >= len(cells):
            return None
        return cells[j].strip() or None

    ticker = _clean_str(get("ticker"))
    name = _clean_str(get("name"))
    cusip = _clean_str(get("cusip"))
    isin = _clean_str(get("isin"))
    figi = _clean_str(get("figi"))
    if not any((ticker, name, cusip, isin, figi)):
        return None
    label = f"{ticker or ''} {name or ''}".strip()
    if re.fullmatch(r"totals?", label, re.IGNORECASE) and not any(
        (cusip, isin, figi)
    ):
        return None
    row_as_of = get("as_of")
    return {
        "ticker": ticker,
        "name": name,
        "cusip": cusip,
        "isin": isin,
        "figi": figi,
        "weight": _parse_decimal(get("weight")),
        "shares": _parse_decimal(get("shares")),
        "market_value": _parse_decimal(get("market_value")),
        "asset_class": _clean_str(get("asset_class")),
        "as_of": _parse_date(row_as_of) if row_as_of else None,
    }


def parse_holdings_csv(raw, issuer=""):
    """Parse an issuer holdings CSV.

    Returns ``(as_of, records)`` where ``as_of`` is the file-level stated
    date (preamble, else first row-level date, else None) and ``records``
    are normalized 10-field dicts. Raises on unusable input.
    """
    try:
        text = bytes(raw).decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        raise IssuerParseError(f"csv decode failed: {exc}") from exc
    rows = list(csv.reader(io.StringIO(text)))
    if not rows:
        raise IssuerParseError("empty csv")
    header_idx, colmap = _find_header(rows)
    if header_idx is None:
        raise IssuerParseError("holdings header not found")
    file_as_of = _extract_preamble_as_of(rows[:header_idx])
    records = []
    for cells in rows[header_idx + 1:]:
        if not any(str(c).strip() for c in cells):
            continue
        try:
            record = _csv_record(cells, colmap)
        except (IndexError, ValueError):
            continue
        if record is None:
            continue
        record["as_of"] = record.get("as_of") or file_as_of
        records.append(record)
    if not records:
        raise IssuerParseError("no holding rows parsed")
    if file_as_of is None:
        file_as_of = next(
            (r["as_of"] for r in records if r["as_of"]), None
        )
    return file_as_of, records


def _dict_has_holding_keys(d):
    """True when a JSON object looks like a holding row."""
    hits = 0
    for key in d:
        if _COLUMN_LOOKUP.get(_norm(key)):
            hits += 1
            if hits >= 2:
                return True
    return False


def _find_holdings_list(doc):
    """Breadth-first search for the holdings array in a provider JSON doc."""
    seen = set()
    queue = [doc]
    while queue:
        node = queue.pop(0)
        if id(node) in seen:
            continue
        seen.add(id(node))
        if isinstance(node, list) and len(node) >= 2:
            dicts = [x for x in node if isinstance(x, dict)]
            sample = dicts[:10]
            if len(dicts) >= max(2, len(node) // 2) and sum(
                1 for x in sample if _dict_has_holding_keys(x)
            ) >= 2:
                return node
        if isinstance(node, dict):
            queue.extend(node.values())
        elif isinstance(node, list):
            queue.extend(node)
    return None


def _find_json_as_of(doc):
    """Breadth-first search for a top-level as-of date in a JSON doc."""
    seen = set()
    queue = [doc]
    while queue:
        node = queue.pop(0)
        if id(node) in seen:
            continue
        seen.add(id(node))
        if isinstance(node, dict):
            for key, value in node.items():
                if _norm(key) in _AS_OF_KEYS and isinstance(value, str):
                    iso = _parse_date(value)
                    if iso:
                        return iso
            queue.extend(node.values())
        elif isinstance(node, list):
            queue.extend(node)
    return None


def _json_record(item, as_of):
    """Build one normalized 10-field record from a JSON holding object."""
    keyed = {_norm(k): v for k, v in item.items() if isinstance(k, str)}

    def get(field):
        """Value for a canonical field from the JSON object, or None."""
        for syn in COLUMN_SYNONYMS[field]:
            if _norm(syn) in keyed:
                return keyed[_norm(syn)]
        return None

    ticker = _clean_str(get("ticker"))
    name = _clean_str(get("name"))
    cusip = _clean_str(get("cusip"))
    isin = _clean_str(get("isin"))
    figi = _clean_str(get("figi"))
    if not any((ticker, name, cusip, isin, figi)):
        return None
    row_as_of = get("as_of")
    parsed_row_as_of = _parse_date(row_as_of) if row_as_of else None
    return {
        "ticker": ticker,
        "name": name,
        "cusip": cusip,
        "isin": isin,
        "figi": figi,
        "weight": _parse_decimal(get("weight")),
        "shares": _parse_decimal(get("shares")),
        "market_value": _parse_decimal(get("market_value")),
        "asset_class": _clean_str(get("asset_class")),
        "as_of": parsed_row_as_of or as_of,
    }


def parse_vanguard_json(raw):
    """Parse a Vanguard portfolio-holdings JSON payload.

    Returns ``(as_of, records)``; tolerant of undocumented nesting via a
    synonym-guided search. Raises on unusable input.
    """
    try:
        doc = json.loads(bytes(raw).decode("utf-8-sig"))
    except (UnicodeDecodeError, ValueError) as exc:
        raise IssuerParseError(f"vanguard json decode failed: {exc}") from exc
    if not isinstance(doc, dict):
        raise IssuerParseError("vanguard payload is not an object")
    holdings = _find_holdings_list(doc)
    if holdings is None:
        raise IssuerParseError("holdings array not found in vanguard payload")
    as_of = _find_json_as_of(doc)
    records = []
    for item in holdings:
        if not isinstance(item, dict):
            continue
        try:
            record = _json_record(item, as_of)
        except (ValueError, TypeError):
            continue
        if record is not None:
            records.append(record)
    if not records:
        raise IssuerParseError("no holding rows parsed from vanguard payload")
    return as_of, records


def fetch_ishares_csv(ticker, product_map):
    """Fetch an iShares holdings CSV; returns (source_url, raw_bytes)."""
    pid = (product_map or {}).get(str(ticker).strip().upper())
    if pid is None:
        raise IssuerHoldingsError(f"no iShares portfolioId for {ticker}")
    url = ISHARES_DOWNLOAD_URL.format(pid=pid)
    return url, _get_body(url)


def fetch_vanguard_json(ticker):
    """Fetch a Vanguard portfolio-holdings JSON payload."""
    url = VANGUARD_URL.format(ticker=str(ticker).strip().upper())
    return url, _get_body(url)


def fetch_ssga_csv(ticker):
    """Fetch a State Street (SPDR) holdings CSV."""
    url = SSGA_URL.format(ticker=str(ticker).strip().upper())
    return url, _get_body(url)


def fetch_invesco_csv(ticker):
    """Fetch an Invesco holdings CSV."""
    url = INVESCO_URL.format(ticker=str(ticker).strip().upper())
    return url, _get_body(url)


def fetch_schwab_csv(ticker):
    """Fetch a Schwab Asset Management holdings CSV."""
    url = SCHWAB_URL.format(ticker=str(ticker).strip().lower())
    return url, _get_body(url)


def _fetch_etf_holdings(ticker, issuer, product_map):
    """Fetch and parse one ETF; raises on any failure."""
    tick = str(ticker).strip().upper()
    if issuer == "ishares":
        url, body = fetch_ishares_csv(tick, product_map)
        kind = "csv"
        as_of, records = parse_holdings_csv(body, issuer)
    elif issuer == "vanguard":
        url, body = fetch_vanguard_json(tick)
        kind = "json"
        as_of, records = parse_vanguard_json(body)
    elif issuer == "ssga":
        url, body = fetch_ssga_csv(tick)
        kind = "csv"
        as_of, records = parse_holdings_csv(body, issuer)
    elif issuer == "invesco":
        url, body = fetch_invesco_csv(tick)
        kind = "csv"
        as_of, records = parse_holdings_csv(body, issuer)
    elif issuer == "schwab":
        url, body = fetch_schwab_csv(tick)
        kind = "csv"
        as_of, records = parse_holdings_csv(body, issuer)
    else:
        raise IssuerHoldingsError(f"unknown issuer {issuer!r}")
    return {
        "ticker": tick,
        "issuer": issuer,
        "source_url": url,
        "raw_kind": kind,
        "raw": body,
        "as_of": as_of,
        "records": records,
    }


def fetch_etf_holdings(ticker, issuer, product_map=None):
    """Fetch and parse one ETF's issuer-published holdings.

    Fail-soft: returns the result dict or ``None`` on any failure, so one
    ETF can never fail a whole run.
    """
    try:
        return _fetch_etf_holdings(ticker, issuer, product_map)
    except (
        IssuerHoldingsError,
        urllib.error.URLError,
        socket.timeout,
        TimeoutError,
        ssl.SSLError,
        OSError,
        ValueError,
        KeyError,
        TypeError,
        IndexError,
        UnicodeDecodeError,
    ):
        return None


def load_ishares_product_map(now, read_state, write_state, http_get=None):
    """Return ``{TICKER: portfolio_id}`` for iShares funds.

    Cached at ``data/_state/ishares-product-map.json`` and refreshed weekly
    from the iShares product screener. ``read_state``/``write_state`` are
    caller-supplied callables (key -> dict / (key, dict) -> None).
    Fail-soft: returns a stale cache, else ``{}``; never raises.
    """
    get = http_get or _http_get
    key = "data/_state/ishares-product-map.json"
    stale = None
    try:
        cached = read_state(key)
        if isinstance(cached, dict) and isinstance(cached.get("map"), dict):
            stale = cached
            refreshed = cached.get("refreshed_at")
            if refreshed:
                ref_dt = datetime.fromisoformat(str(refreshed))
                if ref_dt.tzinfo is None:
                    ref_dt = ref_dt.replace(tzinfo=timezone.utc)
                age_days = (now - ref_dt).total_seconds() / 86400
                if 0 <= age_days < 7:
                    return cached["map"]
    except (OSError, ValueError, KeyError, TypeError):
        stale = None
    try:
        _status, body = get(ISHARES_SCREENER_URL)
        doc = json.loads(bytes(body).decode("utf-8-sig"))
        mapping = {}
        for pid, values in doc.items():
            tick = values.get("localExchangeTicker")
            if tick and str(values.get("portfolioId")) == str(pid):
                mapping[str(tick).upper()] = int(pid)
        if not mapping:
            raise IssuerParseError("empty iShares screener map")
        payload = {
            "refreshed_at": now.isoformat(),
            "count": len(mapping),
            "map": mapping,
        }
        try:
            write_state(key, payload)
        except (OSError, ValueError, TypeError):
            pass
        return mapping
    except (
        IssuerHoldingsError,
        urllib.error.URLError,
        socket.timeout,
        TimeoutError,
        ssl.SSLError,
        OSError,
        ValueError,
        KeyError,
        TypeError,
    ):
        if stale is not None:
            return stale["map"]
        return {}
