"""FINRA fixed-income aggregate client.

Uses documented treasuryDailyAggregates and corporateMarketBreadth datasets.
Aggregate reports are not executable quotes, individual trades, dealer
positions or fund flows. API entitlement/reachability and source completeness
remain separate qualifications. Existing optional OAuth credentials are used
when available; no credential provisioning or paid data subscription occurs.

Requests fail softly on network/auth/JSON errors. Dated snapshots retain the
complete bounded response and its canonical hash; an invalid or saturated
response is unavailable rather than silently truncated. The deprecated
fetch_trace_aggregates API returns None without making a request.
"""
import datetime
import hashlib
import math
import json
import urllib.error
import urllib.request

# ---------- Endpoints ----------
FINRA_DATA_BASE = "https://api.finra.org/data"
FIXED_INCOME_GROUP = "group/fixedIncomeMarket/name"

# ---------- Config ----------
HTTP_TIMEOUT = 25
USER_AGENT = "JustHodl-TRACE-Client/1.0"

_finra_si_module = None
_finra_si_probed = False


def _finra_si():
    """Lazily import finra_si (bundled shared module); None if unavailable."""
    global _finra_si_module, _finra_si_probed
    if not _finra_si_probed:
        _finra_si_probed = True
        try:
            import finra_si as _mod
            _finra_si_module = _mod
        except Exception as e:
            print(f"[finra_trace] finra_si import failed: {e}")
            _finra_si_module = None
    return _finra_si_module


def get_token():
    """Return a FINRA access token, or None for keyless mode.

    Keyless is tried implicitly by the caller (token=None means no
    Authorization header). OAuth2 is only attempted when finra_si is
    importable; if SSM creds are missing, get_access_token raises and we
    return None so the request still goes out keyless.
    """
    mod = _finra_si()
    if mod is None:
        return None
    try:
        return mod.get_access_token()
    except Exception as e:
        print(f"[finra_trace] OAuth2 unavailable, keyless mode: {e}")
        return None


def _post(path, payload):
    """POST a JSON query to the FINRA Data API. Returns parsed JSON or None."""
    url = f"{FINRA_DATA_BASE}/{path}"
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "User-Agent": USER_AGENT,
    }
    token = get_token()
    if token:
        headers["Authorization"] = f"Bearer {token}"
    req = urllib.request.Request(
        url,
        data=json.dumps(payload).encode("utf-8"),
        method="POST",
        headers=headers,
    )
    try:
        with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT) as r:
            return json.loads(r.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        body = ""
        try:
            body = e.read()[:300].decode("utf-8", errors="replace")
        except Exception:
            pass
        print(f"[finra_trace] HTTP {e.code} on {path}: {body[:200]}")
    except (urllib.error.URLError, TimeoutError, OSError) as e:
        print(f"[finra_trace] network error on {path}: {e}")
    except (json.JSONDecodeError, UnicodeDecodeError, ValueError) as e:
        print(f"[finra_trace] bad response on {path}: {e}")
    return None


def query(dataset, compare_filters=None, date_range_filters=None,
          limit=5000, sort_fields=None):
    """Generic POST query against a fixedIncomeMarket dataset.

    dataset: e.g. "treasuryDailyAggregates".
    Returns the row list, or None on any failure.
    """
    payload = {"limit": limit}
    if compare_filters:
        payload["compareFilters"] = compare_filters
    if date_range_filters:
        payload["dateRangeFilters"] = date_range_filters
    if sort_fields:
        payload["sortFields"] = sort_fields
    data = _post(f"{FIXED_INCOME_GROUP}/{dataset}", payload)
    if isinstance(data, list):
        return data
    return None


def _weekday_window(n):
    """Return (start, end) ISO date strings covering the last n weekdays."""
    days = []
    d = datetime.date.today()
    while len(days) < n:
        if d.weekday() < 5:
            days.append(d.isoformat())
        d -= datetime.timedelta(days=1)
    return days[-1], days[0]


# ---------- Dataset-specific fetchers ----------

TREASURY_FIELDS = (
    "tradeDate", "productCategory", "yearsToMaturity", "benchmark",
    "volumeWeightedAveragePrice",
    "atsInterdealerCount", "atsInterdealerVolume",
    "dealerCustomerCount", "dealerCustomerVolume",
)


# Existing aliases are retained; only the all-securities row can populate them.
CORPORATE_BREADTH_FIELDS = (
    "tradeDate", "numberOfIssues", "numberOfIssuesAdvancing",
    "numberOfIssuesDeclining", "numberOfIssuesUnchanged",
    "parValueTraded", "numberOfTrades", "averagePriceChange",
)
BREADTH_DATASET = "corporateMarketBreadth"
BREADTH_DATE_FIELD = "tradeReportDate"
_BREADTH_FIELD_MAP = {
    "tradeDate": "tradeReportDate",
    "numberOfIssuesAdvancing": "advances",
    "numberOfIssuesDeclining": "declines",
    "numberOfIssuesUnchanged": "unchanged",
    "numberOfTrades": "totalTrades",
    "parValueTraded": "totalVolume",
}


def _calendar_date(value):
    """Only an exact ISO calendar date can identify an observation."""
    if type(value) is not str:
        return None
    try:
        day = datetime.date.fromisoformat(value)
        return value if day.isoformat() == value else None
    except ValueError:
        return None


def _finite_number(value, count=False):
    """Preserve missingness; JSON booleans and strings are not measurements."""
    if type(value) not in (int, float):
        return None
    try:
        number = float(value)
    except (OverflowError, ValueError):
        return None
    if not math.isfinite(number) or number < 0:
        return None
    if count and (number > 9007199254740991 or not number.is_integer()):
        return None
    return int(number) if count else number


def _snapshot(dataset, date_field, rows, start, end, limit, dimensions):
    """Bind a complete bounded response to dated, unique category identities.

    Saturation cannot establish completeness. Any malformed date or duplicate
    category/date rejects this response rather than choosing an arbitrary row.
    All returned row fields are retained; this function does not normalize units.
    """
    if not isinstance(rows, list) or not rows or len(rows) >= limit:
        return None
    seen = set()
    for row in rows:
        if not isinstance(row, dict):
            return None
        day = _calendar_date(row.get(date_field))
        if day is None or not start <= day <= end:
            return None
        identity = [day]
        for field in dimensions:
            value = row.get(field)
            if value is not None and (type(value) is not str or not value.strip()):
                return None
            if field == 'productCategory' and value is None:
                return None
            identity.append(value)
        identity = tuple(identity)
        if identity in seen:
            return None
        seen.add(identity)
    try:
        canonical = json.dumps(rows, sort_keys=True, separators=(',', ':'), allow_nan=False).encode('utf-8')
    except (TypeError, ValueError, OverflowError):
        return None
    latest = max(row[date_field] for row in rows)
    selected = [dict(row) for row in rows if row[date_field] == latest]
    return {
        'contract_version': 'finra-aggregates-v1',
        'dataset': dataset,
        'source_url': f'{FINRA_DATA_BASE}/{FIXED_INCOME_GROUP}/{dataset}',
        'documentation_url': 'https://developer.finra.org/docs',
        'observation_date': latest,
        'observation_date_field': date_field,
        'retrieved_at': datetime.datetime.now(datetime.timezone.utc).isoformat(),
        'request_window': {'start': start, 'end': end, 'limit': limit},
        'returned_rows': len(rows),
        'query_limit_reached': False,
        'response_sha256': hashlib.sha256(canonical).hexdigest(),
        'rows': selected,
        'source_rows': [dict(row) for row in rows],
        'coverage': 'returned distinct categories; expected provider coverage not certified',
    }


def fetch_treasury_daily(trade_date):
    """Compatibility API: one exact, validated observation date, or None."""
    if _calendar_date(trade_date) is None:
        return None
    rows = query('treasuryDailyAggregates', compare_filters=[{
        'fieldName': 'tradeDate', 'compareType': 'EQUAL', 'fieldValue': trade_date,
    }], limit=5000, sort_fields=['yearsToMaturity'])
    snapshot = _snapshot('treasuryDailyAggregates', 'tradeDate', rows,
                         trade_date, trade_date, 5000,
                         ('productCategory', 'yearsToMaturity', 'benchmark'))
    return snapshot['rows'] if snapshot else None


def fetch_treasury_latest():
    """Use returned dates, not the run date; FINRA publishes prior-day data."""
    start, end = _weekday_window(5)
    rows = query('treasuryDailyAggregates', date_range_filters=[{
        'fieldName': 'tradeDate', 'startDate': start, 'endDate': end,
    }], limit=5000)
    return _snapshot('treasuryDailyAggregates', 'tradeDate', rows, start, end,
                     5000, ('productCategory', 'yearsToMaturity', 'benchmark'))


def fetch_corporate_breadth():
    """Return dated corporateMarketBreadth categories without adding subsets."""
    start, end = _weekday_window(5)
    rows = query(BREADTH_DATASET, date_range_filters=[{
        'fieldName': BREADTH_DATE_FIELD, 'startDate': start, 'endDate': end,
    }], limit=500)
    snapshot = _snapshot(BREADTH_DATASET, BREADTH_DATE_FIELD, rows, start,
                         end, 500, ('productCategory',))
    if snapshot is None:
        return None
    all_rows = [row for row in snapshot['rows'] if row['productCategory'] == 'all securities']
    all_row = all_rows[0] if len(all_rows) == 1 else {}
    for legacy, field in _BREADTH_FIELD_MAP.items():
        snapshot[legacy] = snapshot['observation_date'] if legacy == 'tradeDate' else _finite_number(all_row.get(field), count=field != 'totalVolume')
    counts = [snapshot[key] for key in ('numberOfIssuesAdvancing', 'numberOfIssuesDeclining', 'numberOfIssuesUnchanged')]
    total = sum(counts) if all(value is not None for value in counts) else None
    snapshot['numberOfIssues'] = total if total is not None and total <= 9007199254740991 else None
    snapshot['averagePriceChange'] = None
    snapshot['legacy_alias_scope'] = 'all securities row only; category subsets are not summed'
    snapshot['parValueTraded_unit'] = 'source_native_unverified'
    return snapshot


def fetch_trace_aggregates(trade_date):
    """Deprecated compatibility API: no documented per-print dataset here.

    The separate TRACE API is not a fixedIncomeMarket Data API dataset. Do
    not request an invented endpoint or call a capped sample a market total.
    """
    return None


def health_check():
    """Returns a dict describing auth/data reachability. Does NOT raise."""
    out = {
        "token_available": False,
        "data_api_reachable": False,
        "latest_treasury_row": None,
        "error": None,
    }
    try:
        tok = get_token()
        out["token_available"] = bool(tok)
        start, end = _weekday_window(5)
        rows = query("treasuryDailyAggregates",
                     date_range_filters=[{
                         "fieldName": "tradeDate",
                         "startDate": start,
                         "endDate": end,
                     }],
                     limit=1)
        if rows:
            out["data_api_reachable"] = True
            out["latest_treasury_row"] = (
                rows[0].get("tradeDate") if isinstance(rows[0], dict)
                else None
            )
        else:
            out["error"] = "no rows returned"
    except Exception as e:
        out["error"] = str(e)
    return out
