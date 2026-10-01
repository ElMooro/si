"""Shared FINRA TRACE / fixed-income aggregate fetcher (Bloomberg parity 6/10).

Docs-shaped client for the FINRA Data API fixedIncomeMarket group:
  - treasuryDailyAggregates   (daily Treasury trading aggregates by bucket)
  - corporateDebtMarketBreadth (daily corporate market breadth snapshot)
  - trace                     (per-print TRACE records; aggregated client-side in Phase 1)

AUTH: tries keyless first, falls back to OAuth2 via finra_si's helpers.
FINRA Gateway registration is NOT required for the keyless path — if the
endpoint demands auth and no SSM creds exist, every function fail-softs to
None and consumers keep running on their proxy fallback.

USAGE in a Lambda (shared modules are bundled into every Lambda zip):

    import finra_trace
    rows = finra_trace.fetch_treasury_daily("2026-09-29")

All public functions fail-soft: they return None on any network, auth,
or parsing failure. No exceptions escape this module.
"""
import datetime
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


def fetch_treasury_daily(trade_date):
    """Fetch treasuryDailyAggregates rows for one tradeDate (YYYY-MM-DD).

    Returns a list of per-bucket dicts (documented fields only), or None.
    """
    # FINRA rejects sortFields unless the partition key (tradeDate) is pinned
    # with an EQUAL compareFilter (HTTP 400 otherwise); a dateRangeFilter
    # alone does not satisfy the rule.
    rows = query(
        "treasuryDailyAggregates",
        compare_filters=[{
            "fieldName": "tradeDate",
            "compareType": "EQUAL",
            "fieldValue": trade_date,
        }],
        limit=5000,
        sort_fields=["yearsToMaturity"],
    )
    if not rows:
        return None
    out = []
    for r in rows:
        if not isinstance(r, dict):
            continue
        out.append({f: r.get(f) for f in TREASURY_FIELDS})
    return out or None


# Legacy consumer-facing key names (kept stable for the bond-trace lambda).
# Sourced live from the `corporateMarketBreadth` dataset (verified 2026-10-01):
#   tradeDate <- tradeReportDate, numberOfIssuesAdvancing <- advances,
#   numberOfIssuesDeclining <- declines, numberOfIssuesUnchanged <- unchanged,
#   numberOfTrades <- totalTrades, parValueTraded <- totalVolume.
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


def fetch_corporate_breadth():
    """Fetch the latest corporateMarketBreadth snapshot.

    FINRA's breadth datasets key their date as `tradeReportDate` (there is
    no `tradeDate` field; the retired `corporateDebtMarketBreadth` name 404s).
    Rows are per productCategory, so the latest date's rows are summed into
    one market-wide snapshot. Returns legacy-shaped dict (see
    CORPORATE_BREADTH_FIELDS), or None.
    """
    # Cannot EQUAL-pin an unknown latest date, and FINRA forbids sortFields
    # without one — pull the last 5 weekdays unsorted and pick the latest
    # tradeReportDate client-side.
    start, end = _weekday_window(5)
    rows = query(
        BREADTH_DATASET,
        date_range_filters=[{
            "fieldName": BREADTH_DATE_FIELD,
            "startDate": start,
            "endDate": end,
        }],
        limit=500,
    )
    if not rows:
        return None
    dated = [r for r in rows if isinstance(r, dict) and r.get(BREADTH_DATE_FIELD)]
    if not dated:
        return None
    latest = max(r[BREADTH_DATE_FIELD] for r in dated)
    day_rows = [r for r in dated if r[BREADTH_DATE_FIELD] == latest]

    def _f(r, k):
        try:
            v = r.get(k)
            return float(v) if v is not None else 0.0
        except (TypeError, ValueError):
            return 0.0

    agg = {f: 0.0 for f in CORPORATE_BREADTH_FIELDS}
    agg["tradeDate"] = latest
    for r in day_rows:
        for legacy, live in _BREADTH_FIELD_MAP.items():
            if legacy == "tradeDate":
                continue
            agg[legacy] += _f(r, live)
    # numberOfIssues is not published separately; derive from components
    agg["numberOfIssues"] = (agg["numberOfIssuesAdvancing"]
                             + agg["numberOfIssuesDeclining"]
                             + agg["numberOfIssuesUnchanged"])
    return agg


def fetch_trace_aggregates(trade_date):
    """Derive TRACE-style aggregates for a trade date.

    There is no per-print TRACE dataset on the FINRA Query API (the
    `trace` name 404s; true tick data is a delayed academic product), so
    this aggregates the `corporateMarketBreadth` snapshot for the date:
    total trades -> n_prints, total volume -> total_volume. Returns the
    same dict shape as before, or None.
    """
    rows = query(
        BREADTH_DATASET,
        compare_filters=[{
            "fieldName": BREADTH_DATE_FIELD,
            "compareType": "EQUAL",
            "fieldValue": trade_date,
        }],
        limit=500,
    )
    if not rows:
        return None
    n_prints = 0
    total_volume = 0.0
    for r in rows:
        if not isinstance(r, dict):
            continue
        try:
            t = r.get("totalTrades")
            if t is not None:
                n_prints += int(float(t))
        except (TypeError, ValueError):
            pass
        try:
            v = r.get("totalVolume")
            if v is not None:
                total_volume += float(v)
        except (TypeError, ValueError):
            pass
    return {
        "trade_date": trade_date,
        "n_prints": n_prints,
        "total_volume": total_volume,
        "avg_price_change": None,
        "source": BREADTH_DATASET,
    }



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
