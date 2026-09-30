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
    rows = query(
        "treasuryDailyAggregates",
        date_range_filters=[{
            "fieldName": "tradeDate",
            "startDate": trade_date,
            "endDate": trade_date,
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


CORPORATE_BREADTH_FIELDS = (
    "tradeDate", "numberOfIssues", "numberOfIssuesAdvancing",
    "numberOfIssuesDeclining", "numberOfIssuesUnchanged",
    "parValueTraded", "numberOfTrades", "averagePriceChange",
)


def fetch_corporate_breadth():
    """Fetch the latest corporateDebtMarketBreadth snapshot.

    Returns a dict of documented fields, or None.
    """
    rows = query(
        "corporateDebtMarketBreadth",
        limit=1,
        sort_fields=["-tradeDate"],
    )
    if not rows:
        return None
    r = rows[0]
    if not isinstance(r, dict):
        return None
    return {f: r.get(f) for f in CORPORATE_BREADTH_FIELDS}


def fetch_trace_aggregates(trade_date):
    """Fetch per-print trace rows for a tradeDate and aggregate client-side.

    Phase 1 keeps this coarse: total volume, print count, and average
    price change across whatever rows the endpoint returns (limit 5000).
    Returns a dict, or None.
    """
    rows = query(
        "trace",
        date_range_filters=[{
            "fieldName": "tradeDate",
            "startDate": trade_date,
            "endDate": trade_date,
        }],
        limit=5000,
        sort_fields=["-tradeDate"],
    )
    if not rows:
        return None
    total_volume = 0.0
    n_prints = 0
    px_changes = []
    for r in rows:
        if not isinstance(r, dict):
            continue
        n_prints += 1
        vol = r.get("volume")
        if vol is None:
            vol = r.get("parValueTraded")
        try:
            if vol is not None:
                total_volume += float(vol)
        except (TypeError, ValueError):
            pass
        pc = r.get("priceChange")
        if pc is None:
            pc = r.get("averagePriceChange")
        try:
            if pc is not None:
                px_changes.append(float(pc))
        except (TypeError, ValueError):
            pass
    return {
        "trade_date": trade_date,
        "n_prints": n_prints,
        "total_volume": total_volume,
        "avg_price_change": (
            sum(px_changes) / len(px_changes) if px_changes else None
        ),
        "n_with_price_change": len(px_changes),
    }


# ---------- Health check ----------
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
        rows = query("treasuryDailyAggregates", limit=1,
                     sort_fields=["-tradeDate"])
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
