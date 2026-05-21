"""Shared FINRA Gateway OAuth2 auth + bi-monthly Short Interest fetcher.

USAGE in a Lambda:

    from finra_si import fetch_short_interest_latest

    # Fetch latest SI snapshot for a list of tickers
    rows = fetch_short_interest_latest(["AAPL", "MSFT", "NVDA"])
    # rows = [{ticker, settlement_date, current_short_position,
    #          previous_short_position, change_percent, avg_daily_volume,
    #          days_to_cover, market_class, exchange_code, ...}, ...]


CREDENTIAL SETUP (one-time, Khalid action — ~5 minutes)

  1. Register at https://gateway.finra.org/
     - "Sign Up" → "Developer / Data Consumer" account type
     - Free; takes 2-3 business days for FINRA approval

  2. Once approved, log in to gateway.finra.org/developer
     - Create an API client: "Create Application" → name "JustHodl-SI"
     - Pick scope: `data.public.read` (covers equityShortInterestStandardized)
     - Note the assigned clientId + clientSecret (shown ONCE; copy both)

  3. Add credentials to AWS SSM Parameter Store:
        aws ssm put-parameter \\
          --name /justhodl/finra/client_id \\
          --value YOUR_CLIENT_ID \\
          --type SecureString --region us-east-1
        aws ssm put-parameter \\
          --name /justhodl/finra/client_secret \\
          --value YOUR_CLIENT_SECRET \\
          --type SecureString --region us-east-1

  After step 3, all Lambdas using this module pick up creds automatically.


DESIGN

  - OAuth2 client-credentials grant (no user, machine-to-machine).
  - Token endpoint: https://ews.fip.finra.org/fip/rest/ews/oauth2/access_token
  - Token TTL ~60 minutes per FINRA defaults; cached in SSM at
    /justhodl/finra/access_token with TTL-aware refresh.
  - Data API base: https://api.finra.org
  - Dataset: data/group/otcMarket/name/equityShortInterestStandardized
    (post-2021 includes exchange-listed equities, not OTC-only despite the
    "otcMarket" group naming — confirmed via ops 1017 schema probe).
  - Filters supported via POST JSON body (compareFilters, dateRangeFilters,
    limit, offset, sortFields). GET only supports basic pagination, not
    filter/sort (confirmed ops 1020).

REFERENCES
  https://gateway.finra.org/developer/  (registration + API console)
  https://www.finra.org/finra-data/browse-catalog/equity-short-interest/data
  https://www.finra.org/sites/default/files/Equity_Short_Interest_Data_File_Download_API.pdf
"""
import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone

import boto3

# ---------- Endpoints ----------
FINRA_TOKEN_URL = "https://ews.fip.finra.org/fip/rest/ews/oauth2/access_token"
FINRA_DATA_BASE = "https://api.finra.org/data"
EQUITY_SI_DATASET = "group/otcMarket/name/equityShortInterestStandardized"

# ---------- SSM keys ----------
SSM_CLIENT_ID = "/justhodl/finra/client_id"
SSM_CLIENT_SECRET = "/justhodl/finra/client_secret"
SSM_TOKEN_CACHE = "/justhodl/finra/access_token"

# ---------- Config ----------
HTTP_TIMEOUT = 25
TOKEN_REFRESH_BUFFER_SEC = 120   # refresh 2 min before expiry
DEFAULT_PAGE_SIZE = 100

_ssm = None


def _ssm_client():
    global _ssm
    if _ssm is None:
        _ssm = boto3.client("ssm", region_name="us-east-1")
    return _ssm


class FinraAuthError(Exception):
    """Raised when FINRA OAuth2 token acquisition fails."""


# ---------- Credentials ----------
def _get_credentials():
    """Pull clientId + clientSecret from SSM. Raises if missing."""
    ssm = _ssm_client()
    try:
        cid = ssm.get_parameter(Name=SSM_CLIENT_ID, WithDecryption=True
                                 )["Parameter"]["Value"]
        sec = ssm.get_parameter(Name=SSM_CLIENT_SECRET, WithDecryption=True
                                 )["Parameter"]["Value"]
    except Exception as e:
        raise FinraAuthError(
            f"FINRA credentials not configured in SSM ({e}). "
            f"See module docstring for one-time setup instructions."
        ) from e
    if not cid or not sec:
        raise FinraAuthError("FINRA SSM credentials are empty")
    return cid, sec


# ---------- Token cache ----------
def _read_cached_token():
    """Return (token, expires_at_unix) or (None, 0) if missing/expired."""
    ssm = _ssm_client()
    try:
        raw = ssm.get_parameter(Name=SSM_TOKEN_CACHE,
                                 WithDecryption=True)["Parameter"]["Value"]
        d = json.loads(raw)
        tok = d.get("access_token")
        exp = d.get("expires_at_unix", 0)
        if tok and exp > time.time() + TOKEN_REFRESH_BUFFER_SEC:
            return tok, exp
    except Exception:
        pass
    return None, 0


def _write_cached_token(token, expires_at_unix):
    ssm = _ssm_client()
    try:
        ssm.put_parameter(
            Name=SSM_TOKEN_CACHE,
            Value=json.dumps({"access_token": token,
                               "expires_at_unix": expires_at_unix,
                               "cached_at_iso": datetime.now(
                                   timezone.utc).isoformat()}),
            Type="SecureString",
            Overwrite=True,
        )
    except Exception as e:
        # Cache write is best-effort; auth still works without it.
        print(f"[finra_si] token cache write failed (non-fatal): {e}")


def _request_new_token():
    """Hit FINRA token endpoint with client-credentials grant."""
    cid, sec = _get_credentials()
    # FINRA uses HTTP Basic auth on the token endpoint
    import base64
    basic = base64.b64encode(f"{cid}:{sec}".encode("utf-8")
                              ).decode("ascii")
    payload = urllib.parse.urlencode({"grant_type": "client_credentials"}
                                      ).encode("ascii")
    req = urllib.request.Request(
        FINRA_TOKEN_URL,
        data=payload,
        method="POST",
        headers={
            "Authorization": f"Basic {basic}",
            "Content-Type": "application/x-www-form-urlencoded",
            "Accept": "application/json",
            "User-Agent": "JustHodl-FINRA-Client/1.0",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT) as r:
            data = json.loads(r.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        body = ""
        try:
            body = e.read()[:400].decode("utf-8", errors="replace")
        except Exception:
            pass
        raise FinraAuthError(
            f"FINRA token endpoint returned {e.code}: {body}") from e
    except Exception as e:
        raise FinraAuthError(f"FINRA token request failed: {e}") from e

    tok = data.get("access_token")
    expires_in = int(data.get("expires_in", 3600))
    if not tok:
        raise FinraAuthError(f"No access_token in FINRA response: {data}")
    expires_at = int(time.time()) + expires_in
    _write_cached_token(tok, expires_at)
    return tok, expires_at


def get_access_token():
    """Cache-aware access-token getter. Returns a string token."""
    tok, _ = _read_cached_token()
    if tok:
        return tok
    tok, _ = _request_new_token()
    return tok


# ---------- Data API (POST with auth) ----------
def _post_query(dataset_path, payload, retries=2):
    """POST a query to FINRA Data API. dataset_path = group/X/name/Y."""
    last_err = None
    for attempt in range(retries + 1):
        try:
            tok = get_access_token()
            url = f"{FINRA_DATA_BASE}/{dataset_path}"
            req = urllib.request.Request(
                url,
                data=json.dumps(payload).encode("utf-8"),
                method="POST",
                headers={
                    "Authorization": f"Bearer {tok}",
                    "Content-Type": "application/json",
                    "Accept": "application/json",
                    "User-Agent": "JustHodl-FINRA-Client/1.0",
                },
            )
            with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT) as r:
                return json.loads(r.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            last_err = e
            body = ""
            try:
                body = e.read()[:400].decode("utf-8", errors="replace")
            except Exception:
                pass
            # 401 = stale token; bust cache and retry
            if e.code == 401 and attempt == 0:
                print("[finra_si] 401 received — busting token cache "
                      "and retrying")
                _ssm_client().delete_parameter(Name=SSM_TOKEN_CACHE)
                continue
            print(f"[finra_si] HTTP {e.code}: {body[:200]}")
            if e.code >= 500 and attempt < retries:
                time.sleep(2 ** attempt)
                continue
            raise
        except Exception as e:
            last_err = e
            if attempt < retries:
                time.sleep(2 ** attempt)
                continue
            raise
    raise last_err or RuntimeError("post_query exhausted retries")


def _normalize_row(row):
    """Map FINRA's verbose field names to our internal schema."""
    return {
        "ticker": row.get("securitiesInformationProcessorSymbolIdentifier"),
        "issue_name": row.get("issueName"),
        "settlement_date": row.get("settlementDate"),
        "accounting_date": row.get("accountingDate"),
        "current_short_position": row.get("currentShortPositionQuantity"),
        "previous_short_position": row.get("previousShortPositionQuantity"),
        "change_percent": row.get("changePercent"),
        "change_previous_number": row.get("changePreviousNumber"),
        "avg_daily_volume": row.get("averageDailyVolumeQuantity"),
        "days_to_cover": row.get("daysToCoverQuantity"),
        "market_class": row.get("marketClassCode"),
        "exchange_code": row.get("issuerServicesGroupExchangeCode"),
        "stock_split_flag": row.get("stockSplitFlag"),
        "revision_flag": row.get("revisionFlag"),
    }


# ---------- Public API ----------
def fetch_short_interest_for_ticker(ticker, n_periods=4):
    """Get most recent N bi-monthly SI snapshots for one ticker.

    Returns list of normalized rows, latest first (sorted by settlementDate
    desc). Empty list if ticker has no SI history in dataset.
    """
    payload = {
        "limit": n_periods,
        "compareFilters": [
            {"compareType": "EQUAL",
             "fieldName": "securitiesInformationProcessorSymbolIdentifier",
             "fieldValue": ticker.upper()}
        ],
        "sortFields": ["-settlementDate"],
    }
    data = _post_query(EQUITY_SI_DATASET, payload)
    if not isinstance(data, list):
        return []
    return [_normalize_row(r) for r in data if isinstance(r, dict)]


def fetch_short_interest_latest(tickers, fail_open=True):
    """Get the latest SI snapshot for each ticker in the list.

    Returns dict {ticker: row} for tickers that had data. Missing tickers
    are silently omitted unless fail_open=False.
    """
    out = {}
    for t in tickers:
        try:
            rows = fetch_short_interest_for_ticker(t, n_periods=1)
            if rows:
                out[t.upper()] = rows[0]
        except Exception as e:
            if not fail_open:
                raise
            print(f"[finra_si] fetch failed for {t}: {e}")
    return out


def fetch_latest_settlement_date_snapshot(limit=5000):
    """Get all tickers' latest SI rows for the most recent settlement date.

    Determines the max settlementDate in the dataset, then fetches ALL
    rows for that date. Use sparingly — pulls thousands of records.
    """
    # Step 1: find latest settlement date
    payload_max = {"limit": 1, "sortFields": ["-settlementDate"]}
    data = _post_query(EQUITY_SI_DATASET, payload_max)
    if not isinstance(data, list) or not data:
        return []
    latest = data[0].get("settlementDate")
    if not latest:
        return []
    # Step 2: fetch all rows for that settlement date
    payload_all = {
        "limit": limit,
        "compareFilters": [
            {"compareType": "EQUAL", "fieldName": "settlementDate",
             "fieldValue": latest}
        ],
    }
    rows = _post_query(EQUITY_SI_DATASET, payload_all)
    return [_normalize_row(r) for r in (rows or [])
            if isinstance(r, dict)]


# ---------- Health check ----------
def health_check():
    """Returns dict suitable for ops verifiers — does NOT raise.

    Use to confirm credentials wired up correctly without doing real
    data work."""
    out = {
        "credentials_configured": False,
        "token_acquired": False,
        "data_api_reachable": False,
        "latest_settlement_date_in_dataset": None,
        "error": None,
    }
    try:
        cid, _ = _get_credentials()
        out["credentials_configured"] = bool(cid)
    except FinraAuthError as e:
        out["error"] = str(e)
        return out
    try:
        tok = get_access_token()
        out["token_acquired"] = bool(tok)
    except FinraAuthError as e:
        out["error"] = str(e)
        return out
    try:
        rows = _post_query(EQUITY_SI_DATASET,
                            {"limit": 1, "sortFields": ["-settlementDate"]})
        if isinstance(rows, list) and rows:
            out["data_api_reachable"] = True
            out["latest_settlement_date_in_dataset"] = (
                rows[0].get("settlementDate"))
    except Exception as e:
        out["error"] = str(e)
    return out


if __name__ == "__main__":
    # Local quick test (requires SSM creds present)
    print(json.dumps(health_check(), indent=2))
