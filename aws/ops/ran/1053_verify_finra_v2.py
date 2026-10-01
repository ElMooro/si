"""
Ops verification v2: FINRA Gateway credentials + Data API query path.

Follow-up to 1052_verify_finra.py, which reported FAIL_API with a
JSONDecodeError. Diagnosis of the 1052 failure:

  1052 sent a bare GET to the *data query* URL:
      GET https://api.finra.org/data/group/fixedIncomeMarket/name/treasuryDailyAggregates
  with no request body and no Content-Type/Accept headers.

  Per FINRA's Query API docs (developer.finra.org), /data/group/.../name/...
  is a *query* endpoint: it requires a POST with a JSON query body
  ({"limit": N, "dateRangeFilters": [...], "sortFields": [...]}, ...).
  A bare GET returns a non-JSON error page (HTML), so json.loads() raised
  JSONDecodeError. (The metadata endpoint is a different URL:
  GET /metadata/group/.../name/... .)

This script tests the SAME request shape that aws/shared/finra_trace.py
uses in production (POST + JSON body + Bearer token), with robust
response introspection so a bad response can never crash the script:
HTTP status, content-type, and a body preview are recorded instead.

Verifies:
  1. SSM parameters /justhodl/finra/client_id and /justhodl/finra/client_secret exist
  2. OAuth2 client-credentials token request succeeds (same flow as 1052, which worked)
  3. POST treasuryDailyAggregates with dateRangeFilters (exact finra_trace
     fetch_treasury_daily shape) returns rows with documented fields
  4. POST corporateDebtMarketBreadth limit=1 (exact finra_trace
     fetch_corporate_breadth shape) returns a snapshot row
  5. (best-effort) aws/shared/finra_trace.py imports and its fetchers work

Writes report to s3://justhodl-dashboard-live/ops/reports/1053.json

Credential values are NEVER logged or written to the report.
"""
from __future__ import annotations

import base64
import datetime
import json
import os
import sys
import time
import urllib.request
import urllib.error

import boto3

SSM_CLIENT_ID = "/justhodl/finra/client_id"
SSM_CLIENT_SECRET = "/justhodl/finra/client_secret"
TOKEN_URL = "https://ews.fip.finra.org/fip/rest/ews/oauth2/access_token"
DATA_BASE = "https://api.finra.org/data"
METADATA_BASE = "https://api.finra.org/metadata"
GROUP_PATH = "group/fixedIncomeMarket/name"
USER_AGENT = "JustHodl-TRACE-Client/1.0"
HTTP_TIMEOUT = 30
S3_BUCKET = "justhodl-dashboard-live"
REPORT_KEY = "ops/reports/1053.json"

# Documented treasury fields expected in a successful response row.
EXPECTED_TREASURY_FIELDS = {
    "tradeDate", "productCategory", "yearsToMaturity",
    "volumeWeightedAveragePrice",
}
EXPECTED_BREADTH_FIELDS = {
    "tradeDate", "numberOfIssues", "parValueTraded", "numberOfTrades",
}


def check_ssm(ssm):
    """Check both SSM params exist. Returns (ok, detail). Never returns values."""
    results = {}
    for name in (SSM_CLIENT_ID, SSM_CLIENT_SECRET):
        try:
            val = ssm.get_parameter(Name=name, WithDecryption=True)["Parameter"]["Value"]
            results[name] = {"exists": True, "length": len(val) if val else 0}
        except ssm.exceptions.ParameterNotFound:
            results[name] = {"exists": False, "length": 0}
        except Exception as e:  # network / permission failure, not a missing param
            results[name] = {"exists": False, "error": type(e).__name__}
    ok = all(r.get("exists") and r.get("length", 0) > 0 for r in results.values())
    return ok, results


def get_token(client_id, client_secret):
    """OAuth2 client-credentials flow. Returns (token, error)."""
    creds = f"{client_id}:{client_secret}"
    b64 = base64.b64encode(creds.encode()).decode()
    data = b"grant_type=client_credentials"
    req = urllib.request.Request(
        TOKEN_URL,
        data=data,
        headers={
            "Authorization": f"Basic {b64}",
            "Content-Type": "application/x-www-form-urlencoded",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT) as resp:
            body = json.loads(resp.read().decode())
            token = body.get("access_token")
            if token:
                return token, None
            return None, "no access_token in response"
    except urllib.error.HTTPError as e:
        return None, f"HTTP {e.code}"
    except Exception as e:  # network failure, bad JSON, timeout
        return None, type(e).__name__


def _introspect_response(resp):
    """Read a urlopen response into an introspection dict. Never raises."""
    raw = resp.read()
    text = raw.decode("utf-8", errors="replace")
    info = {
        "http_status": getattr(resp, "status", None),
        "content_type": resp.headers.get_content_type() if resp.headers else None,
        "body_bytes": len(raw),
        "body_preview": text[:500],
    }
    try:
        info["parsed"] = json.loads(text)
        info["parsed_ok"] = True
    except (json.JSONDecodeError, UnicodeDecodeError, ValueError) as e:
        info["parsed_ok"] = False
        info["parse_error"] = type(e).__name__
    return info


def post_query(token, dataset, payload):
    """POST a JSON query to a FINRA dataset. Returns an introspection dict.

    Uses the exact request shape of finra_trace._post (POST, JSON body,
    Content-Type/Accept/User-Agent/Bearer headers). Never raises.
    """
    url = f"{DATA_BASE}/{GROUP_PATH}/{dataset}"
    detail = {"url": url, "payload": payload}
    req = urllib.request.Request(
        url,
        data=json.dumps(payload).encode("utf-8"),
        method="POST",
        headers={
            "Content-Type": "application/json",
            "Accept": "application/json",
            "User-Agent": USER_AGENT,
            "Authorization": f"Bearer {token}",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT) as resp:
            detail.update(_introspect_response(resp))
    except urllib.error.HTTPError as e:
        detail["http_status"] = e.code
        try:
            err_text = e.read().decode("utf-8", errors="replace")[:500]
        except Exception:  # unreadable error body
            err_text = ""
        detail["body_preview"] = err_text
        detail["parsed_ok"] = False
        detail["parse_error"] = f"HTTPError {e.code}"
    except Exception as e:  # URLError, timeout, socket failure
        detail["http_status"] = None
        detail["parsed_ok"] = False
        detail["parse_error"] = type(e).__name__
    return detail


def get_metadata(token, dataset):
    """GET the dataset metadata endpoint (what 1052 likely intended). Never raises."""
    url = f"{METADATA_BASE}/{GROUP_PATH}/{dataset}"
    detail = {"url": url}
    req = urllib.request.Request(
        url,
        method="GET",
        headers={
            "Accept": "application/json",
            "User-Agent": USER_AGENT,
            "Authorization": f"Bearer {token}",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT) as resp:
            detail.update(_introspect_response(resp))
    except urllib.error.HTTPError as e:
        detail["http_status"] = e.code
        detail["parsed_ok"] = False
        detail["parse_error"] = f"HTTPError {e.code}"
    except Exception as e:  # URLError, timeout, socket failure
        detail["http_status"] = None
        detail["parsed_ok"] = False
        detail["parse_error"] = type(e).__name__
    return detail


def recent_weekdays(n=5):
    """Return the last n weekday dates (YYYY-MM-DD), most recent first."""
    days = []
    d = datetime.date.today()
    while len(days) < n:
        if d.weekday() < 5:
            days.append(d.isoformat())
        d -= datetime.timedelta(days=1)
    return days


def probe_treasury(token):
    """Exact finra_trace.fetch_treasury_daily query shape, walking back weekdays.

    Returns (detail, rows). Tries up to 5 recent weekdays so weekends,
    holidays, and publication lag can't produce a false negative.
    """
    last_detail = None
    for trade_date in recent_weekdays(5):
        payload = {
            "limit": 100,
            "dateRangeFilters": [{
                "fieldName": "tradeDate",
                "startDate": trade_date,
                "endDate": trade_date,
            }],
            "sortFields": ["yearsToMaturity"],
        }
        detail = post_query(token, "treasuryDailyAggregates", payload)
        detail["trade_date_queried"] = trade_date
        rows = detail.get("parsed") if detail.get("parsed_ok") else None
        if isinstance(rows, list) and rows:
            detail["rows"] = len(rows)
            detail["sample_keys"] = sorted(rows[0].keys()) if isinstance(rows[0], dict) else []
            detail["fields_present"] = sorted(
                EXPECTED_TREASURY_FIELDS.intersection(set(detail["sample_keys"])))
            return detail, rows
        last_detail = detail
    last_detail = last_detail or {}
    last_detail["rows"] = 0
    return last_detail, []


def probe_breadth(token):
    """Exact finra_trace.fetch_corporate_breadth query shape. Returns (detail, row)."""
    payload = {"limit": 1, "sortFields": ["-tradeDate"]}
    detail = post_query(token, "corporateDebtMarketBreadth", payload)
    rows = detail.get("parsed") if detail.get("parsed_ok") else None
    row = rows[0] if isinstance(rows, list) and rows and isinstance(rows[0], dict) else None
    detail["rows"] = len(rows) if isinstance(rows, list) else 0
    detail["sample_keys"] = sorted(row.keys()) if row else []
    detail["fields_present"] = sorted(
        EXPECTED_BREADTH_FIELDS.intersection(set(detail["sample_keys"])))
    return detail, row


def probe_shared_module():
    """Best-effort: import aws/shared/finra_trace and call its fetchers.

    Returns a detail dict. Import or call failures are recorded, never raised.
    """
    detail = {"finra_trace_importable": False}
    try:
        # __file__ = <repo>/aws/ops/pending/<script>.py -> up 4 levels = repo root
        here = os.path.abspath(__file__)
        repo_root = os.path.dirname(os.path.dirname(os.path.dirname(
            os.path.dirname(here))))
        shared_dir = os.path.join(repo_root, "aws", "shared")
        if shared_dir not in sys.path:
            sys.path.insert(0, shared_dir)
        import finra_trace  # noqa: E402
        detail["finra_trace_importable"] = True
    except Exception as e:  # module missing or import-time failure
        detail["import_error"] = type(e).__name__
        return detail
    try:
        trade_date = recent_weekdays(5)[0]
        rows = finra_trace.fetch_treasury_daily(trade_date)
        detail["fetch_treasury_daily_rows"] = len(rows) if rows else 0
    except Exception as e:  # fetcher raised (it shouldn't — it fail-softs)
        detail["fetch_treasury_daily_error"] = type(e).__name__
    try:
        snap = finra_trace.fetch_corporate_breadth()
        detail["fetch_corporate_breadth_ok"] = bool(snap)
        if snap:
            detail["breadth_trade_date"] = snap.get("tradeDate")
    except Exception as e:  # fetcher raised (it shouldn't — it fail-softs)
        detail["fetch_corporate_breadth_error"] = type(e).__name__
    return detail


def main():
    """Run all verification probes and write the JSON report to S3."""
    ssm = boto3.client("ssm", region_name="us-east-1")
    s3 = boto3.client("s3", region_name="us-east-1")

    report = {
        "script": "1053_verify_finra_v2",
        "checked_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "ssm": {},
        "oauth": {},
        "api_probes": {},
        "shared_module": {},
        "diagnosis_1052": (
            "1052 sent a bare GET to the data-query URL "
            "/data/group/fixedIncomeMarket/name/treasuryDailyAggregates with no "
            "body. FINRA's Query API requires POST with a JSON query body "
            "({limit, dateRangeFilters, sortFields}); the GET returned a "
            "non-JSON error page, hence JSONDecodeError. This script uses the "
            "exact POST shape of aws/shared/finra_trace.py."
        ),
        "overall": "UNKNOWN",
    }

    # 1. SSM check
    ssm_ok, ssm_detail = check_ssm(ssm)
    report["ssm"] = {k: {"exists": v.get("exists", False)} for k, v in ssm_detail.items()}
    if not ssm_ok:
        report["overall"] = "FAIL_SSM_MISSING"
        _write_report(s3, report)
        return

    # 2. OAuth (values in memory only, never logged)
    client_id = ssm.get_parameter(Name=SSM_CLIENT_ID, WithDecryption=True)["Parameter"]["Value"]
    client_secret = ssm.get_parameter(Name=SSM_CLIENT_SECRET, WithDecryption=True)["Parameter"]["Value"]
    token, oauth_err = get_token(client_id, client_secret)
    del client_id, client_secret

    if not token:
        report["oauth"] = {"token_obtained": False, "error": oauth_err}
        report["overall"] = "FAIL_OAUTH"
        _write_report(s3, report)
        return
    report["oauth"] = {"token_obtained": True}

    # 3. Data probes (exact finra_trace.py request shapes)
    treasury_detail, treasury_rows = probe_treasury(token)
    report["api_probes"]["treasury_filtered"] = treasury_detail
    breadth_detail, breadth_row = probe_breadth(token)
    report["api_probes"]["corporate_breadth"] = breadth_detail
    # Informational: the metadata endpoint 1052 likely intended
    report["api_probes"]["treasury_metadata"] = get_metadata(token, "treasuryDailyAggregates")

    # 4. Shared-module path (best-effort)
    report["shared_module"] = probe_shared_module()

    del token  # clear from memory

    # 5. Verdict
    treasury_ok = (
        treasury_detail.get("parsed_ok")
        and treasury_detail.get("rows", 0) > 0
        and set(treasury_detail.get("fields_present", [])) >= {"tradeDate"}
    )
    breadth_ok = (
        breadth_detail.get("parsed_ok")
        and breadth_detail.get("rows", 0) > 0
        and set(breadth_detail.get("fields_present", [])) >= {"tradeDate"}
    )
    if treasury_ok and breadth_ok:
        report["overall"] = "PASS"
        report["freshness"] = {
            "latest_treasury_trade_date": treasury_detail.get("trade_date_queried"),
            "breadth_trade_date": (breadth_row or {}).get("tradeDate"),
        }
    elif treasury_ok or breadth_ok:
        report["overall"] = "PARTIAL_API"
    else:
        report["overall"] = "FAIL_API"

    _write_report(s3, report)


def _write_report(s3, report):
    """Write the report to S3 and print it."""
    s3.put_object(Bucket=S3_BUCKET, Key=REPORT_KEY,
                  Body=json.dumps(report, indent=2).encode(),
                  ContentType="application/json")
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
