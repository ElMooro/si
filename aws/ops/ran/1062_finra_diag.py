"""
Ops 1062: diagnose the 1059 FINRA failures with raw HTTP detail.

1059 reported 0/4 but recorded no error bodies. Hypotheses:
  H1: 1059 used trade_date=today (2026-10-01); FINRA publishes T+1, so zero
      rows is expected — the EQUAL-filter fix may actually work.
  H2: the EQUAL filter still 400s.
  H3: `trace` needs different handling.
Also: enumerate the fixedIncomeMarket dataset catalog WITH auth to find the
real breadth dataset name (corporateDebtMarketBreadth 404s even with auth).

Captures raw HTTP status + row counts + body previews. Read-only.
Writes: aws/ops/reports/1062_finra_diag.json (committed back by run-ops).
Never raises.
"""
from __future__ import annotations

import json
import os
import sys
import time
import urllib.error
import urllib.request

OUT_PATH = os.path.join("aws", "ops", "reports", "1062_finra_diag.json")
HTTP_TIMEOUT = 30

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                "..", "..", "shared"))
import finra_trace  # noqa: E402  (repo copy, post-fix)


def raw_post(dataset, payload):
    """POST and return (http_status, parsed_or_body_preview, n_rows). Never raises."""
    url = f"{finra_trace.FINRA_DATA_BASE}/{finra_trace.FIXED_INCOME_GROUP}/{dataset}"
    headers = {"Content-Type": "application/json", "Accept": "application/json",
               "User-Agent": finra_trace.USER_AGENT}
    token = finra_trace.get_token()
    if token:
        headers["Authorization"] = f"Bearer {token}"
    req = urllib.request.Request(url, data=json.dumps(payload).encode("utf-8"),
                                 method="POST", headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT) as r:
            data = json.loads(r.read().decode("utf-8"))
            n = len(data) if isinstance(data, list) else -1
            keys = sorted(data[0].keys())[:10] if n > 0 and isinstance(data[0], dict) else []
            return r.status, n, keys, ""
    except urllib.error.HTTPError as e:
        try:
            body = e.read()[:400].decode("utf-8", errors="replace")
        except Exception:
            body = "<unreadable>"
        return e.code, -1, [], body
    except Exception as e:  # network / timeout / bad JSON
        return -1, -1, [], type(e).__name__


def raw_get_metadata(path):
    """GET a metadata path with auth. Returns (status, preview). Never raises."""
    url = f"https://api.finra.org/metadata/{path}"
    headers = {"Accept": "application/json", "User-Agent": finra_trace.USER_AGENT}
    token = finra_trace.get_token()
    if token:
        headers["Authorization"] = f"Bearer {token}"
    req = urllib.request.Request(url, headers=headers, method="GET")
    try:
        with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT) as r:
            return r.status, r.read()[:1500].decode("utf-8", errors="replace")
    except urllib.error.HTTPError as e:
        try:
            body = e.read()[:300].decode("utf-8", errors="replace")
        except Exception:
            body = "<unreadable>"
        return e.code, body
    except Exception as e:
        return -1, type(e).__name__


def main():
    """Run the diagnostic probes, write the report, print it."""
    report = {"script": "1062_finra_diag",
              "read_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
              "probes": {}}
    eq = lambda d: [{"fieldName": "tradeDate", "compareType": "EQUAL",
                     "fieldValue": d}]

    # H1/H2: treasury EQUAL filter on known-good dates (1053 used 2026-09-25).
    for d in ("2026-09-25", "2026-09-30", "2026-10-01"):
        st, n, keys, body = raw_post(
            "treasuryDailyAggregates",
            {"limit": 100, "compareFilters": eq(d),
             "sortFields": ["yearsToMaturity"]})
        report["probes"][f"treasury_equal_{d}"] = {
            "http": st, "rows": n, "keys": keys, "body": body[:300]}

    # H3: trace EQUAL filter on the same dates.
    for d in ("2026-09-25", "2026-09-30"):
        st, n, keys, body = raw_post(
            "trace", {"limit": 100, "compareFilters": eq(d)})
        report["probes"][f"trace_equal_{d}"] = {
            "http": st, "rows": n, "keys": keys, "body": body[:300]}

    # Catalog enumeration with auth: find the real breadth dataset name.
    for path in ("group/fixedIncomeMarket", "groups",
                 "group/fixedIncomeMarket/name/corporateDebtMarketBreadth"):
        st, preview = raw_get_metadata(path)
        report["probes"][f"metadata_{path.replace('/', '_')}"] = {
            "http": st, "preview": preview[:800]}

    os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)
    with open(OUT_PATH, "w") as f:
        json.dump(report, f, indent=2)
    print(json.dumps(report, indent=2)[:14000])


if __name__ == "__main__":
    main()
