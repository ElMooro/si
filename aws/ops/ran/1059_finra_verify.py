"""
Ops 1059: verify the finra_trace.py 400 fix against the live FINRA API.

Uses the UPDATED finra_trace.py from the repo (aws/shared/finra_trace.py),
which now pins tradeDate with an EQUAL compareFilter whenever sortFields
are used (the FINRA rule that caused the HTTP 400s in ops 1053).

Checks:
  1. Treasury daily query with EQUAL filter + sort -> expect 200 + rows.
  2. corporateDebtMarketBreadth: metadata lookup WITH auth -> distinguishes
     "wrong dataset name" from "credentials not entitled" (1053 got 404).
  3. trace dataset: EQUAL-filtered per-print query -> expect 200 + rows.
  4. Breadth latest-date fallback (last 5 weekdays, client-side max).

Bounded: 4 checks, short timeouts. Never raises.
Writes: aws/ops/reports/1059_finra_verify.json (committed back by run-ops).
"""
from __future__ import annotations

import datetime
import json
import os
import sys
import time
import urllib.error
import urllib.request

OUT_PATH = os.path.join("aws", "ops", "reports", "1059_finra_verify.json")
HTTP_TIMEOUT = 30

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                "..", "..", "shared"))
import finra_trace  # noqa: E402  (repo copy, post-fix)


def auth_metadata(dataset):
    """GET metadata for a dataset WITH the finra_trace token. Returns (status, body)."""
    token = finra_trace.get_token()
    url = (f"https://api.finra.org/metadata/group/fixedIncomeMarket"
           f"/name/{dataset}")
    headers = {"Accept": "application/json",
               "User-Agent": finra_trace.USER_AGENT}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    req = urllib.request.Request(url, headers=headers, method="GET")
    try:
        with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT) as r:
            return r.status, r.read().decode("utf-8", errors="replace")[:600]
    except urllib.error.HTTPError as e:
        return e.code, e.read()[:600].decode("utf-8", errors="replace")
    except Exception as e:  # network / DNS / timeout
        return -1, type(e).__name__


def last_weekday():
    """Most recent weekday (FINRA publishes on trading days)."""
    d = datetime.date.today()
    while d.weekday() >= 5:
        d -= datetime.timedelta(days=1)
    return d.isoformat()


def check(name, fn, *args):
    """Run one bounded check. Never raises."""
    started = time.time()
    try:
        result = fn(*args)
        ok = bool(result)
        detail = {"ok": ok}
        if isinstance(result, list):
            detail["rows"] = len(result)
            if result and isinstance(result[0], dict):
                detail["sample_keys"] = sorted(result[0].keys())[:12]
        elif isinstance(result, dict):
            detail["fields"] = sorted(result.keys())[:12]
        detail["elapsed_s"] = round(time.time() - started, 2)
        return name, detail
    except Exception as e:  # never break the report
        return name, {"ok": False, "error": type(e).__name__,
                      "elapsed_s": round(time.time() - started, 2)}


def main():
    """Run the four checks, write the report, print it."""
    report = {"script": "1059_finra_verify",
              "read_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
              "checks": {}}

    trade_date = last_weekday()
    report["trade_date_used"] = trade_date

    # 1. Treasury: EQUAL filter + sort (was HTTP 400 in ops 1053)
    n, res = check("treasury_equal_filter", finra_trace.fetch_treasury_daily,
                   trade_date)
    report["checks"][n] = res

    # 2. Breadth metadata WITH auth: name vs entitlement?
    status, body = auth_metadata("corporateDebtMarketBreadth")
    report["checks"]["breadth_metadata_auth"] = {
        "http_status": status,
        "body_preview": body,
        "conclusion": (
            "dataset name valid; query 404 is entitlement/access, not naming"
            if status == 200 else
            "dataset name invalid or dataset retired (metadata also 404)"
            if status == 404 else
            "metadata lookup inconclusive")}

    # 3. Breadth latest-date fallback (post-fix path)
    n, res = check("breadth_fallback", finra_trace.fetch_corporate_breadth)
    report["checks"][n] = res

    # 4. TRACE per-print aggregates with EQUAL filter (was 400-risk)
    n, res = check("trace_equal_filter", finra_trace.fetch_trace_aggregates,
                   trade_date)
    report["checks"][n] = res

    passed = sum(1 for c in report["checks"].values() if c.get("ok"))
    report["overall"] = "PASS" if passed >= 3 else "FAIL"
    report["passed"] = passed

    os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)
    with open(OUT_PATH, "w") as f:
        json.dump(report, f, indent=2)
    print(json.dumps(report, indent=2)[:12000])


if __name__ == "__main__":
    main()
