"""
Ops 1065: probe candidate FINRA dataset names (read-only).

Background: 1062 proved the EQUAL tradeDate filter works (treasuryDailyAggregates
-> HTTP 200, 22 rows), but the dataset names `trace` and
`corporateDebtMarketBreadth` both 404. A third-party FINRA Query API playbook
maps TRACE/fixed-income routes to `fixedIncomeMarket/corporateMarketBreadth`
(plus agencyMarketBreadth, corporate144AMarketBreadth, corporateMarketSentiment).

This script probes each candidate with an EQUAL tradeDate filter on a known
date (2026-09-30) and records HTTP status, row count, and field names so the
shared finra_trace module can be rewired to real dataset names.

Writes: aws/ops/reports/1065_dataset_probe.json (committed back by run-ops).
Read-only: no writes to AWS resources. Never raises.
"""
from __future__ import annotations

import json
import os
import sys
import time
import urllib.error
import urllib.request

OUT_PATH = os.path.join("aws", "ops", "reports", "1065_dataset_probe.json")
HTTP_TIMEOUT = 30
TRADE_DATE = "2026-09-30"

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                "..", "..", "shared"))
import finra_trace  # noqa: E402  (repo copy, post-fix)


def probe(dataset, use_filter=True):
    """POST to a candidate dataset. Returns dict. Never raises."""
    url = f"{finra_trace.FINRA_DATA_BASE}/{finra_trace.FIXED_INCOME_GROUP}/{dataset}"
    headers = {"Content-Type": "application/json", "Accept": "application/json",
               "User-Agent": finra_trace.USER_AGENT}
    token = finra_trace.get_token()
    if token:
        headers["Authorization"] = f"Bearer {token}"
    payload = {"limit": 5}
    if use_filter:
        payload["compareFilters"] = [{"fieldName": "tradeDate",
                                      "compareType": "EQUAL",
                                      "fieldValue": TRADE_DATE}]
    req = urllib.request.Request(url, data=json.dumps(payload).encode("utf-8"),
                                 method="POST", headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT) as r:
            data = json.loads(r.read().decode("utf-8"))
            n = len(data) if isinstance(data, list) else -1
            keys = sorted(data[0].keys())[:15] if n > 0 and isinstance(data[0], dict) else []
            sample = {k: data[0].get(k) for k in keys[:6]} if n > 0 else {}
            return {"http": r.status, "rows": n, "keys": keys,
                    "sample": sample}
    except urllib.error.HTTPError as e:
        try:
            body = e.read()[:200].decode("utf-8", errors="replace")
        except Exception:
            body = "<unreadable>"
        return {"http": e.code, "rows": -1, "keys": [], "body": body}
    except Exception as e:  # network / timeout / bad JSON
        return {"http": -1, "rows": -1, "keys": [],
                "body": type(e).__name__}


def main():
    """Probe candidate datasets, write the report, print it."""
    candidates = ["corporateMarketBreadth", "agencyMarketBreadth",
                  "corporate144AMarketBreadth", "corporateMarketSentiment"]
    report = {"script": "1065_dataset_probe",
              "read_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
              "trade_date": TRADE_DATE,
              "probes": {}}
    for ds in candidates:
        report["probes"][ds] = probe(ds, use_filter=True)
    os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)
    with open(OUT_PATH, "w") as f:
        json.dump(report, f, indent=2)
    print(json.dumps(report, indent=2)[:10000])


if __name__ == "__main__":
    main()
