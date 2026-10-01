"""
Ops 1066: get the schema of the breadth datasets (read-only).

1065 showed corporateMarketBreadth (and agency/corp144A/sentiment) EXIST
but reject the `tradeDate` filter: "fields not available: [tradeDate]".
This probes each dataset with NO filter (limit 3) to capture the real
field names, especially the date field.

Writes: aws/ops/reports/1066_schema_probe.json (committed back by run-ops).
Read-only. Never raises.
"""
from __future__ import annotations

import json
import os
import sys
import time
import urllib.error
import urllib.request

OUT_PATH = os.path.join("aws", "ops", "reports", "1066_schema_probe.json")
HTTP_TIMEOUT = 30

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                "..", "..", "shared"))
import finra_trace  # noqa: E402


def schema_probe(dataset):
    """Fetch 3 unfiltered rows to reveal the schema. Never raises."""
    url = f"{finra_trace.FINRA_DATA_BASE}/{finra_trace.FIXED_INCOME_GROUP}/{dataset}"
    headers = {"Content-Type": "application/json", "Accept": "application/json",
               "User-Agent": finra_trace.USER_AGENT}
    token = finra_trace.get_token()
    if token:
        headers["Authorization"] = f"Bearer {token}"
    req = urllib.request.Request(
        url, data=json.dumps({"limit": 3}).encode("utf-8"),
        method="POST", headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT) as r:
            data = json.loads(r.read().decode("utf-8"))
            n = len(data) if isinstance(data, list) else -1
            keys = sorted(data[0].keys()) if n > 0 and isinstance(data[0], dict) else []
            sample = data[0] if n > 0 else None
            return {"http": r.status, "rows": n, "keys": keys, "sample": sample}
    except urllib.error.HTTPError as e:
        try:
            body = e.read()[:200].decode("utf-8", errors="replace")
        except Exception:
            body = "<unreadable>"
        return {"http": e.code, "keys": [], "body": body}
    except Exception as e:
        return {"http": -1, "keys": [], "body": type(e).__name__}


def main():
    """Probe schemas, write the report, print it."""
    report = {"script": "1066_schema_probe",
              "read_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
              "probes": {}}
    for ds in ("corporateMarketBreadth", "corporateMarketSentiment"):
        report["probes"][ds] = schema_probe(ds)
    os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)
    with open(OUT_PATH, "w") as f:
        json.dump(report, f, indent=2)
    print(json.dumps(report, indent=2)[:8000])


if __name__ == "__main__":
    main()
