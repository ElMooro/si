"""
Ops 1067: live-verify the rewired finra_trace breadth/trace functions (read-only).

Calls fetch_corporate_breadth() and fetch_trace_aggregates(<latest weekday>)
against the LIVE FINRA API using the shared module as committed on main.
Expects:
  - breadth: dict with tradeDate, numberOfIssuesAdvancing/Declining/Unchanged
  - agg: dict with n_prints > 0 and total_volume > 0

Writes: aws/ops/reports/1067_finra_rewire_verify.json (committed back by run-ops).
Read-only. Never raises.
"""
from __future__ import annotations

import json
import os
import sys
import time
from datetime import date, timedelta

OUT_PATH = os.path.join("aws", "ops", "reports", "1067_finra_rewire_verify.json")

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                "..", "..", "shared"))
import finra_trace  # noqa: E402


def latest_weekday():
    """Most recent weekday as YYYY-MM-DD."""
    d = date.today() - timedelta(days=1)
    while d.weekday() >= 5:
        d -= timedelta(days=1)
    return d.isoformat()


def main():
    """Exercise the rewired functions live, write the report, print it."""
    report = {"script": "1067_finra_rewire_verify",
              "read_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
              "module": {"breadth_dataset": getattr(finra_trace, "BREADTH_DATASET", None),
                         "date_field": getattr(finra_trace, "BREADTH_DATE_FIELD", None)}}
    try:
        b = finra_trace.fetch_corporate_breadth()
        report["breadth"] = {
            "ok": isinstance(b, dict),
            "tradeDate": (b or {}).get("tradeDate"),
            "advancing": (b or {}).get("numberOfIssuesAdvancing"),
            "declining": (b or {}).get("numberOfIssuesDeclining"),
            "unchanged": (b or {}).get("numberOfIssuesUnchanged"),
            "n_trades": (b or {}).get("numberOfTrades"),
        }
    except Exception as e:  # noqa: BLE001 - report, don't raise
        report["breadth"] = {"ok": False, "error": f"{type(e).__name__}: {e}"[:200]}
    try:
        td = latest_weekday()
        a = finra_trace.fetch_trace_aggregates(td)
        report["trace_aggregates"] = {
            "ok": isinstance(a, dict) and (a or {}).get("n_prints", 0) > 0,
            "trade_date": td,
            "n_prints": (a or {}).get("n_prints"),
            "total_volume": (a or {}).get("total_volume"),
            "source": (a or {}).get("source"),
        }
    except Exception as e:  # noqa: BLE001 - report, don't raise
        report["trace_aggregates"] = {"ok": False,
                                      "error": f"{type(e).__name__}: {e}"[:200]}
    report["overall"] = ("PASS" if report["breadth"].get("ok")
                         and report["trace_aggregates"].get("ok") else "FAIL")
    os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)
    with open(OUT_PATH, "w") as f:
        json.dump(report, f, indent=2)
    print(json.dumps(report, indent=2)[:4000])


if __name__ == "__main__":
    main()
