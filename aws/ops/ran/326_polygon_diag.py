#!/usr/bin/env python3
"""Step 326 — Test Polygon endpoints directly to see what's actually returned."""
import json
import os
import urllib.parse
import urllib.request
from datetime import datetime, timezone

REPORT = "aws/ops/reports/326_polygon_diag.json"
POLY_KEY = "zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d"


def hit(url):
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "diag/1.0"})
        with urllib.request.urlopen(req, timeout=20) as r:
            data = json.loads(r.read().decode("utf-8"))
        return {
            "ok": True,
            "status": data.get("status"),
            "count": data.get("count") or data.get("resultsCount") or len(data.get("results") or data.get("tickers") or []),
            "first_3": (data.get("tickers") or data.get("results") or [])[:3],
            "request_id": data.get("request_id"),
            "message": data.get("message", "")[:200] if data.get("status") != "OK" else None,
            "next_url": data.get("next_url"),
        }
    except urllib.error.HTTPError as e:
        return {
            "ok": False,
            "code": e.code,
            "body": e.read().decode("utf-8", errors="replace")[:400],
        }
    except Exception as e:
        return {"ok": False, "err": str(e)[:200]}


def main():
    out = {"as_of": datetime.now(timezone.utc).isoformat(), "tests": {}}

    # Test 1: snapshot all stocks
    qs = urllib.parse.urlencode({"apiKey": POLY_KEY})
    url1 = f"https://api.polygon.io/v3/snapshot/locale/us/markets/stocks/tickers?{qs}"
    print(f"[326] Test 1: snapshot all stocks")
    out["tests"]["snapshot_all_stocks"] = hit(url1)

    # Test 2: grouped daily for a recent trading day
    qs = urllib.parse.urlencode({"apiKey": POLY_KEY, "adjusted": "true"})
    for date_str in ("2026-05-06", "2026-05-05", "2026-05-02"):
        url2 = f"https://api.polygon.io/v2/aggs/grouped/locale/us/market/stocks/{date_str}?{qs}"
        print(f"[326] Test 2: grouped daily {date_str}")
        out["tests"][f"grouped_{date_str}"] = hit(url2)

    # Test 3: simple known-good ticker query
    url3 = f"https://api.polygon.io/v3/reference/tickers/AAPL?{qs}"
    print(f"[326] Test 3: ticker reference (AAPL)")
    out["tests"]["aapl_ref"] = hit(url3)

    # Test 4: snapshot for individual ticker (cheaper alternative)
    url4 = f"https://api.polygon.io/v2/snapshot/locale/us/markets/stocks/tickers/AAPL?{qs}"
    print(f"[326] Test 4: AAPL snapshot")
    out["tests"]["aapl_snapshot"] = hit(url4)

    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f:
        json.dump(out, f, indent=2, default=str)
    print(json.dumps(out, indent=2, default=str)[:6000])


if __name__ == "__main__":
    main()
