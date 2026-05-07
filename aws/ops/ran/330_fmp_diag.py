#!/usr/bin/env python3
"""Step 330 — Diagnose FMP price endpoint (global-macro ETF data missing)."""
import json
import os
import urllib.request
from datetime import datetime, timezone

REPORT = "aws/ops/reports/330_fmp_diag.json"
FMP_KEY = "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb"

ENDPOINTS = [
    # The 'stable' path used in current global-macro
    f"https://financialmodelingprep.com/stable/historical-price-eod?symbol=EWG&apikey={FMP_KEY}",
    # The classic v3 path
    f"https://financialmodelingprep.com/api/v3/historical-price-full/EWG?apikey={FMP_KEY}",
    # Light variant
    f"https://financialmodelingprep.com/api/v3/historical-price-full/EWG?serietype=line&apikey={FMP_KEY}",
    # Stable variant 2 (some versions)
    f"https://financialmodelingprep.com/api/v3/historical-chart/1day/EWG?apikey={FMP_KEY}",
]


def hit(url):
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "diag/1.0"})
        with urllib.request.urlopen(req, timeout=15) as r:
            data = json.loads(r.read().decode("utf-8"))
        if isinstance(data, list):
            return {"ok": True, "type": "list", "count": len(data), "first_3": data[:3]}
        elif isinstance(data, dict):
            keys = list(data.keys())[:8]
            hist = data.get("historical")
            return {
                "ok": True, "type": "dict", "top_keys": keys,
                "hist_count": len(hist) if isinstance(hist, list) else None,
                "hist_first_3": hist[:3] if isinstance(hist, list) else None,
                "raw_first_400": json.dumps(data, default=str)[:400],
            }
    except urllib.error.HTTPError as e:
        return {"ok": False, "code": e.code, "body": e.read().decode("utf-8", errors="replace")[:400]}
    except Exception as e:
        return {"ok": False, "err": str(e)[:200]}


def main():
    out = {"as_of": datetime.now(timezone.utc).isoformat(), "tests": {}}
    for i, url in enumerate(ENDPOINTS):
        path = url.split("?")[0].replace("https://financialmodelingprep.com", "")
        print(f"[330] Test {i+1}: {path}")
        out["tests"][path] = hit(url)
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f:
        json.dump(out, f, indent=2, default=str)
    print(json.dumps(out, indent=2, default=str)[:5000])


if __name__ == "__main__":
    main()
