#!/usr/bin/env python3
"""Step 331 — Try more FMP stable endpoint variants for historical price."""
import json
import os
import urllib.request
from datetime import datetime, timezone

REPORT = "aws/ops/reports/331_fmp_variants.json"
FMP_KEY = "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb"

ENDPOINTS = [
    # Stable variants (post-deprecation)
    "https://financialmodelingprep.com/stable/historical-price-eod/full?symbol=EWG&apikey={}",
    "https://financialmodelingprep.com/stable/historical-price-eod/light?symbol=EWG&apikey={}",
    "https://financialmodelingprep.com/stable/historical-chart/1day?symbol=EWG&apikey={}",
    "https://financialmodelingprep.com/stable/historical-chart/4hour?symbol=EWG&apikey={}",
    # Quote endpoint (current price)
    "https://financialmodelingprep.com/stable/quote?symbol=EWG&apikey={}",
    # Profile (just to verify auth works)
    "https://financialmodelingprep.com/stable/profile?symbol=EWG&apikey={}",
    # Full-history alternatives
    "https://financialmodelingprep.com/stable/historical-price-full?symbol=EWG&apikey={}",
    "https://financialmodelingprep.com/stable/historical-prices-eod?symbol=EWG&apikey={}",
]


def hit(url):
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "diag/1.0"})
        with urllib.request.urlopen(req, timeout=15) as r:
            data = json.loads(r.read().decode("utf-8"))
        if isinstance(data, list):
            return {
                "ok": True, "type": "list", "count": len(data),
                "first_record_keys": list(data[0].keys())[:10] if data else [],
                "first_record": data[0] if data else None,
            }
        elif isinstance(data, dict):
            keys = list(data.keys())[:8]
            hist = data.get("historical")
            return {
                "ok": True, "type": "dict", "top_keys": keys,
                "hist_count": len(hist) if isinstance(hist, list) else None,
                "raw_first_400": json.dumps(data, default=str)[:400],
            }
    except urllib.error.HTTPError as e:
        return {"ok": False, "code": e.code, "body": e.read().decode("utf-8", errors="replace")[:300]}
    except Exception as e:
        return {"ok": False, "err": str(e)[:200]}


def main():
    out = {"as_of": datetime.now(timezone.utc).isoformat(), "tests": {}}
    for tpl in ENDPOINTS:
        url = tpl.format(FMP_KEY)
        path = url.split("?")[0].replace("https://financialmodelingprep.com", "")
        print(f"[331] Test: {path}")
        out["tests"][path] = hit(url)
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f:
        json.dump(out, f, indent=2, default=str)
    print(json.dumps(out, indent=2, default=str)[:6000])


if __name__ == "__main__":
    main()
