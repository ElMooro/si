"""ops 1081 — investigate why FMP /stable/ratios-ttm returned 1/15.

The forward-returns Lambda needs earnings yield for SPY/QQQ/IWM/EFA/EEM
(broad equity indices) plus dividend yield for VNQ (REITs). FMP's
/stable/ratios-ttm likely:
  (a) covers individual stocks only, not ETFs
  (b) returns ETF data via a different shape
  (c) requires a different endpoint for ETFs (e.g. /stable/etf-info or
      /stable/etf-holder)

This op fires the endpoint at each ticker, captures raw response,
and also tests alternate endpoints we might use for ETF earnings yield:

  /stable/quote                  → already known to work
  /stable/ratios-ttm             → suspected stock-only
  /stable/etf-info               → check
  /stable/etf-holdings           → check
  /stable/key-metrics-ttm        → alternative ratios source
  /stable/income-statement       → could compute E/P manually
  /stable/historical-rating      → not relevant
  + For underlying-index proxies: SPX for SPY index P/E (sometimes works)
"""
import json, os, urllib.request, urllib.error
from datetime import datetime, timezone

REPO_ROOT = os.environ.get("REPO_ROOT", os.getcwd())
FMP_KEY = "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb"
SYMBOLS = ["SPY", "QQQ", "IWM", "EFA", "EEM", "TLT", "GLD", "VNQ", "AAPL", "MSFT"]
ENDPOINTS = [
    "ratios-ttm",
    "key-metrics-ttm",
    "etf-info",
    "etf-holdings",
    "income-statement",
    "profile",
]


def fmp(endpoint, symbol):
    url = f"https://financialmodelingprep.com/stable/{endpoint}?symbol={symbol}&apikey={FMP_KEY}&limit=1"
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "JustHodl/RatiosDebug"})
        with urllib.request.urlopen(req, timeout=12) as r:
            status = r.status
            body = r.read().decode("utf-8", errors="replace")
            try:
                data = json.loads(body)
                if isinstance(data, list):
                    return {
                        "status": status,
                        "type": "list",
                        "len": len(data),
                        "first_keys": list(data[0].keys())[:25] if data else None,
                        "first_50_chars_raw": body[:50],
                    }
                elif isinstance(data, dict):
                    return {
                        "status": status,
                        "type": "dict",
                        "keys": list(data.keys())[:25],
                        "first_50": body[:50],
                    }
                return {"status": status, "type": "other", "first_100": body[:100]}
            except Exception as je:
                return {"status": status, "parse_err": str(je)[:120], "first_200": body[:200]}
    except urllib.error.HTTPError as e:
        return {"status": e.code, "err": str(e)[:100]}
    except Exception as e:
        return {"err": str(e)[:120]}


def main():
    report = {"started_at": datetime.now(timezone.utc).isoformat(), "tests": {}}
    for sym in SYMBOLS:
        report["tests"][sym] = {ep: fmp(ep, sym) for ep in ENDPOINTS}

    # Summary
    summary = {}
    for ep in ENDPOINTS:
        ok_for = []
        for sym in SYMBOLS:
            r = report["tests"][sym].get(ep, {})
            if r.get("status") == 200 and r.get("len", 0) > 0:
                ok_for.append(sym)
            elif r.get("status") == 200 and r.get("type") == "dict" and r.get("keys"):
                ok_for.append(sym)
        summary[ep] = {"works_for": ok_for, "count": len(ok_for)}
    report["summary_by_endpoint"] = summary

    out = os.path.join(REPO_ROOT, "aws/ops/reports/1081.json")
    os.makedirs(os.path.dirname(out), exist_ok=True)
    with open(out, "w") as f:
        json.dump(report, f, indent=2, default=str)

    print("=" * 60)
    print("WORKING endpoints by ticker:")
    for sym in SYMBOLS:
        works = [ep for ep in ENDPOINTS if report["tests"][sym].get(ep, {}).get("len", 0) > 0
                 or (report["tests"][sym].get(ep, {}).get("type") == "dict" and report["tests"][sym].get(ep, {}).get("keys"))]
        print(f"  {sym}: {works}")
    print("\nWORKING tickers by endpoint:")
    for ep, info in summary.items():
        print(f"  {ep}: {info['count']}/{len(SYMBOLS)} → {info['works_for']}")


if __name__ == "__main__":
    main()
