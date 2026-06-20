"""1957 — PROBE Benzinga Earnings + ETF Global Constituents endpoints to confirm
entitlement, exact path, required params, and response schema BEFORE building.
Read-only. Uses MASSIVE_API_KEY (CI env) against api.polygon.io."""
import os, json, urllib.request, urllib.error

KEY = os.environ.get("MASSIVE_API_KEY", "")
BASE = "https://api.polygon.io"
print("key present:", bool(KEY), "| len:", len(KEY))

def probe(path, label=""):
    sep = "&" if "?" in path else "?"
    url = f"{BASE}{path}{sep}apiKey={KEY}"
    shown = url.replace(KEY, "***")
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "JustHodl-Probe/1.0"})
        with urllib.request.urlopen(req, timeout=25) as r:
            body = r.read().decode("utf-8", "replace")
            code = r.status
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", "replace")[:400]; code = e.code
    except Exception as e:
        print(f"\n[{label}] {shown}\n  EXC {type(e).__name__}: {e}"); return
    print(f"\n[{label}] HTTP {code}  {shown}")
    try:
        j = json.loads(body)
        if isinstance(j, dict):
            print("  top keys:", list(j.keys()))
            res = j.get("results") or j.get("data") or j.get("constituents") or j.get("holdings")
            if isinstance(res, list) and res:
                print(f"  results: {len(res)} items | first item keys:", list(res[0].keys()) if isinstance(res[0],dict) else type(res[0]))
                print("  sample[0]:", json.dumps(res[0], default=str)[:500])
            elif isinstance(res, dict):
                print("  results(dict) keys:", list(res.keys())[:20])
            else:
                print("  body head:", json.dumps(j, default=str)[:500])
            if j.get("status") and j.get("status") != "OK":
                print("  STATUS:", j.get("status"), j.get("error") or j.get("message"))
            if j.get("next_url"): print("  has next_url (paginated)")
        else:
            print("  (list) len:", len(j), "first:", json.dumps(j[0], default=str)[:300] if j else "empty")
    except Exception:
        print("  non-JSON body head:", body[:300])

print("="*64); print("ETF GLOBAL CONSTITUENTS candidates"); print("="*64)
probe("/etf-global/v1/constituents?ticker=SPY", "constituents?ticker=SPY")
probe("/etf-global/v1/holdings?ticker=SPY", "holdings?ticker=SPY")
probe("/etf-global/v1/constituents?limit=2", "constituents list")
probe("/etf-global/v1/etf-constituents?ticker=SPY", "etf-constituents?ticker=SPY")

print("\n"+"="*64); print("BENZINGA EARNINGS candidates"); print("="*64)
probe("/benzinga/v1/earnings?ticker=AAPL", "earnings?ticker=AAPL")
probe("/benzinga/v1/earnings?limit=2", "earnings list")
probe("/benzinga/v1/earnings?date.gte=2026-06-01&limit=2", "earnings date filter")
probe("/benzinga/v1/ratings?ticker=AAPL&limit=2", "ratings (namespace check)")

print("\nDONE 1957")
