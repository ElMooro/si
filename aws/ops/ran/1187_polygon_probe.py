"""1187 — Diagnostic probe of Polygon endpoints.

We need to find:
  1. Which indices actually return data (I:VIX, I:SPX, I:NDX, I:RUT, I:VVIX)
  2. The correct futures aggregates endpoint
  3. Active front-month contracts for ES, NQ, VX, ZN, ZB, ZT, CL, GC, HG

Tests all candidate endpoints + saves results so we can rewrite the
macro-regime Lambda with VERIFIED working paths.
"""
import json
import time
import urllib.request
import urllib.error
from datetime import datetime, timezone, timedelta
import boto3
from botocore.config import Config

REPORT = "aws/ops/reports/1187_polygon_probe.json"

cfg = Config(read_timeout=120, retries={"max_attempts": 1})
lam = boto3.client("lambda", region_name="us-east-1", config=cfg)

# Pull polygon key from etf-fund-flows Lambda
poly_key = None
try:
    c = lam.get_function_configuration(FunctionName="justhodl-etf-fund-flows")
    poly_key = (c.get("Environment") or {}).get("Variables", {}).get("POLYGON_KEY")
except Exception as e:
    print(f"Couldn't get key: {e}")

if not poly_key:
    print("❌ No Polygon key")
    raise SystemExit(1)

print(f"Using POLYGON_KEY (len {len(poly_key)})")

out = {"started": datetime.now(timezone.utc).isoformat(), "tests": {}}

end_date = datetime.now(timezone.utc).date()
start_date = end_date - timedelta(days=30)


def probe(name: str, url: str, timeout: int = 12) -> dict:
    """Test one URL, return {ok, status, n_results, sample, error}."""
    info = {"name": name, "url": url.replace(poly_key, "<KEY>")}
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "JustHodl-Probe/1.0"})
        with urllib.request.urlopen(req, timeout=timeout) as r:
            data = json.loads(r.read())
            results = data.get("results") or []
            info["ok"] = True
            info["http_status"] = r.status
            info["polygon_status"] = data.get("status")
            info["n_results"] = len(results) if isinstance(results, list) else 1
            if results:
                info["sample"] = results[0] if isinstance(results, list) else results
            else:
                info["sample"] = None
                info["full_resp"] = data
            return info
    except urllib.error.HTTPError as e:
        body = ""
        try:
            body = e.read().decode("utf-8", errors="ignore")[:400]
        except Exception:
            pass
        info["ok"] = False
        info["http_status"] = e.code
        info["body"] = body
        return info
    except Exception as e:
        info["ok"] = False
        info["error"] = str(e)[:200]
        return info


# ═══════════════════════════════════════════════════════════════════
# TEST 1: Indices — try with and without I: prefix
# ═══════════════════════════════════════════════════════════════════
print("\n══ TEST 1: Indices ══")
indices = ["I:VIX", "I:SPX", "I:NDX", "I:RUT", "I:VVIX", "I:VIX9D",
           "VIX", "SPX", "NDX", "VIXY", "VXX"]
out["tests"]["indices"] = {}
for sym in indices:
    url = (f"https://api.polygon.io/v2/aggs/ticker/{sym}/range/1/day/"
           f"{start_date}/{end_date}?adjusted=true&sort=desc&limit=5&apiKey={poly_key}")
    r = probe(sym, url)
    out["tests"]["indices"][sym] = r
    ok = "✓" if r.get("ok") and r.get("n_results", 0) > 0 else "✗"
    print(f"  {ok} {sym:12s} status={r.get('http_status')} n={r.get('n_results')}")


# ═══════════════════════════════════════════════════════════════════
# TEST 2: Futures contracts reference — discover front-month
# ═══════════════════════════════════════════════════════════════════
print("\n══ TEST 2: Futures Contracts Reference ══")
# Try multiple base URL patterns since Polygon has a separate futures API
products = ["ES", "NQ", "VX", "ZN", "ZB", "ZT", "CL", "GC", "HG"]

out["tests"]["futures_reference"] = {}
# Try the /futures/v1/contracts endpoint
for prod in products:
    url = (f"https://api.polygon.io/futures/v1/contracts"
           f"?product_code={prod}&active=true&order=asc&sort=last_trade_date&limit=3&apiKey={poly_key}")
    r = probe(f"contracts_{prod}", url)
    out["tests"]["futures_reference"][prod] = r
    ok = "✓" if r.get("ok") and r.get("n_results", 0) > 0 else "✗"
    contracts = []
    if r.get("ok") and r.get("sample"):
        # Sample is first contract
        if isinstance(r["sample"], dict):
            t = r["sample"].get("ticker") or r["sample"].get("symbol")
            if t: contracts.append(t)
    print(f"  {ok} product={prod:4s} status={r.get('http_status')} n={r.get('n_results')} sample={contracts}")


# ═══════════════════════════════════════════════════════════════════
# TEST 3: Try specific contract codes (hardcoded front-month guesses)
# ═══════════════════════════════════════════════════════════════════
# Month codes for July 2026 (front-month for current date 2026-06-02):
# F=Jan, G=Feb, H=Mar, J=Apr, K=May, M=Jun, N=Jul, Q=Aug, U=Sep, V=Oct, X=Nov, Z=Dec
# Single digit year: 6 = 2026
# Front-month guesses based on quarterly + monthly cycles:
print("\n══ TEST 3: Specific Front-Month Contract Probes ══")
guesses = [
    # ES (E-Mini S&P) — quarterly H/M/U/Z
    "ESM6", "ESU6", "ESZ6",
    # NQ (E-Mini Nasdaq) — quarterly
    "NQM6", "NQU6",
    # VX (VIX) — monthly, current month
    "VXM6", "VXN6", "VXQ6",
    # ZN (10Y T-Note) — quarterly H/M/U/Z
    "ZNM6", "ZNU6",
    # ZB (30Y T-Bond)
    "ZBM6", "ZBU6",
    # ZT (2Y T-Note)
    "ZTM6", "ZTU6",
    # CL (Crude oil) — monthly
    "CLN6", "CLQ6", "CLM6",
    # GC (Gold) — even months (G,J,M,Q,V,Z)
    "GCM6", "GCQ6", "GCV6",
    # HG (Copper) — quarterly H/K/N/U/Z
    "HGN6", "HGU6",
]
out["tests"]["futures_aggs"] = {}
for sym in guesses:
    # Try standard aggs endpoint
    url = (f"https://api.polygon.io/v2/aggs/ticker/{sym}/range/1/day/"
           f"{start_date}/{end_date}?adjusted=true&sort=desc&limit=5&apiKey={poly_key}")
    r = probe(f"aggs_{sym}", url)
    out["tests"]["futures_aggs"][sym] = r
    ok = "✓" if r.get("ok") and r.get("n_results", 0) > 0 else "✗"
    close = None
    if r.get("ok") and r.get("sample"):
        close = r["sample"].get("c")
    print(f"  {ok} {sym:6s} status={r.get('http_status')} n={r.get('n_results')} close={close}")


# ═══════════════════════════════════════════════════════════════════
# TEST 4: Polygon Futures API specific aggregates endpoint
# ═══════════════════════════════════════════════════════════════════
print("\n══ TEST 4: /futures/v1/aggs endpoint (if exists) ══")
out["tests"]["futures_v1_aggs"] = {}
for sym in ["ESM6", "ESU6", "VXM6", "ZNM6", "CLN6", "GCQ6"]:
    url = (f"https://api.polygon.io/futures/v1/aggs/{sym}/range/1/day/"
           f"{start_date}/{end_date}?limit=5&apiKey={poly_key}")
    r = probe(f"futures_v1_{sym}", url)
    out["tests"]["futures_v1_aggs"][sym] = r
    ok = "✓" if r.get("ok") and r.get("n_results", 0) > 0 else "✗"
    print(f"  {ok} {sym:6s} status={r.get('http_status')} n={r.get('n_results')}")


# ═══════════════════════════════════════════════════════════════════
# TEST 5: Forex — confirm what we already know works
# ═══════════════════════════════════════════════════════════════════
print("\n══ TEST 5: FX sanity check ══")
out["tests"]["fx_sanity"] = {}
for sym in ["C:EURUSD", "C:USDJPY", "C:DXY"]:
    url = (f"https://api.polygon.io/v2/aggs/ticker/{sym}/range/1/day/"
           f"{start_date}/{end_date}?adjusted=true&sort=desc&limit=5&apiKey={poly_key}")
    r = probe(sym, url)
    out["tests"]["fx_sanity"][sym] = r
    ok = "✓" if r.get("ok") and r.get("n_results", 0) > 0 else "✗"
    close = None
    if r.get("ok") and r.get("sample"):
        close = r["sample"].get("c")
    print(f"  {ok} {sym:12s} status={r.get('http_status')} n={r.get('n_results')} close={close}")


# Summary
print("\n══ SUMMARY ══")
working = {
    "indices": [s for s, r in out["tests"]["indices"].items() if r.get("ok") and r.get("n_results", 0) > 0],
    "futures_aggs": [s for s, r in out["tests"]["futures_aggs"].items() if r.get("ok") and r.get("n_results", 0) > 0],
    "futures_v1": [s for s, r in out["tests"]["futures_v1_aggs"].items() if r.get("ok") and r.get("n_results", 0) > 0],
    "futures_ref": [p for p, r in out["tests"]["futures_reference"].items() if r.get("ok") and r.get("n_results", 0) > 0],
    "fx": [s for s, r in out["tests"]["fx_sanity"].items() if r.get("ok") and r.get("n_results", 0) > 0],
}
out["working_summary"] = working
for cat, lst in working.items():
    print(f"  {cat}: {lst}")

out["finished"] = datetime.now(timezone.utc).isoformat()
with open(REPORT, "w") as f:
    json.dump(out, f, indent=2, default=str)
print(f"\n[1187] DONE")
