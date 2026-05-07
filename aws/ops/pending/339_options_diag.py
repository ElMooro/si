#!/usr/bin/env python3
"""Step 339 — Diagnose why Polygon options snapshot returns no IV."""
import json
import os
import urllib.parse
import urllib.request
from datetime import date, datetime, timedelta, timezone

REPORT = "aws/ops/reports/339_options_diag.json"
POLY_KEY = "zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d"


def hit(url):
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "diag/1.0"})
        with urllib.request.urlopen(req, timeout=30) as r:
            return json.loads(r.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        return {"_err": "HTTP " + str(e.code), "_body": e.read().decode("utf-8", errors="replace")[:300]}
    except Exception as e:
        return {"_err": str(e)[:200]}


def main():
    out = {"as_of": datetime.now(timezone.utc).isoformat(), "tests": {}}

    # Test 1: options snapshot for SPY — what we use now
    today = date.today()
    expiry_min = (today + timedelta(days=16)).strftime("%Y-%m-%d")
    expiry_max = (today + timedelta(days=44)).strftime("%Y-%m-%d")
    qs = urllib.parse.urlencode({
        "apiKey": POLY_KEY,
        "expiration_date.gte": expiry_min,
        "expiration_date.lte": expiry_max,
        "limit": 250,
    })
    url1 = f"https://api.polygon.io/v3/snapshot/options/SPY?{qs}"
    print(f"[339] Test 1: {url1}")
    d1 = hit(url1)
    if d1.get("_err"):
        out["tests"]["v3_snapshot_options_SPY"] = {"err": d1["_err"], "body": d1.get("_body","")[:200]}
    else:
        results = d1.get("results") or []
        out["tests"]["v3_snapshot_options_SPY"] = {
            "status": d1.get("status"),
            "count": len(results),
            "next_url": bool(d1.get("next_url")),
            "first_record_keys": list(results[0].keys())[:15] if results else None,
            "first_iv": results[0].get("implied_volatility") if results else None,
            "first_greeks": results[0].get("greeks") if results else None,
            "first_strike": (results[0].get("details") or {}).get("strike_price") if results else None,
            "first_expiry": (results[0].get("details") or {}).get("expiration_date") if results else None,
            "first_underlying": results[0].get("underlying_asset") if results else None,
        }

    # Test 2: contract reference (alternative endpoint)
    qs2 = urllib.parse.urlencode({
        "apiKey": POLY_KEY,
        "underlying_ticker": "SPY",
        "expiration_date.gte": expiry_min,
        "expiration_date.lte": expiry_max,
        "limit": 10,
    })
    url2 = f"https://api.polygon.io/v3/reference/options/contracts?{qs2}"
    print(f"[339] Test 2: {url2}")
    d2 = hit(url2)
    if d2.get("_err"):
        out["tests"]["v3_reference_contracts"] = {"err": d2["_err"]}
    else:
        results = d2.get("results") or []
        out["tests"]["v3_reference_contracts"] = {
            "count": len(results),
            "first_record": results[0] if results else None,
        }

    # Test 3: options snapshot (alt path - lower rate limit?)
    url3 = f"https://api.polygon.io/v3/snapshot/options/SPY?apiKey={POLY_KEY}&limit=10"
    print(f"[339] Test 3: {url3}")
    d3 = hit(url3)
    results = d3.get("results") or []
    out["tests"]["v3_snapshot_no_filter"] = {
        "status": d3.get("status"),
        "count": len(results),
        "first_iv": results[0].get("implied_volatility") if results else None,
        "first_greeks_keys": list((results[0].get("greeks") or {}).keys()) if results else None,
        "underlying": (results[0].get("underlying_asset") or {}).get("price") if results else None,
    }

    # Test 4: single contract snapshot
    if results:
        contract_ticker = (results[0].get("details") or {}).get("ticker")
        if contract_ticker:
            url4 = f"https://api.polygon.io/v3/snapshot/options/SPY/{contract_ticker}?apiKey={POLY_KEY}"
            print(f"[339] Test 4: {url4}")
            d4 = hit(url4)
            single = d4.get("results")
            out["tests"]["single_contract"] = {
                "status": d4.get("status"),
                "got_data": single is not None,
                "iv": single.get("implied_volatility") if single else None,
                "greeks": single.get("greeks") if single else None,
                "day": single.get("day") if single else None,
                "underlying": single.get("underlying_asset") if single else None,
            }

    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f:
        json.dump(out, f, indent=2, default=str)
    print(json.dumps(out, indent=2, default=str)[:5000])


if __name__ == "__main__":
    main()
