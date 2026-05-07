#!/usr/bin/env python3
"""Step 313 — Diagnose the 5 no-data FRED series in divergence-v2 and find
working replacements.

The 5 broken series (from divergence-v2.json no_data_pairs):
  USSLIND  — Leading Economic Index (St Louis)       → multiple pairs
  USPHCI   — Coincident Economic Activity Index      → 1 pair
  CHNPROINDMISMEI — China Industrial Production      → 1 pair
  LRHUTTTTCHM156S — Switzerland Unemployment         → 1 pair

Strategy:
  1. Hit FRED API directly for each → capture error
  2. Try plausible alternate IDs for each
  3. For each pair, report which IDs succeed vs fail
  4. Output: a JSON of suggested replacements ready to patch
"""
import json
import os
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone

FRED_KEY = "2f057499936072679d8843d7fce99989"
REPORT = "aws/ops/reports/313_diagnose_no_data.json"

# For each broken series, list candidate replacements (in priority order)
CANDIDATES = {
    "USSLIND": [
        "USSLIND",        # Leading Index for the United States (current)
        "LEIBOR",         # alternate
        "USPHCI",         # Coincident — likely also broken but try
        "T10Y2Y",         # yield curve as fallback
    ],
    "USPHCI": [
        "USPHCI",         # Coincident Economic Activity Index
        "USPBS",          # alternate
        "INDPRO",         # IP as fallback
    ],
    "CHNPROINDMISMEI": [
        "CHNPROINDMISMEI",   # OECD MEI China industrial production
        "CHNPROINDQISMEI",   # quarterly variant
        "CHNCPIALLMINMEI",   # CPI alternative
        "CHNXTEXVA01CXMLM",  # exports
        "MKTGDPCNA646NWDB",  # World Bank China GDP
    ],
    "LRHUTTTTCHM156S": [
        "LRHUTTTTCHM156S",   # OECD harmonized CH unemployment monthly SA
        "LRHUTTTTCHQ156S",   # quarterly variant
        "LRHUTTTTCHA156S",   # annual
        "LRUNTTTTCHM156S",   # alternate harmonized format
        "LRUN64TTCHM156S",   # 15-64 age band
        "LRUN24TTCHM156S",   # 15-24
    ],
}

# Also test known-good series as control (should succeed)
CONTROL = ["UNRATE", "INDPRO", "T10Y2Y", "DEXUSEU"]


def test_fred(series_id):
    """Fetch most recent observations from FRED for a series."""
    qs = urllib.parse.urlencode({
        "series_id": series_id,
        "api_key": FRED_KEY,
        "file_type": "json",
        "limit": 5,
        "sort_order": "desc",
    })
    url = f"https://api.stlouisfed.org/fred/series/observations?{qs}"
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "justhodl-diag/1.0"})
        with urllib.request.urlopen(req, timeout=10) as r:
            data = json.loads(r.read().decode("utf-8"))
        observations = data.get("observations", [])
        if not observations:
            return {"ok": False, "err": "empty observations", "n": 0}
        # Find latest with actual numeric value (FRED uses "." for missing)
        latest = None
        for obs in observations:
            v = obs.get("value")
            if v and v not in (".", "", "NaN"):
                latest = obs
                break
        return {
            "ok": True,
            "n_obs": len(observations),
            "latest_date": latest.get("date") if latest else None,
            "latest_value": latest.get("value") if latest else None,
            "all_dates": [o.get("date") for o in observations[:3]],
        }
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", errors="replace")[:200]
        return {"ok": False, "err": f"HTTP {e.code}: {body}", "code": e.code}
    except Exception as e:
        return {"ok": False, "err": str(e)[:200], "type": type(e).__name__}


def test_series_metadata(series_id):
    """Check FRED series metadata to confirm it exists at all."""
    qs = urllib.parse.urlencode({
        "series_id": series_id,
        "api_key": FRED_KEY,
        "file_type": "json",
    })
    url = f"https://api.stlouisfed.org/fred/series?{qs}"
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "justhodl-diag/1.0"})
        with urllib.request.urlopen(req, timeout=10) as r:
            data = json.loads(r.read().decode("utf-8"))
        ss = data.get("seriess", [])
        if not ss:
            return {"meta_ok": False, "err": "no series in response"}
        s = ss[0]
        return {
            "meta_ok": True,
            "title": s.get("title", "")[:80],
            "frequency": s.get("frequency"),
            "units": s.get("units_short"),
            "last_updated": s.get("last_updated"),
            "observation_start": s.get("observation_start"),
            "observation_end": s.get("observation_end"),
            "popularity": s.get("popularity"),
        }
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", errors="replace")[:200]
        return {"meta_ok": False, "err": f"HTTP {e.code}: {body}", "code": e.code}
    except Exception as e:
        return {"meta_ok": False, "err": str(e)[:200]}


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}

    # Test control series first (should all succeed)
    out["control_test"] = {}
    for sid in CONTROL:
        out["control_test"][sid] = test_fred(sid)
        time.sleep(0.2)

    # Diagnose each broken series + try alternates
    out["diagnosis"] = {}
    for broken, candidates in CANDIDATES.items():
        diag = {"broken_id": broken, "candidates": []}
        for cand in candidates:
            meta = test_series_metadata(cand)
            time.sleep(0.2)
            obs = test_fred(cand) if meta.get("meta_ok") else {"ok": False, "err": "no metadata"}
            time.sleep(0.2)
            diag["candidates"].append({
                "series_id": cand,
                **meta,
                "obs_test": obs,
                "verdict": "WORKS" if (meta.get("meta_ok") and obs.get("ok")) else "BROKEN",
            })
        # Pick first working
        working = next((c for c in diag["candidates"] if c["verdict"] == "WORKS"), None)
        diag["recommended_replacement"] = working["series_id"] if working else None
        out["diagnosis"][broken] = diag

    # Build the patch dict
    out["patches"] = {}
    for broken, diag in out["diagnosis"].items():
        if diag["recommended_replacement"] and diag["recommended_replacement"] != broken:
            out["patches"][broken] = diag["recommended_replacement"]
        elif diag["recommended_replacement"] == broken:
            out["patches"][broken] = "STILL_WORKS_RETRY"
        else:
            out["patches"][broken] = "NO_VIABLE_ALTERNATIVE"

    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f:
        json.dump(out, f, indent=2, default=str)

    # Print summary
    print("═" * 70)
    print("  CONTROL TEST (known-good series)")
    print("═" * 70)
    for sid, r in out["control_test"].items():
        ok = "✅" if r.get("ok") else "❌"
        print(f"  {ok} {sid:<12s}: {r.get('latest_date','?')} = {r.get('latest_value','?')}  ({r.get('err','OK')})")

    print()
    print("═" * 70)
    print("  DIAGNOSIS — 5 broken series + alternates")
    print("═" * 70)
    for broken, diag in out["diagnosis"].items():
        print(f"\n  📍 {broken}")
        for c in diag["candidates"]:
            ok = "✅" if c["verdict"] == "WORKS" else "❌"
            title = c.get("title", "")[:50] if c.get("meta_ok") else c.get("err","unknown")[:50]
            obs = c.get("obs_test", {})
            latest = f"{obs.get('latest_date','?')}={obs.get('latest_value','?')}" if obs.get("ok") else "no data"
            print(f"    {ok} {c['series_id']:<22s} | {title:<50s} | {latest}")
        rec = diag["recommended_replacement"]
        print(f"    → recommended: {rec or '(NONE)'}")

    print()
    print("═" * 70)
    print("  PATCH PLAN")
    print("═" * 70)
    for broken, replacement in out["patches"].items():
        print(f"  {broken} → {replacement}")


if __name__ == "__main__":
    main()
