"""1192 — Diagnose why constituents endpoint returned 0 results per ETF.

Test combinations:
  1. No params (just composite_ticker)
  2. Different sort fields (weight, processed_date, effective_date)
  3. Different order (asc, desc)
  4. With and without limit
  5. Different limit values
"""
import json
import urllib.request
import urllib.error
import boto3
from botocore.config import Config

REPORT = "aws/ops/reports/1192_constituents_probe.json"

cfg = Config(read_timeout=120, retries={"max_attempts": 1})
lam = boto3.client("lambda", region_name="us-east-1", config=cfg)
c = lam.get_function_configuration(FunctionName="justhodl-etf-fund-flows")
poly_key = (c.get("Environment") or {}).get("Variables", {}).get("POLYGON_KEY")
print(f"Using POLYGON_KEY len {len(poly_key)}")

BASE = "https://api.polygon.io/etf-global/v1/constituents"
out = {"tests": {}}

def probe(name, url):
    info = {"name": name, "url": url.replace(poly_key, "<KEY>")}
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "JustHodl-Probe/1.0"})
        with urllib.request.urlopen(req, timeout=15) as r:
            data = json.loads(r.read())
            results = data.get("results") or []
            info["ok"] = True
            info["http_status"] = r.status
            info["polygon_status"] = data.get("status")
            info["count"] = data.get("count")
            info["n_results"] = len(results) if isinstance(results, list) else 1
            info["request_id"] = data.get("request_id")
            info["next_url"] = data.get("next_url")
            if results:
                # Show first 3 with weights
                info["sample_first_3"] = [
                    {k: v for k, v in r.items() if k in ["constituent_ticker","constituent_name","weight","market_value","processed_date","effective_date","shares_held"]}
                    for r in (results[:3] if isinstance(results, list) else [results])
                ]
                if isinstance(results, list):
                    info["all_keys"] = list(results[0].keys())
            else:
                info["full_resp"] = data
            return info
    except urllib.error.HTTPError as e:
        body = ""
        try:
            body = e.read().decode("utf-8", errors="ignore")[:500]
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

# Test combinations
tests = [
    ("1_no_params",          f"{BASE}?composite_ticker=SPY&apiKey={poly_key}"),
    ("2_limit_50",           f"{BASE}?composite_ticker=SPY&limit=50&apiKey={poly_key}"),
    ("3_limit_500",          f"{BASE}?composite_ticker=SPY&limit=500&apiKey={poly_key}"),
    ("4_order_desc",         f"{BASE}?composite_ticker=SPY&order=desc&limit=100&apiKey={poly_key}"),
    ("5_sort_processed",     f"{BASE}?composite_ticker=SPY&sort=processed_date&order=desc&limit=100&apiKey={poly_key}"),
    ("6_sort_weight",        f"{BASE}?composite_ticker=SPY&sort=weight&order=desc&limit=100&apiKey={poly_key}"),
    ("7_sort_effective",     f"{BASE}?composite_ticker=SPY&sort=effective_date&order=desc&limit=100&apiKey={poly_key}"),
    ("8_qqq",                f"{BASE}?composite_ticker=QQQ&limit=100&apiKey={poly_key}"),
    ("9_xlk",                f"{BASE}?composite_ticker=XLK&limit=100&apiKey={poly_key}"),
    ("10_date_range",        f"{BASE}?composite_ticker=SPY&processed_date.gte=2026-05-25&limit=500&apiKey={poly_key}"),
]

for name, url in tests:
    r = probe(name, url)
    out["tests"][name] = r
    ok = "✓" if r.get("ok") and r.get("n_results", 0) > 0 else "✗"
    print(f"  {ok} {name:20s} http={r.get('http_status'):>4}  n_results={r.get('n_results')}  count={r.get('count')}  request_id={(r.get('request_id') or '')[:10]}")
    if r.get("ok") and r.get("n_results", 0) > 0:
        sample = r.get("sample_first_3", [{}])[0]
        print(f"     first row: {sample.get('constituent_ticker')} weight={sample.get('weight')} mv={sample.get('market_value')} pd={sample.get('processed_date')}")
    if not r.get("ok"):
        print(f"     err body: {(r.get('body') or '')[:200]}")
    if r.get("n_results", 0) == 0 and r.get("ok") and r.get("full_resp"):
        print(f"     empty resp: {json.dumps(r['full_resp'])[:300]}")

# Try the pagination/next_url approach
# Find first working query
first_ok = next((r for r in out["tests"].values() if r.get("ok") and r.get("n_results", 0) > 0), None)
if first_ok and first_ok.get("next_url"):
    print(f"\n  Pagination: next_url present → {first_ok['next_url']}")

import json
with open(REPORT, "w") as f:
    json.dump(out, f, indent=2, default=str)
print(f"\n[1192] DONE")
