"""1193 — Probe FMP ETF holdings endpoint as alternative to Polygon constituents.

User has FMP \$99/mo plan. If /stable/etf/holdings works, we pivot the
constituents Lambda from Polygon (403 NOT_AUTHORIZED) to FMP (free with
existing subscription).
"""
import json
import urllib.request
import urllib.error
import boto3
from botocore.config import Config

REPORT = "aws/ops/reports/1193_fmp_holdings_probe.json"

cfg = Config(read_timeout=120, retries={"max_attempts": 1})
lam = boto3.client("lambda", region_name="us-east-1", config=cfg)
# Pull FMP key from equity-research Lambda
c = lam.get_function_configuration(FunctionName="justhodl-equity-research")
fmp_key = (c.get("Environment") or {}).get("Variables", {}).get("FMP_KEY")
print(f"Using FMP_KEY len {len(fmp_key) if fmp_key else 0}")

out = {"tests": {}}


def probe(name, url):
    info = {"name": name, "url": url.replace(fmp_key, "<KEY>") if fmp_key else url}
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "JustHodl-Probe/1.0"})
        with urllib.request.urlopen(req, timeout=15) as r:
            data = json.loads(r.read())
            info["ok"] = True
            info["http_status"] = r.status
            if isinstance(data, list):
                info["n_results"] = len(data)
                if data:
                    info["sample_keys"] = list(data[0].keys())
                    info["sample_first_3"] = data[:3]
            elif isinstance(data, dict):
                if "error" in data or "message" in data or "Error Message" in data:
                    info["error_in_body"] = data
                else:
                    info["sample_keys"] = list(data.keys())
                    info["sample"] = data
                    info["n_results"] = 1
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


tests = [
    # Latest FMP stable endpoints
    ("1_stable_holdings_SPY",         f"https://financialmodelingprep.com/stable/etf/holdings?symbol=SPY&apikey={fmp_key}"),
    ("2_stable_holdings_QQQ",         f"https://financialmodelingprep.com/stable/etf/holdings?symbol=QQQ&apikey={fmp_key}"),
    ("3_stable_holdings_XLK",         f"https://financialmodelingprep.com/stable/etf/holdings?symbol=XLK&apikey={fmp_key}"),
    ("4_stable_holdings_EWU",         f"https://financialmodelingprep.com/stable/etf/holdings?symbol=EWU&apikey={fmp_key}"),
    ("5_stable_holdings_IBIT",        f"https://financialmodelingprep.com/stable/etf/holdings?symbol=IBIT&apikey={fmp_key}"),
    # Holding-dates endpoint
    ("6_holding_dates_SPY",           f"https://financialmodelingprep.com/stable/etf/holding-dates?symbol=SPY&apikey={fmp_key}"),
    # Asset exposure (reverse: which ETFs hold AAPL)
    ("7_asset_exposure_AAPL",         f"https://financialmodelingprep.com/stable/etf/asset-exposure?symbol=AAPL&apikey={fmp_key}"),
    # Sector + country weighting
    ("8_sector_weighting_SPY",        f"https://financialmodelingprep.com/stable/etf/sector-weightings?symbol=SPY&apikey={fmp_key}"),
    ("9_country_weighting_SPY",       f"https://financialmodelingprep.com/stable/etf/country-weightings?symbol=SPY&apikey={fmp_key}"),
    # ETF info
    ("10_info_SPY",                   f"https://financialmodelingprep.com/stable/etf/info?symbol=SPY&apikey={fmp_key}"),
]

for name, url in tests:
    r = probe(name, url)
    out["tests"][name] = r
    ok = "✓" if r.get("ok") and r.get("n_results", 0) > 0 else "✗"
    print(f"  {ok} {name:32s} http={r.get('http_status'):>4}  n_results={r.get('n_results')}")
    if r.get("sample_keys"):
        print(f"     keys: {r['sample_keys'][:10]}")
    if r.get("sample_first_3"):
        first = r["sample_first_3"][0]
        print(f"     first row: {json.dumps(first, default=str)[:240]}")
    if r.get("body"):
        print(f"     err: {r['body'][:200]}")
    if r.get("error_in_body"):
        print(f"     api error: {json.dumps(r['error_in_body'])[:300]}")

# Show winner
print("\nSummary of working endpoints:")
working = []
for name, r in out["tests"].items():
    if r.get("ok") and r.get("n_results", 0) > 0:
        working.append({"endpoint": name, "n": r.get("n_results")})
print(json.dumps(working, indent=2))

out["working"] = working
with open(REPORT, "w") as f:
    json.dump(out, f, indent=2, default=str)
print(f"\n[1193] DONE")
