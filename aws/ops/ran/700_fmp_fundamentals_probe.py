"""ops/700 — probe FMP /stable/ fundamentals endpoints for the 100x Bagger Engine.

The engine needs: ROIC, revenue history (CAGR + variance), gross/operating
margins (trend), reinvestment rate, insider ownership %, debt/cash, valuation.

We probe each candidate endpoint on a real small-cap (we'll use a few) and
print the exact field names so the engine never guesses.
"""
import json, os, urllib.request, urllib.error
import boto3
from datetime import datetime, timezone

ssm = boto3.client("ssm", region_name="us-east-1")
s3 = boto3.client("s3", region_name="us-east-1")


def get_param(name, default=None):
    try:
        return ssm.get_parameter(Name=name, WithDecryption=True)["Parameter"]["Value"]
    except Exception:
        return default


def http_json(url, timeout=15):
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "JustHodl/1.0"})
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return json.loads(r.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        return {"_http_err": e.code}
    except Exception as e:
        return {"_err": f"{type(e).__name__}: {str(e)[:150]}"}


def shape(resp, n_sample=2):
    """Summarize a response: type, length, field names, sample rows."""
    if isinstance(resp, dict) and ("_err" in resp or "_http_err" in resp):
        return resp
    if isinstance(resp, list):
        out = {"type": "list", "n": len(resp)}
        if resp and isinstance(resp[0], dict):
            out["field_names"] = sorted(resp[0].keys())
            out["sample"] = resp[:n_sample]
        elif resp:
            out["sample"] = resp[:n_sample]
        return out
    if isinstance(resp, dict):
        return {"type": "dict", "field_names": sorted(resp.keys()), "sample": resp}
    return {"type": str(type(resp)), "raw": str(resp)[:200]}


def main():
    fmp = get_param("/justhodl/fmp-key") or "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb"
    report = {"started": datetime.now(timezone.utc).isoformat()}

    # Probe on AAPL (large, always works) + a couple small caps to confirm
    # the endpoints return data for small names too.
    test_symbols = ["AAPL", "CELH", "AXON"]
    base = "https://financialmodelingprep.com/stable"

    # Endpoint -> URL template (use {s} for symbol)
    endpoints = {
        "key-metrics":            f"{base}/key-metrics?symbol={{s}}&limit=6&apikey={fmp}",
        "key-metrics-ttm":        f"{base}/key-metrics-ttm?symbol={{s}}&apikey={fmp}",
        "ratios":                 f"{base}/ratios?symbol={{s}}&limit=6&apikey={fmp}",
        "ratios-ttm":             f"{base}/ratios-ttm?symbol={{s}}&apikey={fmp}",
        "income-statement":       f"{base}/income-statement?symbol={{s}}&limit=6&apikey={fmp}",
        "income-statement-growth": f"{base}/income-statement-growth?symbol={{s}}&limit=6&apikey={fmp}",
        "balance-sheet-statement": f"{base}/balance-sheet-statement?symbol={{s}}&limit=2&apikey={fmp}",
        "cash-flow-statement":    f"{base}/cash-flow-statement?symbol={{s}}&limit=3&apikey={fmp}",
        "financial-growth":       f"{base}/financial-growth?symbol={{s}}&limit=6&apikey={fmp}",
        "profile":                f"{base}/profile?symbol={{s}}&apikey={fmp}",
        "enterprise-values":      f"{base}/enterprise-values?symbol={{s}}&limit=2&apikey={fmp}",
    }

    # Ownership endpoints (may live at different paths)
    ownership_candidates = {
        "insider-ownership":          f"{base}/insider-ownership?symbol={{s}}&apikey={fmp}",
        "institutional-ownership":    f"{base}/institutional-ownership/symbol-ownership?symbol={{s}}&apikey={fmp}",
        "shares-float":               f"{base}/shares-float?symbol={{s}}&apikey={fmp}",
        "key-executives":             f"{base}/key-executives?symbol={{s}}&apikey={fmp}",
        "insider-trading-statistics": f"{base}/insider-trading/statistics?symbol={{s}}&apikey={fmp}",
    }

    # --- probe fundamentals on AAPL only (field names are symbol-independent) ---
    fund = {}
    for name, tmpl in endpoints.items():
        fund[name] = shape(http_json(tmpl.format(s="AAPL")))
    report["fundamentals_AAPL"] = fund

    # --- probe ownership endpoints on AAPL ---
    own = {}
    for name, tmpl in ownership_candidates.items():
        own[name] = shape(http_json(tmpl.format(s="AAPL")))
    report["ownership_AAPL"] = own

    # --- confirm small-cap coverage: key-metrics + income on CELH/AXON ---
    smallcap = {}
    for s in ["CELH", "AXON"]:
        smallcap[s] = {
            "key-metrics": shape(http_json(endpoints["key-metrics"].format(s=s)), n_sample=1),
            "income-statement-growth": shape(
                http_json(endpoints["income-statement-growth"].format(s=s)), n_sample=1),
        }
    report["smallcap_coverage"] = smallcap

    # --- universe.json sanity ---
    try:
        obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="data/universe.json")
        u = json.loads(obj["Body"].read())
        stocks = u.get("stocks", [])
        report["universe"] = {
            "total": len(stocks),
            "stats": u.get("stats"),
            "sample_stock": stocks[0] if stocks else None,
            "by_bucket_count": {
                b: sum(1 for x in stocks if x.get("cap_bucket") == b)
                for b in ["nano", "micro", "small", "mid", "large", "mega"]
            },
        }
    except Exception as e:
        report["universe"] = {"_err": str(e)[:200]}

    report["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs("aws/ops/reports", exist_ok=True)
    with open("aws/ops/reports/700_fmp_fundamentals_probe.json", "w") as f:
        json.dump(report, f, indent=2, default=str)
    print("DONE -> 700_fmp_fundamentals_probe.json")


if __name__ == "__main__":
    main()
