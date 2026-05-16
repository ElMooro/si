"""ops/600 — diagnostic probes for the 2 remaining issues.

A) FMP /stable/quote on AAPL — print every field name + value to identify
   the correct intraday-change-pct field name (sector-heatmap assumes
   `changePercentage` but FMP /stable/ may use a different name).

B) Read macro-surprise sidecar from S3 and print the shape of a single
   `by_indicator` entry to identify the date+z_score field names ESI
   should look for.
"""
import json, os, urllib.request, urllib.error
import boto3
from datetime import datetime, timezone

s3 = boto3.client("s3", region_name="us-east-1")
ssm = boto3.client("ssm", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"


def get_param(name):
    try:
        return ssm.get_parameter(Name=name, WithDecryption=True)["Parameter"]["Value"]
    except Exception:
        return None


def http_get_json(url, timeout=10):
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "JustHodl/1.0"})
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return json.loads(r.read().decode("utf-8"))
    except Exception as e:
        return {"_err": f"{type(e).__name__}: {str(e)[:200]}"}


def main():
    report = {"started": datetime.now(timezone.utc).isoformat()}

    # ─────────────────────────────────────────────────────────────────
    # A. FMP /stable/quote on AAPL
    # ─────────────────────────────────────────────────────────────────
    fmp = get_param("/justhodl/fmp-key") or "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb"

    # Test BOTH the documented /stable/quote/AAPL pattern and
    # the /stable/quote?symbol=AAPL,MSFT pattern
    urls = [
        f"https://financialmodelingprep.com/stable/quote/AAPL?apikey={fmp}",
        f"https://financialmodelingprep.com/stable/quote?symbol=AAPL&apikey={fmp}",
        f"https://financialmodelingprep.com/stable/quote?symbol=AAPL,MSFT&apikey={fmp}",
        f"https://financialmodelingprep.com/stable/quote-short?symbol=AAPL&apikey={fmp}",
    ]
    fmp_results = {}
    for u in urls:
        # Hide key in label
        label = u.split("apikey=")[0]
        r = http_get_json(u)
        if isinstance(r, list) and r:
            entry = r[0] if isinstance(r[0], dict) else {}
            fmp_results[label] = {
                "type": "list",
                "n": len(r),
                "field_names": sorted(entry.keys()) if isinstance(entry, dict) else None,
                "sample_entry": entry,
            }
        elif isinstance(r, dict):
            fmp_results[label] = {"type": "dict", "data": r}
        else:
            fmp_results[label] = {"type": str(type(r)), "raw": str(r)[:300]}
    report["A_fmp_quote"] = fmp_results

    # ─────────────────────────────────────────────────────────────────
    # B. macro-surprise sidecar shape
    # ─────────────────────────────────────────────────────────────────
    try:
        obj = s3.get_object(Bucket=BUCKET, Key="data/macro-surprise.json")
        macro = json.loads(obj["Body"].read())
    except Exception as e:
        macro = {"_err": str(e)}
    report["B_macro_surprise"] = {
        "top_keys": list(macro.keys()) if isinstance(macro, dict) else None,
        "by_indicator_keys_first_10": (
            list(macro.get("by_indicator", {}).keys())[:10]
            if isinstance(macro.get("by_indicator"), dict) else None
        ),
        "by_indicator_n": (
            len(macro.get("by_indicator", {}))
            if isinstance(macro.get("by_indicator"), dict) else None
        ),
        "sample_entry_first_key": (
            next(iter(macro.get("by_indicator", {}).values()), None)
            if isinstance(macro.get("by_indicator"), dict) else None
        ),
        "sample_entry_field_names": (
            sorted(next(iter(macro.get("by_indicator", {}).values()), {}).keys())
            if isinstance(macro.get("by_indicator"), dict)
            and isinstance(next(iter(macro.get("by_indicator", {}).values()), None), dict)
            else None
        ),
        "by_category_keys": (
            list(macro.get("by_category", {}).keys())
            if isinstance(macro.get("by_category"), dict) else None
        ),
        "top_beats_first_3": (macro.get("top_beats") or [])[:3],
        "top_misses_first_3": (macro.get("top_misses") or [])[:3],
        "composite_z": macro.get("composite_z"),
        "regime": macro.get("regime"),
    }

    report["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs("aws/ops/reports", exist_ok=True)
    with open("aws/ops/reports/600_fmp_and_macro_probe.json", "w") as f:
        json.dump(report, f, indent=2, default=str)
    print(f"DONE -> 600_fmp_and_macro_probe.json")


if __name__ == "__main__":
    main()
