"""Ops 1104: check ticker-360 artifact version and domain count."""
from __future__ import annotations
import json, os, sys
from datetime import datetime, timezone
OUT = os.path.join("aws", "ops", "reports", "1104_t360_check.json")
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "shared"))
import boto3
s3 = boto3.client("s3", region_name="us-east-1")
def main():
    rep = {"read_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")}
    try:
        r = s3.get_object(Bucket="justhodl-dashboard-live", Key="data/ticker-360.json")
        # Read first 100KB to get metadata
        data = json.loads(r["Body"].read(102400))
        # Try to get full structure info
        rep["version"] = data.get("version", data.get("artifact_version", "unknown"))
        rep["generated_at"] = data.get("generated_at", "unknown")
        # Count domains from a sample ticker
        tickers = data.get("tickers", {})
        if tickers:
            sample = list(tickers.values())[0]
            domains = sample.get("domains", {})
            rep["sample_ticker_domains"] = len(domains)
            rep["sample_domain_names"] = sorted(domains.keys())[:30]
        rep["universe_size"] = data.get("universe_size", len(tickers))
        rep["modified"] = r["LastModified"].strftime("%Y-%m-%dT%H:%M:%SZ")
    except Exception as e:
        rep["error"] = str(e)[:150]
    os.makedirs(os.path.dirname(OUT), exist_ok=True)
    open(OUT,"w").write(json.dumps(rep,indent=2))
    print(json.dumps({k: v for k, v in rep.items() if k != "sample_domain_names"}, indent=2)[:1500])
if __name__=="__main__":
    main()
