"""
Ops 1096: inspect 13F/insider artifact structures for t360 registry.
Writes: aws/ops/reports/1096_13f_structure.json. Never raises.
"""
from __future__ import annotations
import json, os, sys
from datetime import datetime, timezone
OUT_PATH = os.path.join("aws", "ops", "reports", "1096_13f_structure.json")
BUCKET = "justhodl-dashboard-live"
REGION = "us-east-1"
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "shared"))
import boto3
s3 = boto3.client("s3", region_name=REGION)

def inspect(key, max_bytes=50000):
    try:
        r = s3.get_object(Bucket=BUCKET, Key=key, Range=f"bytes=0-{max_bytes}")
        d = json.loads(r["Body"].read())
        if isinstance(d, dict):
            keys = list(d.keys())[:10]
            # Check for ticker-like structure
            tickers = None
            for k in ["tickers", "by_ticker", "data", "positions", "trades"]:
                if k in d and isinstance(d[k], dict):
                    tk = list(d[k].keys())[:3]
                    tickers = {"key": k, "sample_tickers": tk}
                    break
            return {"top_keys": keys, "ticker_structure": tickers,
                    "is_dict": True}
        elif isinstance(d, list):
            sample = d[0] if d else None
            return {"is_list": True, "len": len(d),
                    "sample_keys": list(sample.keys())[:10] if isinstance(sample, dict) else None}
        return {"type": type(d).__name__}
    except Exception as e:
        return {"error": str(e)[:80]}

def main():
    rep = {"script": "1096_13f_structure",
           "read_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")}
    for key in ["data/13f-by-ticker.json", "data/insider-trades.json",
                "data/13f-positions.json", "data/insider-aggregate.json"]:
        rep[key] = inspect(key)
    rep["status"] = "COMPLETE"
    os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)
    with open(OUT_PATH, "w") as f:
        json.dump(rep, f, indent=2)
    print(json.dumps({k: v.get("ticker_structure", v.get("sample_keys", v.get("error")))
                      for k, v in rep.items() if k.startswith("data/")}, indent=2)[:2000])

if __name__ == "__main__":
    main()
