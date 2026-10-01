"""
Ops 1091: inspect actual structure of flow-confluence and best-ideas artifacts.
Writes: aws/ops/reports/1091_structure.json. Never raises.
"""
from __future__ import annotations
import json, os, sys
from datetime import datetime, timezone
OUT_PATH = os.path.join("aws", "ops", "reports", "1091_structure.json")
BUCKET = "justhodl-dashboard-live"
REGION = "us-east-1"
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "shared"))
import boto3
from botocore.exceptions import ClientError
s3 = boto3.client("s3", region_name=REGION)

def inspect(key):
    try:
        r = s3.get_object(Bucket=BUCKET, Key=key)
        d = json.loads(r["Body"].read())
        lm = r["LastModified"]
        # Top-level keys
        keys = list(d.keys()) if isinstance(d, dict) else f"list[{len(d)}]"
        # Look for ticker-like entries
        sample = None
        for k in ["entries", "flows", "tickers", "results", "data", "items"]:
            if isinstance(d, dict) and k in d:
                v = d[k]
                if isinstance(v, list) and v:
                    sample = {"key": k, "sample_keys": list(v[0].keys())[:15] if isinstance(v[0], dict) else type(v[0]).__name__}
                    break
                elif isinstance(v, dict) and v:
                    fk = list(v.keys())[0]
                    sample = {"key": k, "first_subkey": fk, "sample_keys": list(v[fk].keys())[:15] if isinstance(v[fk], dict) else type(v[fk]).__name__}
                    break
        return {"exists": True, "top_keys": keys if isinstance(keys, list) else keys,
                "modified": lm.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "ticker_sample": sample,
                "version": d.get("version", d.get("schema_version", "n/a")) if isinstance(d, dict) else "n/a"}
    except ClientError:
        return {"exists": False}
    except Exception as e:
        return {"error": str(e)[:100]}

def main():
    rep = {"script": "1091_structure",
           "read_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")}
    rep["flow-confluence"] = inspect("data/flow-confluence.json")
    rep["best-ideas"] = inspect("data/best-ideas.json")
    rep["status"] = "COMPLETE"
    os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)
    with open(OUT_PATH, "w") as f:
        json.dump(rep, f, indent=2)
    print(json.dumps(rep, indent=2)[:3000])

if __name__ == "__main__":
    main()
