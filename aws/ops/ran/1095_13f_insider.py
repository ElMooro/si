"""
Ops 1095: discover 13F and insider artifact paths for t360 registry.
Writes: aws/ops/reports/1095_13f_insider.json. Never raises.
"""
from __future__ import annotations
import json, os, sys
from datetime import datetime, timezone
OUT_PATH = os.path.join("aws", "ops", "reports", "1095_13f_insider.json")
BUCKET = "justhodl-dashboard-live"
REGION = "us-east-1"
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "shared"))
import boto3
from botocore.exceptions import ClientError
s3 = boto3.client("s3", region_name=REGION)

CANDIDATES = [
    "data/13f-clone-alpha.json",
    "data/13f-positions.json",
    "data/smart-money-13f.json",
    "data/insider-cluster.json",
    "data/insider-trades.json",
    "data/edgar-insiders.json",
    "data/insider-aggregate.json",
]

def check(key):
    try:
        r = s3.head_object(Bucket=BUCKET, Key=key)
        lm = r["LastModified"]
        age_h = (datetime.now(timezone.utc) - lm).total_seconds() / 3600
        return {"exists": True, "modified": lm.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "age_h": round(age_h, 1), "size": r.get("ContentLength", 0)}
    except ClientError:
        return {"exists": False}
    except Exception as e:
        return {"error": str(e)[:60]}

def main():
    rep = {"script": "1095_13f_insider",
           "read_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
           "candidates": {}}
    for key in CANDIDATES:
        rep["candidates"][key] = check(key)
    # Also list data/ prefix for 13f/insider
    try:
        for prefix in ["data/13f", "data/insider"]:
            r = s3.list_objects_v2(Bucket=BUCKET, Prefix=prefix, MaxKeys=15)
            objs = [{"key": o["Key"][:60],
                     "modified": o["LastModified"].strftime("%Y-%m-%dT%H:%M:%SZ"),
                     "size": o["Size"]}
                    for o in r.get("Contents", [])]
            rep[f"list_{prefix.replace('/', '_')}"] = objs
    except Exception as e:
        rep["list_error"] = str(e)[:80]
    rep["status"] = "COMPLETE"
    os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)
    with open(OUT_PATH, "w") as f:
        json.dump(rep, f, indent=2)
    found = [k for k, v in rep["candidates"].items() if v.get("exists")]
    print(json.dumps({"found": found}))

if __name__ == "__main__":
    main()
