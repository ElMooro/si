"""
Ops 1100: discover earnings calendar artifacts.
Writes: aws/ops/reports/1100_earnings.json. Never raises.
"""
from __future__ import annotations
import json, os, sys
from datetime import datetime, timezone
OUT_PATH = os.path.join("aws", "ops", "reports", "1100_earnings.json")
BUCKET = "justhodl-dashboard-live"
REGION = "us-east-1"
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "shared"))
import boto3
from botocore.exceptions import ClientError
s3 = boto3.client("s3", region_name=REGION)
CANDIDATES = ["data/earnings-calendar.json", "data/earnings-tracker.json",
              "data/catalyst-calendar.json", "data/econ-calendar.json"]
def check(k):
    try:
        r = s3.head_object(Bucket=BUCKET, Key=k)
        return {"exists": True, "size": r.get("ContentLength", 0),
                "modified": r["LastModified"].strftime("%Y-%m-%dT%H:%M:%SZ")}
    except ClientError:
        return {"exists": False}
    except Exception as e:
        return {"error": str(e)[:60]}
def main():
    rep = {"read_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")}
    for k in CANDIDATES:
        rep[k] = check(k)
    os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)
    open(OUT_PATH, "w").write(json.dumps(rep, indent=2))
    print(json.dumps({k: v.get("exists") for k, v in rep.items() if k.startswith("data/")}))
if __name__ == "__main__":
    main()
