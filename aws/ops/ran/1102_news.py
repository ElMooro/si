"""Ops 1102: discover news sentiment artifacts."""
from __future__ import annotations
import json, os, sys
from datetime import datetime, timezone
OUT = os.path.join("aws", "ops", "reports", "1102_news.json")
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "shared"))
import boto3
from botocore.exceptions import ClientError
s3 = boto3.client("s3", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"
CANDS = ["data/news-sentiment.json", "data/news-velocity.json",
         "data/gdelt-sentiment.json", "data/tiingo-news.json"]
def check(k):
    try:
        r = s3.head_object(Bucket=BUCKET, Key=k)
        age = (datetime.now(timezone.utc) - r["LastModified"]).total_seconds()/3600
        return {"exists": True, "age_h": round(age,1), "size": r.get("ContentLength",0)}
    except ClientError:
        return {"exists": False}
    except Exception as e:
        return {"error": str(e)[:60]}
def main():
    rep = {"read_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")}
    for k in CANDS:
        rep[k] = check(k)
    os.makedirs(os.path.dirname(OUT), exist_ok=True)
    open(OUT,"w").write(json.dumps(rep,indent=2))
    print(json.dumps({k:v.get("exists") for k,v in rep.items() if k.startswith("data/")}))
if __name__=="__main__":
    main()
