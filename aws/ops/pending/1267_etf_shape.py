"""1267 — inspect etf-flows shape."""
import json, boto3
from botocore.config import Config
s3=boto3.client("s3",region_name="us-east-1",config=Config(read_timeout=60))
for k in ["data/etf-flows.json","data/etf-fund-flows.json"]:
    try:
        d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key=k)["Body"].read())
        print(f"\n=== {k} ===")
        print("top keys:", list(d.keys())[:12] if isinstance(d,dict) else "LIST len "+str(len(d)))
        # find the array of etfs
        for key in (d.keys() if isinstance(d,dict) else []):
            v=d[key]
            if isinstance(v,list) and v and isinstance(v[0],dict):
                print(f"  array '{key}' [{len(v)}] sample fields:", list(v[0].keys())[:12])
            elif isinstance(v,dict) and v:
                fk=list(v.keys())[0]
                if isinstance(v[fk],dict): print(f"  dict '{key}' sample item fields:", list(v[fk].keys())[:12])
    except Exception as e: print(f"{k}: {str(e)[:80]}")
open("aws/ops/reports/1267_etf_shape.txt","w").write("done")
