"""ops 2810 — debug why EIA fetches fail (invoke /debug + read a raw series error)."""
import os, json
from datetime import datetime, timezone
import boto3
lam=boto3.client("lambda",region_name="us-east-1"); s3=boto3.client("s3",region_name="us-east-1")
R={"ops":2810,"ts":datetime.now(timezone.utc).isoformat()}
try:
    inv=lam.invoke(FunctionName="eia-energy-agent",InvocationType="RequestResponse",
        Payload=json.dumps({"rawPath":"/debug"}).encode())
    R["debug"]=json.loads(json.loads(inv["Payload"].read())["body"])
except Exception as e:
    R["debug_err"]=repr(e)[:200]
# also read a couple raw errors from the feed
try:
    d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/eia-energy.json")["Body"].read())
    al=d.get("all_series") or {}
    R["sample_errors"]={k:al[k].get("error") for k in list(al)[:3]}
except Exception as e:
    R["feed_err"]=repr(e)[:120]
print(json.dumps(R,indent=1,default=str))
os.makedirs("aws/ops/reports",exist_ok=True)
json.dump(R,open("aws/ops/reports/2810_eia_debug.json","w"),indent=1,default=str)
print("OPS 2810 COMPLETE")
