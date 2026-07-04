"""ops 2826 — final verify all 3 gov agents after fixes."""
import os, json, time
from datetime import datetime, timezone
import boto3
from botocore.config import Config
lam=boto3.client("lambda",region_name="us-east-1",config=Config(read_timeout=120,retries={"max_attempts":0}))
s3=boto3.client("s3",region_name="us-east-1")
R={"ops":2826,"ts":datetime.now(timezone.utc).isoformat(),"agents":{}}
M={"bls-labor-agent":"data/bls-labor.json","bea-economic-agent":"data/bea-economic.json","census-economic-agent":"data/census-economic.json"}
for fn,key in M.items():
    a={}
    try:
        lam.invoke(FunctionName=fn,InvocationType="RequestResponse")["Payload"].read(); time.sleep(2)
        d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key=key)["Body"].read())
        a["series_live"]=d.get("_series_live") if d.get("_series_live") is not None else d.get("_blocks_live")
        a["summary"]=d.get("summary") or {k:d.get(k) for k in ("gdp","pce_inflation","income")}
        a["err"]=d.get("_error") or d.get("_fetch_error")
        a["status"]="LIVE" if (a["series_live"] or 0)>=1 else "0"
    except Exception as e:
        a["status"]="ERR"; a["err"]=repr(e)[:150]
    R["agents"][fn]=a
R["status"]="%d/3 LIVE"%sum(1 for a in R["agents"].values() if a.get("status")=="LIVE")
print(json.dumps(R,indent=1,default=str))
os.makedirs("aws/ops/reports",exist_ok=True); json.dump(R,open("aws/ops/reports/2826_verify3.json","w"),indent=1,default=str)
print("OPS 2826 COMPLETE")
