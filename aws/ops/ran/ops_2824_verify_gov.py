"""ops 2824 — invoke + verify the 3 fixed gov-data agents' S3 outputs."""
import os, json, time
from datetime import datetime, timezone
import boto3
from botocore.config import Config
REGION="us-east-1"
lam=boto3.client("lambda",region_name=REGION,config=Config(read_timeout=120,retries={"max_attempts":0}))
s3=boto3.client("s3",region_name=REGION)
R={"ops":2824,"ts":datetime.now(timezone.utc).isoformat(),"agents":{}}
M={"bls-labor-agent":"data/bls-labor.json","bea-economic-agent":"data/bea-economic.json","census-economic-agent":"data/census-economic.json"}
for fn,key in M.items():
    a={}
    try:
        lam.invoke(FunctionName=fn,InvocationType="RequestResponse")["Payload"].read(); time.sleep(2)
        d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key=key)["Body"].read())
        a["series_live"]=d.get("_series_live") if d.get("_series_live") is not None else d.get("_blocks_live")
        a["summary"]=d.get("summary") or {k:d.get(k) for k in ("gdp","pce_inflation","income")}
        a["api_version"]=d.get("api_version"); a["key_valid"]=d.get("key_valid")
        a["error"]=d.get("_error")
        a["status"]="LIVE" if (a["series_live"] or 0)>=1 else "0 series"
    except Exception as e:
        a["status"]="ERR"; a["error"]=repr(e)[:160]
    R["agents"][fn]=a
ok=sum(1 for a in R["agents"].values() if a.get("status")=="LIVE")
R["status"]="%d/3 LIVE"%ok
print(json.dumps(R,indent=1,default=str))
os.makedirs("aws/ops/reports",exist_ok=True)
json.dump(R,open("aws/ops/reports/2824_verify_gov.json","w"),indent=1,default=str)
print("OPS 2824 COMPLETE")
