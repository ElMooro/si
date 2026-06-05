"""1283 — audit vintage-fred output + portfolio data + track-record."""
import json, boto3
from botocore.config import Config
s3=boto3.client("s3",region_name="us-east-1",config=Config(read_timeout=60))
lam=boto3.client("lambda",region_name="us-east-1",config=Config(read_timeout=60))
out={}
# vintage-fred deployed?
try:
    c=lam.get_function_configuration(FunctionName="justhodl-vintage-fred")
    out["vintage_deployed"]=True; out["vintage_last"]=c.get("LastModified")
except Exception as e: out["vintage_deployed"]=str(e)[:60]
for k in ["data/vintage/_index.json","data/track-record.json"]:
    try:
        d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key=k)["Body"].read())
        out[k]={"exists":True,"keys":list(d.keys())[:8]}
    except Exception as e: out[k]={"exists":False,"err":str(e)[:60]}
open("aws/ops/reports/1283_audit.json","w").write(json.dumps(out,indent=2,default=str)); print("done")
