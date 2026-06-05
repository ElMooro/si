"""1283 — status of vintage-fred + check for portfolio-aware / nlq / explain gaps."""
import json, boto3
from botocore.config import Config
cfg=Config(read_timeout=60,retries={"max_attempts":1})
lam=boto3.client("lambda",region_name="us-east-1",config=cfg); s3=boto3.client("s3",region_name="us-east-1",config=cfg)
out={}
try:
    c=lam.get_function_configuration(FunctionName="justhodl-vintage-fred")
    out["vintage_fred"]={"exists":True,"last":c.get("LastModified"),"timeout":c.get("Timeout")}
except Exception as e: out["vintage_fred"]={"exists":False,"err":str(e)[:80]}
# is vintage output on S3?
try:
    idx=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/vintage/_index.json")["Body"].read())
    out["vintage_index"]={"series":len(idx) if isinstance(idx,(list,dict)) else "?","sample":(list(idx)[:5] if isinstance(idx,dict) else idx[:5] if isinstance(idx,list) else None)}
except Exception as e: out["vintage_index"]={"err":str(e)[:80]}
# does a user-portfolio store exist?
for k in ["data/user-portfolios.json","data/portfolio-aware.json","data/explainability.json","data/track-record-live.json"]:
    try: s3.head_object(Bucket="justhodl-dashboard-live",Key=k); out[k]="exists"
    except Exception: out[k]="missing"
open("aws/ops/reports/1283_status.json","w").write(json.dumps(out,indent=2,default=str)); print("done")
