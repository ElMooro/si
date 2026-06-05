import json, boto3
from botocore.config import Config
cfg=Config(read_timeout=60,retries={"max_attempts":1})
lam=boto3.client("lambda",region_name="us-east-1",config=cfg); s3=boto3.client("s3",region_name="us-east-1",config=cfg)
out={}
try:
    c=lam.get_function_configuration(FunctionName="justhodl-vintage-fred")
    out["vintage_deployed"]=True; out["vintage_last"]=c.get("LastModified")
except Exception as e: out["vintage_deployed"]=False
try:
    d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/vintage/_index.json")["Body"].read())
    out["vintage_data"]={"n_series":d.get("n_series"),"updated":d.get("updated")}
except Exception as e: out["vintage_data"]="none ("+str(e)[:50]+")"
# portfolio data?
for k in ["data/portfolio-analysis.json","data/explainability.json","data/track-record-public.json"]:
    try: s3.head_object(Bucket="justhodl-dashboard-live",Key=k); out[k]="EXISTS"
    except Exception: out[k]="missing"
print(json.dumps(out,indent=2,default=str))
open("aws/ops/reports/1283.txt","w").write("done")
