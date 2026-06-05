"""1283 — verify vintage-fred is deployed + producing data (foundation)."""
import json, time, boto3
from datetime import datetime, timezone
from botocore.config import Config
cfg=Config(read_timeout=200,retries={"max_attempts":1})
lam=boto3.client("lambda",region_name="us-east-1",config=cfg); s3=boto3.client("s3",region_name="us-east-1",config=cfg)
out={}
try:
    c=lam.get_function_configuration(FunctionName="justhodl-vintage-fred")
    out["deployed"]=True; out["last_modified"]=c.get("LastModified")
except Exception as e: out["deployed"]=False; out["err"]=str(e)[:100]
# is there vintage data on S3?
try:
    idx=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/vintage/_index.json")["Body"].read())
    out["vintage_index"]={"n_series":len(idx) if isinstance(idx,(list,dict)) else "?","sample":list(idx)[:5] if isinstance(idx,dict) else idx[:5] if isinstance(idx,list) else idx}
except Exception as e: out["vintage_index_err"]=str(e)[:120]
# invoke if deployed to confirm it runs
if out.get("deployed"):
    try:
        r=lam.invoke(FunctionName="justhodl-vintage-fred",InvocationType="RequestResponse",Payload=b"{}")
        out["invoke"]=r.get("Payload").read().decode()[:200]
    except Exception as e: out["invoke"]=str(e)[:150]
open("aws/ops/reports/1283_foundation.json","w").write(json.dumps(out,indent=2,default=str)); print("done")
