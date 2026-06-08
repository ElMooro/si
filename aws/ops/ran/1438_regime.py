import json, os, time, zipfile, io
import boto3
from botocore.config import Config
cfg=Config(read_timeout=180,retries={"max_attempts":1})
lam=boto3.client("lambda",region_name="us-east-1",config=cfg)
s3=boto3.client("s3",region_name="us-east-1",config=cfg)
out={}
try:
    r=lam.invoke(FunctionName="justhodl-brain-sync",InvocationType="RequestResponse",Payload=b"{}")
    out["run"]=r["Payload"].read().decode()[:100]
except Exception as e: out["run_err"]=str(e)[:100]
time.sleep(8)
try:
    b=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/brain.json")["Body"].read())
    out["n_notes"]=b.get("n_notes")
    out["regime_read"]=b.get("regime_read")
    d=b.get("directive"); out["profile"]=(d.get("investor_profile") if d else None); out["themes"]=(d.get("themes") if d else None); out["hard_rules"]=(d.get("hard_rules") if d else None)
except Exception as e: out["mirror_err"]=str(e)[:100]
open("aws/ops/reports/1438_r.json","w").write(json.dumps(out,indent=2,default=str)); print("done")
