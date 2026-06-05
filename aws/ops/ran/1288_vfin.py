import json, time, boto3
from datetime import datetime, timezone
from botocore.config import Config
cfg=Config(read_timeout=300,retries={"max_attempts":1})
lam=boto3.client("lambda",region_name="us-east-1",config=cfg); s3=boto3.client("s3",region_name="us-east-1",config=cfg)
out={}
try:
    r=lam.invoke(FunctionName="justhodl-vintage-fred",InvocationType="RequestResponse",Payload=b"{}")
    out["invoke"]=r.get("Payload").read().decode()[:160]
except Exception as e: out["invoke"]=str(e)[:120]
time.sleep(2)
try:
    idx=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/vintage/_index.json")["Body"].read())
    out["n_series"]=idx.get("n_series")
    if idx.get("series"):
        s0=idx["series"][0]; v=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key=f"data/vintage/{s0}.json")["Body"].read())
        vs=v.get("vintages",[]); out["sample"]={"series":s0,"n":len(vs),"latest":vs[-1] if vs else None}
except Exception as e: out["err"]=str(e)[:120]
open("aws/ops/reports/1288_vfin.json","w").write(json.dumps(out,indent=2,default=str)); print("done")
