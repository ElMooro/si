import json, os, time, zipfile, io, boto3
from botocore.config import Config
cfg=Config(read_timeout=180,retries={"max_attempts":1})
lam=boto3.client("lambda",region_name="us-east-1",config=cfg); s3=boto3.client("s3",region_name="us-east-1",config=cfg)
out={}
buf=io.BytesIO(); src="aws/lambdas/justhodl-brain-sync/source"
with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as zf:
    for r,_,fs in os.walk(src):
        for f in fs:
            if "__pycache__" in r or f.endswith(".pyc"): continue
            zf.write(os.path.join(r,f),arcname=os.path.relpath(os.path.join(r,f),src))
lam.update_function_code(FunctionName="justhodl-brain-sync",ZipFile=buf.getvalue())
for _ in range(25):
    time.sleep(2); c=lam.get_function_configuration(FunctionName="justhodl-brain-sync")
    if c.get("LastUpdateStatus") in ("Successful",None): break
r=lam.invoke(FunctionName="justhodl-brain-sync",InvocationType="RequestResponse",Payload=b"{}"); out["run"]=r["Payload"].read().decode()[:70]
time.sleep(8)
b=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/brain.json")["Body"].read())
rr=b.get("regime_read") or {}
out["regime"]=rr.get("regime"); out["headline"]=rr.get("headline")
out["assessment"]=(rr.get("assessment") or "")[:400]
out["invest_in"]=rr.get("invest_in"); out["avoid"]=rr.get("avoid")
open("aws/ops/reports/1501_r.json","w").write(json.dumps(out,indent=2,default=str)); print("done")
