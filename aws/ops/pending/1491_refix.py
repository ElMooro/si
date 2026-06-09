import json, os, time, zipfile, io
import boto3
from botocore.config import Config
cfg=Config(read_timeout=300,retries={"max_attempts":1})
lam=boto3.client("lambda",region_name="us-east-1",config=cfg)
s3=boto3.client("s3",region_name="us-east-1",config=cfg)
out={}
buf=io.BytesIO(); src="aws/lambdas/justhodl-ecb-history/source"
with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as zf:
    for r,_,fs in os.walk(src):
        for f in fs:
            if "__pycache__" in r or f.endswith(".pyc"): continue
            zf.write(os.path.join(r,f),arcname=os.path.relpath(os.path.join(r,f),src))
lam.update_function_code(FunctionName="justhodl-ecb-history",ZipFile=buf.getvalue())
for _ in range(25):
    time.sleep(2); c=lam.get_function_configuration(FunctionName="justhodl-ecb-history")
    if c.get("LastUpdateStatus") in ("Successful",None): break
r=lam.invoke(FunctionName="justhodl-ecb-history",InvocationType="RequestResponse",Payload=b"{}")
out["run"]=r["Payload"].read().decode()[:80]
time.sleep(3)
m=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/ecb-hist/_manifest.json")["Body"].read())
out["ciss_ea_latest"]=next((s["latest"] for s in m["series"] if s["id"]=="ciss_ea"),None)
out["all_latest"]={s["id"]:s["latest"] for s in m["series"]}
open("aws/ops/reports/1491_r.json","w").write(json.dumps(out,indent=2,default=str)); print("done")
