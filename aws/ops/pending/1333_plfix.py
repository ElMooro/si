import json, os, time, zipfile, io
import boto3
from botocore.config import Config
cfg=Config(read_timeout=120,retries={"max_attempts":1})
lam=boto3.client("lambda",region_name="us-east-1",config=cfg); s3=boto3.client("s3",region_name="us-east-1",config=cfg)
out={}
buf=io.BytesIO(); src="aws/lambdas/justhodl-funding-plumbing/source"
with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as zf:
    for r,_,fs in os.walk(src):
        for f in fs:
            if "__pycache__" in r or f.endswith(".pyc"): continue
            fp=os.path.join(r,f); zf.write(fp,arcname=os.path.relpath(fp,src))
lam.update_function_code(FunctionName="justhodl-funding-plumbing",ZipFile=buf.getvalue())
for _ in range(25):
    time.sleep(2); c=lam.get_function_configuration(FunctionName="justhodl-funding-plumbing")
    if c.get("LastUpdateStatus") in ("Successful",None): break
lam.invoke(FunctionName="justhodl-funding-plumbing",InvocationType="RequestResponse",Payload=b"{}"); time.sleep(2)
d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/funding-plumbing.json")["Body"].read())
out["regime"]=d.get("regime"); out["score"]=d.get("plumbing_stress_score"); out["bs"]=d.get("balance_sheet_direction")
sigs=d.get("signals",{})
out["has_tgcr"]="tgcr_iorb" in sigs
out["reserves_share"]=sigs.get("reserves_share",{}).get("note")
out["all_signals"]=list(sigs.keys())
# also confirm bond-vol carries funding_plumbing for the ribbon
bv=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/bond-vol.json")["Body"].read())
out["bondvol_has_plumbing"]=bool(bv.get("funding_plumbing"))
out["bondvol_plumbing"]=bv.get("funding_plumbing")
open("aws/ops/reports/1333_pl.json","w").write(json.dumps(out,indent=2,default=str)); print("done")
