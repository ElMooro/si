import json, os, time, zipfile, io
import boto3
from botocore.config import Config
cfg=Config(read_timeout=300,retries={"max_attempts":1})
lam=boto3.client("lambda",region_name="us-east-1",config=cfg)
s3=boto3.client("s3",region_name="us-east-1",config=cfg)
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
try:
    r=lam.invoke(FunctionName="justhodl-brain-sync",InvocationType="RequestResponse",Payload=b"{}")
    out["run"]=r["Payload"].read().decode()[:100]
except Exception as e: out["run_err"]=str(e)[:100]
time.sleep(4)
try:
    b=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/brain.json")["Body"].read())
    out["n_notes_seen"]=b.get("n_notes")
    rr=b.get("regime_read")
    if rr: out["regime_read"]={"regime":rr.get("regime"),"headline":rr.get("headline"),"risk_assets":(rr.get("risk_assets") or "")[:200],"invest_in":rr.get("invest_in"),"avoid":rr.get("avoid")}
    else: out["regime_read"]=None
    d=b.get("directive")
    out["directive_profile"]=(d.get("investor_profile") if d else None)
except Exception as e: out["mirror_err"]=str(e)[:100]
open("aws/ops/reports/1420_r.json","w").write(json.dumps(out,indent=2,default=str)); print("done")
