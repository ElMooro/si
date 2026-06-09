import json, os, time, zipfile, io, boto3
from botocore.config import Config
from datetime import datetime, timezone, timedelta
cfg=Config(read_timeout=180,retries={"max_attempts":1})
lam=boto3.client("lambda",region_name="us-east-1",config=cfg); cw=boto3.client("cloudwatch",region_name="us-east-1",config=cfg)
out={}
buf=io.BytesIO(); src="aws/lambdas/justhodl-calibrator/source"
with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as zf:
    for r,_,fs in os.walk(src):
        for f in fs:
            if "__pycache__" in r or f.endswith(".pyc"): continue
            zf.write(os.path.join(r,f),arcname=os.path.relpath(os.path.join(r,f),src))
lam.update_function_code(FunctionName="justhodl-calibrator",ZipFile=buf.getvalue())
for _ in range(25):
    time.sleep(2); c=lam.get_function_configuration(FunctionName="justhodl-calibrator")
    if c.get("LastUpdateStatus") in ("Successful",None): break
# run 3× to confirm consistent success
runs=[]
for i in range(3):
    r=lam.invoke(FunctionName="justhodl-calibrator",InvocationType="RequestResponse",Payload=b"{}")
    runs.append({"function_error":r.get("FunctionError","NONE"),"body":r["Payload"].read().decode()[:90]})
    time.sleep(2)
out["runs"]=runs
open("aws/ops/reports/1520_cf.json","w").write(json.dumps(out,indent=2,default=str)); print("done")
