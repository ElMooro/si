import json, os, time, zipfile, io, boto3
from botocore.config import Config
cfg=Config(read_timeout=180,retries={"max_attempts":1})
lam=boto3.client("lambda",region_name="us-east-1",config=cfg); s3=boto3.client("s3",region_name="us-east-1",config=cfg)
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
# run it — should now succeed
r=lam.invoke(FunctionName="justhodl-calibrator",InvocationType="RequestResponse",Payload=b"{}")
out["function_error"]=r.get("FunctionError","NONE")
out["response"]=r["Payload"].read().decode()[:200]
time.sleep(3)
# confirm S3 mirror written
try:
    o=s3.get_object(Bucket="justhodl-dashboard-live",Key="calibration/weights.json")
    d=json.loads(o["Body"].read())
    out["s3_weights_written"]={"n_weights":len(d.get("weights",{})),"generated_at":d.get("generated_at")}
except Exception as e: out["s3_err"]=str(e)[:50]
open("aws/ops/reports/1519_cf.json","w").write(json.dumps(out,indent=2,default=str)); print("done")
