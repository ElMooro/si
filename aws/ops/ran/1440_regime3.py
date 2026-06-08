import json, os, time, zipfile, io
import boto3
from botocore.config import Config
cfg=Config(read_timeout=180,retries={"max_attempts":1})
lam=boto3.client("lambda",region_name="us-east-1",config=cfg)
s3=boto3.client("s3",region_name="us-east-1",config=cfg)
out={}
# confirm key set
try:
    env=(lam.get_function_configuration(FunctionName="justhodl-brain-sync").get("Environment",{}).get("Variables",{}) or {})
    out["has_anthropic_key"]=bool(env.get("ANTHROPIC_KEY"))
except Exception as e: out["cfg_err"]=str(e)[:80]
# redeploy
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
# if no key, set it from morning-intel
if not out.get("has_anthropic_key"):
    try:
        mk=(lam.get_function_configuration(FunctionName="justhodl-morning-intelligence").get("Environment",{}).get("Variables",{}) or {}).get("ANTHROPIC_KEY","")
        if mk:
            env["ANTHROPIC_KEY"]=mk; lam.update_function_configuration(FunctionName="justhodl-brain-sync",Environment={"Variables":env}); time.sleep(6); out["key_set_from_morning"]=True
    except Exception as e: out["key_set_err"]=str(e)[:80]
try:
    r=lam.invoke(FunctionName="justhodl-brain-sync",InvocationType="RequestResponse",Payload=b"{}")
    out["run"]=r["Payload"].read().decode()[:90]
except Exception as e: out["run_err"]=str(e)[:90]
time.sleep(8)
try:
    b=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/brain.json")["Body"].read())
    out["regime_read"]=b.get("regime_read")
except Exception as e: out["mirror_err"]=str(e)[:90]
open("aws/ops/reports/1440_r.json","w").write(json.dumps(out,indent=2,default=str)); print("done")
