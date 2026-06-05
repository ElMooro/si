import json, os, time, zipfile, io
import boto3
from botocore.config import Config
cfg=Config(read_timeout=650,retries={"max_attempts":1})
lam=boto3.client("lambda",region_name="us-east-1",config=cfg); s3=boto3.client("s3",region_name="us-east-1",config=cfg)
out={}
# 1) clear poisoned cache
try:
    s3.put_object(Bucket="justhodl-dashboard-live",Key="data/backlog-coverage-cache.json",
                  Body=json.dumps({"has_backlog":[],"no_backlog":[]}).encode(),ContentType="application/json")
    out["cache_reset"]=True
except Exception as e: out["cache_reset"]=str(e)[:80]
# 2) redeploy
buf=io.BytesIO(); src="aws/lambdas/justhodl-backlog/source"
with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as zf:
    for r,_,fs in os.walk(src):
        for f in fs:
            if "__pycache__" in r or f.endswith(".pyc"): continue
            fp=os.path.join(r,f); zf.write(fp,arcname=os.path.relpath(fp,src))
lam.update_function_code(FunctionName="justhodl-backlog",ZipFile=buf.getvalue())
for _ in range(30):
    time.sleep(2); c=lam.get_function_configuration(FunctionName="justhodl-backlog")
    if c.get("LastUpdateStatus") in ("Successful",None): break
# 3) single run (no double-invoke → no throttle)
try:
    t0=time.time(); r=lam.invoke(FunctionName="justhodl-backlog",InvocationType="RequestResponse",Payload=b"{}")
    out["invoke"]={"elapsed":round(time.time()-t0,1),"body":r.get("Payload").read().decode()[:120]}
    time.sleep(2); bl=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/backlog.json")["Body"].read())
    out["covered"]=bl.get("n_covered"); out["caps"]=bl.get("cap_distribution"); out["accel"]=len(bl.get("accelerating",[]))
    out["top"]=[{"t":r["ticker"],"div":r.get("rpo_minus_rev_growth"),"cap":r.get("cap_bucket")} for r in bl.get("accelerating",[])[:6]]
except Exception as e: out["err"]=str(e)[:120]
open("aws/ops/reports/1304_reset.json","w").write(json.dumps(out,indent=2,default=str)); print("done")
