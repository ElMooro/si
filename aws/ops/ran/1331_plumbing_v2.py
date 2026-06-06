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
out["n_signals"]=len(d.get("signals",{}))
out["new_signals"]={k:(d.get("signals",{}).get(k,{}).get("note") or "MISSING")[:90] for k in ["sofr_tail","tgcr_iorb","rate_band","reserves_share"]}
open("aws/ops/reports/1331_pl.json","w").write(json.dumps(out,indent=2,default=str)); print("done")
