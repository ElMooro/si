import json, os, time, zipfile, io, boto3
from botocore.config import Config
cfg=Config(read_timeout=180,retries={"max_attempts":1})
lam=boto3.client("lambda",region_name="us-east-1",config=cfg); s3=boto3.client("s3",region_name="us-east-1",config=cfg)
out={}
buf=io.BytesIO(); src="aws/lambdas/justhodl-ecb-derived/source"
with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as zf:
    for r,_,fs in os.walk(src):
        for f in fs:
            if "__pycache__" in r or f.endswith(".pyc"): continue
            zf.write(os.path.join(r,f),arcname=os.path.relpath(os.path.join(r,f),src))
lam.update_function_code(FunctionName="justhodl-ecb-derived",ZipFile=buf.getvalue())
for _ in range(25):
    time.sleep(2); c=lam.get_function_configuration(FunctionName="justhodl-ecb-derived")
    if c.get("LastUpdateStatus") in ("Successful",None): break
lam.invoke(FunctionName="justhodl-ecb-derived",InvocationType="RequestResponse",Payload=b"{}")
time.sleep(4)
d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/ecb-derived.json")["Body"].read())
t2=d["indicators"].get("target2_imbalance",{})
out["target2"]={k:t2.get(k) for k in ['de_target2_eur_mn','it_target2_eur_mn','de_3m_change_eur_mn','de_minus_it_eur_mn','signal','err']}
out["all_indicators"]=list(d["indicators"].keys())
out["n_indicators"]=len(d["indicators"])
open("aws/ops/reports/1516_t2.json","w").write(json.dumps(out,indent=2,default=str)); print("done")
