import json, os, time, zipfile, io
import boto3
from botocore.config import Config
cfg=Config(read_timeout=120,retries={"max_attempts":1})
lam=boto3.client("lambda",region_name="us-east-1",config=cfg); s3=boto3.client("s3",region_name="us-east-1",config=cfg)
out={}
buf=io.BytesIO(); src="aws/lambdas/justhodl-crypto-cycle-risk/source"
with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as zf:
    for r,_,fs in os.walk(src):
        for f in fs:
            if "__pycache__" in r or f.endswith(".pyc"): continue
            fp=os.path.join(r,f); zf.write(fp,arcname=os.path.relpath(fp,src))
lam.update_function_code(FunctionName="justhodl-crypto-cycle-risk",ZipFile=buf.getvalue())
for _ in range(25):
    time.sleep(2); c=lam.get_function_configuration(FunctionName="justhodl-crypto-cycle-risk")
    if c.get("LastUpdateStatus") in ("Successful",None): break
lam.invoke(FunctionName="justhodl-crypto-cycle-risk",InvocationType="RequestResponse",Payload=b"{}"); time.sleep(2)
d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/crypto-cycle-risk.json")["Body"].read())
out["score"]=d.get("dump_risk_score"); out["level"]=d.get("risk_level")
fac=d.get("factors",{})
out["mvrv"]=fac.get("mvrv_extension",{}).get("mvrv")
out["fear_greed"]=fac.get("fear_greed",{}).get("value")
out["funding"]=fac.get("funding_leverage",{}).get("avg_funding_pct")
mr=fac.get("macro_risk",{})
out["macro_risk_score"]=mr.get("risk")
out["macro_components"]={k:v.get("note") for k,v in (mr.get("components") or {}).items()}
open("aws/ops/reports/1323_cr.json","w").write(json.dumps(out,indent=2,default=str)); print("done")
