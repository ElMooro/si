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
for _ in range(30):
    time.sleep(2); c=lam.get_function_configuration(FunctionName="justhodl-crypto-cycle-risk")
    if c.get("LastUpdateStatus") in ("Successful",None): break
time.sleep(3)
r=lam.invoke(FunctionName="justhodl-crypto-cycle-risk",InvocationType="RequestResponse",Payload=b"{}")
out["invoke"]=r.get("Payload").read().decode()[:120]; time.sleep(2)
d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/crypto-cycle-risk.json")["Body"].read())
out["score"]=d.get("dump_risk_score"); out["level"]=d.get("risk_level")
fac=d.get("factors",{}); out["keys"]=list(fac.keys())
for k in ["inflation_print","etf_flows","ai_rotation"]: out[k]={"risk":fac.get(k,{}).get("risk"),"note":(fac.get(k,{}).get("note") or "")[:110]}
out["drivers"]=[x["note"][:90] for x in d.get("top_drivers",[])]
open("aws/ops/reports/1326_dr.json","w").write(json.dumps(out,indent=2,default=str)); print("done")
