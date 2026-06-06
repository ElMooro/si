import json, os, time, zipfile, io
import boto3
from botocore.config import Config
cfg=Config(read_timeout=120,retries={"max_attempts":1})
lam=boto3.client("lambda",region_name="us-east-1",config=cfg); s3=boto3.client("s3",region_name="us-east-1",config=cfg)
out={}
buf=io.BytesIO(); src="aws/lambdas/justhodl-best-setups/source"
with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as zf:
    for r,_,fs in os.walk(src):
        for f in fs:
            if "__pycache__" in r or f.endswith(".pyc"): continue
            fp=os.path.join(r,f); zf.write(fp,arcname=os.path.relpath(fp,src))
lam.update_function_code(FunctionName="justhodl-best-setups",ZipFile=buf.getvalue())
for _ in range(25):
    time.sleep(2); c=lam.get_function_configuration(FunctionName="justhodl-best-setups")
    if c.get("LastUpdateStatus") in ("Successful",None): break
lam.invoke(FunctionName="justhodl-best-setups",InvocationType="RequestResponse",Payload=b"{}"); time.sleep(2)
bs=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/best-setups.json")["Body"].read())
keys=set()
for s in bs.get("top_setups",[]):
    for k in (s.get("signal_keys") or []): keys.add(k)
out["board_signals"]=sorted(keys)
out["has_buyback"]="BUYBACK" in keys; out["has_capex"]="CAPEX_ACCEL" in keys
out["n_buildout"]=len(bs.get("buildout_threats",[]))
out["n_triple"]=len(bs.get("triple_threats",[]))
out["buildout_sample"]=[{"t":s["ticker"],"conv":s.get("conviction"),"why":(s.get("why") or "")[:120]} for s in bs.get("buildout_threats",[])[:4]]
# any setup carrying buyback or capex
bbcx=[s for s in bs.get("top_setups",[]) if set(s.get("signal_keys") or []) & {"BUYBACK","CAPEX_ACCEL"}]
out["sample_with_bbcx"]=[{"t":s["ticker"],"keys":[k for k in (s.get("signal_keys") or []) if k in ("BUYBACK","CAPEX_ACCEL")],"verdict":s.get("verdict")} for s in bbcx[:6]]
open("aws/ops/reports/1329_bo.json","w").write(json.dumps(out,indent=2,default=str)); print("done")
