import json, os, time, zipfile, io
import boto3
from botocore.config import Config
cfg=Config(read_timeout=120,retries={"max_attempts":1})
lam=boto3.client("lambda",region_name="us-east-1",config=cfg); s3=boto3.client("s3",region_name="us-east-1",config=cfg)
ssm=boto3.client("ssm",region_name="us-east-1",config=cfg)
out={}
def zd(src):
    buf=io.BytesIO()
    with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as zf:
        for r,_,fs in os.walk(src):
            for f in fs:
                if "__pycache__" in r or f.endswith(".pyc"): continue
                fp=os.path.join(r,f); zf.write(fp,arcname=os.path.relpath(fp,src))
    return buf.getvalue()
# deploy brain-sync + best-setups
for n,src in [("justhodl-brain-sync","aws/lambdas/justhodl-brain-sync/source"),("justhodl-best-setups","aws/lambdas/justhodl-best-setups/source"),("justhodl-ask","aws/lambdas/justhodl-ask/source")]:
    lam.update_function_code(FunctionName=n,ZipFile=zd(src))
    for _ in range(25):
        time.sleep(2); c=lam.get_function_configuration(FunctionName=n)
        if c.get("LastUpdateStatus") in ("Successful",None): break
# ensure brain-sync has ANTHROPIC_KEY — copy from morning-intelligence's env
try:
    mi=lam.get_function_configuration(FunctionName="justhodl-morning-intelligence")
    akey=(mi.get("Environment",{}).get("Variables",{}) or {}).get("ANTHROPIC_KEY","")
    if akey:
        bs=lam.get_function_configuration(FunctionName="justhodl-brain-sync")
        env=(bs.get("Environment",{}).get("Variables",{}) or {})
        env["ANTHROPIC_KEY"]=akey
        lam.update_function_configuration(FunctionName="justhodl-brain-sync",Environment={"Variables":env})
        out["env_set"]=True
        for _ in range(20):
            time.sleep(2); c=lam.get_function_configuration(FunctionName="justhodl-brain-sync")
            if c.get("LastUpdateStatus") in ("Successful",None): break
    else: out["env_set"]="no key found on morning-intel"
except Exception as e: out["env_set"]="ERR:"+str(e)[:100]
# seed a few brain notes (set PIN if needed) then run sync to test AI extraction
import urllib.request
try:
    # try to read current brain to see if pin set
    g=json.loads(urllib.request.urlopen("https://justhodl-data-proxy.raafouis.workers.dev/brain?t=1",timeout=15).read().decode())
    out["pin_set"]=g.get("pin_set"); out["existing_notes"]=len(g.get("notes",[]))
except Exception as e: out["brain_read"]=str(e)[:80]
# run brain-sync
lam.invoke(FunctionName="justhodl-brain-sync",InvocationType="RequestResponse",Payload=b"{}"); time.sleep(3)
b=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/brain.json")["Body"].read())
out["brain_version"]=b.get("version"); out["n_notes"]=b.get("n_notes")
out["has_directive"]=bool(b.get("directive"))
out["directive"]=b.get("directive")
open("aws/ops/reports/1342_sb.json","w").write(json.dumps(out,indent=2,default=str)); print("done")
