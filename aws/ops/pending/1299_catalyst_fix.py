"""1299 — redeploy catalyst-calendar (restored+FDA+gov) + best-setups; verify."""
import json, os, time, zipfile, io
from datetime import datetime, timezone
import boto3
from botocore.config import Config
cfg=Config(read_timeout=180,retries={"max_attempts":1})
lam=boto3.client("lambda",region_name="us-east-1",config=cfg); s3=boto3.client("s3",region_name="us-east-1",config=cfg)
out={}
def zipdir(src):
    buf=io.BytesIO()
    with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as zf:
        for r,_,fs in os.walk(src):
            for f in fs:
                if "__pycache__" in r or f.endswith(".pyc"): continue
                fp=os.path.join(r,f); zf.write(fp,arcname=os.path.relpath(fp,src))
    return buf.getvalue()
def redeploy(n,src):
    lam.update_function_code(FunctionName=n,ZipFile=zipdir(src))
    for _ in range(25):
        time.sleep(2); c=lam.get_function_configuration(FunctionName=n)
        if c.get("LastUpdateStatus") in ("Successful",None): break
# ensure FMP_KEY env on catalyst
try:
    c=lam.get_function_configuration(FunctionName="justhodl-catalyst-calendar")
    env=c.get("Environment",{}).get("Variables",{})
    if not env.get("FMP_KEY"):
        env["FMP_KEY"]="wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb"
        lam.update_function_configuration(FunctionName="justhodl-catalyst-calendar",Environment={"Variables":env}); time.sleep(5)
except Exception as e: out["env_err"]=str(e)[:100]
redeploy("justhodl-catalyst-calendar","aws/lambdas/justhodl-catalyst-calendar/source")
redeploy("justhodl-best-setups","aws/lambdas/justhodl-best-setups/source")
try:
    r=lam.invoke(FunctionName="justhodl-catalyst-calendar",InvocationType="RequestResponse",Payload=b"{}"); time.sleep(2)
    cc=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/catalyst-calendar.json")["Body"].read())
    bytype={}
    for e in cc.get("events",[]): bytype[e.get("type")]=bytype.get(e.get("type"),0)+1
    out["catalyst"]={"n_events":cc.get("n_events"),"by_type":bytype,
        "fda_gov_sample":[{"t":e.get("ticker"),"ty":e.get("type"),"d":e.get("date"),"ti":(e.get("title") or "")[:50]} for e in cc.get("events",[]) if e.get("type") in ("FDA","GOV_CONTRACT")][:5]}
except Exception as e: out["catalyst"]=str(e)[:150]
try:
    r=lam.invoke(FunctionName="justhodl-best-setups",InvocationType="RequestResponse",Payload=b"{}"); time.sleep(2)
    bs=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/best-setups.json")["Body"].read())
    keys=set()
    for s in bs.get("top_setups",[]):
        for k in (s.get("signal_keys") or []): keys.add(k)
    out["best_setups_signals"]=sorted(keys)
except Exception as e: out["best_setups"]=str(e)[:150]
open("aws/ops/reports/1299_catalyst.json","w").write(json.dumps(out,indent=2,default=str)); print("done")
