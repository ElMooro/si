"""1298 — deploy catalyst-calendar + finra-short + best-setups; verify all 4."""
import json, os, time, zipfile, io
from datetime import datetime, timezone
import boto3
from botocore.config import Config
REPORT="aws/ops/reports/1298_early.json"; BUCKET="justhodl-dashboard-live"; REGION="us-east-1"
ROLE="arn:aws:iam::857687956942:role/lambda-execution-role"; ACC="857687956942"
cfg=Config(read_timeout=180,retries={"max_attempts":1})
lam=boto3.client("lambda",region_name=REGION,config=cfg); s3=boto3.client("s3",region_name=REGION,config=cfg)
events=boto3.client("events",region_name=REGION,config=cfg)
out={}
def zipdir(src):
    buf=io.BytesIO()
    with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as zf:
        for r,_,fs in os.walk(src):
            for f in fs:
                if "__pycache__" in r or f.endswith(".pyc"): continue
                fp=os.path.join(r,f); zf.write(fp,arcname=os.path.relpath(fp,src))
    return buf.getvalue()
def deploy(name,src,timeout,sched=None,env=None):
    try:
        zb=zipdir(src)
        try: lam.get_function_configuration(FunctionName=name); lam.update_function_code(FunctionName=name,ZipFile=zb); act="updated"
        except lam.exceptions.ResourceNotFoundException:
            kw=dict(FunctionName=name,Runtime="python3.12",Role=ROLE,Handler="lambda_function.lambda_handler",Code={"ZipFile":zb},Timeout=timeout,MemorySize=512,Architectures=["x86_64"])
            if env: kw["Environment"]={"Variables":env}
            lam.create_function(**kw); act="created"
        for _ in range(30):
            time.sleep(2); c=lam.get_function_configuration(FunctionName=name)
            if c.get("State")=="Active" and c.get("LastUpdateStatus") in ("Successful",None): break
        if sched:
            rule=name+"-daily"
            events.put_rule(Name=rule,ScheduleExpression=sched,State="ENABLED")
            fn=lam.get_function(FunctionName=name); events.put_targets(Rule=rule,Targets=[{"Id":"1","Arn":fn["Configuration"]["FunctionArn"]}])
            try: lam.add_permission(FunctionName=name,StatementId=f"EB-{rule}",Action="lambda:InvokeFunction",Principal="events.amazonaws.com",SourceArn=f"arn:aws:events:{REGION}:{ACC}:rule/{rule}")
            except lam.exceptions.ResourceConflictException: pass
        return act
    except Exception as e: return "ERR: "+str(e)[:200]
out["catalyst_deploy"]=deploy("justhodl-catalyst-calendar","aws/lambdas/justhodl-catalyst-calendar/source",120,"cron(0 12 * * ? *)",{"FMP_KEY":"wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb"})
# finra-short: deploy if missing
try: lam.get_function_configuration(FunctionName="justhodl-finra-short"); out["finra_deploy"]="exists"
except lam.exceptions.ResourceNotFoundException:
    out["finra_deploy"]=deploy("justhodl-finra-short","aws/lambdas/justhodl-finra-short/source",300,"cron(30 21 * * ? *)",{"POLYGON_KEY":"zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d","FMP_KEY":"wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb"})
out["bestsetups_deploy"]=deploy("justhodl-best-setups","aws/lambdas/justhodl-best-setups/source",120)
# invoke catalyst + best-setups
try:
    r=lam.invoke(FunctionName="justhodl-catalyst-calendar",InvocationType="RequestResponse",Payload=b"{}"); time.sleep(2)
    cc=json.loads(s3.get_object(Bucket=BUCKET,Key="data/catalyst-calendar.json")["Body"].read())
    out["catalyst"]={"stats":cc.get("stats"),"sample_gov":cc.get("gov_contracts_mapped",[])[:3],"sample_fda":cc.get("fda_upcoming",[])[:2]}
except Exception as e: out["catalyst"]=str(e)[:150]
try:
    r=lam.invoke(FunctionName="justhodl-best-setups",InvocationType="RequestResponse",Payload=b"{}"); time.sleep(2)
    bs=json.loads(s3.get_object(Bucket=BUCKET,Key="data/best-setups.json")["Body"].read())
    keys=set()
    for s in bs.get("top_setups",[]):
        for k in (s.get("signal_keys") or []): keys.add(k)
    s0=(bs.get("top_setups") or [{}])[0]
    out["best_setups"]={"regime":(bs.get("bond_vol_regime") or {}).get("regime"),"distinct_signals":sorted(keys),
        "top":{"t":s0.get("ticker"),"conv":s0.get("conviction"),"why":(s0.get("why") or "")[:140]}}
except Exception as e: out["best_setups"]=str(e)[:150]
open(REPORT,"w").write(json.dumps(out,indent=2,default=str)); print("done")
