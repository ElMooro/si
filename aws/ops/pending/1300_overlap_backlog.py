"""1300 — deploy backlog + overlap engines, invoke, verify."""
import json, os, time, zipfile, io
from datetime import datetime, timezone
import boto3
from botocore.config import Config
REPORT="aws/ops/reports/1300_ovbl.json"; BUCKET="justhodl-dashboard-live"; REGION="us-east-1"
ROLE="arn:aws:iam::857687956942:role/lambda-execution-role"; ACC="857687956942"
cfg=Config(read_timeout=320,retries={"max_attempts":1})
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
def deploy(name,src,timeout,sched,env=None):
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
        rule=name+"-daily"; events.put_rule(Name=rule,ScheduleExpression=sched,State="ENABLED")
        fn=lam.get_function(FunctionName=name); events.put_targets(Rule=rule,Targets=[{"Id":"1","Arn":fn["Configuration"]["FunctionArn"]}])
        try: lam.add_permission(FunctionName=name,StatementId=f"EB-{rule}",Action="lambda:InvokeFunction",Principal="events.amazonaws.com",SourceArn=f"arn:aws:events:{REGION}:{ACC}:rule/{rule}")
        except lam.exceptions.ResourceConflictException: pass
        return act
    except Exception as e: return "ERR:"+str(e)[:160]
out["backlog_deploy"]=deploy("justhodl-backlog","aws/lambdas/justhodl-backlog/source",300,"cron(30 11 * * ? *)",{"FMP_KEY":"wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb"})
out["overlap_deploy"]=deploy("justhodl-deep-value-overlap","aws/lambdas/justhodl-deep-value-overlap/source",120,"cron(0 17 * * ? *)")
# invoke backlog
try:
    r=lam.invoke(FunctionName="justhodl-backlog",InvocationType="RequestResponse",Payload=b"{}"); time.sleep(2)
    bl=json.loads(s3.get_object(Bucket=BUCKET,Key="data/backlog.json")["Body"].read())
    out["backlog"]={"covered":bl.get("n_covered"),"accel":len(bl.get("accelerating",[])),
        "sample":[{"t":r["ticker"],"rpo_yoy":r.get("rpo_yoy"),"rev_yoy":r.get("rev_yoy"),"ev_rpo":r.get("ev_to_rpo"),"accel":r.get("demand_accelerating")} for r in bl.get("accelerating",[])[:5]]}
except Exception as e: out["backlog"]=str(e)[:160]
# invoke overlap (after backlog so it can use it)
try:
    r=lam.invoke(FunctionName="justhodl-deep-value-overlap",InvocationType="RequestResponse",Payload=b"{}"); time.sleep(2)
    ov=json.loads(s3.get_object(Bucket=BUCKET,Key="data/deep-value-overlap.json")["Body"].read())
    out["overlap"]={"scored":ov.get("n_scored"),"prime":len(ov.get("prime_setups",[])),
        "top":[{"t":r["ticker"],"score":r["overlap_score"],"lenses":r["n_value_lenses"],"cats":r["n_catalysts"],"why":(r.get("value_lenses",[])+r.get("catalysts",[]))[:4]} for r in ov.get("prime_setups",[])[:6]]}
except Exception as e: out["overlap"]=str(e)[:160]
open(REPORT,"w").write(json.dumps(out,indent=2,default=str)); print("done")
