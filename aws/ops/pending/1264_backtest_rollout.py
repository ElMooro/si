"""1264 — deploy + invoke signal-backtest; verify board banner build."""
import json, os, time, zipfile, io
from datetime import datetime, timezone
import boto3
from botocore.config import Config
REPORT="aws/ops/reports/1264_backtest.json"; BUCKET="justhodl-dashboard-live"
LAMBDA="justhodl-signal-backtest"; SRC="aws/lambdas/justhodl-signal-backtest/source"
ROLE="arn:aws:iam::857687956942:role/lambda-execution-role"; ACC="857687956942"; REGION="us-east-1"
RULE="justhodl-signal-backtest-daily"; SCHED="cron(0 16 * * ? *)"
cfg=Config(read_timeout=320,retries={"max_attempts":1})
lam=boto3.client("lambda",region_name=REGION,config=cfg); s3=boto3.client("s3",region_name=REGION,config=cfg)
events=boto3.client("events",region_name=REGION,config=cfg)
out={"started":datetime.now(timezone.utc).isoformat()}
def zipit():
    buf=io.BytesIO()
    with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as zf:
        for r,_,fs in os.walk(SRC):
            for f in fs:
                if f.endswith(".pyc") or "__pycache__" in r: continue
                fp=os.path.join(r,f); zf.write(fp,arcname=os.path.relpath(fp,SRC))
    return buf.getvalue()
try:
    zb=zipit()
    try: lam.get_function_configuration(FunctionName=LAMBDA); lam.update_function_code(FunctionName=LAMBDA,ZipFile=zb); act="updated"
    except lam.exceptions.ResourceNotFoundException:
        lam.create_function(FunctionName=LAMBDA,Runtime="python3.12",Role=ROLE,Handler="lambda_function.lambda_handler",
            Code={"ZipFile":zb},Description="Signal backtest",Timeout=300,MemorySize=512,Architectures=["x86_64"],
            Environment={"Variables":{"FMP_KEY":"wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb"}},Publish=False); act="created"
    for _ in range(30):
        time.sleep(2); c=lam.get_function_configuration(FunctionName=LAMBDA)
        if c.get("State")=="Active" and c.get("LastUpdateStatus") in ("Successful",None): break
    out["deploy"]=act
except Exception as e: out["deploy_err"]=str(e)[:300]
try:
    events.put_rule(Name=RULE,ScheduleExpression=SCHED,State="ENABLED",Description="Daily backtest")
    fn=lam.get_function(FunctionName=LAMBDA); events.put_targets(Rule=RULE,Targets=[{"Id":"1","Arn":fn["Configuration"]["FunctionArn"]}])
    try: lam.add_permission(FunctionName=LAMBDA,StatementId=f"EB-{RULE}",Action="lambda:InvokeFunction",Principal="events.amazonaws.com",SourceArn=f"arn:aws:events:{REGION}:{ACC}:rule/{RULE}")
    except lam.exceptions.ResourceConflictException: pass
except Exception as e: out["sched_err"]=str(e)[:200]
try:
    r=lam.invoke(FunctionName=LAMBDA,InvocationType="RequestResponse",Payload=b"{}")
    out["invoke"]=r.get("Payload").read().decode()[:400]
except Exception as e: out["invoke"]=str(e)[:200]
# count snapshots available
try:
    n=0; tok=None
    while True:
        kw={"Bucket":BUCKET,"Prefix":"data/track-record/snapshots/","MaxKeys":1000}
        if tok: kw["ContinuationToken"]=tok
        rr=s3.list_objects_v2(**kw); n+=len([o for o in rr.get("Contents",[]) if o["Key"].endswith(".json")]); tok=rr.get("NextContinuationToken")
        if not tok: break
    out["snapshots_available"]=n
except Exception as e: out["snap_err"]=str(e)[:100]
out["finished"]=datetime.now(timezone.utc).isoformat()
open(REPORT,"w").write(json.dumps(out,indent=2,default=str)); print(json.dumps(out,indent=2,default=str)[:800])
