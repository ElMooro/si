"""1284 — deploy + schedule + invoke justhodl-vintage-fred (point-in-time foundation)."""
import json, os, time, zipfile, io
from datetime import datetime, timezone
import boto3
from botocore.config import Config
REPORT="aws/ops/reports/1284_vintage_deploy.json"; BUCKET="justhodl-dashboard-live"
LAMBDA="justhodl-vintage-fred"; SRC="aws/lambdas/justhodl-vintage-fred/source"
ROLE="arn:aws:iam::857687956942:role/lambda-execution-role"; ACC="857687956942"; REGION="us-east-1"
RULE="justhodl-vintage-fred-daily"; SCHED="cron(0 12 * * ? *)"
cfg=Config(read_timeout=420,retries={"max_attempts":1})
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
            Code={"ZipFile":zb},Description="Point-in-time vintage FRED (ALFRED)",Timeout=300,MemorySize=512,Architectures=["x86_64"],
            Environment={"Variables":{"FRED_KEY":"2f057499936072679d8843d7fce99989"}},Publish=False); act="created"
    for _ in range(30):
        time.sleep(2); c=lam.get_function_configuration(FunctionName=LAMBDA)
        if c.get("State")=="Active" and c.get("LastUpdateStatus") in ("Successful",None): break
    out["deploy"]=act
except Exception as e: out["deploy_err"]=str(e)[:300]
try:
    events.put_rule(Name=RULE,ScheduleExpression=SCHED,State="ENABLED",Description="Daily vintage FRED")
    fn=lam.get_function(FunctionName=LAMBDA); events.put_targets(Rule=RULE,Targets=[{"Id":"1","Arn":fn["Configuration"]["FunctionArn"]}])
    try: lam.add_permission(FunctionName=LAMBDA,StatementId=f"EB-{RULE}",Action="lambda:InvokeFunction",Principal="events.amazonaws.com",SourceArn=f"arn:aws:events:{REGION}:{ACC}:rule/{RULE}")
    except lam.exceptions.ResourceConflictException: pass
except Exception as e: out["sched_err"]=str(e)[:200]
try:
    r=lam.invoke(FunctionName=LAMBDA,InvocationType="RequestResponse",Payload=b"{}")
    out["invoke"]=r.get("Payload").read().decode()[:300]
except Exception as e: out["invoke"]=str(e)[:200]
time.sleep(2)
try:
    idx=json.loads(s3.get_object(Bucket=BUCKET,Key="data/vintage/_index.json")["Body"].read())
    out["vintage_index"]=idx if isinstance(idx,list) else list(idx) if isinstance(idx,dict) else str(idx)[:200]
except Exception as e: out["index_err"]=str(e)[:120]
out["finished"]=datetime.now(timezone.utc).isoformat()
open(REPORT,"w").write(json.dumps(out,indent=2,default=str)); print(json.dumps(out,default=str)[:600])
