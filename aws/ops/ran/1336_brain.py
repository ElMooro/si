import json, os, time, zipfile, io, urllib.request
import boto3
from botocore.config import Config
cfg=Config(read_timeout=120,retries={"max_attempts":1})
lam=boto3.client("lambda",region_name="us-east-1",config=cfg); s3=boto3.client("s3",region_name="us-east-1",config=cfg)
events=boto3.client("events",region_name="us-east-1",config=cfg)
ROLE="arn:aws:iam::857687956942:role/lambda-execution-role"; ACC="857687956942"
out={}
def zd(src):
    buf=io.BytesIO()
    with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as zf:
        for r,_,fs in os.walk(src):
            for f in fs:
                if "__pycache__" in r or f.endswith(".pyc"): continue
                fp=os.path.join(r,f); zf.write(fp,arcname=os.path.relpath(fp,src))
    return buf.getvalue()
# deploy brain-sync
try:
    zb=zd("aws/lambdas/justhodl-brain-sync/source")
    try: lam.get_function_configuration(FunctionName="justhodl-brain-sync"); lam.update_function_code(FunctionName="justhodl-brain-sync",ZipFile=zb); out["deploy"]="updated"
    except lam.exceptions.ResourceNotFoundException:
        lam.create_function(FunctionName="justhodl-brain-sync",Runtime="python3.12",Role=ROLE,Handler="lambda_function.lambda_handler",Code={"ZipFile":zb},Timeout=30,MemorySize=128,Architectures=["x86_64"]); out["deploy"]="created"
    for _ in range(25):
        time.sleep(2); c=lam.get_function_configuration(FunctionName="justhodl-brain-sync")
        if c.get("State")=="Active" and c.get("LastUpdateStatus") in ("Successful",None): break
    rule="justhodl-brain-sync-15min"; events.put_rule(Name=rule,ScheduleExpression="rate(15 minutes)",State="ENABLED")
    fn=lam.get_function(FunctionName="justhodl-brain-sync"); events.put_targets(Rule=rule,Targets=[{"Id":"1","Arn":fn["Configuration"]["FunctionArn"]}])
    try: lam.add_permission(FunctionName="justhodl-brain-sync",StatementId=f"EB-{rule}",Action="lambda:InvokeFunction",Principal="events.amazonaws.com",SourceArn=f"arn:aws:events:us-east-1:{ACC}:rule/{rule}")
    except lam.exceptions.ResourceConflictException: pass
except Exception as e: out["deploy"]="ERR:"+str(e)[:150]
# test brain write via worker (set a seed note with a PIN) then sync
try:
    seed={"notes":[{"id":"seed1","cat":"philosophy","text":"Never confuse the end of QT with the start of QE — wait for the balance sheet to actually EXPAND before going long liquidity.","created":int(time.time()*1000),"pinned":True}],"updated_at":"2026-06-06","v":1}
    req=urllib.request.Request("https://justhodl-data-proxy.raafouis.workers.dev/brain",data=json.dumps(seed).encode(),
        headers={"Content-Type":"application/json","X-Brain-Pin":"7777"},method="PUT")
    out["seed_write"]=json.loads(urllib.request.urlopen(req,timeout=15).read().decode())
except Exception as e: out["seed_write"]="ERR:"+str(e)[:100]
# run brain-sync
lam.invoke(FunctionName="justhodl-brain-sync",InvocationType="RequestResponse",Payload=b"{}"); time.sleep(2)
try:
    b=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/brain.json")["Body"].read())
    out["brain_s3"]={"n_notes":b.get("n_notes"),"n_pinned":b.get("n_pinned"),"prompt_block":(b.get("prompt_block") or "")[:200]}
except Exception as e: out["brain_s3"]="ERR:"+str(e)[:100]
open("aws/ops/reports/1336_brain.json","w").write(json.dumps(out,indent=2,default=str)); print("done")
