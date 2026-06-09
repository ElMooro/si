import json, os, time, zipfile, io
import boto3
from botocore.config import Config
cfg=Config(read_timeout=180,retries={"max_attempts":1})
lam=boto3.client("lambda",region_name="us-east-1",config=cfg)
events=boto3.client("events",region_name="us-east-1",config=cfg); ACC="857687956942"
ROLE="arn:aws:iam::857687956942:role/lambda-execution-role"
out={}
def zd(src):
    buf=io.BytesIO()
    with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as zf:
        for r,_,fs in os.walk(src):
            for f in fs:
                if "__pycache__" in r or f.endswith(".pyc"): continue
                zf.write(os.path.join(r,f),arcname=os.path.relpath(os.path.join(r,f),src))
    return buf.getvalue()
for name,sched in [("justhodl-move-index","cron(20 13 * * ? *)"),("justhodl-basket-var","cron(10 14 * * ? *)")]:
    zb=zd(f"aws/lambdas/{name}/source")
    try:
        lam.get_function_configuration(FunctionName=name); lam.update_function_code(FunctionName=name,ZipFile=zb); out[name]="updated"
    except lam.exceptions.ResourceNotFoundException:
        to=40 if "move" in name else 120; mem=128 if "move" in name else 256
        lam.create_function(FunctionName=name,Runtime="python3.12",Role=ROLE,Handler="lambda_function.lambda_handler",Code={"ZipFile":zb},Timeout=to,MemorySize=mem,Architectures=["x86_64"],Environment={"Variables":{}}); out[name]="created"
    for _ in range(25):
        time.sleep(2); c=lam.get_function_configuration(FunctionName=name)
        if c.get("State")=="Active" and c.get("LastUpdateStatus") in ("Successful",None): break
    rn=name+"-daily"; events.put_rule(Name=rn,ScheduleExpression=sched,State="ENABLED")
    fn=lam.get_function(FunctionName=name); events.put_targets(Rule=rn,Targets=[{"Id":"1","Arn":fn["Configuration"]["FunctionArn"]}])
    try: lam.add_permission(FunctionName=name,StatementId="EB-"+rn,Action="lambda:InvokeFunction",Principal="events.amazonaws.com",SourceArn=f"arn:aws:events:us-east-1:{ACC}:rule/{rn}")
    except lam.exceptions.ResourceConflictException: pass
    # run now
    try: r=lam.invoke(FunctionName=name,InvocationType="RequestResponse",Payload=b"{}"); out[name+"_run"]=r["Payload"].read().decode()[:120]
    except Exception as e: out[name+"_run"]=str(e)[:90]
open("aws/ops/reports/1484_d.json","w").write(json.dumps(out,indent=2,default=str)); print("done")
