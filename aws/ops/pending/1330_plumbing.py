import json, os, time, zipfile, io
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
# create funding-plumbing
try:
    zb=zd("aws/lambdas/justhodl-funding-plumbing/source")
    try: lam.get_function_configuration(FunctionName="justhodl-funding-plumbing"); lam.update_function_code(FunctionName="justhodl-funding-plumbing",ZipFile=zb); out["deploy"]="updated"
    except lam.exceptions.ResourceNotFoundException:
        lam.create_function(FunctionName="justhodl-funding-plumbing",Runtime="python3.12",Role=ROLE,Handler="lambda_function.lambda_handler",Code={"ZipFile":zb},Timeout=60,MemorySize=256,Architectures=["x86_64"]); out["deploy"]="created"
    for _ in range(25):
        time.sleep(2); c=lam.get_function_configuration(FunctionName="justhodl-funding-plumbing")
        if c.get("State")=="Active" and c.get("LastUpdateStatus") in ("Successful",None): break
    rule="justhodl-funding-plumbing-daily"; events.put_rule(Name=rule,ScheduleExpression="cron(15 13 * * ? *)",State="ENABLED")
    fn=lam.get_function(FunctionName="justhodl-funding-plumbing"); events.put_targets(Rule=rule,Targets=[{"Id":"1","Arn":fn["Configuration"]["FunctionArn"]}])
    try: lam.add_permission(FunctionName="justhodl-funding-plumbing",StatementId=f"EB-{rule}",Action="lambda:InvokeFunction",Principal="events.amazonaws.com",SourceArn=f"arn:aws:events:us-east-1:{ACC}:rule/{rule}")
    except lam.exceptions.ResourceConflictException: pass
except Exception as e: out["deploy"]="ERR:"+str(e)[:150]
# invoke + read
lam.invoke(FunctionName="justhodl-funding-plumbing",InvocationType="RequestResponse",Payload=b"{}"); time.sleep(2)
d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/funding-plumbing.json")["Body"].read())
out["regime"]=d.get("regime"); out["score"]=d.get("plumbing_stress_score")
out["balance_sheet"]=d.get("balance_sheet_direction"); out["qt_not_qe"]=d.get("qt_ended_not_qe")
out["drivers"]=[x["note"] for x in d.get("top_drivers",[])]
out["signals"]={k:v.get("note") for k,v in (d.get("signals") or {}).items()}
open("aws/ops/reports/1330_pl.json","w").write(json.dumps(out,indent=2,default=str)); print("done")
