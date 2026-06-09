import json, os, time, zipfile, io
import boto3
from botocore.config import Config
cfg=Config(read_timeout=300,retries={"max_attempts":1})
lam=boto3.client("lambda",region_name="us-east-1",config=cfg)
s3=boto3.client("s3",region_name="us-east-1",config=cfg)
ROLE="arn:aws:iam::857687956942:role/lambda-execution-role"
out={}
buf=io.BytesIO(); src="aws/lambdas/justhodl-ecb-history/source"
with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as zf:
    for r,_,fs in os.walk(src):
        for f in fs:
            if "__pycache__" in r or f.endswith(".pyc"): continue
            zf.write(os.path.join(r,f),arcname=os.path.relpath(os.path.join(r,f),src))
zb=buf.getvalue()
try:
    lam.get_function_configuration(FunctionName="justhodl-ecb-history"); lam.update_function_code(FunctionName="justhodl-ecb-history",ZipFile=zb)
except lam.exceptions.ResourceNotFoundException:
    lam.create_function(FunctionName="justhodl-ecb-history",Runtime="python3.12",Role=ROLE,Handler="lambda_function.lambda_handler",Code={"ZipFile":zb},Timeout=180,MemorySize=512,Architectures=["x86_64"],Environment={"Variables":{}})
for _ in range(25):
    time.sleep(2); c=lam.get_function_configuration(FunctionName="justhodl-ecb-history")
    if c.get("State")=="Active" and c.get("LastUpdateStatus") in ("Successful",None): break
# schedule
ev=boto3.client("events",region_name="us-east-1",config=cfg)
ev.put_rule(Name="justhodl-ecb-history-weekly",ScheduleExpression="cron(0 6 ? * SAT *)",State="ENABLED")
fn=lam.get_function(FunctionName="justhodl-ecb-history"); ev.put_targets(Rule="justhodl-ecb-history-weekly",Targets=[{"Id":"1","Arn":fn["Configuration"]["FunctionArn"]}])
try: lam.add_permission(FunctionName="justhodl-ecb-history",StatementId="EB-ecbhist",Action="lambda:InvokeFunction",Principal="events.amazonaws.com",SourceArn="arn:aws:events:us-east-1:857687956942:rule/justhodl-ecb-history-weekly")
except Exception: pass
# run
r=lam.invoke(FunctionName="justhodl-ecb-history",InvocationType="RequestResponse",Payload=b"{}")
out["run"]=r["Payload"].read().decode()[:120]
time.sleep(3)
# verify a history file has full range
try:
    m=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/ecb-hist/_manifest.json")["Body"].read())
    out["manifest_n"]=m.get("n"); out["series"]=[(s["id"],s["first_date"],s["latest_date"],s["n_points"]) for s in m.get("series",[])[:6]]
except Exception as e: out["manifest_err"]=str(e)[:60]
open("aws/ops/reports/1488_h.json","w").write(json.dumps(out,indent=2,default=str)); print("done")
