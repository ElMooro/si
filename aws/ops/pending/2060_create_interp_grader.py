"""ops 2060: boto3-create interpretation-grader (bundle llm_router), schedule, invoke, verify."""
import boto3, json, time, io, os, zipfile
from botocore.config import Config
REGION="us-east-1"; FN="justhodl-interpretation-grader"; B="justhodl-dashboard-live"
ROLE="arn:aws:iam::857687956942:role/lambda-execution-role"
lam=boto3.client("lambda",REGION,config=Config(read_timeout=320,retries={"max_attempts":0}))
events=boto3.client("events",REGION); s3=boto3.client("s3",REGION)
buf=io.BytesIO()
with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as z:
    src=f"aws/lambdas/{FN}/source"
    for r,_,fs in os.walk(src):
        for f in fs:
            if f.endswith(".py"): p=os.path.join(r,f); z.write(p,os.path.relpath(p,src))
    for f in os.listdir("aws/shared"):
        if f.endswith(".py"): z.write(os.path.join("aws/shared",f),f)
zb=buf.getvalue()
try: lam.get_function(FunctionName=FN); ex=True
except lam.exceptions.ResourceNotFoundException: ex=False
ENV={"Variables":{"S3_BUCKET":B}}
if ex:
    lam.update_function_code(FunctionName=FN,ZipFile=zb)
    for _ in range(30):
        if lam.get_function(FunctionName=FN)["Configuration"].get("LastUpdateStatus")!="InProgress":break
        time.sleep(3)
    lam.update_function_configuration(FunctionName=FN,Environment=ENV,Timeout=300,MemorySize=512,Runtime="python3.12",Handler="lambda_function.lambda_handler"); print("updated")
else:
    lam.create_function(FunctionName=FN,Runtime="python3.12",Role=ROLE,Handler="lambda_function.lambda_handler",Code={"ZipFile":zb},Timeout=300,MemorySize=512,Environment=ENV,Architectures=["x86_64"],Description="Interpretation grader"); print("created")
for _ in range(40):
    c=lam.get_function(FunctionName=FN)["Configuration"]
    if c.get("State")=="Active" and c.get("LastUpdateStatus") in ("Successful",None):break
    time.sleep(3)
arn=c["FunctionArn"]
rule="justhodl-interp-grader-daily"
rarn=events.put_rule(Name=rule,ScheduleExpression="cron(0 15 * * ? *)",State="ENABLED")["RuleArn"]
try: lam.add_permission(FunctionName=FN,StatementId="evt-ig",Action="lambda:InvokeFunction",Principal="events.amazonaws.com",SourceArn=rarn)
except lam.exceptions.ResourceConflictException: pass
events.put_targets(Rule=rule,Targets=[{"Id":"1","Arn":arn}]); print("scheduled")
try:
    r=lam.invoke(FunctionName=FN,InvocationType="RequestResponse"); print("invoke:",r["StatusCode"],r["Payload"].read().decode()[:250])
except Exception as e: print("invoke note:",str(e)[:80])
time.sleep(2)
d=json.loads(s3.get_object(Bucket=B,Key="data/interpretation-scorecard.json")["Body"].read())
st=d["stats"]
print("\nSTATUS:",d["status"],"| graded",st["n_claims_graded"],"resolved",st["n_resolved"],"pending-horizon",st["n_pending_horizon"],"| hit_rate",st["hit_rate_pct"])
print("DONE 2060")
