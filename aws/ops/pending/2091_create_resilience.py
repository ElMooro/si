import boto3, json, time, io, zipfile, os
from botocore.config import Config
lam=boto3.client("lambda","us-east-1",config=Config(read_timeout=600,retries={"max_attempts":0}))
events=boto3.client("events","us-east-1")
FN="justhodl-resilience"
ROLE="arn:aws:iam::857687956942:role/lambda-execution-role"
src_path="aws/lambdas/justhodl-resilience/source/lambda_function.py"
if not os.path.exists(src_path):
    # find it
    for root,_,files in os.walk("."):
        if "justhodl-resilience" in root and "lambda_function.py" in files:
            src_path=os.path.join(root,"lambda_function.py"); break
print("source:",src_path,"exists",os.path.exists(src_path))
code=open(src_path,"rb").read()
buf=io.BytesIO()
with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as z:
    z.writestr("lambda_function.py",code)
zip_bytes=buf.getvalue()

exists=True
try: lam.get_function(FunctionName=FN)
except lam.exceptions.ResourceNotFoundException: exists=False
print("exists?",exists)
if not exists:
    try:
        lam.create_function(FunctionName=FN,Runtime="python3.12",Role=ROLE,
            Handler="lambda_function.lambda_handler",Code={"ZipFile":zip_bytes},
            Timeout=600,MemorySize=2048,Architectures=["x86_64"],
            Description="Resilience Radar: abnormal return on adverse days / absorption / pre-breakout.")
        print("CREATE submitted")
    except Exception as e:
        print("CREATE ERROR:",str(e)[:300])
else:
    lam.update_function_code(FunctionName=FN,ZipFile=zip_bytes)
    print("UPDATE code submitted")
# wait active
for _ in range(40):
    c=lam.get_function(FunctionName=FN)["Configuration"]
    if c.get("State")=="Active" and c.get("LastUpdateStatus")=="Successful": break
    time.sleep(3)
print("state:",c.get("State"),c.get("LastUpdateStatus"),"mem",c.get("MemorySize"),"to",c.get("Timeout"))
# schedule rule
try:
    rule=events.put_rule(Name="justhodl-resilience-daily",ScheduleExpression="cron(45 22 ? * MON-FRI *)",State="ENABLED")
    arn=lam.get_function(FunctionName=FN)["Configuration"]["FunctionArn"]
    try:
        lam.add_permission(FunctionName=FN,StatementId="resilience-sched",Action="lambda:InvokeFunction",
                           Principal="events.amazonaws.com",SourceArn=rule["RuleArn"])
    except Exception as e: print("perm:",str(e)[:60])
    events.put_targets(Rule="justhodl-resilience-daily",Targets=[{"Id":"1","Arn":arn}])
    print("schedule wired")
except Exception as e: print("sched err:",str(e)[:120])
print("DONE 2091")
