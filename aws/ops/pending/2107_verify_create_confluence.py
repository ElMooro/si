import boto3, json, time, io, zipfile, urllib.request
from botocore.config import Config
lam=boto3.client("lambda","us-east-1",config=Config(read_timeout=600,retries={"max_attempts":0}))
ev=boto3.client("events","us-east-1")
FN="justhodl-equity-confluence"
ROLE="arn:aws:iam::857687956942:role/lambda-execution-role"
exists=True
try: lam.get_function(FunctionName=FN); print("get_function: EXISTS (deploy-lambdas created it)")
except lam.exceptions.ResourceNotFoundException: exists=False; print("get_function: NotFound -> boto3-create fallback")
if not exists:
    import urllib.request
    src=open("aws/lambdas/justhodl-equity-confluence/source/lambda_function.py").read()
    buf=io.BytesIO()
    with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as z: z.writestr("lambda_function.py",src)
    lam.create_function(FunctionName=FN,Runtime="python3.12",Role=ROLE,Handler="lambda_function.lambda_handler",
        Code={"ZipFile":buf.getvalue()},Timeout=120,MemorySize=512,Architectures=["x86_64"])
    print("created.")
    for _ in range(20):
        c=lam.get_function(FunctionName=FN)["Configuration"]
        if c.get("State")=="Active": break
        time.sleep(3)
    rule="justhodl-equity-confluence-daily"
    ev.put_rule(Name=rule,ScheduleExpression="cron(30 0 * * ? *)",State="ENABLED")
    try: lam.add_permission(FunctionName=FN,StatementId="ev-"+rule,Action="lambda:InvokeFunction",Principal="events.amazonaws.com",SourceArn=f"arn:aws:events:us-east-1:857687956942:rule/{rule}")
    except Exception as e: print("perm:",str(e)[:60])
    arn=lam.get_function(FunctionName=FN)["Configuration"]["FunctionArn"]
    ev.put_targets(Rule=rule,Targets=[{"Id":"1","Arn":arn}])
    print("schedule wired.")
# wait for code update settle then invoke
for _ in range(20):
    c=lam.get_function(FunctionName=FN)["Configuration"]
    if c.get("LastUpdateStatus")=="Successful" and c.get("State")=="Active": break
    time.sleep(3)
t=time.time()
print("invoke:",lam.invoke(FunctionName=FN,InvocationType="RequestResponse")["Payload"].read().decode()[:300],f"({time.time()-t:.0f}s)")
print("DONE 2107")
