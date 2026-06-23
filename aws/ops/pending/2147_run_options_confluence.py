import boto3, json, time, io, zipfile
from botocore.config import Config
lam=boto3.client("lambda","us-east-1",config=Config(read_timeout=200,retries={"max_attempts":0}))
ev=boto3.client("events","us-east-1"); s3=boto3.client("s3","us-east-1")
FN="justhodl-options-confluence"; ROLE="arn:aws:iam::857687956942:role/lambda-execution-role"
try: lam.get_function(FunctionName=FN); print("exists")
except lam.exceptions.ResourceNotFoundException:
    buf=io.BytesIO()
    with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as z:
        z.writestr("lambda_function.py",open("aws/lambdas/justhodl-options-confluence/source/lambda_function.py").read())
    lam.create_function(FunctionName=FN,Runtime="python3.12",Role=ROLE,Handler="lambda_function.lambda_handler",
        Code={"ZipFile":buf.getvalue()},Timeout=120,MemorySize=512,Architectures=["x86_64"])
    for _ in range(20):
        if lam.get_function(FunctionName=FN)["Configuration"].get("State")=="Active": break
        time.sleep(3)
    rule="justhodl-options-confluence-hourly"; ev.put_rule(Name=rule,ScheduleExpression="cron(20 * * * ? *)",State="ENABLED")
    try: lam.add_permission(FunctionName=FN,StatementId="ev-"+rule,Action="lambda:InvokeFunction",Principal="events.amazonaws.com",SourceArn=f"arn:aws:events:us-east-1:857687956942:rule/{rule}")
    except Exception as e: print("perm",str(e)[:40])
    ev.put_targets(Rule=rule,Targets=[{"Id":"1","Arn":lam.get_function(FunctionName=FN)["Configuration"]["FunctionArn"]}]); print("created+scheduled")
for _ in range(25):
    c=lam.get_function(FunctionName=FN)["Configuration"]
    if c.get("LastUpdateStatus")=="Successful" and c.get("State")=="Active": break
    time.sleep(3)
t=time.time(); r=lam.invoke(FunctionName=FN,InvocationType="RequestResponse")
print("invoke:",r["Payload"].read().decode()[:200],f"({time.time()-t:.0f}s)")
d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/options-confluence.json")["Body"].read())
print("counts:",json.dumps(d.get("counts",{})))
print("\nMULTI-ENGINE CONFLUENCE (cross-engine agreement):")
for b in d.get("multi_engine_confluence",[])[:10]:
    print(f"   {b['ticker']:<6} {b['posture']:<13} score={b['score']} engines={b['n_engines']} {b.get('tags')}")
print("DONE 2147")
