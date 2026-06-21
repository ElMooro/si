"""ops 2062: boto3-create paper-book, schedule, invoke twice (init then mark), verify."""
import boto3, json, time, io, os, zipfile
REGION="us-east-1"; FN="justhodl-paper-book"; B="justhodl-dashboard-live"
ROLE="arn:aws:iam::857687956942:role/lambda-execution-role"
lam=boto3.client("lambda",REGION); events=boto3.client("events",REGION); s3=boto3.client("s3",REGION)
buf=io.BytesIO()
with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as z:
    src=f"aws/lambdas/{FN}/source"
    for r,_,fs in os.walk(src):
        for f in fs:
            if f.endswith(".py"): p=os.path.join(r,f); z.write(p,os.path.relpath(p,src))
zb=buf.getvalue()
try: lam.get_function(FunctionName=FN); ex=True
except lam.exceptions.ResourceNotFoundException: ex=False
ENV={"Variables":{"S3_BUCKET":B}}
if ex:
    lam.update_function_code(FunctionName=FN,ZipFile=zb)
    for _ in range(30):
        if lam.get_function(FunctionName=FN)["Configuration"].get("LastUpdateStatus")!="InProgress":break
        time.sleep(3)
    lam.update_function_configuration(FunctionName=FN,Environment=ENV,Timeout=180,MemorySize=512,Runtime="python3.12",Handler="lambda_function.lambda_handler"); print("updated")
else:
    lam.create_function(FunctionName=FN,Runtime="python3.12",Role=ROLE,Handler="lambda_function.lambda_handler",Code={"ZipFile":zb},Timeout=180,MemorySize=512,Environment=ENV,Architectures=["x86_64"],Description="Paper book"); print("created")
for _ in range(40):
    c=lam.get_function(FunctionName=FN)["Configuration"]
    if c.get("State")=="Active" and c.get("LastUpdateStatus") in ("Successful",None):break
    time.sleep(3)
arn=c["FunctionArn"]
rule="justhodl-paper-book-daily"
rarn=events.put_rule(Name=rule,ScheduleExpression="cron(30 21 * * ? *)",State="ENABLED")["RuleArn"]
try: lam.add_permission(FunctionName=FN,StatementId="evt-pb",Action="lambda:InvokeFunction",Principal="events.amazonaws.com",SourceArn=rarn)
except lam.exceptions.ResourceConflictException: pass
events.put_targets(Rule=rule,Targets=[{"Id":"1","Arn":arn}]); print("scheduled")
print("init run:",lam.invoke(FunctionName=FN,InvocationType="RequestResponse")["Payload"].read().decode()[:220])
time.sleep(2)
d=json.loads(s3.get_object(Bucket=B,Key="data/paper-book.json")["Body"].read())
print("\nPAPER BOOK INITIALIZED:")
print(f"  inception {d['inception']} | NAV ${d['nav']:.0f} | rebalanced into {len(d['positions'])} positions | cash {d['cash_pct']}%")
print("  regime at rebal:",d.get("regime_at_last_rebalance"))
print("  top positions:",[f"{p['ticker']} {p['weight_pct']}%" for p in d["positions"][:8]])
print("  trades logged:",len(d.get("recent_trades",[])))
print("DONE 2062")
