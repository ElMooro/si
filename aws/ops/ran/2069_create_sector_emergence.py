import boto3, json, time, io, os, zipfile
REGION="us-east-1"; FN="justhodl-sector-emergence"; B="justhodl-dashboard-live"
ROLE="arn:aws:iam::857687956942:role/lambda-execution-role"
lam=boto3.client("lambda",REGION); events=boto3.client("events",REGION); s3=boto3.client("s3",REGION)
buf=io.BytesIO()
with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as z:
    for r,_,fs in os.walk(f"aws/lambdas/{FN}/source"):
        for f in fs:
            if f.endswith(".py"): p=os.path.join(r,f); z.write(p,os.path.relpath(p,f"aws/lambdas/{FN}/source"))
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
    lam.create_function(FunctionName=FN,Runtime="python3.12",Role=ROLE,Handler="lambda_function.lambda_handler",Code={"ZipFile":zb},Timeout=180,MemorySize=512,Environment=ENV,Architectures=["x86_64"],Description="Sector emergence"); print("created")
for _ in range(40):
    c=lam.get_function(FunctionName=FN)["Configuration"]
    if c.get("State")=="Active" and c.get("LastUpdateStatus") in ("Successful",None):break
    time.sleep(3)
arn=c["FunctionArn"]; rule="justhodl-sector-emergence-daily"
rarn=events.put_rule(Name=rule,ScheduleExpression="cron(0 22 * * ? *)",State="ENABLED")["RuleArn"]
try: lam.add_permission(FunctionName=FN,StatementId="evt-se",Action="lambda:InvokeFunction",Principal="events.amazonaws.com",SourceArn=rarn)
except lam.exceptions.ResourceConflictException: pass
events.put_targets(Rule=rule,Targets=[{"Id":"1","Arn":arn}]); print("scheduled")
print("invoke:",lam.invoke(FunctionName=FN,InvocationType="RequestResponse")["Payload"].read().decode()[:200])
time.sleep(2)
d=json.loads(s3.get_object(Bucket=B,Key="data/sector-emergence.json")["Body"].read())
print("\nSECTOR EMERGENCE | regime:",d.get("regime_context"))
print(f"{'sector':<22}{'stage':<11}{'score':>6}  rs/trend/breadth  signals")
for o in d["sectors"]:
    print(f"  {o['name']:<20}{o['stage']:<11}{o['emergence_score']:>6}  {o['rs_score']}/{o['trend_score']}/{o['breadth_score']:<3}  {', '.join(o['signals'][:3])}")
print("\nEMERGING now:",d.get("emerging_now"),"| top_picks:",[p['ticker'] for p in d.get("top_picks",[])])
print("DONE 2069")
