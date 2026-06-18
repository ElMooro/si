import io,json,zipfile,os,time,boto3
from botocore.config import Config
REGION="us-east-1"; FN="justhodl-eurodollar-plumbing"
ROLE="arn:aws:iam::857687956942:role/lambda-execution-role"
lam=boto3.client("lambda",region_name=REGION,config=Config(read_timeout=300,retries={"max_attempts":2}))
ev=boto3.client("events",region_name=REGION); s3=boto3.client("s3",region_name=REGION); B="justhodl-dashboard-live"
def wait():
    for _ in range(30):
        c=lam.get_function_configuration(FunctionName=FN)
        if c.get("State")=="Active" and c.get("LastUpdateStatus")!="InProgress": return
        time.sleep(5)
buf=io.BytesIO()
with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as z:
    added=set(); src="aws/lambdas/%s/source"%FN
    for root,_,files in os.walk(src):
        for f in files:
            if f.endswith(".pyc"):continue
            p=os.path.join(root,f);arc=os.path.relpath(p,src);z.write(p,arc);added.add(arc)
    for f in os.listdir("aws/shared"):
        if f.endswith(".py") and f not in added: z.write(os.path.join("aws/shared",f),f)
zb=buf.getvalue(); print("zip",len(zb))
try:
    lam.get_function(FunctionName=FN); exists=True
except lam.exceptions.ResourceNotFoundException: exists=False
if exists:
    lam.update_function_code(FunctionName=FN,ZipFile=zb); wait()
    print("UPDATED")
else:
    env={"S3_BUCKET":B,"FRED_API_KEY":"2f057499936072679d8843d7fce99989","FMP_API_KEY":"wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb"}
    lam.create_function(FunctionName=FN,Runtime="python3.12",Role=ROLE,Handler="lambda_function.lambda_handler",
        Code={"ZipFile":zb},Timeout=300,MemorySize=512,Architectures=["x86_64"],Environment={"Variables":env},
        Description="Offshore USD funding monitor")
    wait(); print("CREATED")
    rule=ev.put_rule(Name="eurodollar-plumbing-daily",ScheduleExpression="cron(0 12 * * ? *)",State="ENABLED")["RuleArn"]
    arn=lam.get_function(FunctionName=FN)["Configuration"]["FunctionArn"]
    try: lam.add_permission(FunctionName=FN,StatementId="ed-plumbing-evt",Action="lambda:InvokeFunction",Principal="events.amazonaws.com",SourceArn=rule)
    except lam.exceptions.ResourceConflictException: pass
    ev.put_targets(Rule="eurodollar-plumbing-daily",Targets=[{"Id":"1","Arn":arn}]); print("scheduled")
r=lam.invoke(FunctionName=FN,InvocationType="RequestResponse"); print("invoke:",r["Payload"].read().decode()[:160])
d=json.loads(s3.get_object(Bucket=B,Key="data/eurodollar-plumbing.json")["Body"].read())
print("HEALTH",d["plumbing_health"],"VERDICT",d["verdict"],"| reds:",d["red_flags"],"| yellows:",d["yellow_flags"])
print("AI:",json.dumps(d["ai"])[:400])
for lk,lv in d["layers"].items():
    print(" ",lk,":",", ".join(f"{m['label'].split('(')[0].strip()}={m['value']}{m['unit']}[{m['status']}]" for m in lv["metrics"]))
