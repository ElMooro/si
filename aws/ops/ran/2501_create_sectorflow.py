import boto3, json, io, zipfile, time
from botocore.config import Config
REGION="us-east-1"; FN="justhodl-sector-flow-state"
ROLE="arn:aws:iam::857687956942:role/lambda-execution-role"
SRC="aws/lambdas/justhodl-sector-flow-state/source/lambda_function.py"
lam=boto3.client("lambda",REGION,config=Config(read_timeout=290,retries={"max_attempts":0}))
events=boto3.client("events",REGION)
# zip the source
buf=io.BytesIO()
with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as z:
    z.writestr("lambda_function.py", open(SRC).read())
code=buf.getvalue()
# create or update
try:
    lam.get_function(FunctionName=FN); exists=True
except lam.exceptions.ResourceNotFoundException:
    exists=False
if exists:
    lam.update_function_code(FunctionName=FN, ZipFile=code); print("updated code")
else:
    lam.create_function(FunctionName=FN, Runtime="python3.12", Role=ROLE,
        Handler="lambda_function.lambda_handler", Code={"ZipFile":code},
        Timeout=60, MemorySize=256, Architectures=["x86_64"],
        Description="Fused sector conviction feed -> data/sector-flow-state.json")
    print("created function")
time.sleep(8)
# schedule
rule="sector-flow-state-hourly"
events.put_rule(Name=rule, ScheduleExpression="cron(20 * * * ? *)", State="ENABLED",
                Description="Hourly fused sector conviction refresh")
arn=lam.get_function(FunctionName=FN)["Configuration"]["FunctionArn"]
try:
    lam.add_permission(FunctionName=FN, StatementId="sfs-evt", Action="lambda:InvokeFunction",
        Principal="events.amazonaws.com", SourceArn=f"arn:aws:events:{REGION}:857687956942:rule/{rule}")
    print("permission added")
except Exception as e: print("perm:",str(e)[:50])
events.put_targets(Rule=rule, Targets=[{"Id":"sfs","Arn":arn}])
print("scheduled", rule)
# invoke once
r=lam.invoke(FunctionName=FN, InvocationType="RequestResponse", Payload=b"{}")
print("invoke status:", r["StatusCode"], "err:", r.get("FunctionError"))
print("payload:", r["Payload"].read().decode()[:300])
# verify the emitted file
s3=boto3.client("s3",REGION)
d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/sector-flow-state.json")["Body"].read())
print("EMITTED n_sectors:",d.get("n_sectors"),"| liq:",d.get("liquidity_regime"),"| phase:",d.get("cycle_phase"))
print("OVERWEIGHT:",d.get("overweight"),"| UNDERWEIGHT:",d.get("underweight"))
for x in d.get("sectors",[])[:5]:
    print("  %-5s conv=%-5s %-11s %-9s conf=%s drivers=%s"%(x["symbol"],x["conviction"],x["posture"],x["quadrant"],x["confluence"],",".join(x["drivers"])))
print("DONE 2501")
