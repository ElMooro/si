import boto3, json, io, zipfile, time
from botocore.config import Config
lam=boto3.client("lambda","us-east-1",config=Config(read_timeout=290,retries={"max_attempts":0}))
ev=boto3.client("events","us-east-1")
s3=boto3.client("s3","us-east-1")
FN="justhodl-sector-capital-fusion"
ROLE="arn:aws:iam::857687956942:role/lambda-execution-role"
SRC="aws/lambdas/justhodl-sector-capital-fusion/source/lambda_function.py"
buf=io.BytesIO()
with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as z: z.writestr("lambda_function.py",open(SRC).read())
code=buf.getvalue()
try:
    lam.get_function(FunctionName=FN); exists=True
except lam.exceptions.ResourceNotFoundException: exists=False
if exists:
    lam.update_function_code(FunctionName=FN,ZipFile=code); print("updated existing")
else:
    lam.create_function(FunctionName=FN,Runtime="python3.12",Role=ROLE,
        Handler="lambda_function.lambda_handler",Code={"ZipFile":code},
        Timeout=120,MemorySize=512,Architectures=["x86_64"],
        Environment={"Variables":{"S3_BUCKET":"justhodl-dashboard-live"}},
        Description="Sector capital-flow fusion verdict")
    print("created new function")
for _ in range(40):
    if lam.get_function_configuration(FunctionName=FN).get("LastUpdateStatus","Successful")=="Successful"\
       and lam.get_function_configuration(FunctionName=FN).get("State")=="Active": break
    time.sleep(2)
# hourly schedule
RULE="sector-capital-fusion-hourly"
ev.put_rule(Name=RULE,ScheduleExpression="cron(45 * * * ? *)",State="ENABLED")
arn=lam.get_function(FunctionName=FN)["Configuration"]["FunctionArn"]
try:
    lam.add_permission(FunctionName=FN,StatementId="evt-fusion-hourly",Action="lambda:InvokeFunction",
        Principal="events.amazonaws.com",SourceArn=ev.describe_rule(Name=RULE)["Arn"])
except lam.exceptions.ResourceConflictException: pass
ev.put_targets(Rule=RULE,Targets=[{"Id":"fusion","Arn":arn}])
print("scheduled hourly :45")
# invoke now
r=lam.invoke(FunctionName=FN,InvocationType="RequestResponse",Payload=b"{}")
print("invoke err:",r.get("FunctionError"))
print("resp:",r["Payload"].read().decode()[:300])
time.sleep(2)
d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/sector-capital-fusion.json")["Body"].read())
print("\nHEADLINE:",d.get("headline"))
print("BACKDROP:",json.dumps(d.get("backdrop")))
print("TOP INFLOW:",d.get("top_inflow"),"| TOP OUTFLOW:",d.get("top_outflow"))
print("\n=== PER-SECTOR VERDICT (ranked by net flow) ===")
for x in d.get("sectors",[]):
    fams="".join({"rotation":"R","tape":"T","etf":"E","institutional":"I","darkpool":"D"}[k] if x["families"][k]["dir"]>0 else ("." if x["families"][k]["dir"]==0 else {"rotation":"r","tape":"t","etf":"e","institutional":"i","darkpool":"d"}[k]) for k in ["rotation","tape","etf","institutional","darkpool"])
    dv=" ⚠DIVERGENCE" if x["divergence"] else ""
    print(f"  {x['sector']:24} net={x['net_score']:+.2f} conf={x['confluence']}/{x['n_families']} [{fams}] {x['posture']:11}{dv}")
    print(f"       → {x['conclusion']}")
print("DONE 2519")
