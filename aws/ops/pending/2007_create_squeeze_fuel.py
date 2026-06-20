"""ops 2007: create justhodl-squeeze-fuel via boto3 (new dir no-op), schedule, invoke, verify."""
import boto3, json, time, io, os, zipfile
REGION="us-east-1"; FN="justhodl-squeeze-fuel"; B="justhodl-dashboard-live"
ROLE="arn:aws:iam::857687956942:role/lambda-execution-role"
lam=boto3.client("lambda",REGION); events=boto3.client("events",REGION); s3=boto3.client("s3",REGION)

# FMP_KEY from finra-short
env_src=lam.get_function(FunctionName="justhodl-finra-short")["Configuration"].get("Environment",{}).get("Variables",{})
fmp=env_src.get("FMP_KEY") or "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb"
ENV={"Variables":{"FMP_KEY":fmp,"S3_BUCKET":B}}

SRC=f"aws/lambdas/{FN}/source"; buf=io.BytesIO()
with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as z:
    for r,_,fs in os.walk(SRC):
        for f in fs:
            if f.endswith(".py"): p=os.path.join(r,f); z.write(p,os.path.relpath(p,SRC))
zb=buf.getvalue()

try:
    lam.get_function(FunctionName=FN); exists=True
except lam.exceptions.ResourceNotFoundException:
    exists=False
if exists:
    print("exists -> update")
    lam.update_function_code(FunctionName=FN,ZipFile=zb)
    for _ in range(24):
        c=lam.get_function(FunctionName=FN)["Configuration"]
        if c.get("LastUpdateStatus")!="InProgress": break
        time.sleep(4)
    lam.update_function_configuration(FunctionName=FN,Environment=ENV,Timeout=300,MemorySize=1024,Runtime="python3.12",Handler="lambda_function.lambda_handler")
else:
    print("create new")
    lam.create_function(FunctionName=FN,Runtime="python3.12",Role=ROLE,
        Handler="lambda_function.lambda_handler",Code={"ZipFile":zb},
        Timeout=300,MemorySize=1024,Environment=ENV,Architectures=["x86_64"],
        Description="Per-name short-squeeze fuel gauge (FINRA SI + SEC FTD + daily short-vol + float)")
for _ in range(30):
    c=lam.get_function(FunctionName=FN)["Configuration"]
    if c.get("State")=="Active" and c.get("LastUpdateStatus")!="InProgress": break
    time.sleep(4)
arn=lam.get_function(FunctionName=FN)["Configuration"]["FunctionArn"]
print("fn active:",arn)

# schedule
rule="justhodl-squeeze-fuel-daily"
rarn=events.put_rule(Name=rule,ScheduleExpression="cron(30 13 ? * TUE-SAT *)",State="ENABLED",
    Description="Daily 13:30 UTC Tue-Sat squeeze-fuel refresh")["RuleArn"]
try:
    lam.add_permission(FunctionName=FN,StatementId="evt-squeeze-fuel",Action="lambda:InvokeFunction",
        Principal="events.amazonaws.com",SourceArn=rarn)
except lam.exceptions.ResourceConflictException: pass
events.put_targets(Rule=rule,Targets=[{"Id":"1","Arn":arn}])
print("scheduled:",rule)

# invoke + verify
print("invoking (may take ~30-60s)…")
r=lam.invoke(FunctionName=FN,InvocationType="RequestResponse")
print("status:",r["StatusCode"]); print("payload:",r["Payload"].read().decode()[:600])
time.sleep(2)
d=json.loads(s3.get_object(Bucket=B,Key="data/squeeze-fuel.json")["Body"].read())
print("\nok:",d.get("ok"),"settlement:",d.get("si_settlement_date"),"ftd:",d.get("ftd_file"))
print("n_finra_universe:",d.get("n_finra_universe"),"n_scored:",d.get("n_scored"),"dist:",d.get("distribution"))
print("top_picks:",len(d.get("top_picks") or []))
print("\nTOP BOARD:")
for r in (d.get("board") or [])[:12]:
    print(f"  {r['ticker']:<6} fuel={r['score']:<5} {r['state']:<9} %flt={r.get('pct_of_float')} dtc={r.get('days_to_cover')} siΔ={r.get('si_change_pct')} | {' · '.join((r.get('reasons') or [])[:2])}")
print("\nPICKS:", [(p['ticker'],p['score'],p['state']) for p in (d.get('top_picks') or [])])
print("DONE 2007")
