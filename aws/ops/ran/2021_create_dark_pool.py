"""ops 2021: create justhodl-dark-pool via boto3, schedule weekly, invoke, verify."""
import boto3, json, time, io, os, zipfile
REGION="us-east-1"; FN="justhodl-dark-pool"; B="justhodl-dashboard-live"
ROLE="arn:aws:iam::857687956942:role/lambda-execution-role"
lam=boto3.client("lambda",REGION); events=boto3.client("events",REGION); s3=boto3.client("s3",REGION)
ENV={"Variables":{"POLYGON_KEY":"zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d","S3_BUCKET":B}}
SRC=f"aws/lambdas/{FN}/source"; buf=io.BytesIO()
with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as z:
    for r,_,fs in os.walk(SRC):
        for f in fs:
            if f.endswith(".py"): p=os.path.join(r,f); z.write(p,os.path.relpath(p,SRC))
zb=buf.getvalue()
try: lam.get_function(FunctionName=FN); ex_=True
except lam.exceptions.ResourceNotFoundException: ex_=False
if ex_:
    print("update"); lam.update_function_code(FunctionName=FN,ZipFile=zb)
    for _ in range(24):
        if lam.get_function(FunctionName=FN)["Configuration"].get("LastUpdateStatus")!="InProgress":break
        time.sleep(4)
    lam.update_function_configuration(FunctionName=FN,Environment=ENV,Timeout=300,MemorySize=1024,Runtime="python3.12",Handler="lambda_function.lambda_handler")
else:
    print("create"); lam.create_function(FunctionName=FN,Runtime="python3.12",Role=ROLE,Handler="lambda_function.lambda_handler",
        Code={"ZipFile":zb},Timeout=300,MemorySize=1024,Environment=ENV,Architectures=["x86_64"],Description="Per-name dark-pool accumulation (FINRA ATS)")
for _ in range(30):
    c=lam.get_function(FunctionName=FN)["Configuration"]
    if c.get("State")=="Active" and c.get("LastUpdateStatus")=="Successful":break
    time.sleep(4)
arn=c["FunctionArn"];print("active")
rule="justhodl-dark-pool-weekly"
rarn=events.put_rule(Name=rule,ScheduleExpression="cron(0 14 ? * WED *)",State="ENABLED",Description="weekly dark-pool")["RuleArn"]
try: lam.add_permission(FunctionName=FN,StatementId="evt-dark-pool",Action="lambda:InvokeFunction",Principal="events.amazonaws.com",SourceArn=rarn)
except lam.exceptions.ResourceConflictException: pass
events.put_targets(Rule=rule,Targets=[{"Id":"1","Arn":arn}]);print("scheduled")
print("invoking (FINRA bulk + grouped, ~30-60s)…")
r=lam.invoke(FunctionName=FN,InvocationType="RequestResponse")
print("status:",r["StatusCode"]," payload:",r["Payload"].read().decode()[:500])
time.sleep(2)
d=json.loads(s3.get_object(Bucket=B,Key="data/dark-pool.json")["Body"].read())
print("\nok:",d.get("ok"),"week:",d.get("latest_week"),"scored:",d.get("n_scored"),"dist:",d.get("distribution"),"picks:",len(d.get("top_picks") or []))
print("\nTOP ACCUMULATION:")
for r in (d.get("top_accumulation") or [])[:12]:
    print(f"  {r['ticker']:<6} score={r['score']:<5} dark%={r['dark_pool_pct']:<6} offex%={r['offex_pct']:<6} accel={r['dark_accel']} wkRet={r['week_return_pct']}% ATS={r['ats_shares_wk']:,}")
print("\nPICKS:",[(p['ticker'],p['score'],p['dark_pool_pct'],p['dark_accel']) for p in d.get('top_picks',[])])
print("dark_map size:",len(d.get("dark_map") or {}))
print("DONE 2021")
