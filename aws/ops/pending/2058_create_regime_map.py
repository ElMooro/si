"""ops 2058: boto3-create justhodl-regime-map, schedule, invoke, verify the dispersion read."""
import boto3, json, time, io, os, zipfile
REGION="us-east-1"; FN="justhodl-regime-map"; B="justhodl-dashboard-live"
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
    lam.update_function_configuration(FunctionName=FN,Environment=ENV,Timeout=180,MemorySize=512,Runtime="python3.12",Handler="lambda_function.lambda_handler")
    print("updated")
else:
    lam.create_function(FunctionName=FN,Runtime="python3.12",Role=ROLE,Handler="lambda_function.lambda_handler",
        Code={"ZipFile":zb},Timeout=180,MemorySize=512,Environment=ENV,Architectures=["x86_64"],Description="Risk Map cross-asset dispersion")
    print("created")
for _ in range(40):
    c=lam.get_function(FunctionName=FN)["Configuration"]
    if c.get("State")=="Active" and c.get("LastUpdateStatus") in ("Successful",None):break
    time.sleep(3)
arn=c["FunctionArn"]
rule="justhodl-regime-map-daily"
rarn=events.put_rule(Name=rule,ScheduleExpression="cron(0 13,21 * * ? *)",State="ENABLED")["RuleArn"]
try: lam.add_permission(FunctionName=FN,StatementId="evt-rmap",Action="lambda:InvokeFunction",Principal="events.amazonaws.com",SourceArn=rarn)
except lam.exceptions.ResourceConflictException: pass
events.put_targets(Rule=rule,Targets=[{"Id":"1","Arn":arn}]); print("scheduled")
r=lam.invoke(FunctionName=FN,InvocationType="RequestResponse")
print("invoke:",r["StatusCode"],r["Payload"].read().decode()[:200])
time.sleep(2)
d=json.loads(s3.get_object(Bucket=B,Key="data/regime-map.json")["Body"].read())
rg=d["regime"]
print("\n=== REGIME:",rg["label"],"===")
print(rg["summary"])
print(f"\nstats: eq_avg {rg['equity_avg']} | crypto {rg['crypto_avg']} | rates {rg['rates_avg']} | commod {rg['commodities_avg']} | breadth {rg['equity_breadth_pct']}% | conc-spread {rg['concentration_spread_3m']}pts")
print("\n🔥 BOOMING:")
for x in d["booming"]: print(f"   {x['ticker']:<6} {x['name']:<22} ro {x['risk_on']:+4}  {x['state']:<9} (3m {x['r3m']:+.0f}%)")
print("💀 DESTROYED:")
for x in d["destroyed"]: print(f"   {x['ticker']:<6} {x['name']:<22} ro {x['risk_on']:+4}  {x['state']:<9} (3m {x['r3m']:+.0f}%)")
print("\nn_instruments:",d["n_instruments"])
print("DONE 2058")
