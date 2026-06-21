"""ops 2064: boto3-create regime-conditional-trust, schedule, invoke (heavy), verify conditional stats vs real outcomes."""
import boto3, json, time, io, os, zipfile
from botocore.config import Config
REGION="us-east-1"; FN="justhodl-regime-conditional-trust"; B="justhodl-dashboard-live"
ROLE="arn:aws:iam::857687956942:role/lambda-execution-role"
lam=boto3.client("lambda",REGION,config=Config(read_timeout=620,retries={"max_attempts":0}))
events=boto3.client("events",REGION); s3=boto3.client("s3",REGION)
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
    lam.update_function_configuration(FunctionName=FN,Environment=ENV,Timeout=600,MemorySize=1024,Runtime="python3.12",Handler="lambda_function.lambda_handler"); print("updated")
else:
    lam.create_function(FunctionName=FN,Runtime="python3.12",Role=ROLE,Handler="lambda_function.lambda_handler",Code={"ZipFile":zb},Timeout=600,MemorySize=1024,Environment=ENV,Architectures=["x86_64"],Description="Regime-conditional engine trust"); print("created")
for _ in range(40):
    c=lam.get_function(FunctionName=FN)["Configuration"]
    if c.get("State")=="Active" and c.get("LastUpdateStatus") in ("Successful",None):break
    time.sleep(3)
arn=c["FunctionArn"]
rule="justhodl-regime-cond-trust-daily"
rarn=events.put_rule(Name=rule,ScheduleExpression="cron(0 12 * * ? *)",State="ENABLED")["RuleArn"]
try: lam.add_permission(FunctionName=FN,StatementId="evt-rct",Action="lambda:InvokeFunction",Principal="events.amazonaws.com",SourceArn=rarn)
except lam.exceptions.ResourceConflictException: pass
events.put_targets(Rule=rule,Targets=[{"Id":"1","Arn":arn}]); print("scheduled")
try:
    r=lam.invoke(FunctionName=FN,InvocationType="RequestResponse"); print("invoke:",r["StatusCode"],r["Payload"].read().decode()[:240])
except Exception as e: print("invoke note (engine may still complete):",str(e)[:90])
time.sleep(3)
d=json.loads(s3.get_object(Bucket=B,Key="data/regime-conditional-trust.json")["Body"].read())
print("\nREGIME-CONDITIONAL TRUST:")
print("  current_regime:",d.get("current_regime"),"| engines measured:",d.get("n_engines"))
print("  reconstructed regime distribution (weekly):",d.get("regime_distribution"))
print("  BEST suited to current regime:")
for b in (d.get("best_suited_to_current_regime") or [])[:6]:
    print(f"    {b['engine']:<32} mean_excess {b.get('regime_mean_excess_pct')}% n{b.get('n')} factor {b.get('factor')}")
print("  WORST suited to current regime:")
for w in (d.get("worst_suited_to_current_regime") or [])[:6]:
    print(f"    {w['engine']:<32} mean_excess {w.get('regime_mean_excess_pct')}% n{w.get('n')} factor {w.get('factor')}")
print("DONE 2064")
