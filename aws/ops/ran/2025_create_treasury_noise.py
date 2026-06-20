"""ops 2025: create justhodl-treasury-noise via boto3, schedule, invoke, verify."""
import boto3, json, time, io, os, zipfile
REGION="us-east-1"; FN="justhodl-treasury-noise"; B="justhodl-dashboard-live"
ROLE="arn:aws:iam::857687956942:role/lambda-execution-role"
lam=boto3.client("lambda",REGION); events=boto3.client("events",REGION); s3=boto3.client("s3",REGION)
ENV={"Variables":{"FRED_KEY":"2f057499936072679d8843d7fce99989","S3_BUCKET":B}}
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
    lam.update_function_configuration(FunctionName=FN,Environment=ENV,Timeout=180,MemorySize=512,Runtime="python3.12",Handler="lambda_function.lambda_handler")
else:
    print("create"); lam.create_function(FunctionName=FN,Runtime="python3.12",Role=ROLE,Handler="lambda_function.lambda_handler",
        Code={"ZipFile":zb},Timeout=180,MemorySize=512,Environment=ENV,Architectures=["x86_64"],Description="Treasury curve-noise & funding stress")
for _ in range(30):
    c=lam.get_function(FunctionName=FN)["Configuration"]
    if c.get("State")=="Active" and c.get("LastUpdateStatus")=="Successful":break
    time.sleep(4)
arn=c["FunctionArn"];print("active")
rule="justhodl-treasury-noise-daily"
rarn=events.put_rule(Name=rule,ScheduleExpression="cron(30 13 ? * TUE-SAT *)",State="ENABLED",Description="daily treasury-noise")["RuleArn"]
try: lam.add_permission(FunctionName=FN,StatementId="evt-treasury-noise",Action="lambda:InvokeFunction",Principal="events.amazonaws.com",SourceArn=rarn)
except lam.exceptions.ResourceConflictException: pass
events.put_targets(Rule=rule,Targets=[{"Id":"1","Arn":arn}]);print("scheduled")
print("invoking…")
r=lam.invoke(FunctionName=FN,InvocationType="RequestResponse")
print("status:",r["StatusCode"]," payload:",r["Payload"].read().decode()[:450])
time.sleep(2)
d=json.loads(s3.get_object(Bucket=B,Key="data/treasury-noise.json")["Body"].read())
print("\nstress:",d.get("treasury_stress"),"regime:",d.get("regime"),"| noise:",d.get("curve_noise_bps"),"bps pct:",d.get("curve_noise_pctile"),"z:",d.get("curve_noise_z"))
print("bill-SOFR:",d.get("bill_sofr_spread_bps"),"bps | funding stress pct:",d.get("funding_stress_pctile"),"| history pts:",d.get("history_points"))
print("highest noise days:",d.get("highest_noise_days"))
print("DONE 2025")
