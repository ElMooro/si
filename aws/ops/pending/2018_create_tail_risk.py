"""ops 2018: create justhodl-tail-risk via boto3, schedule, invoke, verify."""
import boto3, json, time, io, os, zipfile
REGION="us-east-1"; FN="justhodl-tail-risk"; B="justhodl-dashboard-live"
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
    lam.update_function_configuration(FunctionName=FN,Environment=ENV,Timeout=180,MemorySize=512,Runtime="python3.12",Handler="lambda_function.lambda_handler")
else:
    print("create"); lam.create_function(FunctionName=FN,Runtime="python3.12",Role=ROLE,Handler="lambda_function.lambda_handler",
        Code={"ZipFile":zb},Timeout=180,MemorySize=512,Environment=ENV,Architectures=["x86_64"],
        Description="Option-implied crash probability & tail index")
for _ in range(30):
    c=lam.get_function(FunctionName=FN)["Configuration"]
    if c.get("State")=="Active" and c.get("LastUpdateStatus")=="Successful":break
    time.sleep(4)
arn=c["FunctionArn"];print("active")
rule="justhodl-tail-risk-daily"
rarn=events.put_rule(Name=rule,ScheduleExpression="cron(0 13 ? * TUE-SAT *)",State="ENABLED",Description="daily tail-risk")["RuleArn"]
try: lam.add_permission(FunctionName=FN,StatementId="evt-tail-risk",Action="lambda:InvokeFunction",Principal="events.amazonaws.com",SourceArn=rarn)
except lam.exceptions.ResourceConflictException: pass
events.put_targets(Rule=rule,Targets=[{"Id":"1","Arn":arn}]); print("scheduled")
print("invoking…")
r=lam.invoke(FunctionName=FN,InvocationType="RequestResponse")
print("status:",r["StatusCode"]," payload:",r["Payload"].read().decode()[:600])
time.sleep(2)
d=json.loads(s3.get_object(Bucket=B,Key="data/tail-risk.json")["Body"].read())
print("\nsystem_tail:",d.get("system_tail_gauge"),"regime:",d.get("tail_regime"),"valuation:",d.get("tail_valuation"))
for r in d.get("indices",[]):
    print(f"  {r['ticker']}: stress={r.get('tail_stress')} ATM_IV={r.get('atm_iv')} put10={r.get('put10_iv')} skew_slope={r.get('put_skew_slope')} RR25={r.get('risk_reversal_25')} RRterm={r.get('rr_term_slope')} P(drop10%)={r.get('p_drop_10')} P(20%)={r.get('p_drop_20')} RNskew={r.get('rn_skew')} SKEWidx={r.get('skew_index')}")
print("DONE 2018")
