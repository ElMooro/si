import io, json, zipfile, time, boto3
REGION="us-east-1"; ACCT="857687956942"
FN="justhodl-refining-stress"
ROLE="arn:aws:iam::%s:role/lambda-execution-role"%ACCT
SRC="aws/lambdas/justhodl-refining-stress/source/lambda_function.py"
lam=boto3.client("lambda",region_name=REGION); ev=boto3.client("events",region_name=REGION); s3=boto3.client("s3",region_name=REGION)

buf=io.BytesIO()
with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as z:
    z.write(SRC, "lambda_function.py")
code=buf.getvalue()
env={"Variables":{"EIA_API_KEY":"trvQDpg2GdvBixLeieVMyaQwsnkFQlYSuecVm4Pl","DASH_BUCKET":"justhodl-dashboard-live"}}

try:
    r=lam.create_function(FunctionName=FN, Runtime="python3.12", Role=ROLE,
        Handler="lambda_function.lambda_handler", Code={"ZipFile":code},
        Timeout=120, MemorySize=256, Architectures=["x86_64"], Environment=env,
        Description="Refining-margin / physical-energy stress (crack spreads + Cushing, EIA v2)")
    print("created:", r["FunctionArn"])
except lam.exceptions.ResourceConflictException:
    lam.update_function_code(FunctionName=FN, ZipFile=code); print("exists -> code updated")
    waiter=lam.get_waiter("function_updated"); waiter.wait(FunctionName=FN)
    lam.update_function_configuration(FunctionName=FN, Timeout=120, MemorySize=256, Environment=env)

# wait active
for _ in range(30):
    c=lam.get_function_configuration(FunctionName=FN)
    if c.get("State")=="Active" and c.get("LastUpdateStatus")!="InProgress": break
    time.sleep(2)
farn=lam.get_function_configuration(FunctionName=FN)["FunctionArn"]

# schedule EventBridge daily 13:20 UTC
RULE="justhodl-refining-stress-daily"
rarn=ev.put_rule(Name=RULE, ScheduleExpression="cron(20 13 * * ? *)", State="ENABLED",
                 Description="Daily refining-margin / physical-energy refresh")["RuleArn"]
try:
    lam.add_permission(FunctionName=FN, StatementId="evt-"+RULE, Action="lambda:InvokeFunction",
                       Principal="events.amazonaws.com", SourceArn=rarn)
except lam.exceptions.ResourceConflictException: pass
ev.put_targets(Rule=RULE, Targets=[{"Id":"1","Arn":farn}])
print("scheduled:", rarn)

# invoke + verify
inv=lam.invoke(FunctionName=FN, InvocationType="RequestResponse")
print("invoke:", inv["Payload"].read().decode()[:300])
time.sleep(2)
d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/refining-stress.json")["Body"].read())
print("REGIME=%s  | %s"%(d.get("regime"), d.get("summary")))
print("errors:", d.get("errors"))
for m in d.get("metrics",[]):
    print("  [%s] %-38s = %s%s  (pctile %s)"%(m["status"],m["label"][:38],m["value"],m["unit"],m.get("percentile")))
