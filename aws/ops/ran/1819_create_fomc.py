import io, json, zipfile, os, time, boto3
from botocore.config import Config
REGION="us-east-1"; FN="justhodl-fomc-reaction"
ROLE="arn:aws:iam::857687956942:role/lambda-execution-role"
lam=boto3.client("lambda",region_name=REGION,config=Config(read_timeout=300,retries={"max_attempts":0}))
ev=boto3.client("events",region_name=REGION); s3=boto3.client("s3",region_name=REGION); B="justhodl-dashboard-live"

# 1) build zip: shared/*.py + source/* (source wins)
buf=io.BytesIO()
with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as z:
    added=set()
    src="aws/lambdas/%s/source"%FN
    for root,_,files in os.walk(src):
        for f in files:
            if f.endswith(".pyc"): continue
            p=os.path.join(root,f); arc=os.path.relpath(p,src); z.write(p,arc); added.add(arc)
    for f in os.listdir("aws/shared"):
        if f.endswith(".py") and f not in added: z.write(os.path.join("aws/shared",f),f)
zip_bytes=buf.getvalue(); print("zip bytes:",len(zip_bytes))

# 2) env from buyback-scanner (standard secrets bundle incl ANTHROPIC)
env={}
try:
    env=lam.get_function_configuration(FunctionName="justhodl-buyback-scanner").get("Environment",{}).get("Variables",{})
    print("inherited env keys:",sorted(env.keys()))
except Exception as e: print("env inherit failed:",e)
env["S3_BUCKET"]="justhodl-dashboard-live"

# 3) create or update
try:
    lam.get_function(FunctionName=FN); exists=True
except lam.exceptions.ResourceNotFoundException: exists=False
if exists:
    lam.update_function_code(FunctionName=FN,ZipFile=zip_bytes); 
    waiter=lam.get_waiter("function_updated"); waiter.wait(FunctionName=FN)
    lam.update_function_configuration(FunctionName=FN,Runtime="python3.12",Handler="lambda_function.lambda_handler",
        Timeout=300,MemorySize=512,Environment={"Variables":env})
    print("UPDATED existing")
else:
    lam.create_function(FunctionName=FN,Runtime="python3.12",Role=ROLE,Handler="lambda_function.lambda_handler",
        Code={"ZipFile":zip_bytes},Timeout=300,MemorySize=512,Architectures=["x86_64"],
        Environment={"Variables":env},Description="FOMC decision-day Reaction Map (fusion engine)")
    print("CREATED new function")
lam.get_waiter("function_active_v2").wait(FunctionName=FN)

# 4) EventBridge daily schedule 21:35 UTC
try:
    rule=ev.put_rule(Name="fomc-reaction-daily",ScheduleExpression="cron(35 21 * * ? *)",State="ENABLED",
        Description="FOMC reaction map daily post-close")["RuleArn"]
    arn=lam.get_function(FunctionName=FN)["Configuration"]["FunctionArn"]
    try: lam.add_permission(FunctionName=FN,StatementId="fomc-reaction-daily-evt",Action="lambda:InvokeFunction",
            Principal="events.amazonaws.com",SourceArn=rule)
    except lam.exceptions.ResourceConflictException: pass
    ev.put_targets(Rule="fomc-reaction-daily",Targets=[{"Id":"1","Arn":arn}])
    print("schedule wired:",rule)
except Exception as e: print("schedule err:",e)

# 5) run it
r=lam.invoke(FunctionName=FN,InvocationType="RequestResponse")
print("invoke:",r["Payload"].read().decode()[:200])
d=json.loads(s3.get_object(Bucket=B,Key="data/fomc-reaction.json")["Body"].read())
print("\nmeeting:",d["meeting_date"],"decision_day:",d["is_decision_day"])
print("SURPRISE:",d["surprise"]["label"],"Δ2y_bp:",d["surprise"]["d2y_bp"],"tone:",d["surprise"]["statement_tone"])
print("calib:",d["calibration"]["events_by_sign"],"n=",d["calibration"]["n_events"])
print("\nREACTION MAP ("+d["surprise"]["label"]+"):")
for k,v in d["reaction_map"].items():
    s=v.get("short") or {}; l=v.get("long") or {}
    su="—" if not s else f"{s['median']:+g}{v['unit']} [{s['p25']:+g}..{s['p75']:+g}] up{s['prob_up_pct']}% n{s['n']}"
    lu="—" if not l else f"{l['median']:+g}{v['unit']} [{l['p25']:+g}..{l['p75']:+g}] up{l['prob_up_pct']}% n{l['n']}"
    print(f"  {k:24} 5d {su:44} 63d {lu}")
