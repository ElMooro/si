import boto3, json, zipfile, io, glob, time
from botocore.config import Config
from botocore.exceptions import ClientError
lam=boto3.client("lambda","us-east-1",config=Config(read_timeout=150,retries={"max_attempts":0}))
s3=boto3.client("s3","us-east-1"); ev=boto3.client("events","us-east-1")
B="justhodl-dashboard-live"; FN="justhodl-capital-flow-radar"; REGION="us-east-1"; ACCT="857687956942"
ROLE="arn:aws:iam::857687956942:role/lambda-execution-role"; RULE="capital-flow-radar-daily"
src=open(glob.glob("**/justhodl-capital-flow-radar/source/lambda_function.py",recursive=True)[0]).read()
buf=io.BytesIO()
with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as z:
    z.writestr("lambda_function.py",src)
    mp=glob.glob("aws/shared/massive.py")
    if mp: z.writestr("massive.py", open(mp[0]).read()); print("bundled massive.py")
code=buf.getvalue()
try:
    lam.create_function(FunctionName=FN,Runtime="python3.12",Role=ROLE,Handler="lambda_function.lambda_handler",
        Code={"ZipFile":code},Timeout=120,MemorySize=256,Architectures=["x86_64"],
        Description="Institutional Capital Flow Radar")
    print("CREATED")
except ClientError as e:
    if "already exist" in str(e).lower() or "ResourceConflict" in str(e):
        for _ in range(24):
            try: lam.update_function_code(FunctionName=FN,ZipFile=code); print("UPDATED"); break
            except ClientError as e2:
                if "ResourceConflict" in str(e2): time.sleep(5); continue
                raise
    else: raise
for _ in range(40):
    st=lam.get_function_configuration(FunctionName=FN)
    if st.get("State")=="Active" and st.get("LastUpdateStatus")!="InProgress": break
    time.sleep(3)
ev.put_rule(Name=RULE,ScheduleExpression="cron(30 22 * * ? *)",State="ENABLED",Description="Daily 22:30 UTC")
arn=lam.get_function(FunctionName=FN)["Configuration"]["FunctionArn"]
try:
    lam.add_permission(FunctionName=FN,StatementId=RULE+"-evt",Action="lambda:InvokeFunction",
        Principal="events.amazonaws.com",SourceArn="arn:aws:events:%s:%s:rule/%s"%(REGION,ACCT,RULE))
except ClientError as e:
    if "ResourceConflict" not in str(e): print("perm",str(e)[:40])
ev.put_targets(Rule=RULE,Targets=[{"Id":"1","Arn":arn}])
print("SCHEDULED 22:30 UTC")
r=lam.invoke(FunctionName=FN,InvocationType="RequestResponse"); print("INVOKE:",r["Payload"].read().decode()[:160])
time.sleep(2)
d=json.loads(s3.get_object(Bucket=B,Key="data/capital-flow-radar.json")["Body"].read())
print("\nDOLLAR TIDE:",json.dumps(d.get("dollar_tide")))
print("\n>>> SEMICONDUCTORS (your example):")
semi=[c for c in d.get("complexes",[]) if c["complex"]=="Semiconductors"]
if semi: print(json.dumps(semi[0],indent=1)[:900])
print("\nTOP COMPLEXES BY PUMP PROBABILITY:")
for c in d.get("complexes",[])[:8]:
    print("  %-24s pump=%-5s %-38s net5d=$%sM acc=%s breadth=%s z=%s div=%s"%(
        c["complex"],c["pump_probability"],c["regime"][:38],
        round((c["net_flow_5d_usd"] or 0)/1e6,1),c["accelerating"],c["breadth"],c["flow_zscore_90d"],c["flow_price_divergence"]))
print("\nPARTY-OVER ALERTS:",[c["complex"] for c in d.get("party_over_alerts",[])])
print("PUMP SETUPS:",[c["complex"] for c in d.get("pump_setups",[])])
print("CASCADE (sector inflow + options agree):",json.dumps(d.get("top_pick_cascade",[])[:8]))
