import boto3, json, zipfile, io, glob, time
from botocore.config import Config
from botocore.exceptions import ClientError
lam=boto3.client("lambda","us-east-1",config=Config(read_timeout=240,retries={"max_attempts":0}))
s3=boto3.client("s3","us-east-1"); events=boto3.client("events","us-east-1")
B="justhodl-dashboard-live"; FN="justhodl-theme-rotation"; REGION="us-east-1"; ACCT="857687956942"
ROLE="arn:aws:iam::857687956942:role/lambda-execution-role"; RULE="theme-rotation-daily"
src=open(glob.glob("**/justhodl-theme-rotation/source/lambda_function.py",recursive=True)[0]).read()
buf=io.BytesIO()
with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as z: z.writestr("lambda_function.py",src)
code=buf.getvalue()
try:
    lam.create_function(FunctionName=FN,Runtime="python3.12",Role=ROLE,Handler="lambda_function.lambda_handler",
        Code={"ZipFile":code},Timeout=240,MemorySize=512,Architectures=["x86_64"],Description="Cross-theme rotation RRG")
    print("CREATED")
except ClientError as e:
    if "already exist" in str(e).lower() or "ResourceConflict" in str(e):
        for _ in range(24):
            try: lam.update_function_code(FunctionName=FN,ZipFile=code); print("UPDATED"); break
            except ClientError as e2:
                if "ResourceConflict" in str(e2): time.sleep(5); continue
                raise
    else: raise
for _ in range(60):
    st=lam.get_function_configuration(FunctionName=FN)
    if st.get("State")=="Active" and st.get("LastUpdateStatus")!="InProgress": break
    time.sleep(3)
events.put_rule(Name=RULE,ScheduleExpression="cron(30 13 * * ? *)",State="ENABLED",Description="Daily theme rotation 13:30 UTC")
arn=lam.get_function(FunctionName=FN)["Configuration"]["FunctionArn"]
try:
    lam.add_permission(FunctionName=FN,StatementId="theme-rotation-daily-evt",Action="lambda:InvokeFunction",
        Principal="events.amazonaws.com",SourceArn="arn:aws:events:%s:%s:rule/%s"%(REGION,ACCT,RULE))
except ClientError as e:
    if "ResourceConflict" not in str(e): print("perm:",str(e)[:50])
events.put_targets(Rule=RULE,Targets=[{"Id":"1","Arn":arn}])
print("SCHEDULED 13:30 UTC")
r=lam.invoke(FunctionName=FN,InvocationType="RequestResponse"); print("INVOKE:",r["Payload"].read().decode()[:160])
time.sleep(2)
d=json.loads(s3.get_object(Bucket=B,Key="data/theme-rotation.json")["Body"].read()); sm=d["summary"]
print("themes=%s axis=%sd counts=%s"%(d.get("n_themes"),d.get("axis_days"),sm.get("quadrant_counts")))
print("\nROTATING IN (early money):")
for r in (sm.get("rotating_in") or [])[:8]:
    print("  %-30s %-10s rs_ratio=%-7s rs_mom=%-7s score=%s"%(r["theme"],r["quadrant"],r["rs_ratio"],r["rs_momentum"],r["rotation_score"]))
print("\nLEADING:", [(r["theme"],r["rotation_score"]) for r in (sm.get("leading") or [])][:6])
print("WEAKENING (rotating out):", [r["theme"] for r in (sm.get("weakening") or [])][:6])
print("harvester picks (rotating-in seeds):", [p["symbol"] for p in (sm.get("top_picks") or [])][:14])
