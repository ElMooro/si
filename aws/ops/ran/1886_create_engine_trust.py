import boto3, json, zipfile, io, glob, time
from botocore.exceptions import ClientError
lam=boto3.client("lambda","us-east-1"); s3=boto3.client("s3","us-east-1"); events=boto3.client("events","us-east-1")
B="justhodl-dashboard-live"; FN="justhodl-engine-trust"; REGION="us-east-1"; ACCT="857687956942"
ROLE="arn:aws:iam::857687956942:role/lambda-execution-role"; RULE="engine-trust-daily"
src=open(glob.glob("**/justhodl-engine-trust/source/lambda_function.py",recursive=True)[0]).read()
buf=io.BytesIO()
with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as z: z.writestr("lambda_function.py",src)
code=buf.getvalue()
try:
    lam.create_function(FunctionName=FN,Runtime="python3.12",Role=ROLE,Handler="lambda_function.lambda_handler",
        Code={"ZipFile":code},Timeout=120,MemorySize=256,Architectures=["x86_64"],
        Description="Auto-demotion gate: regime-conditioned trust registry from scorecard")
    print("CREATED")
except ClientError as e:
    if "already exist" in str(e).lower() or "ResourceConflict" in str(e):
        for _ in range(24):
            try: lam.update_function_code(FunctionName=FN,ZipFile=code); print("UPDATED"); break
            except ClientError as e2:
                if "ResourceConflict" in str(e2): time.sleep(5); continue
                raise
    else: raise
for _ in range(50):
    st=lam.get_function_configuration(FunctionName=FN)
    if st.get("State")=="Active" and st.get("LastUpdateStatus")!="InProgress": break
    time.sleep(3)
events.put_rule(Name=RULE,ScheduleExpression="cron(30 12 * * ? *)",State="ENABLED",Description="Daily trust-gate 12:30 UTC")
arn=lam.get_function(FunctionName=FN)["Configuration"]["FunctionArn"]
try:
    lam.add_permission(FunctionName=FN,StatementId="engine-trust-daily-evt",Action="lambda:InvokeFunction",
        Principal="events.amazonaws.com",SourceArn="arn:aws:events:%s:%s:rule/%s"%(REGION,ACCT,RULE))
except ClientError as e:
    if "ResourceConflict" not in str(e): print("perm:",str(e)[:50])
events.put_targets(Rule=RULE,Targets=[{"Id":"1","Arn":arn}])
print("SCHEDULED 12:30 UTC")
r=lam.invoke(FunctionName=FN,InvocationType="RequestResponse"); print("INVOKE:",r["Payload"].read().decode()[:240])
time.sleep(2)
d=json.loads(s3.get_object(Bucket=B,Key="data/engine-trust.json")["Body"].read())
print("\nREGISTRY: engines=%s harvested=%s regime=%s"%(d.get("n_engines"),d.get("n_harvested_engines"),d.get("current_regime")))
print("counts:",d.get("counts"))
print("\nMost-trusted right now (matured + above 0.55 LB):")
for e in (d.get("trusted") or [])[:6]:
    print("  %-26s status=%-9s n=%-4s LB=%-6s eff_trust=%s"%(e["signal_type"],e["status"],e["n_scored"],e["wilson_lb"],e["effective_trust"]))
if not d.get("trusted"): print("  (none yet — fleet still WARMING; gate is correctly a no-op until outcomes mature)")
print("\nDemoted (proven below coinflip):", [e["signal_type"] for e in (d.get("demoted") or [])][:8] or "(none yet)")
