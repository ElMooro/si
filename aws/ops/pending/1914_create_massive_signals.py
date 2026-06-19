import boto3, json, zipfile, io, glob, time
from botocore.config import Config
from botocore.exceptions import ClientError
lam=boto3.client("lambda","us-east-1",config=Config(read_timeout=150,retries={"max_attempts":0}))
s3=boto3.client("s3","us-east-1"); ev=boto3.client("events","us-east-1")
B="justhodl-dashboard-live"; FN="justhodl-massive-signals"; REGION="us-east-1"; ACCT="857687956942"
ROLE="arn:aws:iam::857687956942:role/lambda-execution-role"; RULE="massive-signals-daily"
src=open(glob.glob("**/justhodl-massive-signals/source/lambda_function.py",recursive=True)[0]).read()
buf=io.BytesIO()
with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as z: z.writestr("lambda_function.py",src)
code=buf.getvalue()
try:
    lam.create_function(FunctionName=FN,Runtime="python3.12",Role=ROLE,Handler="lambda_function.lambda_handler",
        Code={"ZipFile":code},Timeout=120,MemorySize=256,Architectures=["x86_64"],Description="Unified Massive-data layer")
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
ev.put_rule(Name=RULE,ScheduleExpression="cron(0 22 * * ? *)",State="ENABLED",Description="Daily 22:00 UTC")
arn=lam.get_function(FunctionName=FN)["Configuration"]["FunctionArn"]
try:
    lam.add_permission(FunctionName=FN,StatementId=RULE+"-evt",Action="lambda:InvokeFunction",
        Principal="events.amazonaws.com",SourceArn="arn:aws:events:%s:%s:rule/%s"%(REGION,ACCT,RULE))
except ClientError as e:
    if "ResourceConflict" not in str(e): print("perm",str(e)[:40])
ev.put_targets(Rule=RULE,Targets=[{"Id":"1","Arn":arn}])
print("SCHEDULED 22:00 UTC")
r=lam.invoke(FunctionName=FN,InvocationType="RequestResponse"); print("INVOKE:",r["Payload"].read().decode()[:160])
time.sleep(2)
d=json.loads(s3.get_object(Bucket=B,Key="data/massive-signals.json")["Body"].read()); m=d["market"]
print("\nMARKET: gamma_regime=%s smallcap_bid=%s(IWM %s) strongest_in=%s strongest_out=%s"%(
    m.get("gamma_regime"),m.get("smallcap_bid"),m.get("iwm_flow_z"),m.get("strongest_inflow_sector"),m.get("strongest_outflow_sector")))
print("sector_flows:",{k:v for k,v in (m.get("sector_flows") or {}).items()})
print("fx_signals:",m.get("fx_signals"))
print("futures_signals:",m.get("futures_signals"))
print("\nTOP PRE-PUMP (gamma + flow):")
for r in (d.get("top_prepump") or [])[:10]:
    print("  %-6s score=%-6s | %s"%(r["symbol"],r.get("prepump_score"),(r.get("massive_why") or "")[:60]))
print("n_tickers=%s"%d.get("n_tickers"))
