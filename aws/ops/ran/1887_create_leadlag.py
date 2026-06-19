import boto3, json, zipfile, io, glob, time
from botocore.exceptions import ClientError
lam=boto3.client("lambda","us-east-1"); s3=boto3.client("s3","us-east-1"); events=boto3.client("events","us-east-1")
B="justhodl-dashboard-live"; FN="justhodl-leadlag-graph"; REGION="us-east-1"; ACCT="857687956942"
ROLE="arn:aws:iam::857687956942:role/lambda-execution-role"; RULE="leadlag-graph-daily"
src=open(glob.glob("**/justhodl-leadlag-graph/source/lambda_function.py",recursive=True)[0]).read()
buf=io.BytesIO()
with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as z: z.writestr("lambda_function.py",src)
code=buf.getvalue()
try:
    lam.create_function(FunctionName=FN,Runtime="python3.12",Role=ROLE,Handler="lambda_function.lambda_handler",
        Code={"ZipFile":code},Timeout=300,MemorySize=512,Architectures=["x86_64"],
        Description="Lead-lag causal graph — who moves before whom")
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
events.put_rule(Name=RULE,ScheduleExpression="cron(0 13 * * ? *)",State="ENABLED",Description="Daily lead-lag 13:00 UTC")
arn=lam.get_function(FunctionName=FN)["Configuration"]["FunctionArn"]
try:
    lam.add_permission(FunctionName=FN,StatementId="leadlag-graph-daily-evt",Action="lambda:InvokeFunction",
        Principal="events.amazonaws.com",SourceArn="arn:aws:events:%s:%s:rule/%s"%(REGION,ACCT,RULE))
except ClientError as e:
    if "ResourceConflict" not in str(e): print("perm:",str(e)[:50])
events.put_targets(Rule=RULE,Targets=[{"Id":"1","Arn":arn}])
print("SCHEDULED 13:00 UTC")
r=lam.invoke(FunctionName=FN,InvocationType="RequestResponse"); print("INVOKE:",r["Payload"].read().decode()[:260])
time.sleep(2)
d=json.loads(s3.get_object(Bucket=B,Key="data/lead-lag-graph.json")["Body"].read())
if d.get("error"): print("ERROR:",d); raise SystemExit
print("\nGRAPH: universe=%s with_history=%s nodes=%s edges=%s axis=%sd"%(d.get("n_universe"),d.get("n_with_history"),d.get("n_nodes"),d.get("n_edges"),d.get("axis_days")))
print("\nTOP LEADERS (move before others):")
for n in (d.get("top_leaders") or [])[:8]:
    print("  %-7s out=%-3d in=%-3d lead_strength=%-6s recent2d=%s%%"%(n["symbol"],n["out_degree"],n["in_degree"],n["lead_strength"],n["recent_2d_pct"]))
print("\nSTRONGEST EDGES (A leads B):")
for e in (d.get("edges") or [])[:8]:
    print("  %-7s -> %-7s lag=%dd corr=%.2f asym=%.2f"%(e["from"],e["to"],e["lag_days"],e["lead_corr"],e["asymmetry"]))
print("\nLIVE PREDICTIONS:")
for p in (d.get("live_predictions") or [])[:5]:
    fl=", ".join("%s(%s,%dd)"%(f["symbol"],f["expected_dir"],f["lag_days"]) for f in p["followers_expected"][:4])
    print("  %s moved %s%% 2d -> expect: %s"%(p["leader"],p["leader_move_2d_pct"],fl))
if not d.get("live_predictions"): print("  (no leader made a threshold move in the last 2d — quiet tape)")
