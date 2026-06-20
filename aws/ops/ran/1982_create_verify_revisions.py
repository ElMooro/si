import boto3, json, io, zipfile, time, datetime
lam=boto3.client("lambda","us-east-1"); ev=boto3.client("events","us-east-1"); s3=boto3.client("s3","us-east-1")
FN="justhodl-estimate-revisions"; ROLE="arn:aws:iam::857687956942:role/lambda-execution-role"
ACCT="857687956942"; REGION="us-east-1"; BUCKET="justhodl-dashboard-live"; STATE="estimate-revisions/state.json"
buf=io.BytesIO()
with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as z:
    for src,arc in [(f"aws/lambdas/{FN}/source/lambda_function.py","lambda_function.py"),("aws/shared/benzinga.py","benzinga.py")]:
        zi=zipfile.ZipInfo(arc); zi.external_attr=0o644<<16; z.writestr(zi,open(src,"rb").read())
zb=buf.getvalue()
try: lam.get_function(FunctionName=FN); exists=True
except lam.exceptions.ResourceNotFoundException: exists=False
if not exists:
    try:
        lam.create_function(FunctionName=FN,Runtime="python3.12",Role=ROLE,Handler="lambda_function.lambda_handler",
            Code={"ZipFile":zb},Timeout=120,MemorySize=512,Description="estimate-revision momentum")
        print("created",FN)
    except lam.exceptions.ResourceConflictException: exists=True
if exists:
    for _ in range(24):
        try: lam.update_function_code(FunctionName=FN,ZipFile=zb); break
        except lam.exceptions.ResourceConflictException: time.sleep(5)
    print("updated",FN)
for _ in range(30):
    c=lam.get_function_configuration(FunctionName=FN)
    if c.get("State")=="Active" and c.get("LastUpdateStatus")!="InProgress": break
    time.sleep(3)
RULE="justhodl-estimate-revisions-daily"
ev.put_rule(Name=RULE,ScheduleExpression="cron(0 12 * * ? *)",State="ENABLED",Description="daily estimate-revisions")
arn=lam.get_function(FunctionName=FN)["Configuration"]["FunctionArn"]
ev.put_targets(Rule=RULE,Targets=[{"Id":"1","Arn":arn}])
try: lam.remove_permission(FunctionName=FN,StatementId="evt-rev-daily")
except Exception: pass
lam.add_permission(FunctionName=FN,StatementId="evt-rev-daily",Action="lambda:InvokeFunction",
    Principal="events.amazonaws.com",SourceArn=f"arn:aws:events:{REGION}:{ACCT}:rule/{RULE}")
print("scheduled",RULE)

def run(): 
    r=lam.invoke(FunctionName=FN,InvocationType="RequestResponse"); return json.loads(r["Payload"].read())
print("\n--- run #1 (baseline) ---"); print(run())
st=json.loads(s3.get_object(Bucket=BUCKET,Key=STATE)["Body"].read())
ks=[k for k,v in st["keys"].items() if v["obs"] and isinstance(v["obs"][-1][1],(int,float)) and v["obs"][-1][1]][:3]
old=(datetime.date.today()-datetime.timedelta(days=10)).isoformat()
print("\n--- inject synthetic 10d-old snapshot (eps*0.95 → expect +5.26% revision) for:",ks)
for k in ks:
    o=st["keys"][k]["obs"][-1]; st["keys"][k]["obs"]=[[old, round(o[1]*0.95,4), (round(o[2]*0.95,2) if isinstance(o[2],(int,float)) else o[2])]]
s3.put_object(Bucket=BUCKET,Key=STATE,Body=json.dumps(st).encode(),ContentType="application/json")
print("\n--- run #2 (should detect revisions) ---"); print(run())
out=json.loads(s3.get_object(Bucket=BUCKET,Key="data/estimate-revisions.json")["Body"].read())
print("status:",out["status"],"n_with_history:",out["n_with_history"],"n_up:",len(out["upward_revisions"]))
for s in out["upward_revisions"][:5]:
    print(f"  {s['ticker']:<6} eps_rev={s['eps_rev_pct']}% (base {s['baseline_eps_est']}→{s['current_eps_est']}) d2e={s['days_to_earnings']} conf={s['revenue_confirms']}")
inj=[s for s in out["upward_revisions"] if s["ticker"] in [k.split("|")[0] for k in ks]]
print("  injected names detected:",[(s['ticker'],s['eps_rev_pct']) for s in inj])
print("\n--- cleanup: delete state, run #3 for clean real baseline ---")
s3.delete_object(Bucket=BUCKET,Key=STATE); print(run())
print("DONE 1982")
