import boto3, json, time, io, zipfile
from botocore.config import Config
lam=boto3.client("lambda","us-east-1",config=Config(read_timeout=300,retries={"max_attempts":0}))
ev=boto3.client("events","us-east-1"); s3=boto3.client("s3","us-east-1")
FN="justhodl-alpha-decay"; ROLE="arn:aws:iam::857687956942:role/lambda-execution-role"
try: lam.get_function(FunctionName=FN); print("exists (deploy pipeline created)")
except lam.exceptions.ResourceNotFoundException:
    print("create via boto3")
    buf=io.BytesIO()
    with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as z:
        z.writestr("lambda_function.py",open("aws/lambdas/justhodl-alpha-decay/source/lambda_function.py").read())
    lam.create_function(FunctionName=FN,Runtime="python3.12",Role=ROLE,Handler="lambda_function.lambda_handler",
        Code={"ZipFile":buf.getvalue()},Timeout=120,MemorySize=256,Architectures=["x86_64"])
    for _ in range(20):
        if lam.get_function(FunctionName=FN)["Configuration"].get("State")=="Active": break
        time.sleep(3)
    rule="justhodl-alpha-decay-daily"
    ev.put_rule(Name=rule,ScheduleExpression="cron(45 13 * * ? *)",State="ENABLED")
    try: lam.add_permission(FunctionName=FN,StatementId="ev-"+rule,Action="lambda:InvokeFunction",Principal="events.amazonaws.com",SourceArn=f"arn:aws:events:us-east-1:857687956942:rule/{rule}")
    except Exception as e: print("perm",str(e)[:40])
    ev.put_targets(Rule=rule,Targets=[{"Id":"1","Arn":lam.get_function(FunctionName=FN)["Configuration"]["FunctionArn"]}])
    print("created+scheduled")
for _ in range(25):
    c=lam.get_function(FunctionName=FN)["Configuration"]
    if c.get("LastUpdateStatus")=="Successful" and c.get("State")=="Active": break
    time.sleep(3)
r=lam.invoke(FunctionName=FN,InvocationType="RequestResponse")
print("invoke:",r["Payload"].read().decode()[:200])
d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/alpha-decay.json")["Body"].read())
print("stats:",json.dumps(d["stats"]),"| regime:",d.get("current_regime"),"| baseline_ready:",d.get("baseline_ready"))
print("\nPROVEN-ENGINE GUARD:")
for p in d.get("proven_engine_guard",[]):
    print(f"   {p['signal']:<26} excess={p['live_excess_vs_spy_pct']}% hit={round((p['hit_rate'] or 0)*100,1)}% worst_regime={p.get('worst_regime')}({p.get('worst_regime_excess_pct')}) -> {p['status']}")
print("\nBACKTEST-vs-LIVE:",len(d.get("backtest_vs_live",[])),"deployable archetypes")
for g in d.get("backtest_vs_live",[])[:6]:
    print(f"   {g['archetype']:<22} gateSR={g.get('backtest_gate_sharpe')} live={g.get('mapped_live_signal')} [{g['verdict']}]")
print("DONE 2136")
