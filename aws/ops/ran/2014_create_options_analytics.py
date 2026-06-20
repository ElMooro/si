"""ops 2014: create justhodl-options-analytics via boto3 (new dir no-op), schedule, invoke, verify."""
import boto3, json, time, io, os, zipfile
REGION="us-east-1"; FN="justhodl-options-analytics"; B="justhodl-dashboard-live"
ROLE="arn:aws:iam::857687956942:role/lambda-execution-role"
lam=boto3.client("lambda",REGION); events=boto3.client("events",REGION); s3=boto3.client("s3",REGION)
ENV={"Variables":{"POLYGON_KEY":"zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d","S3_BUCKET":B}}
SRC=f"aws/lambdas/{FN}/source"; buf=io.BytesIO()
with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as z:
    for r,_,fs in os.walk(SRC):
        for f in fs:
            if f.endswith(".py"): p=os.path.join(r,f); z.write(p,os.path.relpath(p,SRC))
zb=buf.getvalue()
try:
    lam.get_function(FunctionName=FN); exists=True
except lam.exceptions.ResourceNotFoundException: exists=False
if exists:
    print("update"); lam.update_function_code(FunctionName=FN,ZipFile=zb)
    for _ in range(24):
        if lam.get_function(FunctionName=FN)["Configuration"].get("LastUpdateStatus")!="InProgress": break
        time.sleep(4)
    lam.update_function_configuration(FunctionName=FN,Environment=ENV,Timeout=300,MemorySize=1024,Runtime="python3.12",Handler="lambda_function.lambda_handler")
else:
    print("create"); lam.create_function(FunctionName=FN,Runtime="python3.12",Role=ROLE,
        Handler="lambda_function.lambda_handler",Code={"ZipFile":zb},Timeout=300,MemorySize=1024,
        Environment=ENV,Architectures=["x86_64"],Description="Per-name options analytics (GEX/IV/skew/unusual)")
for _ in range(30):
    c=lam.get_function(FunctionName=FN)["Configuration"]
    if c.get("State")=="Active" and c.get("LastUpdateStatus")=="Successful": break
    time.sleep(4)
arn=c["FunctionArn"]; print("active:",arn)
rule="justhodl-options-analytics-daily"
rarn=events.put_rule(Name=rule,ScheduleExpression="cron(0 13 ? * TUE-SAT *)",State="ENABLED",Description="daily options analytics")["RuleArn"]
try: lam.add_permission(FunctionName=FN,StatementId="evt-options-analytics",Action="lambda:InvokeFunction",Principal="events.amazonaws.com",SourceArn=rarn)
except lam.exceptions.ResourceConflictException: pass
events.put_targets(Rule=rule,Targets=[{"Id":"1","Arn":arn}])
print("scheduled:",rule)
print("invoking (chains for ~58 names, ~60-120s)…")
r=lam.invoke(FunctionName=FN,InvocationType="RequestResponse")
print("status:",r["StatusCode"]," payload:",r["Payload"].read().decode()[:500])
time.sleep(2)
d=json.loads(s3.get_object(Bucket=B,Key="data/options-analytics.json")["Body"].read())
print("\nok:",d.get("ok"),"| analyzed:",d.get("n_analyzed"),"| dist:",d.get("distribution"),"| picks:",len(d.get("top_picks") or []))
print("\nTOP BOARD (by ignition score):")
for r in (d.get("board") or [])[:12]:
    print(f"  {r['ticker']:<6} sc={r['score']:<5} {r['signal']:<20} {r['gamma_regime']:<11} GEX={r['net_gex_musd_per_1pct']:>8}M flip={r.get('gamma_flip_strike')} ATM_IV={r.get('atm_iv_front')} skew={r.get('skew_25d')} pcr={r.get('pcr_vol')} unu={r.get('n_unusual')}")
print("\nSQUEEZE SETUPS:",[(r['ticker'],r['signal'],r['net_gex_musd_per_1pct']) for r in d.get('squeeze_setups',[])[:8]])
print("PICKS:",[(p['ticker'],p['score'],p['signal']) for p in d.get('top_picks',[])])
print("MOST UNUSUAL:",[(r['ticker'],r['n_unusual'],r['pcr_vol']) for r in d.get('most_unusual',[])[:6]])
print("RICHEST IV(VRP):",[(r['ticker'],r['vrp']) for r in d.get('richest_iv_vrp',[])[:6]])
print("DONE 2014")
