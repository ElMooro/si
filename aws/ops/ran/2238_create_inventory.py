import boto3, json, io, zipfile, time
from botocore.config import Config
lam=boto3.client("lambda","us-east-1",config=Config(read_timeout=890,retries={"max_attempts":0}))
s3=boto3.client("s3","us-east-1"); ev=boto3.client("events","us-east-1")
FN="justhodl-inventory-drawdown"; ROLE="arn:aws:iam::857687956942:role/lambda-execution-role"
# FRED key from an existing lambda env (kept out of repo)
fred=lam.get_function_configuration(FunctionName="justhodl-global-liquidity").get("Environment",{}).get("Variables",{}).get("FRED_API_KEY","")
print("FRED key sourced:", bool(fred))
src=open("aws/lambdas/justhodl-inventory-drawdown/source/lambda_function.py","rb").read()
buf=io.BytesIO()
with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as z: z.writestr("lambda_function.py",src)
zb=buf.getvalue()
exists=True
try: lam.get_function(FunctionName=FN)
except lam.exceptions.ResourceNotFoundException: exists=False
if exists:
    lam.update_function_code(FunctionName=FN,ZipFile=zb)
    for _ in range(25):
        if lam.get_function(FunctionName=FN)["Configuration"].get("LastUpdateStatus")=="Successful": break
        time.sleep(3)
    lam.update_function_configuration(FunctionName=FN,Environment={"Variables":{"FRED_API_KEY":fred}},
        MemorySize=512,Timeout=180,Runtime="python3.12",Handler="lambda_function.lambda_handler")
    print("UPDATED existing")
else:
    lam.create_function(FunctionName=FN,Runtime="python3.12",Role=ROLE,Handler="lambda_function.lambda_handler",
        Code={"ZipFile":zb},MemorySize=512,Timeout=180,Environment={"Variables":{"FRED_API_KEY":fred}},
        Publish=True)
    print("CREATED new")
for _ in range(30):
    c=lam.get_function(FunctionName=FN)["Configuration"]
    if c.get("State")=="Active" and c.get("LastUpdateStatus") in ("Successful",None): break
    time.sleep(3)
# schedule
rule="justhodl-inventory-drawdown-weekly"
ev.put_rule(Name=rule,ScheduleExpression="cron(30 23 ? * TUE *)",State="ENABLED",
            Description="Inventory drawdown weekly")
arn=lam.get_function(FunctionName=FN)["Configuration"]["FunctionArn"]
try:
    lam.add_permission(FunctionName=FN,StatementId="inv-drawdown-sched",Action="lambda:InvokeFunction",
        Principal="events.amazonaws.com",SourceArn=f"arn:aws:events:us-east-1:857687956942:rule/{rule}")
except Exception as e: print("perm:",str(e)[:50])
ev.put_targets(Rule=rule,Targets=[{"Id":"1","Arn":arn}])
print("scheduled")
# invoke + verify
r=lam.invoke(FunctionName=FN,InvocationType="RequestResponse")
print("invoke status:",r["StatusCode"],"payload:",r["Payload"].read().decode()[:200])
d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/inventory-drawdown.json")["Body"].read())
print("counts:",json.dumps(d.get("counts")))
print("\nSECTOR DRAWDOWN (falling I/S = drawing down):")
for s in d.get("sector_drawdown",[])[:9]:
    print(f"  {s['sector']:<24} ratio={s['latest_ratio']} 6m={s['chg_6m']}% pctl5y={s['percentile_5y']} score={s['drawdown_score']} [{s['flag']}]")
print("\nBOOM SETUPS (DIO falling into rising demand):")
for r in d.get("boom_setups",[])[:10]:
    print(f"  {r['ticker']:<6} DIO {r['dio_4q_ago']}->{r['dio_latest']} ({r['dio_chg_pct']}%) rev={r['rev_growth_yoy']}% boom={r['boom_score']} | {r.get('industry')}")
print("\nTOP DRAWDOWN BOARD (any):")
for r in d.get("stock_drawdown_board",[])[:8]:
    print(f"  {r['ticker']:<6} [{r['classification']}] DIO {r['dio_chg_pct']}% rev={r['rev_growth_yoy']}% boom={r['boom_score']}")
print("signals_logged:",d.get("signals_logged"))
print("DONE 2238")
