import boto3, json, io, zipfile, time
lam=boto3.client("lambda","us-east-1"); ev=boto3.client("events","us-east-1")
FN="justhodl-crypto-confluence"
SRC=open("aws/lambdas/justhodl-crypto-confluence/source/lambda_function.py").read()
def zipsrc():
    b=io.BytesIO()
    with zipfile.ZipFile(b,"w",zipfile.ZIP_DEFLATED) as z: z.writestr("lambda_function.py",SRC)
    return b.getvalue()
try:
    lam.get_function(FunctionName=FN); print("exists via deploy"); lam.update_function_code(FunctionName=FN,ZipFile=zipsrc())
except lam.exceptions.ResourceNotFoundException:
    print("brand-new no-op -> boto3 create")
    lam.create_function(FunctionName=FN,Runtime="python3.12",Handler="lambda_function.lambda_handler",
        Role="arn:aws:iam::857687956942:role/lambda-execution-role",Code={"ZipFile":zipsrc()},Timeout=180,MemorySize=512)
for _ in range(40):
    c=lam.get_function(FunctionName=FN)["Configuration"]
    if c.get("State")=="Active" and c.get("LastUpdateStatus") in ("Successful",None): break
    time.sleep(3)
RULE="justhodl-crypto-confluence-daily"
try:
    ev.put_rule(Name=RULE,ScheduleExpression="cron(20 23 * * ? *)",State="ENABLED",Description="Daily crypto synthesizer")
    arn=lam.get_function(FunctionName=FN)["Configuration"]["FunctionArn"]
    ev.put_targets(Rule=RULE,Targets=[{"Id":"1","Arn":arn}])
    try: lam.add_permission(FunctionName=FN,StatementId="evt-cryptoconf",Action="lambda:InvokeFunction",Principal="events.amazonaws.com",SourceArn=f"arn:aws:events:us-east-1:857687956942:rule/{RULE}")
    except Exception as e: print("perm:",str(e)[:30])
    print("schedule OK")
except Exception as e: print("schedule FAIL:",str(e)[:70])
lam.invoke(FunctionName=FN,InvocationType="RequestResponse")
d=json.loads(boto3.client("s3","us-east-1").get_object(Bucket="justhodl-dashboard-live",Key="data/crypto-confluence.json")["Body"].read())
print("counts:",json.dumps(d.get("counts")))
print("sources ok:",[s["engine"] for s in d.get("sources_bull",[]) if s.get("ok")])
ctx=d.get("market_context",{})
print(f"market_context: regime={ctx.get('regime')} tilt={ctx.get('tilt')} liq={ctx.get('liquidity')} dump_risk={ctx.get('dump_risk')} stablecoin={ctx.get('stablecoin_flow')}")
for r in (d.get("confluence_book") or [])[:8]:
    print(f"  {r['coin']:<6} dims={r['n_dimensions']} {r['dimensions']} comp {r['composite']}")
print("DONE 2213")
