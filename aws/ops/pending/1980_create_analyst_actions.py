import boto3, json, io, zipfile, time
lam=boto3.client("lambda","us-east-1"); ev=boto3.client("events","us-east-1"); s3=boto3.client("s3","us-east-1")
FN="justhodl-analyst-actions"; ROLE="arn:aws:iam::857687956942:role/lambda-execution-role"
ACCT="857687956942"; REGION="us-east-1"
# bundle engine + shared benzinga.py flat
buf=io.BytesIO()
with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as z:
    for src,arc in [(f"aws/lambdas/{FN}/source/lambda_function.py","lambda_function.py"),
                    ("aws/shared/benzinga.py","benzinga.py")]:
        zi=zipfile.ZipInfo(arc); zi.external_attr=0o644<<16
        z.writestr(zi,open(src,"rb").read())
zb=buf.getvalue()
# idempotent create-or-update
try:
    lam.get_function(FunctionName=FN); exists=True
except lam.exceptions.ResourceNotFoundException: exists=False
if not exists:
    try:
        lam.create_function(FunctionName=FN,Runtime="python3.12",Role=ROLE,
            Handler="lambda_function.lambda_handler",Code={"ZipFile":zb},
            Timeout=120,MemorySize=512,
            Description="Benzinga analyst ratings/PT/guidance signal board",
            Environment={"Variables":{}})
        print("created",FN)
    except lam.exceptions.ResourceConflictException:
        exists=True
if exists:
    for _ in range(24):
        try: lam.update_function_code(FunctionName=FN,ZipFile=zb); break
        except lam.exceptions.ResourceConflictException: time.sleep(5)
    print("updated",FN)
for _ in range(30):
    c=lam.get_function_configuration(FunctionName=FN)
    if c.get("State")=="Active" and c.get("LastUpdateStatus")!="InProgress": break
    time.sleep(3)
# daily schedule 13:45 UTC
RULE="justhodl-analyst-actions-daily"
ev.put_rule(Name=RULE,ScheduleExpression="cron(45 13 * * ? *)",State="ENABLED",
    Description="daily analyst-actions engine")
arn=lam.get_function(FunctionName=FN)["Configuration"]["FunctionArn"]
ev.put_targets(Rule=RULE,Targets=[{"Id":"1","Arn":arn}])
try: lam.remove_permission(FunctionName=FN,StatementId="evt-analyst-daily")
except Exception: pass
lam.add_permission(FunctionName=FN,StatementId="evt-analyst-daily",
    Action="lambda:InvokeFunction",Principal="events.amazonaws.com",
    SourceArn=f"arn:aws:events:{REGION}:{ACCT}:rule/{RULE}")
print("scheduled",RULE)
# invoke + verify
r=lam.invoke(FunctionName=FN,InvocationType="RequestResponse")
print("invoke:",r.get("StatusCode"),r.get("FunctionError"),"|",r["Payload"].read()[:300])
time.sleep(2)
j=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/analyst-actions.json")["Body"].read())
print("\nv",j.get("version"),"counts:",json.dumps(j.get("counts")))
print("\n★ UPGRADES (sample):")
for x in j.get("upgrades",[])[:5]: print(f"  {x['ticker']:<6} {x.get('previous_rating')}→{x.get('rating')} ({x.get('firm')}) imp{x.get('importance')}")
print("★ PT RAISES (top by %):")
for x in j.get("pt_raises",[])[:5]: print(f"  {x['ticker']:<6} {x.get('pt_prev')}→{x.get('pt')} (+{round(x.get('pt_pct') or 0,1)}%) {x.get('firm')}")
print("★ GUIDANCE RAISES:")
for x in j.get("guidance_raises",[])[:5]: print(f"  {x['ticker']:<6} {x.get('fiscal_period')}{x.get('fiscal_year')} eps {x.get('eps_prev')}→{x.get('eps_mid')} rev_dir={x.get('rev_dir')}")
print("★ MOST BULLISH (net analyst score):")
for x in j.get("most_bullish",[])[:6]: print(f"  {x['ticker']:<6} score={x['net_score']} [{', '.join(x['signals'][:2])}]")
print("★ TOP PICKS (corroborated → harvester):",[(p['ticker'],p['score']) for p in j.get('top_picks',[])[:10]])
print("DONE 1980")
