import boto3, json, io, zipfile, time
lam=boto3.client("lambda","us-east-1"); ev=boto3.client("events","us-east-1"); s3=boto3.client("s3","us-east-1")
ROLE="arn:aws:iam::857687956942:role/lambda-execution-role"; ACCT="857687956942"; REGION="us-east-1"

# 1) patch estimate-revisions (dedup + 60d window)
FN1="justhodl-estimate-revisions"
b=io.BytesIO()
with zipfile.ZipFile(b,"w",zipfile.ZIP_DEFLATED) as z:
    for src,arc in [(f"aws/lambdas/{FN1}/source/lambda_function.py","lambda_function.py"),("aws/shared/benzinga.py","benzinga.py")]:
        zi=zipfile.ZipInfo(arc); zi.external_attr=0o644<<16; z.writestr(zi,open(src,"rb").read())
for _ in range(24):
    try: lam.update_function_code(FunctionName=FN1,ZipFile=b.getvalue()); break
    except lam.exceptions.ResourceConflictException: time.sleep(5)
for _ in range(30):
    c=lam.get_function_configuration(FunctionName=FN1)
    if c.get("State")=="Active" and c.get("LastUpdateStatus")!="InProgress": break
    time.sleep(3)
print("--- estimate-revisions patched; invoke ---")
r=lam.invoke(FunctionName=FN1,InvocationType="RequestResponse"); print(r["Payload"].read()[:220])

# 2) create boom-radar
FN="justhodl-boom-radar"
b=io.BytesIO()
with zipfile.ZipFile(b,"w",zipfile.ZIP_DEFLATED) as z:
    zi=zipfile.ZipInfo("lambda_function.py"); zi.external_attr=0o644<<16
    z.writestr(zi,open(f"aws/lambdas/{FN}/source/lambda_function.py","rb").read())
zb=b.getvalue()
try: lam.get_function(FunctionName=FN); exists=True
except lam.exceptions.ResourceNotFoundException: exists=False
if not exists:
    try: lam.create_function(FunctionName=FN,Runtime="python3.12",Role=ROLE,Handler="lambda_function.lambda_handler",Code={"ZipFile":zb},Timeout=120,MemorySize=512,Description="Catalyst-convergence boom detector"); print("created",FN)
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
RULE="justhodl-boom-radar-daily"
ev.put_rule(Name=RULE,ScheduleExpression="cron(30 14 * * ? *)",State="ENABLED",Description="daily boom-radar after inputs refresh")
ev.put_targets(Rule=RULE,Targets=[{"Id":"1","Arn":lam.get_function(FunctionName=FN)["Configuration"]["FunctionArn"]}])
try: lam.remove_permission(FunctionName=FN,StatementId="evt-boom-daily")
except Exception: pass
lam.add_permission(FunctionName=FN,StatementId="evt-boom-daily",Action="lambda:InvokeFunction",Principal="events.amazonaws.com",SourceArn=f"arn:aws:events:{REGION}:{ACCT}:rule/{RULE}")
print("scheduled",RULE)
print("\n--- boom-radar invoke ---")
r=lam.invoke(FunctionName=FN,InvocationType="RequestResponse")
print("invoke:",r.get("StatusCode"),r.get("FunctionError"),"|",r["Payload"].read()[:500])
time.sleep(2)
j=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/boom-radar.json")["Body"].read())
print("\ndimensions loaded:",j.get("dimensions_loaded"))
print(f"scanned={j['n_scanned']} 2way={j['n_2way']} 3way={j['n_3way']} 4way+={j['n_4way_plus']}")
print("\n★ HIGH-CONVICTION BOOM CANDIDATES (>=3 independent signals):")
for c in j.get("high_conviction",[])[:12]:
    print(f"  {c['ticker']:<6} conv={c['convergence']} score={c['boom_score']}  [{', '.join(c['dimensions'])}]")
    for rs in c['reasons'][:4]: print(f"        - {rs}")
print("\n★ TOP PICKS → harvester:",[(p['ticker'],p['convergence'],p['score']) for p in j.get('top_picks',[])])
print("DONE 1988")
