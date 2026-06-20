"""1971 — create justhodl-schedule-liveness (new dir), schedule daily, invoke, verify self-heal."""
import boto3, json, io, zipfile, time
lam=boto3.client("lambda","us-east-1"); events=boto3.client("events","us-east-1"); s3=boto3.client("s3","us-east-1")
ACCT="857687956942"; R="us-east-1"; ROLE=f"arn:aws:iam::{ACCT}:role/lambda-execution-role"
FN="justhodl-schedule-liveness"; RULE="justhodl-schedule-liveness-daily"; CRON="cron(30 13 * * ? *)"
src=open(f"aws/lambdas/{FN}/source/lambda_function.py","rb").read()
buf=io.BytesIO()
with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as z:
    zi=zipfile.ZipInfo("lambda_function.py"); zi.external_attr=0o644<<16; z.writestr(zi,src)
zb=buf.getvalue()
try:
    lam.get_function(FunctionName=FN); exists=True
except lam.exceptions.ResourceNotFoundException: exists=False
if exists:
    for _ in range(24):
        try: lam.update_function_code(FunctionName=FN,ZipFile=zb); break
        except lam.exceptions.ResourceConflictException: time.sleep(5)
    print("updated", FN)
else:
    try:
        lam.create_function(FunctionName=FN,Runtime="python3.12",Role=ROLE,
            Handler="lambda_function.lambda_handler",Code={"ZipFile":zb},Timeout=600,MemorySize=512,
            Environment={"Variables":{"S3_BUCKET":"justhodl-dashboard-live"}},
            Description="schedule liveness watchdog",
            Tags={"Project":"JustHodl","Component":"observability","Tier":"watchdog"})
        print("created", FN)
    except lam.exceptions.ResourceConflictException:
        for _ in range(24):
            try: lam.update_function_code(FunctionName=FN,ZipFile=zb); break
            except lam.exceptions.ResourceConflictException: time.sleep(5)
        print("existed->updated", FN)
for _ in range(30):
    c=lam.get_function_configuration(FunctionName=FN)
    if c.get("State")=="Active" and c.get("LastUpdateStatus")!="InProgress": break
    time.sleep(3)
events.put_rule(Name=RULE,ScheduleExpression=CRON,State="ENABLED",Description="daily liveness sweep")
events.put_targets(Rule=RULE,Targets=[{"Id":"1","Arn":f"arn:aws:lambda:{R}:{ACCT}:function:{FN}"}])
sid=f"{RULE}-invoke"
try: lam.remove_permission(FunctionName=FN,StatementId=sid)
except Exception: pass
lam.add_permission(FunctionName=FN,StatementId=sid,Action="lambda:InvokeFunction",
    Principal="events.amazonaws.com",SourceArn=f"arn:aws:events:{R}:{ACCT}:rule/{RULE}")
print("scheduled:", CRON)
print("invoking watchdog (sync)...")
r=lam.invoke(FunctionName=FN,InvocationType="RequestResponse")
print("StatusCode:",r.get("StatusCode"),"FunctionError:",r.get("FunctionError"))
print("payload:",r["Payload"].read()[:300])
time.sleep(2)
j=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/schedule-liveness.json")["Body"].read())
print(f"\nchecked={j['n_rules_checked']} assessable={j['n_assessable']} healthy={j['n_healthy']} REVIVED={j['n_revived']} genuine_fail={j['n_genuine_failures']} no_feed={j['n_no_feed']}")
print("\nrevived this run:")
for x in j.get("revived",[])[:15]:
    print(f"  {x['fn']:<34} age={x['age_h']}h cad={x['cadence_h']}h rebuilt={x.get('binding_rebuilt')}")
print("\ngenuine failures:")
for x in j.get("genuine_failures",[])[:10]:
    print(f"  {x['fn']:<34} still {x['age_h']}h after {x['consecutive_revives']}x")
print("\noldest healthy (sanity):")
for x in j.get("stale_healthy_sample",[])[:5]:
    print(f"  {x['fn']:<34} age={x['age_h']}h cad={x['cadence_h']}h")
print("DONE 1971")
