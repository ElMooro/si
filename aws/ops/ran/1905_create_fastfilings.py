import boto3, json, zipfile, io, glob, time
from botocore.config import Config
from botocore.exceptions import ClientError
lam=boto3.client("lambda","us-east-1",config=Config(read_timeout=240,retries={"max_attempts":0}))
s3=boto3.client("s3","us-east-1"); events=boto3.client("events","us-east-1")
B="justhodl-dashboard-live"; FN="justhodl-fast-filings"; REGION="us-east-1"; ACCT="857687956942"
ROLE="arn:aws:iam::857687956942:role/lambda-execution-role"; RULE="fast-filings-daily"
src=open(glob.glob("**/justhodl-fast-filings/source/lambda_function.py",recursive=True)[0]).read()
buf=io.BytesIO()
with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as z: z.writestr("lambda_function.py",src)
code=buf.getvalue()
try:
    lam.create_function(FunctionName=FN,Runtime="python3.12",Role=ROLE,Handler="lambda_function.lambda_handler",
        Code={"ZipFile":code},Timeout=240,MemorySize=512,Architectures=["x86_64"],Description="Fast filings 13D/G + Form 4 clusters")
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
events.put_rule(Name=RULE,ScheduleExpression="cron(0 12 * * ? *)",State="ENABLED",Description="Daily fast filings 12:00 UTC")
arn=lam.get_function(FunctionName=FN)["Configuration"]["FunctionArn"]
try:
    lam.add_permission(FunctionName=FN,StatementId="fast-filings-daily-evt",Action="lambda:InvokeFunction",
        Principal="events.amazonaws.com",SourceArn="arn:aws:events:%s:%s:rule/%s"%(REGION,ACCT,RULE))
except ClientError as e:
    if "ResourceConflict" not in str(e): print("perm:",str(e)[:50])
events.put_targets(Rule=RULE,Targets=[{"Id":"1","Arn":arn}])
print("SCHEDULED 12:00 UTC")
try:
    r=lam.invoke(FunctionName=FN,InvocationType="RequestResponse"); print("INVOKE:",r["Payload"].read().decode()[:160])
except Exception as e:
    print("sync invoke timed out (engine runs long), async instead:",str(e)[:60]); lam.invoke(FunctionName=FN,InvocationType="Event")
    time.sleep(0)
time.sleep(2)
try:
    d=json.loads(s3.get_object(Bucket=B,Key="data/fast-filings.json")["Body"].read())
    print("activist=%s (w/ticker %s) clusters=%s scanned=%s"%(d.get("n_activist"),len(d.get("activist_with_ticker",[])),d.get("n_clusters"),d.get("universe_scanned")))
    print("ACTIVIST w/ticker:",[(a["subject_ticker"],a["form"],a["date"]) for a in (d.get("activist_with_ticker") or [])][:8])
    print("FORM4 CLUSTERS:",[(c["symbol"],c["n_buyers"],c["shares_bought"]) for c in (d.get("form4_clusters") or [])][:8])
    print("picks:",[p["symbol"] for p in (d.get("picks") or [])][:14])
except Exception as e: print("output not ready yet:",str(e)[:60])
