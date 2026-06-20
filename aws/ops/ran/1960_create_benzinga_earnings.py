"""1960 — create justhodl-benzinga-earnings (new dir => boto3 create), bundle
shared massive.py, attach daily schedule, invoke + verify output."""
import boto3, json, io, zipfile, time, os
lam=boto3.client("lambda","us-east-1"); events=boto3.client("events","us-east-1")
s3=boto3.client("s3","us-east-1")
FN="justhodl-benzinga-earnings"; ACCT="857687956942"
ROLE=f"arn:aws:iam::{ACCT}:role/lambda-execution-role"
SRC="aws/lambdas/justhodl-benzinga-earnings/source/lambda_function.py"
SHARED="aws/shared/massive.py"

buf=io.BytesIO()
with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as z:
    z.write(SRC,"lambda_function.py")
    z.write(SHARED,"massive.py")
zip_bytes=buf.getvalue()
print(f"zip built: {len(zip_bytes)} bytes")

exists=True
try: lam.get_function(FunctionName=FN)
except lam.exceptions.ResourceNotFoundException: exists=False

if not exists:
    lam.create_function(FunctionName=FN, Runtime="python3.12", Role=ROLE,
        Handler="lambda_function.lambda_handler", Code={"ZipFile":zip_bytes},
        Timeout=300, MemorySize=512,
        Environment={"Variables":{"BUCKET":"justhodl-dashboard-live"}},
        Description="Authoritative Benzinga earnings feed (surprises + calendar) for PEAD engines")
    print("created function")
else:
    for a in range(24):
        try: lam.update_function_code(FunctionName=FN, ZipFile=zip_bytes); break
        except lam.exceptions.ResourceConflictException: time.sleep(5)
    print("updated code")

# wait active
for _ in range(30):
    c=lam.get_function_configuration(FunctionName=FN)
    if c.get("State")=="Active" and c.get("LastUpdateStatus")!="InProgress": break
    time.sleep(3)

# schedule
RULE="benzinga-earnings-daily"; CRON="cron(20 11 * * ? *)"
events.put_rule(Name=RULE, ScheduleExpression=CRON, State="ENABLED")
events.put_targets(Rule=RULE, Targets=[{"Id":"1","Arn":f"arn:aws:lambda:us-east-1:{ACCT}:function:{FN}"}])
sid=f"{RULE}-invoke"
try: lam.remove_permission(FunctionName=FN, StatementId=sid)
except Exception: pass
lam.add_permission(FunctionName=FN, StatementId=sid, Action="lambda:InvokeFunction",
    Principal="events.amazonaws.com", SourceArn=f"arn:aws:events:us-east-1:{ACCT}:rule/{RULE}")
print(f"schedule {RULE} {CRON} wired")

# invoke + verify
print("invoking (sync, may take ~1-2min for bulk pull)...")
r=lam.invoke(FunctionName=FN, InvocationType="RequestResponse")
payload=r["Payload"].read().decode()
print("invoke StatusCode:", r.get("StatusCode"), "FunctionError:", r.get("FunctionError"))
print("body:", payload[:600])
time.sleep(2)
try:
    head=s3.head_object(Bucket="justhodl-dashboard-live", Key="data/benzinga-earnings.json")
    print("data/benzinga-earnings.json size:", head["ContentLength"], "modified:", head["LastModified"])
    obj=json.loads(s3.get_object(Bucket="justhodl-dashboard-live", Key="data/benzinga-earnings.json")["Body"].read())
    print("  n_tickers:", obj.get("n_tickers"), "n_reported_rows:", obj.get("n_reported_rows"))
    print("  pead_top_positive[:3]:", json.dumps(obj.get("pead_top_positive",[])[:3]))
except Exception as e:
    print("verify err:", e)
print("DONE 1960")
