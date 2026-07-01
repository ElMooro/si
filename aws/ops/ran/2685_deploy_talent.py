"""ops 2685 — create + deploy justhodl-talent-migration + EventBridge schedule."""
import boto3, io, zipfile, json, time
from botocore.config import Config
REGION="us-east-1"; FN="justhodl-talent-migration"; SRC=f"aws/lambdas/{FN}/source/lambda_function.py"
lam=boto3.client("lambda",region_name=REGION, config=Config(read_timeout=180, connect_timeout=10, retries={"max_attempts":0}))
ev=boto3.client("events",region_name=REGION)
s3=boto3.client("s3",region_name=REGION)

def wait():
    for i in range(40):
        c=lam.get_function(FunctionName=FN)["Configuration"]
        if c.get("State")=="Active" and c.get("LastUpdateStatus")=="Successful": return
        time.sleep(5)

try:
    lam.get_function(FunctionName=FN); exists=True
except lam.exceptions.ResourceNotFoundException:
    exists=False

buf=io.BytesIO()
with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as z: z.writestr("lambda_function.py",open(SRC,"rb").read())

if not exists:
    r = lam.create_function(FunctionName=FN, Runtime="python3.12", Role="arn:aws:iam::857687956942:role/lambda-execution-role",
        Handler="lambda_function.lambda_handler", Code={"ZipFile": buf.getvalue()}, Timeout=150, MemorySize=512,
        Description="Talent Migration: executive departures/appointments via 8-K Item 5.02")
    print("created:", r.get("FunctionArn"))
else:
    wait()
    lam.update_function_code(FunctionName=FN, ZipFile=buf.getvalue()); print("code updated")
wait()

rule_name = "talent-migration-daily"
ev.put_rule(Name=rule_name, ScheduleExpression="cron(5 13 * * ? *)", State="ENABLED")
try:
    lam.add_permission(FunctionName=FN, StatementId="EventBridgeInvoke", Action="lambda:InvokeFunction",
                       Principal="events.amazonaws.com", SourceArn=f"arn:aws:events:{REGION}:857687956942:rule/{rule_name}")
except Exception: pass
fn_arn = lam.get_function(FunctionName=FN)["Configuration"]["FunctionArn"]
ev.put_targets(Rule=rule_name, Targets=[{"Id":"1","Arn":fn_arn}])
print("schedule wired")

r=lam.invoke(FunctionName=FN,InvocationType="RequestResponse",Payload=b"{}")
print("INVOKE:",r.get("StatusCode"),r.get("FunctionError"))
print("BODY:", r["Payload"].read().decode()[:400])
time.sleep(2)

j=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/talent-migration.json")["Body"].read())
print("\nversion:", j.get("version"), "elapsed_s:", j.get("elapsed_s"))
print(f"total: {j.get('n_total')} | classified: {j.get('n_classified')} | departures: {j.get('n_departures')} | appointments: {j.get('n_appointments')}")
print("\nsample departures:")
for r2 in (j.get("departures") or [])[:5]:
    print(f"  {r2.get('ticker') or '?':6s} {r2.get('company')} | {r2.get('file_date')} | roles={r2.get('roles_mentioned')}")
print("\nsample appointments:")
for r2 in (j.get("appointments") or [])[:5]:
    print(f"  {r2.get('ticker') or '?':6s} {r2.get('company')} | {r2.get('file_date')} | roles={r2.get('roles_mentioned')}")

from collections import Counter
moves = j.get("recent_moves", [])
dupe_check = Counter(r.get("adsh") for r in moves)
dupes = {k:v for k,v in dupe_check.items() if v>1}
print(f"\nduplicate adsh check: {len(dupes)} {'(clean)' if not dupes else dupes}")
print("DONE 2685")
