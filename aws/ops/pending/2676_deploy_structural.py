"""ops 2676 — create + deploy justhodl-structural-pre-signals + EventBridge schedule."""
import boto3, io, zipfile, json, time
from botocore.config import Config
REGION="us-east-1"; FN="justhodl-structural-pre-signals"; SRC=f"aws/lambdas/{FN}/source/lambda_function.py"
lam=boto3.client("lambda",region_name=REGION, config=Config(read_timeout=150, connect_timeout=10, retries={"max_attempts":0}))
ev=boto3.client("events",region_name=REGION)

buf=io.BytesIO()
with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as z: z.writestr("lambda_function.py",open(SRC,"rb").read())

try:
    lam.get_function(FunctionName=FN); exists=True
except lam.exceptions.ResourceNotFoundException:
    exists=False

if not exists:
    r = lam.create_function(FunctionName=FN, Runtime="python3.12", Role="arn:aws:iam::857687956942:role/lambda-execution-role",
        Handler="lambda_function.lambda_handler", Code={"ZipFile": buf.getvalue()}, Timeout=120, MemorySize=512,
        Description="Structural Pre-Signals: SEC EDGAR mandated-disclosure early warnings")
    print("created:", r.get("FunctionArn"))
else:
    lam.update_function_code(FunctionName=FN, ZipFile=buf.getvalue()); print("code updated")

for _ in range(30):
    c=lam.get_function(FunctionName=FN)["Configuration"]
    if c.get("State")=="Active" and c.get("LastUpdateStatus")=="Successful": break
    time.sleep(4)

rule_name = "structural-pre-signals-daily"
ev.put_rule(Name=rule_name, ScheduleExpression="cron(20 11 * * ? *)", State="ENABLED")
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

s3=boto3.client("s3",region_name=REGION)
j=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/structural-pre-signals.json")["Body"].read())
print("\nversion:", j.get("version"), "elapsed_s:", j.get("elapsed_s"))
rest = j.get("restructuring",{}); bo = j.get("buildout",{})
print(f"restructuring: {rest.get('n')} filings | buildout: {bo.get('n')} filings")
print("\nsample restructuring filings:")
for r2 in (rest.get("items") or [])[:5]:
    print(f"  {r2.get('ticker') or '?':6s} {r2.get('company')} | {r2.get('file_date')} | {r2.get('sector')}")
print("\nsample buildout filings:")
for r2 in (bo.get("items") or [])[:5]:
    print(f"  {r2.get('ticker') or '?':6s} {r2.get('company')} | {r2.get('file_date')} | {r2.get('sector')}")
print("by_sector:", bo.get("by_sector"))
print("DONE 2676")
