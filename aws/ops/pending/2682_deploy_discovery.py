"""ops 2682 — create + deploy justhodl-universe-discovery + EventBridge schedule."""
import boto3, io, zipfile, json, time
from botocore.config import Config
REGION="us-east-1"; FN="justhodl-universe-discovery"; SRC=f"aws/lambdas/{FN}/source/lambda_function.py"
lam=boto3.client("lambda",region_name=REGION, config=Config(read_timeout=150, connect_timeout=10, retries={"max_attempts":0}))
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
        Handler="lambda_function.lambda_handler", Code={"ZipFile": buf.getvalue()}, Timeout=120, MemorySize=512,
        Description="Universe Discovery: IPO calendar, new SEC registrants, threshold crossers")
    print("created:", r.get("FunctionArn"))
else:
    wait()
    lam.update_function_code(FunctionName=FN, ZipFile=buf.getvalue()); print("code updated")
wait()

rule_name = "universe-discovery-daily"
ev.put_rule(Name=rule_name, ScheduleExpression="cron(10 12 * * ? *)", State="ENABLED")
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

j=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/universe-discovery.json")["Body"].read())
print("\nversion:", j.get("version"), "elapsed_s:", j.get("elapsed_s"))
ipo=j.get("ipo_calendar",{}); reg=j.get("new_registrants",{}); tc=j.get("threshold_crossers",{})
print(f"ipos: {ipo.get('n')} | new_registrants: {reg.get('n')} | threshold_crossers: {tc.get('n')}")
print("registrant errors:", reg.get("_debug_errors"))
print("crossers note:", tc.get("note"))
print("\nsample IPOs:")
for r2 in (ipo.get("items") or [])[:5]:
    print(f"  {r2.get('symbol') or '?':8s} {r2.get('company')} | {r2.get('date')} | {r2.get('exchange')}")
print("\nsample new registrants:")
for r2 in (reg.get("items") or [])[:5]:
    print(f"  {r2.get('ticker') or '?':6s} {r2.get('company')} | {r2.get('registration_type')} | {r2.get('file_date')}")
print("DONE 2682")
