"""ops 2677 — retry: wait for justhodl-structural-pre-signals to settle out of Pending,
then update code + ensure schedule + invoke + verify."""
import boto3, io, zipfile, json, time
from botocore.config import Config
REGION="us-east-1"; FN="justhodl-structural-pre-signals"; SRC=f"aws/lambdas/{FN}/source/lambda_function.py"
lam=boto3.client("lambda",region_name=REGION, config=Config(read_timeout=150, connect_timeout=10, retries={"max_attempts":0}))
ev=boto3.client("events",region_name=REGION)
s3=boto3.client("s3",region_name=REGION)

def wait():
    for i in range(40):
        c=lam.get_function(FunctionName=FN)["Configuration"]
        print(f"  state check {i}: State={c.get('State')} LastUpdateStatus={c.get('LastUpdateStatus')}")
        if c.get("State")=="Active" and c.get("LastUpdateStatus")=="Successful": return
        time.sleep(5)
wait()

buf=io.BytesIO()
with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as z: z.writestr("lambda_function.py",open(SRC,"rb").read())
for _ in range(6):
    try: lam.update_function_code(FunctionName=FN,ZipFile=buf.getvalue()); print("code updated"); break
    except lam.exceptions.ResourceConflictException: time.sleep(10); wait()
wait()

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

j=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/structural-pre-signals.json")["Body"].read())
print("\nversion:", j.get("version"), "elapsed_s:", j.get("elapsed_s"))
rest = j.get("restructuring",{}); bo = j.get("buildout",{})
print(f"restructuring: {rest.get('n')} filings | buildout: {bo.get('n')} filings")
print("\nsample restructuring filings:")
for r2 in (rest.get("items") or [])[:6]:
    print(f"  {r2.get('ticker') or '?':6s} {r2.get('company')} | {r2.get('file_date')} | {r2.get('sector')}")
print("\nsample buildout filings:")
for r2 in (bo.get("items") or [])[:6]:
    print(f"  {r2.get('ticker') or '?':6s} {r2.get('company')} | {r2.get('file_date')} | {r2.get('sector')}")
print("by_sector:", bo.get("by_sector"))
print("DONE 2677")
