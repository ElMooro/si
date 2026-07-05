"""ops 2848 — create justhodl-taiwan-moea (new dir → deploy no-ops) + EventBridge + verify."""
import os, io, json, time, zipfile, boto3
from datetime import datetime, timezone
from botocore.config import Config
REGION="us-east-1"; ACCT="857687956942"; FN="justhodl-taiwan-moea"
ROLE="arn:aws:iam::%s:role/lambda-execution-role"%ACCT
lam=boto3.client("lambda",region_name=REGION,config=Config(read_timeout=300,retries={"max_attempts":0}))
events=boto3.client("events",region_name=REGION)
s3=boto3.client("s3",region_name=REGION)
R={"ops":2848,"ts":datetime.now(timezone.utc).isoformat()}
src=open("aws/lambdas/justhodl-taiwan-moea/source/lambda_function.py",encoding="utf-8").read()
buf=io.BytesIO(); z=zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED); z.writestr("lambda_function.py",src); z.close(); code=buf.getvalue()
def wait_ready():
    for _ in range(40):
        try:
            c=lam.get_function_configuration(FunctionName=FN)
            if c.get("State")=="Active" and c.get("LastUpdateStatus")=="Successful": return
        except Exception: pass
        time.sleep(3)
try:
    lam.get_function(FunctionName=FN); exists=True
except Exception: exists=False
if exists:
    lam.update_function_code(FunctionName=FN,ZipFile=code); wait_ready(); R["action"]="updated"
else:
    lam.create_function(FunctionName=FN,Runtime="python3.12",Role=ROLE,Handler="lambda_function.lambda_handler",
        Code={"ZipFile":code},Timeout=120,MemorySize=256,
        Description="Taiwan MOEA forward-momentum leads (export orders + semiconductor production)")
    wait_ready(); R["action"]="created"
# EventBridge daily
rule="justhodl-taiwan-moea-daily"
events.put_rule(Name=rule,ScheduleExpression="cron(30 3 * * ? *)",State="ENABLED",Description="Daily Taiwan MOEA pull")
try:
    lam.add_permission(FunctionName=FN,StatementId="tw-moea-sched",Action="lambda:InvokeFunction",
        Principal="events.amazonaws.com",SourceArn="arn:aws:events:%s:%s:rule/%s"%(REGION,ACCT,rule))
except Exception as e: R["perm_note"]=str(e)[:60]
events.put_targets(Rule=rule,Targets=[{"Id":"1","Arn":"arn:aws:lambda:%s:%s:function:%s"%(REGION,ACCT,FN)}])
R["scheduled"]=True
# invoke + verify
try:
    resp=lam.invoke(FunctionName=FN,InvocationType="RequestResponse")
    R["invoke_payload"]=json.loads(resp["Payload"].read().decode())
except Exception as e: R["invoke_err"]=str(e)[:150]
time.sleep(3)
d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/taiwan-moea.json")["Body"].read())
eo=d.get("export_orders") or {}; sc=(d.get("semiconductor") or {}).get("production") or {}
inv=(d.get("semiconductor") or {}).get("inventory") or {}
R["export_orders"]={"latest":eo.get("latest_period"),"value":eo.get("latest_value"),"yoy":eo.get("yoy_pct"),
    "yoy_3mma":eo.get("yoy_3mma_pct"),"z":eo.get("yoy_z_5y"),"n":eo.get("n"),"read":eo.get("read")}
R["semiconductor_production"]={"latest":sc.get("latest_period"),"yoy":sc.get("yoy_pct"),
    "yoy_3mma":sc.get("yoy_3mma_pct"),"z":sc.get("yoy_z_5y"),"n":sc.get("n"),"read":sc.get("read")}
R["semiconductor_inventory_yoy"]=inv.get("yoy_pct")
R["headline"]=d.get("headline")
R["status"]="LIVE" if (eo.get("yoy_pct") is not None and sc.get("yoy_pct") is not None) else "CHECK"
print(json.dumps(R,ensure_ascii=False,indent=1,default=str)[:2600])
os.makedirs("aws/ops/reports",exist_ok=True); json.dump(R,open("aws/ops/reports/2848_create_taiwan.json","w"),ensure_ascii=False,indent=1,default=str)
print("OPS 2848 COMPLETE")
