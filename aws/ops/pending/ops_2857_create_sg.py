import os, io, json, time, zipfile, boto3
from datetime import datetime, timezone
from botocore.config import Config
REGION="us-east-1"; ACCT="857687956942"; FN="justhodl-singapore-nodx"
ROLE="arn:aws:iam::%s:role/lambda-execution-role"%ACCT
lam=boto3.client("lambda",region_name=REGION,config=Config(read_timeout=120,retries={"max_attempts":0}))
events=boto3.client("events",region_name=REGION); s3=boto3.client("s3",region_name=REGION)
R={"ops":2857,"ts":datetime.now(timezone.utc).isoformat()}
src=open("aws/lambdas/justhodl-singapore-nodx/source/lambda_function.py",encoding="utf-8").read()
buf=io.BytesIO(); z=zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED); z.writestr("lambda_function.py",src); z.close(); code=buf.getvalue()
def ready():
    for _ in range(40):
        try:
            c=lam.get_function_configuration(FunctionName=FN)
            if c.get("State")=="Active" and c.get("LastUpdateStatus")=="Successful": return
        except Exception: pass
        time.sleep(3)
try: lam.get_function(FunctionName=FN); ex=True
except Exception: ex=False
if ex: lam.update_function_code(FunctionName=FN,ZipFile=code); ready(); R["action"]="updated"
else:
    lam.create_function(FunctionName=FN,Runtime="python3.12",Role=ROLE,Handler="lambda_function.lambda_handler",
        Code={"ZipFile":code},Timeout=60,MemorySize=128,Description="Singapore NODX (SingStat)"); ready(); R["action"]="created"
rule="justhodl-singapore-nodx-daily"
events.put_rule(Name=rule,ScheduleExpression="cron(20 4 * * ? *)",State="ENABLED",Description="Daily Singapore NODX")
try: lam.add_permission(FunctionName=FN,StatementId="sg-sched",Action="lambda:InvokeFunction",Principal="events.amazonaws.com",SourceArn="arn:aws:events:%s:%s:rule/%s"%(REGION,ACCT,rule))
except Exception: pass
events.put_targets(Rule=rule,Targets=[{"Id":"1","Arn":"arn:aws:lambda:%s:%s:function:%s"%(REGION,ACCT,FN)}])
try: R["sg_invoke"]=json.loads(lam.invoke(FunctionName=FN,InvocationType="RequestResponse")["Payload"].read().decode())
except Exception as e: R["sg_invoke_err"]=str(e)[:120]
time.sleep(2)
sg=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/singapore-nodx.json")["Body"].read())
R["nodx_total"]={k:(sg.get("nodx_total") or {}).get(k) for k in ("latest_period","yoy_pct","yoy_3mma_pct","n","read")}
R["electronics"]={k:(sg.get("electronics") or {}).get(k) for k in ("yoy_pct","yoy_3mma_pct","read")}
R["integrated_circuits_yoy"]=(sg.get("integrated_circuits") or {}).get("yoy_pct")
try: lam.invoke(FunctionName="justhodl-canary-grid",InvocationType="RequestResponse")["Payload"].read()
except Exception as e: R["canary_inv"]=str(e)[:60]
time.sleep(3)
cg=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/canary-grid.json")["Body"].read())
sn=[s for s in (cg.get("signals") or []) if s.get("key")=="singapore_nodx"]
R["canary_singapore"]=({"available":sn[0].get("available"),"value":sn[0].get("value"),"stress":sn[0].get("stress"),"age":sn[0].get("age_days")} if sn else "missing")
R["grid_n"]={"avail":cg.get("n_available"),"total":cg.get("n_total"),"ew":cg.get("early_warning_level")}
R["status"]="LIVE" if (R["nodx_total"]["yoy_pct"] is not None and sn and sn[0].get("available")) else "CHECK"
print(json.dumps(R,ensure_ascii=False,indent=1,default=str)[:2200])
os.makedirs("aws/ops/reports",exist_ok=True); json.dump(R,open("aws/ops/reports/2857_create_sg.json","w"),ensure_ascii=False,indent=1,default=str)
print("OPS 2857 COMPLETE")
