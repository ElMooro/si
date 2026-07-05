import os, io, json, time, zipfile, boto3
from datetime import datetime, timezone
from botocore.config import Config
REGION="us-east-1"; ACCT="857687956942"; FN="justhodl-peru-copper"
ROLE="arn:aws:iam::%s:role/lambda-execution-role"%ACCT
lam=boto3.client("lambda",region_name=REGION,config=Config(read_timeout=120,retries={"max_attempts":0}))
events=boto3.client("events",region_name=REGION); s3=boto3.client("s3",region_name=REGION)
R={"ops":2853,"ts":datetime.now(timezone.utc).isoformat()}
src=open("aws/lambdas/justhodl-peru-copper/source/lambda_function.py",encoding="utf-8").read()
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
        Code={"ZipFile":code},Timeout=60,MemorySize=128,Description="Peru copper production (BCRP)"); ready(); R["action"]="created"
rule="justhodl-peru-copper-daily"
events.put_rule(Name=rule,ScheduleExpression="cron(0 4 * * ? *)",State="ENABLED",Description="Daily Peru BCRP copper")
try: lam.add_permission(FunctionName=FN,StatementId="peru-sched",Action="lambda:InvokeFunction",Principal="events.amazonaws.com",SourceArn="arn:aws:events:%s:%s:rule/%s"%(REGION,ACCT,rule))
except Exception: pass
events.put_targets(Rule=rule,Targets=[{"Id":"1","Arn":"arn:aws:lambda:%s:%s:function:%s"%(REGION,ACCT,FN)}])
# invoke peru agent
try: R["peru_invoke"]=json.loads(lam.invoke(FunctionName=FN,InvocationType="RequestResponse")["Payload"].read().decode())
except Exception as e: R["peru_invoke_err"]=str(e)[:120]
time.sleep(2)
pc=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/peru-copper.json")["Body"].read())
R["peru_copper"]={"yoy":pc.get("copper_production",{}).get("yoy_pct"),"yoy_3mma":pc.get("copper_production",{}).get("yoy_3mma_pct"),"latest":pc.get("copper_production",{}).get("latest_period"),"n":pc.get("copper_production",{}).get("n"),"read":pc.get("read")}
# now canary-grid should pick up the peru feed
try: lam.invoke(FunctionName="justhodl-canary-grid",InvocationType="RequestResponse")["Payload"].read()
except Exception as e: R["canary_inv"]=str(e)[:80]
time.sleep(3)
cg=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/canary-grid.json")["Body"].read())
peru=[s for s in (cg.get("signals") or []) if s.get("key")=="peru_copper"]
R["canary_peru"]=({"available":peru[0].get("available"),"value":peru[0].get("value"),"stress":peru[0].get("stress_score") or peru[0].get("stress")} if peru else "not found")
R["grid"]={"n_available":cg.get("n_available"),"n_total":cg.get("n_total"),"early_warning":cg.get("early_warning_level")}
R["status"]="LIVE" if (R["peru_copper"]["yoy"] is not None and peru and peru[0].get("available")) else "CHECK"
print(json.dumps(R,ensure_ascii=False,indent=1,default=str)[:2200])
os.makedirs("aws/ops/reports",exist_ok=True); json.dump(R,open("aws/ops/reports/2853_create_peru.json","w"),ensure_ascii=False,indent=1,default=str)
print("OPS 2853 COMPLETE")
